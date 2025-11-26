# utils/data_loader.py
import pandas as pd
from Review.functions import read_semicolon_csv
import os

def load_eod_data(date, co_date, area, area2, dlf_folder):
    """Load and combine EOD data from different areas, handling missing files gracefully"""
    
    def safe_load_csv(file_path, description):
        """Safely load CSV file, return None if file doesn't exist"""
        try:
            if os.path.exists(file_path):
                return read_semicolon_csv(file_path, encoding="latin1")
            else:
                print(f"Warning: {description} file not found: {file_path}")
                return None
        except Exception as e:
            print(f"Error loading {description}: {str(e)}")
            return None
    
    # Define file paths
    files_to_load = {
        'index_eod_us': os.path.join(dlf_folder, f"TTMIndex{area}1_GIS_EOD_INDEX_{date}.csv"),
        'stock_eod_us': os.path.join(dlf_folder, f"TTMIndex{area}1_GIS_EOD_STOCK_{date}.csv"),
        'index_eod_eu': os.path.join(dlf_folder, f"TTMIndex{area2}1_GIS_EOD_INDEX_{date}.csv"),
        'stock_eod_eu': os.path.join(dlf_folder, f"TTMIndex{area2}1_GIS_EOD_STOCK_{date}.csv"),
        'stock_co_us': os.path.join(dlf_folder, f"TTMIndex{area}1_GIS_EOD_STOCK_{co_date}.csv"),
        'stock_co_eu': os.path.join(dlf_folder, f"TTMIndex{area2}1_GIS_EOD_STOCK_{co_date}.csv")
    }
    
    # Load all files
    loaded_data = {}
    for key, file_path in files_to_load.items():
        loaded_data[key] = safe_load_csv(file_path, key.replace('_', ' ').title())
    
    # Combine INDEX files (need at least one)
    index_dfs = [df for df in [loaded_data['index_eod_us'], loaded_data['index_eod_eu']] if df is not None]
    if not index_dfs:
        raise ValueError("No index EOD files found! At least one index file is required.")
    
    index_eod_df = pd.concat(index_dfs, ignore_index=True) if len(index_dfs) > 1 else index_dfs[0]
    print(f"Loaded index data from {len(index_dfs)} file(s)")
    
    # Combine STOCK EOD files (need at least one)
    stock_eod_dfs = [df for df in [loaded_data['stock_eod_us'], loaded_data['stock_eod_eu']] if df is not None]
    if not stock_eod_dfs:
        raise ValueError("No stock EOD files found! At least one stock EOD file is required.")
    
    stock_eod_df = pd.concat(stock_eod_dfs, ignore_index=True) if len(stock_eod_dfs) > 1 else stock_eod_dfs[0]
    print(f"Loaded stock EOD data from {len(stock_eod_dfs)} file(s)")
    
    # Combine STOCK CO files (need at least one)
    stock_co_dfs = [df for df in [loaded_data['stock_co_us'], loaded_data['stock_co_eu']] if df is not None]
    if not stock_co_dfs:
        raise ValueError("No stock CO files found! At least one stock CO file is required.")
    
    stock_co_df = pd.concat(stock_co_dfs, ignore_index=True) if len(stock_co_dfs) > 1 else stock_co_dfs[0]
    print(f"Loaded stock CO data from {len(stock_co_dfs)} file(s)")
    
    # Add 'Index Curr' column to stock_eod_df by merging with index_eod_df
    stock_eod_df = stock_eod_df.merge(
        index_eod_df[['Mnemo', 'Curr']], 
        left_on='Index', 
        right_on='Mnemo', 
        how='left',
        suffixes=('', '_index')
    )
    stock_eod_df = stock_eod_df.rename(columns={'Curr': 'Index Curr'}).drop(columns=['Mnemo_index'])
    
    # Add 'Index Curr' column to stock_co_df by merging with index_eod_df
    stock_co_df = stock_co_df.merge(
        index_eod_df[['Mnemo', 'Curr']], 
        left_on='Index', 
        right_on='Mnemo', 
        how='left',
        suffixes=('', '_index')
    )
    stock_co_df = stock_co_df.rename(columns={'Curr': 'Index Curr'}).drop(columns=['Mnemo_index'])
    
    return index_eod_df, stock_eod_df, stock_co_df

def load_reference_data(current_data_folder, required_files=None, universe_name=None, sheet_names=None):
    """
    Load reference data files
    
    Args:
        current_data_folder (str): Path to data folder
        required_files (list, optional): List of specific files needed
        universe_name (str, optional): Name of the universe file
        sheet_names (dict, optional): Dictionary mapping file keys to sheet names
            e.g., {'cac_family': 'PX1'}
            
    Returns:
        dict: Dictionary of DataFrames, keys are standardized names
    """
    # Sheet names is optional
    sheet_names = sheet_names or {}

    # Define all possible files and their loading parameters
    all_files = {
        'ff': {
            'filename': 'FF.xlsx',
            'loader': lambda f: pd.read_excel(f).drop_duplicates(subset=['ISIN Code:'], keep='first')
        },
        'developed_market': {
            'filename': 'Developed Market.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'icb': {
            'filename': 'ICB.xlsx',
            'loader': lambda f: pd.read_excel(f, header=3)
        },
        '98_universe': {
            'filename': 'universe_investable_final_ffmc.csv',
            'loader': lambda f: read_semicolon_csv(f, encoding="latin1")
        },
        'emerging_market': {
            'filename': 'Emerging Market.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'nace': {
            'filename': 'NACE.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'oekom_trustcarbon': {
            'filename': 'Oekom Trust&Carbon.xlsx',
            'loader': lambda f: pd.read_excel(f, header=1)
        },
        'sesamm': {
            'filename': 'SESAMm.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'cac_family': {
            'filename': 'CAC Family.xlsx',
            'loader': lambda f, sheet=None: pd.read_excel(f, header=1, sheet_name=sheet if sheet else 0)
        },
        'aex_family': {
            'filename': 'AEX Family.xlsx',
            'loader': lambda f, sheet=None: pd.read_excel(f, header=1, sheet_name=sheet if sheet else 0)
        },
        'oekom_score': {
            'filename': 'Oekom Score.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'eurozone_300': {
            'filename': 'Eurozone 300.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'north_america_500': {
            'filename': 'North America 500.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'europe_500': {
            'filename': 'Europe 500.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'aex_bel': {
            'filename': 'AEX BEL20.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'master_report': {
            'filename': 'Master Report.xlsx',
            'loader': lambda f: pd.read_excel(f, header=1)
        },
        'eu_taxonomy_pocket': {
            'filename': 'EuTaxonomyPocket_after_Committee.xlsx',
            'loader': lambda f, sheet=None: pd.read_excel(f, sheet_name=sheet if sheet else 0)
        },
        'gafi_black_list': {
            'filename': '20250221_GAFI_Black_List.xlsx',
            'loader': lambda f, sheet=None: pd.read_excel(f, sheet_name=sheet if sheet else 0)
        },
        'gafi_grey_list': {
            'filename': '20250221_GAFI_Grey_List.xlsx',
            'loader': lambda f, sheet=None: pd.read_excel(f, sheet_name=sheet if sheet else 0)
        },
        'non_fiscally_cooperative_with_eu': {
            'filename': '20250221_Non_Fiscally_Cooperative_with_EU.xlsx',
            'loader': lambda f, sheet=None: pd.read_excel(f, sheet_name=sheet if sheet else 0)
        },
        'cdp_climate': {
            'filename': 'CDP Climate.xlsx',
            'loader': lambda f, sheet=None: pd.read_excel(f, sheet_name=sheet if sheet else 0)
        },
        'euronext_world': {
            'filename': 'Euronext World.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'sbf_120': {
            'filename': 'SBF120.xlsx',
            'loader': lambda f: pd.read_excel(f, header=1)
        },
        'edwpt': {
            'filename': 'EDWPT.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'euspt': {
            'filename': 'EUSPT.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'deupt': {
            'filename': 'DEUPT.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'dappt': {
            'filename': 'DAPPT.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'deup': {
            'filename': 'DEUP.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'dezp': {
            'filename': 'DEZP.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'edwp': {
            'filename': 'EDWP.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'eusp': {
            'filename': 'EUSP.xlsx',
            'loader': lambda f: pd.read_excel(f)
        },
        'sustainalytics': {
            'filename': 'sustainalytics.xlsx',
            'loader': lambda f: pd.read_excel(f)
        }
    }
    
    # Rest of the function remains the same
    if required_files is None:
        required_files = [k for k in all_files.keys() if k != 'universe' or universe_name is not None]
    
    results = {}
    
    for file_key in required_files:
        if file_key in all_files:
            try:
                file_path = os.path.join(current_data_folder, all_files[file_key]['filename'])
                
                # Pass sheet name if specified
                if file_key in sheet_names:
                    sheet = sheet_names[file_key]
                    
                    # Special handling for CAC Family and AEX Family
                    if file_key in ['cac_family', 'aex_family']:
                        # Verify the sheet exists
                        try:
                            xls = pd.ExcelFile(file_path)
                            if sheet not in xls.sheet_names:
                                print(f"Sheet '{sheet}' not found in {file_path}. Available sheets: {', '.join(xls.sheet_names)}")
                                results[file_key] = None
                                continue
                        except Exception as e:
                            print(f"Error checking sheets in {file_path}: {str(e)}")
                            
                        results[file_key] = all_files[file_key]['loader'](file_path, sheet)
                    else:
                        results[file_key] = all_files[file_key]['loader'](file_path, sheet)
                else:
                    results[file_key] = all_files[file_key]['loader'](file_path)
            except Exception as e:
                print(f"Error loading {file_key}: {str(e)}")
                results[file_key] = None
    
    return results