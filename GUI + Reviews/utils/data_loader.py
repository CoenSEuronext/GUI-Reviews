# utils/data_loader.py
import pandas as pd
from Review.functions import read_semicolon_csv
import os

def load_eod_data(date, co_date, area, area2, dlf_folder):
    """Load and combine EOD data from different areas"""
    # Load EOD data
    index_eod_us_df = read_semicolon_csv(
        os.path.join(dlf_folder, f"TTMIndex{area}1_GIS_EOD_INDEX_{date}.csv"), 
        encoding="latin1"
    )
    stock_eod_us_df = read_semicolon_csv(
        os.path.join(dlf_folder, f"TTMIndex{area}1_GIS_EOD_STOCK_{date}.csv"), 
        encoding="latin1"
    )
    index_eod_eu_df = read_semicolon_csv(
        os.path.join(dlf_folder, f"TTMIndex{area2}1_GIS_EOD_INDEX_{date}.csv"), 
        encoding="latin1"
    )
    stock_eod_eu_df = read_semicolon_csv(
        os.path.join(dlf_folder, f"TTMIndex{area2}1_GIS_EOD_STOCK_{date}.csv"), 
        encoding="latin1"
    )
    stock_co_eu_df = read_semicolon_csv(
        os.path.join(dlf_folder, f"TTMIndex{area2}1_GIS_EOD_STOCK_{co_date}.csv"), 
        encoding="latin1"
    )
    stock_co_us_df = read_semicolon_csv(
        os.path.join(dlf_folder, f"TTMIndex{area}1_GIS_EOD_STOCK_{co_date}.csv"), 
        encoding="latin1"
    )    
    stock_co_df = pd.concat([stock_co_eu_df, stock_co_us_df], ignore_index=True)
    index_eod_df = pd.concat([index_eod_us_df, index_eod_eu_df], ignore_index=True)
    stock_eod_df = pd.concat([stock_eod_us_df, stock_eod_eu_df], ignore_index=True)
    
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
        'universe': {
            'filename': f"{universe_name}.xlsx" if universe_name else "98% Universe.xlsx",
            'loader': lambda f: pd.read_excel(f)
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