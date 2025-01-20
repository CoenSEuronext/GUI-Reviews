import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging
import traceback
from Review.functions import read_semicolon_csv
from config import DLF_FOLDER, DATA_FOLDER, DATA_FOLDER2

# Set up logging
def setup_logging():
    """Configure logging for the review process"""
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Create log file name with timestamp
    log_file = os.path.join(log_dir, f'review_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # This will also print to console
        ]
    )
    
    return logging.getLogger(__name__)

logger = logging.getLogger(__name__)

logger = setup_logging()

def run_edwpt_review(date, effective_date, index="EDWPT", isin="NLIX00001932", 
                    area="US", area2="EU", type="STOCK", universe="98% Universe", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "edwpt"
        isin (str, optional): ISIN code. Defaults to "NLIX00001932"
        area (str, optional): Primary area. Defaults to "US"
        area2 (str, optional): Secondary area. Defaults to "EU"
        type (str, optional): Type of instrument. Defaults to "STOCK"
        universe (str, optional): Universe name. Defaults to "98% Universe"
        feed (str, optional): Feed source. Defaults to "Reuters"
        currency (str, optional): Currency code. Defaults to "EUR"
        year (str, optional): Year for calculation. Defaults to None (extracted from date)

    Returns:
        dict: Result dictionary containing status, message, and data
    """
    
    try:
        logger.info("Starting EDWPT review calculation")  # First log message
        # If year is not provided, get it from the date
        if year is None:
            year = str(datetime.strptime(date, '%Y%m%d').year)

        # Set data folder for current month
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        ff_df = pd.read_excel(os.path.join(current_data_folder, "FF.xlsx"))
        
        full_universe_df = pd.read_excel(
            os.path.join(current_data_folder, f"{universe}.xlsx"))
        
        # Load EOD data
        index_eod_us_df = read_semicolon_csv(
            os.path.join(DLF_FOLDER, f"TTMIndex{area}1_GIS_EOD_INDEX_{date}.csv"), 
            encoding="latin1"
        )
        stock_eod_us_df = read_semicolon_csv(
            os.path.join(DLF_FOLDER, f"TTMIndex{area}1_GIS_EOD_STOCK_{date}.csv"), 
            encoding="latin1"
        )
        index_eod_eu_df = read_semicolon_csv(
            os.path.join(DLF_FOLDER, f"TTMIndex{area2}1_GIS_EOD_INDEX_{date}.csv"), 
            encoding="latin1"
        )
        stock_eod_eu_df = read_semicolon_csv(
            os.path.join(DLF_FOLDER, f"TTMIndex{area2}1_GIS_EOD_STOCK_{date}.csv"), 
            encoding="latin1"
        )

        index_eod_df = pd.concat([index_eod_us_df, index_eod_eu_df], ignore_index=True)
        stock_eod_df = pd.concat([stock_eod_us_df, stock_eod_eu_df], ignore_index=True)
        
        full_universe_df['Mcap in EUR'] = full_universe_df['fx_rate'] * full_universe_df['cutoff_nosh'] * full_universe_df['cutoff_price'] * full_universe_df['free_float']
        # Create column mapping dictionary where the key is the new column name and value is the original column name
        # Create column mapping dictionary where the key is the new column name and value is the original name
        column_mapping = {
            'Ticker': 'fs_ticker',           
            'Name': 'proper_name',           
            'ISIN': 'ISIN',         
            'MIC': 'MIC_GIS',                   
            'NOSH': 'cutoff_nosh',          
            'Price (EUR) ': 'cutoff_price',  
            'Currency (Local)': 'p_currency',
            'FFMC': 'free_float_market_cap' 
        }     
        
        # Create universe_df with selected and renamed columns
        universe_df = full_universe_df[list(column_mapping.values())]  # Select columns using current names
        universe_df = universe_df.rename(columns={v: k for k, v in column_mapping.items()})  # Rename to desired names

        # Sort entire DataFrame by FFMC descending
        universe_df = universe_df.sort_values('FFMC', ascending=False)
        # Before merging, remove duplicates
        ff_df = ff_df.drop_duplicates(subset=['ISIN Code:'], keep='first')
        
        # Add Free Float data from FF.xlsx
        universe_df = universe_df.merge(
            ff_df[['ISIN Code:', 'Free Float Round:']],
            left_on='ISIN',
            right_on='ISIN Code:',
            how='left'
        ).drop('ISIN Code:', axis=1).rename(columns={'Free Float Round:': 'Free Float'})
        
        
        universe_df['Final Capping'] = 1
        universe_df['Effective Date of Review'] = effective_date
        
        # Add cumulative count for entire universe
        universe_df['Cumulative Count'] = range(1, len(universe_df) + 1)

        # Calculate universe-level cumulative statistics
        universe_df['Total Universe FFMC'] = universe_df['FFMC'].sum()
        universe_df['Universe Cumulative FFMC'] = universe_df['FFMC'].cumsum()
        universe_df['Universe Cumulative Percentage'] = (universe_df['Universe Cumulative FFMC'] / universe_df['Total Universe FFMC']) * 100



        # Create MIC grouping mapping
        mic_groups = {
            'US': ['XNYS', 'XNGS', 'BATS'],
            'SE': ['XSTO', 'XNGM'],
            'IT': ['XMIL', 'MTAA'],
            'IE': ['XESM', 'XMSM'],
            'AU': ['XASX'],
            'AT': ['WBAH'],
            'BE': ['XBRU'],
            'CA': ['XTSE'],
            'DK': ['XCSE'],
            'FI': ['XHEL'],
            'FR': ['XPAR', 'ALXP'],
            'DE': ['XETR'],
            'ES': ['XMAD'],
            'JP': ['XTKS'],
            'NL': ['XAMS'],
            'NZ': ['XNZE'],
            'NO': ['XOSL', 'MERK'],
            'PT': ['XLIS'],
            'SG': ['XSES'],
            'CH': ['XSWX'],
            'UK': ['XLON'],
            'HK': ['XHKG'],
            'IL': ['XTAE'],             
        }

        # Create a 'Country Group' column for grouping MICs
        mic_to_country = {mic: country for country, mics in mic_groups.items() for mic in mics}

        # Update get_country_group function
        def get_country_group(mic):
            return mic_to_country.get(mic, mic)  # Get country code from mapping, or return mic if not found

        # Add country group column
        universe_df['Country Group'] = universe_df['MIC'].apply(get_country_group)

        # Recalculate MIC-level statistics using Country Group instead of MIC
        universe_df['MIC Rank'] = universe_df.groupby('Country Group')['FFMC'].rank(method='first', ascending=False)
        universe_df['MIC Cumulative FFMC'] = universe_df.groupby('Country Group')['FFMC'].cumsum()
        universe_df['Total MIC FFMC'] = universe_df.groupby('Country Group')['FFMC'].transform('sum')
        universe_df['MIC Cumulative Percentage'] = (universe_df['MIC Cumulative FFMC'] / universe_df['Total MIC FFMC']) * 100
        
        # Define all country groups
        country_groups = {
            'EDWPT': ['US', 'SE', 'IT', 'IE', 'AU', 'AT', 'BE', 'CA', 'DK', 
                    'FI', 'FR', 'DE', 'ES', 'JP', 'NL', 'NZ', 'NO', 'PT', 
                    'SG', 'CH', 'UK', 'HK', 'IL'],
            'DEUPT': ['NL', 'AT', 'BE', 'DK', 'DE', 'FI', 'PT', 'UK', 'ES', 'IT', 'IE', 'NO', 'FR', 'SE', 'CH'],
            'DEZPT': ['AT', 'NL', 'BE', 'DE', 'FI', 'PT', 'ES', 'IT', 'IE', 'FR'],
            'DAPPT': ['HK', 'JP', 'SG', 'AU', 'NZ'],
            'DNAPT': ['US', 'CA'],
            'DASPT': ['HK', 'JP', 'SG'],
            'DPAPT': ['NZ', 'AU'],
            'EUSPT': ['US'],
            'EJPPT': ['JP'],
            'ECHPT': ['CH'],
            'EUKPT': ['UK'],
            'CANPT': ['CA']
        }

        # Function to calculate selection for a group
        def calculate_group_selection(df, group_name, countries):
            # Create mask for countries in this group
            group_mask = df['Country Group'].isin(countries)
            
            # Initialize selection column
            df[f'{group_name}_selection'] = 0
            
            # Calculate total FFMC for the group
            group_total_ffmc = df.loc[group_mask, 'FFMC'].sum()
            
            if group_total_ffmc > 0:
                # Sort group data by FFMC descending
                group_df = df[group_mask].sort_values('FFMC', ascending=False).copy()
                
                # Calculate group level cumulative stats
                group_df[f'{group_name}_Cumulative_FFMC'] = group_df['FFMC'].cumsum()
                group_df[f'{group_name}_Cumulative_Percentage'] = (
                    group_df[f'{group_name}_Cumulative_FFMC'] / group_total_ffmc * 100
                )
                
                # Add the group percentage column to main DataFrame
                df.loc[group_mask, f'{group_name}_Group_Percentage'] = group_df[f'{group_name}_Cumulative_Percentage']
                
                # Select companies up to 98% for the entire group plus the first one exceeding
                exceeds_98_group = group_df[f'{group_name}_Cumulative_Percentage'] > 98
                first_exceed_group = exceeds_98_group & ~exceeds_98_group.shift(1, fill_value=False)
                group_selection = (group_df[f'{group_name}_Cumulative_Percentage'] <= 98) | first_exceed_group
                
                # For each country in the group, calculate its own 98% threshold
                for country in countries:
                    country_mask = group_df['Country Group'] == country
                    if country_mask.any():
                        country_df = group_df[country_mask].copy()
                        country_total = country_df['FFMC'].sum()
                        
                        if country_total > 0:
                            country_df['Country_Cumulative_FFMC'] = country_df['FFMC'].cumsum()
                            country_df['Country_Cumulative_Percentage'] = (
                                country_df['Country_Cumulative_FFMC'] / country_total * 100
                            )
                            
                            # Add the country percentage column to main DataFrame
                            df.loc[country_df.index, f'{group_name}_Country_Percentage'] = country_df['Country_Cumulative_Percentage']
                            
                            # Select companies up to 98% for this country plus the first one exceeding
                            exceeds_98_country = country_df['Country_Cumulative_Percentage'] > 98
                            first_exceed_country = exceeds_98_country & ~exceeds_98_country.shift(1, fill_value=False)
                            country_selection = (country_df['Country_Cumulative_Percentage'] <= 98) | first_exceed_country
                            
                            # Update group_selection to include country selections
                            group_selection.loc[country_df.index] |= country_selection
                
                # Update the main DataFrame with selections
                df.loc[group_df.index, f'{group_name}_selection'] = group_selection.astype(int)
            
            return df

        # Apply selections for all groups
        for group_name, countries in country_groups.items():
            universe_df = calculate_group_selection(universe_df, group_name, countries)
            
            # Print summary statistics for each group
            group_mask = universe_df['Country Group'].isin(countries)
            print(f"\n{group_name} Selection Summary:")
            print(f"Total companies selected: {universe_df[universe_df[f'{group_name}_selection'] == 1].shape[0]}")
            print(f"Total FFMC covered: {universe_df[universe_df[f'{group_name}_selection'] == 1]['FFMC'].sum() / universe_df.loc[group_mask, 'FFMC'].sum() * 100:.2f}%")

        # Keep DataFrame sorted by FFMC descending
        universe_df = universe_df.sort_values('FFMC', ascending=False)

        # Print summary statistics
        print("\nGlobal Selection Summary:")
        print(f"Total companies selected: {universe_df['EDWPT_selection'].sum()}")
        print(f"Total FFMC covered: {universe_df[universe_df['EDWPT_selection'] == 1]['FFMC'].sum() / universe_df['FFMC'].sum() * 100:.2f}%")

        mic_summary = universe_df.groupby('MIC').agg({
            'ISIN': 'count',
            'FFMC': 'sum',
            'EDWPT_selection': 'sum'
        }).round(2)

        print("\nMIC-level Summary:")
        print(mic_summary)
        
        # Create DataFrames for all country groups
        all_dfs = {}  # Dictionary to store all DataFrames
        selected_columns = ['Name', 'ISIN', 'MIC', 'NOSH', 'Free Float', 'Final Capping', 'Effective Date of Review', 'Currency (Local)']

        for group_name in country_groups.keys():
            # Create DataFrame for each group
            all_dfs[group_name] = universe_df[universe_df[f'{group_name}_selection'] == 1][selected_columns].copy()

        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            edwpt_path = os.path.join(output_dir, f'EDWPT_df_{timestamp}.xlsx')
            
            logger.info(f"Saving output to: {edwpt_path}")
            with pd.ExcelWriter(edwpt_path) as writer:
                # Write each group's DataFrame to a separate sheet
                for group_name, df in all_dfs.items():
                    df.to_excel(writer, sheet_name=f'{group_name} Composition', index=False)
                universe_df.to_excel(writer, sheet_name='Full Universe', index=False)
            
            # Add this print to confirm file exists
            if os.path.exists(edwpt_path):
                print(f"File successfully saved to: {edwpt_path}")
            else:
                print("File was not saved successfully")

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "edwpt_path": edwpt_path
                }
            }
            
        except Exception as e:
            error_msg = f"Error saving output file: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg,
                "data": None
            }
    except Exception as e:
        logger.error(f"Error during review calculation: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": f"Error during review calculation: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None
        }