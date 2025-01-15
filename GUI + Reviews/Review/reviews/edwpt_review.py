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
        
        full_universe_df = pd.read_csv(
            os.path.join(current_data_folder, f"{universe}.csv"),
            encoding='latin1',  # Try different encoding
            sep=';',           # Explicitly specify separator
            engine='python'    # Use python engine which is more forgiving
        )
        
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
            'MIC': 'MIC',                   
            'NOSH': 'cutoff_nosh',          
            'Price (EUR) ': 'cutoff_price',  
            'Currency (Local)': 'p_currency',
            'Mcap in EUR': 'Mcap in EUR',   
            'FFMC': 'free_float_market_cap' 
        }

        # Create universe_df with selected and renamed columns
        universe_df = full_universe_df[list(column_mapping.values())]  # Select columns using current names
        universe_df = universe_df.rename(columns={v: k for k, v in column_mapping.items()})  # Rename to desired names

        # Sort entire DataFrame by FFMC descending
        universe_df = universe_df.sort_values('FFMC', ascending=False)

        # Add cumulative count for entire universe
        universe_df['Cumulative Count'] = range(1, len(universe_df) + 1)

        # Calculate universe-level cumulative statistics
        universe_df['Total Universe FFMC'] = universe_df['FFMC'].sum()
        universe_df['Universe Cumulative FFMC'] = universe_df['FFMC'].cumsum()
        universe_df['Universe Cumulative Percentage'] = (universe_df['Universe Cumulative FFMC'] / universe_df['Total Universe FFMC']) * 100

        # Add rank column for each MIC group while maintaining global FFMC sort
        universe_df['MIC Rank'] = universe_df.groupby('MIC')['FFMC'].rank(method='first', ascending=False)

        # Calculate MIC-level statistics
        universe_df['MIC Cumulative FFMC'] = universe_df.groupby('MIC')['FFMC'].cumsum()
        universe_df['Total MIC FFMC'] = universe_df.groupby('MIC')['FFMC'].transform('sum')
        universe_df['MIC Cumulative Percentage'] = (universe_df['MIC Cumulative FFMC'] / universe_df['Total MIC FFMC']) * 100
        
        # Create EDWPT_selection column (1 if within 98% of either universe or MIC, 0 if not)
        universe_df['EDWPT_selection'] = np.where(
            (universe_df['Universe Cumulative Percentage'] <= 98) |  # Universe top 98%
            (universe_df['MIC Cumulative Percentage'] <= 98),        # MIC top 98%
            1, 0)

        # Create list of DNAPT MICs
        dnapt_mics = ['XTSE', 'XNAS', 'XNYS', 'BATS']

        # Initialize DNAPT_selection column with 0s
        universe_df['DNAPT_selection'] = 0

        # Update selection for DNAPT MICs using existing calculations
        dnapt_mask = universe_df['MIC'].isin(dnapt_mics)
        universe_df.loc[dnapt_mask, 'DNAPT_selection'] = np.where(
            (universe_df.loc[dnapt_mask, 'Universe Cumulative Percentage'] <= 98) |
            (universe_df.loc[dnapt_mask, 'MIC Cumulative Percentage'] <= 98),
            1, 0
        )
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
        EDWPT_df = None

        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            edwpt_path = os.path.join(output_dir, f'EDWPT_df_{timestamp}.xlsx')
            
            logger.info(f"Saving EDWPT output to: {edwpt_path}")
            with pd.ExcelWriter(edwpt_path) as writer:
                # EDWPT_df.to_excel(writer, sheet_name='Index Composition', index=False)
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