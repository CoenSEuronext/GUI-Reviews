import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging
import traceback
from Review.functions import read_semicolon_csv
from config import DLF_FOLDER, DATA_FOLDER

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

def run_gicp_review(date, effective_date, index="GICP", isin="NLIX00005321", 
                    area="US", area2="EU", type="STOCK", universe="Developed Market", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "gicp"
        isin (str, optional): ISIN code. Defaults to "FRIX00003031"
        area (str, optional): Primary area. Defaults to "US"
        area2 (str, optional): Secondary area. Defaults to "EU"
        type (str, optional): Type of instrument. Defaults to "STOCK"
        universe (str, optional): Universe name. Defaults to "Developed Market"
        feed (str, optional): Feed source. Defaults to "Reuters"
        currency (str, optional): Currency code. Defaults to "EUR"
        year (str, optional): Year for calculation. Defaults to None (extracted from date)

    Returns:
        dict: Result dictionary containing status, message, and data
    """
    try:
        # If year is not provided, get it from the date
        if year is None:
            year = str(datetime.strptime(date, '%Y%m%d').year)

        # Set data folder for current month
        current_data_folder = os.path.join(DATA_FOLDER, date[:6])

        # Load files into DataFrames from the specified folder
        developed_market_df = pd.read_excel(os.path.join(current_data_folder, "Developed Market.xlsx"))
        ff_df = pd.read_excel(os.path.join(current_data_folder, "FF.xlsx"))
        
        icb_df = pd.read_excel(
            os.path.join(current_data_folder, "ICB.xlsx"),
            header=3
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

        developed_market_df = developed_market_df.merge(
            icb_df[['ISIN Code', 'Subsector Code']], 
            left_on='ISIN',
            right_on='ISIN Code',
            how='left'
        ).drop('ISIN Code', axis=1)

        # Convert index column to string type before string operations
        developed_market_df['index'] = developed_market_df['index'].astype(str)
        # Add EU500 exclusion
        developed_market_df['exclusion_1'] = None
        developed_market_df['exclusion_1'] = np.where(
            ~(developed_market_df['index'].str.contains('EZ300', na=False) | 
            developed_market_df['index'].str.contains('NA500', na=False)),
            'exclude_Area',
            None
        )

        # Similarly convert Subsector Code to string
        developed_market_df['Subsector Code'] = developed_market_df['Subsector Code'].astype(str)

        # Add eligibility categories exclusion
        developed_market_df['exclusion_2'] = None

        # Create the eligibility mask for each category
        category_conditions = (
            # Category 1: XPAR + Banks Super Sector (3010)
            ((developed_market_df['MIC'] == 'XPAR') & 
            (developed_market_df['Subsector Code'].str[:4] == '3010')) |
            
            # Category 2: XPAR + Clothing and Accessories Subsector (40204020)
            ((developed_market_df['MIC'] == 'XPAR') & 
            (developed_market_df['Subsector Code'] == '40204020')) |
            
            # Category 3: XETR + Automobiles and Parts Super Sector (4010)
            ((developed_market_df['MIC'] == 'XETR') & 
            (developed_market_df['Subsector Code'].str[:4] == '4010')) |
            
            # Category 4: XETR + Industrial Goods and Services Super Sector (5020)
            ((developed_market_df['MIC'] == 'XETR') & 
            (developed_market_df['Subsector Code'].str[:4] == '5020')) |
            
            # Category 5: XAMS + Technology Hardware and Equipment Super Sector (101020)
            ((developed_market_df['MIC'] == 'XAMS') & 
            (developed_market_df['Subsector Code'].str[:6] == '101020')) |
            
            # Category 6: XNYS/XNGS + Technology Industry (10)
            ((developed_market_df['MIC'].isin(['XNYS', 'XNGS'])) & 
            (developed_market_df['Subsector Code'].str[:2] == '10')) |
            
            # Category 7: XNYS/XNGS + Consumer Discretionary Industry (40)
            ((developed_market_df['MIC'].isin(['XNYS', 'XNGS'])) & 
            (developed_market_df['Subsector Code'].str[:2] == '40'))
        )

        # Apply the exclusion for companies that don't meet any category criteria
        developed_market_df['exclusion_2'] = np.where(
            ~category_conditions,
            'exclude_category',
            None
        )
        # Create a mask for non-excluded companies
        non_excluded_mask = (
            developed_market_df['exclusion_1'].isna() & 
            developed_market_df['exclusion_2'].isna()
        )

        # Add rank for non-excluded companies based on Mcap in EUR
        developed_market_df['rank'] = None
        developed_market_df.loc[non_excluded_mask, 'rank'] = (
            developed_market_df.loc[non_excluded_mask, 'Mcap in EUR']
            .rank(method='first', ascending=False)
        )

        # Initialize rank column
        developed_market_df['rank'] = None

        # Define the category conditions and rank within each category
        category_definitions = [
            # Category 1: XPAR + Banks Super Sector (3010)
            {
                'mask': (
                    (developed_market_df['MIC'] == 'XPAR') & 
                    (developed_market_df['Subsector Code'].str[:4] == '3010') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_1'
            },
            # Category 2: XPAR + Clothing and Accessories Subsector (40204020)
            {
                'mask': (
                    (developed_market_df['MIC'] == 'XPAR') & 
                    (developed_market_df['Subsector Code'] == '40204020') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_2'
            },
            # Category 3: XETR + Automobiles and Parts Super Sector (4010)
            {
                'mask': (
                    (developed_market_df['MIC'] == 'XETR') & 
                    (developed_market_df['Subsector Code'].str[:4] == '4010') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_3'
            },
            # Category 4: XETR + Industrial Goods and Services Super Sector (5020)
            {
                'mask': (
                    (developed_market_df['MIC'] == 'XETR') & 
                    (developed_market_df['Subsector Code'].str[:4] == '5020') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_4'
            },
            # Category 5: XAMS + Technology Hardware and Equipment Super Sector (101020)
            {
                'mask': (
                    (developed_market_df['MIC'] == 'XAMS') & 
                    (developed_market_df['Subsector Code'].str[:6] == '101020') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_5'
            },
            # Category 6: XNYS/XNGS + Technology Industry (10)
            {
                'mask': (
                    (developed_market_df['MIC'].isin(['XNYS', 'XNGS'])) & 
                    (developed_market_df['Subsector Code'].str[:2] == '10') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_6'
            },
            # Category 7: XNYS/XNGS + Consumer Discretionary Industry (40)
            {
                'mask': (
                    (developed_market_df['MIC'].isin(['XNYS', 'XNGS'])) & 
                    (developed_market_df['Subsector Code'].str[:2] == '40') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_7'
            }
        ]

        # Add category column to identify which category each company belongs to
        developed_market_df['category'] = None

        # Rank within each category
        for category in category_definitions:
            mask = category['mask']
            # Assign category name
            developed_market_df.loc[mask, 'category'] = category['name']
            # Rank within category based on market cap
            developed_market_df.loc[mask, 'rank'] = (
                developed_market_df.loc[mask, 'Mcap in EUR']
                .rank(method='first', ascending=False)
            )
        # Define the number of companies to select from each category
        category_selections = {
            'Category_1': 3,  # XPAR Banks
            'Category_2': 3,  # XPAR Clothing and Accessories
            'Category_3': 3,  # XETR Automobiles and Parts
            'Category_4': 3,  # XETR Industrial Goods and Services
            'Category_5': 3,  # XAMS Technology Hardware and Equipment
            'Category_6': 10, # XNYS/XNGS Technology
            'Category_7': 5   # XNYS/XNGS Consumer Discretionary
        }

        # Create selection column
        developed_market_df['selected'] = False

        # Select top companies from each category
        for category, n_select in category_selections.items():
            category_mask = (developed_market_df['category'] == category)
            top_n_mask = (developed_market_df['rank'] <= n_select) & category_mask
            developed_market_df.loc[top_n_mask, 'selected'] = True

        # Verify total number of selected companies
        total_selected = developed_market_df['selected'].sum()
        if total_selected != 30:
            logger.warning(f"Selected {total_selected} companies instead of expected 30")

        # Create final selection dataframe
        final_selection_df = developed_market_df[developed_market_df['selected']].copy()
        
        final_selection_df['Final Capping'] = 1  # Or calculate appropriate capping
        final_selection_df['Effective Date of Review'] = effective_date
        
        # Create final output DataFrame
        GICP_df = final_selection_df[[
            'Name', 
            'ISIN', 
            'MIC', 
            'NOSH', 
            'Free Float',
            'Final Capping',
            'Effective Date of Review',
            'Currency (Local)'
        ]].copy()

        # Rename columns and sort
        GICP_df = GICP_df.rename(columns={
            'Currency (Local)': 'Currency',
        })
        GICP_df = GICP_df.sort_values('Name')

        # Save output files
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            gicp_path = os.path.join(output_dir, f'GICP_df_{timestamp}.xlsx')
            
            logger.info(f"Saving GICP output to: {gicp_path}")
            with pd.ExcelWriter(gicp_path) as writer:
                GICP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                developed_market_df.to_excel(writer, sheet_name='Full Universe', index=False)
            
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "gicp_path": gicp_path
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