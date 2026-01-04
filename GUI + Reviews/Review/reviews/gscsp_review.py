import pandas as pd
import numpy as np
from datetime import datetime
import os
import traceback
from config import DLF_FOLDER, DATA_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data
from utils.inclusion_exclusion import inclusion_exclusion_analysis

logger = setup_logging(__name__)

def run_gscsp_review(date, co_date, effective_date, index="GSCSP", isin="FR0014005GI3", 
                   area="US", area2="EU", type="STOCK", universe="index_of_index", 
                   feed="Reuters", currency="EUR", year=None):

    try:
        # Simplify year extraction
        year = year or str(datetime.strptime(date, '%Y%m%d').year)


        # Load data with error handling
        logger.info("Loading EOD data...")
        index_eod_df, stock_eod_df, stock_co_df, fx_lookup_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)

        logger.info("Loading reference data...")

        # Create selection_df from stock_eod_df for GSCSP constituents
        logger.info("Creating selection_df from GSCSP constituents...")
        selection_df = stock_eod_df[stock_eod_df['Index'] == 'GSCSP'].copy()

        if selection_df.empty:
            raise ValueError("No constituents found with Index = 'GSCSP' in stock_eod_df")

        # Rename columns to match expected format
        column_mapping = {
            'Isin Code': 'ISIN code',
            'Name': 'Company',
            'MIC': 'MIC',
            'Currency': 'Currency',
            'Shares': 'NOSH',
            'Close Prc': 'Close Prc_EOD',
            'FX/Index Ccy': 'FX/Index Ccy'
        }

        # Rename columns that exist in the dataframe
        for old_col, new_col in column_mapping.items():
            if old_col in selection_df.columns:
                selection_df.rename(columns={old_col: new_col}, inplace=True)

        # Ensure required columns exist
        required_columns = ['ISIN code', 'Company', 'MIC', 'Currency', 'NOSH', 'Close Prc_EOD']
        missing_columns = [col for col in required_columns if col not in selection_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in stock_eod_df: {missing_columns}")

        # Add FX/Index Ccy if not present (default to 1 if currency matches index currency)
        if 'FX/Index Ccy' not in selection_df.columns:
            selection_df['FX/Index Ccy'] = 1.0

        logger.info(f"Found {len(selection_df)} GSCSP constituents")


        # Validate data loading
        if selection_df is None:
            raise ValueError("Failed to load required reference data files")

        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]

        # Ensure 'Mnemo' column exists
        if 'Mnemo' not in selection_df.columns:
            raise ValueError("'Mnemo' column not found in stock_eod_df")

        # Define weight allocation
        weight_allocation = {
            'JPCEG': 0.05,
            'EZCLG': 0.40,
            'UC3EG': 0.55
        }

        # Calculate target market cap for each constituent based on their Mnemo
        selection_df['Target Weight'] = selection_df['Mnemo'].map(weight_allocation)

        # Validate that all constituents have a valid Mnemo
        if selection_df['Target Weight'].isna().any():
            invalid_mnemos = selection_df[selection_df['Target Weight'].isna()]['Mnemo'].unique()
            raise ValueError(f"Invalid Mnemo values found: {invalid_mnemos}. Expected 'JPCEG', 'EZCLG', or 'UC3EG'")

        # Calculate target market cap per company
        selection_df['Target Market Cap'] = index_mcap * selection_df['Target Weight']

        # Calculate the number of shares needed to achieve target market cap
        selection_df['Unrounded NOSH'] = selection_df['Target Market Cap'] / (selection_df['Close Prc_EOD'] * selection_df['FX/Index Ccy'])
        selection_df['Rounded NOSH'] = selection_df['Unrounded NOSH'].round()
        selection_df['Free Float'] = 1
        selection_df['Effective Date of Review'] = effective_date
        selection_df['Capping Factor'] = 1.0

        # Prepare GSCSP DataFrame
        GSCSP_df = (
            selection_df[
                ['Company', 'ISIN code', 'MIC', 'Rounded NOSH', 'Free Float', 'Capping Factor', 
                'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'Rounded NOSH': 'Number of Shares'})
            .rename(columns={'ISIN code': 'ISIN Code'})
            .sort_values('Company')  # Sort by Company name
        )

        # Perform Inclusion/Exclusion Analysis
        analysis_results = inclusion_exclusion_analysis(
            selection_df, 
            stock_eod_df, 
            index, 
            isin_column='ISIN code'
        )
        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']

        # Save output files
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
           
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            gscsp_path = os.path.join(output_dir, f'GSCSP_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving GSCSP output to: {gscsp_path}")
            with pd.ExcelWriter(gscsp_path) as writer:
                GSCSP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"gscsp_path": gscsp_path}
            }
           
        except Exception as e:
            error_msg = f"Error saving output file: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg, "data": None}
   
    except Exception as e:
        logger.error(f"Error during review calculation: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": f"Error during review calculation: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None
        }