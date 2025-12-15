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

def run_ses5p_review(date, co_date, effective_date, index="SES5P", isin="NL0015000EF0", 
                    area="US", area2="EU", type="STOCK", universe="Eurozone 300", 
                    feed="Reuters", currency="EUR", year=None):
    try:
        # Simplify year extraction
        year = year or str(datetime.strptime(date, '%Y%m%d').year)
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        # Load data with error handling
        logger.info("Loading EOD data...")
        index_eod_df, stock_eod_df, stock_co_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)
        
        logger.info("Loading reference data...")
        ref_data = load_reference_data(
            current_data_folder,
            ['ff', 'icb', 'eurozone_300']
        )
        
        # Validate data loading
        if any(df is None for df in [ref_data['ff'], ref_data['eurozone_300'], ref_data['icb']]):
            raise ValueError("Failed to load one or more required reference data files")

        # Filter symbols once and prepare ICB codes
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        icb_codes_stock = stock_eod_df[['Isin Code', 'ICBCode', 'MIC']].drop_duplicates(subset=['Isin Code', 'MIC'], keep='first')
        icb_codes_icb = (ref_data['icb'][['ISIN Code', 'Subsector Code', 'MIC Code']]
            .drop_duplicates(subset=['ISIN Code', 'MIC Code'], keep='first'))

        # Chain all data preparation operations
        selection_df = (ref_data['eurozone_300']
            # Merge Free Float data
            .merge(
                ref_data['ff'][['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset=['ISIN Code:'], keep='first'),
                left_on='ISIN',
                right_on='ISIN Code:',
                how='left'
            )
            .drop('ISIN Code:', axis=1)
            .rename(columns={'Free Float Round:': 'Free Float', 'Name': 'Company'})
            
            # Merge symbols and prices
            .merge(symbols_filtered, left_on='ISIN', right_on='Isin Code', how='left')
            .drop('Isin Code', axis=1)
            .merge(
                stock_eod_df[['#Symbol', 'FX/Index Ccy']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left'
            )
            .merge(
                stock_eod_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left',
                suffixes=('', '_EOD')
            )
            .merge(
                stock_co_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left',
                suffixes=('_EOD', '_CO')
            )
            
            # Merge ICB codes
            .merge(
                icb_codes_stock,
                left_on=['ISIN', 'MIC'],
                right_on=['Isin Code', 'MIC'],
                how='left'
            )
            .drop('Isin Code', axis=1)
            .merge(
                icb_codes_icb,
                left_on=['ISIN', 'MIC'],
                right_on=['ISIN Code', 'MIC Code'],
                how='left'
            )
            .drop('ISIN Code', axis=1)
        )

        # Calculate derived columns
        selection_df['Uni_Supersector'] = selection_df['ICBCode'].astype(str).str[:4]
        selection_df['ICB_Supersector'] = selection_df['Subsector Code'].astype(str).str[:4]
        selection_df['FFMC CO'] = selection_df['NOSH'] * selection_df['Free Float'] * selection_df['Close Prc_CO']
        selection_df['Effective Date of Review'] = effective_date
        
        # Apply inclusion criteria
        eligible_supersectors = ['3030', '1510', '6510', '6010']
        selection_df['Inclusion_Sector'] = (
            (selection_df['Uni_Supersector'].isin(eligible_supersectors)) | 
            (selection_df['ICB_Supersector'].isin(eligible_supersectors))
        )

        # Rank and select final companies
        selection_df['Rank Universe'] = selection_df.loc[selection_df['Inclusion_Sector'], 'FFMC CO'].rank(ascending=False, method='first')
        selection_df['Final Selection'] = (selection_df['Inclusion_Sector'] & (selection_df['Rank Universe'] <= 50))
        selection_df['Final Capping'] = 1
        full_universe_df = selection_df
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        selection_df = selection_df[selection_df['Final Selection']].copy()

        SES5P_df = (selection_df
            [['Company', 'ISIN', 'MIC', 'NOSH', 'Free Float', 'Final Capping', 
            'Effective Date of Review', 'Currency']]
            .rename(columns={
                'ISIN': 'ISIN Code',
                'Currency': 'Currency (Local)'
            })
            .sort_values('Company')
        )

        # Call inclusion_exclusion with correct parameters
        analysis_results = inclusion_exclusion_analysis(
            selection_df, 
            stock_eod_df, 
            index, 
            isin_column='ISIN'  # Explicitly specify the ISIN column name
        )

        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']
        # Save output files
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            # Create filename with timestamp to avoid conflicts
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            ses5p_path = os.path.join(output_dir, f'SES5P_df_{timestamp}.xlsx')
            
            # Save with timestamp to avoid any conflicts
            logger.info(f"Saving SES5P output to: {ses5p_path}")
            with pd.ExcelWriter(ses5p_path) as writer:
                    # Write each DataFrame to a different sheet
                    SES5P_df.to_excel(writer, sheet_name='Index Composition', index=False)
                    inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                    exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                    full_universe_df.to_excel(writer, sheet_name='Full Universe', index=False)
                    index_mcap_df = pd.DataFrame({'Index Market Cap': [index_mcap]})
                    index_mcap_df.to_excel(writer, sheet_name='Index Market Cap', index=False)

                
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "ses5p_path": ses5p_path
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