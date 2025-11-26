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

def run_eluxp_review(date, co_date, effective_date, index="ELUXP", isin="NLIX00005982", 
                   area="US", area2="EU", type="STOCK", universe="fixed_basket", 
                   feed="Reuters", currency="EUR", year=None):

    try:
        # Simplify year extraction
        year = year or str(datetime.strptime(date, '%Y%m%d').year)

        # Set data folder for current month
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        # Load data with error handling
        logger.info("Loading EOD data...")
        index_eod_df, stock_eod_df, stock_co_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)

        logger.info("Loading reference data...")
        
        ref_data = load_reference_data(
            current_data_folder,
            ['ff']
        )

        # Luxury Goods universe
        luxury_goods_universe = [
            {'Company': 'FERRARI', 'ISIN': 'NL0011585146', 'MIC': 'MTAA', 'Currency': 'EUR'},
            {'Company': 'STELLANTIS NV', 'ISIN': 'NL00150001Q9', 'MIC': 'MTAA', 'Currency': 'EUR'},
            {'Company': 'PANDORA A/S', 'ISIN': 'DK0060252690', 'MIC': 'XCSE', 'Currency': 'DKK'},
            {'Company': 'BMW', 'ISIN': 'DE0005190003', 'MIC': 'XETR', 'Currency': 'EUR'},
            {'Company': 'PORSCHE AG', 'ISIN': 'DE000PAG9113', 'MIC': 'XETR', 'Currency': 'EUR'},
            {'Company': 'CIE FIN RICHEMONT', 'ISIN': 'CH0210483332', 'MIC': 'XSWX', 'Currency': 'CHF'},
            {'Company': 'BRUNELLO CUCINELLI', 'ISIN': 'IT0004764699', 'MIC': 'MTAA', 'Currency': 'EUR'},
            {'Company': 'MONCLER', 'ISIN': 'IT0004965148', 'MIC': 'MTAA', 'Currency': 'EUR'},
            {'Company': 'BURBERRY GROUP', 'ISIN': 'GB0031743007', 'MIC': 'XLON', 'Currency': 'GBP'},
            {'Company': "L'OREAL", 'ISIN': 'FR0000120321', 'MIC': 'XPAR', 'Currency': 'EUR'},
            {'Company': 'LVMH', 'ISIN': 'FR0000121014', 'MIC': 'XPAR', 'Currency': 'EUR'},
            {'Company': 'KERING', 'ISIN': 'FR0000121485', 'MIC': 'XPAR', 'Currency': 'EUR'},
            {'Company': 'ESSILORLUXOTTICA', 'ISIN': 'FR0000121667', 'MIC': 'XPAR', 'Currency': 'EUR'},
            {'Company': 'HERMES INTL', 'ISIN': 'FR0000052292', 'MIC': 'XPAR', 'Currency': 'EUR'},
            {'Company': 'HUGO BOSS AG', 'ISIN': 'DE000A1PHFF7', 'MIC': 'XETR', 'Currency': 'EUR'}
        ]

        # Convert to DataFrame when needed
        luxury_goods_df = pd.DataFrame(luxury_goods_universe)
        ff_df = ref_data['ff']

        
        # Add the required columns to the combined dataframe
        luxury_goods_df['Capping Factor'] = 1
        luxury_goods_df['Effective Date of Review'] = effective_date

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')
        
        # Chain all data preparation operations using the combined dataframe
        selection_df = (luxury_goods_df
           # Initial renaming
           .rename(columns={
               'NOSH': 'Number of Shares',
               'ISIN': 'ISIN code',
               'Name': 'Company',
            })
            # Merge symbols
            .merge(
                symbols_filtered,
                left_on='ISIN code',
                right_on='Isin Code',
                how='left'
            )
            .drop('Isin Code', axis=1)
            # Merge FX data
            .merge(
            stock_eod_df[stock_eod_df['Index Curr'] == currency][['#Symbol', 'FX/Index Ccy']].drop_duplicates(subset='#Symbol', keep='first'),
            on='#Symbol',
            how='left'
            )
            # Merge EOD prices
            .merge(
                stock_eod_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left',
                suffixes=('', '_EOD')
            )
            # Merge CO prices
            .merge(
                stock_co_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left',
                suffixes=('_EOD', '_CO')
            )
            # Merge FF data for Free Float Round
            .merge(
                ff_df[['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset='ISIN Code:', keep='first'),
                left_on='ISIN code',
                right_on='ISIN Code:',
                how='left'
            )
            .drop('ISIN Code:', axis=1)         
        )

        # Validate data loading
        if selection_df is None:
            raise ValueError("Failed to load required reference data files")
    
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        
        # Use the merged Free Float Round value, fallback to original Free Float if not available
        selection_df["Free Float"] = selection_df["Free Float Round:"]

        # Select top companies from each universe based on FFMC
        top_n = 15

        # Calculate the target market cap per company (equal weighting across all 50 companies)
        target_mcap_per_company = index_mcap / top_n
        selection_df['Unrounded NOSH'] = target_mcap_per_company / (selection_df['Close Prc_EOD'] * selection_df['FX/Index Ccy'])
        selection_df['Rounded NOSH'] = selection_df['Unrounded NOSH'].round()
        selection_df['Free Float'] = 1
        selection_df['Effective Date of Review'] = effective_date
        selection_df['Unrounded NOSH'] = target_mcap_per_company / selection_df['Close Prc_EOD']
        
        # Prepare ELUXP DataFrame
        ELUXP_df = (
            selection_df[
                ['Company', 'ISIN code', 'MIC', 'Rounded NOSH', 'Free Float', 'Capping Factor', 
                'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'Rounded NOSH': 'Number of Shares'})
            .rename(columns={'ISIN code': 'ISIN Code'})
            .sort_values('Company')  # Sort by Universe first, then by Company name
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
            eluxp_path = os.path.join(output_dir, f'ELUXP_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving ELUXP output to: {eluxp_path}")
            with pd.ExcelWriter(eluxp_path) as writer:
                ELUXP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"eluxp_path": eluxp_path}
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