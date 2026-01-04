import pandas as pd
import numpy as np
from datetime import datetime
import os
import traceback
from config import DLF_FOLDER, DATA_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data
from utils.inclusion_exclusion import inclusion_exclusion_analysis
from utils.capping_standard import calculate_capped_weights

logger = setup_logging(__name__)

def run_eurep_review(date, co_date, effective_date, index="EUREP", isin="NLIX00004803", 
                   area="US", area2="EU", type="STOCK", universe="euspt", 
                   feed="Reuters", currency="EUR", year=None):

    try:
        # Simplify year extraction
        year = year or str(datetime.strptime(date, '%Y%m%d').year)

        # Set data folder for current month
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        # Load data with error handling
        logger.info("Loading EOD data...")
        index_eod_df, stock_eod_df, stock_co_df, fx_lookup_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)

        logger.info("Loading reference data...")
        
        ref_data = load_reference_data(
            current_data_folder,
            [universe, 'icb', 'ff']
        )
        selection_df = ref_data[universe]
        ff_df = ref_data['ff']
        icb_df = ref_data['icb']
        
        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        # Chain all data preparation operations
        selection_df = (ref_data[universe]
           # Initial renaming
           .rename(columns={
               'NOSH': 'Number of Shares',
               'ISIN': 'ISIN code',
               'Name': 'Company',
               'Currency (Local)': 'Currency',
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
            # Merge Free Float data
            .merge(
                ff_df[['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset='ISIN Code:', keep='first'),
                left_on='ISIN code',
                right_on='ISIN Code:',
                how='left'
            )
            .drop('ISIN Code:', axis=1)
            .rename(columns={'Free Float Round:': 'Free Float'})
            # Merge ICB subsector code
            .merge(
                icb_df[['ISIN Code', 'Subsector Code']].drop_duplicates(subset='ISIN Code', keep='first'),
                left_on='ISIN code',
                right_on='ISIN Code',
                how='left'
            )
            .drop('ISIN Code', axis=1)
        )

        # Validate data loading
        if any(df is None for df in [selection_df]):
            raise ValueError("Failed to load one or more required reference data files")
        
        # Find index market cap
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        
        # Calculate FFMC (Free Float Market Cap)
        selection_df['FFMC'] = selection_df['Number of Shares'] * selection_df['Free Float'] * selection_df['Price (EUR) ']
        
        # Add ICB exclusion logic (exclude subsector 3510)
        selection_df['exclusion_icb'] = ~(selection_df['Subsector Code'].astype(str).str[:4] == '3510')

        # Add Effective Date and Capping Factor
        selection_df['Effective Date of Review'] = effective_date

        # Create eligible_df based on companies NOT excluded by ICB OR MIC
        logger.info("Creating eligible companies dataframe...")
        eligible_df = selection_df[
            (selection_df['exclusion_icb'] == False)
        ].copy()
        eligible_df['FFMC_WD'] = eligible_df['Close Prc_EOD'] * eligible_df['Number of Shares'] * eligible_df['Free Float'] * eligible_df['FX/Index Ccy']
        
        # Calculate total FFMC for eligible companies
        total_eligible_ffmc = eligible_df['FFMC_WD'].sum()
        
        # Calculate initial weights and apply 10% capping
        initial_weights = eligible_df['FFMC_WD'] / total_eligible_ffmc
        capped_weights = calculate_capped_weights(initial_weights, cap_limit=0.1)
        
        # Calculate raw capping factors
        raw_capping_factors = capped_weights / initial_weights

        # Normalize capping factors so the maximum becomes 1
        max_capping_factor = raw_capping_factors.max()
        normalized_capping_factors = raw_capping_factors / max_capping_factor

        # Update capping factors and weights
        eligible_df['Capping Factor'] = normalized_capping_factors.round(14)
        eligible_df['Weight'] = capped_weights

        # Additional logging for transparency
        capped_companies = eligible_df[normalized_capping_factors < 1.0]
        logger.info(f"Companies with capping applied: {len(capped_companies)}")
        if len(capped_companies) > 0:
            logger.info(f"Average capping factor for capped companies: {capped_companies['Capping Factor'].mean():.4f}")

        # Prepare EUREP DataFrame
        EUREP_df = (
            eligible_df[
                ['Company', 'ISIN code', 'MIC', 'Number of Shares', 'Free Float', 'Capping Factor', 
                'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'ISIN code': 'ISIN Code'})
            .sort_values('Company')
        )

        # Perform Inclusion/Exclusion Analysis
        analysis_results = inclusion_exclusion_analysis(
            eligible_df, 
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
            eurep_path = os.path.join(output_dir, f'EUREP_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving EUREP output to: {eurep_path}")
            with pd.ExcelWriter(eurep_path) as writer:
                EUREP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                eligible_df.to_excel(writer, sheet_name='Eligible Companies', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "eurep_path": eurep_path
                }
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