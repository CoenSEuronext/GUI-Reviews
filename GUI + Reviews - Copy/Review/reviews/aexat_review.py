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

def run_aexat_review(date, co_date, effective_date, index="AEXAT", isin="QS0011159744", 
                   area="US", area2="EU", type="STOCK", universe="aex_family", 
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

        # Load each sheet separately
        aex_data = load_reference_data(
            current_data_folder,
            ['aex_family'],
            sheet_names={'aex_family': 'AEX'}
        )

        amx_data = load_reference_data(
            current_data_folder,
            ['aex_family'],
            sheet_names={'aex_family': 'AMX'}
        )

        ascx_data = load_reference_data(
            current_data_folder,
            ['aex_family'],
            sheet_names={'aex_family': 'AScX'}
        )

        # Concatenate all three sheets into a single DataFrame
        selection_df = pd.concat([
            aex_data['aex_family'],
            amx_data['aex_family'],
            ascx_data['aex_family']
        ], ignore_index=True)

        logger.info(f"Loaded {len(selection_df)} companies from AEX, AMX, and AScX")

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() == 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        selection_df = (selection_df
            # Initial renaming
            .rename(columns={
                'Preliminary Number of shares': 'Number of Shares',
                'Preliminary Free Float': 'Free Float',
                'Preliminary Capping Factor': 'Capping Factor',
                'Effective date of review': 'Effective Date of Review'
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
            # Merge current AEXAT Capping Factor
            .merge(
                stock_eod_df[stock_eod_df['Index'] == 'AEXAT'][['Isin Code', 'Capping Factor-Coeff']].drop_duplicates(subset='Isin Code', keep='first'),
                left_on='ISIN code',
                right_on='Isin Code',
                how='left'
            )
            .drop('Isin Code', axis=1)
        )

        # Fill missing capping factors with 0
        selection_df['Capping Factor-Coeff'] = selection_df['Capping Factor-Coeff'].fillna(0)
                
        # Validate data loading
        if any(df is None for df in [selection_df]):
            raise ValueError("Failed to load one or more required reference data files")

        # Find index market cap
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]

        # Calculate Free Float Market Cap for weighting
        selection_df['FF Market Cap'] = (
            selection_df['Number of Shares'] * 
            selection_df['Free Float'] * 
            selection_df['Close Prc_EOD']
        )

        # Calculate uncapped weights
        total_ffmc = selection_df['FF Market Cap'].sum()
        selection_df['Uncapped Weight'] = selection_df['FF Market Cap'] / total_ffmc

        # Apply 15% capping using the standard function
        capped_weights = calculate_capped_weights(
            weights=selection_df['Uncapped Weight'],
            cap_limit=0.15,  # 15% cap
            max_iterations=100,
            tolerance=1e-8
        )

        # Calculate Capping Factor as ratio of capped to uncapped weight
        selection_df['Capping Factor'] = capped_weights / selection_df['Uncapped Weight']
        selection_df['Capping Factor'] = selection_df['Capping Factor'].fillna(1)  # Handle division by zero
        max_capping_factor = selection_df['Capping Factor'].max()
        selection_df['Capping Factor'] = selection_df['Capping Factor'] / max_capping_factor
        logger.info(f"Applied 15% capping. Companies capped: {(selection_df['Capping Factor'] < 1).sum()}")

        # Add Effective Date
        selection_df['Effective Date of Review'] = effective_date
        selection_df['Currency'] = currency

        # Perform Inclusion/Exclusion Analysis
        analysis_results = inclusion_exclusion_analysis(
            selection_df, 
            stock_eod_df, 
            index, 
            isin_column='ISIN code'
        )
        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']
        AEXAT_df = (
            selection_df[
                ['Company', 'ISIN code', 'MIC', 'Number of Shares', 'Free Float', 'Capping Factor', 
                'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'ISIN code': 'ISIN Code'})
            .sort_values('Company')
        )

        # Save output files
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
           
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            aexat_path = os.path.join(output_dir, f'AEXAT_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving AEXAT output to: {aexat_path}")
            with pd.ExcelWriter(aexat_path) as writer:
                AEXAT_df.to_excel(writer, sheet_name=index + ' Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"aexat_path": aexat_path}
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