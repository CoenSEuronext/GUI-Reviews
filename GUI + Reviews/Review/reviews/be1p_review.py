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

def run_be1p_review(date, co_date, effective_date, index="BE1P", isin="NL0012949143", 
                   area="US", area2="EU", type="STOCK", universe="eurozone_300", 
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
            [universe, 'ff', 'icb']
        )
        selection_df = ref_data[universe]
        ff_df = ref_data['ff']
        icb_df = ref_data['icb']

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')
        selection_df['Capping Factor'] = 1
        selection_df['Effective Date of Review'] = effective_date
        
        # Chain all data preparation operations
        selection_df = (ref_data[universe]
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
                stock_eod_df[['#Symbol', 'FX/Index Ccy']].drop_duplicates(subset='#Symbol', keep='first'),
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
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        
        # Use the merged Free Float Round value, fallback to original Free Float if not available
        selection_df["Free Float"] = selection_df["Free Float Round:"]
        selection_df['FFMC'] = selection_df['Capping Factor'] * selection_df['Number of Shares'] * selection_df['Free Float'] * selection_df['Price']
        
        # Add exclusion filters as separate columns
        allowed_mics = ['XPAR', 'XMAD', 'XETR', 'MTAA']
        
        # MIC exclusion column
        selection_df['MIC_Excluded'] = ~selection_df['MIC'].isin(allowed_mics)
        
        # ICB subsector exclusion column
        selection_df['Subsector_Excluded'] = (
            selection_df['Subsector Code'].isna() | 
            (selection_df['Subsector Code'].astype(str).str[:4] != '3010')
        )
        
        # Turnover exclusion column
        selection_df['Turnover_Excluded'] = (
            selection_df['3 months aver. Turnover EUR'].isna() | 
            (selection_df['3 months aver. Turnover EUR'] < 10000000)
        )
        
        # Overall exclusion status (True if any exclusion criteria is met)
        selection_df['Any_Exclusion'] = (
            selection_df['MIC_Excluded'] | 
            selection_df['Subsector_Excluded'] | 
            selection_df['Turnover_Excluded']
        )
        
        # Combine exclusion reasons for readability
        selection_df['Exclusion_Reason'] = selection_df.apply(
            lambda row: (
                'Excluded: MIC not in allowed list' if row['MIC_Excluded'] 
                else 'Excluded: Subsector Code does not start with 3010' if row['Subsector_Excluded']
                else 'Excluded: 3 months average turnover EUR < 10M' if row['Turnover_Excluded']
                else ''
            ), axis=1
        )
        
        # Log exclusion statistics
        initial_count = len(selection_df)
        excluded_count = len(selection_df[selection_df['Exclusion_Reason'] != ''])
        eligible_count = initial_count - excluded_count
        
        # Additional logging for exclusion breakdown
        mic_excluded = selection_df['MIC_Excluded'].sum()
        subsector_excluded = selection_df['Subsector_Excluded'].sum()
        turnover_excluded = selection_df['Turnover_Excluded'].sum()
        any_excluded = selection_df['Any_Exclusion'].sum()
        
        logger.info(f"Total universe: {initial_count} companies")
        logger.info(f"Excluded by MIC filter: {mic_excluded} companies")
        logger.info(f"Excluded by ICB subsector filter: {subsector_excluded} companies")
        logger.info(f"Excluded by turnover filter: {turnover_excluded} companies")
        logger.info(f"Total excluded (any criteria): {any_excluded} companies")
        logger.info(f"Eligible for selection: {initial_count - any_excluded} companies")
        
        # Filter only eligible companies for top 150 selection
        eligible_df = selection_df[~selection_df['Any_Exclusion']].copy()
        
        # Sort by FFMC and select the top 150 companies with highest FFMC values from eligible companies
        top_n = 10
        logger.info(f"Selecting top {top_n} companies with highest FFMC from eligible companies...")
        top_10_eligible = eligible_df.nlargest(top_n, 'FFMC').copy()
        companies_51_150 = top_10_eligible.copy()
        # Create top_10_df by merging back with full selection_df to preserve all columns including exclusion info
        top_10_df = selection_df[selection_df['ISIN code'].isin(companies_51_150['ISIN code'])].copy()
        
        # Calculate the target market cap per company (equal weighting)
        target_mcap_per_company = index_mcap / top_n
        top_10_df['Unrounded NOSH'] = target_mcap_per_company / top_10_df['Close Prc_EOD']
        top_10_df['Rounded NOSH'] = top_10_df['Unrounded NOSH'].round()
        top_10_df['Free Float'] = 1
        top_10_df['Effective Date of Review'] = effective_date
        top_10_df['Currency'] = currency
        selection_df['Unrounded NOSH'] = target_mcap_per_company / selection_df['Close Prc_EOD']
        
        # Prepare BE1P DataFrame
        BE1P_df = (
            top_10_df[
                ['Company', 'ISIN code', 'MIC', 'Rounded NOSH', 'Free Float', 'Capping Factor', 
                'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'Rounded NOSH': 'Number of Shares'})
            .rename(columns={'ISIN code': 'ISIN Code'})
            .sort_values('Company')
        )

        # Perform Inclusion/Exclusion Analysis
        analysis_results = inclusion_exclusion_analysis(
            top_10_df, 
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
            be1p_path = os.path.join(output_dir, f'BE1P_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving BE1P output to: {be1p_path}")
            with pd.ExcelWriter(be1p_path) as writer:
                BE1P_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"be1p_path": be1p_path}
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