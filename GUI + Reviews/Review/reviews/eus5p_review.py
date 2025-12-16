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

def run_eus5p_review(date, co_date, effective_date, index="EUS5P", isin="NL0012949143", 
                   area="US", area2="EU", type="STOCK", universe="developed_market", 
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

        # Get the individual dataframes
        developed_market_df = ref_data[universe]  # developed_market
        ff_df = ref_data['ff']
        icb_df = ref_data['icb']

        # Filter developed_market for EZ300 companies (including EU500;EZ300)
        eurozone_df = developed_market_df[
            (developed_market_df['index'] == 'EZ300') | 
            (developed_market_df['index'] == 'EU500;EZ300')
        ].copy()
        
        # Filter developed_market for NA500 companies (excluding XTSE)
        north_america_df = developed_market_df[
            (developed_market_df['index'] == 'NA500') & 
            (developed_market_df['MIC'] != 'XTSE')
        ].copy()

        logger.info(f"Eurozone companies (EZ300): {len(eurozone_df)}")
        logger.info(f"North America companies (NA500, excluding XTSE): {len(north_america_df)}")

        # Combine the filtered universes
        combined_universe_df = pd.concat([eurozone_df, north_america_df], 
                                        ignore_index=True, sort=False)

        # Add source flag column to identify origin of each row
        source_flags = ['EZ300'] * len(eurozone_df) + ['NA500'] * len(north_america_df)
        combined_universe_df['Universe'] = source_flags
        
        # Add the required columns to the combined dataframe
        combined_universe_df['Capping Factor'] = 1
        combined_universe_df['Effective Date of Review'] = effective_date

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')
        
        # Chain all data preparation operations using the combined dataframe
        selection_df = (combined_universe_df
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
        if selection_df is None:
            raise ValueError("Failed to load required reference data files")
    
        # Populate MIC column for NA500 companies
        mic_mapping = stock_eod_df[['Isin Code', '#Symbol', 'MIC']].drop_duplicates()

        # Match by ISIN Code
        mic_by_isin = mic_mapping[['Isin Code', 'MIC']].drop_duplicates(subset='Isin Code', keep='first')
        mask_isin = (selection_df['MIC'].isna()) & (selection_df['ISIN code'].notna())
        selection_df.loc[mask_isin, 'MIC'] = selection_df.loc[mask_isin, 'ISIN code'].map(
        mic_by_isin.set_index('Isin Code')['MIC']
        )

        # Match by #Symbol for remaining missing values
        mic_by_symbol = mic_mapping[['#Symbol', 'MIC']].drop_duplicates(subset='#Symbol', keep='first')
        mask_symbol = (selection_df['MIC'].isna()) & (selection_df['#Symbol'].notna())
        selection_df.loc[mask_symbol, 'MIC'] = selection_df.loc[mask_symbol, '#Symbol'].map(
        mic_by_symbol.set_index('#Symbol')['MIC']
        )
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        
        # Use the merged Free Float Round value, fallback to original Free Float if not available
        selection_df["Free Float"] = selection_df["Free Float Round:"]
        selection_df['FFMC'] = selection_df['Capping Factor'] * selection_df['Number of Shares'] * selection_df['Free Float'] * selection_df['Price (EUR) ']
        
        # Add ranking columns by universe
        selection_df['Rank_EZ300'] = selection_df[selection_df['Universe'] == 'EZ300']['FFMC'].rank(ascending=False, method='first')
        selection_df['Rank_NA500'] = selection_df[selection_df['Universe'] == 'NA500']['FFMC'].rank(ascending=False, method='first')
        
        # All companies are eligible (no exclusions)
        eligible_df = selection_df.copy()

        # Separate companies by universe
        eligible_eurozone = eligible_df[eligible_df['Universe'] == 'EZ300'].copy()
        eligible_north_america = eligible_df[eligible_df['Universe'] == 'NA500'].copy()

        # Select top companies from each universe based on FFMC
        top_eurozone_n = 35
        top_north_america_n = 15
        total_companies = top_eurozone_n + top_north_america_n

        # Get top companies from each universe
        top_eurozone = eligible_eurozone.nlargest(top_eurozone_n, 'FFMC').copy()
        top_north_america = eligible_north_america.nlargest(top_north_america_n, 'FFMC').copy()

        # Combine the selected companies from both universes
        top_companies_combined = pd.concat([top_eurozone, top_north_america], ignore_index=True)

        # Create top_companies_df by merging back with full selection_df to preserve all columns
        top_companies_df = selection_df[selection_df['ISIN code'].isin(top_companies_combined['ISIN code'])].copy()

        # Calculate the target market cap per company (equal weighting across all 50 companies)
        target_mcap_per_company = index_mcap / total_companies
        top_companies_df['Unrounded NOSH'] = target_mcap_per_company / (top_companies_df['Close Prc_EOD'] * top_companies_df['FX/Index Ccy'])
        top_companies_df['Rounded NOSH'] = top_companies_df['Unrounded NOSH'].round()
        top_companies_df['Free Float'] = 1
        top_companies_df['Effective Date of Review'] = effective_date
        selection_df['Unrounded NOSH'] = target_mcap_per_company / selection_df['Close Prc_EOD']
        
        # Prepare EUS5P DataFrame
        EUS5P_df = (
            top_companies_df[
                ['Company', 'ISIN code', 'MIC', 'Rounded NOSH', 'Free Float', 'Capping Factor', 
                'Effective Date of Review', 'Currency (Local)']
            ]
            .rename(columns={'Rounded NOSH': 'Number of Shares'})
            .rename(columns={'ISIN code': 'ISIN Code'})
            .rename(columns={'Currency (Local)': 'Currency'})
            .sort_values('Company')
        )

        # Perform Inclusion/Exclusion Analysis
        analysis_results = inclusion_exclusion_analysis(
            top_companies_df, 
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
            eus5p_path = os.path.join(output_dir, f'EUS5P_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving EUS5P output to: {eus5p_path}")
            with pd.ExcelWriter(eus5p_path) as writer:
                EUS5P_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                top_companies_df.to_excel(writer, sheet_name='Selection', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"eus5p_path": eus5p_path}
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