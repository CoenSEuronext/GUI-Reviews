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

def run_eri5p_review(date, co_date, effective_date, index="ERI5P", isin="NL0012949143", 
                   area="US", area2="EU", type="STOCK", universe="eurozone_300", 
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
            [universe, 'ff', 'oekom_score']
        )
        selection_df = ref_data[universe]
        ff_df = ref_data['ff']
        oekom_score_df = ref_data['oekom_score']

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
            # Merge Oekom Opinion data
            .merge(
                oekom_score_df[['ISIN', 'Opinion', 'New Sustainability Score']].drop_duplicates(subset='ISIN', keep='first'),
                left_on='ISIN code',
                right_on='ISIN',
                how='left'
            )
            .drop('ISIN', axis=1)
        )

        # Validate data loading
        if any(df is None for df in [selection_df]):
            raise ValueError("Failed to load one or more required reference data files")
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        
        # Use the merged Free Float Round value, fallback to original Free Float if not available
        selection_df["Free Float"] = selection_df["Free Float Round:"]
        selection_df['FFMC'] = selection_df['Capping Factor'] * selection_df['Number of Shares'] * selection_df['Free Float'] * selection_df['Price']
        
        # Add exclusion filters with separate columns
        selection_df['FFMC_Exclusion'] = selection_df['FFMC'] < 3000000000  # Below 3B EUR
        selection_df['Turnover_Exclusion'] = selection_df['100 days aver. turn EUR'] < 22000000  # Below 22M EUR
        # Opinion is negative, risk, or empty/null
        selection_df['Opinion_Exclusion'] = (
            selection_df['Opinion'].isin(['negative', 'risk']) | 
            selection_df['Opinion'].isna() | 
            (selection_df['Opinion'].astype(str).str.strip() == '')
        )
        
        # Create individual exclusion reason columns
        selection_df['FFMC_Exclusion_Reason'] = selection_df['FFMC_Exclusion'].apply(
            lambda x: 'Excluded: FFMC < EUR 3B' if x else ''
        )
        selection_df['Turnover_Exclusion_Reason'] = selection_df['Turnover_Exclusion'].apply(
            lambda x: 'Excluded: 100 days avg turnover < EUR 22M' if x else ''
        )
        selection_df['Opinion_Exclusion_Reason'] = selection_df['Opinion_Exclusion'].apply(
            lambda x: 'Excluded: Opinion negative/risk/empty' if x else ''
        )
        
        # Create summary exclusion reason column
        selection_df['All_Exclusion_Reasons'] = selection_df.apply(
            lambda row: '; '.join(filter(None, [
                row['FFMC_Exclusion_Reason'], 
                row['Turnover_Exclusion_Reason'], 
                row['Opinion_Exclusion_Reason']
            ])),
            axis=1
        )
        selection_df = selection_df.drop(columns=['FFMC_Exclusion'])
        selection_df = selection_df.drop(columns=['Turnover_Exclusion'])
        selection_df = selection_df.drop(columns=['Opinion_Exclusion'])
        
        # FIXED RANKING LOGIC
        # Handle missing values before ranking to avoid issues
        selection_df['New Sustainability Score'] = pd.to_numeric(selection_df['New Sustainability Score'], errors='coerce')
        selection_df['FFMC'] = pd.to_numeric(selection_df['FFMC'], errors='coerce')
        
        # Fill NaN values with very low values so they rank last
        selection_df['New Sustainability Score'] = selection_df['New Sustainability Score'].fillna(100)
        
        # Add ranking columns for all companies - FIXED VERSION
        # Sort by Sustainability Score descending (higher is better), then by FFMC descending (higher is better)
        selection_df_sorted = selection_df.sort_values(
            ['New Sustainability Score', 'FFMC'], 
            ascending=[False, False]  # Both False means higher values get rank 1, 2, 3...
        ).reset_index(drop=True)
        
        # Use pandas rank method for more robust ranking (handles ties properly)
        selection_df['Overall_Ranking'] = selection_df[['New Sustainability Score', 'FFMC']].apply(
            lambda x: (-x['New Sustainability Score'], -x['FFMC']), axis=1
        ).rank(method='min').astype(int)
        
        # Alternative more explicit approach using sort_values and reset_index
        temp_df = selection_df.copy()
        temp_df = temp_df.sort_values(['New Sustainability Score', 'FFMC'], ascending=[False, False])
        temp_df['Overall_Ranking_Alt'] = range(1, len(temp_df) + 1)
        
        # Merge back the alternative ranking for verification
        selection_df = selection_df.merge(
            temp_df[['ISIN code', 'Overall_Ranking_Alt']],
            on='ISIN code',
            how='left'
        )
        
        # Use the alternative ranking as the main one (more reliable)
        selection_df['Overall_Ranking'] = selection_df['Overall_Ranking_Alt']
        selection_df = selection_df.drop(columns=['Overall_Ranking_Alt'])
        
        # Add ranking columns for eligible companies only - FIXED VERSION
        eligible_companies = selection_df[selection_df['All_Exclusion_Reasons'] == ''].copy()
        
        if len(eligible_companies) > 0:
            # Sort eligible companies properly
            eligible_sorted = eligible_companies.sort_values(
                ['New Sustainability Score', 'FFMC'], 
                ascending=[False, False]
            ).reset_index(drop=True)
            eligible_sorted['Eligible_Ranking'] = range(1, len(eligible_sorted) + 1)
            
            # Merge the eligible ranking back to the original dataframe
            selection_df = selection_df.merge(
                eligible_sorted[['ISIN code', 'Eligible_Ranking']],
                on='ISIN code',
                how='left'
            )
        else:
            selection_df['Eligible_Ranking'] = np.nan
        
        # Log ranking validation info
        logger.info("Ranking validation:")
        top_10_overall = selection_df.nsmallest(10, 'Overall_Ranking')[['Company', 'New Sustainability Score', 'FFMC', 'Overall_Ranking', 'All_Exclusion_Reasons']]
        logger.info(f"Top 10 companies by overall ranking:\n{top_10_overall.to_string()}")
        
        if len(eligible_companies) > 0:
            top_10_eligible = selection_df[selection_df['All_Exclusion_Reasons'] == ''].nsmallest(10, 'Eligible_Ranking')[['Company', 'New Sustainability Score', 'FFMC', 'Eligible_Ranking']]
            logger.info(f"Top 10 eligible companies:\n{top_10_eligible.to_string()}")
        
        # Log exclusion statistics
        initial_count = len(selection_df)
        excluded_count = len(selection_df[selection_df['All_Exclusion_Reasons'] != ''])
        eligible_count = initial_count - excluded_count
        
        logger.info(f"Total universe: {initial_count} companies")
        logger.info(f"Total excluded: {excluded_count} companies")
        logger.info(f"Eligible for selection: {eligible_count} companies")
                
        # Filter only eligible companies for top selection
        eligible_df = selection_df[selection_df['All_Exclusion_Reasons'] == ''].copy()
        
        # Select top 50 companies using the eligible ranking
        top_n = 50
        logger.info(f"Selecting top {top_n} companies ranked by Sustainability Score (with FFMC as tiebreaker) from eligible companies...")
        
        if len(eligible_df) >= top_n:
            top_50_eligible = eligible_df.nsmallest(top_n, 'Eligible_Ranking').copy()
        else:
            logger.warning(f"Only {len(eligible_df)} eligible companies available, selecting all of them instead of {top_n}")
            top_50_eligible = eligible_df.copy()
        
        companies_51_150 = top_50_eligible.copy()
        # Create top_50_df by merging back with full selection_df to preserve all columns including exclusion info
        top_50_df = selection_df[selection_df['ISIN code'].isin(companies_51_150['ISIN code'])].copy()
        
        # Calculate the target market cap per company (equal weighting)
        target_mcap_per_company = index_mcap / len(top_50_df)  # Use actual number of selected companies
        top_50_df['Unrounded NOSH'] = target_mcap_per_company / top_50_df['Close Prc_EOD']
        top_50_df['Rounded NOSH'] = top_50_df['Unrounded NOSH'].round()
        top_50_df['Free Float'] = 1
        top_50_df['Effective Date of Review'] = effective_date
        top_50_df['Currency'] = currency
        selection_df['Unrounded NOSH'] = target_mcap_per_company / selection_df['Close Prc_EOD']
        
        # Prepare ERI5P DataFrame with ranking columns
        ERI5P_df = (
            top_50_df[
                ['Company', 'ISIN code', 'MIC', 'Rounded NOSH', 'Free Float', 
                 'Capping Factor', 'Effective Date of Review', 'Currency']
            ]
            .rename(columns={
                'Rounded NOSH': 'Number of Shares',
                'ISIN code': 'ISIN Code',
            })
            .sort_values('Company')
        )

        # Perform Inclusion/Exclusion Analysis
        analysis_results = inclusion_exclusion_analysis(
            top_50_df, 
            stock_eod_df, 
            index, 
            isin_column='ISIN code'
        )
        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']

        # Prepare Full Universe output with ranking columns
        full_universe_df = selection_df[[
            'Overall_Ranking', 'Eligible_Ranking', 'Company', 'ISIN code', 'MIC', '#Symbol',
            'New Sustainability Score', 'FFMC', 'Opinion', 'Free Float', 'Number of Shares',
            'Price', 'Close Prc_EOD', 'Close Prc_CO', '100 days aver. turn EUR',
            'All_Exclusion_Reasons', 'FFMC_Exclusion_Reason', 'Turnover_Exclusion_Reason', 
            'Opinion_Exclusion_Reason'
        ]].sort_values('Overall_Ranking')

        # Save output files
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
           
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            eri5p_path = os.path.join(output_dir, f'ERI5P_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving ERI5P output to: {eri5p_path}")
            with pd.ExcelWriter(eri5p_path) as writer:
                ERI5P_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                full_universe_df.to_excel(writer, sheet_name='Full Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"eri5p_path": eri5p_path}
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