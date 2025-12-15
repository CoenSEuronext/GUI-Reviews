import pandas as pd
import numpy as np
from datetime import datetime
import os
import traceback
from Review.functions import read_semicolon_csv
from config import DLF_FOLDER, DATA_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data
from utils.inclusion_exclusion import inclusion_exclusion_analysis

logger = setup_logging(__name__)

def run_c6rip_review(date, co_date, effective_date, index="C6RIP", isin="QS0011256235", 
                    area="US", area2="EU", type="STOCK", universe="cac_family", 
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
        # Load CAC Family (CACLG sheet), plus Sustainalytics data
        ref_data = load_reference_data(current_data_folder, ['ff', 'cac_family', 'icb', 'sustainalytics'])
        
        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        # Chain all data preparation operations
        base_df = (ref_data['cac_family']
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
            # Merge Sustainalytics ESG data (Mirova/ISS-oekom score)
            .merge(
                ref_data['sustainalytics'][['ISIN', 'ESG Risk Score']].drop_duplicates(subset='ISIN', keep='first'),
                left_on='ISIN code',
                right_on='ISIN',
                how='left'
            )
            .drop('ISIN', axis=1)
            .merge(
                ref_data['ff'][['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset='ISIN Code:', keep='first'), 
                left_on='ISIN code', 
                right_on='ISIN Code:', 
                how='left'
            )
            .drop('ISIN Code:', axis=1)
        )
        
        # Validate data loading
        if base_df is None or len(base_df) == 0:
            raise ValueError("Failed to load one or more required reference data files")
        
        logger.info(f"Starting universe size: {len(base_df)} stocks")
        
        # STEP 1: Index Universe - All CAC Large 60 constituents (no filtering)
        logger.info("Step 1: Index Universe - CAC Large 60 constituents...")
        universe_df = base_df.copy()
        
        logger.info(f"Universe size: {len(universe_df)} stocks")
        
        # STEP 2: No eligibility screening required
        logger.info("Step 2: No eligibility screening applied")
        
        # Calculate Free Float Market Cap for ranking (tiebreaker)
        universe_df['FF_Market_Cap'] = (
            universe_df['Number of shares'] * 
            universe_df['Close Prc_CO'] * 
            universe_df['Free Float Round:'] / 100 *
            universe_df['FX/Index Ccy']  # Convert to EUR if needed
        )
        
        # STEP 3: Rank by Sustainability Score (Mirova/ISS-oekom = ESG Risk Score)
        # Lower ESG Risk Score is better, so we rank in ascending order
        logger.info("Step 3: Ranking by Sustainability Score (ESG Risk Score) and Free Float Market Cap...")
        
        # Handle missing ESG ratings (put them at the end)
        universe_df['Sustainability_Score'] = universe_df['ESG Risk Score'].fillna(999)
        
        universe_df = universe_df.sort_values(
            by=['Sustainability_Score', 'FF_Market_Cap'],
            ascending=[True, False]  # Lower ESG risk ranks higher, higher market cap ranks higher
        ).reset_index(drop=True)
        
        # STEP 4: Select all constituents (60 companies)
        logger.info("Step 4: Selecting all constituents...")
        selection_df = universe_df.copy()
        
        logger.info(f"Selected {len(selection_df)} constituents for c6rip index")

        # WEIGHTING PROCEDURE - Rank-based fixed weights
        logger.info("Calculating rank-based weights...")
        
        # Assign rank
        selection_df['Rank'] = range(1, len(selection_df) + 1)
        
        # Function to assign weight based on rank
        def assign_weight_by_rank(rank):
            if 1 <= rank <= 15:
                return 0.025  # 2.500%
            elif 16 <= rank <= 30:
                return 31.25 / 15 / 100  # 2.083%
            elif 31 <= rank <= 45:
                return 0.0125  # 1.250%
            elif 46 <= rank <= 60:
                return 12.5 / 15 / 100  # 0.833%
            else:
                return 0
        
        selection_df['Weight_Final'] = selection_df['Rank'].apply(assign_weight_by_rank)
        
        logger.info(f"Weight range: {selection_df['Weight_Final'].min():.4%} to {selection_df['Weight_Final'].max():.4%}")
        logger.info(f"Total weight: {selection_df['Weight_Final'].sum():.4%}")
        
        # Calculate Number of Shares based on weights
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == str(isin).strip(), 'Mkt Cap'].iloc[0]
        
        selection_df['Target_Market_Cap'] = selection_df['Weight_Final'] * index_mcap
        selection_df['Number_of_Shares_Calculated'] = np.round(
            selection_df['Target_Market_Cap'] / 
            (selection_df['Close Prc_EOD'] * selection_df['FX/Index Ccy'])
        )
        
        # Set Free Float and Capping Factor to 1 (not applied)
        selection_df['Free_Float_Output'] = 1
        selection_df['Capping_Factor'] = 1
        selection_df['Effective Date of Review'] = effective_date
        selection_df['Currency'] = currency

        # Prepare final composition dataframe
        c6rip_df = (
            selection_df[[
                'Company',
                'ISIN code',
                'MIC', 
                'Number_of_Shares_Calculated',
                'Free_Float_Output',
                'Capping_Factor',
                'Effective Date of Review', 
                'Currency'
            ]]
            .rename(columns={
                'ISIN code': 'ISIN Code',
                'Free_Float_Output': 'Free Float',
                'Capping_Factor': 'Capping Factor',
                'Number_of_Shares_Calculated': 'Number of Shares'
            })
            .sort_values('Company')
        )
        
        # Perform Inclusion/Exclusion Analysis using existing function
        logger.info("Performing inclusion/exclusion analysis...")
        analysis_results = inclusion_exclusion_analysis(
            c6rip_df, 
            stock_eod_df, 
            index, 
            isin_column='ISIN Code'
        )
        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']
        
        logger.info(f"Inclusions: {len(inclusion_df)}, Exclusions: {len(exclusion_df)}")

        # Save output files
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
           
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            c6rip_path = os.path.join(output_dir, f'C6RIP_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving c6rip output to: {c6rip_path}")
            with pd.ExcelWriter(c6rip_path) as writer:
                c6rip_df.to_excel(writer, sheet_name=index + ' Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(
                    writer, sheet_name='Index Market Cap', index=False
                )
                selection_df.to_excel(writer, sheet_name='Selection', index=False)
                universe_df.to_excel(writer, sheet_name='Full Universe', index=False)
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "c6rip_path": c6rip_path}
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