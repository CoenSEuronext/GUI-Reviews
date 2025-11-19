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

def run_envb_review(date, co_date, effective_date, index="ENVB", isin="QS0011256235", 
                    area="US", area2="EU", type="STOCK", universe="EUROPE500", 
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
        # Load Euronext Europe 500, plus Sustainalytics data
        ref_data = load_reference_data(current_data_folder, ['ff', 'europe_500', 'icb', 'sustainalytics'])
        
        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() == 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        # Chain all data preparation operations
        base_df = (ref_data['europe_500']
            # Merge symbols
            .merge(
                symbols_filtered,
                left_on='ISIN',
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
            # Merge Sustainalytics ESG data
            .merge(
                ref_data['sustainalytics'][['ISIN', 'ESG Risk Score', 'Overall Global Compact Compliance Status']].drop_duplicates(subset='ISIN', keep='first'),
                on='ISIN',
                how='left'
            )
            .merge(
                ref_data['ff'][['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset='ISIN Code:', keep='first'), 
                left_on='ISIN', 
                right_on='ISIN Code:', 
                how='left'
            )
            .drop('ISIN Code:', axis=1)
            
        
    
    )
        

        # Validate data loading
        if base_df is None or len(base_df) == 0:
            raise ValueError("Failed to load one or more required reference data files")
        
        logger.info(f"Starting universe size: {len(base_df)} stocks")
        
        # STEP 1: Filter to Index Universe (Amsterdam, Brussels, Luxembourg only)
        logger.info("Step 1: Filtering Index Universe to AMS/BRU/LUX exchanges...")
        eligible_mics = ['XAMS', 'XBRU', 'XLUX']  # Adjust MIC codes as needed
        universe_df = base_df[base_df['MIC'].isin(eligible_mics)].copy()
        
        logger.info(f"Universe size after exchange filtering: {len(universe_df)} stocks")
        
        # universe_df = index_universe(base_df)
        
        # STEP 2: ESG Eligibility Screening
        logger.info("Step 2: Applying ESG compliance screening...")
        # Exclude companies flagged as NONCOMPLIANT (Field ID: 231112111799)
        universe_df = universe_df[
            (universe_df['Overall Global Compact Compliance Status'] != 'Non-Compliant') | 
            (universe_df['Overall Global Compact Compliance Status'].isna())
        ].copy()
        
        logger.info(f"Universe size after ESG screening: {len(universe_df)} stocks")
        
        # Calculate Free Float Market Cap for ranking (tiebreaker)
        universe_df['FF_Market_Cap'] = (
            universe_df['NOSH'] * 
            universe_df['Price (EUR) '] * 
            universe_df['Free Float Round:'] / 100 *
            universe_df['FX/Index Ccy']  # Convert to EUR if needed
        )
        
        # STEP 3: Rank by ESG Risk Rating (lower is better), then by FF Market Cap
        logger.info("Step 3: Ranking by ESG Risk Rating and Free Float Market Cap...")
        
        # Handle missing ESG ratings (put them at the end)
        universe_df['ESG_Risk_Rating'] = universe_df['ESG Risk Score'].fillna(999)
        
        universe_df = universe_df.sort_values(
            by=['ESG_Risk_Rating', 'FF_Market_Cap'],
            ascending=[True, False]  # Lower ESG risk ranks higher, higher market cap ranks higher
        ).reset_index(drop=True)
        
        # STEP 4: Select top 20
        logger.info("Step 4: Selecting top 20 constituents...")
        selection_df = universe_df.head(20).copy()
        
        logger.info(f"Selected {len(selection_df)} constituents for ENVB index")

        # WEIGHTING PROCEDURE (Section 2.4)
        logger.info("Calculating ESG-based weights...")
        
        # Calculate ESG Score: ESG_Score_i = 1 / ESG_risk_rating_i
        selection_df['ESG_Score'] = 1 / selection_df['ESG_Risk_Rating']
        
        # Calculate raw weights: w_i = ESG_Score_i / sum(ESG_Score_j)
        total_esg_score = selection_df['ESG_Score'].sum()
        selection_df['Weight_Uncapped'] = selection_df['ESG_Score'] / total_esg_score
        
        # Apply 10% capping (Section 2.3)
        logger.info("Applying 10% weight cap...")
        max_weight = 0.10
        
        # Iterative capping procedure
        selection_df['Weight_Capped'] = selection_df['Weight_Uncapped'].copy()
        capping_iterations = 0
        max_iterations = 20  # Prevent infinite loops
        
        while (selection_df['Weight_Capped'] > max_weight).any() and capping_iterations < max_iterations:
            # Identify stocks exceeding cap
            excess_mask = selection_df['Weight_Capped'] > max_weight
            
            # Cap the excess stocks
            excess_weight = selection_df.loc[excess_mask, 'Weight_Capped'].sum() - (excess_mask.sum() * max_weight)
            
            # Set capped stocks to max weight
            selection_df.loc[excess_mask, 'Weight_Capped'] = max_weight
            
            # Redistribute excess proportionally to uncapped stocks
            uncapped_mask = ~excess_mask
            if uncapped_mask.sum() > 0:
                uncapped_sum = selection_df.loc[uncapped_mask, 'Weight_Capped'].sum()
                if uncapped_sum > 0:
                    selection_df.loc[uncapped_mask, 'Weight_Capped'] += (
                        excess_weight * selection_df.loc[uncapped_mask, 'Weight_Capped'] / uncapped_sum
                    )
            
            capping_iterations += 1
        
        # Normalize to ensure weights sum to 100%
        selection_df['Weight_Final'] = selection_df['Weight_Capped'] / selection_df['Weight_Capped'].sum()
        
        logger.info(f"Capping completed in {capping_iterations} iterations")
        logger.info(f"Weight range: {selection_df['Weight_Final'].min():.2%} to {selection_df['Weight_Final'].max():.2%}")
        
        # Calculate Number of Shares based on weights
        # This ensures each constituent's market cap reflects its target weight
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == str(isin).strip(), 'Mkt Cap'].iloc[0]
        
        selection_df['Target_Market_Cap'] = selection_df['Weight_Final'] * index_mcap
        selection_df['Number_of_Shares_Calculated'] = np.floor(
            selection_df['Target_Market_Cap'] / 
            (selection_df['Close Prc_EOD'] * selection_df['FX/Index Ccy'])
        )
        
        # Calculate Capping Factor (for reference)
        selection_df['Capping_Factor'] = selection_df['Weight_Final'] / selection_df['Weight_Uncapped']
        selection_df['Effective Date of Review'] = effective_date
        # Create Free Float column with value 1 for all constituents
        selection_df['Free Float companies'] = 1

        # Prepare final composition dataframe
        ENVB_df = (
            selection_df[[
                'Name',
                'ISIN',
                'MIC', 
                'Number_of_Shares_Calculated',  # Use calculated shares
                'Free Float companies', # Use the new column instead of 'Free Float Round:'
                'Capping_Factor',
                'Effective Date of Review', 
                'Currency (Local)'
            ]]
            .rename(columns={
                'Name': 'Company',
                'ISIN': 'ISIN Code',
                'Free Float companies': 'Free Float',# Rename to 'Free Float' in output
                'Capping_Factor': 'Capping Factor', 
                'Number_of_Shares_Calculated': 'Number of Shares',
                'Currency (Local)': 'Currency'
            })
            .sort_values('Company')
        )
        
       

        # Perform Inclusion/Exclusion Analysis using existing function
        logger.info("Performing inclusion/exclusion analysis...")
        analysis_results = inclusion_exclusion_analysis(
            ENVB_df, 
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
            ENVB_path = os.path.join(output_dir, f'ENVB_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving ENVB output to: {ENVB_path}")
            with pd.ExcelWriter(ENVB_path) as writer:
                ENVB_df.to_excel(writer, sheet_name=index + ' Composition', index=False)
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
                    "ENVB_path": ENVB_path}
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


def index_universe(base_df):
    # STEP 1: Filter to Index Universe (Amsterdam, Brussels, Luxembourg only)
    logger.info("Step 1: Filtering Index Universe to AMS/BRU/LUX exchanges...")
    eligible_mics = ['XAMS', 'XBRU', 'XLUX']  # Adjust MIC codes as needed
    universe_df = base_df[base_df['MIC'].isin(eligible_mics)].copy()
    
    logger.info(f"Universe size after exchange filtering: {len(universe_df)} stocks")

    return universe_df