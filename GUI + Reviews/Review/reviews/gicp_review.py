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

def run_gicp_review(date, co_date, effective_date, index="GICP", isin="NLIX00005321", 
                    area="US", area2="EU", type="STOCK", universe="Developed Market", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "gicp"
        isin (str, optional): ISIN code. Defaults to "NLIX00005321"
        area (str, optional): Primary area. Defaults to "US"
        area2 (str, optional): Secondary area. Defaults to "EU"
        type (str, optional): Type of instrument. Defaults to "STOCK"
        universe (str, optional): Universe name. Defaults to "Developed Market"
        feed (str, optional): Feed source. Defaults to "Reuters"
        currency (str, optional): Currency code. Defaults to "EUR"
        year (str, optional): Year for calculation. Defaults to None (extracted from date)

    Returns:
        dict: Result dictionary containing status, message, and data
    """
    try:
        # If year is not provided, get it from the date
        if year is None:
            year = str(datetime.strptime(date, '%Y%m%d').year)

        # Set data folder for current month
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        # Use data_loader functions to load data
        logger.info("Loading EOD data...")
        index_eod_df, stock_eod_df, stock_co_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)
        
        logger.info("Loading reference data...")
        ref_data = load_reference_data(current_data_folder, ['ff', 'developed_market', 'icb'])
        
        # Extract the needed DataFrames
        ff_df = ref_data['ff']
        developed_market_df = ref_data['developed_market']
        icb_df = ref_data['icb']
        
        if any(df is None for df in [ff_df, developed_market_df, icb_df]):
            raise ValueError("Failed to load one or more required reference data files")

        # Rest of your code remains the same...
        logger.info("Processing market data...")
        developed_market_df = developed_market_df.merge(
            icb_df.drop_duplicates('ISIN Code', keep='first')[['ISIN Code', 'Subsector Code']],
            left_on='ISIN',
            right_on='ISIN Code',
            how='left'
        ).drop('ISIN Code', axis=1)

        # Add Free Float data
        developed_market_df = developed_market_df.merge(
            ff_df[['ISIN Code:', 'Free Float Round:']],
            left_on='ISIN',
            right_on='ISIN Code:',
            how='left'
        ).drop('ISIN Code:', axis=1).rename(columns={'Free Float Round:': 'Free Float',
                                                     'Name': 'Company'})

        # Continue with the rest of your existing logic...
        developed_market_df['FFMC'] = (developed_market_df['NOSH'] * 
                                     developed_market_df['Price (EUR) '] * 
                                     developed_market_df['Free Float'])
        
        # Convert index column to string type before string operations
        developed_market_df['index'] = developed_market_df['index'].astype(str)
        
        # FIXED ISSUE 1: Correct Universe Definition
        # The universe should consist of constituents from:
        # - Euronext® North America 500 (excluding Toronto Stock Exchange XTSE)
        # - Euronext® Eurozone 300
        developed_market_df['exclusion_1'] = None
        
        # Create proper universe mask
        universe_mask = (
            # Companies in EZ300 (Eurozone 300)
            (developed_market_df['index'].str.contains('EZ300', na=False)) |
            # Companies in NA500 (North America 500) but NOT on Toronto Stock Exchange (XTSE)
            ((developed_market_df['index'].str.contains('NA500', na=False)) & 
             (developed_market_df['MIC'] != 'XTSE'))
        )
        
        # Apply exclusion for companies NOT in the universe
        developed_market_df['exclusion_1'] = np.where(
            ~universe_mask,
            'exclude_Area',
            None
        )
        
        # Similarly convert Subsector Code to string
        developed_market_df['Subsector Code'] = developed_market_df['Subsector Code'].astype(str)

        # FIXED ISSUE 2: Correct Eligibility Screening Logic
        # Out of the Index Universe, companies belonging to specific ICB classifications are eligible
        # (NOT tied to specific exchanges)
        developed_market_df['exclusion_2'] = None

        # Create the eligibility mask for ICB classifications (applied only to universe companies)
        universe_companies_mask = developed_market_df['exclusion_1'].isna()
        
        eligibility_conditions = (
            # Banks Super Sector (3010) - any exchange in universe
            (developed_market_df['Subsector Code'].str[:4] == '3010') |
            
            # Clothing and Accessories Subsector (40204020) - any exchange in universe
            (developed_market_df['Subsector Code'].str[:8] == '40204020') |
            
            # Automobiles and Parts Super Sector (4010) - any exchange in universe
            (developed_market_df['Subsector Code'].str[:4] == '4010') |
            
            # Industrial Goods and Services Super Sector (5020) - any exchange in universe
            (developed_market_df['Subsector Code'].str[:4] == '5020') |
            
            # Technology Hardware and Equipment Sector (101020) - any exchange in universe
            (developed_market_df['Subsector Code'].str[:6] == '101020') |
            
            # Technology Industry (10) - any exchange in universe
            (developed_market_df['Subsector Code'].str[:2] == '10') |
            
            # Consumer Discretionary Industry (40) - any exchange in universe
            (developed_market_df['Subsector Code'].str[:2] == '40')
        )

        # Apply the exclusion for universe companies that don't meet ICB eligibility criteria
        developed_market_df.loc[universe_companies_mask & ~eligibility_conditions, 'exclusion_2'] = 'exclude_category'
        
        # Create a mask for non-excluded companies (in universe and eligible)
        non_excluded_mask = (
            developed_market_df['exclusion_1'].isna() & 
            developed_market_df['exclusion_2'].isna()
        )

        # FIXED ISSUE 3: Correct Grouping and Selection
        # Group 1: Companies from Euronext® Eurozone 300 (15 largest by FFMC)
        # Group 2: Companies from Euronext® North America 500 (15 largest by FFMC)
        
        # Add group classification for eligible companies
        developed_market_df['category'] = None
        
        # Group 1: EZ300 companies that are eligible
        group1_mask = (non_excluded_mask & 
                      developed_market_df['index'].str.contains('EZ300', na=False))
        developed_market_df.loc[group1_mask, 'category'] = 'Group_1_EZ300'
        
        # Group 2: NA500 companies (excluding XTSE) that are eligible
        group2_mask = (non_excluded_mask & 
                      developed_market_df['index'].str.contains('NA500', na=False) &
                      (developed_market_df['MIC'] != 'XTSE'))
        developed_market_df.loc[group2_mask, 'category'] = 'Group_2_NA500'

        # Add rank for each group based on FFMC
        developed_market_df['rank'] = None
        
        # Rank within Group 1 (EZ300)
        group1_companies = developed_market_df['category'] == 'Group_1_EZ300'
        if group1_companies.sum() > 0:
            developed_market_df.loc[group1_companies, 'rank'] = (
                developed_market_df.loc[group1_companies, 'FFMC']
                .rank(method='first', ascending=False)
            )
        
        # Rank within Group 2 (NA500)
        group2_companies = developed_market_df['category'] == 'Group_2_NA500'
        if group2_companies.sum() > 0:
            developed_market_df.loc[group2_companies, 'rank'] = (
                developed_market_df.loc[group2_companies, 'FFMC']
                .rank(method='first', ascending=False)
            )

        # Select top 15 from each group (30 total)
        developed_market_df['selected'] = False
        
        # Select top 15 from Group 1 (EZ300)
        group1_top15_mask = (
            (developed_market_df['category'] == 'Group_1_EZ300') & 
            (developed_market_df['rank'] <= 15)
        )
        developed_market_df.loc[group1_top15_mask, 'selected'] = True
        
        # Select top 15 from Group 2 (NA500)
        group2_top15_mask = (
            (developed_market_df['category'] == 'Group_2_NA500') & 
            (developed_market_df['rank'] <= 15)
        )
        developed_market_df.loc[group2_top15_mask, 'selected'] = True

        # Verify total number of selected companies
        total_selected = developed_market_df['selected'].sum()
        group1_selected = group1_top15_mask.sum()
        group2_selected = group2_top15_mask.sum()
        
        logger.info(f"Selected {group1_selected} companies from Group 1 (EZ300)")
        logger.info(f"Selected {group2_selected} companies from Group 2 (NA500)")
        logger.info(f"Total selected: {total_selected} companies")
        
        if total_selected != 30:
            logger.warning(f"Selected {total_selected} companies instead of expected 30")

        # Create selection dataframe
        selection_df = developed_market_df[developed_market_df['selected']].copy()
        selection_df.to_excel('debug_output.xlsx', index=False)
        os.startfile('debug_output.xlsx')
        # First remove duplicates from stock_eod_df
        unique_stock_eod = (stock_eod_df[['Isin Code', 'Currency', 'Close Prc', 'FX/Index Ccy']]
                        .drop_duplicates(subset=['Isin Code', 'Currency'], keep='first'))

        # Then merge with selection_df
        selection_df = selection_df.merge(
            unique_stock_eod,
            left_on=['ISIN', 'Currency (Local)'],
            right_on=['Isin Code', 'Currency'],
            how='left'
        ).drop(['Isin Code', 'Currency'], axis=1)

        # Now calculate Original market cap using the merged data
        selection_df['Original market cap'] = (
            selection_df['Close Prc'] * 
            selection_df['FX/Index Ccy'] * 
            selection_df['Free Float'] * 
            selection_df['NOSH']
        )

        # Add logging to check if all matches were found
        missing_data = selection_df[selection_df['Close Prc'].isna()]
        if not missing_data.empty:
            logger.warning(f"Missing price/FX data for {len(missing_data)} companies:")
            for _, row in missing_data.iterrows():
                logger.warning(f"ISIN: {row['ISIN']}, Currency: {row['Currency (Local)']}")
        
        # Apply Group-based weighting according to specification:
        # Group 1 (EZ300): 60% weight, Group 2 (NA500): 40% weight
        # Maximum 10% per constituent
        
        def apply_category_capping(df, target_weight, total_mcap, max_weight=0.10):
            """
            Apply capping within a group
            
            Args:
                df: DataFrame containing the group's constituents
                target_weight: Target weight for the entire group (e.g., 0.60 for 60%)
                total_mcap: Total market cap of all selected companies
                max_weight: Maximum weight for any individual constituent (as % of total portfolio)
            """
            # Calculate initial weights relative to total portfolio
            df['Initial Weight'] = df['Original market cap'] / total_mcap
            
            # Calculate the scaling factor to achieve target group weight
            category_weight = df['Initial Weight'].sum()
            if category_weight > 0:
                initial_scaling = target_weight / category_weight
                df['Initial Weight'] *= initial_scaling
            
            iteration = 0
            weights_changed = True
            max_iterations = 100
            
            while weights_changed and iteration < max_iterations:
                weights_changed = False
                
                # Identify constituents exceeding max weight
                capped_constituents = df['Initial Weight'] > max_weight
                n_capped = capped_constituents.sum()
                
                if n_capped > 0:
                    # Cap the exceeding constituents
                    df.loc[capped_constituents, 'Initial Weight'] = max_weight
                    
                    # Calculate remaining weight to distribute
                    remaining_target = target_weight - (max_weight * n_capped)
                    
                    if remaining_target > 0:
                        # Redistribute excess to uncapped constituents proportionally
                        uncapped = ~capped_constituents
                        uncapped_sum = df.loc[uncapped, 'Initial Weight'].sum()
                        
                        if uncapped_sum > 0:
                            scaling_factor = remaining_target / uncapped_sum
                            df.loc[uncapped, 'Initial Weight'] *= scaling_factor
                            weights_changed = True
                
                iteration += 1
            
            return df

        # Apply weighting by group (not arbitrary categories)
        total_mcap = selection_df['Original market cap'].sum()
        selection_df['Initial Weight'] = 0.0

        # Group 1: EZ300 companies get 60% total weight
        group1_mask = selection_df['category'] == 'Group_1_EZ300'
        if group1_mask.sum() > 0:
            group1_df = selection_df[group1_mask].copy()
            group1_df = apply_category_capping(group1_df, 0.60, total_mcap)  # 60% target
            selection_df.loc[group1_mask, 'Initial Weight'] = group1_df['Initial Weight']

        # Group 2: NA500 companies get 40% total weight
        group2_mask = selection_df['category'] == 'Group_2_NA500'
        if group2_mask.sum() > 0:
            group2_df = selection_df[group2_mask].copy()
            group2_df = apply_category_capping(group2_df, 0.40, total_mcap)  # 40% target
            selection_df.loc[group2_mask, 'Initial Weight'] = group2_df['Initial Weight']

        # Verify group weights
        group1_weight = selection_df[selection_df['category'] == 'Group_1_EZ300']['Initial Weight'].sum()
        group2_weight = selection_df[selection_df['category'] == 'Group_2_NA500']['Initial Weight'].sum()
        logger.info(f"Group 1 (EZ300) weight: {group1_weight:.4%} (target: 60.00%)")
        logger.info(f"Group 2 (NA500) weight: {group2_weight:.4%} (target: 40.00%)")

        # Calculate Final Capping factor
        selection_df['Final Capping'] = (selection_df['Initial Weight'] * total_mcap) / selection_df['Original market cap']

        # Verify final weights
        selection_df['Final Weight'] = (selection_df['Original market cap'] * selection_df['Final Capping']) / (selection_df['Original market cap'] * selection_df['Final Capping']).sum()

        # Log verification
        logger.info("\nFinal Weight Verification:")
        group1_final_weight = selection_df[selection_df['category'] == 'Group_1_EZ300']['Final Weight'].sum()
        group2_final_weight = selection_df[selection_df['category'] == 'Group_2_NA500']['Final Weight'].sum()
        logger.info(f"Group 1 (EZ300) final weight: {group1_final_weight:.4%}")
        logger.info(f"Group 2 (NA500) final weight: {group2_final_weight:.4%}")

        max_weight = selection_df['Final Weight'].max()
        logger.info(f"Maximum constituent weight: {max_weight:.4%}")
        selection_df['Effective Date of Review'] = effective_date
        
        # Create final output DataFrame
        GICP_df = selection_df[[
            'Company', 
            'ISIN', 
            'MIC', 
            'NOSH', 
            'Free Float',
            'Final Capping',
            'Effective Date of Review',
            'Currency (Local)'
        ]].copy()

        # Rename columns and sort
        GICP_df = GICP_df.rename(columns={
            'Currency (Local)': 'Currency',
        })
        GICP_df = GICP_df.sort_values('Company')
        
        analysis_results = inclusion_exclusion_analysis(
            selection_df, 
            stock_eod_df, 
            index, 
            isin_column='ISIN'
        )

        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']
        GICP_df = GICP_df.rename(columns={
            'ISIN': 'ISIN Code',
            'NOSH': 'Number of Shares',
        })
        # Save output files
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            gicp_path = os.path.join(output_dir, f'GICP_df_{timestamp}.xlsx')
            
            logger.info(f"Saving GICP output to: {gicp_path}")
            with pd.ExcelWriter(gicp_path) as writer:
                GICP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                developed_market_df.to_excel(writer, sheet_name='Full Universe', index=False)
                selection_df.to_excel(writer, sheet_name='selection', index=False)
            
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "gicp_path": gicp_path
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