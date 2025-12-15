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
from utils.capping_proportional import apply_proportional_capping

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

        # Filter symbols (length < 12) and get unique symbols per ISIN
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        # Chain all data preparation operations using the symbol-based approach
        selection_df = (selection_df
            # Merge symbols first
            .merge(
                symbols_filtered,
                left_on='ISIN',
                right_on='Isin Code',
                how='left'
            )
            .drop('Isin Code', axis=1)
            # Merge FX data using symbols and currency filter
            .merge(
                stock_eod_df[stock_eod_df['Index Curr'] == currency][['#Symbol', 'FX/Index Ccy']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left'
            )
            # Merge EOD prices using symbols
            .merge(
                stock_eod_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left'
            )
        )

        # Now calculate Original market cap using the merged data
        selection_df['Original market cap'] = (
            selection_df['Close Prc'] * 
            selection_df['FX/Index Ccy'] * 
            selection_df['Free Float'] * 
            selection_df['NOSH']
        )

        # Add logging to check if all matches were found
        missing_symbols = selection_df[selection_df['#Symbol'].isna()]
        if not missing_symbols.empty:
            logger.warning(f"Missing symbols for {len(missing_symbols)} companies:")
            for _, row in missing_symbols.iterrows():
                logger.warning(f"ISIN: {row['ISIN']}, Company: {row['Company']}")

        missing_fx_data = selection_df[selection_df['FX/Index Ccy'].isna()]
        if not missing_fx_data.empty:
            logger.warning(f"Missing FX data for {len(missing_fx_data)} companies:")
            for _, row in missing_fx_data.iterrows():
                logger.warning(f"Symbol: {row['#Symbol']}, ISIN: {row['ISIN']}")

        missing_price_data = selection_df[selection_df['Close Prc'].isna()]
        if not missing_price_data.empty:
            logger.warning(f"Missing price data for {len(missing_price_data)} companies:")
            for _, row in missing_price_data.iterrows():
                logger.warning(f"Symbol: {row['#Symbol']}, ISIN: {row['ISIN']}")
        
        # Apply Group-based weighting using colleague's approach
        # Group 1 (EZ300): 60% weight, Group 2 (NA500): 40% weight
        # Maximum 10% per constituent in the ENTIRE portfolio
        
        logger.info("Applying group-based weighting with capping...")
        
        # Initialize weight columns
        selection_df['Weight_group_1'] = 0.0
        selection_df['Weight_group_2'] = 0.0
        selection_df['Weight_FFMC'] = 0.0
        
        # Calculate base FFMC weight (uncapped)
        total_portfolio_mcap = selection_df['Original market cap'].sum()
        selection_df['Weight_FFMC'] = selection_df['Original market cap'] / total_portfolio_mcap
        
        # Process Group 1 (EZ300) - Calculate weights within group, capped at 16.67% within group
        group1_mask = selection_df['category'] == 'Group_1_EZ300'
        if group1_mask.sum() > 0:
            logger.info(f"Processing Group 1 (EZ300) with {group1_mask.sum()} companies...")
            group1_df = selection_df[group1_mask].copy()
            
            # Calculate group market cap
            group1_mcap = group1_df['Original market cap'].sum()
            
            # Max weight is 10% of TOTAL portfolio, which is 10%/60% = 16.67% within Group 1
            # Use round() to avoid floating point precision issues
            max_weight_within_group1 = round(0.1 / 0.6, 14)
            logger.info(f"Group 1 max allowed weight within group: {max_weight_within_group1:.14f}")
            
            # Apply capping within the group (weights sum to 1.0 within group)
            group1_df = apply_proportional_capping(
                group1_df, 
                mcap_column='Original market cap', 
                max_weight=max_weight_within_group1,
                max_iterations=200
            )
            
            # Store the capped within-group weights
            selection_df.loc[group1_mask, 'Weight_group_1'] = group1_df['Current Weight'].values
            
            group1_weight_sum = group1_df['Current Weight'].sum()
            group1_max_weight = group1_df['Current Weight'].max()
            logger.info(f"Group 1 weight sum within group: {group1_weight_sum:.6f}")
            logger.info(f"Group 1 max weight within group: {group1_max_weight:.6f}")
        
        # Process Group 2 (NA500) - Calculate weights within group, capped at 25% within group
        group2_mask = selection_df['category'] == 'Group_2_NA500'
        if group2_mask.sum() > 0:
            logger.info(f"Processing Group 2 (NA500) with {group2_mask.sum()} companies...")
            group2_df = selection_df[group2_mask].copy()
            
            # Calculate group market cap
            group2_mcap = group2_df['Original market cap'].sum()
            
            # Max weight is 10% of TOTAL portfolio, which is 10%/40% = 25% within Group 2
            max_weight_within_group2 = round(0.1 / 0.4, 14)  # 0.25 is exact, but use round for consistency
            logger.info(f"Group 2 max allowed weight within group: {max_weight_within_group2:.14f}")
            
            # Apply capping within the group (weights sum to 1.0 within group)
            group2_df = apply_proportional_capping(
                group2_df, 
                mcap_column='Original market cap', 
                max_weight=max_weight_within_group2,
                max_iterations=200
            )
            
            # Store the capped within-group weights
            selection_df.loc[group2_mask, 'Weight_group_2'] = group2_df['Current Weight'].values
            
            group2_weight_sum = group2_df['Current Weight'].sum()
            group2_max_weight = group2_df['Current Weight'].max()
            logger.info(f"Group 2 weight sum within group: {group2_weight_sum:.6f}")
            logger.info(f"Group 2 max weight within group: {group2_max_weight:.6f}")
        
        # Scale the within-group weights to portfolio allocation (60% and 40%)
        selection_df['Weight_group_1'] = selection_df['Weight_group_1'] * 0.60
        selection_df['Weight_group_2'] = selection_df['Weight_group_2'] * 0.40
        
        # Final weight is the maximum (since each stock is only in one group, the other will be 0)
        selection_df['Final Weight'] = selection_df[['Weight_group_1', 'Weight_group_2']].max(axis=1)
        
        # Calculate capping factor: ratio of final weight to uncapped FFMC weight
        selection_df['Weight_delta'] = selection_df['Final Weight'] / selection_df['Weight_FFMC']
        
        # Normalize capping factor so maximum = 1
        max_weight_delta = selection_df['Weight_delta'].max()
        selection_df['Final Capping'] = (selection_df['Weight_delta'] / max_weight_delta).round(14)
        
        # Log final verification
        group1_final_weight = selection_df[selection_df['category'] == 'Group_1_EZ300']['Final Weight'].sum()
        group2_final_weight = selection_df[selection_df['category'] == 'Group_2_NA500']['Final Weight'].sum()
        total_weight = selection_df['Final Weight'].sum()
        max_weight = selection_df['Final Weight'].max()
        
        logger.info(f"\nFinal Weight Verification:")
        logger.info(f"Group 1 (EZ300) portfolio weight: {group1_final_weight:.4%} (target: 60.00%)")
        logger.info(f"Group 2 (NA500) portfolio weight: {group2_final_weight:.4%} (target: 40.00%)")
        logger.info(f"Total portfolio weight: {total_weight:.6f} (target: 1.000000)")
        logger.info(f"Maximum constituent weight: {max_weight:.4%} (max allowed: 10.00%)")
        logger.info(f"Maximum weight delta before normalization: {max_weight_delta:.6f}")

        # Add effective date
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
            'ISIN': 'ISIN Code',
            'NOSH': 'Number of Shares',
        })
        GICP_df = GICP_df.sort_values('Company')
        
        # Perform inclusion/exclusion analysis
        analysis_results = inclusion_exclusion_analysis(
            selection_df, 
            stock_eod_df, 
            index, 
            isin_column='ISIN'
        )

        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']
        
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