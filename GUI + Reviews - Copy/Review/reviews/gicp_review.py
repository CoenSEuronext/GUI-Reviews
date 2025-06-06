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
        # Add EU500 exclusion
        developed_market_df['exclusion_1'] = None
        developed_market_df['exclusion_1'] = np.where(
            ~(developed_market_df['index'].str.contains('EZ300', na=False) | 
            developed_market_df['index'].str.contains('NA500', na=False)),
            'exclude_Area',
            None
        )
        
        # Similarly convert Subsector Code to string
        developed_market_df['Subsector Code'] = developed_market_df['Subsector Code'].astype(str)

        # Add eligibility categories exclusion
        developed_market_df['exclusion_2'] = None

        # Create the eligibility mask for each category
        category_conditions = (
            # Category 1: XPAR + Banks Super Sector (3010)
            ((developed_market_df['MIC'] == 'XPAR') & 
            (developed_market_df['Subsector Code'].str[:4] == '3010')) |
            
            # Category 2: XPAR + Clothing and Accessories Subsector (40204020)
            ((developed_market_df['MIC'] == 'XPAR') & 
            (developed_market_df['Subsector Code'].str[:8] == '40204020')) |
            
            # Category 3: XETR + Automobiles and Parts Super Sector (4010)
            ((developed_market_df['MIC'] == 'XETR') & 
            (developed_market_df['Subsector Code'].str[:4] == '4010')) |
            
            # Category 4: XETR + Industrial Goods and Services Super Sector (5020)
            ((developed_market_df['MIC'] == 'XETR') & 
            (developed_market_df['Subsector Code'].str[:4] == '5020')) |
            
            # Category 5: XAMS + Technology Hardware and Equipment Super Sector (101020)
            ((developed_market_df['MIC'] == 'XAMS') & 
            (developed_market_df['Subsector Code'].str[:6] == '101020')) |
            
            # Category 6: XNYS/XNGS + Technology Industry (10)
            ((developed_market_df['MIC'].isin(['XNYS', 'XNGS'])) & 
            (developed_market_df['Subsector Code'].str[:2] == '10')) |
            
            # Category 7: XNYS/XNGS + Consumer Discretionary Industry (40)
            ((developed_market_df['MIC'].isin(['XNYS', 'XNGS'])) & 
            (developed_market_df['Subsector Code'].str[:2] == '40'))
        )

        # Apply the exclusion for companies that don't meet any category criteria
        developed_market_df['exclusion_2'] = np.where(
            ~category_conditions,
            'exclude_category',
            None
        )
        # Create a mask for non-excluded companies
        non_excluded_mask = (
            developed_market_df['exclusion_1'].isna() & 
            developed_market_df['exclusion_2'].isna()
        )


        # Add rank for non-excluded companies based on FFMC in EUR
        developed_market_df['rank'] = None
        developed_market_df.loc[non_excluded_mask, 'rank'] = (
            developed_market_df.loc[non_excluded_mask, 'FFMC']
            .rank(method='first', ascending=False)
        )


        
        # Define the category conditions and rank within each category
        category_definitions = [
            # Category 1: XPAR + Banks Super Sector (3010)
            {
                'mask': (
                    (developed_market_df['MIC'] == 'XPAR') & 
                    (developed_market_df['Subsector Code'].str[:4] == '3010') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_1'
            },
            # Category 2: XPAR + Clothing and Accessories Subsector (40204020)
            {
                'mask': (
                    (developed_market_df['MIC'] == 'XPAR') & 
                    (developed_market_df['Subsector Code'].str[:8] == '40204020') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_2'
            },
            # Category 3: XETR + Automobiles and Parts Super Sector (4010)
            {
                'mask': (
                    (developed_market_df['MIC'] == 'XETR') & 
                    (developed_market_df['Subsector Code'].str[:4] == '4010') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_3'
            },
            # Category 4: XETR + Industrial Goods and Services Super Sector (5020)
            {
                'mask': (
                    (developed_market_df['MIC'] == 'XETR') & 
                    (developed_market_df['Subsector Code'].str[:4] == '5020') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_4'
            },
            # Category 5: XAMS + Technology Hardware and Equipment Super Sector (101020)
            {
                'mask': (
                    (developed_market_df['MIC'] == 'XAMS') & 
                    (developed_market_df['Subsector Code'].str[:6] == '101020') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_5'
            },
            # Category 6: XNYS/XNGS + Technology Industry (10)
            {
                'mask': (
                    (developed_market_df['MIC'].isin(['XNYS', 'XNGS'])) & 
                    (developed_market_df['Subsector Code'].str[:2] == '10') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_6'
            },
            # Category 7: XNYS/XNGS + Consumer Discretionary Industry (40)
            {
                'mask': (
                    (developed_market_df['MIC'].isin(['XNYS', 'XNGS'])) & 
                    (developed_market_df['Subsector Code'].str[:2] == '40') &
                    developed_market_df['exclusion_1'].isna()
                ),
                'name': 'Category_7'
            }
        ]

        # Add category column to identify which category each company belongs to
        developed_market_df['category'] = None

        # Rank within each category
        for category in category_definitions:
            mask = category['mask']
            # Assign category name
            developed_market_df.loc[mask, 'category'] = category['name']
            # Rank within category based on market cap
            developed_market_df.loc[mask, 'rank'] = (
                developed_market_df.loc[mask, 'FFMC']
                .rank(method='first', ascending=False)
            )
        # Define the number of companies to select from each category
        category_selections = {
            'Category_1': 3,  # XPAR Banks
            'Category_2': 3,  # XPAR Clothing and Accessories
            'Category_3': 3,  # XETR Automobiles and Parts
            'Category_4': 3,  # XETR Industrial Goods and Services
            'Category_5': 3,  # XAMS Technology Hardware and Equipment
            'Category_6': 10, # XNYS/XNGS Technology
            'Category_7': 5   # XNYS/XNGS Consumer Discretionary
        }

        # Create selection column
        developed_market_df['selected'] = False

        # Select top companies from each category
        for category, n_select in category_selections.items():
            category_mask = (developed_market_df['category'] == category)
            top_n_mask = (developed_market_df['rank'] <= n_select) & category_mask
            developed_market_df.loc[top_n_mask, 'selected'] = True

        # Verify total number of selected companies
        total_selected = developed_market_df['selected'].sum()
        if total_selected != 30:
            logger.warning(f"Selected {total_selected} companies instead of expected 30")

        # Create selection dataframe
        selection_df = developed_market_df[developed_market_df['selected']].copy()
        
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
        # Define category weights
        category_weights = {
            'Category_1': 0.12,  # 12%
            'Category_2': 0.12,
            'Category_3': 0.12,
            'Category_4': 0.12,
            'Category_5': 0.12,
            'Category_6': 0.20,  # 20%
            'Category_7': 0.20
        }

        def apply_category_capping(df, target_weight, total_mcap, max_weight=0.10):
            """
            Apply capping within a category
            
            Args:
                df: DataFrame containing the category's constituents
                target_weight: Target weight for the entire category (e.g., 0.12 for 12%)
                total_mcap: Total market cap of all selected companies
                max_weight: Maximum weight for any individual constituent (as % of total portfolio)
            """
            # Calculate initial weights relative to total portfolio
            df['Initial Weight'] = df['Original market cap'] / total_mcap
            
            # Calculate the scaling factor to achieve target category weight
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

        # Apply capping by category
        total_mcap = selection_df['Original market cap'].sum()
        selection_df['Initial Weight'] = 0.0

        for category, target_weight in category_weights.items():
            category_mask = selection_df['category'] == category
            category_df = selection_df[category_mask].copy()
            
            if not category_df.empty:
                # Apply category capping with total market cap
                category_df = apply_category_capping(category_df, target_weight, total_mcap)
                selection_df.loc[category_mask, 'Initial Weight'] = category_df['Initial Weight']

        # Verify category weights
        for category, target in category_weights.items():
            actual = selection_df[selection_df['category'] == category]['Initial Weight'].sum()
            logger.info(f"{category} weight: {actual:.4%} (target: {target:.4%})")

        # Calculate Final Capping factor
        selection_df['Final Capping'] = (selection_df['Initial Weight'] * total_mcap) / selection_df['Original market cap']

        # Verify final weights
        selection_df['Final Weight'] = (selection_df['Original market cap'] * selection_df['Final Capping']) / (selection_df['Original market cap'] * selection_df['Final Capping']).sum()

        # Log verification
        logger.info("\nFinal Weight Verification:")
        for category in category_weights:
            cat_weight = selection_df[selection_df['category'] == category]['Final Weight'].sum()
            logger.info(f"{category} final weight: {cat_weight:.4%}")

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