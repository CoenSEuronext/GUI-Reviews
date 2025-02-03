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

def run_aerdp_review(date, co_date, effective_date, index="AERDP", isin="NLIX00003086", 
                    area="US", area2="EU", type="STOCK", universe="Developed Market", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "AERDP"
        isin (str, optional): ISIN code. Defaults to "NLIX00003086"
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
        ref_data = load_reference_data(
            current_data_folder,
            ['ff', 'developed_market', 'icb'] 
        )
        
        # Extract the needed DataFrames
        ff_df = ref_data['ff']
        developed_market_df = ref_data['developed_market']
        icb_df = ref_data = ref_data['icb']
        
                
        if any(df is None for df in [ff_df, icb_df, developed_market_df]):
            raise ValueError("Failed to load one or more required reference data files")

        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')


        # Merge with the filtered symbols
        developed_market_df = developed_market_df.merge(
            symbols_filtered,
            left_on='ISIN',
            right_on='Isin Code', 
            how='left'
        ).drop('Isin Code', axis=1)

        developed_market_df = developed_market_df.merge(
            stock_eod_df[['#Symbol', 'FX/Index Ccy']].drop_duplicates(subset='#Symbol', keep='first'),
            left_on='#Symbol', 
            right_on='#Symbol', 
            how='left'
        )

        developed_market_df = developed_market_df.merge(
            stock_eod_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
            left_on='#Symbol', 
            right_on='#Symbol', 
            how='left',
            suffixes=('', '_EOD')
        )

        # Merge CO Close Price
        developed_market_df = developed_market_df.merge(
            stock_co_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
            left_on='#Symbol', 
            right_on='#Symbol', 
            how='left',
            suffixes=('_EOD', '_CO')
        )

        
        ff_df = ff_df.drop_duplicates(subset=['ISIN Code:'], keep='first')

        # Add Free Float data
        developed_market_df = developed_market_df.merge(
            ff_df[['ISIN Code:', 'Free Float Round:']],
            left_on='ISIN',
            right_on='ISIN Code:',
            how='left'
        ).drop('ISIN Code:', axis=1).rename(columns={'Free Float Round:': 'Free Float'})
        
        icb_codes_stock = stock_eod_df[['Isin Code', 'ICBCode', 'MIC']].drop_duplicates(subset=['Isin Code', 'MIC'], keep='first')
        icb_codes_icb = icb_df[['ISIN Code', 'Subsector Code', 'MIC Code']].drop_duplicates(subset=['ISIN Code', 'MIC Code'], keep='first')

        # Do the merge
        developed_market_df = developed_market_df.merge(
            icb_codes_stock,
            left_on=['ISIN', 'MIC'],
            right_on=['Isin Code', 'MIC'],
            how='left'
        ).drop('Isin Code', axis=1)
        
        # eurozone_300_df = eurozone_300_df.merge(
        #     icb_codes_icb,
        #     left_on=['ISIN', 'MIC'],
        #     right_on=['ISIN Code', 'MIC Code'], 
        #     how='left'
        # ).drop('ISIN Code', axis=1)
        

        
        # Create Sector column from first 6 characters of Subsector Code
        developed_market_df['Sector'] = developed_market_df['ICBCode'].astype(str).str[:6]
        
        # Create Inclusion column based on both criteria
        developed_market_df['Inclusion'] = (
            (developed_market_df['Sector'] == '502010') & 
            (developed_market_df['3 months aver. Turnover EUR'] > 4000000)
        )
        
        developed_market_df['Close Prc_CO'] = developed_market_df['Close Prc_CO'].fillna(0)
        developed_market_df['FFMC CO'] = developed_market_df['NOSH'] * developed_market_df['Free Float'] * developed_market_df['Close Prc_EOD'] * developed_market_df['FX/Index Ccy']
        developed_market_df['Effective Date of Review'] = effective_date
        developed_market_df['Final Capping'] = 1
        # Only rank included companies
        developed_market_df['Rank Selection'] = np.nan
        developed_market_df.loc[developed_market_df['Inclusion'], 'Rank Selection'] = \
            developed_market_df.loc[developed_market_df['Inclusion'], 'FFMC CO'].rank(ascending=False, method='first')

        # Find index market cap
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == 'NLIX00003086', 'Mkt Cap'].iloc[0]
        total_ffmc = developed_market_df.loc[developed_market_df['Inclusion'], 'FFMC CO'].sum()
                
        # Initialize weight column for all components
        developed_market_df['Weight'] = 0.0

        # Calculate initial weights only for included companies
        included_mask = developed_market_df['Inclusion']

        def calculate_initial_weights(df, mask):
            """Calculate initial weights based on FFMC proportions for included companies"""
            total_ffmc = df.loc[mask, 'FFMC CO'].sum()
            df.loc[mask, 'Weight'] = df.loc[mask, 'FFMC CO'] / total_ffmc * 100
            return df

        def redistribute_excess_weight(df, mask, capped_companies, cap_level=5.0):
            """Redistribute excess weight above cap proportionally by FFMC to uncapped companies"""
            # Only work with included companies
            included_df = df[mask].copy()
            
            # Identify companies above cap (that aren't already capped)
            above_cap = (included_df['Weight'] > cap_level) & ~included_df.index.isin(capped_companies)
            
            if not any(above_cap):
                return df, capped_companies
            
            # Calculate excess weight
            excess = sum(included_df.loc[above_cap, 'Weight'] - cap_level)
            
            # Cap the companies above threshold
            included_df.loc[above_cap, 'Weight'] = cap_level
            capped_companies = capped_companies.union(included_df[above_cap].index)
            
            # Redistribute excess only to companies that aren't capped
            available_mask = ~included_df.index.isin(capped_companies)
            if any(available_mask):
                ffmc_available = included_df.loc[available_mask, 'FFMC CO']
                included_df.loc[available_mask, 'Weight'] += excess * (ffmc_available / ffmc_available.sum())
            
            # Update the original dataframe
            df.loc[included_df.index, 'Weight'] = included_df['Weight']
            
            return df, capped_companies

        # Initial weight calculation
        developed_market_df = calculate_initial_weights(developed_market_df, included_mask)

        # Keep track of companies that have been capped
        capped_companies = set()

        # Perform four rounds of 5% capping
        for round in range(4):
            developed_market_df, capped_companies = redistribute_excess_weight(
                developed_market_df, 
                included_mask, 
                capped_companies, 
                cap_level=5.0
            )

        # Calculate final capping factors
        developed_market_df['Final Capping'] = 1.0
        developed_market_df.loc[included_mask, 'Final Capping'] = \
            developed_market_df.loc[included_mask, 'Weight'] / \
            (developed_market_df.loc[included_mask, 'FFMC CO'] / total_ffmc * 100)

        # Normalize capping factors
        max_included_capping = developed_market_df.loc[included_mask, 'Final Capping'].max()
        developed_market_df.loc[included_mask, 'Final Capping'] = \
            developed_market_df.loc[included_mask, 'Final Capping'] / max_included_capping
        
        # Create selection_df from eligible companies
        selection_df = developed_market_df[developed_market_df['Inclusion']].copy()

         

        
        AERDP_df = selection_df[
            ['Name', 'ISIN', 'MIC', 'NOSH', 
            'Free Float', 'Final Capping', 
            'Effective Date of Review', 'Currency (Local)']
        ]

        AERDP_df = AERDP_df.sort_values('Name')

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
            
            # Create filename with timestamp to avoid conflicts
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            aerdp_path = os.path.join(output_dir, f'AERDP_df_{timestamp}.xlsx')
            
            # Save with timestamp to avoid any conflicts
            logger.info(f"Saving AERDP output to: {aerdp_path}")
            with pd.ExcelWriter(aerdp_path) as writer:
                    # Write each DataFrame to a different sheet
                    AERDP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                    inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                    exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                    developed_market_df.to_excel(writer, sheet_name='Full Universe', index=False)
                    index_mcap_df = pd.DataFrame({'Index Market Cap': [index_mcap]})
                    index_mcap_df.to_excel(writer, sheet_name='Index Market Cap', index=False)

                
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "aerdp_path": aerdp_path
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