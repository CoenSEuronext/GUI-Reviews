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

def run_efesp_review(date, co_date, effective_date, index="EFESP", isin="NLIX00006584", 
                   area="US", area2="EU", type="STOCK", universe="deupt", 
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
            [universe, 'icb', 'ff']
        )
        selection_df = ref_data[universe]
        ff_df = ref_data['ff']
        icb_df = ref_data['icb']
        
        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        # Chain all data preparation operations
        selection_df = (ref_data[universe]
           # Initial renaming
           .rename(columns={
               'NOSH': 'Number of Shares',
               'ISIN': 'ISIN code',
               'Name': 'Company',
               'Currency (Local)': 'Currency',
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
            # Merge Free Float data
            .merge(
                ff_df[['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset='ISIN Code:', keep='first'),
                left_on='ISIN code',
                right_on='ISIN Code:',
                how='left'
            )
            .drop('ISIN Code:', axis=1)
            .rename(columns={'Free Float Round:': 'Free Float'})
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
        
        # Find index market cap
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        
        # Calculate FFMC (Free Float Market Cap)
        selection_df['FFMC'] = selection_df['Number of Shares'] * selection_df['Free Float'] * selection_df['Price (EUR) ']
        
        # Updated exclusion logic for EFESP index
        # Replace the existing ICB and MIC exclusion logic with this:

        # Define the allowed ICB subsector codes (first 6 characters)
        allowed_icb_codes = ['501010', '502010', '502040', '502020', '101010', '502060']

        # ICB inclusion logic - keep only the specified subsectors
        selection_df['inclusion_icb'] = selection_df['Subsector Code'].astype(str).str[:6].isin(allowed_icb_codes)

        # Turnover filter - keep only companies with 3-month average turnover > 10M EUR
        selection_df['inclusion_turnover'] = selection_df['3M AVG Turnover EUR'] > 10000000

        # Remove the old MIC exclusion logic completely
        # selection_df['exclusion_mic'] = ~selection_df['MIC'].isin(['XLIS', 'XTAE'])  # Remove this line

        # Updated eligible companies filter - must pass BOTH ICB and turnover filters
        logger.info("Creating eligible companies dataframe...")
        eligible_df = selection_df[
            (selection_df['inclusion_icb'] == True) & 
            (selection_df['inclusion_turnover'] == True)
        ].copy()
               
        
        eligible_df['FFMC_WD'] = eligible_df['Close Prc_EOD'] * eligible_df['Number of Shares'] * eligible_df['Free Float'] * eligible_df['FX/Index Ccy']
        # Add this code after calculating FFMC_WD and before calculating total_eligible_ffmc

        # Create ICB code to group mapping
        icb_to_group = {
            '501010': 1,  # Construction and Materials
            '502010': 2,  # Aerospace and Defense
            '502040': 3,  # Industrial Engineering
            '502020': 4,  # Electronic and Electrical Equipment
            '101010': 5,  # Software and Computer Services
            '502060': 6   # Industrial Transportation
        }

        # Add Group column based on ICB subsector codes
        eligible_df['Group'] = eligible_df['Subsector Code'].astype(str).str[:6].map(icb_to_group)

        # Add Rank column based on FFMC (higher FFMC = lower rank number)
        eligible_df['Rank'] = eligible_df['FFMC_WD'].rank(method='dense', ascending=False).astype(int)

        # Add Selection column - select top 5 companies per group based on FFMC_WD
        eligible_df['Rank_within_Group'] = eligible_df.groupby('Group')['FFMC_WD'].rank(method='dense', ascending=False).astype(int)
        eligible_df['Selection'] = (eligible_df['Rank_within_Group'] <= 5).astype(int)

        # Optional: Log selection summary
        for group in sorted(icb_to_group.values()):
            group_companies = eligible_df[eligible_df['Group'] == group]
            selected_companies = group_companies[group_companies['Selection'] == 1]
            icb_code = [k for k, v in icb_to_group.items() if v == group][0]
            logger.info(f"Group {group} (ICB {icb_code}): {len(selected_companies)} selected out of {len(group_companies)} companies")

        # Add Effective Date to all eligible companies
        eligible_df['Effective Date of Review'] = effective_date

        # Filter to get only selected companies for weight calculations
        selected_df = eligible_df[eligible_df['Selection'] == 1].copy()

        # Calculate total FFMC for SELECTED companies only
        total_selected_ffmc = selected_df['FFMC_WD'].sum()

        # Calculate initial weights and apply 10% capping for SELECTED companies only
        initial_weights = selected_df['FFMC_WD'] / total_selected_ffmc
        capped_weights = calculate_capped_weights(initial_weights, cap_limit=0.1)

        # Calculate raw capping factors
        raw_capping_factors = capped_weights / initial_weights

        # Normalize capping factors so the maximum becomes 1
        max_capping_factor = raw_capping_factors.max()
        normalized_capping_factors = raw_capping_factors / max_capping_factor

        # Update capping factors and weights for SELECTED companies only
        selected_df['Capping Factor'] = normalized_capping_factors.round(14)
        selected_df['Weight'] = capped_weights

        # For non-selected companies, set capping factor and weight to 0 or NaN
        eligible_df.loc[eligible_df['Selection'] == 0, 'Capping Factor'] = 0
        eligible_df.loc[eligible_df['Selection'] == 0, 'Weight'] = 0

        # Update the selected companies' data back to the main eligible_df
        eligible_df.update(selected_df[['Capping Factor', 'Weight']])

        logger.info(f"Total eligible companies: {len(eligible_df)}")
        logger.info(f"Total selected companies: {len(selected_df)}")
        logger.info(f"Total selected FFMC: {total_selected_ffmc:,.2f}")
        logger.info(f"Weight sum check for selected companies: {selected_df['Weight'].sum():.6f}")

        # Prepare EFESP DataFrame
        EFESP_df = (
            eligible_df[eligible_df['Selection'] == 1][  # Filter for selected companies only
                ['Company', 'ISIN code', 'MIC', 'Number of Shares', 'Free Float', 'Capping Factor', 
                'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'ISIN code': 'ISIN Code'})
            .sort_values('Company')
        )

        # Perform Inclusion/Exclusion Analysis
        analysis_results = inclusion_exclusion_analysis(
            eligible_df, 
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
            efesp_path = os.path.join(output_dir, f'EFESP_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving EFESP output to: {efesp_path}")
            with pd.ExcelWriter(efesp_path) as writer:
                EFESP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                eligible_df.to_excel(writer, sheet_name='Eligible Companies', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "efesp_path": efesp_path
                }
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