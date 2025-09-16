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

def run_euadp_review(date, co_date, effective_date, index="EUADP", isin="NL0012949143", 
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
            [universe, 'ff', 'icb']
        )
        selection_df = ref_data[universe]
        ff_df = ref_data['ff']
        icb_df = ref_data['icb']

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')
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
        selection_df["Free Float"] = selection_df["Free Float Round:"].fillna(selection_df.get("Free Float", 0))
        selection_df['FFMC'] = selection_df['Number of Shares'] * selection_df['Free Float'] * selection_df['Price (EUR) ']
        selection_df['FFMC_CO'] = selection_df['Number of Shares'] * selection_df['Free Float'] * selection_df['Close Prc_CO'] * selection_df['FX/Index Ccy']
        selection_df['FFMC_EOD'] = selection_df['Number of Shares'] * selection_df['Free Float'] * selection_df['Close Prc_EOD'] * selection_df['FX/Index Ccy']
        
        # Add exclusion filters with separate columns
        selection_df['FFMC_Exclusion'] = selection_df['FFMC'] < 2000000000  # Below €2B
        selection_df['Turnover_Exclusion'] = selection_df['3M AVG Turnover EUR'] < 1000000  # Below €1M
        selection_df['ICB_Exclusion'] = selection_df['Subsector Code'].astype(str).str[:6] != '502010'
        
        # Create combined exclusion flag
        selection_df['Is_Excluded'] = (selection_df['FFMC_Exclusion'] | 
                                     selection_df['Turnover_Exclusion'] | 
                                     selection_df['ICB_Exclusion'])
        
        # Create individual exclusion reason columns
        selection_df['FFMC_Exclusion_Reason'] = selection_df['FFMC_Exclusion'].apply(
            lambda x: 'Excluded: FFMC < €2B' if x else ''
        )
        selection_df['Turnover_Exclusion_Reason'] = selection_df['Turnover_Exclusion'].apply(
            lambda x: 'Excluded: 100 days avg turnover < €1M' if x else ''
        )
        selection_df['ICB_Exclusion_Reason'] = selection_df['ICB_Exclusion'].apply(
            lambda x: 'Excluded: ICB Subsector Code not 502010' if x else ''
        )
        # Create summary exclusion reason column
        selection_df['All_Exclusion_Reasons'] = selection_df.apply(
            lambda row: '; '.join(filter(None, [
                row['FFMC_Exclusion_Reason'], 
                row['Turnover_Exclusion_Reason'],
                row['ICB_Exclusion_Reason']
            ])),
            axis=1
        )
        
        # Clean up temporary exclusion columns
        selection_df = selection_df.drop(columns=['FFMC_Exclusion', 'Turnover_Exclusion', 'ICB_Exclusion'])
        
        # Calculate initial weights for non-excluded companies
        logger.info("Calculating initial weights...")
        non_excluded_df = selection_df[~selection_df['Is_Excluded']].copy()
        
        if len(non_excluded_df) == 0:
            raise ValueError("No companies remaining after applying exclusion criteria")
        
        # Calculate total FFMC of non-excluded companies
        total_ffmc_non_excluded = non_excluded_df['FFMC_EOD'].sum()
        logger.info(f"Total FFMC of non-excluded companies: €{total_ffmc_non_excluded:,.0f}")
        
        # Calculate initial weights (FFMC / Total FFMC)
        initial_weights = non_excluded_df['FFMC_EOD'] / total_ffmc_non_excluded
        
        # Apply capping to weights
        logger.info("Applying weight capping...")
        capped_weights = calculate_capped_weights(initial_weights, cap_limit=0.1)
        
        # Calculate capping factors for non-excluded companies
        capping_factors = capped_weights / initial_weights
        
        # Verify weights sum to 1
        total_capped_weight = capped_weights.sum()
        logger.info(f"Total capped weights sum: {total_capped_weight:.6f}")
        
        if abs(total_capped_weight - 1.0) > 1e-6:
            logger.warning(f"Capped weights do not sum to 1.0: {total_capped_weight}")
        
        # Initialize weight columns for all companies in selection_df
        selection_df['Initial_Weight'] = 0.0
        selection_df['Capped_Weight'] = 0.0
        selection_df['Capping_Factor'] = 0.0
        
        # Set weights for non-excluded companies
        non_excluded_mask = ~selection_df['Is_Excluded']
        selection_df.loc[non_excluded_mask, 'Initial_Weight'] = initial_weights
        selection_df.loc[non_excluded_mask, 'Capped_Weight'] = capped_weights
        selection_df.loc[non_excluded_mask, 'Capping_Factor'] = capping_factors
        
        # Calculate Final_Capping = Capping_Factor / max(Capping_Factor)
        max_capping_factor = selection_df['Capping_Factor'].max()
        selection_df['Final_Capping'] = (selection_df['Capping_Factor'] / max_capping_factor).round(14) if max_capping_factor > 0 else 0
        
        # Get top 50 companies by capped weight
        top_50_df = selection_df[~selection_df['Is_Excluded']].copy()
        
        logger.info(f"Selected top 50 companies")
        
        # Prepare EUADP DataFrame from selection_df (top 50 non-excluded companies)
        EUADP_df = (
            top_50_df[
                ['Company', 'ISIN code', 'MIC', 'Number of Shares', 'Free Float', 'Final_Capping', 
                'Effective Date of Review', 'Currency (Local)']
            ]
            .rename(columns={'ISIN code': 'ISIN Code', 'Final_Capping': 'Capping Factor', 'Currency (Local)': 'Currency'})
            .sort_values('Company')
            .reset_index(drop=True)
        )

        # Perform Inclusion/Exclusion Analysis
        logger.info("Performing inclusion/exclusion analysis...")
        analysis_results = inclusion_exclusion_analysis(
            selection_df, 
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
            euadp_path = os.path.join(output_dir, f'EUADP_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving EUADP output to: {euadp_path}")
            with pd.ExcelWriter(euadp_path) as writer:
                EUADP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                top_50_df.to_excel(writer, sheet_name='Selection', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            logger.info("EUADP review completed successfully")
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "euadp_path": euadp_path,
                    "total_companies": len(selection_df),
                    "non_excluded_companies": len(non_excluded_df),
                    "top_50_companies": len(top_50_df),
                    "total_ffmc": total_ffmc_non_excluded
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