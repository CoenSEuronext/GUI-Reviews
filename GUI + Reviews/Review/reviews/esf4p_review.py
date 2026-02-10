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

def run_esf4p_review(date, co_date, effective_date, index="ESF4P", isin="FR0013317732", 
                   area="US", area2="EU", type="STOCK", universe="cac_family", 
                   feed="Reuters", currency="EUR", year=None):
    """
    Euronext France ESG Leaders 40 EW Index Review
    
    Methodology:
    - Universe: CAC Family index
    - Selection: Top 40 companies by ESG Risk Score (lower is better)
    - Tie-breaker: Highest free float market capitalisation ranks higher
    - Weighting: Equal Weight
    - Free Float Factor: Not applied (1)
    - Capping Factor: Not applied (1)
    """

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
            ['cac_family', 'ff', 'sustainalytics'],
            sheet_names={'cac_family': 'CACLG'}
        )

        # Step 1: Index Universe - CAC Large 60
        cac_large_df = ref_data.get('cac_family')
        if cac_large_df is None:
            raise ValueError("Failed to load CAC Large 60 universe data")

        universe_df = pd.DataFrame(cac_large_df)
        ff_df = ref_data['ff']

        logger.info(f"Starting universe size: {len(universe_df)}")
        
        # Add the required columns
        universe_df['Effective Date of Review'] = effective_date

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')
        
        # Chain all data preparation operations
        selection_df = (universe_df
           .rename(columns={
               'Number of shares': 'Number of Shares',
               'ISIN': 'ISIN code',
               'Name': 'Company',
            })
            .merge(
                symbols_filtered,
                left_on='ISIN code',
                right_on='Isin Code',
                how='left'
            )
            .drop('Isin Code', axis=1)
            .merge(
                stock_eod_df[stock_eod_df['Index Curr'] == currency][['#Symbol', 'FX/Index Ccy']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left'
            )
            .merge(
                stock_eod_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left',
                suffixes=('', '_EOD')
            )
            .merge(
                stock_co_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left',
                suffixes=('_EOD', '_CO')
            )
            .merge(
                ff_df[['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset='ISIN Code:', keep='first'),
                left_on='ISIN code',
                right_on='ISIN Code:',
                how='left'
            )
            .drop('ISIN Code:', axis=1)
        )
        
        # Use the merged Free Float Round value
        selection_df["Free Float"] = selection_df["Free Float Round:"]
        selection_df['Currency'] = currency
        # Calculate FFMC for tie-breaking
        selection_df['FFMC'] = selection_df['Free Float Round:'] * selection_df['Number of Shares'] * selection_df["Close Prc_EOD"] * selection_df['FX/Index Ccy']

        # Step 2: No additional eligibility screening per rulebook
        logger.info("Step 2: No additional eligibility screening required")
        
        # Step 3: Merge Sustainalytics data for ESG Risk Score
        logger.info("Merging Sustainalytics data...")
        
        sustainalytics_raw = ref_data.get('sustainalytics')

        if sustainalytics_raw is None:
            raise ValueError("Sustainalytics data required for ESF4P index")
        
        # ESG Risk Score code
        esg_risk_score_code = '181110112399'
        
        # Get the first row which contains the codes
        codes_row = sustainalytics_raw.iloc[0].copy()
        
        # Convert all numeric values in row 1 to integers (as strings)
        for col in codes_row.index:
            if col != 'ISIN':
                try:
                    if pd.notna(codes_row[col]):
                        codes_row[col] = str(int(float(codes_row[col])))
                except (ValueError, TypeError):
                    codes_row[col] = str(codes_row[col]).strip()
        
        # Find the ESG Risk Score column
        cols_to_keep = ['ISIN']
        col_name_mapping = {}
        
        for col_name in sustainalytics_raw.columns:
            if col_name != 'ISIN':
                cell_value = codes_row[col_name]
                if cell_value == esg_risk_score_code:
                    cols_to_keep.append(col_name)
                    col_name_mapping[col_name] = cell_value
        
        if len(cols_to_keep) <= 1:
            raise ValueError("ESG Risk Score (181110112399) not found in Sustainalytics data")
        
        # Take only the columns we need and skip the first row (which has codes)
        sustainalytics_filtered = sustainalytics_raw[cols_to_keep].iloc[1:].copy()
        
        # Rename columns to include code
        rename_dict = {}
        for col in cols_to_keep:
            if col != 'ISIN':
                code = col_name_mapping[col]
                original_header = col
                rename_dict[col] = f"{code} - {original_header}"
        
        sustainalytics_filtered.rename(columns=rename_dict, inplace=True)
        sustainalytics_filtered = sustainalytics_filtered.drop_duplicates(subset='ISIN', keep='first')
        
        # Merge with selection_df
        selection_df = selection_df.merge(
            sustainalytics_filtered,
            left_on='ISIN code',
            right_on='ISIN',
            how='left'
        ).drop('ISIN', axis=1, errors='ignore')
        
        logger.info("Sustainalytics ESG Risk Score merge completed.")
        
        # Helper function to find column by code
        def find_column_by_code(df, code):
            for col in df.columns:
                if col.startswith(f"{code} - "):
                    return col
            return None
        
        # Step 3: Selection Ranking by ESG Risk Score
        logger.info("Step 3: Ranking by ESG Risk Score (lower is better)...")
        
        col_esg_risk = find_column_by_code(selection_df, esg_risk_score_code)
        
        # Convert to numeric
        selection_df['ESG_Risk_Score'] = pd.to_numeric(selection_df[col_esg_risk], errors='coerce')
        
        # Filter out companies without ESG Risk Score
        eligible_df = selection_df[selection_df['ESG_Risk_Score'].notna()].copy()
        logger.info(f"Companies with ESG Risk Score: {len(eligible_df)}")
        
        # Rank by ESG Risk Score (ascending = lower score is better)
        # Ties broken by highest FFMC (descending)
        eligible_df = eligible_df.sort_values(['ESG_Risk_Score', 'FFMC'], ascending=[True, False])
        eligible_df['ESG_Risk_Rank'] = range(1, len(eligible_df) + 1)
        
        logger.info(f"ESG Risk Score ranking completed for {len(eligible_df)} companies")
        
        # Step 4: Select top 40 companies
        logger.info("Step 4: Selecting top 40 companies with lowest ESG Risk Score...")
        
        top_40_df = eligible_df.head(40).copy()
        logger.info(f"Selected {len(top_40_df)} companies for index")
        
        # Get index market cap
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        
        # Calculate Equal Weight Number of Shares
        num_constituents = len(top_40_df)
        target_mcap_per_company = index_mcap / num_constituents
        
        top_40_df['Unrounded NOSH'] = target_mcap_per_company / (top_40_df['Close Prc_EOD'] * top_40_df['FX/Index Ccy'])
        top_40_df['Rounded NOSH'] = top_40_df['Unrounded NOSH'].round()
        
        # Set Free Float and Capping Factor to 1 (not applied per rulebook)
        top_40_df['Free Float'] = 1
        top_40_df['Capping Factor'] = 1
        top_40_df
        # Prepare ESF4P DataFrame
        ESF4P_df = (
            top_40_df[
                ['Company', 'ISIN code', 'MIC', 'Rounded NOSH', 'Free Float', 'Capping Factor', 
                'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'Rounded NOSH': 'Number of Shares'})
            .rename(columns={'ISIN code': 'ISIN Code'})
            .sort_values('Company')
        )

        # Perform Inclusion/Exclusion Analysis
        analysis_results = inclusion_exclusion_analysis(
            top_40_df, 
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
           
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            esf4p_path = os.path.join(output_dir, f'ESF4P_df_{timestamp}.xlsx')
           
            logger.info(f"Saving ESF4P output to: {esf4p_path}")
            with pd.ExcelWriter(esf4p_path) as writer:
                ESF4P_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                top_40_df.to_excel(writer, sheet_name='Top 40 Selection', index=False)
                eligible_df.to_excel(writer, sheet_name='ESG Ranked Universe', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"esf4p_path": esf4p_path}
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