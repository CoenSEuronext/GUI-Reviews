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

def run_esf5p_review(date, co_date, effective_date, index="ESF5P", isin="FR0013371416",
                     area="US", area2="EU", type="STOCK", universe="sbf_120",
                     feed="Reuters", currency="EUR", year=None):
    """
    SBF Top 50 ESG EW Index Review

    Methodology:
    - Universe: SBF 120 index
    - Step 2: Rank all universe companies by free float market capitalisation
              (using Close Prc_CO). Top 80 are eligible.
    - Step 3: Rank the 80 eligible companies by Sustainalytics ESG Risk Score
              ascending. Tie-breaker: highest free float market capitalisation
              (using Close Prc_CO).
    - Step 4: Top 50 by ESG Risk Score ranking are selected.
    - Weighting: Equal Weight, Number of Shares calculated using Close Prc_EOD.
    - Free Float Factor: Not applied (set to 1).
    - Capping Factor: Not applied (set to 1).
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
            ['ff', 'sustainalytics', 'sbf_120']
        )

        # Validate data loading
        if ref_data.get('sbf_120') is None:
            raise ValueError("Failed to load sbf_120 universe data")

        # Step 1: Index Universe - SBF 120
        sbf_120_universe = ref_data['sbf_120']
        ff_df = ref_data['ff']

        universe_df = pd.DataFrame(sbf_120_universe)
        logger.info(f"Starting universe size: {len(universe_df)}")

        # Add effective date
        universe_df['Effective Date of Review'] = effective_date

        # Resolve symbols (keep short symbols only, one per ISIN)
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        # Build selection DataFrame with all required fields
        selection_df = (
            universe_df
            .rename(columns={
                'Number of shares': 'Number of Shares',
                'ISIN': 'ISIN code',
                'Name': 'Company',
            })
            # Attach symbol
            .merge(
                symbols_filtered,
                left_on='ISIN code',
                right_on='Isin Code',
                how='left'
            )
            .drop('Isin Code', axis=1)
            # Attach FX rate (index currency)
            .merge(
                stock_eod_df[stock_eod_df['Index Curr'] == currency][
                    ['#Symbol', 'FX/Index Ccy']
                ].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left'
            )
            # Attach EOD close price (used for equal-weight NOSH calculation)
            .merge(
                stock_eod_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left'
            )
            .rename(columns={'Close Prc': 'Close Prc_EOD'})
            # Attach cut-off close price (used for FFMC in eligibility and ESG tie-breaking)
            .merge(
                stock_co_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left'
            )
            .rename(columns={'Close Prc': 'Close Prc_CO'})
            # Attach free float
            .merge(
                ff_df[['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset='ISIN Code:', keep='first'),
                left_on='ISIN code',
                right_on='ISIN Code:',
                how='left'
            )
            .drop('ISIN Code:', axis=1)
        )

        selection_df['Currency'] = currency

        # FFMC using Close Prc_CO - used for eligibility screening and ESG tie-breaking
        selection_df['FFMC_CO'] = (
            selection_df['Free Float Round:']
            * selection_df['Number of Shares']
            * selection_df['Close Prc_CO']
            * selection_df['FX/Index Ccy']
        )

        # ----------------------------------------------------------------
        # Step 2: Eligibility screening - rank by FFMC_CO, keep top 80
        # ----------------------------------------------------------------
        logger.info("Step 2: Ranking universe by free float market cap (Close Prc_CO), keeping top 80...")

        ranked_by_ffmc = selection_df.sort_values('FFMC_CO', ascending=False).copy()
        ranked_by_ffmc['FFMC_Rank'] = range(1, len(ranked_by_ffmc) + 1)

        eligible_df = ranked_by_ffmc.head(80).copy()
        logger.info(f"Eligible companies after FFMC screen (top 80): {len(eligible_df)}")

        # ----------------------------------------------------------------
        # Step 3: Load Sustainalytics ESG Risk Score and rank eligible 80
        # ----------------------------------------------------------------
        logger.info("Loading Sustainalytics ESG Risk Score data...")

        sustainalytics_raw = ref_data.get('sustainalytics')
        if sustainalytics_raw is None:
            raise ValueError("Sustainalytics data required for ESF5P index")

        esg_risk_score_code = '181110112399'

        # Row 0 contains the column codes
        codes_row = sustainalytics_raw.iloc[0].copy()
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

        sustainalytics_filtered = sustainalytics_raw[cols_to_keep].iloc[1:].copy()

        rename_dict = {
            col: f"{col_name_mapping[col]} - {col}"
            for col in cols_to_keep
            if col != 'ISIN'
        }
        sustainalytics_filtered.rename(columns=rename_dict, inplace=True)
        sustainalytics_filtered = sustainalytics_filtered.drop_duplicates(subset='ISIN', keep='first')

        # Merge ESG scores into eligible universe
        eligible_df = eligible_df.merge(
            sustainalytics_filtered,
            left_on='ISIN code',
            right_on='ISIN',
            how='left'
        ).drop('ISIN', axis=1, errors='ignore')

        # Also merge into full selection_df for output completeness
        selection_df = selection_df.merge(
            sustainalytics_filtered,
            left_on='ISIN code',
            right_on='ISIN',
            how='left'
        ).drop('ISIN', axis=1, errors='ignore')

        logger.info("Sustainalytics ESG Risk Score merge completed.")

        # Helper to locate a column by its code prefix
        def find_column_by_code(df, code):
            for col in df.columns:
                if col.startswith(f"{code} - "):
                    return col
            return None

        col_esg_risk = find_column_by_code(eligible_df, esg_risk_score_code)
        if col_esg_risk is None:
            raise ValueError("Could not locate ESG Risk Score column after merge")

        eligible_df['ESG_Risk_Score'] = pd.to_numeric(eligible_df[col_esg_risk], errors='coerce')

        # Rank eligible 80 by ESG Risk Score ascending; tie-break by FFMC_CO descending
        logger.info("Step 3: Ranking eligible companies by ESG Risk Score (ascending)...")

        scored_eligible_df = eligible_df[eligible_df['ESG_Risk_Score'].notna()].copy()
        logger.info(f"Eligible companies with ESG Risk Score: {len(scored_eligible_df)}")

        scored_eligible_df = scored_eligible_df.sort_values(
            ['ESG_Risk_Score', 'FFMC_CO'],
            ascending=[True, False]
        )
        scored_eligible_df['ESG_Risk_Rank'] = range(1, len(scored_eligible_df) + 1)

        # ----------------------------------------------------------------
        # Step 4: Select top 50 by ESG Risk Score ranking
        # ----------------------------------------------------------------
        logger.info("Step 4: Selecting top 50 companies by ESG Risk Score...")

        top_50_df = scored_eligible_df.head(50).copy()
        logger.info(f"Selected {len(top_50_df)} companies for index")

        # ----------------------------------------------------------------
        # Weighting: Equal Weight using Close Prc_EOD
        # ----------------------------------------------------------------
        logger.info("Calculating equal-weight Number of Shares using Close Prc_EOD...")

        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]

        num_constituents = len(top_50_df)
        target_mcap_per_company = index_mcap / num_constituents

        top_50_df['Unrounded NOSH'] = (
            target_mcap_per_company
            / (top_50_df['Close Prc_EOD'] * top_50_df['FX/Index Ccy'])
        )
        top_50_df['Rounded NOSH'] = top_50_df['Unrounded NOSH'].round()

        # Free Float Factor and Capping Factor not applied per rulebook
        top_50_df['Free Float'] = 1
        top_50_df['Capping Factor'] = 1

        # ----------------------------------------------------------------
        # Final index composition DataFrame
        # ----------------------------------------------------------------
        ESF5P_df = (
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

        # ----------------------------------------------------------------
        # Inclusion / Exclusion analysis
        # ----------------------------------------------------------------
        analysis_results = inclusion_exclusion_analysis(
            top_50_df,
            stock_eod_df,
            index,
            isin_column='ISIN code'
        )
        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']

        # ----------------------------------------------------------------
        # Save output
        # ----------------------------------------------------------------
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            esf5p_path = os.path.join(output_dir, f'ESF5P_df_{timestamp}.xlsx')

            logger.info(f"Saving ESF5P output to: {esf5p_path}")
            with pd.ExcelWriter(esf5p_path) as writer:
                ESF5P_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                top_50_df.to_excel(writer, sheet_name='Top 50 Selection', index=False)
                scored_eligible_df.to_excel(writer, sheet_name='ESG Ranked Eligible 80', index=False)
                eligible_df.to_excel(writer, sheet_name='FFMC Eligible 80', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(
                    writer, sheet_name='Index Market Cap', index=False
                )

            return {
                "status": "success",
                "message": "ESF5P review completed successfully",
                "data": {"esf5p_path": esf5p_path}
            }

        except Exception as e:
            error_msg = f"Error saving output file: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg, "data": None}

    except Exception as e:
        logger.error(f"Error during ESF5P review calculation: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": f"Error during ESF5P review calculation: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None
        }