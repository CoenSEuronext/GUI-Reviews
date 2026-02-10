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

# ---------------------------------------------------------------------------
# Sustainalytics field codes
# ---------------------------------------------------------------------------

# ESG Risk Score: exclude if > 40
ESG_RISK_SCORE_CODE         = '181110112399'

# Global Compact Compliance: exclude if == 'Non-Compliant'
GLOBAL_COMPACT_CODE         = '231112111799'

ALL_REQUIRED_CODES = {
    ESG_RISK_SCORE_CODE,
    GLOBAL_COMPACT_CODE,
}

# ICB Industry Code for Health Care
HEALTH_CARE_INDUSTRY_CODE = '20'


def run_ghcpr_review(date, co_date, effective_date, index="GHCPR", isin="FRESG0000009",
                     area="US", area2="EU", type="STOCK", universe="developed_market",
                     feed="Reuters", currency="EUR", year=None):
    """
    Euronext Global Health Care 50 EW ESG Index Review

    Methodology:
    - Universe: Euronext Developed Market index, filtered to ICB Industry Health Care (20).
    - Step 2a: Exclude companies with ESG Risk Score > 40.
    - Step 2b: Exclude companies with non-compliant Global Compact compliance status.
    - Step 3:  Rank remaining eligible companies by FFMC (Close Prc_CO) descending.
    - Step 4:  Top 50 by FFMC are selected.
    - Weighting: Equal Weight, Number of Shares calculated using Close Prc_EOD.
    - Free Float Factor: Not applied (set to 1).
    - Capping Factor: Not applied (set to 1).
    """

    try:
        year = year or str(datetime.strptime(date, '%Y%m%d').year)
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        logger.info("Loading EOD data...")
        index_eod_df, stock_eod_df, stock_co_df, fx_lookup_df = load_eod_data(
            date, co_date, area, area2, DLF_FOLDER
        )

        logger.info("Loading reference data...")
        ref_data = load_reference_data(
            current_data_folder,
            ['ff', 'sustainalytics', 'icb', 'developed_market']
        )

        if ref_data.get('developed_market') is None:
            raise ValueError("Failed to load developed_market universe data")

        # Step 1: Index Universe - Developed Market filtered to Health Care (ICB Industry 20)
        dm_universe = ref_data['developed_market']
        ff_df = ref_data['ff']
        icb_df = ref_data['icb']

        universe_df = pd.DataFrame(dm_universe)
        logger.info(f"Developed Market universe size: {len(universe_df)}")

        universe_df['Effective Date of Review'] = effective_date

        # Resolve symbols (short symbols only, one per ISIN)
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        # Build base selection DataFrame
        selection_df = (
            universe_df
            .rename(columns={
                'NOSH': 'Number of Shares',
                'ISIN': 'ISIN code',
                'Name': 'Company',
            })
            .merge(symbols_filtered, left_on='ISIN code', right_on='Isin Code', how='left')
            .drop('Isin Code', axis=1)
            .merge(
                stock_eod_df[stock_eod_df['Index Curr'] == currency][
                    ['#Symbol', 'FX/Index Ccy']
                ].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol', how='left'
            )
            .merge(
                stock_eod_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol', how='left'
            )
            .rename(columns={'Close Prc': 'Close Prc_EOD'})
            .merge(
                stock_co_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol', how='left'
            )
            .rename(columns={'Close Prc': 'Close Prc_CO'})
            .merge(
                ff_df[['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset='ISIN Code:', keep='first'),
                left_on='ISIN code', right_on='ISIN Code:', how='left'
            )
            .drop('ISIN Code:', axis=1)
            # Merge ICB data for Industry Code
            .merge(
                icb_df[['ISIN Code', 'Subsector Code']].drop_duplicates(subset='ISIN Code', keep='first'),
                left_on='ISIN code', right_on='ISIN Code', how='left'
            )
            .drop('ISIN Code', axis=1)
        )

        selection_df['Currency'] = currency

        # Extract ICB Industry Code from Subsector Code (first 2 digits)
        selection_df['Industry Code'] = (
            selection_df['Subsector Code'].astype(str).str[:2]
        )

        # Filter to Health Care (ICB Industry Code 20) only
        pre_filter_count = len(selection_df)
        selection_df = selection_df[
            selection_df['Industry Code'] == HEALTH_CARE_INDUSTRY_CODE
        ].copy()
        logger.info(
            f"After Health Care (ICB 20) filter: {len(selection_df)} companies "
            f"(excluded {pre_filter_count - len(selection_df)})"
        )

        # FFMC using Close Prc_CO - used for selection ranking
        selection_df['FFMC_CO'] = (
            selection_df['Free Float Round:']
            * selection_df['Number of Shares']
            * selection_df['Close Prc_CO']
            * selection_df['FX/Index Ccy']
        )

        # ----------------------------------------------------------------
        # Load Sustainalytics data and extract required fields in one pass
        # ----------------------------------------------------------------
        logger.info("Loading Sustainalytics data...")
        sustainalytics_raw = ref_data.get('sustainalytics')
        if sustainalytics_raw is None:
            raise ValueError("Sustainalytics data required for GHCPR index")

        # Row 0 contains the column codes
        codes_row = sustainalytics_raw.iloc[0].copy()
        for col in codes_row.index:
            if col != 'ISIN':
                try:
                    if pd.notna(codes_row[col]):
                        codes_row[col] = str(int(float(codes_row[col])))
                except (ValueError, TypeError):
                    codes_row[col] = str(codes_row[col]).strip()

        # Find all columns matching required codes
        cols_to_keep = ['ISIN']
        col_name_mapping = {}
        for col_name in sustainalytics_raw.columns:
            if col_name != 'ISIN':
                cell_value = codes_row[col_name]
                if cell_value in ALL_REQUIRED_CODES:
                    cols_to_keep.append(col_name)
                    col_name_mapping[col_name] = cell_value

        # Warn if any required code was not found
        found_codes = set(col_name_mapping.values())
        missing_codes = ALL_REQUIRED_CODES - found_codes
        if missing_codes:
            logger.warning(f"The following Sustainalytics codes were not found in data: {missing_codes}")

        sustainalytics_filtered = sustainalytics_raw[cols_to_keep].iloc[1:].copy()

        rename_dict = {
            col: f"{col_name_mapping[col]} - {col}"
            for col in cols_to_keep
            if col != 'ISIN'
        }
        sustainalytics_filtered.rename(columns=rename_dict, inplace=True)
        sustainalytics_filtered = sustainalytics_filtered.drop_duplicates(subset='ISIN', keep='first')

        # Merge Sustainalytics fields into the Health Care universe
        selection_df = selection_df.merge(
            sustainalytics_filtered,
            left_on='ISIN code', right_on='ISIN', how='left'
        ).drop('ISIN', axis=1, errors='ignore')

        logger.info("Sustainalytics data merge completed.")

        # Helper: find column by code prefix
        def find_col(df, code):
            for col in df.columns:
                if col.startswith(f"{code} - "):
                    return col
            return None

        # ----------------------------------------------------------------
        # Step 2a: Exclude companies with ESG Risk Score > 40
        # ----------------------------------------------------------------
        logger.info("Step 2a: Excluding companies with ESG Risk Score > 40...")

        col_esg_risk = find_col(selection_df, ESG_RISK_SCORE_CODE)
        if col_esg_risk is None:
            raise ValueError("Could not locate ESG Risk Score column after merge")

        selection_df['ESG_Risk_Score'] = pd.to_numeric(
            selection_df[col_esg_risk], errors='coerce'
        )

        # Companies without a score are treated as ineligible (conservative approach)
        selection_df['excl_esg_risk_score'] = (
            selection_df['ESG_Risk_Score'].isna() |
            (selection_df['ESG_Risk_Score'] > 40)
        )

        esg_excluded_count = selection_df['excl_esg_risk_score'].sum()
        logger.info(f"ESG Risk Score screen: {esg_excluded_count} companies excluded (score > 40 or missing)")

        # ----------------------------------------------------------------
        # Step 2b: Exclude companies with non-compliant Global Compact status
        # ----------------------------------------------------------------
        logger.info("Step 2b: Excluding non-compliant Global Compact companies...")

        col_global_compact = find_col(selection_df, GLOBAL_COMPACT_CODE)
        if col_global_compact is None:
            logger.warning("Global Compact Compliance column not found - skipping this exclusion screen")
            selection_df['excl_global_compact'] = False
        else:
            selection_df['excl_global_compact'] = (
                selection_df[col_global_compact] == 'Non-Compliant'
            )

        gc_excluded_count = selection_df['excl_global_compact'].sum()
        logger.info(f"Global Compact screen: {gc_excluded_count} companies excluded")

        # Combine exclusion flags
        selection_df['excluded'] = (
            selection_df['excl_esg_risk_score'] |
            selection_df['excl_global_compact']
        )

        excluded_df = selection_df[selection_df['excluded']].copy()
        eligible_df = selection_df[~selection_df['excluded']].copy()

        logger.info(
            f"After all exclusions: {len(eligible_df)} eligible companies "
            f"({len(excluded_df)} excluded)"
        )

        # ----------------------------------------------------------------
        # Step 3: Rank eligible companies by FFMC_CO descending
        # ----------------------------------------------------------------
        logger.info("Step 3: Ranking eligible companies by FFMC (Close Prc_CO) descending...")

        eligible_df = eligible_df.sort_values('FFMC_CO', ascending=False).copy()
        eligible_df['FFMC_Rank'] = range(1, len(eligible_df) + 1)

        # ----------------------------------------------------------------
        # Step 4: Select top 50 by FFMC ranking
        # ----------------------------------------------------------------
        logger.info("Step 4: Selecting top 50 companies by FFMC...")

        top_50_df = eligible_df.head(50).copy()
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
        GHCPR_df = (
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
            ghcpr_path = os.path.join(output_dir, f'GHCPR_df_{timestamp}.xlsx')

            logger.info(f"Saving GHCPR output to: {ghcpr_path}")
            with pd.ExcelWriter(ghcpr_path) as writer:
                GHCPR_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                top_50_df.to_excel(writer, sheet_name='Top 50 Selection', index=False)
                eligible_df.to_excel(writer, sheet_name='Eligible Universe', index=False)
                excluded_df.to_excel(writer, sheet_name='Excluded Companies', index=False)
                selection_df.to_excel(writer, sheet_name='Health Care Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(
                    writer, sheet_name='Index Market Cap', index=False
                )

            return {
                "status": "success",
                "message": "GHCPR review completed successfully",
                "data": {"ghcpr_path": ghcpr_path}
            }

        except Exception as e:
            error_msg = f"Error saving output file: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg, "data": None}

    except Exception as e:
        logger.error(f"Error during GHCPR review calculation: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": f"Error during GHCPR review calculation: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None
        }