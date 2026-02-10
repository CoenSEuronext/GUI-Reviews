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

# Global Standards Screening: exclude if == 'Non-Compliant'
GLOBAL_COMPACT_CODE              = '231112111799'

# Tobacco Production: exclude if any involvement (Level ID > 0)
TOBACCO_PRODUCTION_CODE          = '172911112999'

# Thermal Coal Extraction: exclude if Revenue % >= 1%
THERMAL_COAL_EXTRACTION_CODE     = '171025111199'

# Oil Production (sum of all fields): exclude if sum >= 10%
OIL_PRODUCTION_CODES = [
    '171114221199',
    '171114261199',
    '171114301199',
]

# Gas Production (sum of all fields): exclude if sum >= 50%
GAS_PRODUCTION_CODES = [
    '171114201199',
    '171114241199',
    '171114281199',
]

# Fossil Fuel Power Generation (sum of both): exclude if sum >= 50%
FOSSIL_FUEL_POWER_CODES = [
    '171025141199',
    '171114141199',
]

# Controversial Weapons - both fields required together
# Category of involvement (string): CW1 (tailor-made) or CW3 (non-tailor-made)
CONTROV_WEAPONS_ESSENTIAL_CODE     = '171611102999'   # Category ID; exclude if CW1
CONTROV_WEAPONS_NON_ESSENTIAL_CODE = '171613102999'   # Category ID; exclude if CW3
# Type of weapon (string): exclude only if BC, AP, or CM
WEAPON_TYPE_CODE                   = '211010122999'

# ESG Risk Score: exclude top 30% highest scorers; companies without score are ineligible
ESG_RISK_SCORE_CODE                = '181110112399'

ALL_REQUIRED_CODES = {
    GLOBAL_COMPACT_CODE,
    TOBACCO_PRODUCTION_CODE,
    THERMAL_COAL_EXTRACTION_CODE,
    *OIL_PRODUCTION_CODES,
    *GAS_PRODUCTION_CODES,
    *FOSSIL_FUEL_POWER_CODES,
    CONTROV_WEAPONS_ESSENTIAL_CODE,
    CONTROV_WEAPONS_NON_ESSENTIAL_CODE,
    WEAPON_TYPE_CODE,
    ESG_RISK_SCORE_CODE,
}

# Weapon types that trigger exclusion
EXCLUDED_WEAPON_TYPES = {'BC', 'AP', 'CM'}


def run_edwpe_review(date, co_date, effective_date, index="EDWPE", isin="FRESG0003326",
                     area="US", area2="EU", type="STOCK", universe="edwp",
                     feed="Reuters", currency="EUR", year=None):
    """
    Euronext Developed World ESG Index Review

    Methodology:
    - Universe: Euronext Developed World Index (edwp).
    - Step 2: Apply exclusion screens:
        a) Global Standards Screening: exclude Non-Compliant companies.
        b) Product involvement: Tobacco, Thermal Coal, Oil, Gas, Fossil Fuel Power,
           Controversial Weapons (only BC/AP/CM weapon types).
        c) ESG Risk Score: exclude top 30% highest scorers (and those with no score).
    - Step 3: No selection ranking applied.
    - Step 4: All eligible companies are selected.
    - Weighting: Free Float Market Cap weighted.
      Number of Shares and Free Float Factor taken directly from universe/CO data.
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
            ['ff', 'sustainalytics', 'edwp']
        )

        if ref_data.get('edwp') is None:
            raise ValueError("Failed to load edwp universe data")

        # Step 1: Index Universe - Euronext Developed World
        edwp_universe = ref_data['edwp']
        ff_df = ref_data['ff']

        universe_df = pd.DataFrame(edwp_universe)
        logger.info(f"Developed World universe size: {len(universe_df)}")

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
        )

        selection_df['Currency'] = currency

        # ----------------------------------------------------------------
        # Load Sustainalytics data and extract all required fields in one pass
        # ----------------------------------------------------------------
        logger.info("Loading Sustainalytics data...")
        sustainalytics_raw = ref_data.get('sustainalytics')
        if sustainalytics_raw is None:
            raise ValueError("Sustainalytics data required for EDWPE index")

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

        # Merge all Sustainalytics fields into the universe
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

        # Helper: coerce to numeric, NaN -> 0
        def to_numeric(df, code):
            col = find_col(df, code)
            if col is None:
                logger.warning(f"Code {code} not found - treating all as 0")
                return pd.Series(0, index=df.index)
            return pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Helper: sum multiple numeric fields
        def sum_fields(df, codes):
            total = pd.Series(0.0, index=df.index)
            for code in codes:
                total += to_numeric(df, code)
            return total

        # ----------------------------------------------------------------
        # Step 2: Apply exclusion screens
        # ----------------------------------------------------------------
        logger.info("Step 2: Applying exclusion screens...")
        pre_exclusion_count = len(selection_df)
        exclusion_flags = pd.DataFrame(index=selection_df.index)

        # -- Global Standards Screening: Non-Compliant
        col_gc = find_col(selection_df, GLOBAL_COMPACT_CODE)
        if col_gc is None:
            logger.warning("Global Standards column not found - skipping")
            exclusion_flags['excl_global_standards'] = False
        else:
            exclusion_flags['excl_global_standards'] = (
                selection_df[col_gc] == 'Non-Compliant'
            )

        # -- Tobacco Production: any involvement (Level ID > 0)
        exclusion_flags['excl_tobacco'] = to_numeric(selection_df, TOBACCO_PRODUCTION_CODE) > 0

        # -- Thermal Coal Extraction: Revenue % >= 1%
        exclusion_flags['excl_thermal_coal'] = (
            to_numeric(selection_df, THERMAL_COAL_EXTRACTION_CODE) >= 1
        )

        # -- Oil Production: sum of 3 fields >= 10%
        exclusion_flags['excl_oil'] = sum_fields(selection_df, OIL_PRODUCTION_CODES) >= 10

        # -- Gas Production: sum of 3 fields >= 50%
        exclusion_flags['excl_gas'] = sum_fields(selection_df, GAS_PRODUCTION_CODES) >= 50

        # -- Fossil Fuel Power Generation: sum of 2 fields >= 50%
        exclusion_flags['excl_fossil_fuel_power'] = (
            sum_fields(selection_df, FOSSIL_FUEL_POWER_CODES) >= 50
        )

        # -- Controversial Weapons: Category CW1/CW3 AND weapon type in BC, AP, CM
        #    Both conditions must be met simultaneously for exclusion
        col_cw_essential     = find_col(selection_df, CONTROV_WEAPONS_ESSENTIAL_CODE)
        col_cw_non_essential = find_col(selection_df, CONTROV_WEAPONS_NON_ESSENTIAL_CODE)
        col_weapon_type      = find_col(selection_df, WEAPON_TYPE_CODE)

        if col_weapon_type is not None:
            weapon_type_series = selection_df[col_weapon_type].astype(str).str.strip()
            weapon_is_excluded = weapon_type_series.isin(EXCLUDED_WEAPON_TYPES)
        else:
            logger.warning("Weapon type code 211010122999 not found - controversial weapons exclusion skipped")
            weapon_is_excluded = pd.Series(False, index=selection_df.index)

        cw_essential_flag = pd.Series(False, index=selection_df.index)
        if col_cw_essential is not None:
            cw_essential_flag = (
                selection_df[col_cw_essential].astype(str).str.strip() == 'CW1'
            ) & weapon_is_excluded

        cw_non_essential_flag = pd.Series(False, index=selection_df.index)
        if col_cw_non_essential is not None:
            cw_non_essential_flag = (
                selection_df[col_cw_non_essential].astype(str).str.strip() == 'CW3'
            ) & weapon_is_excluded

        exclusion_flags['excl_controv_weapons'] = cw_essential_flag | cw_non_essential_flag

        # -- ESG Risk Score: exclude top 30% highest scorers and companies with no score
        col_esg = find_col(selection_df, ESG_RISK_SCORE_CODE)
        if col_esg is None:
            raise ValueError("Could not locate ESG Risk Score column after merge")

        selection_df['ESG_Risk_Score'] = pd.to_numeric(selection_df[col_esg], errors='coerce')

        # Companies with no score are ineligible per rulebook
        no_score_mask = selection_df['ESG_Risk_Score'].isna()

        # Determine top 30% cutoff on scored companies only
        scored_mask = ~no_score_mask
        if scored_mask.sum() > 0:
            scores_only = selection_df.loc[scored_mask, 'ESG_Risk_Score'].sort_values(ascending=False)
            cutoff_n = int(np.ceil(len(scores_only) * 0.30))
            # Score at the boundary - all companies with this score or higher are excluded
            cutoff_score = scores_only.iloc[cutoff_n - 1]
            esg_excl_mask = scored_mask & (selection_df['ESG_Risk_Score'] >= cutoff_score)
        else:
            logger.warning("No companies have an ESG Risk Score - all will be excluded")
            esg_excl_mask = scored_mask  # empty

        exclusion_flags['excl_esg_no_score'] = no_score_mask
        exclusion_flags['excl_esg_top_30pct'] = esg_excl_mask

        logger.info(
            f"ESG Risk Score screen: cutoff score = {cutoff_score:.4f}, "
            f"{esg_excl_mask.sum()} in top 30%, {no_score_mask.sum()} with no score"
        )

        # Combine all exclusion flags
        any_exclusion = exclusion_flags.any(axis=1)

        # Attach flags to selection_df for auditability
        selection_df = pd.concat([selection_df, exclusion_flags], axis=1)
        selection_df['excluded'] = any_exclusion

        excluded_df = selection_df[selection_df['excluded']].copy()
        eligible_df = selection_df[~selection_df['excluded']].copy()

        logger.info(
            f"Exclusion screen: {pre_exclusion_count} -> {len(eligible_df)} companies "
            f"({len(excluded_df)} excluded)"
        )

        # ----------------------------------------------------------------
        # Step 3: No selection ranking applied
        # Step 4: All eligible companies are selected
        # ----------------------------------------------------------------
        logger.info("Steps 3 & 4: All eligible companies selected (no ranking applied).")
        final_df = eligible_df.copy()
        logger.info(f"Final selection: {len(final_df)} companies")

        # ----------------------------------------------------------------
        # Weighting: Free Float Market Cap weighted
        # Number of Shares and Free Float Factor taken from CO data
        # Capping Factor not applied (set to 1)
        # ----------------------------------------------------------------
        logger.info("Applying Free Float Market Cap weighting...")

        final_df['Free Float'] = final_df['Free Float Round:']
        final_df['Capping Factor'] = 1

        # ----------------------------------------------------------------
        # Final index composition DataFrame
        # ----------------------------------------------------------------
        EDWPE_df = (
            final_df[
                ['Company', 'ISIN code', 'MIC', 'Number of Shares', 'Free Float',
                 'Capping Factor', 'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'ISIN code': 'ISIN Code'})
            .sort_values('Company')
        )

        # ----------------------------------------------------------------
        # Inclusion / Exclusion analysis
        # ----------------------------------------------------------------
        analysis_results = inclusion_exclusion_analysis(
            final_df,
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
            edwpe_path = os.path.join(output_dir, f'EDWPE_df_{timestamp}.xlsx')

            logger.info(f"Saving EDWPE output to: {edwpe_path}")
            with pd.ExcelWriter(edwpe_path) as writer:
                EDWPE_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                final_df.to_excel(writer, sheet_name='Final Selection', index=False)
                excluded_df.to_excel(writer, sheet_name='Excluded Companies', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)

            return {
                "status": "success",
                "message": "EDWPE review completed successfully",
                "data": {"edwpe_path": edwpe_path}
            }

        except Exception as e:
            error_msg = f"Error saving output file: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg, "data": None}

    except Exception as e:
        logger.error(f"Error during EDWPE review calculation: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": f"Error during EDWPE review calculation: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None
        }