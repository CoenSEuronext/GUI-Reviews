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
# Sustainalytics field code mappings
# ---------------------------------------------------------------------------

# Level of Involvement ID -> revenue range mapping (for reference/logging)
LEVEL_OF_INVOLVEMENT_RANGES = {
    0: '0',
    1: '0-4.9%',
    2: '5-9.9%',
    3: '10-24.9%',
    4: '25-49.9%',
    5: '50-100%',
}

# All level-of-involvement field codes (used to retrieve columns from Sustainalytics data)
LEVEL_OF_INVOLVEMENT_FIELDS = {
    'Adult Entertainment Distribution-Level of Involvement Id':                    '171213112999',
    'Adult Entertainment Production-Level of Involvement Id':                      '171211112999',
    'Arctic Oil & Gas Exploration Extraction-Level of Involvement Id':             '173111112999',
    'Gambling Operations-Level of Involvement Id':                                 '171911112999',
    'Gambling Specialized Equipment-Level of Involvement Id':                      '171913112999',
    'Gambling Supporting Products/Services-Level of Involvement Id':               '171915112999',
    'Military Contracting Weapon-related products and/or services-Level of Involvement Id': '172113112999',
    'Military Contracting Weapons-Level of Involvement Id':                        '172111112999',
    'Nuclear Distribution-Level of Involvement Id':                                '172215112999',
    'Nuclear Production-Revenue Level of Involvement Id':                          '172216171899',
    'Nuclear Supporting Products/Services-Level of Involvement Id':                '172213112999',
    'Oil & Gas Generation-Revenue Level of Involvement Id':                        '173316171899',
    'Oil & Gas Production-Level of Involvement Id':                                '173311112999',
    'Oil & Gas Supporting Products/Services-Level of Involvement Id':              '173313112999',
    'Oil Sands Extraction-Revenue Level of Involvement Id':                        '173012171899',
    'Palm Oil Production and distribution-Level of Involvement Id':                '172511112999',
    'Shale Energy Extraction-Level of Involvement Id':                             '173211112999',
    'Small Arms Civilian customers (Assault weapons)-Level of Involvement Id':     '171711112999',
    'Small Arms Civilian customers (Non-assault weapons)-Level of Involvement Id': '171721112999',
    'Small Arms Key components-Level of Involvement Id':                           '171715112999',
    'Small Arms Military/law enforcement customers-Level of Involvement Id':       '171713112999',
    'Thermal Coal Extraction-Level of Involvement Id':                             '172811112999',
    'Thermal Coal Power Generation-Level of Involvement Id':                       '172813112999',
    'Thermal Coal Supporting Products/Services-Level of Involvement Id':           '171025171999',
    'Tobacco Products Production-Level of Involvement Id':                         '172911112999',
    'Tobacco Products Related Products/Services-Level of Involvement Id':          '172913112999',
    'Tobacco Products Retail-Level of Involvement Id':                             '172915112999',
    'Pesticides Production-Level of Involvement Id':                               '172311112999',
    'Alcoholic Beverages Production-Level of Involvement Id':                      '171311112999',
    'Nuclear Overall-Level of Involvement Id':                                     '171018221999',
}

# Sustainalytics codes needed for EEEPR exclusions
# Controversies
CONTROVERSY_LEVEL_CODE    = '121010112599'   # Highest Controversy Level; exclude if >= 4 (numeric)

# Tobacco
TOBACCO_PRODUCTION_CODE   = '172911112999'   # Tobacco Products Production; exclude if Level ID > 0 (derived turnover > 0%)
TOBACCO_RETAIL_CODE       = '172915112999'   # Tobacco Products Retail; exclude if Level ID >= 2 (derived turnover >= 5%)

# Military Contracting
MILITARY_WEAPONS_CODE     = '172111112999'   # Military Contracting Weapons; exclude if Level ID > 0 (derived turnover > 0%)
MILITARY_RELATED_CODE     = '172113112999'   # Military Contracting Weapon-related; exclude if Level ID >= 3 (derived turnover >= 10%)

# Controversial Weapons - category of involvement (not level of involvement)
CONTROV_WEAPONS_ESSENTIAL_CODE    = '171611102999'  # Tailor-made and essential; exclude if any involvement
CONTROV_WEAPONS_NON_ESSENTIAL_CODE = '171613102999' # Non tailor-made or non-essential; exclude if any involvement

# Civilian Firearms (small arms)
SMALL_ARMS_ASSAULT_CODE   = '171711112999'   # Small Arms Civilian Assault; exclude if Level ID > 0 (derived turnover > 0%)
SMALL_ARMS_NON_ASSAULT_CODE = '171721112999' # Small Arms Civilian Non-assault; exclude if Level ID > 0 (derived turnover > 0%)

# Unconventional Oil & Gas - category of involvement
OIL_SANDS_CODE            = '173011102999'   # Oil Sands Extraction; exclude if any involvement
SHALE_ENERGY_CODE         = '173211102999'   # Shale Energy Extraction/Production; exclude if any involvement

# Coal
THERMAL_COAL_EXTRACTION_CODE      = '171025111199'  # Thermal Coal Extraction; exclude if Revenue % > 0%
THERMAL_COAL_POWER_CODE           = '172813112999'  # Thermal Coal Power Generation; exclude if Level ID > 0 (derived turnover > 0%)

# ESG Risk Score (used for Step 3 ranking)
ESG_RISK_SCORE_CODE       = '181110112399'

# Collect all codes needed (exclusion + ranking) so we can retrieve them in one pass
ALL_REQUIRED_CODES = {
    CONTROVERSY_LEVEL_CODE,
    TOBACCO_PRODUCTION_CODE,
    TOBACCO_RETAIL_CODE,
    MILITARY_WEAPONS_CODE,
    MILITARY_RELATED_CODE,
    CONTROV_WEAPONS_ESSENTIAL_CODE,
    CONTROV_WEAPONS_NON_ESSENTIAL_CODE,
    SMALL_ARMS_ASSAULT_CODE,
    SMALL_ARMS_NON_ASSAULT_CODE,
    OIL_SANDS_CODE,
    SHALE_ENERGY_CODE,
    THERMAL_COAL_EXTRACTION_CODE,
    THERMAL_COAL_POWER_CODE,
    ESG_RISK_SCORE_CODE,
}


def run_eeepr_review(date, co_date, effective_date, index="EEEPR", isin="NL0015000AA9",
                     area="US", area2="EU", type="STOCK", universe="eurozone_300",
                     feed="Reuters", currency="EUR", year=None):
    """
    Euronext EZ ESGL 40 EW Index Review

    Methodology:
    - Universe: Euronext Eurozone 300 index
    - Step 2a: Apply Sustainalytics activity and controversy exclusions.
    - Step 2b: Rank remaining companies by FFMC (Close Prc_CO). Top 60 are eligible.
    - Step 3:  Rank the 60 eligible companies by Sustainalytics ESG Risk Score
               ascending. Tie-breaker: highest FFMC (Close Prc_CO).
    - Step 4:  Top 40 by ESG Risk Score ranking are selected.
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
            ['ff', 'sustainalytics', 'eurozone_300']
        )

        if ref_data.get('eurozone_300') is None:
            raise ValueError("Failed to load eurozone_300 universe data")

        # Step 1: Index Universe - Euronext Eurozone 300
        ezgp_universe = ref_data['eurozone_300']
        ff_df = ref_data['ff']

        universe_df = pd.DataFrame(ezgp_universe)
        logger.info(f"Starting universe size: {len(universe_df)}")

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

        # FFMC using Close Prc_CO - used for eligibility screen and ESG tie-breaking
        selection_df['FFMC_CO'] = (
            selection_df['Free Float Round:']
            * selection_df['Number of Shares']
            * selection_df['Close Prc_CO']
            * selection_df['FX/Index Ccy']
        )

        # ----------------------------------------------------------------
        # Load Sustainalytics data and extract all required fields in one pass
        # ----------------------------------------------------------------
        logger.info("Loading Sustainalytics data...")
        sustainalytics_raw = ref_data.get('sustainalytics')
        if sustainalytics_raw is None:
            raise ValueError("Sustainalytics data required for EEEPR index")

        # Row 0 contains the column codes
        codes_row = sustainalytics_raw.iloc[0].copy()
        for col in codes_row.index:
            if col != 'ISIN':
                try:
                    if pd.notna(codes_row[col]):
                        codes_row[col] = str(int(float(codes_row[col])))
                except (ValueError, TypeError):
                    codes_row[col] = str(codes_row[col]).strip()

        # Find all columns matching any of our required codes
        cols_to_keep = ['ISIN']
        col_name_mapping = {}  # original col name -> code string
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

        # Rename columns to 'CODE - original_header' for traceability
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

        # Helper: coerce a column to numeric (for level-of-involvement and numeric fields)
        def to_numeric(df, code):
            col = find_col(df, code)
            if col is None:
                logger.warning(f"Code {code} not found - exclusion check skipped, treating all as 0")
                return pd.Series(0, index=df.index)
            return pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Helper: exclude if value is anything other than 0, NaN, or empty string
        # Used for category-of-involvement fields that store a string when there is involvement
        def has_any_involvement(df, code):
            col = find_col(df, code)
            if col is None:
                logger.warning(f"Code {code} not found - exclusion check skipped, treating all as no involvement")
                return pd.Series(False, index=df.index)
            raw = df[col]
            # Numeric zero or NaN -> no involvement
            # Any non-zero number OR any non-empty string -> involvement
            numeric = pd.to_numeric(raw, errors='coerce')
            is_numeric_zero_or_nan = numeric.isna() | (numeric == 0)
            is_empty_string = raw.astype(str).str.strip().isin(['', 'nan', 'None', '0'])
            return ~(is_numeric_zero_or_nan & is_empty_string)

        # ----------------------------------------------------------------
        # Step 2a: Apply exclusion screens
        # ----------------------------------------------------------------
        logger.info("Step 2a: Applying exclusion screens...")
        pre_exclusion_count = len(selection_df)
        exclusion_flags = pd.DataFrame(index=selection_df.index)

        # -- Controversies: Highest Controversy Level >= 4 (numeric)
        controversy = to_numeric(selection_df, CONTROVERSY_LEVEL_CODE)
        exclusion_flags['excl_controversy'] = controversy >= 4

        # -- Tobacco Production: Level ID > 0  (derived turnover > 0%)
        excl_tobacco_prod = to_numeric(selection_df, TOBACCO_PRODUCTION_CODE)
        exclusion_flags['excl_tobacco_production'] = excl_tobacco_prod > 0

        # -- Tobacco Retail: Level ID >= 2  (derived turnover >= 5%)
        excl_tobacco_retail = to_numeric(selection_df, TOBACCO_RETAIL_CODE)
        exclusion_flags['excl_tobacco_retail'] = excl_tobacco_retail >= 2

        # -- Military Contracting Weapons: Level ID > 0  (derived turnover > 0%)
        excl_mil_weapons = to_numeric(selection_df, MILITARY_WEAPONS_CODE)
        exclusion_flags['excl_military_weapons'] = excl_mil_weapons > 0

        # -- Military Contracting Weapon-related: Level ID >= 3  (derived turnover >= 10%)
        excl_mil_related = to_numeric(selection_df, MILITARY_RELATED_CODE)
        exclusion_flags['excl_military_related'] = excl_mil_related >= 3

        # -- Controversial Weapons Tailor-made/essential: any involvement (non-zero/non-empty string)
        exclusion_flags['excl_controv_weapons_essential'] = has_any_involvement(
            selection_df, CONTROV_WEAPONS_ESSENTIAL_CODE
        )

        # -- Controversial Weapons Non-tailor-made/non-essential: any involvement (non-zero/non-empty string)
        exclusion_flags['excl_controv_weapons_non_essential'] = has_any_involvement(
            selection_df, CONTROV_WEAPONS_NON_ESSENTIAL_CODE
        )

        # -- Small Arms Civilian Assault: Level ID > 0  (derived turnover > 0%)
        excl_sa_assault = to_numeric(selection_df, SMALL_ARMS_ASSAULT_CODE)
        exclusion_flags['excl_small_arms_assault'] = excl_sa_assault > 0

        # -- Small Arms Civilian Non-assault: Level ID > 0  (derived turnover > 0%)
        excl_sa_non_assault = to_numeric(selection_df, SMALL_ARMS_NON_ASSAULT_CODE)
        exclusion_flags['excl_small_arms_non_assault'] = excl_sa_non_assault > 0

        # -- Oil Sands Extraction: any involvement (non-zero/non-empty string)
        exclusion_flags['excl_oil_sands'] = has_any_involvement(selection_df, OIL_SANDS_CODE)

        # -- Shale Energy Extraction: any involvement (non-zero/non-empty string)
        exclusion_flags['excl_shale_energy'] = has_any_involvement(selection_df, SHALE_ENERGY_CODE)

        # -- Thermal Coal Extraction: Revenue % > 0%
        #    Field 171025111199 is stored as a revenue percentage (numeric), exclude if > 0
        excl_coal_extraction = to_numeric(selection_df, THERMAL_COAL_EXTRACTION_CODE)
        exclusion_flags['excl_thermal_coal_extraction'] = excl_coal_extraction > 0

        # -- Thermal Coal Power Generation: Level ID > 0  (derived turnover > 0%)
        excl_coal_power = to_numeric(selection_df, THERMAL_COAL_POWER_CODE)
        exclusion_flags['excl_thermal_coal_power'] = excl_coal_power > 0

        # Combine all exclusion flags
        any_exclusion = exclusion_flags.any(axis=1)

        # Attach flags to selection_df for auditability
        selection_df = pd.concat([selection_df, exclusion_flags], axis=1)
        selection_df['excluded'] = any_exclusion

        excluded_df = selection_df[selection_df['excluded']].copy()
        post_exclusion_df = selection_df[~selection_df['excluded']].copy()

        logger.info(
            f"Exclusion screen: {pre_exclusion_count} -> {len(post_exclusion_df)} companies "
            f"({len(excluded_df)} excluded)"
        )

        # ----------------------------------------------------------------
        # Step 2b: Rank surviving companies by FFMC_CO, keep top 60
        # ----------------------------------------------------------------
        logger.info("Step 2b: Ranking post-exclusion universe by FFMC_CO, keeping top 60...")

        ranked_by_ffmc = post_exclusion_df.sort_values('FFMC_CO', ascending=False).copy()
        ranked_by_ffmc['FFMC_Rank'] = range(1, len(ranked_by_ffmc) + 1)

        eligible_df = ranked_by_ffmc.head(60).copy()
        logger.info(f"Eligible companies after FFMC screen (top 60): {len(eligible_df)}")

        # ----------------------------------------------------------------
        # Step 3: Rank eligible 60 by ESG Risk Score (ascending)
        #         Tie-breaker: FFMC_CO descending
        # ----------------------------------------------------------------
        logger.info("Step 3: Ranking eligible companies by ESG Risk Score (ascending)...")

        col_esg_risk = find_col(eligible_df, ESG_RISK_SCORE_CODE)
        if col_esg_risk is None:
            raise ValueError("Could not locate ESG Risk Score column after merge")

        eligible_df['ESG_Risk_Score'] = pd.to_numeric(eligible_df[col_esg_risk], errors='coerce')

        scored_eligible_df = eligible_df[eligible_df['ESG_Risk_Score'].notna()].copy()
        logger.info(f"Eligible companies with ESG Risk Score: {len(scored_eligible_df)}")

        scored_eligible_df = scored_eligible_df.sort_values(
            ['ESG_Risk_Score', 'FFMC_CO'],
            ascending=[True, False]
        )
        scored_eligible_df['ESG_Risk_Rank'] = range(1, len(scored_eligible_df) + 1)

        # ----------------------------------------------------------------
        # Step 4: Select top 40 by ESG Risk Score ranking
        # ----------------------------------------------------------------
        logger.info("Step 4: Selecting top 40 companies by ESG Risk Score...")

        top_40_df = scored_eligible_df.head(40).copy()
        logger.info(f"Selected {len(top_40_df)} companies for index")

        # ----------------------------------------------------------------
        # Weighting: Equal Weight using Close Prc_EOD
        # ----------------------------------------------------------------
        logger.info("Calculating equal-weight Number of Shares using Close Prc_EOD...")

        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]

        num_constituents = len(top_40_df)
        target_mcap_per_company = index_mcap / num_constituents

        top_40_df['Unrounded NOSH'] = (
            target_mcap_per_company
            / (top_40_df['Close Prc_EOD'] * top_40_df['FX/Index Ccy'])
        )
        top_40_df['Rounded NOSH'] = top_40_df['Unrounded NOSH'].round()

        # Free Float Factor and Capping Factor not applied per rulebook
        top_40_df['Free Float'] = 1
        top_40_df['Capping Factor'] = 1

        # ----------------------------------------------------------------
        # Final index composition DataFrame
        # ----------------------------------------------------------------
        EEEPR_df = (
            top_40_df[
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
            top_40_df,
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
            eeepr_path = os.path.join(output_dir, f'EEEPR_df_{timestamp}.xlsx')

            logger.info(f"Saving EEEPR output to: {eeepr_path}")
            with pd.ExcelWriter(eeepr_path) as writer:
                EEEPR_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                top_40_df.to_excel(writer, sheet_name='Top 40 Selection', index=False)
                scored_eligible_df.to_excel(writer, sheet_name='ESG Ranked Eligible 60', index=False)
                eligible_df.to_excel(writer, sheet_name='FFMC Eligible 60', index=False)
                excluded_df.to_excel(writer, sheet_name='Excluded Companies', index=False)
                post_exclusion_df.to_excel(writer, sheet_name='Post Exclusion Universe', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(
                    writer, sheet_name='Index Market Cap', index=False
                )

            return {
                "status": "success",
                "message": "EEEPR review completed successfully",
                "data": {"eeepr_path": eeepr_path}
            }

        except Exception as e:
            error_msg = f"Error saving output file: {str(e)}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg, "data": None}

    except Exception as e:
        logger.error(f"Error during EEEPR review calculation: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "message": f"Error during EEEPR review calculation: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None
        }