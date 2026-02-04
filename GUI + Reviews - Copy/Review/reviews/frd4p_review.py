import pandas as pd
import numpy as np
from datetime import datetime
import os
import traceback
from config import DLF_FOLDER, DATA_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data
from utils.inclusion_exclusion import inclusion_exclusion_analysis
from utils.capping_proportional import apply_proportional_capping

logger = setup_logging(__name__)

def run_frd4p_review(date, co_date, effective_date, index="FRD4P", isin="FRIX00003031", 
                    area="US", area2="EU", type="STOCK", universe="Developed Market", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        co_date (str): Close-out date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "FRD4P"
        isin (str, optional): ISIN code. Defaults to "FRIX00003031"
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
        index_eod_df, stock_eod_df, stock_co_df, fx_lookup_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)
        
        logger.info("Loading reference data...")
        ref_data = load_reference_data(current_data_folder, ['ff', 'developed_market', 'icb', 'sesamm', 'oekom_trustcarbon', 'nace'])
        
        # Extract the needed DataFrames
        ff_df = ref_data['ff']
        developed_market_df = ref_data['developed_market']
        icb_df = ref_data['icb']
        Oekom_TrustCarbon_df = ref_data['oekom_trustcarbon']
        nace_df = ref_data['nace']
        sesamm_df = ref_data['sesamm']
        
        sesamm_df = sesamm_df.drop_duplicates(subset='ISIN', keep='first')
        
        failed_files = []
        file_mappings = {
            'ff_df': 'ff',
            'developed_market_df': 'developed_market', 
            'icb_df': 'icb',
            'Oekom_TrustCarbon_df': 'oekom_trustcarbon',
            'nace_df': 'nace',
            'sesamm_df': 'sesamm'
        }

        for df_name, file_key in file_mappings.items():
            df_value = ref_data[file_key]
            if df_value is None:
                failed_files.append(f"{file_key} (assigned to {df_name})")

        if failed_files:
            failed_files_str = ", ".join(failed_files)
            error_msg = f"Failed to load the following reference data files: {failed_files_str}"
            logger.error(error_msg)
            raise ValueError(error_msg) 
        
        # Keep MIC to distinguish between different symbols for the same ISIN
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol', 'MIC']].drop_duplicates(subset=['Isin Code', 'MIC'], keep='first').drop_duplicates(subset=['Isin Code', 'MIC'], keep='first')

        # Chain all data preparation operations
        developed_market_df = (developed_market_df
            # Merge symbols using both ISIN and MIC
            .merge(
                symbols_filtered,
                left_on=['ISIN', 'MIC'],
                right_on=['Isin Code', 'MIC'],
                how='left'
            )
            .drop('Isin Code', axis=1)
            # Merge FX data using both Symbol and MIC
            .merge(
                stock_eod_df[stock_eod_df['Index Curr'] == currency][['#Symbol', 'MIC', 'FX/Index Ccy']].drop_duplicates(subset=['#Symbol', 'MIC'], keep='first'),
                on=['#Symbol', 'MIC'],
                how='left'
            )
            # Merge EOD prices using both Symbol and MIC
            .merge(
                stock_eod_df[['#Symbol', 'MIC', 'Close Prc']].drop_duplicates(subset=['#Symbol', 'MIC'], keep='first'),
                on=['#Symbol', 'MIC'],
                how='left',
                suffixes=('', '_EOD')
            )
            .rename(columns={'Close Prc': 'Close Prc_EOD'})
            # Merge CO prices using both Symbol and MIC
            .merge(
                stock_co_df[['#Symbol', 'MIC', 'Close Prc']].drop_duplicates(subset=['#Symbol', 'MIC'], keep='first'),
                on=['#Symbol', 'MIC'],
                how='left',
                suffixes=('_EOD', '_CO')
            )
            .rename(columns={'Close Prc': 'Close Prc_CO'})
            # Merge FF data for Free Float
            .merge(
                ff_df[['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset='ISIN Code:', keep='first'),
                left_on='ISIN',
                right_on='ISIN Code:',
                how='left'
            )
            .drop('ISIN Code:', axis=1)
            .rename(columns={'Free Float Round:': 'Free Float'})
        )

        # Calculate market cap columns right after the merges
        developed_market_df['Price in Index Currency'] = developed_market_df['Close Prc_EOD'] * developed_market_df['FX/Index Ccy']
        developed_market_df['Original market cap'] = (
            developed_market_df['Price in Index Currency'] * 
            developed_market_df['NOSH'] * 
            developed_market_df['Free Float']
        )
        
        # Add Flag for XPAR or NON Xpar MIC
        developed_market_df['XPAR Flag'] = developed_market_df['MIC'].apply(lambda x: 1 if x == 'XPAR' else 0)

        # Add Area Flag
        developed_market_df['Area Flag'] = developed_market_df['index'].apply(
            lambda x: 'NA' if 'NA500' in str(x) 
            else 'AS' if 'AS500' in str(x)
            else 'EU' if ('EU500' in str(x) or 'EZ300' in str(x))
            else None
        )

        # ===================================================================
        # EARLY MERGE: Merge ALL Oekom, NACE, ICB, and SesamM data at the beginning
        # ===================================================================
        logger.info("Merging all reference data points early...")
        
        # Deduplicate all reference dataframes BEFORE merging to prevent duplicates
        logger.info("Deduplicating reference dataframes...")
        Oekom_TrustCarbon_df = Oekom_TrustCarbon_df.drop_duplicates(subset='ISIN', keep='first')
        nace_df = nace_df.drop_duplicates(subset='ISIN', keep='first')
        icb_df = icb_df.drop_duplicates(subset='ISIN Code', keep='first')
        # sesamm_df already deduplicated at line 67

        # Merge Oekom data
        logger.info("Merging Oekom data...")
        oekom_columns_to_merge = [
            'ISIN',
            'ClimateGHGReductionTargets',
            'NBR Overall Flag',
            'Anti-personnel Mines - Overall Flag',
            'Biological Weapons - Overall Flag',
            'Chemical Weapons - Overall Flag',
            'Cluster Munitions - Overall Flag',
            'Depleted Uranium - Overall Flag',
            'Incendiary Weapons - Overall Flag',
            'Nuclear Weapons Outside NPT - Overall Flag',
            'Nuclear Weapons Inside NPT - Overall Flag',
            'White Phosphorous Weapons - Overall Flag',
            'Coal Mining and Power Gen - Maximum Percentage of Revenues (%)',
            'FossilFuelProdMaxRev-values',
            'FossilFuelDistMaxRev-values',
            'Power Generation - Thermal Maximum Percentage of Revenues (%)',
            'Tobacco - Production Maximum Percentage of Revenues (%)',
            'Tobacco - Distribution Maximum Percentage of Revenues (%)',
            'CRStaffRatingNum'
        ]

        developed_market_df = developed_market_df.merge(
            Oekom_TrustCarbon_df[oekom_columns_to_merge],
            on='ISIN',
            how='left'
        )

        # Merge NACE data
        logger.info("Merging NACE data...")
        developed_market_df = developed_market_df.merge(
            nace_df[['ISIN', 'NACE']],
            on='ISIN',
            how='left'
        )

        # Merge ICB data
        logger.info("Merging ICB data...")
        # Check which columns are available in ICB dataframe
        icb_columns_to_merge = ['ISIN Code', 'Supersector Code']
        if 'Supersector Name' in icb_df.columns:
            icb_columns_to_merge.append('Supersector Name')
        
        developed_market_df = developed_market_df.merge(
            icb_df[icb_columns_to_merge],
            left_on='ISIN',
            right_on='ISIN Code',
            how='left'
        ).drop('ISIN Code', axis=1)

        # Merge SesamM data
        logger.info("Merging SesamM data...")
        developed_market_df = developed_market_df.merge(
            sesamm_df[['ISIN', 'layoff_score_6m', 'Job_score_3Y']],
            on='ISIN',
            how='left'
        )

        # Convert numeric columns AFTER merging
        logger.info("Converting numeric columns...")
        numeric_columns = {
            'Coal Mining and Power Gen - Maximum Percentage of Revenues (%)': 'Coal Mining Numeric',
            'FossilFuelProdMaxRev-values': 'Fossil Fuel Prod Numeric',
            'FossilFuelDistMaxRev-values': 'Fossil Fuel Dist Numeric',
            'Power Generation - Thermal Maximum Percentage of Revenues (%)': 'Thermal Power Numeric',
            'Tobacco - Production Maximum Percentage of Revenues (%)': 'Tobacco Prod Numeric',
            'Tobacco - Distribution Maximum Percentage of Revenues (%)': 'Tobacco Dist Numeric',
            'CRStaffRatingNum': 'CRStaffRatingNum Numeric',
            'layoff_score_6m': 'layoff_score_6m Numeric',
            'Job_score_3Y': 'Job_score_3Y Numeric'
        }

        # After converting to numeric, fill NaN with 0
        
        for original_col, numeric_col in numeric_columns.items():
            developed_market_df[numeric_col] = pd.to_numeric(
                developed_market_df[original_col], 
                errors='coerce'
            )
            
        developed_market_df['Job_score_3Y Numeric'] = developed_market_df['Job_score_3Y Numeric'].fillna(0)
        developed_market_df['CRStaffRatingNum Numeric'] = developed_market_df['CRStaffRatingNum Numeric'].fillna(0)
        
        # Create NACE Climate Impact Classification
        developed_market_df['NACE First Letter'] = developed_market_df['NACE'].str[0]
        high_impact_nace_letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'M']  # FIXED: Changed L to M
        developed_market_df['NACE Climate Impact'] = developed_market_df['NACE First Letter'].apply(
            lambda x: 'High' if x in high_impact_nace_letters else 'Low' if pd.notna(x) else 'Unknown'
        )

        # Create helper column for Fossil Fuel total
        developed_market_df['Fossil Fuel Total Numeric'] = (
            developed_market_df['Fossil Fuel Prod Numeric'].fillna(0) + 
            developed_market_df['Fossil Fuel Dist Numeric'].fillna(0)
        )

        logger.info("All reference data merged successfully")
        logger.info(f"Universe size after merges: {len(developed_market_df)} companies")

        # ===================================================================
        # START EXCLUSION CRITERIA
        # ===================================================================
        exclusion_count = 1

        # Currency exclusion
        logger.info("Applying currency exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}'] = None
        allowed_currencies = ['EUR', 'JPY', 'USD', 'CAD', 'GBP']
        excluded_currency = developed_market_df[
            ~developed_market_df['Currency (Local)'].isin(allowed_currencies)
        ]['ISIN'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}'] = np.where(
            developed_market_df['ISIN'].isin(excluded_currency),
            'exclude_currency',
            None
        )
        logger.info(f"Currency exclusions: {len(excluded_currency)}")
        exclusion_count += 1

        # SesamM Layoff score exclusion (companies without data)
        logger.info("Applying SesamM data availability exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}'] = None
        excluded_no_sesamm = developed_market_df[
            developed_market_df['layoff_score_6m'].isna()
        ]['ISIN'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}'] = np.where(
            developed_market_df['ISIN'].isin(excluded_no_sesamm),
            'exclude_no_sesamm_data',
            None
        )
        logger.info(f"SesamM data unavailable exclusions: {len(excluded_no_sesamm)}")
        exclusion_count += 1

        # Turnover EUR exclusion
        logger.info("Applying turnover exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}'] = None
        excluded_turnover = developed_market_df[
            developed_market_df['3 months ADTV'] < 10000000
        ]['ISIN'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}'] = np.where(
            developed_market_df['ISIN'].isin(excluded_turnover),
            'exclude_turnover_EUR',
            None
        )
        logger.info(f"Turnover exclusions: {len(excluded_turnover)}")
        exclusion_count += 1

        # NBR Overall Flag exclusion
        logger.info("Applying NBR Overall Flag exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}'] = None
        NBR_Overall_Flag_Red = developed_market_df[
            developed_market_df['NBR Overall Flag'] == 'RED'
        ]['ISIN'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}'] = np.where(
            developed_market_df['ISIN'].isin(NBR_Overall_Flag_Red),
            'exclude_NBROverallFlag',
            None
        )
        logger.info(f"NBR Overall Flag exclusions: {len(NBR_Overall_Flag_Red)}")
        exclusion_count += 1

        # Weapons exclusions
        logger.info("Applying controversial weapons exclusions...")
        weapons_columns = {
            'Biological Weapons - Overall Flag': ('exclude_BiologicalWeaponsFlag', 'BiologicalWeapons'),
            'Chemical Weapons - Overall Flag': ('exclude_ChemicalWeaponsFlag', 'ChemicalWeapons'),
            'Nuclear Weapons Inside NPT - Overall Flag': ('exclude_NuclearWeaponsFlag', 'NuclearWeapons'),
            'Nuclear Weapons Outside NPT - Overall Flag': ('exclude_NuclearWeaponsNonNPTFlag', 'NuclearWeaponsNonNPT'),
            'Cluster Munitions - Overall Flag': ('exclude_ClusterMunitionsFlag', 'ClusterMunitions'),
            'Depleted Uranium - Overall Flag': ('exclude_DepletedUraniumFlag', 'DepletedUranium'),
            'Anti-personnel Mines - Overall Flag': ('exclude_APMinesFlag', 'APMines'),
            'White Phosphorous Weapons - Overall Flag': ('exclude_WhitePhosphorusFlag', 'WhitePhosphorus')
        }

        for column, (exclude_value, label) in weapons_columns.items():
            developed_market_df[f'exclusion_{exclusion_count}_{label}'] = None
            
            flagged_isins = developed_market_df[
                developed_market_df[column].isin(['RED', 'Amber'])
            ]['ISIN'].tolist()
            
            developed_market_df[f'exclusion_{exclusion_count}_{label}'] = np.where(
                developed_market_df['ISIN'].isin(flagged_isins),
                exclude_value,
                None
            )
            logger.info(f"{label} exclusions: {len(flagged_isins)}")
            exclusion_count += 1

        # Energy Screening - Coal
        logger.info("Applying coal mining exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}_CoalMining'] = None
        excluded_coal = developed_market_df[
            developed_market_df['Coal Mining Numeric'] >= 0.01
        ]['ISIN'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}_CoalMining'] = np.where(
            developed_market_df['ISIN'].isin(excluded_coal),
            'exclude_CoalMining',
            None
        )
        logger.info(f"Coal Mining exclusions: {len(excluded_coal)}")
        exclusion_count += 1

        # Energy Screening - Fossil Fuel
        logger.info("Applying fossil fuel exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}_FossilFuel'] = None
        excluded_fossil = developed_market_df[
            developed_market_df['Fossil Fuel Total Numeric'] >= 0.10
        ]['ISIN'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}_FossilFuel'] = np.where(
            developed_market_df['ISIN'].isin(excluded_fossil),
            'exclude_FossilFuel',
            None
        )
        logger.info(f"Fossil Fuel exclusions: {len(excluded_fossil)}")
        exclusion_count += 1

        # Energy Screening - Thermal Power
        logger.info("Applying thermal power exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}_ThermalPower'] = None
        excluded_thermal = developed_market_df[
            developed_market_df['Thermal Power Numeric'] >= 0.50
        ]['ISIN'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}_ThermalPower'] = np.where(
            developed_market_df['ISIN'].isin(excluded_thermal),
            'exclude_ThermalPower',
            None
        )
        logger.info(f"Thermal Power exclusions: {len(excluded_thermal)}")
        exclusion_count += 1

        # SBT alignment exclusion - NOW USES MERGED DATA
        logger.info("Applying SBT alignment exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}_SBT'] = None

        # Log companies with non-Approved SBT for debugging
        non_approved_sbt = developed_market_df[
            (developed_market_df['ClimateGHGReductionTargets'].notna()) &
            (developed_market_df['ClimateGHGReductionTargets'] != 'Approved SBT')
        ]
        logger.info(f"Companies with non-Approved SBT: {len(non_approved_sbt)}")
        
        # Check NACE classification distribution
        logger.info(f"NACE Climate Impact distribution:")
        logger.info(f"  High: {(developed_market_df['NACE Climate Impact'] == 'High').sum()}")
        logger.info(f"  Low: {(developed_market_df['NACE Climate Impact'] == 'Low').sum()}")
        logger.info(f"  Unknown: {(developed_market_df['NACE Climate Impact'] == 'Unknown').sum()}")

        # Companies that should be excluded:
        # 1. Not 'Approved SBT' 
        # 2. In high climate impact NACE section
        # 3. OR missing NACE data (Unknown should also be excluded for high-impact assumption)
        sbt_excluded_isins = developed_market_df[
            (developed_market_df['ClimateGHGReductionTargets'] != 'Approved SBT') & 
            ((developed_market_df['NACE Climate Impact'] == 'High') |
             (developed_market_df['NACE Climate Impact'] == 'Unknown'))  # Exclude if NACE is missing
        ]['ISIN'].tolist()

        developed_market_df[f'exclusion_{exclusion_count}_SBT'] = np.where(
            developed_market_df['ISIN'].isin(sbt_excluded_isins),
            'exclude_SBT_NACE',
            None
        )
        logger.info(f"SBT alignment exclusions: {len(sbt_excluded_isins)}")
        
        # Log specific companies for debugging
        amazon_check = developed_market_df[developed_market_df['Name'].str.contains('Amazon', case=False, na=False)]
        if not amazon_check.empty:
            for idx, row in amazon_check.iterrows():
                logger.info(f"Amazon debug - ISIN: {row['ISIN']}, "
                          f"ClimateGHGReductionTargets: {row['ClimateGHGReductionTargets']}, "
                          f"NACE: {row['NACE']}, "
                          f"NACE Climate Impact: {row['NACE Climate Impact']}, "
                          f"Excluded: {row[f'exclusion_{exclusion_count}_SBT']}")
        
        exclusion_count += 1

        # Tobacco Screening - Production
        logger.info("Applying tobacco production exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}_TobaccoProduction'] = None
        excluded_tobacco_prod = developed_market_df[
            developed_market_df['Tobacco Prod Numeric'] > 0
        ]['ISIN'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}_TobaccoProduction'] = np.where(
            developed_market_df['ISIN'].isin(excluded_tobacco_prod),
            'exclude_TobaccoProduction',
            None
        )
        logger.info(f"Tobacco Production exclusions: {len(excluded_tobacco_prod)}")
        exclusion_count += 1

        # Tobacco Screening - Distribution
        logger.info("Applying tobacco distribution exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}_TobaccoDistribution'] = None
        excluded_tobacco_dist = developed_market_df[
            developed_market_df['Tobacco Dist Numeric'] >= 0.15
        ]['ISIN'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}_TobaccoDistribution'] = np.where(
            developed_market_df['ISIN'].isin(excluded_tobacco_dist),
            'exclude_TobaccoDistribution',
            None
        )
        logger.info(f"Tobacco Distribution exclusions: {len(excluded_tobacco_dist)}")
        exclusion_count += 1

        # Layoff Screening
        logger.info("Applying layoff screening exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}_Layoff'] = None
        excluded_layoff = developed_market_df[
            developed_market_df['layoff_score_6m Numeric'] > 0
        ]['ISIN'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}_Layoff'] = np.where(
            developed_market_df['ISIN'].isin(excluded_layoff),
            'exclude_Layoff',
            None
        )
        logger.info(f"Layoff exclusions: {len(excluded_layoff)}")
        exclusion_count += 1

        # Staff Rating Screening - SIMPLIFIED
        logger.info("Applying staff rating exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}_StaffRating'] = None

        # Fill NaN staff ratings with 3 (neutral)
        developed_market_df['CRStaffRatingNum Numeric'] = developed_market_df['CRStaffRatingNum Numeric'].fillna(3)

        excluded_staff_isins = []

        for (sector, area), group in developed_market_df.groupby(['Supersector Code', 'Area Flag']):
            if pd.isna(sector) or pd.isna(area):
                continue
                
            logger.debug(f"Processing sector: {sector}, area: {area}, group size: {len(group)}")
            sorted_group = group.sort_values('CRStaffRatingNum Numeric')
            n_companies = len(group)
            n_to_exclude = int(np.floor(n_companies * 0.20))  # Simplified to 0.20
            logger.debug(f"Companies in group: {n_companies}, to exclude: {n_to_exclude}")
            
            if n_companies > 0 and n_to_exclude > 0:
                bottom_isins = sorted_group['ISIN'].iloc[:n_to_exclude].tolist()
                excluded_staff_isins.extend(bottom_isins)

        developed_market_df[f'exclusion_{exclusion_count}_StaffRating'] = np.where(
            developed_market_df['ISIN'].isin(excluded_staff_isins),
            'exclude_StaffRating',
            None
        )
        logger.info(f"Staff Rating exclusions: {len(excluded_staff_isins)}")
        exclusion_count += 1

        # ===================================================================
        # CREATE EXCLUSION SUMMARY
        # ===================================================================
        exclusion_columns = [col for col in developed_market_df.columns if col.startswith('exclusion_')]

        def summarize_exclusions(row, exclusion_cols):
            """Summarize all exclusion reasons for a company"""
            exclusions = []
            for col in exclusion_cols:
                if pd.notna(row[col]):
                    reason = row[col].replace('exclude_', '')
                    exclusions.append(reason)
            
            if not exclusions:
                return 'Included'
            else:
                return '; '.join(exclusions)

        developed_market_df['Exclusion Summary'] = developed_market_df.apply(
            lambda row: summarize_exclusions(row, exclusion_columns), axis=1
        )

        developed_market_df['Excluded'] = developed_market_df['Exclusion Summary'].apply(
            lambda x: 'No' if x == 'Included' else 'Yes'
        )

        # ===================================================================
        # SELECTION PROCESS
        # ===================================================================
        # Select companies that have no exclusions
        selection_df = developed_market_df[
            developed_market_df[exclusion_columns].isna().all(axis=1)
        ].copy()
        
        logger.info(f"Companies remaining after exclusions: {len(selection_df)}")
        
        # Check if we have enough companies per region
        xpar_eligible = selection_df[selection_df['MIC'] == 'XPAR']
        noxpar_eligible = selection_df[selection_df['MIC'] != 'XPAR']
        
        logger.info(f"Eligible companies - XPAR: {len(xpar_eligible)}, Non-XPAR: {len(noxpar_eligible)}")
        
        # TODO: Add fallback mechanism if less than 25 per region
        # (See rulebook: "Should this list count less than 50 companies the threshold 
        # on the Invest_In_France_Layoff_Score will be increased until reaching 25 companies in each Region")
        if len(xpar_eligible) < 25 or len(noxpar_eligible) < 25:
            logger.warning(f"Insufficient eligible companies (need 25 per region). Consider implementing fallback mechanism.")

        # Create ranking within each MIC type (XPAR vs non-XPAR)
        selection_df['MIC_Type'] = selection_df['MIC'].apply(lambda x: 'XPAR' if x == 'XPAR' else 'Non-XPAR')
        
        # Sort by MIC_Type, Job_score_3Y (descending), and CRStaffRatingNum (descending)
        selection_df = selection_df.sort_values(
            ['MIC_Type', 'Job_score_3Y Numeric', 'CRStaffRatingNum Numeric'],
            ascending=[True, False, False]
        )
        
        # Assign combined rank within each MIC_Type group
        selection_df['Combined Rank'] = selection_df.groupby('MIC_Type').cumcount() + 1
        
        # Add ranking to full universe for visibility
        developed_market_df['MIC_Type'] = developed_market_df['MIC'].apply(lambda x: 'XPAR' if x == 'XPAR' else 'Non-XPAR')
        developed_market_df = developed_market_df.merge(
            selection_df[['ISIN', 'Combined Rank']],
            on='ISIN',
            how='left'
        )
        
        def select_top_stocks(df, mic_type, n_stocks):
            """Select top N stocks for a given MIC type"""
            if mic_type == 'XPAR':
                filtered_df = df[df['MIC'] == 'XPAR'].copy()
            else:
                filtered_df = df[df['MIC'] != 'XPAR'].copy()
            
            sorted_df = filtered_df.sort_values(
                by=['Job_score_3Y Numeric', 'CRStaffRatingNum Numeric'],
                ascending=[False, False],
                na_position='last'
            )
            
            return sorted_df.head(n_stocks)

        # Select stocks for full selection (25+25)
        xpar_selected_25 = select_top_stocks(selection_df, 'XPAR', 25)
        noxpar_selected_25 = select_top_stocks(selection_df, 'NOXPAR', 25)
        full_selection_df = pd.concat([xpar_selected_25, noxpar_selected_25])
        
        # Select stocks for final index (20+20)
        xpar_selected_20 = select_top_stocks(selection_df, 'XPAR', 20)
        noxpar_selected_20 = select_top_stocks(selection_df, 'NOXPAR', 20)

        # ===================================================================
        # EQUAL WEIGHTING WITH CAPPING
        # ===================================================================
        # Get index market cap
        logger.info(f"Checking IsinCode {isin} in index_eod_df")
        matching_rows = index_eod_df[index_eod_df['IsinCode'] == isin]
        logger.info(f"Found {len(matching_rows)} matching rows")
        if len(matching_rows) > 0:
            index_mkt_cap = matching_rows['Mkt Cap'].iloc[0]
        else:
            logger.error(f"No matching index found for ISIN {isin}")
            raise ValueError(f"No matching index found for ISIN {isin}")

        logger.info(f"Index market cap: {index_mkt_cap}")
        
        # Each region should represent 50% of the index
        # Within each region, stocks are weighted by their market cap
        # But no single stock can exceed 10% of total index (= 20% of region)
        
        # Calculate total market cap for each region
        xpar_total_mcap = xpar_selected_20['Original market cap'].sum()
        noxpar_total_mcap = noxpar_selected_20['Original market cap'].sum()
        
        logger.info(f"XPAR region total mcap: {xpar_total_mcap:,.0f}")
        logger.info(f"Non-XPAR region total mcap: {noxpar_total_mcap:,.0f}")
        
        # Calculate initial weights within each region (before capping)
        xpar_selected_20['Regional Weight'] = xpar_selected_20['Original market cap'] / xpar_total_mcap
        noxpar_selected_20['Regional Weight'] = noxpar_selected_20['Original market cap'] / noxpar_total_mcap
        
        logger.info("Applying capping: max 20% per stock within each region (= 10% of total index)...")
        
        # Apply proportional capping within each region
        # Each region is treated as a separate portfolio that will be 50% of the total index
        # So 20% cap within region = 10% of total index
        xpar_selected_20 = apply_proportional_capping(
            xpar_selected_20,
            mcap_column='Original market cap',
            max_weight=0.20,  # 20% of the region
            max_iterations=100
        )

        noxpar_selected_20 = apply_proportional_capping(
            noxpar_selected_20,
            mcap_column='Original market cap',
            max_weight=0.20,  # 20% of the region
            max_iterations=100
        )
        
        # The 'Current Weight' from capping is relative to the region (sums to 1.0)
        # Convert to index weight by scaling to 50% of total index
        xpar_selected_20['Index Weight'] = xpar_selected_20['Current Weight'] * 0.5
        noxpar_selected_20['Index Weight'] = noxpar_selected_20['Current Weight'] * 0.5
        
        # Calculate Final Capping Factor
        # This is what gets applied to the number of shares
        # Capping Factor = (Index Weight × Total Index Mcap) / Original Mcap
        xpar_selected_20['Final Capping'] = (xpar_selected_20['Index Weight'] * index_mkt_cap) / xpar_selected_20['Original market cap']
        noxpar_selected_20['Final Capping'] = (noxpar_selected_20['Index Weight'] * index_mkt_cap) / noxpar_selected_20['Original market cap']
        
        # Log final weights for verification
        logger.info(f"XPAR region - Index weight sum: {xpar_selected_20['Index Weight'].sum():.4f} (should be ~0.50)")
        logger.info(f"XPAR region - Max index weight: {xpar_selected_20['Index Weight'].max():.4f} (should be <= 0.10)")
        logger.info(f"Non-XPAR region - Index weight sum: {noxpar_selected_20['Index Weight'].sum():.4f} (should be ~0.50)")
        logger.info(f"Non-XPAR region - Max index weight: {noxpar_selected_20['Index Weight'].max():.4f} (should be <= 0.10)")
        
        # Verify no stock exceeds 10% of total index
        max_xpar_weight = xpar_selected_20['Index Weight'].max()
        max_noxpar_weight = noxpar_selected_20['Index Weight'].max()
        if max_xpar_weight > 0.10 or max_noxpar_weight > 0.10:
            logger.warning(f"WARNING: Stock exceeds 10% cap! XPAR max: {max_xpar_weight:.4f}, Non-XPAR max: {max_noxpar_weight:.4f}")

        # Normalize capping factors
        max_capping = max(xpar_selected_20['Final Capping'].max(), noxpar_selected_20['Final Capping'].max())
        xpar_selected_20['Final Capping'] = (xpar_selected_20['Final Capping'] / max_capping).round(14)
        noxpar_selected_20['Final Capping'] = (noxpar_selected_20['Final Capping'] / max_capping).round(14)

        # Combine final selections
        final_selection_df = pd.concat([xpar_selected_20, noxpar_selected_20])
        final_selection_df['Effective Date of Review'] = effective_date

        # ===================================================================
        # CREATE FINAL OUTPUT
        # ===================================================================
        # Create final output DataFrame
        FRD4P_df = final_selection_df[[
            'Name', 
            'ISIN', 
            'MIC', 
            'NOSH', 
            'Free Float',
            'Final Capping',
            'Effective Date of Review',
            'Currency (Local)'
        ]].copy().rename(columns={
            'Name': 'Company',
            'ISIN': 'ISIN Code',
            'NOSH': 'Number of Shares',
            'Final Capping': 'Capping Factor',
            'Currency (Local)': 'Currency'
        }).sort_values('Company')

        # Perform Inclusion/Exclusion Analysis
        analysis_results = inclusion_exclusion_analysis(
            FRD4P_df, 
            stock_eod_df, 
            index, 
            isin_column='ISIN Code'
        )
        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']
        
        # ===================================================================
        # SAVE OUTPUT FILES
        # ===================================================================
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            # Create filename with timestamp to avoid conflicts
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            frd4p_path = os.path.join(output_dir, f'FRD4P_df_{timestamp}.xlsx')
            
            # Save with timestamp to avoid any conflicts
            logger.info(f"Saving FRD4P output to: {frd4p_path}")
            with pd.ExcelWriter(frd4p_path) as writer:
                # Write each DataFrame to a different sheet
                FRD4P_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                developed_market_df.to_excel(writer, sheet_name='Full Universe', index=False)
                selection_df.to_excel(writer, sheet_name='Eligible Companies', index=False)
                final_selection_df.to_excel(writer, sheet_name='Final Selection', index=False)
                full_selection_df.to_excel(writer, sheet_name='Full Selection 50', index=False)
                
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "frd4p_path": frd4p_path
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