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

def run_wifrp_review(date, co_date, effective_date, index="WIFRP", isin="FRIX00002777", 
                    area="US", area2="EU", type="STOCK", universe="Developed Market", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the World Invest In France index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        co_date (str): Close-out date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "WIFRP"
        isin (str, optional): ISIN code. Defaults to "FRIX00002777"
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
        ref_data = load_reference_data(current_data_folder, ['ff', 'developed_market', 'icb', 'sesamm', 'oekom_trustcarbon'])
        
        # Extract the needed DataFrames
        ff_df = ref_data['ff']
        developed_market_df = ref_data['developed_market']
        icb_df = ref_data['icb']
        Oekom_TrustCarbon_df = ref_data['oekom_trustcarbon']
        sesamm_df = ref_data['sesamm']
        
        failed_files = []
        file_mappings = {
            'ff_df': 'ff',
            'developed_market_df': 'developed_market', 
            'icb_df': 'icb',
            'Oekom_TrustCarbon_df': 'oekom_trustcarbon',
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
        ][['Isin Code', '#Symbol', 'MIC']].drop_duplicates(subset=['Isin Code', 'MIC'], keep='first')

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
            .rename(columns={'Free Float Round:': 'Free Float', 'Name': 'Company', 'ISIN': 'ISIN Code', 'NOSH': 'Number of Shares', 'Currency (Local)': 'Currency'})
        )

        # Calculate market cap columns right after the merges
        developed_market_df['Price in Index Currency'] = developed_market_df['Close Prc_EOD'] * developed_market_df['FX/Index Ccy']
        developed_market_df['Original market cap'] = (
            developed_market_df['Price in Index Currency'] * 
            developed_market_df['Number of Shares'] * 
            developed_market_df['Free Float']
        )
        
        # STEP 1: Filter out Euronext Paris (XPAR) listings
        # Rulebook: "do not have their Main Listing on Euronext Paris at review"
        logger.info("Filtering universe to exclude Euronext Paris (XPAR) listings...")
        developed_market_df = developed_market_df[developed_market_df['MIC'] != 'XPAR'].copy()
        
        # Add Area/Region Flag
        developed_market_df['Region'] = developed_market_df['index'].apply(
            lambda x: 'NA' if 'NA500' in str(x) 
            else 'AS' if 'AS500' in str(x)
            else 'EU' if ('EU500' in str(x) or 'EZ300' in str(x))
            else None
        )

        # ===================================================================
        # EARLY MERGE: Merge ALL Oekom, ICB, and SesamM data at the beginning
        # ===================================================================
        logger.info("Merging all reference data points early...")
        
        # Deduplicate all reference dataframes BEFORE merging to prevent duplicates
        logger.info("Deduplicating reference dataframes...")
        Oekom_TrustCarbon_df = Oekom_TrustCarbon_df.drop_duplicates(subset='ISIN', keep='first')
        icb_df = icb_df.drop_duplicates(subset='ISIN Code', keep='first')
        sesamm_df = sesamm_df.drop_duplicates(subset='ISIN', keep='first')

        # Merge Oekom data
        logger.info("Merging Oekom data...")
        oekom_columns_to_merge = [
            'ISIN',
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
            'Fossil Fuel - Total Maximum Percentage of Revenues (%)',
            'Power Generation - Thermal Maximum Percentage of Revenues (%)',
            'Tobacco - Production Maximum Percentage of Revenues (%)',
            'Tobacco - Distribution Maximum Percentage of Revenues (%)',
            'CRStaffRatingNum'
        ]
        
        # Add animal welfare columns if they exist
        animal_columns = ['AnimalWelfareInvolvement', 'AnimalWelfareAnimTestInvolvement']
        for col in animal_columns:
            if col in Oekom_TrustCarbon_df.columns:
                oekom_columns_to_merge.append(col)

        developed_market_df = developed_market_df.merge(
            Oekom_TrustCarbon_df[oekom_columns_to_merge],
            left_on='ISIN Code',
            right_on='ISIN',
            how='left'
        ).drop('ISIN', axis=1)

        # Merge ICB data
        logger.info("Merging ICB data...")
        icb_columns_to_merge = ['ISIN Code', 'Supersector Code']
        if 'Supersector Name' in icb_df.columns:
            icb_columns_to_merge.append('Supersector Name')
        
        developed_market_df = developed_market_df.merge(
            icb_df[icb_columns_to_merge],
            left_on='ISIN Code',
            right_on='ISIN Code',
            how='left'
        )
        
        # Rename for consistency
        if 'Supersector Code' in developed_market_df.columns:
            developed_market_df = developed_market_df.rename(columns={'Supersector Code': 'Supersector'})

        # Merge SesamM data
        logger.info("Merging SesamM data...")
        sesamm_columns = ['ISIN', 'layoff_score_3m']  # WIFRP uses 3 months, not 6
        # Only merge columns that exist
        sesamm_columns_available = [col for col in sesamm_columns if col in sesamm_df.columns]
        
        if len(sesamm_columns_available) > 1:  # At least ISIN + one other column
            developed_market_df = developed_market_df.merge(
                sesamm_df[sesamm_columns_available],
                left_on='ISIN Code',
                right_on='ISIN',
                how='left'
            ).drop('ISIN', axis=1)
        else:
            logger.warning("layoff_score_3m column not found in SesamM data")
        # Convert numeric columns AFTER merging
        logger.info("Converting numeric columns...")
        numeric_columns = {
            'Coal Mining and Power Gen - Maximum Percentage of Revenues (%)': 'Coal Mining Numeric',
            'Fossil Fuel - Total Maximum Percentage of Revenues (%)': 'Fossil Fuel Numeric',
            'Power Generation - Thermal Maximum Percentage of Revenues (%)': 'Thermal Power Numeric',
            'Tobacco - Production Maximum Percentage of Revenues (%)': 'Tobacco Prod Numeric',
            'Tobacco - Distribution Maximum Percentage of Revenues (%)': 'Tobacco Dist Numeric',
            'CRStaffRatingNum': 'CRStaffRatingNum Numeric',
            'layoff_score_3m': 'layoff_score_3m Numeric'
        }

        for original_col, numeric_col in numeric_columns.items():
            if original_col in developed_market_df.columns:
                developed_market_df[numeric_col] = pd.to_numeric(
                    developed_market_df[original_col], 
                    errors='coerce'
                )

        # Fill NaN with appropriate values for ranking
        if 'CRStaffRatingNum Numeric' in developed_market_df.columns:
            # Fill all NaN values (including 'Not Collected' which was converted to NaN) with 4 (best ranking)
            developed_market_df['CRStaffRatingNum Numeric'] = developed_market_df['CRStaffRatingNum Numeric'].fillna(4)
            
            logger.info(f"Staff Rating distribution: Min={developed_market_df['CRStaffRatingNum Numeric'].min()}, Max={developed_market_df['CRStaffRatingNum Numeric'].max()}")
            logger.info(f"Companies with staff rating = 4 (Missing or Not Collected): {(developed_market_df['CRStaffRatingNum Numeric'] == 4).sum()}")

        logger.info("All reference data merged successfully")
        logger.info(f"Universe size after merges: {len(developed_market_df)} companies")

        # ===================================================================
        # CALCULATE PERCENTILES AND RANKINGS FOR INDIVIDUAL METRICS
        # ===================================================================
        logger.info("Calculating percentiles and rankings for staff rating...")

        # Initialize percentile and rank columns
        developed_market_df['StaffRating_Regional_Sector_Percentile'] = np.nan
        developed_market_df['StaffRating_Regional_Sector_Rank'] = np.nan

        if 'Supersector' in developed_market_df.columns and 'Region' in developed_market_df.columns:
            for (sector, region), group_indices in developed_market_df.groupby(['Supersector', 'Region']).groups.items():
                if pd.isna(sector) or pd.isna(region):
                    continue
                
                # Get the group data
                group_data = developed_market_df.loc[group_indices, 'CRStaffRatingNum Numeric']
                n_companies = len(group_indices)
                
                # Calculate rank within group (1 = best, higher staff rating = better)
                group_rank = group_data.rank(ascending=False, method='min')
                developed_market_df.loc[group_indices, 'StaffRating_Regional_Sector_Rank'] = group_rank
                
                # Calculate percentile manually: (n - rank + 1) / n * 100
                # This gives: rank 1 → 100th percentile, rank n → (1/n)*100 percentile
                group_percentile = ((n_companies - group_rank + 1) / n_companies * 100).round(2)
                developed_market_df.loc[group_indices, 'StaffRating_Regional_Sector_Percentile'] = group_percentile
                
                logger.debug(f"Calculated staff rating percentiles for sector: {sector}, region: {region}, companies: {n_companies}")
                
                # Log the range in this group
                if n_companies > 0:
                    logger.debug(f"  Rating range: {group_data.min():.2f} to {group_data.max():.2f}")
                    logger.debug(f"  Percentile range: {group_percentile.min():.2f} to {group_percentile.max():.2f}")
        else:
            logger.warning("Cannot calculate regional sector percentiles. Missing Supersector or Region columns.")

        logger.info("Percentile and ranking calculations completed")

        # Log overall statistics
        if 'StaffRating_Regional_Sector_Percentile' in developed_market_df.columns:
            logger.info(f"Staff Rating Percentile range: {developed_market_df['StaffRating_Regional_Sector_Percentile'].min():.2f} to {developed_market_df['StaffRating_Regional_Sector_Percentile'].max():.2f}")
            logger.info(f"Companies with percentiles calculated: {developed_market_df['StaffRating_Regional_Sector_Percentile'].notna().sum()}")

        # NOW START EXCLUSION CRITERIA...
        # ===================================================================
        # START EXCLUSION CRITERIA
        # ===================================================================
        exclusion_count = 1

        # 1. Companies without Invest_In_France_Layoff_Score from SESAMm
        logger.info("Applying SesamM data availability exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}'] = None
        excluded_no_sesamm = developed_market_df[
            developed_market_df['layoff_score_3m'].isna()
        ]['ISIN Code'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}'] = np.where(
            developed_market_df['ISIN Code'].isin(excluded_no_sesamm),
            'exclude_layoff_score_missing',
            None
        )
        logger.info(f"SesamM data unavailable exclusions: {len(excluded_no_sesamm)}")
        exclusion_count += 1

        # 2. 3-months Average Daily Traded Value lower than 10 Million EUR
        logger.info("Applying turnover exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}'] = None
        excluded_turnover = developed_market_df[
            developed_market_df['3 months ADTV'] < 10000000
        ]['ISIN Code'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}'] = np.where(
            developed_market_df['ISIN Code'].isin(excluded_turnover),
            'exclude_low_liquidity',
            None
        )
        logger.info(f"Turnover exclusions: {len(excluded_turnover)}")
        exclusion_count += 1

        # 3. Breaches of international standards - NBR Overall Flag "Red"
        logger.info("Applying NBR Overall Flag exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}'] = None
        NBR_Overall_Flag_Red = developed_market_df[
            developed_market_df['NBR Overall Flag'] == 'RED'
        ]['ISIN Code'].tolist()
        developed_market_df[f'exclusion_{exclusion_count}'] = np.where(
            developed_market_df['ISIN Code'].isin(NBR_Overall_Flag_Red),
            'exclude_NBR_red_flag',
            None
        )
        logger.info(f"NBR Overall Flag exclusions: {len(NBR_Overall_Flag_Red)}")
        exclusion_count += 1

        # 4. Controversial Weapons - RED or Amber flags
        logger.info("Applying controversial weapons exclusions...")
        weapons_columns = {
            'Biological Weapons - Overall Flag': ('exclude_biological_weapons', 'BiologicalWeapons'),
            'Chemical Weapons - Overall Flag': ('exclude_chemical_weapons', 'ChemicalWeapons'),
            'Nuclear Weapons Inside NPT - Overall Flag': ('exclude_nuclear_weapons_inside_npt', 'NuclearWeapons'),
            'Nuclear Weapons Outside NPT - Overall Flag': ('exclude_nuclear_weapons_outside_npt', 'NuclearWeaponsNonNPT'),
            'Cluster Munitions - Overall Flag': ('exclude_cluster_munitions', 'ClusterMunitions'),
            'Depleted Uranium - Overall Flag': ('exclude_depleted_uranium', 'DepletedUranium'),
            'Anti-personnel Mines - Overall Flag': ('exclude_anti_personnel_mines', 'APMines'),
            'White Phosphorous Weapons - Overall Flag': ('exclude_white_phosphorus_weapons', 'WhitePhosphorus')
        }

        for column, (exclude_value, label) in weapons_columns.items():
            if column in developed_market_df.columns:
                developed_market_df[f'exclusion_{exclusion_count}_{label}'] = None
                
                flagged_isins = developed_market_df[
                    developed_market_df[column].isin(['RED', 'Amber'])
                ]['ISIN Code'].tolist()
                
                developed_market_df[f'exclusion_{exclusion_count}_{label}'] = np.where(
                    developed_market_df['ISIN Code'].isin(flagged_isins),
                    exclude_value,
                    None
                )
                logger.info(f"{label} exclusions: {len(flagged_isins)}")
                exclusion_count += 1
            else:
                logger.warning(f"Column '{column}' not found. Skipping this exclusion.")

        # 5. Animal Welfare Screening
        logger.info("Applying animal welfare exclusions...")
        
        # Animal Welfare Involvement
        if 'AnimalWelfareInvolvement' in developed_market_df.columns:
            developed_market_df[f'exclusion_{exclusion_count}_AnimalWelfare'] = None
            excluded_animal_welfare = developed_market_df[
                (developed_market_df['AnimalWelfareInvolvement'].notna()) & 
                (developed_market_df['AnimalWelfareInvolvement'] != 0) & 
                (developed_market_df['AnimalWelfareInvolvement'] != '0')
            ]['ISIN Code'].tolist()
            developed_market_df[f'exclusion_{exclusion_count}_AnimalWelfare'] = np.where(
                developed_market_df['ISIN Code'].isin(excluded_animal_welfare),
                'exclude_animal_welfare',
                None
            )
            logger.info(f"Animal Welfare exclusions: {len(excluded_animal_welfare)}")
            exclusion_count += 1
        else:
            logger.warning("AnimalWelfareInvolvement column not found. Skipping.")
        
        # Animal Testing Involvement
        if 'AnimalWelfareAnimTestInvolvement' in developed_market_df.columns:
            developed_market_df[f'exclusion_{exclusion_count}_AnimalTesting'] = None
            excluded_animal_testing = developed_market_df[
                (developed_market_df['AnimalWelfareAnimTestInvolvement'].notna()) & 
                (developed_market_df['AnimalWelfareAnimTestInvolvement'] != 0) & 
                (developed_market_df['AnimalWelfareAnimTestInvolvement'] != '0')
            ]['ISIN Code'].tolist()
            developed_market_df[f'exclusion_{exclusion_count}_AnimalTesting'] = np.where(
                developed_market_df['ISIN Code'].isin(excluded_animal_testing),
                'exclude_animal_testing',
                None
            )
            logger.info(f"Animal Testing exclusions: {len(excluded_animal_testing)}")
            exclusion_count += 1
        else:
            logger.warning("AnimalWelfareAnimTestInvolvement column not found. Skipping.")

        # 6. Energy Screening
        logger.info("Applying energy screening exclusions...")
        
        # Coal Mining and Power Gen > 0%
        if 'Coal Mining Numeric' in developed_market_df.columns:
            developed_market_df[f'exclusion_{exclusion_count}_CoalMining'] = None
            excluded_coal = developed_market_df[
                developed_market_df['Coal Mining Numeric'] > 0
            ]['ISIN Code'].tolist()
            developed_market_df[f'exclusion_{exclusion_count}_CoalMining'] = np.where(
                developed_market_df['ISIN Code'].isin(excluded_coal),
                'exclude_coal_mining',
                None
            )
            logger.info(f"Coal Mining exclusions: {len(excluded_coal)}")
            exclusion_count += 1
        else:
            logger.warning("Coal Mining column not found. Skipping.")
        
        # Fossil Fuel > 0%
        if 'Fossil Fuel Numeric' in developed_market_df.columns:
            developed_market_df[f'exclusion_{exclusion_count}_FossilFuel'] = None
            excluded_fossil = developed_market_df[
                developed_market_df['Fossil Fuel Numeric'] > 0
            ]['ISIN Code'].tolist()
            developed_market_df[f'exclusion_{exclusion_count}_FossilFuel'] = np.where(
                developed_market_df['ISIN Code'].isin(excluded_fossil),
                'exclude_fossil_fuel',
                None
            )
            logger.info(f"Fossil Fuel exclusions: {len(excluded_fossil)}")
            exclusion_count += 1
        else:
            logger.warning("Fossil Fuel column not found. Skipping.")
        
        # Thermal Power Generation > 0%
        if 'Thermal Power Numeric' in developed_market_df.columns:
            developed_market_df[f'exclusion_{exclusion_count}_ThermalPower'] = None
            excluded_thermal = developed_market_df[
                developed_market_df['Thermal Power Numeric'] > 0
            ]['ISIN Code'].tolist()
            developed_market_df[f'exclusion_{exclusion_count}_ThermalPower'] = np.where(
                developed_market_df['ISIN Code'].isin(excluded_thermal),
                'exclude_thermal_power',
                None
            )
            logger.info(f"Thermal Power exclusions: {len(excluded_thermal)}")
            exclusion_count += 1
        else:
            logger.warning("Thermal Power column not found. Skipping.")

        # 7. Tobacco Screening
        logger.info("Applying tobacco screening exclusions...")
        
        # Tobacco Production > 5%
        if 'Tobacco Prod Numeric' in developed_market_df.columns:
            developed_market_df[f'exclusion_{exclusion_count}_TobaccoProduction'] = None
            excluded_tobacco_prod = developed_market_df[
                developed_market_df['Tobacco Prod Numeric'] > 5
            ]['ISIN Code'].tolist()
            developed_market_df[f'exclusion_{exclusion_count}_TobaccoProduction'] = np.where(
                developed_market_df['ISIN Code'].isin(excluded_tobacco_prod),
                'exclude_tobacco_production',
                None
            )
            logger.info(f"Tobacco Production exclusions: {len(excluded_tobacco_prod)}")
            exclusion_count += 1
        else:
            logger.warning("Tobacco Production column not found. Skipping.")
        
        # Tobacco Distribution > 15%
        if 'Tobacco Dist Numeric' in developed_market_df.columns:
            developed_market_df[f'exclusion_{exclusion_count}_TobaccoDistribution'] = None
            excluded_tobacco_dist = developed_market_df[
                developed_market_df['Tobacco Dist Numeric'] > 15
            ]['ISIN Code'].tolist()
            developed_market_df[f'exclusion_{exclusion_count}_TobaccoDistribution'] = np.where(
                developed_market_df['ISIN Code'].isin(excluded_tobacco_dist),
                'exclude_tobacco_distribution',
                None
            )
            logger.info(f"Tobacco Distribution exclusions: {len(excluded_tobacco_dist)}")
            exclusion_count += 1
        else:
            logger.warning("Tobacco Distribution column not found. Skipping.")

        # 8. Layoff Screening
        logger.info("Applying layoff screening exclusion...")
        if 'layoff_score_3m Numeric' in developed_market_df.columns:
            developed_market_df[f'exclusion_{exclusion_count}_Layoff'] = None
            # Rulebook: "Score > 0" - also exclude missing scores
            excluded_layoff = developed_market_df[
                (developed_market_df['layoff_score_3m Numeric'] > 0) | 
                (developed_market_df['layoff_score_3m Numeric'].isna())
            ]['ISIN Code'].tolist()
            developed_market_df[f'exclusion_{exclusion_count}_Layoff'] = np.where(
                developed_market_df['ISIN Code'].isin(excluded_layoff),
                'exclude_layoffs_or_missing_score',
                None
            )
            logger.info(f"Layoff exclusions: {len(excluded_layoff)}")
            exclusion_count += 1
        else:
            logger.warning("layoff_score_3m column not found. Skipping layoff exclusion.")

        # 9. Staff Rating Screening - Worst companies (below 20th percentile) by Region and Super Sector
        logger.info("Applying staff rating exclusion...")
        developed_market_df[f'exclusion_{exclusion_count}_StaffRating'] = None

        excluded_staff_isins = []

        if 'StaffRating_Regional_Sector_Percentile' in developed_market_df.columns:
            # Exclude companies with percentile STRICTLY LESS THAN 20 (not equal to 20)
            excluded_staff_isins = developed_market_df[
                developed_market_df['StaffRating_Regional_Sector_Percentile'] < 20
            ]['ISIN Code'].tolist()
            
            logger.info(f"Companies with percentile < 20: {len(excluded_staff_isins)}")
            
            # Log boundary cases for debugging
            boundary_companies = developed_market_df[
                (developed_market_df['StaffRating_Regional_Sector_Percentile'] >= 19) & 
                (developed_market_df['StaffRating_Regional_Sector_Percentile'] <= 21)
            ]
            if len(boundary_companies) > 0:
                logger.info(f"Companies near 20th percentile boundary: {len(boundary_companies)}")
                for idx, row in boundary_companies.iterrows():
                    logger.info(f"  {row['Company']}: {row['StaffRating_Regional_Sector_Percentile']:.2f}% - {'EXCLUDED' if row['ISIN Code'] in excluded_staff_isins else 'INCLUDED'}")
        else:
            logger.warning("Cannot apply staff rating exclusion. Missing StaffRating_Regional_Sector_Percentile column.")

        developed_market_df[f'exclusion_{exclusion_count}_StaffRating'] = np.where(
            developed_market_df['ISIN Code'].isin(excluded_staff_isins),
            'exclude_staff_rating',
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
        eligible_df = developed_market_df[
            developed_market_df[exclusion_columns].isna().all(axis=1)
        ].copy()
        
        logger.info(f"Companies remaining after exclusions: {len(eligible_df)}")
        
        # Check if we have enough companies (need 50)
        if len(eligible_df) < 50:
            logger.warning(f"Only {len(eligible_df)} eligible companies. Rulebook requires adjusting threshold to reach 50.")
            # TODO: Implement fallback mechanism to relax layoff score threshold
        
        # Calculate FFMC for ranking (using cut-off date data - Close Prc_CO)
        logger.info("Calculating FFMC for ranking...")
        eligible_df['FFMC'] = eligible_df['Free Float'] * eligible_df['Number of Shares'] * eligible_df["Price (EUR) "]
        
        # Rank by FFMC (descending)
        eligible_df = eligible_df.sort_values('FFMC', ascending=False)
        eligible_df['Rank'] = range(1, len(eligible_df) + 1)
        
        # Select top 40 by FFMC
        logger.info("Selecting top 40 companies by FFMC...")
        final_selection = eligible_df.head(40).copy()

        # ===================================================================
        # CAPPING (10% max weight)
        # ===================================================================
        logger.info("Applying 10% capping using most recent pricing data...")
        
        # Recalculate market cap using EOD prices for capping
        final_selection['EOD_FFMC'] = final_selection['Free Float'] * final_selection['Number of Shares'] * final_selection['Close Prc_EOD'] * final_selection['FX/Index Ccy']
        
        # Apply proportional capping with 10% max weight
        final_selection = apply_proportional_capping(
            final_selection,
            mcap_column='EOD_FFMC',
            max_weight=0.10,  # 10% max weight
            max_iterations=100
        )
        
        # Rename 'Capping Factor' to 'Final Capping' for consistency
        final_selection = final_selection.rename(columns={'Capping Factor': 'Final Capping'})
        
        # Normalize capping factors
        max_capping = final_selection['Final Capping'].max()
        final_selection['Final Capping'] = (final_selection['Final Capping'] / max_capping).round(14)
        
        # Log capping results
        logger.info(f"Total weight after capping: {final_selection['Current Weight'].sum():.6f}")
        logger.info(f"Max weight after capping: {final_selection['Current Weight'].max():.6f}")
        logger.info(f"Companies capped: {final_selection['Is Capped'].sum()}")

        # ===================================================================
        # CREATE FINAL OUTPUT
        # ===================================================================
        # Create final output DataFrame
        WIFRP_df = final_selection[[
            'Company',
            'ISIN Code',
            'MIC',
            'Number of Shares',
            'Free Float',
            'Final Capping',
            'Currency'
        ]].copy()
        
        # Add effective date
        WIFRP_df['Effective Date of Review'] = effective_date
        
        # Sort by Company name
        WIFRP_df = WIFRP_df.sort_values('Company')

        # Perform Inclusion/Exclusion Analysis
        analysis_results = inclusion_exclusion_analysis(
            WIFRP_df, 
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
            wifrp_path = os.path.join(output_dir, f'WIFRP_df_{timestamp}.xlsx')
            
            # Save with timestamp to avoid any conflicts
            logger.info(f"Saving WIFRP output to: {wifrp_path}")
            with pd.ExcelWriter(wifrp_path) as writer:
                # Write each DataFrame to a different sheet
                WIFRP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                developed_market_df.to_excel(writer, sheet_name='Full Universe', index=False)
                eligible_df.to_excel(writer, sheet_name='Eligible Companies', index=False)
                final_selection.to_excel(writer, sheet_name='Final Selection', index=False)
                
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "wifrp_path": wifrp_path
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