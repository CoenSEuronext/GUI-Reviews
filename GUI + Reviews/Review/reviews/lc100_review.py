import pandas as pd
import numpy as np
from datetime import datetime
import os
import traceback
from config import DLF_FOLDER, DATA_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data

logger = setup_logging(__name__)

def run_lc100_review(date, co_date, effective_date, index="LC100", isin="QS0011131735", 
                    area="US", area2="EU", type="STOCK", universe="Europe 500", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the Low Carbon 100 Europe PAB index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        co_date (str): Cut-off date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "LC100".
        isin (str, optional): ISIN code. Defaults to "QS0011131735".
        area (str, optional): Primary area. Defaults to "EU".
        area2 (str, optional): Secondary area. Defaults to None.
        type (str, optional): Type of instrument. Defaults to "STOCK".
        universe (str, optional): Universe name. Defaults to "Europe 500".
        feed (str, optional): Feed source. Defaults to "Reuters".
        currency (str, optional): Currency code. Defaults to "EUR".
        year (str, optional): Year for calculation. Defaults to None (extracted from date).

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
        ref_data = load_reference_data(current_data_folder, [
            'developed_market', 'ff', 'icb', 'cdp_climate', 'oekom_trustcarbon', 
            'nace', 'eu_taxonomy_pocket', 'gafi_black_list', 'gafi_grey_list', 
            'non_fiscally_cooperative_with_eu'
        ])
        
        # Extract the needed DataFrames from reference data
        developed_market_df = ref_data['developed_market']
        ff_df = ref_data['ff']
        icb_df = ref_data['icb']
        cdp_climate_df = ref_data['cdp_climate']
        oekom_trustcarbon_df = ref_data['oekom_trustcarbon']
        nace_df = ref_data['nace']
        eu_taxonomy_df = ref_data['eu_taxonomy_pocket']
        gafi_black_list_df = ref_data['gafi_black_list']
        gafi_grey_list_df = ref_data['gafi_grey_list']
        non_fiscally_cooperative_df = ref_data['non_fiscally_cooperative_with_eu']
        
        # Check if all required dataframes are loaded successfully
        required_dfs = ['developed_market_df', 'ff_df', 'icb_df', 'cdp_climate_df', 
                       'oekom_trustcarbon_df', 'nace_df', 'eu_taxonomy_df', 
                       'gafi_black_list_df', 'gafi_grey_list_df', 'non_fiscally_cooperative_df']
        
        for df_name in required_dfs:
            if locals()[df_name] is None:
                raise ValueError(f"Failed to load {df_name} from reference data")
        
        # Check for and remove duplicates in source data
        for df_name, id_col in [
            ('oekom_trustcarbon_df', 'ISIN'),
            ('cdp_climate_df', 'ISIN 1'),
            ('nace_df', 'ISIN'),
            ('eu_taxonomy_df', 'ISIN')
        ]:
            if id_col in locals()[df_name].columns:
                df = locals()[df_name]
                before_count = len(df)
                df = df.drop_duplicates(subset=[id_col])
                after_count = len(df)
                if before_count != after_count:
                    logger.warning(f"Removed {before_count - after_count} duplicate rows from {df_name}")
                locals()[df_name] = df
        
        # STEP 1: Prepare Universe
        # Rename columns as specified in the prompt
        universe_df = developed_market_df.rename(columns={
            'Name': 'Company',
            'ISIN': 'ISIN Code',
            'NOSH': 'Number of Shares',
            'Currency (Local)': 'Currency'
        }).copy()
        
        # Check for and remove any duplicates in the initial universe
        before_count = len(universe_df)
        universe_df = universe_df.drop_duplicates(subset=['ISIN Code'])
        after_count = len(universe_df)
        if before_count != after_count:
            logger.warning(f"Removed {before_count - after_count} duplicate rows from initial universe")
        
        # Index Universe definition - Filter for EU500 as mentioned in step 1 of 2.2 Review Selection
        logger.info("Filtering universe for EU500 companies...")
        universe_df = universe_df[universe_df['index'].str.contains('EU500', na=False)].copy()
        logger.info(f"Universe size after EU500 filter: {len(universe_df)}")
        
        # Add Free Float data from FF.xlsx
        logger.info("Adding Free Float data...")
        universe_df = universe_df.merge(
            ff_df[['ISIN Code:', 'Free Float Round:']],
            left_on='ISIN Code',
            right_on='ISIN Code:',
            how='left'
        ).drop('ISIN Code:', axis=1).rename(columns={'Free Float Round:': 'Free Float'})        
        
        # Add ICB Subsector data
        logger.info("Adding ICB Subsector data...")
        # First, check for and remove duplicates in icb_df
        before_count = len(icb_df)
        icb_df = icb_df.drop_duplicates(subset=['ISIN Code'])
        after_count = len(icb_df)
        if before_count != after_count:
            logger.warning(f"Removed {before_count - after_count} duplicate rows from ICB data")

        # Now merge the deduplicated ICB data with universe_df
        universe_df = universe_df.merge(
            icb_df[['ISIN Code', 'Subsector Code', 'Supersector Code']],
            on='ISIN Code',
            how='left'
        )
        
        # Simple debug code to output universe_df to Excel
        debug_dir = os.path.join(os.getcwd(), 'debug')
        os.makedirs(debug_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_path = os.path.join(debug_dir, f'universe_df_after_nace_{timestamp}.xlsx')

        # Save universe_df to Excel
        universe_df.to_excel(debug_path, index=False)
        logger.info(f"Debug file saved to {debug_path}")
        
        # Add CDP Climate data
        logger.info("Adding CDP Climate data...")
        universe_df = universe_df.merge(
            cdp_climate_df[['ISIN 1', 'itr_scope123_target', 'itr_scope12_trend', 'CDP_climate_score']],
            left_on='ISIN Code',
            right_on='ISIN 1',
            how='left'
        ).drop('ISIN 1', axis=1)
        
        # Add data from Oekom Trust & Carbon for exclusion checks
        logger.info("Adding Oekom Trust & Carbon data...")
        oekom_columns = [
            'ISIN', 'NBR Overall Flag', 
            'Tobacco - Production Maximum Percentage of Revenues (%)',
            'Tobacco - Distribution Maximum Percentage of Revenues (%)',
            'Biological Weapons - Overall Flag', 'Chemical Weapons - Overall Flag',
            'Nuclear Weapons Outside NPT - Overall Flag', 'Nuclear Weapons Inside NPT - Overall Flag',
            'Cluster Munitions - Overall Flag', 'Depleted Uranium - Overall Flag',
            'Anti-personnel Mines - Overall Flag', 
            'coal_mining_and_power_gen_maximum_percentage_of_revenues-values',
            'Fossil Fuel - Total Maximum Percentage of Revenues (%)',
            'power_generation_thermal_maximum_percentage_of_revenues-values',
            'shale_oil_and_or_gas_involvement_tie', 'arctic_drilling_share_max-values',
            'deepwater_drilling_involvement', 'HydraulicFracturingInvolvement',
            'CoalMiningExpInvolved', 'OilGasExtractExpInvolved',
            'OtherFFInfraInvolved', 'NuclearPowerInvolvement',
            'NuclearPowerRevShareMax-values', 'NuclearPowerUraniumRevShareMax-values',
            'CivFAProdServMaxRev-values', 'MilitaryEqmtDistMaxRev-values',
            'Social Rating (Num)', 'Governance Rating (Num)'
        ]
        
        # Check which columns exist in the dataset
        available_oekom_columns = ['ISIN']
        for col in oekom_columns[1:]:
            if col in oekom_trustcarbon_df.columns:
                available_oekom_columns.append(col)
            else:
                logger.warning(f"Column '{col}' not found in Oekom data. Skipping this column.")
        
        # Merge Oekom data with available columns
        universe_df = universe_df.merge(
            oekom_trustcarbon_df[available_oekom_columns],
            left_on='ISIN Code',
            right_on='ISIN',
            how='left'
        ).drop('ISIN', axis=1)
        
        # Add NACE data and flag for climate impact sections
        logger.info("Adding NACE data...")
        universe_df = universe_df.merge(
            nace_df[['ISIN', 'NACE']],
            left_on='ISIN Code',
            right_on='ISIN',
            how='left'
        ).drop('ISIN', axis=1)
        
        # Add climate impact flag based on NACE section
        universe_df['High_Climate_Impact'] = universe_df['NACE'].apply(
            lambda x: 1 if pd.notna(x) and x[0] in 'ABCDEFGHL' else 0
        )
        
        # Add EU Taxonomy flag
        logger.info("Adding EU Taxonomy flag...")
        if 'ISIN' in eu_taxonomy_df.columns:
            eu_taxonomy_isins = eu_taxonomy_df['ISIN'].unique()
            universe_df['EU_Taxonomy'] = np.where(universe_df['ISIN Code'].isin(eu_taxonomy_isins), 1, 0)
        else:
            logger.warning("No EU Taxonomy data available. Setting EU_Taxonomy flag to 0 for all companies.")
            universe_df['EU_Taxonomy'] = 0
        
        # Check GAFI Black List, Grey List and Non-fiscally cooperative countries with EU
        logger.info("Checking GAFI lists and Non-fiscally cooperative countries...")
        
        # Extract first 2 characters of ISIN (country code)
        universe_df['Country_Code'] = universe_df['ISIN Code'].str[:2]
        
        # Create GAFI/Fiscal flag
        universe_df['GAFI_Black_List'] = np.where(
            universe_df['Country_Code'].isin(gafi_black_list_df['Code']), 1, 0
        )
        
        universe_df['GAFI_Grey_List'] = np.where(
            universe_df['Country_Code'].isin(gafi_grey_list_df['Code']), 1, 0
        )
        
        universe_df['Non_Fiscally_Cooperative'] = np.where(
            universe_df['Country_Code'].isin(non_fiscally_cooperative_df['Code']), 1, 0
        )
        
        # STEP 2: Eligibility screening at reviews - Apply exclusion criteria
        # Initialize exclusion columns
        logger.info("Applying exclusion criteria...")
        
        # 1. GAFI Lists and Non-fiscally cooperative exclusions
        universe_df['exclude_gafi_black'] = np.where(universe_df['GAFI_Black_List'] == 1, 'exclude_gafi_black', None)
        universe_df['exclude_gafi_grey'] = np.where(universe_df['GAFI_Grey_List'] == 1, 'exclude_gafi_grey', None)
        universe_df['exclude_non_fiscal_coop'] = np.where(universe_df['Non_Fiscally_Cooperative'] == 1, 'exclude_non_fiscal_coop', None)
        
        # 2. ICB Subsector exclusions
        excluded_subsectors = [45103010, 50201010, 50201020, 60101030]  # Tobacco, Defense, Aerospace, Oil Equipment & Services

        # First make sure Subsector Code is numeric
        universe_df['Subsector Code'] = pd.to_numeric(universe_df['Subsector Code'], errors='coerce')

        # Simple and direct exclusion using isin
        universe_df['exclude_subsector'] = np.where(
            universe_df['Subsector Code'].isin(excluded_subsectors),
            universe_df['Subsector Code'].apply(lambda x: f"exclude_subsector_{x}"),
            None
        )
        
        # 3. Breaches of international standards
        if 'NBR Overall Flag' in universe_df.columns:
            universe_df['exclude_nbr_red'] = np.where(
                universe_df['NBR Overall Flag'] == 'RED',
                'exclude_nbr_red_flag',
                None
            )
        else:
            logger.warning("'NBR Overall Flag' column not found. Cannot apply this exclusion.")
        
        # 4. Tobacco exclusions
        tobacco_prod_col = 'Tobacco - Production Maximum Percentage of Revenues (%)'
        if tobacco_prod_col in universe_df.columns:
            universe_df['exclude_tobacco_prod'] = np.where(
                pd.to_numeric(universe_df[tobacco_prod_col], errors='coerce') > 0,
                'exclude_tobacco_production',
                None
            )
        else:
            logger.warning(f"{tobacco_prod_col} not found. Cannot apply this exclusion.")
        
        tobacco_dist_col = 'Tobacco - Distribution Maximum Percentage of Revenues (%)'
        if tobacco_dist_col in universe_df.columns:
            universe_df['exclude_tobacco_dist'] = np.where(
                pd.to_numeric(universe_df[tobacco_dist_col], errors='coerce') > 0.05,
                'exclude_tobacco_distribution',
                None
            )
        else:
            logger.warning(f"{tobacco_dist_col} not found. Cannot apply this exclusion.")
        
        # 5. Controversial Weapons exclusions
        weapons_criteria = {
            'Biological Weapons - Overall Flag': 'exclude_biological_weapons',
            'Chemical Weapons - Overall Flag': 'exclude_chemical_weapons',
            'Nuclear Weapons Inside NPT - Overall Flag': 'exclude_nuclear_weapons_inside_npt',
            'Nuclear Weapons Outside NPT - Overall Flag': 'exclude_nuclear_weapons_outside_npt',
            'Cluster Munitions - Overall Flag': 'exclude_cluster_munitions',
            'Depleted Uranium - Overall Flag': 'exclude_depleted_uranium',
            'Anti-personnel Mines - Overall Flag': 'exclude_anti_personnel_mines'
        }
        
        for column, exclude_value in weapons_criteria.items():
            if column in universe_df.columns:
                universe_df[exclude_value] = np.where(
                    universe_df[column].isin(['RED', 'Amber']),
                    exclude_value,
                    None
                )
            else:
                logger.warning(f"Column '{column}' not found. Cannot apply this exclusion criterion.")
        
        # 6. Energy and Extractives exclusions
        energy_criteria = {
            'coal_mining_and_power_gen_maximum_percentage_of_revenues-values': 'exclude_coal_mining',
            'Fossil Fuel - Total Maximum Percentage of Revenues (%)': 'exclude_fossil_fuel',
            'power_generation_thermal_maximum_percentage_of_revenues-values': 'exclude_thermal_power'
        }
        
        for column, exclude_value in energy_criteria.items():
            if column in universe_df.columns:
                universe_df[exclude_value] = np.where(
                    pd.to_numeric(universe_df[column], errors='coerce') > 0,
                    exclude_value,
                    None
                )
            else:
                logger.warning(f"Column '{column}' not found. Cannot apply this exclusion criterion.")
        
        # 7. Unconventional Oil & Gas exclusions
        oil_gas_criteria = {
            'shale_oil_and_or_gas_involvement_tie': ('exclude_shale_oil_gas', ['Production', 'Services']),
            'arctic_drilling_share_max-values': ('exclude_arctic_drilling', ['T']),
            'deepwater_drilling_involvement': ('exclude_deepwater_drilling', ['T']),
            'HydraulicFracturingInvolvement': ('exclude_hydraulic_fracturing', ['Production', 'Services']),
            'CoalMiningExpInvolved': ('exclude_coal_mining_expansion', ['T']),
            'OilGasExtractExpInvolved': ('exclude_oil_gas_extraction_expansion', ['T']),
            'OtherFFInfraInvolved': ('exclude_other_fossil_fuel_infra', ['T'])
        }
        
        for column, (exclude_value, flag_values) in oil_gas_criteria.items():
            if column in universe_df.columns:
                universe_df[exclude_value] = np.where(
                    universe_df[column].isin(flag_values),
                    exclude_value,
                    None
                )
            else:
                logger.warning(f"Column '{column}' not found. Cannot apply this exclusion criterion.")
        
        # 8. Nuclear exclusions
        nuclear_criteria = {
            'Nuclear Power - Involvement Tie': ('exclude_nuclear_power', ['Production', 'Services']),
            'Nuclear Power - Total Maximum Percentage of Revenues': ('exclude_nuclear_power_revenue', 0.05), 
            'Nuclear Power - Uranium Mining Max Percentage of Revenues (%)': ('exclude_nuclear_uranium', 0)  # > 0%
        }
        
        for column, (exclude_value, threshold) in nuclear_criteria.items():
            if column in universe_df.columns:
                if isinstance(threshold, (int, float)):
                    if column == 'Nuclear Power - Total Maximum Percentage of Revenues':
                        # Exclude if >= 5%
                        universe_df[exclude_value] = np.where(
                            pd.to_numeric(universe_df[column], errors='coerce') >= threshold,
                            exclude_value,
                            None
                        )
                    elif column == 'Nuclear Power - Uranium Mining Max Percentage of Revenues (%)':
                        # Exclude if > 0%
                        universe_df[exclude_value] = np.where(
                            pd.to_numeric(universe_df[column], errors='coerce') > threshold,
                            exclude_value,
                            None
                        )
                else:
                    universe_df[exclude_value] = np.where(
                        universe_df[column].isin(threshold),
                        exclude_value,
                        None
                    )
            else:
                alt_column = None
                if column == 'Nuclear Power - Involvement Tie':
                    alt_column = 'NuclearPowerInvolvement'
                elif column == 'Nuclear Power - Total Maximum Percentage of Revenues':
                    alt_column = 'NuclearPowerRevShareMax-values'
                elif column == 'Nuclear Power - Uranium Mining Max Percentage of Revenues (%)':
                    alt_column = 'NuclearPowerUraniumRevShareMax-values'
                
                if alt_column and alt_column in universe_df.columns:
                    if isinstance(threshold, (int, float)):
                        if column == 'Nuclear Power - Total Maximum Percentage of Revenues':
                            # Exclude if >= 5%
                            universe_df[exclude_value] = np.where(
                                pd.to_numeric(universe_df[alt_column], errors='coerce') >= threshold,
                                exclude_value,
                                None
                            )
                        elif column == 'Nuclear Power - Uranium Mining Max Percentage of Revenues (%)':
                            # Exclude if > 0%
                            universe_df[exclude_value] = np.where(
                                pd.to_numeric(universe_df[alt_column], errors='coerce') > threshold,
                                exclude_value,
                                None
                            )
                    else:
                        universe_df[exclude_value] = np.where(
                            universe_df[alt_column].isin(threshold),
                            exclude_value,
                            None
                        )
                else:
                    logger.warning(f"Column '{column}' not found. Cannot apply this exclusion criterion.")
        
        # 9. Civilian firearms and Military exclusions
        # Define criteria in a standard format
        if 'CivFAProdServMaxRev-values' in universe_df.columns:
            universe_df['exclude_civilian_firearms'] = np.where(
                pd.to_numeric(universe_df['CivFAProdServMaxRev-values'], errors='coerce') >= 0.05,
                'exclude_civilian_firearms',
                None
            )
        else:
            logger.warning("Column 'CivFAProdServMaxRev-values' not found. Cannot apply civilian firearms exclusion.")

        if 'MilitaryEqmtDistMaxRev-values' in universe_df.columns:
            universe_df['exclude_military_equipment'] = np.where(
                pd.to_numeric(universe_df['MilitaryEqmtDistMaxRev-values'], errors='coerce') >= 0.05,
                'exclude_military_equipment', 
                None
            )
        else:
            logger.warning("Column 'MilitaryEqmtDistMaxRev-values' not found. Cannot apply military equipment exclusion.")
        
        # 10. Social and Governance Score exclusions (bottom 10%)
        if 'Social Rating (Num)' in universe_df.columns and 'Governance Rating (Num)' in universe_df.columns:
            # Calculate average of Social and Governance scores
            universe_df['SG_Score'] = (
                pd.to_numeric(universe_df['Social Rating (Num)'], errors='coerce') +
                pd.to_numeric(universe_df['Governance Rating (Num)'], errors='coerce')
            ) / 2
            
            # Find the threshold for the bottom 10%
            bottom_10_threshold = universe_df['SG_Score'].quantile(0.1)
            
            # Mark companies in the bottom 10% for exclusion
            bottom_10_percent = universe_df[universe_df['SG_Score'] <= bottom_10_threshold].copy()
            
            # In case of ties at the threshold, keep companies with higher Social score
            if len(bottom_10_percent) > len(universe_df) * 0.1:
                # For companies at the threshold, sort by Social score and keep the better ones
                threshold_companies = bottom_10_percent[bottom_10_percent['SG_Score'] == bottom_10_threshold]
                threshold_companies = threshold_companies.sort_values('Social Rating (Num)', ascending=False)
                
                # Calculate how many to keep
                total_to_exclude = int(len(universe_df) * 0.1)
                below_threshold_count = len(bottom_10_percent[bottom_10_percent['SG_Score'] < bottom_10_threshold])
                threshold_to_exclude = total_to_exclude - below_threshold_count
                
                # Get ISINs to exclude
                threshold_to_exclude_isins = threshold_companies.iloc[:threshold_to_exclude]['ISIN Code'].tolist()
                all_to_exclude_isins = (
                    bottom_10_percent[bottom_10_percent['SG_Score'] < bottom_10_threshold]['ISIN Code'].tolist() +
                    threshold_to_exclude_isins
                )
            else:
                all_to_exclude_isins = bottom_10_percent['ISIN Code'].tolist()
            
            # Add exclusion flag
            universe_df['exclude_sg_bottom_10'] = np.where(
                universe_df['ISIN Code'].isin(all_to_exclude_isins),
                'exclude_sg_bottom_10',
                None
            )
            
            logger.info(f"Marked {len(all_to_exclude_isins)} companies (10%) with worst SG scores for exclusion")
        else:
            logger.warning("Social Rating and/or Governance Rating columns not found. Cannot apply SG exclusion criterion.")
        
        # 10. Create a general exclusion flag based on all exclusion columns
        # Get all exclusion columns
        exclusion_columns = [col for col in universe_df.columns if col.startswith('exclude_')]
        
        # Create a summary exclusion flag and reason
        universe_df['Excluded'] = 'No'
        universe_df['Exclusion_Reason'] = ''
        
        # Check all exclusion columns - if any have a value, mark as excluded
        for exclusion_col in exclusion_columns:
            mask = universe_df[exclusion_col].notna()
            universe_df.loc[mask, 'Excluded'] = 'Yes'
            universe_df.loc[mask, 'Exclusion_Reason'] = universe_df.loc[mask, 'Exclusion_Reason'] + universe_df.loc[mask, exclusion_col] + '; '
        
        # Remove trailing semicolon and space if present
        universe_df['Exclusion_Reason'] = universe_df['Exclusion_Reason'].str.rstrip('; ')
        
        # STEP 3: Selection Ranking - Calculate Climate Score
        logger.info("Calculating Climate Score for ranking...")
        
        # Fill empty values with 3.4 for temperature scores as mentioned in Step 3 of the prompt
        universe_df['itr_scope123_target_filled'] = universe_df['itr_scope123_target'].fillna(3.4)
        universe_df['itr_scope12_trend_filled'] = universe_df['itr_scope12_trend'].fillna(3.4)
        
        # Calculate CDP temperature score (average of target and trend)
        universe_df['cdp_temperature_score'] = (universe_df['itr_scope123_target_filled'] + universe_df['itr_scope12_trend_filled']) / 2
        
        # Define the CDP equivalence table for bonus/malus calculation
        cdp_equivalence = {
            'A': -0.3,
            'A-': -0.24,
            'B': -0.18,
            'B-': -0.12,
            'C': 0,
            'C-': 0.12,
            'D': 0.18,
            'D-': 0.24,
            'F': 0.3,
            None: 0.3  # For "Not requested to respond"
        }
        
        # Create a function to map CDP climate score to bonus/malus
        def map_to_bonus_malus(score):
            return cdp_equivalence.get(score, 0.3)  # Default to 0.3 for any unmapped values
        
        # Calculate bonus/malus and apply to temperature score
        universe_df['bonus_malus'] = universe_df['CDP_climate_score'].apply(map_to_bonus_malus)
        universe_df['climate_score'] = universe_df['cdp_temperature_score'] + universe_df['bonus_malus']
        
        # STEP 4: Selection of constituents
        # First, separate EU Taxonomy and Non-EU Taxonomy companies
        logger.info("Selecting EU Taxonomy companies...")
        eu_taxonomy_companies = universe_df[
            (universe_df['EU_Taxonomy'] == 1) & 
            (universe_df['Excluded'] == 'No')
        ].copy()
        
        logger.info(f"Found {len(eu_taxonomy_companies)} EU Taxonomy companies after exclusions")
        
        # Get Non-EU Taxonomy eligible companies (after all exclusions)
        non_eu_taxonomy_eligible = universe_df[
            (universe_df['EU_Taxonomy'] == 0) & 
            (universe_df['Excluded'] == 'No')
        ].copy()

        logger.info(f"Found {len(non_eu_taxonomy_eligible)} Non-EU Taxonomy eligible companies after exclusions")


        

        # STEP 4a: Determination of the target number of Non-EU Taxonomy companies within each ICB super-sector
        logger.info("STEP 4a: Determining target number of Non-EU Taxonomy companies per super-sector...")

        # Define target number of index constituents
        target_constituents = 100  # As mentioned in the prompt

        # Calculate target number of Non-EU Taxonomy companies
        target_non_eu_taxonomy = target_constituents - len(eu_taxonomy_companies)
        logger.info(f"Target number of Non-EU Taxonomy companies: {target_non_eu_taxonomy}")

        # Create a DataFrame to track the super-sector distribution
        # Get counts of eligible companies by Supersector
        supersector_counts = non_eu_taxonomy_eligible.groupby('Supersector Code').size().reset_index(name='Eligible_Count')
        supersector_counts = supersector_counts.sort_values('Supersector Code')

        # Get the total number of eligible companies
        total_eligible = non_eu_taxonomy_eligible['ISIN Code'].nunique()
        logger.info(f"Total eligible Non-EU Taxonomy companies: {total_eligible}")

        # Calculate the proportional target for each super-sector and round up
        supersector_counts['Target_Raw'] = supersector_counts['Eligible_Count'] / total_eligible * target_non_eu_taxonomy
        # Make sure to convert to integer type properly
        supersector_counts['Target_Rounded'] = np.ceil(supersector_counts['Target_Raw']).astype('int64')

        # Calculate the total after rounding
        total_rounded = supersector_counts['Target_Rounded'].sum()
        logger.info(f"Total target after rounding: {total_rounded} (vs. target of {target_non_eu_taxonomy})")

        # Check if we need to reduce the number of companies
        excess = total_rounded - target_non_eu_taxonomy
        if excess > 0:
            logger.info(f"Need to remove {excess} companies due to rounding up")

        # Create a detailed output DataFrame for the step-by-step process
        step_output = pd.DataFrame()
        step_output['ICB_Supersector_Code'] = supersector_counts['Supersector Code']
        step_output['Eligible_Count'] = supersector_counts['Eligible_Count']
        step_output['Proportion'] = supersector_counts['Eligible_Count'] / total_eligible
        step_output['Raw_Target'] = supersector_counts['Target_Raw']
        step_output['Rounded_Target'] = supersector_counts['Target_Rounded']

        # STEP 4b: Selection of Non-EU Taxonomy companies
        logger.info("STEP 4b: Selecting Non-EU Taxonomy companies based on climate score...")

        # Create a dictionary to store selected companies from each super-sector
        supersector_selections = {}

        # Select the best companies from each super-sector based on climate score
        for _, row in supersector_counts.iterrows():
            supersector_code = row['Supersector Code']
            target_count = int(row['Target_Rounded'])  # Explicitly cast to int
            
            # Get companies in this super-sector
            supersector_companies = non_eu_taxonomy_eligible[
                non_eu_taxonomy_eligible['Supersector Code'] == supersector_code
            ].copy()
            
            # Sort by climate score (lowest/best first)
            supersector_companies = supersector_companies.sort_values('climate_score')
            
            # Select the top N companies (using int)
            selected = supersector_companies.head(target_count)
            
            # Store in dictionary
            supersector_selections[supersector_code] = selected
            
            logger.info(f"Selected {len(selected)} companies from super-sector {supersector_code}")

        # Combine all selected companies
        non_eu_taxonomy_selected_initial = pd.concat(supersector_selections.values())

        # Update step output with initial selection counts
        supersector_initial_selections = non_eu_taxonomy_selected_initial.groupby('Supersector Code').size().reset_index(name='Initial_Selected')
        step_output = step_output.merge(
            supersector_initial_selections, 
            left_on='ICB_Supersector_Code', 
            right_on='Supersector Code', 
            how='left'
        ).drop('Supersector Code', axis=1)
        step_output['Initial_Selected'] = step_output['Initial_Selected'].fillna(0).astype(int)

        # Fix if we have too many companies due to rounding up
        if len(non_eu_taxonomy_selected_initial) > target_non_eu_taxonomy:
            logger.info(f"Initial selection has {len(non_eu_taxonomy_selected_initial)} companies, need to remove {len(non_eu_taxonomy_selected_initial) - target_non_eu_taxonomy}")
            
            # Create a DataFrame to track removals
            removals_tracking = pd.DataFrame()
            
            # Continue removing companies until we reach the target
            non_eu_taxonomy_selected = non_eu_taxonomy_selected_initial.copy()
            
            # Keep track of how many companies we've removed from each super-sector
            removed_counts = {code: 0 for code in supersector_counts['Supersector Code']}
            
            # Identify supersectors with more than 2 companies (eligible for removal)
            removal_iterations = []
            
            while len(non_eu_taxonomy_selected) > target_non_eu_taxonomy:
                # Get current counts per supersector
                current_supersector_counts = non_eu_taxonomy_selected.groupby('Supersector Code').size().to_dict()
                
                # Create a list of supersectors eligible for removal (more than 2 companies)
                eligible_supersectors = [code for code, count in current_supersector_counts.items() if count > 2]
                
                if not eligible_supersectors:
                    logger.warning("Cannot remove more companies while maintaining minimum 2 per supersector. Keeping extra companies.")
                    break
                
                # For each eligible supersector, find the company with the worst climate score
                candidates_for_removal = []
                
                for supersector in eligible_supersectors:
                    # Get companies in this supersector
                    supersector_companies = non_eu_taxonomy_selected[non_eu_taxonomy_selected['Supersector Code'] == supersector]
                    
                    # Sort by climate score (highest/worst first)
                    supersector_companies = supersector_companies.sort_values('climate_score', ascending=False)
                    
                    # Get the worst company
                    worst_company = supersector_companies.iloc[0]
                    
                    # Add to candidates
                    candidates_for_removal.append(worst_company)
                
                # Sort candidates by climate score (worst first)
                candidates_df = pd.DataFrame(candidates_for_removal)
                candidates_df = candidates_df.sort_values('climate_score', ascending=False)
                
                # In case of equal climate score, sort by Free Float Market Cap (lower first)
                if 'FFMC' in candidates_df.columns:
                    candidates_with_same_score = candidates_df.duplicated('climate_score', keep=False)
                    if candidates_with_same_score.any():
                        # Sort those with duplicate scores by FFMC
                        for score_group in candidates_df.loc[candidates_with_same_score, 'climate_score'].unique():
                            score_mask = candidates_df['climate_score'] == score_group
                            candidates_df.loc[score_mask] = candidates_df.loc[score_mask].sort_values('FFMC')
                
                # Get the worst company overall
                company_to_remove = candidates_df.iloc[0]
                supersector_to_remove_from = company_to_remove['Supersector Code']
                
                # Remove the company
                non_eu_taxonomy_selected = non_eu_taxonomy_selected[
                    non_eu_taxonomy_selected['ISIN Code'] != company_to_remove['ISIN Code']
                ]
                
                # Update removal count
                removed_counts[supersector_to_remove_from] += 1
                
                # Track this removal iteration
                removal_info = {
                    'Iteration': len(removal_iterations) + 1,
                    'Removed_ISIN': company_to_remove['ISIN Code'],
                    'Removed_Company': company_to_remove['Company'] if 'Company' in company_to_remove else 'Unknown',
                    'Supersector_Code': supersector_to_remove_from,
                    'Climate_Score': company_to_remove['climate_score'],
                    'FFMC': company_to_remove['FFMC'] if 'FFMC' in company_to_remove else None,
                    'Remaining_Count': len(non_eu_taxonomy_selected)
                }
                removal_iterations.append(removal_info)
                
                logger.info(f"Removed company {company_to_remove['ISIN Code']} from supersector {supersector_to_remove_from} (Climate Score: {company_to_remove['climate_score']})")
            
            # Create a DataFrame of removal iterations
            removals_df = pd.DataFrame(removal_iterations)
            
            # Update step output with final selection counts
            supersector_final_selections = non_eu_taxonomy_selected.groupby('Supersector Code').size().reset_index(name='Final_Selected')
            step_output = step_output.merge(
                supersector_final_selections, 
                left_on='ICB_Supersector_Code', 
                right_on='Supersector Code', 
                how='left'
            ).drop('Supersector Code', axis=1)
            step_output['Final_Selected'] = step_output['Final_Selected'].fillna(0).astype(int)
            step_output['Companies_Removed'] = step_output['Initial_Selected'] - step_output['Final_Selected']
        else:
            # If we don't need to remove any companies
            non_eu_taxonomy_selected = non_eu_taxonomy_selected_initial.copy()
            step_output['Final_Selected'] = step_output['Initial_Selected']
            step_output['Companies_Removed'] = 0
            removals_df = pd.DataFrame(columns=['Iteration', 'Removed_ISIN', 'Removed_Company', 'Supersector_Code', 'Climate_Score', 'FFMC', 'Remaining_Count'])

        logger.info(f"Final Non-EU Taxonomy selection has {len(non_eu_taxonomy_selected)} companies")

        # Verify that we have at least 2 companies per supersector
        final_supersector_counts = non_eu_taxonomy_selected.groupby('Supersector Code').size()
        min_companies_per_supersector = final_supersector_counts.min()
        logger.info(f"Minimum companies per supersector: {min_companies_per_supersector}")

        if min_companies_per_supersector < 2:
            logger.warning(f"Some supersectors have fewer than 2 companies! Minimum is {min_companies_per_supersector}")

        # Save detailed step output to a separate Excel file for analysis
        output_dir = os.path.join(os.getcwd(), 'output')
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        step_output_path = os.path.join(output_dir, f'non_eu_taxonomy_selection_steps_{timestamp}.xlsx')

        with pd.ExcelWriter(step_output_path) as writer:
            step_output.to_excel(writer, sheet_name='Target Calculation', index=False)
            non_eu_taxonomy_eligible.to_excel(writer, sheet_name='Eligible Companies', index=False)
            non_eu_taxonomy_selected_initial.to_excel(writer, sheet_name='Initial Selection', index=False)
            non_eu_taxonomy_selected.to_excel(writer, sheet_name='Final Selection', index=False)
            if len(removals_df) > 0:
                removals_df.to_excel(writer, sheet_name='Removal Process', index=False)

        logger.info(f"Detailed selection steps saved to {step_output_path}")


        
        # Combine EU Taxonomy and Non-EU Taxonomy selected companies for final selection
        final_selection = pd.concat([eu_taxonomy_companies, non_eu_taxonomy_selected])
        
        logger.info(f"Final selection contains {len(final_selection)} companies")
        logger.info(f"- EU Taxonomy: {len(eu_taxonomy_companies)}")
        logger.info(f"- Non-EU Taxonomy: {len(non_eu_taxonomy_selected)}")
        
        # Create a base weighting for both pockets
        # EU Taxonomy pocket: Minimum 5%, maximum 10% of index weight
        eu_taxonomy_weight = min(max(0.05, len(eu_taxonomy_companies) / target_constituents), 0.1)
        non_eu_taxonomy_weight = 1 - eu_taxonomy_weight
        
        logger.info(f"Initial weight allocations:")
        logger.info(f"- EU Taxonomy pocket: {eu_taxonomy_weight:.2%}")
        logger.info(f"- Non-EU Taxonomy pocket: {non_eu_taxonomy_weight:.2%}")
        
        # Assign equal weight within each pocket
        if len(eu_taxonomy_companies) > 0:
            eu_taxonomy_companies['Initial_Weight'] = eu_taxonomy_weight / len(eu_taxonomy_companies)
        else:
            logger.warning("No EU Taxonomy companies in selection.")
        
        non_eu_taxonomy_selected['Initial_Weight'] = non_eu_taxonomy_weight / len(non_eu_taxonomy_selected)
        
        # Add Initial_Weight to final selection
        final_selection['Initial_Weight'] = np.where(
            final_selection['EU_Taxonomy'] == 1,
            eu_taxonomy_weight / len(eu_taxonomy_companies) if len(eu_taxonomy_companies) > 0 else 0,
            non_eu_taxonomy_weight / len(non_eu_taxonomy_selected)
        )
        
        # Calculate Free Float Market Cap (FFMC) for capping purposes
        if 'Price (EUR)' in final_selection.columns and 'Number of Shares' in final_selection.columns and 'Free Float' in final_selection.columns:
            final_selection['FFMC'] = final_selection['Price (EUR)'] * final_selection['Number of Shares'] * final_selection['Free Float']
        elif 'Mcap in EUR' in final_selection.columns and 'Free Float' in final_selection.columns:
            final_selection['FFMC'] = final_selection['Mcap in EUR'] * final_selection['Free Float']
        else:
            logger.warning("Missing columns for FFMC calculation. Using alternative approach with EOD data.")
            
            # Try to merge price data from stock_eod_df
            if 'Close Prc' in stock_eod_df.columns and 'Isin Code' in stock_eod_df.columns:
                # Create a price lookup dataframe
                price_df = stock_eod_df[['Isin Code', 'Close Prc']].drop_duplicates('Isin Code').rename(
                    columns={'Isin Code': 'ISIN Code', 'Close Prc': 'Price'}
                )
                
                # Merge price data
                final_selection = final_selection.merge(
                    price_df,
                    on='ISIN Code',
                    how='left'
                )
                
                # Calculate FFMC using the Close Prc
                if 'Number of Shares' in final_selection.columns and 'Free Float' in final_selection.columns:
                    final_selection['FFMC'] = final_selection['Price'] * final_selection['Number of Shares'] * final_selection['Free Float']
                else:
                    logger.error("Cannot calculate FFMC. Missing required columns.")
                    final_selection['FFMC'] = np.nan
            else:
                logger.error("Cannot calculate FFMC. Missing required columns in stock_eod_df.")
                final_selection['FFMC'] = np.nan
        
        # Save final selection to output
        logger.info("Saving output files...")
        output_dir = os.path.join(os.getcwd(), 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        # Create filename with timestamp to avoid conflicts
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir, f'LC100_review_{timestamp}.xlsx')
        
        # Save results to Excel
        with pd.ExcelWriter(output_path) as writer:
            # Write each DataFrame to a different sheet
            final_selection.to_excel(writer, sheet_name='Selected Companies', index=False)
            universe_df.to_excel(writer, sheet_name='Full Universe', index=False)
        
        logger.info(f"Results saved to {output_path}")
        
        return {
            "status": "success",
            "message": "Review completed successfully",
            "data": {
                "output_path": output_path,
                "num_selected": len(final_selection),
                "num_eu_taxonomy": len(eu_taxonomy_companies),
                "num_non_eu_taxonomy": len(non_eu_taxonomy_selected)
            }
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