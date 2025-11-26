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

def run_wifrp_review(date, co_date, effective_date, index="WIFRP", isin="FRIX00002777", 
                    area="US", area2="EU", type="STOCK", universe="Developed Market", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the World Invest In France index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        co_date (str): Cut-off date in format YYYYMMDD
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
        index_eod_df, stock_eod_df, stock_co_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)
        
        logger.info("Loading reference data...")
        ref_data = load_reference_data(current_data_folder, ['ff', 'developed_market', 'icb', 'sesamm', 'oekom_trustcarbon'])
        
        # Extract the needed DataFrames
        ff_df = ref_data['ff']
        developed_market_df = ref_data['developed_market'] 
        icb_df = ref_data['icb']
        sesamm_df = ref_data['sesamm']
        oekom_trustcarbon_df = ref_data['oekom_trustcarbon']
        
        if any(df is None for df in [ff_df, developed_market_df, icb_df, sesamm_df, oekom_trustcarbon_df]):
            raise ValueError("Failed to load one or more required reference data files")
            
        # Check for and remove duplicates in source data 
        if 'ISIN' in oekom_trustcarbon_df.columns:
            before_count = len(oekom_trustcarbon_df)
            oekom_trustcarbon_df = oekom_trustcarbon_df.drop_duplicates(subset=['ISIN'])
            after_count = len(oekom_trustcarbon_df)
            if before_count != after_count:
                logger.warning(f"Removed {before_count - after_count} duplicate rows from Oekom data")
                
        if 'ISIN' in sesamm_df.columns:
            before_count = len(sesamm_df)
            sesamm_df = sesamm_df.drop_duplicates(subset=['ISIN'])
            after_count = len(sesamm_df)
            if before_count != after_count:
                logger.warning(f"Removed {before_count - after_count} duplicate rows from SESAMm data")
        
        # Add Index_Currency to stock_eod_df and stock_co_df by looking up Index in index_eod_df
        logger.info("Adding Index_Currency to stock_eod_df by looking up Index in index_eod_df...")
        
        # Check if required columns exist in both dataframes
        if 'Index' in stock_eod_df.columns and 'Mnemo' in index_eod_df.columns and 'Curr' in index_eod_df.columns:
            # Create a mapping dictionary from index_eod_df for faster lookup
            index_currency_map = dict(zip(index_eod_df['Mnemo'], index_eod_df['Curr']))
            
            # Apply the mapping to stock_eod_df to create the Index_Currency column
            stock_eod_df['Index_Currency'] = stock_eod_df['Index'].map(index_currency_map)
            
            logger.info(f"Added Index_Currency to stock_eod_df. Found {stock_eod_df['Index_Currency'].notna().sum()} matches.")
            
            # Also add to stock_co_df if it has the Index column
            if 'Index' in stock_co_df.columns:
                stock_co_df['Index_Currency'] = stock_co_df['Index'].map(index_currency_map)
                logger.info(f"Added Index_Currency to stock_co_df. Found {stock_co_df['Index_Currency'].notna().sum()} matches.")
        else:
            logger.warning("Cannot add Index_Currency. Missing required columns in stock_eod_df or index_eod_df.")
            # Create empty column if lookup can't be performed
            stock_eod_df['Index_Currency'] = None
            stock_co_df['Index_Currency'] = None
        
        # STEP 1: Prepare Universe
        # Rename columns as specified
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
        
        # Step 1: Filter out companies listed on Euronext Paris (XPAR)
        # In the rulebook: "The Index Universe consists of the Companies included in the Euronext® Developed Market Index 
        # and do not have their Main Listing on Euronext Paris at review."
        logger.info("Filtering universe to exclude Euronext Paris (XPAR) listings...")
        universe_df = universe_df[universe_df['MIC'] != 'XPAR'].copy()
        
        # Check for and remove duplicates in FF data
        if 'ISIN Code:' in ff_df.columns:
            ff_before_count = len(ff_df)
            ff_df = ff_df.drop_duplicates(subset=['ISIN Code:'])
            ff_after_count = len(ff_df)
            if ff_before_count != ff_after_count:
                logger.warning(f"Removed {ff_before_count - ff_after_count} duplicate rows from Free Float data")
        
        # Add Free Float data from FF.xlsx
        logger.info("Adding Free Float data...")
        universe_df = universe_df.merge(
            ff_df[['ISIN Code:', 'Free Float Round:']],
            left_on='ISIN Code',
            right_on='ISIN Code:',
            how='left'
        ).drop('ISIN Code:', axis=1).rename(columns={'Free Float Round:': 'Free Float'})
        
        # Check for duplicates after Free Float merge
        before_count = len(universe_df)
        universe_df = universe_df.drop_duplicates(subset=['ISIN Code'])
        after_count = len(universe_df)
        if before_count != after_count:
            logger.warning(f"Removed {before_count - after_count} duplicate rows after Free Float merge")
        
        # Step 2: Eligibility screening - Apply exclusion criteria
        # Initialize exclusion columns
        logger.info("Applying exclusion criteria...")
        exclusion_count = 1
        
        # 1. Companies without Invest_In_France_Layoff_Score from SESAMm
        universe_df[f'exclusion_{exclusion_count}'] = None
        universe_df[f'exclusion_{exclusion_count}'] = np.where(
            ~universe_df['ISIN Code'].isin(sesamm_df['ISIN']),
            'exclude_layoff_score_missing',
            None
        )
        exclusion_count += 1
        
        # 2. 3-months Average Daily Traded Value lower than 10 Million EUR
        universe_df[f'exclusion_{exclusion_count}'] = None
        universe_df[f'exclusion_{exclusion_count}'] = np.where(
            (universe_df['3 months ADTV'] < 10000000),  # 10 million EUR
            'exclude_low_liquidity',
            None
        )
        exclusion_count += 1
        
        # 3. Breaches of international standards - NBR Overall Flag "Red"
        universe_df = universe_df.merge(
            oekom_trustcarbon_df[['ISIN', 'NBR Overall Flag']],
            left_on='ISIN Code',
            right_on='ISIN',
            how='left'
        ).drop('ISIN', axis=1)
        
        universe_df[f'exclusion_{exclusion_count}'] = None
        universe_df[f'exclusion_{exclusion_count}'] = np.where(
            (universe_df['NBR Overall Flag'] == 'RED'),
            'exclude_NBR_red_flag',
            None
        )
        exclusion_count += 1
        
        # 4. Controversial Weapons - RED or Amber flags
        weapons_criteria = {
            'Biological Weapons - Overall Flag': 'exclude_biological_weapons',
            'Chemical Weapons - Overall Flag': 'exclude_chemical_weapons',
            'Nuclear Weapons Inside NPT - Overall Flag': 'exclude_nuclear_weapons_inside_npt',
            'Nuclear Weapons Outside NPT - Overall Flag': 'exclude_nuclear_weapons_outside_npt',
            'Cluster Munitions - Overall Flag': 'exclude_cluster_munitions',
            'Depleted Uranium - Overall Flag': 'exclude_depleted_uranium',
            'Anti-personnel Mines - Overall Flag': 'exclude_anti_personnel_mines',
            'White Phosphorous Weapons - Overall Flag': 'exclude_white_phosphorus_weapons'
        }
        
        # Merge weapons data from Oekom Trust&Carbon
        # Check if all weapons columns exist in the data
        available_weapon_columns = ['ISIN']
        for col in weapons_criteria.keys():
            if col in oekom_trustcarbon_df.columns:
                available_weapon_columns.append(col)
            else:
                logger.warning(f"Column '{col}' not found in Oekom data. Skipping this in merge.")
                
        universe_df = universe_df.merge(
            oekom_trustcarbon_df[available_weapon_columns],
            left_on='ISIN Code',
            right_on='ISIN',
            how='left'
        ).drop('ISIN', axis=1)
        
        # Apply weapons exclusions
        for column, exclude_value in weapons_criteria.items():
            new_col = f'exclusion_{exclusion_count}'
            universe_df[new_col] = None
            
            # Only apply if the column exists
            if column in universe_df.columns:
                # Apply exclusion for RED or Amber flags
                universe_df[new_col] = np.where(
                    universe_df[column].isin(['RED', 'Amber']),
                    exclude_value,
                    None
                )
                exclusion_count += 1
            else:
                logger.warning(f"Column '{column}' not found. Skipping this exclusion criterion.")
                # Remove the empty column we just created
                universe_df.drop(new_col, axis=1, inplace=True)
        
        # 5. Animal Welfare Screening
        # Check if animal welfare columns exist in oekom data
        animal_columns = ['AnimalWelfareInvolvement', 'AnimalWelfareAnimTestInvolvement']
        available_animal_columns = ['ISIN']
        
        for col in animal_columns:
            if col in oekom_trustcarbon_df.columns:
                available_animal_columns.append(col)
            else:
                logger.warning(f"Column '{col}' not found in Oekom data. Skipping this in merge.")
        
        if len(available_animal_columns) > 1:  # If we have any animal columns
            universe_df = universe_df.merge(
                oekom_trustcarbon_df[available_animal_columns],
                left_on='ISIN Code',
                right_on='ISIN',
                how='left'
            ).drop('ISIN', axis=1)
            
            # Animal Welfare Involvement
            if 'AnimalWelfareInvolvement' in universe_df.columns:
                universe_df[f'exclusion_{exclusion_count}'] = None
                universe_df[f'exclusion_{exclusion_count}'] = np.where(
                    (universe_df['AnimalWelfareInvolvement'].notna() & 
                    (universe_df['AnimalWelfareInvolvement'] != 0) & 
                    (universe_df['AnimalWelfareInvolvement'] != '0')),
                    'exclude_animal_welfare',
                    None
                )
                exclusion_count += 1
            
            # Animal Testing Involvement
            if 'AnimalWelfareAnimTestInvolvement' in universe_df.columns:
                universe_df[f'exclusion_{exclusion_count}'] = None
                universe_df[f'exclusion_{exclusion_count}'] = np.where(
                    (universe_df['AnimalWelfareAnimTestInvolvement'].notna() & 
                    (universe_df['AnimalWelfareAnimTestInvolvement'] != 0) & 
                    (universe_df['AnimalWelfareAnimTestInvolvement'] != '0')),
                    'exclude_animal_testing',
                    None
                )
                exclusion_count += 1
        else:
            logger.warning("No Animal Welfare columns available. Skipping animal welfare exclusions.")
        
        # 6. Energy Screening
        # Check which energy columns exist in oekom data
        energy_columns = [
            'Coal Mining and Power Gen - Maximum Percentage of Revenues (%)',
            'Fossil Fuel - Total Maximum Percentage of Revenues (%)',
            'Power Generation - Thermal Maximum Percentage of Revenues (%)'
        ]
        
        available_energy_columns = ['ISIN']
        for col in energy_columns:
            if col in oekom_trustcarbon_df.columns:
                available_energy_columns.append(col)
            else:
                logger.warning(f"Column '{col}' not found in Oekom data. Skipping this in merge.")
        
        if len(available_energy_columns) > 1:  # If we have any energy columns
            # Merge energy data from Oekom Trust&Carbon
            universe_df = universe_df.merge(
                oekom_trustcarbon_df[available_energy_columns],
                left_on='ISIN Code',
                right_on='ISIN',
                how='left'
            ).drop('ISIN', axis=1)
        else:
            logger.warning("No energy columns available for exclusions.")
            
        # Process energy exclusions
        # Convert energy columns to numeric safely
        for col in energy_columns:
            if col in universe_df.columns:
                universe_df[col] = pd.to_numeric(universe_df[col], errors='coerce')
        
        # Coal Mining and Power Gen > 0%
        col_name = 'Coal Mining and Power Gen - Maximum Percentage of Revenues (%)'
        universe_df[f'exclusion_{exclusion_count}'] = None
        if col_name in universe_df.columns:
            universe_df[f'exclusion_{exclusion_count}'] = np.where(
                (universe_df[col_name] > 0),
                'exclude_coal_mining',
                None
            )
        else:
            logger.warning(f"Column '{col_name}' not found in data. Skipping this exclusion.")
        exclusion_count += 1
        
        # Fossil Fuel > 0%
        col_name = 'Fossil Fuel - Total Maximum Percentage of Revenues (%)'
        universe_df[f'exclusion_{exclusion_count}'] = None
        if col_name in universe_df.columns:
            universe_df[f'exclusion_{exclusion_count}'] = np.where(
                (universe_df[col_name] > 0),
                'exclude_fossil_fuel',
                None
            )
        else:
            logger.warning(f"Column '{col_name}' not found in data. Skipping this exclusion.")
        exclusion_count += 1
        
        # Thermal Power Generation > 0%
        col_name = 'Power Generation - Thermal Maximum Percentage of Revenues (%)'
        universe_df[f'exclusion_{exclusion_count}'] = None
        if col_name in universe_df.columns:
            universe_df[f'exclusion_{exclusion_count}'] = np.where(
                (universe_df[col_name] > 0),
                'exclude_thermal_power',
                None
            )
        else:
            logger.warning(f"Column '{col_name}' not found in data. Skipping this exclusion.")
        exclusion_count += 1
        
        # 7. Tobacco Screening
        # Check which tobacco columns exist in the data
        tobacco_columns = [
            'Tobacco - Production Maximum Percentage of Revenues (%)',
            'Tobacco - Distribution Maximum Percentage of Revenues (%)'
        ]
        
        available_tobacco_columns = ['ISIN']
        for col in tobacco_columns:
            if col in oekom_trustcarbon_df.columns:
                available_tobacco_columns.append(col)
            else:
                logger.warning(f"Column '{col}' not found in Oekom data. Skipping this in merge.")
        
        if len(available_tobacco_columns) > 1:  # If we have any tobacco columns
            # Merge tobacco data from Oekom Trust&Carbon
            universe_df = universe_df.merge(
                oekom_trustcarbon_df[available_tobacco_columns],
                left_on='ISIN Code',
                right_on='ISIN',
                how='left'
            ).drop('ISIN', axis=1)
        
        # Convert tobacco columns to numeric if they exist
        for col in tobacco_columns:
            if col in universe_df.columns:
                universe_df[col] = pd.to_numeric(universe_df[col], errors='coerce')
        
        # Tobacco Production > 5%
        col_name = 'Tobacco - Production Maximum Percentage of Revenues (%)'
        universe_df[f'exclusion_{exclusion_count}'] = None
        if col_name in universe_df.columns:
            universe_df[f'exclusion_{exclusion_count}'] = np.where(
                (universe_df[col_name] > 5),
                'exclude_tobacco_production',
                None
            )
        else:
            logger.warning(f"Column '{col_name}' not found in data. Skipping this exclusion.")
        exclusion_count += 1
        
        # Tobacco Distribution > 15%
        col_name = 'Tobacco - Distribution Maximum Percentage of Revenues (%)'
        universe_df[f'exclusion_{exclusion_count}'] = None
        if col_name in universe_df.columns:
            universe_df[f'exclusion_{exclusion_count}'] = np.where(
                (universe_df[col_name] > 15),
                'exclude_tobacco_distribution',
                None
            )
        else:
            logger.warning(f"Column '{col_name}' not found in data. Skipping this exclusion.")
        exclusion_count += 1
        
        # 8. Layoff Screening
        # Check if layoff_score_3m exists in sesamm data
        if 'layoff_score_3m' in sesamm_df.columns:
            # Merge SESAMm data
            universe_df = universe_df.merge(
                sesamm_df[['ISIN', 'layoff_score_3m']],
                left_on='ISIN Code',
                right_on='ISIN',
                how='left'
            ).drop('ISIN', axis=1)
            
            # Companies with layoff_score_3m > 0
            universe_df[f'exclusion_{exclusion_count}'] = np.where(
                (universe_df['layoff_score_3m'] > 0) | (universe_df['layoff_score_3m'].isna()),
                'exclude_layoffs_or_missing_score',
                None
            )
            exclusion_count += 1
        else:
            logger.warning("Column 'layoff_score_3m' not found in SESAMm data. Skipping layoff exclusion.")
        
        # Check for and remove duplicates in ICB data before merging
        if 'ISIN Code' in icb_df.columns:
            icb_before_count = len(icb_df)
            icb_df = icb_df.drop_duplicates(subset=['ISIN Code'])
            icb_after_count = len(icb_df)
            if icb_before_count != icb_after_count:
                logger.warning(f"Removed {icb_before_count - icb_after_count} duplicate rows from ICB data")
        
        # 9. Staff Rating Screening - Worst companies (below 20th percentile) by Region and Super Sector
        # Merge ICB data for supersector using the confirmed 'Supersector Code' column
        logger.info("Merging ICB data with 'Supersector Code' column")
        
        universe_df = universe_df.merge(
            icb_df[['ISIN Code', 'Supersector Code']],
            on='ISIN Code',
            how='left'
        )
        # Rename to standard 'Supersector' for consistent use
        universe_df.rename(columns={'Supersector Code': 'Supersector'}, inplace=True)
        
        # Check for duplicates after ICB merge
        before_count = len(universe_df)
        universe_df = universe_df.drop_duplicates(subset=['ISIN Code'])
        after_count = len(universe_df)
        if before_count != after_count:
            logger.warning(f"Removed {before_count - after_count} duplicate rows after ICB merge")
        
        # Merge CRStaffRatingNum from Oekom data
        universe_df = universe_df.merge(
            oekom_trustcarbon_df[['ISIN', 'CRStaffRatingNum']],
            left_on='ISIN Code',
            right_on='ISIN',
            how='left'
        ).drop('ISIN', axis=1)
        
        # Convert CRStaffRatingNum to numeric
        universe_df['CRStaffRatingNum'] = pd.to_numeric(universe_df['CRStaffRatingNum'], errors='coerce')
        
        # Add a Region column based on index field (similar to Area Flag in FRD4P)
        universe_df['Region'] = universe_df['index'].apply(
            lambda x: 'NA' if 'NA500' in str(x) 
            else 'AS' if 'AS500' in str(x)
            else 'EU' if ('EU500' in str(x) or 'EZ300' in str(x))
            else None
        )
        
        # Find companies below 20th percentile by Region and Super Sector
        excluded_isins = []
        
        # Group by Supersector and Region for staff rating screening
        for (sector, region), group in universe_df.groupby(['Supersector', 'Region']):
            if pd.notna(sector) and pd.notna(region) and len(group) > 0:
                logger.info(f"Processing sector: {sector}, region: {region}, group size: {len(group)}")
                # Sort by staff rating (lower is worse)
                sorted_group = group.sort_values('CRStaffRatingNum')
                n_companies = len(group)
                n_to_exclude = int(np.floor(n_companies * 0.1999999999))  # 20th percentile
                logger.info(f"Companies in group: {n_companies}, to exclude: {n_to_exclude}")
                if n_to_exclude > 0:
                    bottom_isins = sorted_group['ISIN Code'].iloc[:n_to_exclude].tolist()
                    excluded_isins.extend(bottom_isins)
                else:
                    logger.warning(f"No companies to exclude for sector {sector} and region {region}")
                    
        universe_df[f'exclusion_{exclusion_count}'] = None
        universe_df[f'exclusion_{exclusion_count}'] = np.where(
            universe_df['ISIN Code'].isin(excluded_isins),
            'exclude_staff_rating',
            None
        )
        exclusion_count += 1
        
        # Step 3: Calculate FFMC for all companies in the universe for ranking purposes
        # According to the rulebook, Step 3: Selection ranking should use the Cut-Off Date data
        logger.info("Calculating FFMC for all companies in universe...")

        # Always initialize the FFMC column first to avoid KeyError
        universe_df['FFMC'] = np.nan

        # Check if universe_df has the 'Price (EUR)' column
        if 'Price (EUR)' in universe_df.columns:
            logger.info("Using Price (EUR) from universe_df for FFMC calculation")
            universe_df['FFMC'] = universe_df['Free Float'] * universe_df['Number of Shares'] * universe_df['Price (EUR)']
        elif 'Mcap in EUR' in universe_df.columns:
            logger.info("Using Mcap in EUR to calculate FFMC")
            universe_df['FFMC'] = universe_df['Mcap in EUR'] * universe_df['Free Float']
        else:
            # Fall back to the old approach if neither 'Price (EUR)' nor 'Mcap in EUR' is available
            logger.warning("Neither 'Price (EUR)' nor 'Mcap in EUR' columns found in universe_df. Falling back to stock_co_df data.")
            
            # Check if stock_co_df has the necessary columns for calculating FFMC
            required_cols = ['Isin Code', 'Close Prc', 'Index_Currency']
            if all(col in stock_co_df.columns for col in required_cols):
                # Filter for records where Index_Currency is EUR
                eur_records = stock_co_df[stock_co_df['Index_Currency'] == 'EUR']
                
                if len(eur_records) > 0:
                    logger.info(f"Found {len(eur_records)} records with Index_Currency='EUR' in stock_co_df")
                    # Create a price lookup dataframe from stock_co_df (EUR records only)
                    # In case of multiple entries per ISIN, take the first one
                    price_lookup = eur_records[['Isin Code', 'Close Prc']].drop_duplicates('Isin Code').rename(
                        columns={'Isin Code': 'ISIN Code', 'Close Prc': 'Price'}
                    )
                    
                    # Merge price data from cut-off date
                    universe_df = universe_df.merge(
                        price_lookup[['ISIN Code', 'Price']],
                        on='ISIN Code',
                        how='left'
                    )
                    
                    # Calculate FFMC using the EUR Close Prc from stock_co_df (no FX conversion needed)
                    logger.info("Calculating FFMC using Close Prc from EUR records in stock_co_df (cut-off date data)")
                    universe_df['FFMC'] = universe_df['Free Float'] * universe_df['Number of Shares'] * universe_df['Price']
                else:
                    logger.warning("No records with Index_Currency='EUR' found in stock_co_df. Falling back to all records with FX conversion.")
                    
                    # Check if FX/Index Ccy column exists for conversion
                    if 'FX/Index Ccy' in stock_co_df.columns:
                        # Create a price lookup dataframe from all stock_co_df records
                        price_lookup = stock_co_df[['Isin Code', 'Close Prc', 'FX/Index Ccy']].drop_duplicates('Isin Code').rename(
                            columns={'Isin Code': 'ISIN Code', 'Close Prc': 'Price', 'FX/Index Ccy': 'FX Rate'}
                        )
                        
                        # Merge price and FX data from cut-off date
                        universe_df = universe_df.merge(
                            price_lookup[['ISIN Code', 'Price', 'FX Rate']],
                            on='ISIN Code',
                            how='left'
                        )
                        
                        # Calculate FFMC using the Close Prc and FX Rate from stock_co_df
                        logger.info("Calculating FFMC using Close Prc and FX Rate from stock_co_df (cut-off date data)")
                        universe_df['FX Rate'] = universe_df['FX Rate'].fillna(1.0)
                        universe_df['FFMC'] = universe_df['Free Float'] * universe_df['Number of Shares'] * universe_df['Price'] * universe_df['FX Rate']
                    else:
                        logger.error("Cannot calculate FFMC. Missing 'FX/Index Ccy' column in stock_co_df for currency conversion.")
            else:
                missing_cols = [col for col in required_cols if col not in stock_co_df.columns]
                logger.error(f"Cannot calculate FFMC. Missing columns in stock_co_df: {missing_cols} and missing 'Price (EUR)' in universe_df")

        # Create rank based on FFMC (descending)
        # Fill NaN values with a large negative value to ensure they get ranked last
        logger.info("Creating rank based on FFMC...")
        universe_df['Rank'] = universe_df['FFMC'].fillna(float('-inf')).rank(ascending=False, method='min')

        # Mark rows with NaN FFMC explicitly
        universe_df['Rank'] = np.where(
            universe_df['FFMC'].isna(),
            np.nan,
            universe_df['Rank']
        )
        
        # Create list of all exclusion columns that actually exist in the dataframe
        all_possible_exclusion_columns = [f'exclusion_{i}' for i in range(1, exclusion_count)]
        exclusion_columns = [col for col in all_possible_exclusion_columns if col in universe_df.columns]
        
        logger.info(f"Found {len(exclusion_columns)} valid exclusion columns: {exclusion_columns}")
        
        # Create a general exclusion flag based on all exclusion columns
        universe_df['Excluded'] = 'No'
        # Check all exclusion columns - if any have a value, mark as excluded
        for exclusion_col in exclusion_columns:
            universe_df.loc[universe_df[exclusion_col].notna(), 'Excluded'] = 'Yes'
        
        # Add an 'Exclusion Reason' column that concatenates all applicable exclusion reasons
        universe_df['Exclusion Reason'] = ''
        for exclusion_col in exclusion_columns:
            # For rows where this exclusion applies, add the reason to the list
            mask = universe_df[exclusion_col].notna()
            universe_df.loc[mask, 'Exclusion Reason'] = universe_df.loc[mask, 'Exclusion Reason'] + universe_df.loc[mask, exclusion_col] + '; '
        
        # Remove trailing semicolon and space if present
        universe_df['Exclusion Reason'] = universe_df['Exclusion Reason'].str.rstrip('; ')
        
        # Step 4: Select companies that have no exclusions (all exclusion columns are None)
        logger.info("Selecting eligible companies...")
        if exclusion_columns:
            eligible_df = universe_df[
                universe_df[exclusion_columns].isna().all(axis=1)
            ].copy()
        else:
            logger.warning("No valid exclusion columns found. Using all companies as eligible.")
            eligible_df = universe_df.copy()
        
        # If we have less than 50 companies, adjust the layoff score threshold
        if len(eligible_df) < 50:
            logger.warning(f"Only {len(eligible_df)} companies passed all exclusions. Adjusting layoff score threshold.")
            # Reset the universe and reapply exclusions except for layoff score
            universe_df_copy = universe_df.copy()
            layoff_exclusion = None
            # Find which exclusion column is for layoffs
            for col in exclusion_columns:
                if universe_df[col].eq('exclude_layoffs').any():
                    layoff_exclusion = col
                    break
            
            if layoff_exclusion:
                logger.info(f"Removing layoff exclusion column: {layoff_exclusion}")
                new_exclusion_columns = [col for col in exclusion_columns if col != layoff_exclusion]
                if new_exclusion_columns:
                    eligible_df = universe_df_copy[
                        universe_df_copy[new_exclusion_columns].isna().all(axis=1)
                    ].copy()
                else:
                    eligible_df = universe_df_copy.copy()
            else:
                logger.warning("Could not identify layoff exclusion column. Using available eligible companies.")
        
        # Step 5: Select top 40 by FFMC (already calculated in Step 3 using the cut-off date data)
        logger.info("Selecting top 40 companies by FFMC...")
        # Sort by the FFMC computed using the cut-off date data
        final_selection = eligible_df.sort_values('FFMC', ascending=False).head(40).copy()

        # Add Symbol column to final_selection by looking up ISIN and MIC in stock_eod_df
        logger.info("Adding Symbol column to final_selection...")
        if all(col in stock_eod_df.columns for col in ['Isin Code', 'MIC', '#Symbol']):
            # Create a dictionary for faster lookup
            symbol_map = {}
            for _, row in stock_eod_df.iterrows():
                key = (row['Isin Code'], row['MIC'])
                if key not in symbol_map and pd.notna(row['#Symbol']):
                    symbol_map[key] = row['#Symbol']
            
            # Add Symbol column to final_selection
            final_selection['Symbol'] = final_selection.apply(
                lambda row: symbol_map.get((row['ISIN Code'], row['MIC']), None), 
                axis=1
            )
            
            logger.info(f"Added Symbol to {final_selection['Symbol'].notna().sum()} companies out of {len(final_selection)}")
            
            # Check if any companies are missing a Symbol
            if final_selection['Symbol'].isna().any():
                logger.warning(f"{final_selection['Symbol'].isna().sum()} companies don't have a matching Symbol in stock_eod_df")
        else:
            logger.warning("Cannot add Symbol. Missing required columns in stock_eod_df.")
            final_selection['Symbol'] = None

        # Step 6: Calculate capping factor (max weight 10%)
        # Use the most recent pricing data from stock_eod_df for the capping calculation
        logger.info("Calculating weights and capping factors using the most recent pricing data...")

        # For capping factor calculation, we want to use the most recent data (stock_eod_df)
        # We need to create separate columns for the EOD pricing to distinguish from the cut-off date pricing

        # Check if stock_eod_df has the necessary columns and Symbol column was added successfully
        required_eod_cols = ['#Symbol', 'Close Prc', 'FX/Index Ccy', 'Index_Currency']
        if all(col in stock_eod_df.columns for col in required_eod_cols) and 'Symbol' in final_selection.columns and final_selection['Symbol'].notna().any():
            # Get list of Symbols from final selection to look up in stock_eod_df
            selected_symbols = final_selection['Symbol'].dropna().tolist()
            
            # Filter for records matching selected symbols and with Index_Currency='EUR'
            matched_eur_records = stock_eod_df[
                (stock_eod_df['#Symbol'].isin(selected_symbols)) & 
                (stock_eod_df['Index_Currency'] == 'EUR')
            ]
            
            if len(matched_eur_records) > 0:
                # Create a mapping from Symbol to FX/Index Ccy from the EUR records
                logger.info(f"Found {len(matched_eur_records)} records with Index_Currency='EUR' for selected Symbols")
                fx_map = dict(zip(matched_eur_records['#Symbol'], matched_eur_records['FX/Index Ccy']))
                
                # Create a price lookup dataframe using all records (for maximum coverage)
                # First get all records for the selected symbols
                all_selected_records = stock_eod_df[stock_eod_df['#Symbol'].isin(selected_symbols)]
                
                # Then create a lookup dataframe with Symbol, taking the first occurrence of each Symbol
                eod_price_lookup = all_selected_records[['#Symbol', 'Close Prc']].drop_duplicates('#Symbol').rename(
                    columns={'#Symbol': 'Symbol', 'Close Prc': 'EOD_Price'}
                )
                
                # Add FX/Index Ccy from EUR records mapping
                eod_price_lookup['EOD_FX_Rate'] = eod_price_lookup['Symbol'].map(fx_map)
                
                # For Symbols that don't have FX/Index Ccy from EUR records, get from the general stock_eod_df
                missing_fx = eod_price_lookup['EOD_FX_Rate'].isna()
                if missing_fx.any():
                    logger.warning(f"{missing_fx.sum()} Symbols don't have FX rates from EUR index records, using general FX rates")
                    # Create a general FX map as fallback
                    general_fx_map = dict(zip(stock_eod_df['#Symbol'], stock_eod_df['FX/Index Ccy']))
                    # Only fill missing FX rates
                    missing_symbols = eod_price_lookup.loc[missing_fx, 'Symbol']
                    for symbol in missing_symbols:
                        if symbol in general_fx_map:
                            eod_price_lookup.loc[eod_price_lookup['Symbol'] == symbol, 'EOD_FX_Rate'] = general_fx_map[symbol]
                
                # Merge EOD price and FX data with final_selection based on Symbol
                final_selection = final_selection.merge(
                    eod_price_lookup[['Symbol', 'EOD_Price', 'EOD_FX_Rate']],
                    on='Symbol',
                    how='left'
                )
            else:
                logger.warning("No records with Index_Currency='EUR' found in stock_eod_df for selected Symbols. Falling back to ISIN matching.")
                # Fall back to ISIN matching if no EUR records found for symbols
                selected_isins = final_selection['ISIN Code'].tolist()
                all_selected_records = stock_eod_df[stock_eod_df['Isin Code'].isin(selected_isins)]
                
                eod_price_lookup = all_selected_records[['Isin Code', 'Close Prc', 'FX/Index Ccy']].drop_duplicates('Isin Code').rename(
                    columns={'Isin Code': 'ISIN Code', 'Close Prc': 'EOD_Price', 'FX/Index Ccy': 'EOD_FX_Rate'}
                )
                
                # Merge EOD price and FX data
                final_selection = final_selection.merge(
                    eod_price_lookup[['ISIN Code', 'EOD_Price', 'EOD_FX_Rate']],
                    on='ISIN Code',
                    how='left'
                )
        else:
            logger.warning("Cannot use Symbol matching. Falling back to ISIN matching for price and FX data.")
            # Fall back to the original ISIN-based approach
            selected_isins = final_selection['ISIN Code'].tolist()
            
            if 'Index_Currency' in stock_eod_df.columns:
                # Filter for records matching selected ISINs and with Index_Currency='EUR'
                matched_eur_records = stock_eod_df[
                    (stock_eod_df['Isin Code'].isin(selected_isins)) & 
                    (stock_eod_df['Index_Currency'] == 'EUR')
                ]
                
                if len(matched_eur_records) > 0:
                    # Create a mapping from Isin Code to FX/Index Ccy from the EUR records
                    logger.info(f"Found {len(matched_eur_records)} records with Index_Currency='EUR' for selected ISINs")
                    fx_map = dict(zip(matched_eur_records['Isin Code'], matched_eur_records['FX/Index Ccy']))
                    
                    # Create a price lookup dataframe using all records (for maximum coverage)
                    all_selected_records = stock_eod_df[stock_eod_df['Isin Code'].isin(selected_isins)]
                    eod_price_lookup = all_selected_records[['Isin Code', 'Close Prc']].drop_duplicates('Isin Code').rename(
                        columns={'Isin Code': 'ISIN Code', 'Close Prc': 'EOD_Price'}
                    )
                    
                    # Add FX/Index Ccy from EUR records mapping
                    eod_price_lookup['EOD_FX_Rate'] = eod_price_lookup['ISIN Code'].map(fx_map)
                    
                    # For ISINs that don't have FX/Index Ccy from EUR records, get from the general stock_eod_df
                    missing_fx = eod_price_lookup['EOD_FX_Rate'].isna()
                    if missing_fx.any():
                        # Create a general FX map as fallback
                        general_fx_map = dict(zip(stock_eod_df['Isin Code'], stock_eod_df['FX/Index Ccy']))
                        # Only fill missing FX rates
                        missing_isins = eod_price_lookup.loc[missing_fx, 'ISIN Code']
                        for isin in missing_isins:
                            if isin in general_fx_map:
                                eod_price_lookup.loc[eod_price_lookup['ISIN Code'] == isin, 'EOD_FX_Rate'] = general_fx_map[isin]
                else:
                    logger.warning("No records with Index_Currency='EUR' found. Using all available FX rates.")
                    eod_price_lookup = stock_eod_df[['Isin Code', 'Close Prc', 'FX/Index Ccy']].drop_duplicates('Isin Code').rename(
                        columns={'Isin Code': 'ISIN Code', 'Close Prc': 'EOD_Price', 'FX/Index Ccy': 'EOD_FX_Rate'}
                    )
            else:
                # Basic fallback if Index_Currency is not available
                eod_price_lookup = stock_eod_df[['Isin Code', 'Close Prc', 'FX/Index Ccy']].drop_duplicates('Isin Code').rename(
                    columns={'Isin Code': 'ISIN Code', 'Close Prc': 'EOD_Price', 'FX/Index Ccy': 'EOD_FX_Rate'}
                )
            
            # Merge EOD price and FX data
            final_selection = final_selection.merge(
                eod_price_lookup[['ISIN Code', 'EOD_Price', 'EOD_FX_Rate']],
                on='ISIN Code',
                how='left'
            )

        # Handle potential NaN values in EOD FX Rate
        final_selection['EOD_FX_Rate'] = final_selection['EOD_FX_Rate'].fillna(1.0)

        # Handle potential NaN values in EOD_Price
        if 'EOD_Price' in final_selection.columns and final_selection['EOD_Price'].isna().any():
            logger.warning(f"{final_selection['EOD_Price'].isna().sum()} companies missing EOD_Price")
            
            # For companies missing EOD_Price, try to get it from other sources if available
            if 'Price (EUR)' in final_selection.columns:
                final_selection.loc[final_selection['EOD_Price'].isna(), 'EOD_Price'] = final_selection.loc[final_selection['EOD_Price'].isna(), 'Price (EUR)']
                logger.info("Filled missing EOD_Price values with Price (EUR)")

        # Calculate EOD FFMC using the most recent pricing data
        final_selection['EOD_FFMC'] = final_selection['Free Float'] * final_selection['Number of Shares'] * final_selection['EOD_Price'] * final_selection['EOD_FX_Rate']

        # Use the EOD FFMC for weight calculations
        total_mcap = final_selection['EOD_FFMC'].sum()
        final_selection['Weight'] = final_selection['EOD_FFMC'] / total_mcap
        
        # Identify companies above 10% weight
        capped_companies = final_selection[final_selection['Weight'] > 0.10].copy()
        uncapped_companies = final_selection[final_selection['Weight'] <= 0.10].copy()
        
        if not capped_companies.empty:
            # Calculate the excess weight that needs to be redistributed
            excess_weight = capped_companies['Weight'].sum() - (len(capped_companies) * 0.10)
            
            # Redistribute excess weight to uncapped companies proportionally
            if not uncapped_companies.empty:
                uncapped_weight_sum = uncapped_companies['Weight'].sum()
                scaling_factor = (uncapped_weight_sum + excess_weight) / uncapped_weight_sum
                
                # Apply the new weights
                for idx, row in final_selection.iterrows():
                    if row['Weight'] > 0.10:
                        # Use EOD_FFMC if available, otherwise use FFMC
                        if 'EOD_FFMC' in final_selection.columns:
                            final_selection.at[idx, 'Capping'] = 0.10 * total_mcap / row['EOD_FFMC']
                        else:
                            final_selection.at[idx, 'Capping'] = 0.10 * total_mcap / row['FFMC']
                    else:
                        if 'EOD_FFMC' in final_selection.columns:
                            final_selection.at[idx, 'Capping'] = row['Weight'] * scaling_factor * total_mcap / row['EOD_FFMC']
                        else:
                            final_selection.at[idx, 'Capping'] = row['Weight'] * scaling_factor * total_mcap / row['FFMC']
            else:
                # If all companies are capped, set them all to 10%
                if 'EOD_FFMC' in final_selection.columns:
                    final_selection['Capping'] = 0.10 * total_mcap / final_selection['EOD_FFMC']
                else:
                    final_selection['Capping'] = 0.10 * total_mcap / final_selection['FFMC']
        else:
            # If no companies are above 10%, no capping needed
            final_selection['Capping'] = 1.0
        
        # Calculate the post-capping weights
        logger.info("Calculating post-capping weights...")
        if 'EOD_FFMC' in final_selection.columns:
            final_selection['Capped_FFMC'] = final_selection['EOD_FFMC'] * final_selection['Capping']
        else:
            final_selection['Capped_FFMC'] = final_selection['FFMC'] * final_selection['Capping']
            
        total_capped_mcap = final_selection['Capped_FFMC'].sum()
        final_selection['Post_Capping_Weight'] = final_selection['Capped_FFMC'] / total_capped_mcap
        
        # Calculate normalized capping factor (1.0 as the highest)
        max_capping = final_selection['Capping'].max()
        final_selection['Final Capping'] = final_selection['Capping'] / max_capping
        
        # Create final output DataFrame
        logger.info("Creating final output dataframe...")
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
            final_selection, 
            stock_eod_df, 
            index, 
            isin_column='ISIN Code'
        )
        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']
        # Save output files
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
                # Add a detailed sheet with all information from final_selection
                final_selection.to_excel(writer, sheet_name='Detailed Selection', index=False)
                universe_df.to_excel(writer, sheet_name='Full Universe', index=False)
            
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