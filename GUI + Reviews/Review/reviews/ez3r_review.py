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

def run_ez3r_review(date, co_date, effective_date, index="EZ3R", isin="FRESG0003391", 
                   area="US", area2="EU", type="STOCK", universe="eurozone_300", 
                   feed="Reuters", currency="EUR", year=None):

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
            ['ff', 'sustainalytics', 'icb', 'eurozone_300', 'physical_risk_score']
        )

        # Validate data loading
        if ref_data.get('eurozone_300') is None:
            raise ValueError("Failed to load eurozone_300 universe data")

        # EZ3R universe - Step 1
        ez3r_universe = ref_data['eurozone_300']
        ez3r_df = pd.DataFrame(ez3r_universe)
        ff_df = ref_data['ff']
        icb_df = ref_data['icb']
        physical_risk_df = ref_data.get('physical_risk_score')

        logger.info(f"Starting universe size: {len(ez3r_df)}")
        
        # Add the required columns to the combined dataframe
        ez3r_df['Effective Date of Review'] = effective_date

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')
        
        # Chain all data preparation operations
        universe_df = (ez3r_df
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
            # Merge ICB data for Subsector Code
            .merge(
                icb_df[['ISIN Code', 'Subsector Code']].drop_duplicates(subset='ISIN Code', keep='first'),
                left_on='ISIN code',
                right_on='ISIN Code',
                how='left'
            )
            .drop('ISIN Code', axis=1)
        )
        
        # Use the merged Free Float Round value
        universe_df["Free Float"] = universe_df["Free Float Round:"]
        
        # ===================================================================
        # MERGE PHYSICAL RISK SCORE AT UNIVERSE LEVEL
        # ===================================================================
        logger.info("Merging Physical Risk Score data at universe level...")
        
        # Create Industry Code to Industry Name mapping
        industry_code_to_name = {
            '10': 'Technology',
            '15': 'Telecommunications',
            '20': 'Health Care',
            '30': 'Financials',
            '35': 'Real Estate',
            '40': 'Consumer Discretionary',
            '45': 'Consumer Staples',
            '50': 'Industrials',
            '55': 'Basic Materials',
            '60': 'Energy',
            '65': 'Utilities'
        }
        
        # Extract Industry Code from Subsector Code (first 2 digits)
        universe_df['Industry Code'] = universe_df['Subsector Code'].astype(str).str[:2]
        
        # Map Industry Code to Industry Name
        universe_df['Industry Name'] = universe_df['Industry Code'].map(industry_code_to_name)
        
        # Extract country code from ISIN (first 2 characters)
        universe_df['iso_country_incorp'] = universe_df['ISIN code'].str[:2]
        
        # Merge Physical Risk Score
        if physical_risk_df is not None and not physical_risk_df.empty:
            logger.info(f"Physical Risk Score data loaded with {len(physical_risk_df)} rows")
            
            # Ensure proper column names in physical_risk_df
            physical_risk_merge = physical_risk_df[['Industry Name', 'iso_country_incorp', 'Physical_Risk_Score']].copy()
            
            # Remove duplicates in physical risk data
            physical_risk_merge = physical_risk_merge.drop_duplicates(subset=['Industry Name', 'iso_country_incorp'], keep='first')
            
            logger.info(f"Physical Risk Score unique combinations: {len(physical_risk_merge)}")
            
            # Merge on Industry Name and country
            universe_df = universe_df.merge(
                physical_risk_merge,
                on=['Industry Name', 'iso_country_incorp'],
                how='left'
            )
            
            # Check for missing Physical Risk Scores
            missing_scores = universe_df['Physical_Risk_Score'].isna().sum()
            if missing_scores > 0:
                logger.warning(f"{missing_scores} companies missing Physical Risk Score after merge")
                # Log which combinations are missing
                missing_combos = universe_df[universe_df['Physical_Risk_Score'].isna()][['Industry Name', 'iso_country_incorp', 'Company']].drop_duplicates()
                logger.warning(f"Missing combinations:\n{missing_combos.to_string()}")
            else:
                logger.info("All companies successfully matched with Physical Risk Scores")
                
        else:
            logger.warning("Physical Risk Score data not available. Setting Physical_Risk_Score to NaN.")
            universe_df['Physical_Risk_Score'] = np.nan
        
        # ===================================================================
        # END PHYSICAL RISK SCORE MERGE
        # ===================================================================
        
        # Step 1: Filter to Eurozone countries only
        logger.info("Step 1: Filtering to Eurozone countries...")
        eurozone_prefixes = ['IT', 'NL', 'BE', 'FR', 'ES', 'DE', 'IE', 'AT', 'LU', 'PT', 'FI']
        universe_df['is_eurozone'] = universe_df['ISIN code'].str[:2].isin(eurozone_prefixes)
        eurozone_df = universe_df[universe_df['is_eurozone']].copy()
        logger.info(f"After Eurozone filter: {len(eurozone_df)} companies")
        
        # Step 2: Calculate FFMC and select top 100
        logger.info("Step 2: Calculating FFMC and selecting top 100...")
        eurozone_df['FFMC'] = eurozone_df['Free Float'] * eurozone_df['Number of Shares'] * eurozone_df["Price"]
        eurozone_df = eurozone_df.sort_values('FFMC', ascending=False)
        eurozone_df['Rank_FFMC'] = range(1, len(eurozone_df) + 1)
        
        top_100_df = eurozone_df.head(100).copy()
        logger.info(f"Top 100 by FFMC selected: {len(top_100_df)} companies")
        
        # Merge Sustainalytics data
        logger.info("Merging Sustainalytics data...")
        
        sustainalytics_raw = ref_data.get('sustainalytics')

        # Check if sustainalytics data was loaded
        if sustainalytics_raw is None:
            logger.warning("Sustainalytics data not available. Skipping sustainalytics merge.")
            selection_df = top_100_df
        else:
            # The file has row 0 = headers (text) and row 1 = codes (numbers as text)
            # Define the required codes (these are in row 1 of the Excel file)
            required_codes = [
                '231112111799',  # Global Standards
                '171611102999',  # Controversial Weapons - Tailormade
                '171613102999',  # Controversial Weapons - Non-tailormade
                '172111112999',  # Military Contracting - Weapons
                '171017141199',  # Military Contracting - Weapon-related
                '171017171199',  # Military Contracting - Non-weapon-related
                '171713112999',  # Small Arms
                '172911112999',  # Tobacco - Production
                '171020141199',  # Tobacco - Retail
                '171020171199',  # Tobacco - Related
                '171311112999',  # Alcoholic Beverages - Production
                '171011141199',  # Alcoholic Beverages - Retail
                '171011171199',  # Alcoholic Beverages - Related
                '171911112999',  # Gambling - Operations
                '171015171199',  # Gambling - Supporting
                '171015141199',  # Gambling - Equipment
                '173316171899',  # Oil & Gas - Generation
                '173316102999',  # Oil & Gas - Ownership
                '173012171899',  # Oil Sands - Extraction
                '173012102999',  # Oil Sands - Ownership
                '173211112999',  # Shale Energy - Extraction
                '173212102999',  # Shale Energy - Ownership
                '173111112999',  # Arctic Oil & Gas - Extraction
                '173112102999',  # Arctic Oil & Gas - Ownership
                '172811112999',  # Thermal Coal - Extraction
                '172812102999',  # Thermal Coal - Extraction Ownership
                '172813112999',  # Thermal Coal - Power Generation
                '172814102999',  # Thermal Coal - Power Generation Ownership
                '171025261899',  # Thermal Coal - Supporting Products/Services
                '171415102999',  # Animal Testing
            ]
            
            # Get the first row which contains the codes
            if len(sustainalytics_raw) > 0:
                codes_row = sustainalytics_raw.iloc[0].copy()
                
                # Convert all numeric values in row 1 to integers (as strings)
                for col in codes_row.index:
                    if col != 'ISIN':
                        try:
                            if pd.notna(codes_row[col]):
                                codes_row[col] = str(int(float(codes_row[col])))
                        except (ValueError, TypeError):
                            codes_row[col] = str(codes_row[col]).strip()
                
                # Find columns where the first row value matches our required codes
                cols_to_keep = ['ISIN']
                col_name_mapping = {}
                
                for col_name in sustainalytics_raw.columns:
                    if col_name != 'ISIN':
                        cell_value = codes_row[col_name]
                        if cell_value in required_codes:
                            cols_to_keep.append(col_name)
                            col_name_mapping[col_name] = cell_value
                
                if len(cols_to_keep) > 1:
                    # Take only the columns we need and skip the first row (which has codes)
                    sustainalytics_filtered = sustainalytics_raw[cols_to_keep].iloc[1:].copy()
                    
                    # Rename columns to include both code and original header name (except ISIN)
                    rename_dict = {}
                    for col in cols_to_keep:
                        if col != 'ISIN':
                            code = col_name_mapping[col]
                            original_header = col
                            rename_dict[col] = f"{code} - {original_header}"
                    
                    sustainalytics_filtered.rename(columns=rename_dict, inplace=True)
                    
                    # Remove duplicates by ISIN (first match only)
                    sustainalytics_filtered = sustainalytics_filtered.drop_duplicates(subset='ISIN', keep='first')
                    
                    # Merge with selection_df
                    selection_df = top_100_df.merge(
                        sustainalytics_filtered,
                        left_on='ISIN code',
                        right_on='ISIN',
                        how='left'
                    ).drop('ISIN', axis=1, errors='ignore')
                    
                    logger.info(f"Sustainalytics merge completed. Added {len(cols_to_keep)-1} columns.")
                else:
                    logger.warning("No matching sustainalytics columns found.")
                    selection_df = top_100_df
            else:
                logger.warning("Sustainalytics dataframe is empty")
                selection_df = top_100_df
        
        # Apply Exclusion Criteria
        logger.info("Applying exclusion criteria...")
        
        # Helper function to find column by code
        def find_column_by_code(df, code):
            for col in df.columns:
                if col.startswith(f"{code} - "):
                    return col
            return None
        
        # Helper function to safely convert to numeric
        def safe_numeric(series):
            return pd.to_numeric(series, errors='coerce').fillna(0)
        
        # EXCLUSION 1: Global Standards Screening
        # 231112111799 == 'Non-Compliant'
        col_231112111799 = find_column_by_code(selection_df, '231112111799')
        if col_231112111799:
            selection_df['exclusion_global_standards'] = selection_df[col_231112111799] == 'Non-Compliant'
        else:
            selection_df['exclusion_global_standards'] = False
        
        # EXCLUSION 2: Controversial Weapons - Tailormade
        # 171611102999 (Category of Involvement ID = "CW1")
        col_171611102999 = find_column_by_code(selection_df, '171611102999')
        if col_171611102999:
            selection_df['exclusion_controversial_weapons_tailormade'] = selection_df[col_171611102999] == 'CW1'
        else:
            selection_df['exclusion_controversial_weapons_tailormade'] = False
        
        # EXCLUSION 3: Controversial Weapons - Non-tailormade
        # 171613102999 (Category of Involvement ID = "CW3")
        col_171613102999 = find_column_by_code(selection_df, '171613102999')
        if col_171613102999:
            selection_df['exclusion_controversial_weapons_non_tailormade'] = selection_df[col_171613102999] == 'CW3'
        else:
            selection_df['exclusion_controversial_weapons_non_tailormade'] = False
        
        # EXCLUSION 4: Military Contracting - Weapons
        # 172111112999 > 0%
        col_172111112999 = find_column_by_code(selection_df, '172111112999')
        if col_172111112999:
            selection_df['exclusion_military_contracting_weapons'] = safe_numeric(selection_df[col_172111112999]) > 0
        else:
            selection_df['exclusion_military_contracting_weapons'] = False
        
        # EXCLUSION 5: Military Contracting - Related products/services
        # 171017141199 + 171017171199 >= 5%
        col_171017141199 = find_column_by_code(selection_df, '171017141199')
        col_171017171199 = find_column_by_code(selection_df, '171017171199')
        sum_military_related = 0
        if col_171017141199:
            sum_military_related += safe_numeric(selection_df[col_171017141199])
        if col_171017171199:
            sum_military_related += safe_numeric(selection_df[col_171017171199])
        selection_df['exclusion_military_contracting_related'] = sum_military_related >= 5
        
        # EXCLUSION 6: Small Arms
        # 171713112999 > 0%
        col_171713112999 = find_column_by_code(selection_df, '171713112999')
        if col_171713112999:
            selection_df['exclusion_small_arms'] = safe_numeric(selection_df[col_171713112999]) > 0
        else:
            selection_df['exclusion_small_arms'] = False
        
        # EXCLUSION 7: Tobacco - Production
        # 172911112999 > 0%
        col_172911112999 = find_column_by_code(selection_df, '172911112999')
        if col_172911112999:
            selection_df['exclusion_tobacco_production'] = safe_numeric(selection_df[col_172911112999]) > 0
        else:
            selection_df['exclusion_tobacco_production'] = False
        
        # EXCLUSION 8: Tobacco - Retail and Related
        # 171020141199 + 171020171199 >= 10%
        col_171020141199 = find_column_by_code(selection_df, '171020141199')
        col_171020171199 = find_column_by_code(selection_df, '171020171199')
        sum_tobacco_retail = 0
        if col_171020141199:
            sum_tobacco_retail += safe_numeric(selection_df[col_171020141199])
        if col_171020171199:
            sum_tobacco_retail += safe_numeric(selection_df[col_171020171199])
        selection_df['exclusion_tobacco_retail'] = sum_tobacco_retail >= 10
        
        # EXCLUSION 9: Alcoholic Beverages - Production
        # 171311112999 >= 5%
        col_171311112999 = find_column_by_code(selection_df, '171311112999')
        if col_171311112999:
            selection_df['exclusion_alcohol_production'] = safe_numeric(selection_df[col_171311112999]) >= 5
        else:
            selection_df['exclusion_alcohol_production'] = False
        
        # EXCLUSION 10: Alcoholic Beverages - Retail and Related
        # 171011141199 + 171011171199 >= 10%
        col_171011141199 = find_column_by_code(selection_df, '171011141199')
        col_171011171199 = find_column_by_code(selection_df, '171011171199')
        sum_alcohol_retail = 0
        if col_171011141199:
            sum_alcohol_retail += safe_numeric(selection_df[col_171011141199])
        if col_171011171199:
            sum_alcohol_retail += safe_numeric(selection_df[col_171011171199])
        selection_df['exclusion_alcohol_retail'] = sum_alcohol_retail >= 10
        
        # EXCLUSION 11: Gambling - Operations
        # 171911112999 >= 5%
        col_171911112999 = find_column_by_code(selection_df, '171911112999')
        if col_171911112999:
            selection_df['exclusion_gambling_operations'] = safe_numeric(selection_df[col_171911112999]) >= 5
        else:
            selection_df['exclusion_gambling_operations'] = False
        
        # EXCLUSION 12: Gambling - Supporting Products/Services
        # 171015171199 + 171015141199 >= 10%
        col_171015171199 = find_column_by_code(selection_df, '171015171199')
        col_171015141199 = find_column_by_code(selection_df, '171015141199')
        sum_gambling_support = 0
        if col_171015171199:
            sum_gambling_support += safe_numeric(selection_df[col_171015171199])
        if col_171015141199:
            sum_gambling_support += safe_numeric(selection_df[col_171015141199])
        selection_df['exclusion_gambling_support'] = sum_gambling_support >= 10
        
        # EXCLUSION 13: Oil & Gas - Generation
        # 173316171899 > 0%
        col_173316171899 = find_column_by_code(selection_df, '173316171899')
        if col_173316171899:
            selection_df['exclusion_oil_gas_generation'] = safe_numeric(selection_df[col_173316171899]) > 0
        else:
            selection_df['exclusion_oil_gas_generation'] = False
        
        # EXCLUSION 14: Oil & Gas - Ownership
        # 173316102999 (Category of Involvement Id = "OG6")
        col_173316102999 = find_column_by_code(selection_df, '173316102999')
        if col_173316102999:
            selection_df['exclusion_oil_gas_ownership'] = selection_df[col_173316102999] == 'OG6'
        else:
            selection_df['exclusion_oil_gas_ownership'] = False
        
        # EXCLUSION 15: Oil Sands - Extraction
        # 173012171899 > 0%
        col_173012171899 = find_column_by_code(selection_df, '173012171899')
        if col_173012171899:
            selection_df['exclusion_oil_sands_extraction'] = safe_numeric(selection_df[col_173012171899]) > 0
        else:
            selection_df['exclusion_oil_sands_extraction'] = False
        
        # EXCLUSION 16: Oil Sands - Ownership
        # 173012102999 (Category of Involvement Id = "OS2")
        col_173012102999 = find_column_by_code(selection_df, '173012102999')
        if col_173012102999:
            selection_df['exclusion_oil_sands_ownership'] = selection_df[col_173012102999] == 'OS2'
        else:
            selection_df['exclusion_oil_sands_ownership'] = False
        
        # EXCLUSION 17: Shale Energy - Extraction
        # 173211112999 > 0%
        col_173211112999 = find_column_by_code(selection_df, '173211112999')
        if col_173211112999:
            selection_df['exclusion_shale_extraction'] = safe_numeric(selection_df[col_173211112999]) > 0
        else:
            selection_df['exclusion_shale_extraction'] = False
        
        # EXCLUSION 18: Shale Energy - Ownership
        # 173212102999 (Category of Involvement Id = "SE2")
        col_173212102999 = find_column_by_code(selection_df, '173212102999')
        if col_173212102999:
            selection_df['exclusion_shale_ownership'] = selection_df[col_173212102999] == 'SE2'
        else:
            selection_df['exclusion_shale_ownership'] = False
        
        # EXCLUSION 19: Arctic Oil & Gas - Extraction
        # 173111112999 > 0%
        col_173111112999 = find_column_by_code(selection_df, '173111112999')
        if col_173111112999:
            selection_df['exclusion_arctic_extraction'] = safe_numeric(selection_df[col_173111112999]) > 0
        else:
            selection_df['exclusion_arctic_extraction'] = False
        
        # EXCLUSION 20: Arctic Oil & Gas - Ownership
        # 173112102999 (Category of Involvement Id = "AC2")
        col_173112102999 = find_column_by_code(selection_df, '173112102999')
        if col_173112102999:
            selection_df['exclusion_arctic_ownership'] = selection_df[col_173112102999] == 'AC2'
        else:
            selection_df['exclusion_arctic_ownership'] = False
        
        # EXCLUSION 21: Thermal Coal - Extraction
        # 172811112999 > 0%
        col_172811112999 = find_column_by_code(selection_df, '172811112999')
        if col_172811112999:
            selection_df['exclusion_thermal_coal_extraction'] = safe_numeric(selection_df[col_172811112999]) > 0
        else:
            selection_df['exclusion_thermal_coal_extraction'] = False
        
        # EXCLUSION 22: Thermal Coal - Extraction Ownership
        # 172812102999 (Category of Involvement Id = "TC2")
        col_172812102999 = find_column_by_code(selection_df, '172812102999')
        if col_172812102999:
            selection_df['exclusion_thermal_coal_extraction_ownership'] = selection_df[col_172812102999] == 'TC2'
        else:
            selection_df['exclusion_thermal_coal_extraction_ownership'] = False
        
        # EXCLUSION 23: Thermal Coal - Power Generation
        # 172813112999 > 0%
        col_172813112999 = find_column_by_code(selection_df, '172813112999')
        if col_172813112999:
            selection_df['exclusion_thermal_coal_power_generation'] = safe_numeric(selection_df[col_172813112999]) > 0
        else:
            selection_df['exclusion_thermal_coal_power_generation'] = False
        
        # EXCLUSION 24: Thermal Coal - Power Generation Ownership
        # 172814102999 (Category of Involvement Id = "TC4")
        col_172814102999 = find_column_by_code(selection_df, '172814102999')
        if col_172814102999:
            selection_df['exclusion_thermal_coal_power_ownership'] = selection_df[col_172814102999] == 'TC4'
        else:
            selection_df['exclusion_thermal_coal_power_ownership'] = False
        
        # EXCLUSION 25: Thermal Coal - Supporting Products/Services
        # 171025261899 (Category of Involvement Id = "TC6")
        col_171025261899 = find_column_by_code(selection_df, '171025261899')
        if col_171025261899:
            selection_df['exclusion_thermal_coal_supporting'] = safe_numeric(selection_df[col_171025261899]) > 0
        else:
            selection_df['exclusion_thermal_coal_supporting'] = False
        
        # EXCLUSION 26: Animal Testing
        # 171415102999 (Category of Involvement Id = "AT4")
        col_171415102999 = find_column_by_code(selection_df, '171415102999')
        if col_171415102999:
            selection_df['exclusion_animal_testing'] = selection_df[col_171415102999] == 'AT4'
        else:
            selection_df['exclusion_animal_testing'] = False
        
        # General Exclusion: Any exclusion criteria met
        selection_df['general_exclusion'] = (
            selection_df['exclusion_global_standards'] |
            selection_df['exclusion_controversial_weapons_tailormade'] |
            selection_df['exclusion_controversial_weapons_non_tailormade'] |
            selection_df['exclusion_military_contracting_weapons'] |
            selection_df['exclusion_military_contracting_related'] |
            selection_df['exclusion_small_arms'] |
            selection_df['exclusion_tobacco_production'] |
            selection_df['exclusion_tobacco_retail'] |
            selection_df['exclusion_alcohol_production'] |
            selection_df['exclusion_alcohol_retail'] |
            selection_df['exclusion_gambling_operations'] |
            selection_df['exclusion_gambling_support'] |
            selection_df['exclusion_oil_gas_generation'] |
            selection_df['exclusion_oil_gas_ownership'] |
            selection_df['exclusion_oil_sands_extraction'] |
            selection_df['exclusion_oil_sands_ownership'] |
            selection_df['exclusion_shale_extraction'] |
            selection_df['exclusion_shale_ownership'] |
            selection_df['exclusion_arctic_extraction'] |
            selection_df['exclusion_arctic_ownership'] |
            selection_df['exclusion_thermal_coal_extraction'] |
            selection_df['exclusion_thermal_coal_extraction_ownership'] |
            selection_df['exclusion_thermal_coal_power_generation'] |
            selection_df['exclusion_thermal_coal_power_ownership'] |
            selection_df['exclusion_thermal_coal_supporting'] |
            selection_df['exclusion_animal_testing']
        )
        
        logger.info(f"Exclusion criteria applied. {selection_df['general_exclusion'].sum()} companies excluded.")
        
        # Filter out excluded companies
        eligible_df = selection_df[~selection_df['general_exclusion']].copy()
        logger.info(f"After exclusions: {len(eligible_df)} companies eligible")
        
        # Physical Risk Score Ranking
        logger.info("Applying Physical Risk Score ranking...")
        
        if 'Physical_Risk_Score' in eligible_df.columns and eligible_df['Physical_Risk_Score'].notna().any():
            # Rank by Physical Risk Score (higher is better, less negative is better)
            # Sort descending so highest (least negative) scores come first
            eligible_df = eligible_df.sort_values('Physical_Risk_Score', ascending=False, na_position='last')
            eligible_df['Physical_Risk_Rank'] = range(1, len(eligible_df) + 1)
            
            # Select top 70 by Physical Risk Score
            eligible_df = eligible_df.head(70).copy()
            logger.info(f"After Physical Risk Score ranking: {len(eligible_df)} companies (top 70)")
        else:
            logger.warning("Physical Risk Score not available or all values are NaN. Skipping Physical Risk Score ranking.")
            eligible_df['Physical_Risk_Rank'] = np.nan
        
        # Sector Screening: Exclude specific industries
        logger.info("Applying sector screening...")
        excluded_industries = ['15', '20', '45', '65']
        eligible_df['excluded_sector'] = eligible_df['Industry Code'].isin(excluded_industries)
        
        sector_excluded_count = eligible_df['excluded_sector'].sum()
        logger.info(f"Sector screening: {sector_excluded_count} companies excluded from industries {excluded_industries}")
        
        eligible_df = eligible_df[~eligible_df['excluded_sector']].copy()
        logger.info(f"After sector screening: {len(eligible_df)} companies eligible")
        
        # Step 3: Selection Ranking by FFMC
        logger.info("Ranking by FFMC...")
        eligible_df = eligible_df.sort_values('FFMC', ascending=False)
        eligible_df['Final_Rank'] = range(1, len(eligible_df) + 1)
        
        # Step 4: Select top 30
        logger.info("Selecting top 30 companies...")
        final_selection = eligible_df.head(30).copy()
        logger.info(f"Final selection: {len(final_selection)} companies")
        
        # Apply 10% capping using EOD prices
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
        
        # Prepare EZ3R DataFrame (final 30 companies for index composition)
        EZ3R_df = (
            final_selection[
                ['Company', 'ISIN code', 'MIC', 'Number of Shares', 'Free Float', 'Capping Factor', 
                'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'ISIN code': 'ISIN Code'})
            .sort_values('Company')
        )
        
        # Get index market cap
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]

        # Perform Inclusion/Exclusion Analysis
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
            ez3r_path = os.path.join(output_dir, f'EZ3R_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving EZ3R output to: {ez3r_path}")
            with pd.ExcelWriter(ez3r_path) as writer:
                EZ3R_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                eligible_df.to_excel(writer, sheet_name='Eligible Companies', index=False)
                universe_df.to_excel(writer, sheet_name='Complete Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"ez3r_path": ez3r_path}
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