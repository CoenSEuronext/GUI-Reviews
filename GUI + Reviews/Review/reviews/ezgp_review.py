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

def run_ezgp_review(date, co_date, effective_date, index="EZGP", isin="FRESG0003540", 
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
            ['ff', 'sustainalytics', 'icb', 'eurozone_300']
        )

        # Validate data loading
        if ref_data.get('eurozone_300') is None:
            raise ValueError("Failed to load eurozone_300 universe data")

        # EZGP universe - Step 1
        ezgp_universe = ref_data['eurozone_300']
        ezgp_df = pd.DataFrame(ezgp_universe)
        ff_df = ref_data['ff']
        icb_df = ref_data['icb']

        logger.info(f"Starting universe size: {len(ezgp_df)}")
        
        # Add the required columns to the combined dataframe
        ezgp_df['Effective Date of Review'] = effective_date

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')
        
        # Chain all data preparation operations
        universe_df = (ezgp_df
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
        
        # Extract Industry Code from Subsector Code (first 2 digits)
        universe_df['Industry Code'] = universe_df['Subsector Code'].astype(str).str[:2]
        
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
        
        # Map Industry Code to Industry Name
        universe_df['Industry Name'] = universe_df['Industry Code'].map(industry_code_to_name)
        
        # Step 2.1: Filter by 3-month ADTV (Average Daily Traded Value)
        logger.info("Step 2.1: Filtering by 3-month Average Daily Traded Value...")
        adtv_threshold = 40_000_000  # 40 Million EUR
        
        if '3 months aver. Turnover EUR' in universe_df.columns:
            universe_df['ADTV_EUR'] = pd.to_numeric(universe_df['3 months aver. Turnover EUR'], errors='coerce')
            initial_count = len(universe_df)
            universe_df = universe_df[universe_df['ADTV_EUR'] >= adtv_threshold].copy()
            logger.info(f"After ADTV filter (>= {adtv_threshold:,} EUR): {len(universe_df)} companies (excluded {initial_count - len(universe_df)})")
        else:
            logger.warning("'3 months aver. Turnover EUR' column not found. Skipping ADTV filter.")
        
        # Step 2.3: Filter to Eurozone countries only
        logger.info("Step 2.3: Filtering to Eurozone countries...")
        eurozone_prefixes = ['IT', 'NL', 'BE', 'FR', 'ES', 'DE', 'IE', 'AT', 'LU', 'PT', 'FI']
        universe_df['is_eurozone'] = universe_df['ISIN code'].str[:2].isin(eurozone_prefixes)
        eurozone_df = universe_df[universe_df['is_eurozone']].copy()
        logger.info(f"After Eurozone filter: {len(eurozone_df)} companies")
        
        # Calculate FFMC for later use
        eurozone_df['FFMC'] = eurozone_df['Free Float'] * eurozone_df['Number of Shares'] * eurozone_df["Price"]
        
        # Merge Sustainalytics data
        logger.info("Merging Sustainalytics data...")
        
        sustainalytics_raw = ref_data.get('sustainalytics')

        # Check if sustainalytics data was loaded
        if sustainalytics_raw is None:
            logger.warning("Sustainalytics data not available. Skipping sustainalytics merge.")
            selection_df = eurozone_df
        else:
            # The file has row 0 = headers (text) and row 1 = codes (numbers as text)
            # Define the required codes (these are in row 1 of the Excel file)
            required_codes = [
                '231112111799',  # Global Standards
                '171611102999',  # Controversial Weapons - Tailormade
                '171613102999',  # Controversial Weapons - Non-tailormade
                '172911112999',  # Tobacco - Production
                '172915112999',  # Tobacco - Distribution
                '171711112999',  # Small Arms - Assault weapons
                '171721112999',  # Small Arms - Non-assault weapons
                '171025111199',  # Thermal Coal - Extraction
                '171025291199',  # Thermal Coal - Supporting
                '171114111199',  # Oil & Gas - Production
                '171114171199',  # Oil & Gas - Supporting
                '171025141199',  # Power Generation - Thermal Coal
                '171114141199',  # Power Generation - Oil & Gas
                '171213112999',  # Adult Entertainment - Distribution
                '171311112999',  # Alcoholic Beverages - Production
                '171313112999',  # Alcoholic Beverages - Related
                '171915112999',  # Gambling - Supporting
                '171411102999',  # Animal Testing - Pharmaceutical
                '171415102999',  # Animal Testing - Non-pharmaceutical
                '181268702099',  # GMO - Policy Score
                '171016181199',  # GMO - Revenue
                '171019191999',  # Pesticides
                '191111202799',  # Carbon - Total Emissions Scope 1,2&3
                '134011112599',  # Carbon Impact of Products
                '132211112599',  # Energy Use and GHG Emissions
                '132111112599',  # Emissions, Effluents and Waste
                '132311112599',  # Environmental Impact of Products
                '132811112599',  # Land Use and Biodiversity
                '133811112599',  # Water Use
                '181110112399',  # ESG Risk Score
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
                    selection_df = eurozone_df.merge(
                        sustainalytics_filtered,
                        left_on='ISIN code',
                        right_on='ISIN',
                        how='left'
                    ).drop('ISIN', axis=1, errors='ignore')
                    
                    logger.info(f"Sustainalytics merge completed. Added {len(cols_to_keep)-1} columns.")
                else:
                    logger.warning("No matching sustainalytics columns found.")
                    selection_df = eurozone_df
            else:
                logger.warning("Sustainalytics dataframe is empty")
                selection_df = eurozone_df
        
        # Step 2.2: Carbon Emission Data filter
        logger.info("Step 2.2: Filtering companies without Carbon Emission Data...")
        
        # Helper function to find column by code
        def find_column_by_code(df, code):
            for col in df.columns:
                if col.startswith(f"{code} - "):
                    return col
            return None
        
        col_191111202799 = find_column_by_code(selection_df, '191111202799')
        
        if col_191111202799:
            # Check if column exists and has data
            initial_count = len(selection_df)
            # Exclude companies with missing or null Carbon Emissions data
            selection_df = selection_df[selection_df[col_191111202799].notna()].copy()
            # Also exclude companies with empty string or zero values
            selection_df = selection_df[
                (selection_df[col_191111202799] != '') & 
                (selection_df[col_191111202799] != 0) &
                (selection_df[col_191111202799] != '0')
            ].copy()
            logger.info(f"After Carbon Emissions filter: {len(selection_df)} companies (excluded {initial_count - len(selection_df)})")
        else:
            logger.warning("Carbon Emissions field (191111202799) not found in Sustainalytics data. Cannot apply filter.")
        
        # Apply Exclusion Criteria
        logger.info("Applying exclusion criteria...")
        
        # Helper function to safely convert to numeric
        def safe_numeric(series):
            return pd.to_numeric(series, errors='coerce').fillna(0)
        
        # Step 2.4: Global Standards Screening
        # 231112111799 == 'Non-Compliant'
        col_231112111799 = find_column_by_code(selection_df, '231112111799')
        if col_231112111799:
            selection_df['exclusion_global_standards'] = selection_df[col_231112111799] == 'Non-Compliant'
            if col_231112111799 not in selection_df.columns:
                logger.warning("Global Standards field (231112111799) not found. Exclusion criteria may not be fully applied.")
        else:
            selection_df['exclusion_global_standards'] = False
            logger.warning("Global Standards field (231112111799) not found. Exclusion criteria may not be fully applied.")
        
        # Step 2.5: Product Involvement Screening
        
        # Step 2.5a: Tobacco
        # Production: 172911112999 - Any involvement (>0%)
        # Distribution: 172915112999 - Any involvement (>0%)
        col_172911112999 = find_column_by_code(selection_df, '172911112999')
        col_172915112999 = find_column_by_code(selection_df, '172915112999')
        
        exclusion_tobacco = False
        if col_172911112999:
            exclusion_tobacco |= safe_numeric(selection_df[col_172911112999]) > 0
        else:
            logger.warning("Tobacco Production field (172911112999) not found.")
        if col_172915112999:
            exclusion_tobacco |= safe_numeric(selection_df[col_172915112999]) > 0
        else:
            logger.warning("Tobacco Distribution field (172915112999) not found.")
        selection_df['exclusion_tobacco'] = exclusion_tobacco
        
        # Step 2.5b: Small Arms Civilian
        # Assault weapons: 171711112999 - Any involvement
        # Non-assault weapons: 171721112999 - Any involvement
        col_171711112999 = find_column_by_code(selection_df, '171711112999')
        col_171721112999 = find_column_by_code(selection_df, '171721112999')
        
        exclusion_small_arms = False
        if col_171711112999:
            exclusion_small_arms |= safe_numeric(selection_df[col_171711112999]) > 0
        else:
            logger.warning("Small Arms Assault field (171711112999) not found.")
        if col_171721112999:
            exclusion_small_arms |= safe_numeric(selection_df[col_171721112999]) > 0
        else:
            logger.warning("Small Arms Non-assault field (171721112999) not found.")
        selection_df['exclusion_small_arms'] = exclusion_small_arms
        
        # Step 2.5c: Controversial Weapons
        # Tailormade: 171611102999 - Any involvement
        # Non-tailormade: 171613102999 - Any involvement
        col_171611102999 = find_column_by_code(selection_df, '171611102999')
        col_171613102999 = find_column_by_code(selection_df, '171613102999')
        
        exclusion_controversial_weapons = False
        if col_171611102999:
            exclusion_controversial_weapons |= selection_df[col_171611102999] == 'CW1'
        else:
            logger.warning("Controversial Weapons Tailormade field (171611102999) not found.")
        if col_171613102999:
            exclusion_controversial_weapons |= selection_df[col_171613102999] == 'CW3'
        else:
            logger.warning("Controversial Weapons Non-tailormade field (171613102999) not found.")
        selection_df['exclusion_controversial_weapons'] = exclusion_controversial_weapons
        
        # Step 2.5d: Thermal Coal
        # Extraction: 171025111199 + Supporting: 171025291199 - Sum > 0%
        col_171025111199 = find_column_by_code(selection_df, '171025111199')
        col_171025291199 = find_column_by_code(selection_df, '171025291199')
        
        sum_thermal_coal = 0
        if col_171025111199:
            sum_thermal_coal += safe_numeric(selection_df[col_171025111199])
        else:
            logger.warning("Thermal Coal Extraction field (171025111199) not found.")
        if col_171025291199:
            sum_thermal_coal += safe_numeric(selection_df[col_171025291199])
        else:
            logger.warning("Thermal Coal Supporting field (171025291199) not found.")
        selection_df['exclusion_thermal_coal'] = sum_thermal_coal > 0
        
        # Step 2.5e: Oil & Gas Exploration/Processing
        # Production: 171114111199 + Supporting: 171114171199 - Sum > 5%
        col_171114111199 = find_column_by_code(selection_df, '171114111199')
        col_171114171199 = find_column_by_code(selection_df, '171114171199')
        
        sum_oil_gas = 0
        if col_171114111199:
            sum_oil_gas += safe_numeric(selection_df[col_171114111199])
        else:
            logger.warning("Oil & Gas Production field (171114111199) not found.")
        if col_171114171199:
            sum_oil_gas += safe_numeric(selection_df[col_171114171199])
        else:
            logger.warning("Oil & Gas Supporting field (171114171199) not found.")
        selection_df['exclusion_oil_gas'] = sum_oil_gas > 5
        
        # Step 2.5f: Power Generation
        # Thermal Coal: 171025141199 + Oil & Gas: 171114141199 - Sum > 5%
        col_171025141199 = find_column_by_code(selection_df, '171025141199')
        col_171114141199 = find_column_by_code(selection_df, '171114141199')
        
        sum_power_generation = 0
        if col_171025141199:
            sum_power_generation += safe_numeric(selection_df[col_171025141199])
        else:
            logger.warning("Power Generation Thermal Coal field (171025141199) not found.")
        if col_171114141199:
            sum_power_generation += safe_numeric(selection_df[col_171114141199])
        else:
            logger.warning("Power Generation Oil & Gas field (171114141199) not found.")
        selection_df['exclusion_power_generation'] = sum_power_generation > 5
        
        # Step 2.5g: Involvement
        # Adult Entertainment Distribution: 171213112999 - Any involvement
        # Alcoholic Beverages Production: 171311112999 - Any involvement
        # Alcoholic Beverages Related: 171313112999 - Any involvement
        # Gambling Supporting: 171915112999 - Any involvement
        col_171213112999 = find_column_by_code(selection_df, '171213112999')
        col_171311112999 = find_column_by_code(selection_df, '171311112999')
        col_171313112999 = find_column_by_code(selection_df, '171313112999')
        col_171915112999 = find_column_by_code(selection_df, '171915112999')
        
        exclusion_involvement = False
        if col_171213112999:
            exclusion_involvement |= safe_numeric(selection_df[col_171213112999]) > 0
        else:
            logger.warning("Adult Entertainment field (171213112999) not found.")
        if col_171311112999:
            exclusion_involvement |= safe_numeric(selection_df[col_171311112999]) > 0
        else:
            logger.warning("Alcoholic Beverages Production field (171311112999) not found.")
        if col_171313112999:
            exclusion_involvement |= safe_numeric(selection_df[col_171313112999]) > 0
        else:
            logger.warning("Alcoholic Beverages Related field (171313112999) not found.")
        if col_171915112999:
            exclusion_involvement |= safe_numeric(selection_df[col_171915112999]) > 0
        else:
            logger.warning("Gambling Supporting field (171915112999) not found.")
        selection_df['exclusion_involvement'] = exclusion_involvement
        
        # Step 2.5h: Animal Testing
        # Pharmaceutical: 171411102999 - Flagged "AT1"
        # Non-pharmaceutical: 171415102999 - Flagged "AT4"
        col_171411102999 = find_column_by_code(selection_df, '171411102999')
        col_171415102999 = find_column_by_code(selection_df, '171415102999')
        
        exclusion_animal_testing = False
        if col_171411102999:
            exclusion_animal_testing |= selection_df[col_171411102999] == 'AT1'
        else:
            logger.warning("Animal Testing Pharmaceutical field (171411102999) not found.")
        if col_171415102999:
            exclusion_animal_testing |= selection_df[col_171415102999] == 'AT4'
        else:
            logger.warning("Animal Testing Non-pharmaceutical field (171415102999) not found.")
        selection_df['exclusion_animal_testing'] = exclusion_animal_testing
        
        # Step 2.5i: GMO
        # Policy Score: 181268702099 - Score < 100
        # Revenue: 171016181199 - Derived revenue > 0%
        col_181268702099 = find_column_by_code(selection_df, '181268702099')
        col_171016181199 = find_column_by_code(selection_df, '171016181199')
        
        exclusion_gmo = False
        if col_181268702099:
            exclusion_gmo |= safe_numeric(selection_df[col_181268702099]) > 1
        else:
            logger.warning("GMO Policy Score field (181268702099) not found.")
        if col_171016181199:
            exclusion_gmo |= safe_numeric(selection_df[col_171016181199]) > 0
        else:
            logger.warning("GMO Revenue field (171016181199) not found.")
        selection_df['exclusion_gmo'] = exclusion_gmo
        
        # Step 2.5j: Pesticides
        # Code: 171019191999 - Revenue > 0%
        col_171019191999 = find_column_by_code(selection_df, '171019191999')
        
        if col_171019191999:
            # Exclude if revenue > 0%
            selection_df['exclusion_pesticides'] = safe_numeric(selection_df[col_171019191999]) > 0
        else:
            logger.warning("Pesticides field (171019191999) not found.")
            selection_df['exclusion_pesticides'] = False
        
        # Step 2.5k: Environmental Objectives
        # All fields check for Categories 4 and 5
        environmental_codes = [
            '134011112599',  # Carbon Impact
            '132211112599',  # Energy Use & GHG
            '132111112599',  # Emissions/Waste
            '132311112599',  # Environmental Impact
            '132811112599',  # Land Use/Biodiversity
            '133811112599',  # Water Use
        ]
        
        exclusion_environmental = False
        for code in environmental_codes:
            col = find_column_by_code(selection_df, code)
            if col:
                exclusion_environmental |= selection_df[col].isin([4, 5])
            else:
                logger.warning(f"Environmental field ({code}) not found.")
        selection_df['exclusion_environmental'] = exclusion_environmental
        
        # General Exclusion: Any exclusion criteria met
        selection_df['general_exclusion'] = (
            selection_df['exclusion_global_standards'] |
            selection_df['exclusion_tobacco'] |
            selection_df['exclusion_small_arms'] |
            selection_df['exclusion_controversial_weapons'] |
            selection_df['exclusion_thermal_coal'] |
            selection_df['exclusion_oil_gas'] |
            selection_df['exclusion_power_generation'] |
            selection_df['exclusion_involvement'] |
            selection_df['exclusion_animal_testing'] |
            selection_df['exclusion_gmo'] |
            selection_df['exclusion_pesticides'] |
            selection_df['exclusion_environmental']
        )
        
        logger.info(f"Exclusion criteria applied. {selection_df['general_exclusion'].sum()} companies excluded.")
        
        # Filter out excluded companies
        eligible_df = selection_df[~selection_df['general_exclusion']].copy()
        logger.info(f"After exclusions: {len(eligible_df)} companies eligible")
        
        # Step 2.6: ESG Risk Score Ranking
        logger.info("Step 2.6: Ranking by ESG Risk Score (lower is better)...")
        
        col_181110112399 = find_column_by_code(eligible_df, '181110112399')
        
        if col_181110112399 and eligible_df[col_181110112399].notna().any():
            # Convert to numeric
            eligible_df['ESG_Risk_Score'] = safe_numeric(eligible_df[col_181110112399])
            
            # Rank by ESG Risk Score (ascending = lower score is better)
            # Ties broken by highest FFMC
            eligible_df = eligible_df.sort_values(['ESG_Risk_Score', 'FFMC'], ascending=[True, False])
            eligible_df['ESG_Risk_Rank'] = range(1, len(eligible_df) + 1)
            
            logger.info(f"ESG Risk Score ranking completed for {len(eligible_df)} companies")
        else:
            logger.warning("ESG Risk Score (181110112399) not available. Skipping ESG ranking.")
            eligible_df['ESG_Risk_Score'] = np.nan
            eligible_df['ESG_Risk_Rank'] = np.nan
        
        # Step 2.7: Pre-selection of top 40 by ESG Risk Score
        logger.info("Step 2.7: Selecting top 40 companies with lowest ESG Risk Score...")
        preselection_df = eligible_df.head(40).copy()
        logger.info(f"Pre-selection: {len(preselection_df)} companies")
        
        # Step 3: Selection Ranking by FFMC
        logger.info("Step 3: Ranking pre-selected companies by FFMC...")
        preselection_df = preselection_df.sort_values('FFMC', ascending=False)
        preselection_df['FFMC_Rank'] = range(1, len(preselection_df) + 1)
        
        # Step 4: Selection of top 20 constituents with max 5 per Industry Code
        logger.info("Step 4: Selecting top 20 companies with max 5 per Industry Code...")

        final_selection = []
        industry_counts = {}
        max_per_industry = 5

        for idx, row in preselection_df.iterrows():
            industry = row['Industry Code']
            current_count = industry_counts.get(industry, 0)
            
            if current_count < max_per_industry:
                final_selection.append(idx)  # Append index instead of row
                industry_counts[industry] = current_count + 1
            
            if len(final_selection) >= 20:
                break

        # Use .loc to select rows by index, preserving all columns
        final_selection_df = preselection_df.loc[final_selection].copy()
        logger.info(f"Final selection: {len(final_selection_df)} companies")
        logger.info(f"Industry distribution: {dict(industry_counts)}")

        # Check if we have any companies selected
        if len(final_selection_df) == 0:
            raise ValueError("No companies selected in final selection step")

        # Apply 10% capping using EOD prices
        logger.info("Applying 10% capping using most recent pricing data...")

        # Debug: Check for required columns
        required_cols = ['Free Float', 'Number of Shares', 'Close Prc_EOD', 'FX/Index Ccy']
        missing_cols = [col for col in required_cols if col not in final_selection_df.columns]
        if missing_cols:
            logger.error(f"Missing columns for FFMC calculation: {missing_cols}")
            logger.error(f"Available columns: {final_selection_df.columns.tolist()}")
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Debug: Check for NaN values in required columns
        for col in required_cols:
            nan_count = final_selection_df[col].isna().sum()
            if nan_count > 0:
                logger.warning(f"Column '{col}' has {nan_count} NaN values out of {len(final_selection_df)}")
                logger.warning(f"Companies with NaN in '{col}': {final_selection_df[final_selection_df[col].isna()]['Company'].tolist()}")

        # Recalculate market cap using EOD prices for capping
        final_selection_df['EOD_FFMC'] = (
            final_selection_df['Free Float'] * 
            final_selection_df['Number of Shares'] * 
            final_selection_df['Close Prc_EOD'] * 
            final_selection_df['FX/Index Ccy']
        )

        # Check if FFMC calculation resulted in valid values
        valid_ffmc_count = final_selection_df['EOD_FFMC'].notna().sum()
        logger.info(f"Valid EOD_FFMC values: {valid_ffmc_count} out of {len(final_selection_df)}")

        if valid_ffmc_count == 0:
            logger.error("All EOD_FFMC values are NaN. Cannot proceed with capping.")
            logger.error(f"Sample data:\n{final_selection_df[required_cols + ['Company']].head()}")
            raise ValueError("All market cap values are NaN after calculation")

        # Apply proportional capping with 10% max weight
        final_selection_df = apply_proportional_capping(
            final_selection_df,
            mcap_column='EOD_FFMC',
            max_weight=0.10,  # 10% max weight
            max_iterations=100
        )
        
        # Prepare EZGP DataFrame (final 20 companies for index composition)
        EZGP_df = (
            final_selection_df[
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
            ezgp_path = os.path.join(output_dir, f'EZGP_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving EZGP output to: {ezgp_path}")
            with pd.ExcelWriter(ezgp_path) as writer:
                EZGP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                preselection_df.to_excel(writer, sheet_name='Pre-selection (Top 40)', index=False)
                final_selection_df.to_excel(writer, sheet_name='Final Selection (Top 20)', index=False)
                universe_df.to_excel(writer, sheet_name='Complete Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"ezgp_path": ezgp_path}
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