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

def run_uc3pe_review(date, co_date, effective_date, index="UC3PE", isin="FR0014005IJ7", 
                    area="EU", area2="US", type="STOCK", universe="north_america_500", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the UC3PE index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        co_date (str): Close-out date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "UC3PE"
        isin (str, optional): ISIN code. Defaults to "FR0014005IK5"
        area (str, optional): Primary area. Defaults to "EU"
        area2 (str, optional): Secondary area. Defaults to ""
        type (str, optional): Type of instrument. Defaults to "STOCK"
        universe (str, optional): Universe name. Defaults to "eurozone_300"
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
        ref_data = load_reference_data(current_data_folder, ['ff', 'north_america_500', 'oekom_trustcarbon'])
        
        # Extract the needed DataFrames
        ff_df = ref_data['ff']
        north_america_500_df = ref_data['north_america_500']
        Oekom_TrustCarbon_df = ref_data['oekom_trustcarbon']
        
        if any(df is None for df in [ff_df, north_america_500_df, Oekom_TrustCarbon_df]):
            raise ValueError("Failed to load one or more required reference data files")

        ff_df = ff_df.drop_duplicates(subset=['ISIN Code:'], keep='first')
        
        # Add Free Float data
        north_america_500_df = north_america_500_df.merge(
            ff_df[['ISIN Code:', 'Free Float Round:']],
            left_on='ISIN',
            right_on='ISIN Code:',
            how='left'
        ).drop('ISIN Code:', axis=1).rename(columns={'Free Float Round:': 'Free Float',
                                                     'Name': 'Company'})

        # Filter symbols once (Reuters symbols only, length < 12)
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')
        
        # Add #Symbol column
        north_america_500_df = north_america_500_df.merge(
            symbols_filtered,
            left_on='ISIN',
            right_on='Isin Code',
            how='left'
        ).drop('Isin Code', axis=1)

        # Add FX/Index Ccy data early based on currency parameter
        north_america_500_df = north_america_500_df.merge(
            stock_eod_df[stock_eod_df['Index Curr'] == currency][['#Symbol', 'FX/Index Ccy']].drop_duplicates(subset='#Symbol', keep='first'),
            on='#Symbol',
            how='left'
        )

        # Remove companies with MIC = 'XTSE' from the universe entirely
        initial_count = len(north_america_500_df)
        north_america_500_df = north_america_500_df[north_america_500_df['MIC'] != 'XTSE'].copy()
        removed_xtse_count = initial_count - len(north_america_500_df)
        logger.info(f"Removed {removed_xtse_count} companies with MIC = 'XTSE' from universe")
        logger.info(f"Remaining universe size: {len(north_america_500_df)} companies")

        # ===================================================================
        # EARLY MERGE: Merge ALL Oekom data points at the beginning
        # ===================================================================
        logger.info("Merging Oekom data points early...")
        
        # Convert numeric columns in Oekom data BEFORE merging
        Oekom_TrustCarbon_df['ClimateCuAlignIEANZTgt2050-values'] = pd.to_numeric(
            Oekom_TrustCarbon_df['ClimateCuAlignIEANZTgt2050-values'], 
            errors='coerce'
        )
        
        Oekom_TrustCarbon_df['ESG Performance Score'] = pd.to_numeric(
            Oekom_TrustCarbon_df['ESG Performance Score'],
            errors='coerce'
        )
        
        # Define all Oekom columns to merge
        oekom_columns_to_merge = [
            'ISIN',
            'ClimateCuAlignIEANZTgt2050-values',
            'ESG Performance Score',
            'Reported Emissions - Emissions Trust Metric',
            'NBR Overall Flag',
            'Anti-personnel Mines - Overall Flag',
            'Biological Weapons - Overall Flag',
            'Chemical Weapons - Overall Flag',
            'Cluster Munitions - Overall Flag',
            'Depleted Uranium - Overall Flag',
            'Incendiary Weapons - Overall Flag',
            'Nuclear Weapons Outside NPT - Overall Flag',
            'White Phosphorous Weapons - Overall Flag',
            'Thermal Coal Mining - Maximum Percentage of Revenues (%)',
            'Power Generation - Thermal Maximum Percentage of Revenues (%)',
            'Shale Oil and/or Gas - Involvement tie'
        ]
        
        # Merge all Oekom data early
        north_america_500_df = north_america_500_df.merge(
            Oekom_TrustCarbon_df[oekom_columns_to_merge],
            on='ISIN',
            how='left'
        )
        
        # NOW create numeric helper columns in north_america_500_df AFTER merge
        north_america_500_df['Trust Metric Numeric'] = pd.to_numeric(
            north_america_500_df['Reported Emissions - Emissions Trust Metric'], 
            errors='coerce'
        )
        
        north_america_500_df['Coal Mining Numeric'] = pd.to_numeric(
            north_america_500_df['Thermal Coal Mining - Maximum Percentage of Revenues (%)'], 
            errors='coerce'
        )
        
        north_america_500_df['Thermal Power Numeric'] = pd.to_numeric(
            north_america_500_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'], 
            errors='coerce'
        )
        
        # Calculate Carbon Budget Rank EARLY for ALL companies (LOWER score is BETTER)
        temp_rank = north_america_500_df['ClimateCuAlignIEANZTgt2050-values'].rank(
            method='min',
            ascending=True,
            na_option='keep'
        ).fillna(10000)
        
        north_america_500_df['Carbon Budget Rank'] = temp_rank.rank(
            method='min',
            ascending=True
        )
        
        logger.info("Oekom data merged successfully")

        # ===================================================================
        # START EXCLUSION CRITERIA
        # ===================================================================
        exclusion_count = 1
        
        # Step 2a: Trust Metric screening
        north_america_500_df[f'exclusion_{exclusion_count}_TrustMetric'] = None

        # Get ISINs with Trust Metric < 0.6 or 'Not Collected'
        excluded_trust_metric = north_america_500_df[
            (north_america_500_df['Trust Metric Numeric'] < 0.6) |
            (north_america_500_df['Reported Emissions - Emissions Trust Metric'] == 'Not Collected')
        ]['ISIN'].tolist()

        north_america_500_df[f'exclusion_{exclusion_count}_TrustMetric'] = np.where(
            north_america_500_df['ISIN'].isin(excluded_trust_metric),
            'exclude_TrustMetric',
            None
        )
        logger.info(f"Trust Metric exclusions: {len(excluded_trust_metric)}")
        exclusion_count += 1

        # Step 2b: NBR Overall Flag exclusion
        north_america_500_df[f'exclusion_{exclusion_count}_NBROverallFlag'] = None
        NBR_Overall_Flag_Red = north_america_500_df[
            (north_america_500_df['NBR Overall Flag'] == 'RED') |
            (north_america_500_df['NBR Overall Flag'] == 'Not Collected')
        ]['ISIN'].tolist()

        north_america_500_df[f'exclusion_{exclusion_count}_NBROverallFlag'] = np.where(
            north_america_500_df['ISIN'].isin(NBR_Overall_Flag_Red),
            'exclude_NBROverallFlag',
            None
        )
        logger.info(f"NBR Overall Flag exclusions: {len(NBR_Overall_Flag_Red)}")
        exclusion_count += 1

        # Step 2c: Controversial Weapons screening
        weapons_columns = {
            'Anti-personnel Mines - Overall Flag': ('exclude_APMines', 'APMines'),
            'Biological Weapons - Overall Flag': ('exclude_BiologicalWeapons', 'BiologicalWeapons'),
            'Chemical Weapons - Overall Flag': ('exclude_ChemicalWeapons', 'ChemicalWeapons'),
            'Cluster Munitions - Overall Flag': ('exclude_ClusterMunitions', 'ClusterMunitions'),
            'Depleted Uranium - Overall Flag': ('exclude_DepletedUranium', 'DepletedUranium'),
            'Incendiary Weapons - Overall Flag': ('exclude_IncendiaryWeapons', 'IncendiaryWeapons'),
            'Nuclear Weapons Outside NPT - Overall Flag': ('exclude_NuclearWeaponsNonNPT', 'NuclearWeaponsNonNPT'),
            'White Phosphorous Weapons - Overall Flag': ('exclude_WhitePhosphorus', 'WhitePhosphorus')
        }

        for column, (exclude_value, label) in weapons_columns.items():
            north_america_500_df[f'exclusion_{exclusion_count}_{label}'] = None
            
            # Get ISINs for this weapon type (Red or 'Not Collected')
            flagged_isins = north_america_500_df[
                (north_america_500_df[column] == 'RED') |
                (north_america_500_df[column] == 'Not Collected')
            ]['ISIN'].tolist()
            
            north_america_500_df[f'exclusion_{exclusion_count}_{label}'] = np.where(
                north_america_500_df['ISIN'].isin(flagged_isins),
                exclude_value,
                None
            )
            logger.info(f"{label} exclusions: {len(flagged_isins)}")
            exclusion_count += 1

        # Step 2d: Thermal Coal Mining screening
        north_america_500_df[f'exclusion_{exclusion_count}_CoalMining'] = None

        excluded_coal_mining = north_america_500_df[
            (north_america_500_df['Coal Mining Numeric'] > 0) |
            (north_america_500_df['Thermal Coal Mining - Maximum Percentage of Revenues (%)'] == 'Not Collected') |
            (north_america_500_df['Thermal Coal Mining - Maximum Percentage of Revenues (%)'] == 'Not Disclosed')
        ]['ISIN'].tolist()

        north_america_500_df[f'exclusion_{exclusion_count}_CoalMining'] = np.where(
            north_america_500_df['ISIN'].isin(excluded_coal_mining),
            'exclude_CoalMining',
            None
        )
        logger.info(f"Coal Mining exclusions: {len(excluded_coal_mining)}")
        exclusion_count += 1

        # Step 2e: Thermal Coal Power Generation screening
        north_america_500_df[f'exclusion_{exclusion_count}_ThermalPower'] = None

        excluded_thermal_power = north_america_500_df[
            (north_america_500_df['Thermal Power Numeric'] > 0.1) |
            (north_america_500_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'] == 'Not Collected') |
            (north_america_500_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'] == 'Not Disclosed')
        ]['ISIN'].tolist()

        north_america_500_df[f'exclusion_{exclusion_count}_ThermalPower'] = np.where(
            north_america_500_df['ISIN'].isin(excluded_thermal_power),
            'exclude_ThermalPower',
            None
        )
        logger.info(f"Thermal Power exclusions: {len(excluded_thermal_power)}")
        exclusion_count += 1

        # Step 2f: Shale Oil and/or Gas screening
        north_america_500_df[f'exclusion_{exclusion_count}_ShaleOilGas'] = None

        excluded_shale = north_america_500_df[
            (north_america_500_df['Shale Oil and/or Gas - Involvement tie'] == 'Production') |
            (north_america_500_df['Shale Oil and/or Gas - Involvement tie'] == 'Not Collected') |
            (north_america_500_df['Shale Oil and/or Gas - Involvement tie'] == 'Not Disclosed')
        ]['ISIN'].tolist()

        north_america_500_df[f'exclusion_{exclusion_count}_ShaleOilGas'] = np.where(
            north_america_500_df['ISIN'].isin(excluded_shale),
            'exclude_ShaleOilGas',
            None
        )
        logger.info(f"Shale Oil/Gas exclusions: {len(excluded_shale)}")
        exclusion_count += 1

        # Step 2g: Carbon Budget screening (exclude worst 20% by ranking)
        # Now using the Carbon Budget Rank that was calculated early
        north_america_500_df[f'exclusion_{exclusion_count}_CarbonBudget'] = None

        # Calculate the cutoff rank for worst 20%
        total_companies = len(north_america_500_df)
        cutoff_rank = total_companies * 0.8

        logger.info(f"Total companies in universe: {total_companies}")
        logger.info(f"Carbon Budget cutoff rank (80th percentile): {cutoff_rank}")

        # Exclude companies with ranks > cutoff (worst 20%)
        north_america_500_df[f'exclusion_{exclusion_count}_CarbonBudget'] = np.where(
            north_america_500_df['Carbon Budget Rank'] > cutoff_rank,
            'exclude_CarbonBudget',
            None
        )

        excluded_count = north_america_500_df[f'exclusion_{exclusion_count}_CarbonBudget'].notna().sum()
        logger.info(f"Carbon Budget exclusions (rank > {cutoff_rank}): {excluded_count}")
        exclusion_count += 1

        # ===================================================================
        # CREATE EXCLUSION SUMMARY
        # ===================================================================
        # Create list of all exclusion columns
        exclusion_columns = [col for col in north_america_500_df.columns if col.startswith('exclusion_')]

        # Create the exclusion summary columns
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

        north_america_500_df['Exclusion Summary'] = north_america_500_df.apply(
            lambda row: summarize_exclusions(row, exclusion_columns), axis=1
        )

        north_america_500_df['Excluded'] = north_america_500_df['Exclusion Summary'].apply(
            lambda x: 'No' if x == 'Included' else 'Yes'
        )
        
        # ===================================================================
        # SELECTION PROCESS
        # ===================================================================
        # Step 2h: Select companies with no exclusions
        selection_df = north_america_500_df[
            north_america_500_df[exclusion_columns].isna().all(axis=1)
        ].copy()
        
        logger.info(f"Companies remaining after exclusions: {len(selection_df)}")
        
        # Add ESG Performance Score Ranking ONLY for non-excluded companies (higher is better)
        # Only rank companies with actual ESG scores (not NaN/'Not Collected')
        # Rank descending: highest score gets rank 1 (best)
        selection_df['ESG Performance Rank'] = selection_df['ESG Performance Score'].rank(
            method='min',
            ascending=False,
            na_option='keep'
        )
        
        # Merge ESG Performance Rank back to full universe for visibility
        north_america_500_df = north_america_500_df.merge(
            selection_df[['ISIN', 'ESG Performance Rank']],
            on='ISIN',
            how='left'
        )
        
        # Sort by ESG Performance Score (descending, higher is better)
        selection_df = selection_df.sort_values(
            'ESG Performance Score',
            ascending=False,
            na_position='last'
        )
        
        # Select top 250 eligible companies
        top_250_df = selection_df.head(250).copy()
        logger.info(f"Selected top 250 companies by ESG Performance Score")
        
        # ===================================================================
        # PRICE AND FX DATA
        # ===================================================================
        # Step 3: Get price and FX data
        stock_eod_df['Reuters/Optiq'] = stock_eod_df['#Symbol'].str.len().apply(
            lambda x: 'Reuters' if x < 12 else 'Optiq'
        )

        def get_stock_info(row, stock_df):
            mask = (stock_df['Isin Code'] == row['ISIN']) & \
                   (stock_df['MIC'] == row['MIC']) & \
                   (stock_df['Reuters/Optiq'] == 'Reuters')
            
            matches = stock_df[mask]
            
            if not matches.empty:
                first_match = matches.iloc[0]
                
                return pd.Series({
                    'Symbol': first_match['#Symbol'],
                    'Close Prc_EOD': first_match['Close Prc']
                })
            return pd.Series({'Symbol': None, 'Close Prc_EOD': None})

        # Add Symbol and Price columns
        top_250_df[['Symbol', 'Close Prc_EOD']] = top_250_df.apply(
            lambda row: get_stock_info(row, stock_eod_df), axis=1
        )
        
        # FX/Index Ccy is already in the dataframe from the early merge

        # Calculate FFMC (Free Float Market Cap)
        top_250_df['Price in Index Currency'] = top_250_df['Close Prc_EOD'] * top_250_df['FX/Index Ccy']
        top_250_df['FFMC'] = top_250_df['Price (EUR) '] * top_250_df['NOSH'] * top_250_df['Free Float']

        # Add FFMC Ranking (higher FFMC = better rank)
        top_250_df['FFMC Rank'] = top_250_df['FFMC'].rank(
            method='min',
            ascending=False,
            na_option='bottom'
        )
        
        # Rank by FFMC and select top 25
        top_250_df = top_250_df.sort_values('FFMC', ascending=False)
        final_selection_df = top_250_df.head(35).copy()
        logger.info(f"Selected top 25 companies by FFMC")

        # ===================================================================
        # EQUAL WEIGHTING CALCULATION
        # ===================================================================
        # Step 4: Equal weighting calculation
        logger.info(f"Checking IsinCode {isin} in index_eod_df")
        matching_rows = index_eod_df[index_eod_df['IsinCode'] == isin]
        logger.info(f"Found {len(matching_rows)} matching rows")
        if len(matching_rows) > 0:
            index_mcap = matching_rows['Mkt Cap'].iloc[0]
        else:
            logger.error(f"No matching index found for ISIN {isin}")
            raise ValueError(f"No matching index found for ISIN {isin}")

        # Calculate the target market cap per company (equal weighting across all 35 companies)
        top_n = 35
        target_mcap_per_company = index_mcap / top_n
        
        final_selection_df['Unrounded NOSH'] = target_mcap_per_company / (
            final_selection_df['Close Prc_EOD'] * final_selection_df['FX/Index Ccy']
        )
        final_selection_df['Rounded NOSH'] = final_selection_df['Unrounded NOSH'].round()
        final_selection_df['Free Float'] = 1
        final_selection_df['Capping Factor'] = 1.0
        # Set effective date
        final_selection_df['Effective Date of Review'] = effective_date

        # ===================================================================
        # CREATE FINAL OUTPUT
        # ===================================================================
        # Create final output DataFrame for Index Composition sheet (minimal columns)
        UC3PE_df = final_selection_df[[
            'Company', 
            'ISIN', 
            'MIC', 
            'Rounded NOSH',
            'Free Float',
            'Capping Factor',
            'Effective Date of Review',
            'Currency (Local)'
        ]].copy()

        # Rename columns and sort
        UC3PE_df = UC3PE_df.rename(columns={
            'ISIN': 'ISIN Code',
            'Rounded NOSH': 'Number of Shares',
            'Currency (Local)': 'Currency'
        })
        UC3PE_df = UC3PE_df.sort_values('Company')
        
        # Inclusion/Exclusion analysis
        analysis_results = inclusion_exclusion_analysis(
            UC3PE_df, 
            stock_eod_df, 
            index, 
            isin_column='ISIN Code'
        )

        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']
        
        # ===================================================================
        # SAVE OUTPUT FILES
        # ===================================================================
        # Save output files
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            # Create filename with timestamp to avoid conflicts
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            uc3pe_path = os.path.join(output_dir, f'UC3PE_df_{timestamp}.xlsx')
            
            # Save with timestamp to avoid any conflicts
            logger.info(f"Saving UC3PE output to: {uc3pe_path}")
            with pd.ExcelWriter(uc3pe_path) as writer:
                # Write each DataFrame to a different sheet
                UC3PE_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                north_america_500_df.to_excel(writer, sheet_name='Full Universe', index=False)
                top_250_df.to_excel(writer, sheet_name='Top 250', index=False)
                final_selection_df.to_excel(writer, sheet_name='Final Selection', index=False)
                
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "uc3pe_path": uc3pe_path
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