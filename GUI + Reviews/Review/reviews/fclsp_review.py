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

def run_fclsp_review(date, co_date, effective_date, index="FCLSP", isin="FRESG0001478", 
                    area="EU", area2="US", type="STOCK", universe="sbf_120", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the FCLSP index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        co_date (str): Close-out date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "FCLSP"
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
        ref_data = load_reference_data(current_data_folder, ['ff', 'sbf_120', 'oekom_trustcarbon', 'master_report'])
        
        # Extract the needed DataFrames
        ff_df = ref_data['ff']
        sbf_120_df = ref_data['sbf_120']
        Oekom_TrustCarbon_df = ref_data['oekom_trustcarbon']
        master_report_df = ref_data['master_report']
        
        # Check if any data is None BEFORE trying to use it
        if any(df is None for df in [ff_df, sbf_120_df, Oekom_TrustCarbon_df, master_report_df]):
            missing = []
            if ff_df is None:
                missing.append('ff')
            if sbf_120_df is None:
                missing.append('sbf_120')
            if Oekom_TrustCarbon_df is None:
                missing.append('oekom_trustcarbon')
            if master_report_df is None:
                missing.append('master_report')
            raise ValueError(f"Failed to load reference data files: {', '.join(missing)}")
        
        # Now safe to rename
        sbf_120_df = sbf_120_df.rename(columns={'ISIN code': 'ISIN'})
        sbf_120_df = sbf_120_df.rename(columns={'Number of shares': 'NOSH'})
        sbf_120_df = sbf_120_df.rename(columns={'Free Float': 'Free Float SBF'})

        ff_df = ff_df.drop_duplicates(subset=['ISIN Code:'], keep='first')
        
        # Filter symbols once (Reuters symbols only, length < 12)
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')
        
        # Chain all data preparation operations
        sbf_120_df = (sbf_120_df
            # Merge symbols
            .merge(
                symbols_filtered,
                left_on='ISIN',
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

        # Remove companies with MIC = 'XTSE' from the universe entirely
        initial_count = len(sbf_120_df)
        sbf_120_df = sbf_120_df[sbf_120_df['MIC'] != 'XTSE'].copy()
        removed_xtse_count = initial_count - len(sbf_120_df)
        logger.info(f"Removed {removed_xtse_count} companies with MIC = 'XTSE' from universe")
        logger.info(f"Remaining universe size: {len(sbf_120_df)} companies")

        # Merge Price from Master Report
        logger.info("Merging Price from Master Report...")
        sbf_120_df = sbf_120_df.merge(
            master_report_df[['ISIN', 'MIC of MoR', 'Last price']],
            left_on=['ISIN', 'MIC'],
            right_on=['ISIN', 'MIC of MoR'],
            how='left'
        ).drop('MIC of MoR', axis=1).rename(columns={'Last price': 'Price MR'})

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
            Oekom_TrustCarbon_df['ESG Performance Score'].replace('Not Collected', np.nan),
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
            'Shale Oil and/or Gas - Involvement tie',
            'Oil Sands - Production Maximum Percentage of Revenues (%)',
            'Tobacco - Production Maximum Percentage of Revenues (%)'
        ]
        
        # Merge all Oekom data early
        sbf_120_df = sbf_120_df.merge(
            Oekom_TrustCarbon_df[oekom_columns_to_merge],
            on='ISIN',
            how='left'
        )
        
        # NOW create numeric helper columns in sbf_120_df AFTER merge
        sbf_120_df['Trust Metric Numeric'] = pd.to_numeric(
            sbf_120_df['Reported Emissions - Emissions Trust Metric'], 
            errors='coerce'
        )
        
        sbf_120_df['Coal Mining Numeric'] = pd.to_numeric(
            sbf_120_df['Thermal Coal Mining - Maximum Percentage of Revenues (%)'], 
            errors='coerce'
        )
        
        sbf_120_df['Thermal Power Numeric'] = pd.to_numeric(
            sbf_120_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'], 
            errors='coerce'
        )
        
        sbf_120_df['Oil Sands Numeric'] = pd.to_numeric(
            sbf_120_df['Oil Sands - Production Maximum Percentage of Revenues (%)'],
            errors='coerce'
        )
        
        sbf_120_df['Tobacco Numeric'] = pd.to_numeric(
            sbf_120_df['Tobacco - Production Maximum Percentage of Revenues (%)'],
            errors='coerce'
        )
        
        # Calculate Carbon Budget Rank EARLY for ALL companies (LOWER score is BETTER)
        temp_rank = sbf_120_df['ClimateCuAlignIEANZTgt2050-values'].rank(
            method='min',
            ascending=True,
            na_option='keep'
        ).fillna(10000)
        
        sbf_120_df['Carbon Budget Rank'] = temp_rank.rank(
            method='min',
            ascending=True
        )
        
        logger.info("Oekom data merged successfully")

        # ===================================================================
        # START EXCLUSION CRITERIA
        # ===================================================================
        exclusion_count = 1
        
        # Step 2a: Trust Metric screening
        sbf_120_df[f'exclusion_{exclusion_count}_TrustMetric'] = None

        # Get ISINs with Trust Metric < 0.6 or 'Not Collected'
        excluded_trust_metric = sbf_120_df[
            (sbf_120_df['Trust Metric Numeric'] < 0.6) |
            (sbf_120_df['Reported Emissions - Emissions Trust Metric'] == 'Not Collected')
        ]['ISIN'].tolist()

        sbf_120_df[f'exclusion_{exclusion_count}_TrustMetric'] = np.where(
            sbf_120_df['ISIN'].isin(excluded_trust_metric),
            'exclude_TrustMetric',
            None
        )
        logger.info(f"Trust Metric exclusions: {len(excluded_trust_metric)}")
        exclusion_count += 1

        # Step 2b: NBR Overall Flag exclusion
        sbf_120_df[f'exclusion_{exclusion_count}_NBROverallFlag'] = None
        NBR_Overall_Flag_Red = sbf_120_df[
            (sbf_120_df['NBR Overall Flag'] == 'RED') |
            (sbf_120_df['NBR Overall Flag'] == 'Not Collected')
        ]['ISIN'].tolist()

        sbf_120_df[f'exclusion_{exclusion_count}_NBROverallFlag'] = np.where(
            sbf_120_df['ISIN'].isin(NBR_Overall_Flag_Red),
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
            sbf_120_df[f'exclusion_{exclusion_count}_{label}'] = None
            
            # Get ISINs for this weapon type (Red or 'Not Collected')
            flagged_isins = sbf_120_df[
                (sbf_120_df[column] == 'RED') |
                (sbf_120_df[column] == 'Not Collected')
            ]['ISIN'].tolist()
            
            sbf_120_df[f'exclusion_{exclusion_count}_{label}'] = np.where(
                sbf_120_df['ISIN'].isin(flagged_isins),
                exclude_value,
                None
            )
            logger.info(f"{label} exclusions: {len(flagged_isins)}")
            exclusion_count += 1

        # Step 2d: Thermal Coal Mining screening
        sbf_120_df[f'exclusion_{exclusion_count}_CoalMining'] = None

        excluded_coal_mining = sbf_120_df[
            (sbf_120_df['Coal Mining Numeric'] > 0) |
            (sbf_120_df['Thermal Coal Mining - Maximum Percentage of Revenues (%)'] == 'Not Collected') |
            (sbf_120_df['Thermal Coal Mining - Maximum Percentage of Revenues (%)'] == 'Not Disclosed')
        ]['ISIN'].tolist()

        sbf_120_df[f'exclusion_{exclusion_count}_CoalMining'] = np.where(
            sbf_120_df['ISIN'].isin(excluded_coal_mining),
            'exclude_CoalMining',
            None
        )
        logger.info(f"Coal Mining exclusions: {len(excluded_coal_mining)}")
        exclusion_count += 1

        # Step 2e: Thermal Coal Power Generation screening (changed to 5%)
        sbf_120_df[f'exclusion_{exclusion_count}_ThermalPower'] = None

        excluded_thermal_power = sbf_120_df[
            (sbf_120_df['Thermal Power Numeric'] > 0.05) |
            (sbf_120_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'] == 'Not Collected') |
            (sbf_120_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'] == 'Not Disclosed')
        ]['ISIN'].tolist()

        sbf_120_df[f'exclusion_{exclusion_count}_ThermalPower'] = np.where(
            sbf_120_df['ISIN'].isin(excluded_thermal_power),
            'exclude_ThermalPower',
            None
        )
        logger.info(f"Thermal Power exclusions: {len(excluded_thermal_power)}")
        exclusion_count += 1

        # Step 2f: Oil Sands screening
        sbf_120_df[f'exclusion_{exclusion_count}_OilSands'] = None

        excluded_oil_sands = sbf_120_df[
            (sbf_120_df['Oil Sands Numeric'] > 0) |
            (sbf_120_df['Oil Sands - Production Maximum Percentage of Revenues (%)'] == 'Not Collected') |
            (sbf_120_df['Oil Sands - Production Maximum Percentage of Revenues (%)'] == 'Not Disclosed')
        ]['ISIN'].tolist()

        sbf_120_df[f'exclusion_{exclusion_count}_OilSands'] = np.where(
            sbf_120_df['ISIN'].isin(excluded_oil_sands),
            'exclude_OilSands',
            None
        )
        logger.info(f"Oil Sands exclusions: {len(excluded_oil_sands)}")
        exclusion_count += 1

        # Step 2f continued: Shale Oil and/or Gas screening
        sbf_120_df[f'exclusion_{exclusion_count}_ShaleOilGas'] = None

        excluded_shale = sbf_120_df[
            (sbf_120_df['Shale Oil and/or Gas - Involvement tie'] == 'Production') |
            (sbf_120_df['Shale Oil and/or Gas - Involvement tie'] == 'Not Collected') |
            (sbf_120_df['Shale Oil and/or Gas - Involvement tie'] == 'Not Disclosed')
        ]['ISIN'].tolist()

        sbf_120_df[f'exclusion_{exclusion_count}_ShaleOilGas'] = np.where(
            sbf_120_df['ISIN'].isin(excluded_shale),
            'exclude_ShaleOilGas',
            None
        )
        logger.info(f"Shale Oil/Gas exclusions: {len(excluded_shale)}")
        exclusion_count += 1

        # Step 2g: Carbon Budget screening (exclude worst 20% by ranking)
        sbf_120_df[f'exclusion_{exclusion_count}_CarbonBudget'] = None

        # Calculate the cutoff rank for worst 20%
        total_companies = len(sbf_120_df)
        cutoff_rank = total_companies * 0.8

        logger.info(f"Total companies in universe: {total_companies}")
        logger.info(f"Carbon Budget cutoff rank (80th percentile): {cutoff_rank}")

        # Exclude companies with ranks > cutoff (worst 20%)
        sbf_120_df[f'exclusion_{exclusion_count}_CarbonBudget'] = np.where(
            sbf_120_df['Carbon Budget Rank'] > cutoff_rank,
            'exclude_CarbonBudget',
            None
        )

        excluded_count = sbf_120_df[f'exclusion_{exclusion_count}_CarbonBudget'].notna().sum()
        logger.info(f"Carbon Budget exclusions (rank > {cutoff_rank}): {excluded_count}")
        exclusion_count += 1

        # Step 2h: Tobacco Production screening
        sbf_120_df[f'exclusion_{exclusion_count}_TobaccoProduction'] = None

        excluded_tobacco = sbf_120_df[
            (sbf_120_df['Tobacco Numeric'] > 0.1) |
            (sbf_120_df['Tobacco - Production Maximum Percentage of Revenues (%)'] == 'Not Collected') |
            (sbf_120_df['Tobacco - Production Maximum Percentage of Revenues (%)'] == 'Not Disclosed')
        ]['ISIN'].tolist()

        sbf_120_df[f'exclusion_{exclusion_count}_TobaccoProduction'] = np.where(
            sbf_120_df['ISIN'].isin(excluded_tobacco),
            'exclude_TobaccoProduction',
            None
        )
        logger.info(f"Tobacco Production exclusions: {len(excluded_tobacco)}")
        exclusion_count += 1

        # ===================================================================
        # CREATE EXCLUSION SUMMARY
        # ===================================================================
        # Create list of all exclusion columns
        exclusion_columns = [col for col in sbf_120_df.columns if col.startswith('exclusion_')]

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

        sbf_120_df['Exclusion Summary'] = sbf_120_df.apply(
            lambda row: summarize_exclusions(row, exclusion_columns), axis=1
        )

        sbf_120_df['Excluded'] = sbf_120_df['Exclusion Summary'].apply(
            lambda x: 'No' if x == 'Included' else 'Yes'
        )
        
        # ===================================================================
        # SELECTION PROCESS
        # ===================================================================
        # Step 3: Select companies with no exclusions
        selection_df = sbf_120_df[
            sbf_120_df[exclusion_columns].isna().all(axis=1)
        ].copy()
        
        logger.info(f"Companies remaining after exclusions: {len(selection_df)}")
        
        # Calculate FFMC_MR using Price MR
        selection_df['FFMC_MR'] = selection_df['Free Float'] * selection_df['Price MR'] * selection_df['NOSH']
        
        # Rank by Carbon Budget (lower is better), then by FFMC_MR (higher is better)
        selection_df['Selection Carbon Budget Rank'] = selection_df['ClimateCuAlignIEANZTgt2050-values'].rank(
            method='min',
            ascending=True,
            na_option='bottom'
        )
        
        selection_df['FFMC Rank'] = selection_df['FFMC_MR'].rank(
            method='min',
            ascending=False,
            na_option='bottom'
        )
        
        # Sort by Carbon Budget first (ascending), then FFMC_MR (descending)
        selection_df = selection_df.sort_values(
            ['Selection Carbon Budget Rank', 'FFMC_MR'],
            ascending=[True, False]
        )
        
        # Select top 60
        final_selection_df = selection_df.head(60).copy()
        final_selection_df = final_selection_df.reset_index(drop=True)
        logger.info(f"Selected top 60 companies")
        
        # ===================================================================
        # PRICE AND FX DATA FROM EOD
        # ===================================================================
        # Prices already merged at the beginning (Close Prc_EOD and Close Prc_CO)
        # Add Symbol column to final_selection_df for reference
        stock_eod_df['Reuters/Optiq'] = stock_eod_df['#Symbol'].str.len().apply(
            lambda x: 'Reuters' if x < 12 else 'Optiq'
        )

        def get_symbol(row, stock_df):
            mask = (stock_df['Isin Code'] == row['ISIN']) & \
                   (stock_df['MIC'] == row['MIC']) & \
                   (stock_df['Reuters/Optiq'] == 'Reuters')
            
            matches = stock_df[mask]
            
            if not matches.empty:
                return matches.iloc[0]['#Symbol']
            return None

        # Add Symbol column
        final_selection_df['Symbol'] = final_selection_df.apply(
            lambda row: get_symbol(row, stock_eod_df), axis=1
        )

        # ===================================================================
        # EQUAL WEIGHTING WITH CAPPING
        # ===================================================================
        # Get index market cap
        logger.info(f"Checking IsinCode {isin} in index_eod_df")
        matching_rows = index_eod_df[index_eod_df['IsinCode'] == isin]
        logger.info(f"Found {len(matching_rows)} matching rows")
        if len(matching_rows) > 0:
            index_mcap = matching_rows['Mkt Cap'].iloc[0]
        else:
            logger.error(f"No matching index found for ISIN {isin}")
            raise ValueError(f"No matching index found for ISIN {isin}")

        logger.info(f"Index market cap: {index_mcap}")
        final_selection_df['FFMC'] = final_selection_df['Free Float'] * final_selection_df['Close Prc_EOD'] * final_selection_df['NOSH'] 
        # Apply proportional capping using FFMC
        logger.info("Applying proportional capping with 10% max weight...")
        final_selection_df = apply_proportional_capping(
            final_selection_df,
            mcap_column='FFMC',
            max_weight=0.10,
            max_iterations=100
        )

        # Calculate final number of shares based on capped weights
        final_selection_df['Unrounded NOSH'] = (
            (final_selection_df['Current Weight'] * index_mcap) / final_selection_df['Price MR']
        )
        final_selection_df['Rounded NOSH'] = final_selection_df['Unrounded NOSH'].round()
        # Normalize Capping Factor by dividing by the maximum
        max_capping_factor = final_selection_df['Capping Factor'].max()
        final_selection_df['Capping Factor'] = final_selection_df['Capping Factor'] / max_capping_factor
        
        # Set effective date and currency
        final_selection_df['Effective Date of Review'] = effective_date
        final_selection_df['Currency (Local)'] = currency

        # ===================================================================
        # CREATE FINAL OUTPUT
        # ===================================================================
        # Create final output DataFrame for Index Composition sheet (minimal columns)
        FCLSP_df = final_selection_df[[
            'Company', 
            'ISIN', 
            'MIC', 
            'NOSH',
            'Free Float',
            'Capping Factor',
            'Effective Date of Review',
            'Currency (Local)'
        ]].copy()

        # Rename columns and sort
        FCLSP_df = FCLSP_df.rename(columns={
            'ISIN': 'ISIN Code',
            'NOSH': 'Number of Shares',
            'Currency (Local)': 'Currency'
        })
        FCLSP_df = FCLSP_df.sort_values('Company')
        
        # Inclusion/Exclusion analysis
        analysis_results = inclusion_exclusion_analysis(
            FCLSP_df, 
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
            fclsp_path = os.path.join(output_dir, f'FCLSP_df_{timestamp}.xlsx')
            
            # Save with timestamp to avoid any conflicts
            logger.info(f"Saving FCLSP output to: {fclsp_path}")
            with pd.ExcelWriter(fclsp_path) as writer:
                # Write each DataFrame to a different sheet
                FCLSP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                sbf_120_df.to_excel(writer, sheet_name='Full Universe', index=False)
                selection_df.to_excel(writer, sheet_name='Eligible Companies', index=False)
                final_selection_df.to_excel(writer, sheet_name='Final Selection', index=False)
                
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "fclsp_path": fclsp_path
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