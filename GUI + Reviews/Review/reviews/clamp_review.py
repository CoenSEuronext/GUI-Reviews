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

def run_clamp_review(date, co_date, effective_date, index="CLAMP", isin="FR0014005IK5", 
                    area="EU", area2="US", type="STOCK", universe="eurozone_300", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the CLAMP index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        co_date (str): Close-out date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "CLAMP"
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
        ref_data = load_reference_data(current_data_folder, ['ff', 'eurozone_300', 'oekom_trustcarbon'])
        
        # Extract the needed DataFrames
        ff_df = ref_data['ff']
        eurozone_300_df = ref_data['eurozone_300']
        Oekom_TrustCarbon_df = ref_data['oekom_trustcarbon']
        
        if any(df is None for df in [ff_df, eurozone_300_df, Oekom_TrustCarbon_df]):
            raise ValueError("Failed to load one or more required reference data files")

        ff_df = ff_df.drop_duplicates(subset=['ISIN Code:'], keep='first')
        
        # Add Free Float data
        eurozone_300_df = eurozone_300_df.merge(
            ff_df[['ISIN Code:', 'Free Float Round:']],
            left_on='ISIN',
            right_on='ISIN Code:',
            how='left'
        ).drop('ISIN Code:', axis=1).rename(columns={'Free Float Round:': 'Free Float',
                                                     'Name': 'Company'})

        # Step 2a: FFMC Screening - Keep top 150 by Free Float Market Cap
        logger.info("Step 2a: Calculating FFMC and filtering top 150...")
        eurozone_300_df['FFMC'] = eurozone_300_df['Price'] * eurozone_300_df['NOSH'] * eurozone_300_df['Free Float']
        
        # Rank by FFMC (higher is better)
        eurozone_300_df['FFMC Rank'] = eurozone_300_df['FFMC'].rank(
            method='min',
            ascending=False,
            na_option='bottom'
        )
        
        # Keep only top 150 by FFMC
        eurozone_300_df = eurozone_300_df[eurozone_300_df['FFMC Rank'] <= 150].copy()
        logger.info(f"After FFMC screening: {len(eurozone_300_df)} companies remain (top 150)")

        # Initialize exclusion columns
        exclusion_count = 1
        
        # Step 2b: Trust Metric screening
        logger.info("Step 2b: Trust Metric screening...")
        eurozone_300_df[f'exclusion_{exclusion_count}_TrustMetric'] = None

        # Get ISINs with Trust Metric < 0.6 or 'Not Collected'
        # First, convert to numeric but keep original column for 'Not Collected' check
        Oekom_TrustCarbon_df['Trust Metric Numeric'] = pd.to_numeric(
            Oekom_TrustCarbon_df['Reported Emissions - Emissions Trust Metric'], 
            errors='coerce'
        )

        excluded_trust_metric = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['Trust Metric Numeric'] < 0.6) |
            (Oekom_TrustCarbon_df['Reported Emissions - Emissions Trust Metric'] == 'Not Collected')
        ]['ISIN'].tolist()

        eurozone_300_df[f'exclusion_{exclusion_count}_TrustMetric'] = np.where(
            eurozone_300_df['ISIN'].isin(excluded_trust_metric),
            'exclude_TrustMetric',
            None
        )
        exclusion_count += 1

        # Step 2c: NBR Overall Flag exclusion (UNGC Violators)
        logger.info("Step 2c: NBR Overall Flag (UNGC Violators) screening...")
        eurozone_300_df[f'exclusion_{exclusion_count}_NBROverallFlag'] = None
        NBR_Overall_Flag_Red = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['NBR Overall Flag'] == 'RED') |
            (Oekom_TrustCarbon_df['NBR Overall Flag'] == 'Not Collected')
        ]['ISIN'].tolist()

        eurozone_300_df[f'exclusion_{exclusion_count}_NBROverallFlag'] = np.where(
            eurozone_300_df['ISIN'].isin(NBR_Overall_Flag_Red),
            'exclude_NBROverallFlag',
            None
        )
        exclusion_count += 1

        # Step 2d: Controversial Weapons screening
        logger.info("Step 2d: Controversial Weapons screening...")
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
            eurozone_300_df[f'exclusion_{exclusion_count}_{label}'] = None
            
            # Get ISINs for this weapon type (Red or 'Not Collected')
            flagged_isins = Oekom_TrustCarbon_df[
                (Oekom_TrustCarbon_df[column] == 'RED') |
                (Oekom_TrustCarbon_df[column] == 'Not Collected')
            ]['ISIN'].tolist()
            
            eurozone_300_df[f'exclusion_{exclusion_count}_{label}'] = np.where(
                eurozone_300_df['ISIN'].isin(flagged_isins),
                exclude_value,
                None
            )
            exclusion_count += 1

        # Step 2e: Thermal Coal Mining screening
        logger.info("Step 2e: Thermal Coal Mining screening...")
        eurozone_300_df[f'exclusion_{exclusion_count}_CoalMining'] = None

        # Keep original column for 'Not Collected' and 'Not Disclosed' check
        Oekom_TrustCarbon_df['Coal Mining Numeric'] = pd.to_numeric(
            Oekom_TrustCarbon_df['Thermal Coal Mining - Maximum Percentage of Revenues (%)'], 
            errors='coerce'
        )

        excluded_coal_mining = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['Coal Mining Numeric'] > 0) |
            (Oekom_TrustCarbon_df['Thermal Coal Mining - Maximum Percentage of Revenues (%)'] == 'Not Collected') |
            (Oekom_TrustCarbon_df['Thermal Coal Mining - Maximum Percentage of Revenues (%)'] == 'Not Disclosed')
        ]['ISIN'].tolist()

        eurozone_300_df[f'exclusion_{exclusion_count}_CoalMining'] = np.where(
            eurozone_300_df['ISIN'].isin(excluded_coal_mining),
            'exclude_CoalMining',
            None
        )
        exclusion_count += 1

        # Step 2f: Thermal Coal Power Generation screening
        logger.info("Step 2f: Thermal Coal Power Generation screening...")
        eurozone_300_df[f'exclusion_{exclusion_count}_ThermalPower'] = None

        # Keep original column for 'Not Collected' and 'Not Disclosed' check
        Oekom_TrustCarbon_df['Thermal Power Numeric'] = pd.to_numeric(
            Oekom_TrustCarbon_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'], 
            errors='coerce'
        )

        excluded_thermal_power = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['Thermal Power Numeric'] > 0.1) |
            (Oekom_TrustCarbon_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'] == 'Not Collected') |
            (Oekom_TrustCarbon_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'] == 'Not Disclosed')
        ]['ISIN'].tolist()

        eurozone_300_df[f'exclusion_{exclusion_count}_ThermalPower'] = np.where(
            eurozone_300_df['ISIN'].isin(excluded_thermal_power),
            'exclude_ThermalPower',
            None
        )
        exclusion_count += 1

        # Step 2g: Oil Sands screening (NEW)
        logger.info("Step 2g: Oil Sands screening...")
        eurozone_300_df[f'exclusion_{exclusion_count}_OilSands'] = None

        Oekom_TrustCarbon_df['Oil Sands Numeric'] = pd.to_numeric(
            Oekom_TrustCarbon_df['Oil Sands - Production Maximum Percentage of Revenues (%)'], 
            errors='coerce'
        )

        excluded_oil_sands = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['Oil Sands Numeric'] > 0) |
            (Oekom_TrustCarbon_df['Oil Sands - Production Maximum Percentage of Revenues (%)'] == 'Not Collected') |
            (Oekom_TrustCarbon_df['Oil Sands - Production Maximum Percentage of Revenues (%)'] == 'Not Disclosed')
        ]['ISIN'].tolist()

        eurozone_300_df[f'exclusion_{exclusion_count}_OilSands'] = np.where(
            eurozone_300_df['ISIN'].isin(excluded_oil_sands),
            'exclude_OilSands',
            None
        )
        exclusion_count += 1

        # Step 2h: Shale Oil and/or Gas screening (moved from 2f)
        logger.info("Step 2h: Shale Oil and/or Gas screening...")
        eurozone_300_df[f'exclusion_{exclusion_count}_ShaleOilGas'] = None

        excluded_shale = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['Shale Oil and/or Gas - Involvement tie'] == 'Production') |
            (Oekom_TrustCarbon_df['Shale Oil and/or Gas - Involvement tie'] == 'Not Collected') |
            (Oekom_TrustCarbon_df['Shale Oil and/or Gas - Involvement tie'] == 'Not Disclosed')
        ]['ISIN'].tolist()

        eurozone_300_df[f'exclusion_{exclusion_count}_ShaleOilGas'] = np.where(
            eurozone_300_df['ISIN'].isin(excluded_shale),
            'exclude_ShaleOilGas',
            None
        )
        exclusion_count += 1

        # Step 2i: Arctic Drilling screening (NEW)
        logger.info("Step 2i: Arctic Drilling screening...")
        eurozone_300_df[f'exclusion_{exclusion_count}_ArcticDrilling'] = None

        Oekom_TrustCarbon_df['Arctic Drilling Numeric'] = pd.to_numeric(
            Oekom_TrustCarbon_df['arctic_drilling_involvement'], 
            errors='coerce'
        )

        excluded_arctic = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['Arctic Drilling Numeric'] > 0) |
            (Oekom_TrustCarbon_df['arctic_drilling_involvement'] == 'Not Collected')
        ]['ISIN'].tolist()

        eurozone_300_df[f'exclusion_{exclusion_count}_ArcticDrilling'] = np.where(
            eurozone_300_df['ISIN'].isin(excluded_arctic),
            'exclude_ArcticDrilling',
            None
        )
        exclusion_count += 1

        # Step 2j: Deepwater Drilling screening (NEW)
        logger.info("Step 2j: Deepwater Drilling screening...")
        eurozone_300_df[f'exclusion_{exclusion_count}_DeepwaterDrilling'] = None

        excluded_deepwater = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['deepwater_drilling_involvement'] == 'Not Collected') |
            (Oekom_TrustCarbon_df['deepwater_drilling_involvement'] == 'T')
        ]['ISIN'].tolist()

        eurozone_300_df[f'exclusion_{exclusion_count}_DeepwaterDrilling'] = np.where(
            eurozone_300_df['ISIN'].isin(excluded_deepwater),
            'exclude_DeepwaterDrilling',
            None
        )
        exclusion_count += 1

        # Create list of all exclusion columns
        exclusion_columns = [col for col in eurozone_300_df.columns if col.startswith('exclusion_')]

        # Merge ALL relevant columns from Oekom data BEFORE creating summary columns
        # This ensures all datapoints appear before exclusion columns in output
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
            'Oil Sands - Production Maximum Percentage of Revenues (%)',
            'Shale Oil and/or Gas - Involvement tie',
            'arctic_drilling_involvement',
            'deepwater_drilling_involvement'
        ]

        # Store exclusion columns temporarily
        exclusion_data = eurozone_300_df[exclusion_columns].copy()

        # Drop exclusion columns before merge
        eurozone_300_df = eurozone_300_df.drop(columns=exclusion_columns)

        # Merge Oekom data
        eurozone_300_df = eurozone_300_df.merge(
            Oekom_TrustCarbon_df[oekom_columns_to_merge],
            on='ISIN',
            how='left',
            suffixes=('', '_oekom')
        )

        # Convert to numeric for ranking
        eurozone_300_df['ClimateCuAlignIEANZTgt2050-values'] = pd.to_numeric(
            eurozone_300_df['ClimateCuAlignIEANZTgt2050-values'], 
            errors='coerce'
        ).fillna(10000)

        # Handle 'Not Collected' values for ESG Performance Score
        eurozone_300_df['ESG Performance Score'] = pd.to_numeric(
            eurozone_300_df['ESG Performance Score'].mask(eurozone_300_df['ESG Performance Score'] == 'Not Collected'),
            errors='coerce'
        )

        # Add Carbon Budget Ranking for ALL companies (LOWER score is BETTER)
        temp_rank = eurozone_300_df['ClimateCuAlignIEANZTgt2050-values'].rank(
            method='min',
            ascending=True,
            na_option='keep'
        ).fillna(10000)

        eurozone_300_df['Carbon Budget Rank'] = temp_rank.rank(
            method='min',
            ascending=True
        )

        # NOW add back the exclusion columns (these will appear AFTER all datapoints)
        for col in exclusion_columns:
            eurozone_300_df[col] = exclusion_data[col]

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

        eurozone_300_df['Exclusion Summary'] = eurozone_300_df.apply(
            lambda row: summarize_exclusions(row, exclusion_columns), axis=1
        )

        eurozone_300_df['Excluded'] = eurozone_300_df['Exclusion Summary'].apply(
            lambda x: 'No' if x == 'Included' else 'Yes'
        )

        # Step 3a: Carbon Budget Selection - Select companies with no exclusions first
        logger.info("Step 3a: Carbon Budget Selection - selecting top 75 best Carbon Budget scorers...")
        selection_df = eurozone_300_df[
            eurozone_300_df[exclusion_columns].isna().all(axis=1)
        ].copy()
        
        logger.info(f"Companies passing all exclusions: {len(selection_df)}")

        # Sort by Carbon Budget score (ascending = lower is better) and select top 75
        selection_df = selection_df.sort_values(
            'ClimateCuAlignIEANZTgt2050-values',
            ascending=True
        )
        
        top_75_df = selection_df.head(75).copy()
        logger.info(f"Top 75 Carbon Budget scorers selected: {len(top_75_df)}")
        
        # Step 3b: FFMC Ranking - Of top 75, rank by FFMC
        logger.info("Step 3b: Ranking top 75 by FFMC...")
        top_75_df['FFMC Rank Final'] = top_75_df['FFMC'].rank(
            method='min',
            ascending=False,
            na_option='bottom'
        )
        
        # Add ESG Performance Score Ranking for the top 75
        top_75_df['ESG Performance Rank'] = top_75_df['ESG Performance Score'].rank(
            method='min',
            ascending=False,
            na_option='keep'
        )
        
        # Merge ESG Performance Rank back to full universe for visibility
        eurozone_300_df = eurozone_300_df.merge(
            top_75_df[['ISIN', 'ESG Performance Rank']],
            on='ISIN',
            how='left'
        )
        
        # Step 3c: Select top 40 by FFMC
        logger.info("Step 3c: Selecting top 40 by FFMC from top 75...")
        top_75_df = top_75_df.sort_values('FFMC', ascending=False)
        final_selection_df = top_75_df.head(40).copy()
        logger.info(f"Final selection: {len(final_selection_df)} companies")

        # Step 4: Get price and FX data
        def get_index_currency(row, index_df):
            mask = index_df['Mnemo'] == row['Index']
            matches = index_df[mask]
            if not matches.empty:
                return matches.iloc[0]['Curr']
            return None

        # Add Index Currency column to stock_eod_df
        stock_eod_df['Index Currency'] = stock_eod_df.apply(
            lambda row: get_index_currency(row, index_eod_df), axis=1
        )
        stock_eod_df['ISIN/Index'] = stock_eod_df['Isin Code'] + stock_eod_df['Index']
        stock_eod_df['id5'] = stock_eod_df['#Symbol'] + stock_eod_df['Index Currency']
        stock_eod_df['Reuters/Optiq'] = stock_eod_df['#Symbol'].str.len().apply(
            lambda x: 'Reuters' if x < 12 else 'Optiq'
        )

        def get_stock_info(row, stock_df, target_currency):
            mask = (stock_df['Isin Code'] == row['ISIN']) & \
                   (stock_df['MIC'] == row['MIC']) & \
                   (stock_df['Reuters/Optiq'] == 'Reuters')
            
            matches = stock_df[mask]
            
            if not matches.empty:
                first_match = matches.iloc[0]
                lookup_id5 = f"{first_match['#Symbol']}{target_currency}"
                
                fx_mask = stock_df['id5'] == lookup_id5
                fx_matches = stock_df[fx_mask]
                
                fx_rate = fx_matches.iloc[0]['FX/Index Ccy'] if not fx_matches.empty else None
                
                return pd.Series({
                    'Symbol': first_match['#Symbol'],
                    'Close Prc_EOD': first_match['Close Prc'],
                    'FX/Index Ccy': fx_rate
                })
            return pd.Series({'Symbol': None, 'Close Prc_EOD': None, 'FX/Index Ccy': None})

        # Add Symbol, Price, and FX Rate columns
        final_selection_df[['Symbol', 'Close Prc_EOD', 'FX/Index Ccy']] = final_selection_df.apply(
            lambda row: get_stock_info(row, stock_eod_df, currency), axis=1
        )

        # Calculate Price in Index Currency
        final_selection_df['Price in Index Currency'] = final_selection_df['Close Prc_EOD'] * final_selection_df['FX/Index Ccy']

        # Step 5: Equal weighting calculation
        logger.info(f"Checking IsinCode {isin} in index_eod_df")
        matching_rows = index_eod_df[index_eod_df['IsinCode'] == isin]
        logger.info(f"Found {len(matching_rows)} matching rows")
        if len(matching_rows) > 0:
            index_mcap = matching_rows['Mkt Cap'].iloc[0]
        else:
            logger.error(f"No matching index found for ISIN {isin}")
            raise ValueError(f"No matching index found for ISIN {isin}")
        index_mcap_df = pd.DataFrame({
            'Index Market Cap': [index_mcap]
        })
        # Calculate the target market cap per company (equal weighting across all 40 companies)
        top_n = 40
        target_mcap_per_company = index_mcap / top_n
        
        final_selection_df['Unrounded NOSH'] = target_mcap_per_company / (
            final_selection_df['Close Prc_EOD'] * final_selection_df['FX/Index Ccy']
        )
        final_selection_df['Rounded NOSH'] = final_selection_df['Unrounded NOSH'].round()
        final_selection_df['Free Float'] = 1
        final_selection_df['Capping Factor'] = 1.0
        # Set effective date
        final_selection_df['Effective Date of Review'] = effective_date

        # Create final output DataFrame for Index Composition sheet (minimal columns)
        CLAMP_df = final_selection_df[[
            'Company', 
            'ISIN', 
            'MIC', 
            'Rounded NOSH',
            'Free Float',
            'Capping Factor',
            'Effective Date of Review',
            'Currency'
        ]].copy()

        # Rename columns and sort
        CLAMP_df = CLAMP_df.rename(columns={
            'ISIN': 'ISIN Code',
            'Rounded NOSH': 'Number of Shares',
        })
        CLAMP_df = CLAMP_df.sort_values('Company')
        
        # Inclusion/Exclusion analysis
        analysis_results = inclusion_exclusion_analysis(
            CLAMP_df, 
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
            clamp_path = os.path.join(output_dir, f'CLAMP_df_{timestamp}.xlsx')
            
            # Save with timestamp to avoid any conflicts
            logger.info(f"Saving CLAMP output to: {clamp_path}")
            with pd.ExcelWriter(clamp_path) as writer:
                # Write each DataFrame to a different sheet
                CLAMP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                index_mcap_df.to_excel(writer, sheet_name='Index Market Cap', index=False)
                eurozone_300_df.to_excel(writer, sheet_name='Full Universe', index=False)
                top_75_df.to_excel(writer, sheet_name='Top 75 Carbon Budget', index=False)
                final_selection_df.to_excel(writer, sheet_name='Final Selection', index=False)
                
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "clamp_path": clamp_path
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