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

def run_ezcla_review(date, co_date, effective_date, index="EZCLA", isin="FR0014005IK5", 
                    area="EU", area2="", type="STOCK", universe="eurozone_300", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the EZCLA index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        co_date (str): Close-out date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "EZCLA"
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

        # Initialize exclusion columns
        exclusion_count = 1
        
        # Step 2a: Trust Metric screening
        eurozone_300_df[f'exclusion_{exclusion_count}'] = None
        
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
        
        eurozone_300_df[f'exclusion_{exclusion_count}'] = np.where(
            eurozone_300_df['ISIN'].isin(excluded_trust_metric),
            'exclude_TrustMetric',
            None
        )
        exclusion_count += 1

        # Step 2b: NBR Overall Flag exclusion
        eurozone_300_df[f'exclusion_{exclusion_count}'] = None
        NBR_Overall_Flag_Red = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['NBR Overall Flag'] == 'RED') |
            (Oekom_TrustCarbon_df['NBR Overall Flag'] == 'Not Collected')
        ]['ISIN'].tolist()

        eurozone_300_df[f'exclusion_{exclusion_count}'] = np.where(
            eurozone_300_df['ISIN'].isin(NBR_Overall_Flag_Red),
            'exclude_NBROverallFlag',
            None
        )
        exclusion_count += 1

        # Step 2c: Controversial Weapons screening
        weapons_columns = {
            'Anti-personnel Mines - Overall Flag': 'exclude_APMines',
            'Biological Weapons - Overall Flag': 'exclude_BiologicalWeapons',
            'Chemical Weapons - Overall Flag': 'exclude_ChemicalWeapons',
            'Cluster Munitions - Overall Flag': 'exclude_ClusterMunitions',
            'Depleted Uranium - Overall Flag': 'exclude_DepletedUranium',
            'Incendiary Weapons - Overall Flag': 'exclude_IncendiaryWeapons',
            'Nuclear Weapons Outside NPT - Overall Flag': 'exclude_NuclearWeaponsNonNPT',
            'White Phosphorous Weapons - Overall Flag': 'exclude_WhitePhosphorus'
        }
        
        for column, exclude_value in weapons_columns.items():
            eurozone_300_df[f'exclusion_{exclusion_count}'] = None
            
            # Get ISINs for this weapon type (Red or 'Not Collected')
            flagged_isins = Oekom_TrustCarbon_df[
                (Oekom_TrustCarbon_df[column] == 'RED') |
                (Oekom_TrustCarbon_df[column] == 'Not Collected')
            ]['ISIN'].tolist()
            
            eurozone_300_df[f'exclusion_{exclusion_count}'] = np.where(
                eurozone_300_df['ISIN'].isin(flagged_isins),
                exclude_value,
                None
            )
            exclusion_count += 1

        # Step 2d: Thermal Coal Mining screening
        eurozone_300_df[f'exclusion_{exclusion_count}'] = None
        
        # Keep original column for 'Not Collected' check
        Oekom_TrustCarbon_df['Coal Mining Numeric'] = pd.to_numeric(
            Oekom_TrustCarbon_df['Thermal Coal Mining - Maximum Percentage of Revenues (%)'], 
            errors='coerce'
        )
        
        excluded_coal_mining = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['Coal Mining Numeric'] > 0) |
            (Oekom_TrustCarbon_df['Thermal Coal Mining - Maximum Percentage of Revenues (%)'] == 'Not Collected')
        ]['ISIN'].tolist()
        
        eurozone_300_df[f'exclusion_{exclusion_count}'] = np.where(
            eurozone_300_df['ISIN'].isin(excluded_coal_mining),
            'exclude_CoalMining',
            None
        )
        exclusion_count += 1

        # Step 2e: Thermal Coal Power Generation screening
        eurozone_300_df[f'exclusion_{exclusion_count}'] = None
        
        # Keep original column for 'Not Collected' check
        Oekom_TrustCarbon_df['Thermal Power Numeric'] = pd.to_numeric(
            Oekom_TrustCarbon_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'], 
            errors='coerce'
        )
        
        excluded_thermal_power = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['Thermal Power Numeric'] > 0.1) |
            (Oekom_TrustCarbon_df['Power Generation - Thermal Maximum Percentage of Revenues (%)'] == 'Not Collected')
        ]['ISIN'].tolist()
        
        eurozone_300_df[f'exclusion_{exclusion_count}'] = np.where(
            eurozone_300_df['ISIN'].isin(excluded_thermal_power),
            'exclude_ThermalPower',
            None
        )
        exclusion_count += 1

        # Step 2f: Shale Oil and/or Gas screening
        eurozone_300_df[f'exclusion_{exclusion_count}'] = None
        
        excluded_shale = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['Shale Oil and/or Gas - Involvement tie'] == 'Production') |
            (Oekom_TrustCarbon_df['Shale Oil and/or Gas - Involvement tie'] == 'Not Collected')
        ]['ISIN'].tolist()
        
        eurozone_300_df[f'exclusion_{exclusion_count}'] = np.where(
            eurozone_300_df['ISIN'].isin(excluded_shale),
            'exclude_ShaleOilGas',
            None
        )
        exclusion_count += 1

        # Step 2g: Carbon Budget screening (exclude worst 20% by ranking)
        eurozone_300_df[f'exclusion_{exclusion_count}'] = None
        
        Oekom_TrustCarbon_df['ClimateCuAlignIEANZTgt2050-values'] = pd.to_numeric(
            Oekom_TrustCarbon_df['ClimateCuAlignIEANZTgt2050-values'], 
            errors='coerce'
        )
        
        # Get ISINs in eurozone_300
        eurozone_isins = eurozone_300_df['ISIN'].tolist()
        
        # Filter Oekom data to only eurozone ISINs
        eurozone_oekom = Oekom_TrustCarbon_df[Oekom_TrustCarbon_df['ISIN'].isin(eurozone_isins)].copy()
        
        # Rank all eurozone companies by Carbon Budget score
        # Step 1: Rank companies with data (lower score = better rank)
        # Step 2: Fill NaN with rank 1
        # Step 3: Re-rank to adjust for ties (so 1-1-1-2 becomes 1-1-1-4)
        temp_rank = eurozone_oekom['ClimateCuAlignIEANZTgt2050-values'].rank(
            method='min',
            ascending=True,
            na_option='keep'
        ).fillna(1)
        
        # Re-rank to adjust for ties (this implements the Excel COUNTIF logic)
        eurozone_oekom['Temp_Carbon_Rank'] = temp_rank.rank(
            method='min',
            ascending=True
        )
        
        # Calculate the cutoff rank for worst 20%
        # If 300 companies, worst 20% means ranks > 240
        total_companies = len(eurozone_oekom)
        cutoff_rank = total_companies * 0.8
        
        logger.info(f"Total companies in universe: {total_companies}")
        logger.info(f"Carbon Budget cutoff rank (80th percentile): {cutoff_rank}")
        
        # Exclude companies with ranks > cutoff (worst 20%)
        excluded_carbon_budget = eurozone_oekom[
            eurozone_oekom['Temp_Carbon_Rank'] > cutoff_rank
        ]['ISIN'].tolist()
        
        logger.info(f"Total companies excluded by Carbon Budget (rank > {cutoff_rank}): {len(excluded_carbon_budget)}")
        
        eurozone_300_df[f'exclusion_{exclusion_count}'] = np.where(
            eurozone_300_df['ISIN'].isin(excluded_carbon_budget),
            'exclude_CarbonBudget',
            None
        )
        exclusion_count += 1

        # Create list of all exclusion columns
        exclusion_columns = [f'exclusion_{i}' for i in range(1, exclusion_count)]

        # Merge all companies with Oekom data to get ClimateCuAlignIEANZTgt2050-values and ESG Performance Score
        eurozone_300_df = eurozone_300_df.merge(
            Oekom_TrustCarbon_df[['ISIN', 'ClimateCuAlignIEANZTgt2050-values', 'ESG Performance Score']],
            on='ISIN',
            how='left',
            suffixes=('', '_oekom')
        )
        
        # Convert to numeric for ranking
        eurozone_300_df['ClimateCuAlignIEANZTgt2050-values'] = pd.to_numeric(
            eurozone_300_df['ClimateCuAlignIEANZTgt2050-values'], 
            errors='coerce'
        )
        
        # Handle 'Not Collected' values for ESG Performance Score
        # Use mask to avoid FutureWarning about downcasting
        eurozone_300_df['ESG Performance Score'] = pd.to_numeric(
            eurozone_300_df['ESG Performance Score'].mask(eurozone_300_df['ESG Performance Score'] == 'Not Collected'),
            errors='coerce'
        )
        
        # Add Carbon Budget Ranking for ALL companies (LOWER score is BETTER)
        # Step 1: Rank companies with data (lower score = better rank)
        # Step 2: Fill NaN with rank 1
        # Step 3: Re-rank to adjust for ties (so 1-1-1-2 becomes 1-1-1-4)
        temp_rank = eurozone_300_df['ClimateCuAlignIEANZTgt2050-values'].rank(
            method='min',
            ascending=True,
            na_option='keep'
        ).fillna(1)
        
        # Re-rank to adjust for ties (this implements the Excel COUNTIF logic)
        eurozone_300_df['Carbon Budget Rank'] = temp_rank.rank(
            method='min',
            ascending=True
        )
        
        # Step 2h: Select companies with no exclusions
        selection_df = eurozone_300_df[
            eurozone_300_df[exclusion_columns].isna().all(axis=1)
        ].copy()
        
        # Add ESG Performance Score Ranking ONLY for non-excluded companies (higher is better)
        # Only rank companies with actual ESG scores (not NaN/'Not Collected')
        # Rank descending: highest score gets rank 1 (best)
        selection_df['ESG Performance Rank'] = selection_df['ESG Performance Score'].rank(
            method='min',
            ascending=False,
            na_option='keep'
        )
        
        # Merge ESG Performance Rank back to full universe for visibility
        eurozone_300_df = eurozone_300_df.merge(
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
        
        # Select top 150 eligible companies
        top_150_df = selection_df.head(150).copy()
        
        # Step 3: Get price and FX data
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
        top_150_df[['Symbol', 'Close Prc_EOD', 'FX/Index Ccy']] = top_150_df.apply(
            lambda row: get_stock_info(row, stock_eod_df, currency), axis=1
        )

        # Calculate FFMC (Free Float Market Cap)
        top_150_df['Price in Index Currency'] = top_150_df['Close Prc_EOD'] * top_150_df['FX/Index Ccy']
        top_150_df['FFMC'] = top_150_df['Price in Index Currency'] * top_150_df['NOSH'] * top_150_df['Free Float']

        # Add FFMC Ranking (higher FFMC = better rank)
        top_150_df['FFMC Rank'] = top_150_df['FFMC'].rank(
            method='min',
            ascending=False,
            na_option='bottom'
        )
        
        # Rank by FFMC and select top 35
        top_150_df = top_150_df.sort_values('FFMC', ascending=False)
        final_selection_df = top_150_df.head(35).copy()

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
        
        # Set effective date
        final_selection_df['Effective Date of Review'] = effective_date

        # Create final output DataFrame for Index Composition sheet (minimal columns)
        EZCLA_df = final_selection_df[[
            'Company', 
            'ISIN', 
            'MIC', 
            'Rounded NOSH',
            'Free Float',
            'Effective Date of Review',
            'Currency'
        ]].copy()

        # Rename columns and sort
        EZCLA_df = EZCLA_df.rename(columns={
            'ISIN': 'ISIN Code',
            'Rounded NOSH': 'Number of Shares',
        })
        EZCLA_df = EZCLA_df.sort_values('Company')
        
        # Inclusion/Exclusion analysis
        analysis_results = inclusion_exclusion_analysis(
            EZCLA_df, 
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
            ezcla_path = os.path.join(output_dir, f'EZCLA_df_{timestamp}.xlsx')
            
            # Save with timestamp to avoid any conflicts
            logger.info(f"Saving EZCLA output to: {ezcla_path}")
            with pd.ExcelWriter(ezcla_path) as writer:
                # Write each DataFrame to a different sheet
                EZCLA_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                eurozone_300_df.to_excel(writer, sheet_name='Full Universe', index=False)
                top_150_df.to_excel(writer, sheet_name='Top 150', index=False)
                final_selection_df.to_excel(writer, sheet_name='Final Selection', index=False)
                
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "ezcla_path": ezcla_path
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