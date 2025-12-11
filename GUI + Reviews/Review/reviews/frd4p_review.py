import pandas as pd
import numpy as np
from datetime import datetime
import os
import traceback
from Review.functions import read_semicolon_csv
from config import DLF_FOLDER, DATA_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data
from utils.inclusion_exclusion import inclusion_exclusion_analysis

logger = setup_logging(__name__)

def run_frd4p_review(date, co_date, effective_date, index="FRD4P", isin="FRIX00003031", 
                    area="US", area2="EU", type="STOCK", universe="Developed Market", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "frd4p"
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
        index_eod_df, stock_eod_df, stock_co_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)
        
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
        

        # Replace the entire chained merge section with this more explicit approach:

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

        # Initialize exclusion columns
        # Currency exclusion
        developed_market_df['exclusion_1'] = None
        allowed_currencies = ['EUR', 'JPY', 'USD', 'CAD', 'GBP']
        developed_market_df['exclusion_1'] = np.where(
            ~developed_market_df['Currency (Local)'].isin(allowed_currencies),
            'exclude_currency',
            None
        )

        # SesamM Layoff score exclusion
        developed_market_df['exclusion_2'] = None
        developed_market_df['exclusion_2'] = np.where(
            ~developed_market_df['ISIN'].isin(sesamm_df['ISIN']),
            'exclude_layoff_score_6m',
            None
        )

        # Turnover EUR exclusion
        developed_market_df['exclusion_3'] = None
        developed_market_df['exclusion_3'] = np.where(
            (developed_market_df['3 months ADTV'] < 10000000),
            'exclude_turnover_EUR',
            None
        )

        # NBR Overall Flag exclusion
        developed_market_df['exclusion_4'] = None
        NBR_Overall_Flag_Red = Oekom_TrustCarbon_df[
            Oekom_TrustCarbon_df['NBR Overall Flag'] == 'RED'
        ]['ISIN'].tolist()

        developed_market_df['exclusion_4'] = np.where(
            (developed_market_df['ISIN'].isin(NBR_Overall_Flag_Red)),
            'exclude_NBROverallFlag',
            None
        )

        exclusion_criteria = {
            'Biological Weapons - Overall Flag': 'exclude_BiologicalWeaponsFlag',
            'Chemical Weapons - Overall Flag': 'exclude_ChemicalWeaponsFlag',
            'Nuclear Weapons Inside NPT - Overall Flag': 'exclude_NuclearWeaponsFlag',
            'Nuclear Weapons Outside NPT - Overall Flag': 'exclude_NuclearWeaponsNonNPTFlag',
            'Cluster Munitions - Overall Flag': 'exclude_ClusterMunitionsFlag',
            'Depleted Uranium - Overall Flag': 'exclude_DepletedUraniumFlag',
            'Anti-personnel Mines - Overall Flag': 'exclude_APMinesFlag',
            'White Phosphorous Weapons - Overall Flag': 'exclude_WhitePhosphorusFlag'
        }
        
        # Weapons exclusions
        exclusion_count = 5
        for column, exclude_value in exclusion_criteria.items():
            # Create new exclusion column
            new_col = f'exclusion_{exclusion_count}'
            developed_market_df[new_col] = None
            
            # Get ISINs for this weapon type
            flagged_isins = Oekom_TrustCarbon_df[
                Oekom_TrustCarbon_df[column].isin(['RED', 'Amber'])
            ]['ISIN'].tolist()
            
            # Apply exclusion independently
            developed_market_df[new_col] = np.where(
                developed_market_df['ISIN'].isin(flagged_isins),
                exclude_value,
                None
            )
            
            exclusion_count += 1

        # Energy Screening
        # Convert columns to numeric
        energy_columns = [
            'Coal Mining and Power Gen - Maximum Percentage of Revenues (%)',
            'FossilFuelProdMaxRev-values',
            'FossilFuelDistMaxRev-values',
            'Power Generation - Thermal Maximum Percentage of Revenues (%)'
        ]

        for col in energy_columns:
            Oekom_TrustCarbon_df[col] = pd.to_numeric(Oekom_TrustCarbon_df[col], errors='coerce')

        # Process energy exclusions
        energy_criteria = {
            'Coal': {
                'condition': lambda df: df['Coal Mining and Power Gen - Maximum Percentage of Revenues (%)'] >= 0.01,
                'exclude_value': 'exclude_CoalMining'
            },
            'FossilFuel': {
                'condition': lambda df: (df['FossilFuelProdMaxRev-values'] + df['FossilFuelDistMaxRev-values']) >= 0.10,
                'exclude_value': 'exclude_FossilFuel'
            },
            'Thermal': {
                'condition': lambda df: df['Power Generation - Thermal Maximum Percentage of Revenues (%)'] >= 0.50,
                'exclude_value': 'exclude_ThermalPower'
            }
        }

        for criterion_name, criterion in energy_criteria.items():
            new_col = f'exclusion_{exclusion_count}'
            developed_market_df[new_col] = None
            
            # Get ISINs that meet the exclusion condition
            excluded_isins = Oekom_TrustCarbon_df[
                criterion['condition'](Oekom_TrustCarbon_df)
            ]['ISIN'].tolist()
            
            # Apply exclusion independently
            developed_market_df[new_col] = np.where(
                developed_market_df['ISIN'].isin(excluded_isins),
                criterion['exclude_value'],
                None
            )
            
            exclusion_count += 1

        # SBT alignment exclusion
        # Get list of high climate impact NACE codes (first letter)
        high_impact_nace = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'L']

        # Get ISINs from nace_df that have high impact NACE codes
        # Need to check if the first character of each NACE code is in the high impact list
        high_impact_isins = nace_df[nace_df['NACE'].str[0].isin(high_impact_nace)]['ISIN'].tolist()

        # Create new exclusion column for SBT
        new_col = f'exclusion_{exclusion_count}'
        developed_market_df[new_col] = None

        # Get ISINs that meet the SBT exclusion condition - this part is correct
        # It finds companies that:
        # 1. Don't have 'Approved SBT' in ClimateGHGReductionTargets
        # 2. Are in the high_impact_isins list (have a high-impact NACE code)
        sbt_excluded_isins = Oekom_TrustCarbon_df[
            (Oekom_TrustCarbon_df['ClimateGHGReductionTargets'] != 'Approved SBT') & 
            (Oekom_TrustCarbon_df['ISIN'].isin(high_impact_isins))
        ]['ISIN'].tolist()

        # Apply SBT exclusion
        developed_market_df[new_col] = np.where(
            developed_market_df['ISIN'].isin(sbt_excluded_isins),
            'exclude_SBT_NACE',
            None
        )

        exclusion_count += 1

        # Tobacco Screening
        tobacco_columns = [
            'Tobacco - Production Maximum Percentage of Revenues (%)',
            'Tobacco - Distribution Maximum Percentage of Revenues (%)'
        ]

        for col in tobacco_columns:
            Oekom_TrustCarbon_df[col] = pd.to_numeric(Oekom_TrustCarbon_df[col], errors='coerce')

        tobacco_criteria = {
            'TobaccoProduction': {
                'condition': lambda df: df['Tobacco - Production Maximum Percentage of Revenues (%)'] > 0,
                'exclude_value': 'exclude_TobaccoProduction'
            },
            'TobaccoDistribution': {
                'condition': lambda df: df['Tobacco - Distribution Maximum Percentage of Revenues (%)'] >= 0.15,
                'exclude_value': 'exclude_TobaccoDistribution'
            }
        }

        for criterion_name, criterion in tobacco_criteria.items():
            new_col = f'exclusion_{exclusion_count}'
            developed_market_df[new_col] = None
            
            # Get ISINs that meet the exclusion condition
            excluded_isins = Oekom_TrustCarbon_df[
                criterion['condition'](Oekom_TrustCarbon_df)
            ]['ISIN'].tolist()
            
            # Apply exclusion independently
            developed_market_df[new_col] = np.where(
                developed_market_df['ISIN'].isin(excluded_isins),
                criterion['exclude_value'],
                None
            )
            
            exclusion_count += 1

        # Layoff Screening
        new_col = f'exclusion_{exclusion_count}'
        developed_market_df[new_col] = None

        excluded_isins = sesamm_df[
            sesamm_df['layoff_score_6m'] > 0
        ]['ISIN'].tolist()

        developed_market_df[new_col] = np.where(
            developed_market_df['ISIN'].isin(excluded_isins),
            'exclude_Layoff',
            None
        )

        exclusion_count += 1

        # Staff Rating Screening
        developed_market_isins = developed_market_df['ISIN'].tolist()

        analysis_df = (Oekom_TrustCarbon_df[Oekom_TrustCarbon_df['ISIN'].isin(developed_market_isins)]
            .merge(
                developed_market_df[['ISIN', 'Area Flag']],
                on='ISIN',
                how='left'
            )
            .merge(
                icb_df,
                left_on='ISIN',
                right_on='ISIN Code',
                how='left'
            )
            .drop_duplicates(subset=['ISIN'])
        )

        analysis_df['CRStaffRatingNum'] = pd.to_numeric(analysis_df['CRStaffRatingNum'], errors='coerce').fillna(3)

        excluded_isins = []

        for (sector, area), group in analysis_df.groupby(['Supersector Code', 'Area Flag']):
            logger.info(f"Processing sector: {sector}, area: {area}, group size: {len(group)}")
            sorted_group = group.sort_values('CRStaffRatingNum')
            n_companies = len(group)
            n_to_exclude = int(np.floor(n_companies * 0.1999999999))
            logger.info(f"Companies in group: {n_companies}, to exclude: {n_to_exclude}")
            if n_companies > 0 and n_to_exclude > 0:  # Add this check
                bottom_isins = sorted_group['ISIN'].iloc[:n_to_exclude].tolist()
                excluded_isins.extend(bottom_isins)
            else:
                logger.warning(f"No companies to exclude for sector {sector} and area {area}")

        new_col = f'exclusion_{exclusion_count}'
        developed_market_df[new_col] = None
        developed_market_df[new_col] = np.where(
            developed_market_df['ISIN'].isin(excluded_isins),
            'exclude_StaffRating',
            None
        )

        exclusion_count += 1

        # Create list of all exclusion columns
        exclusion_columns = [f'exclusion_{i}' for i in range(1, exclusion_count)]

        # Select companies that have no exclusions (all exclusion columns are None)
        selection_df = developed_market_df[
            developed_market_df[exclusion_columns].isna().all(axis=1)
        ].copy()
        
        selection_df = selection_df.merge(
            sesamm_df[['ISIN', 'Job_score_3Y']],
            on='ISIN',
            how='left'
        ).merge(
            analysis_df[['ISIN', 'CRStaffRatingNum']],
            on='ISIN',
            how='left'
        )

        selection_df['Job_score_3Y'] = pd.to_numeric(selection_df['Job_score_3Y'], errors='coerce').fillna(0)

        selection_df['Job_score_3Y_numeric'] = pd.to_numeric(selection_df['Job_score_3Y'], errors='coerce')
        selection_df['CRStaffRatingNum_numeric'] = pd.to_numeric(selection_df['CRStaffRatingNum'], errors='coerce')
        
        # Create ranking within each MIC type (XPAR vs non-XPAR)
        selection_df['MIC_Type'] = selection_df['MIC'].apply(lambda x: 'XPAR' if x == 'XPAR' else 'Non-XPAR')
        selection_df['Ranking'] = selection_df.groupby('MIC_Type')[['Job_score_3Y_numeric', 'CRStaffRatingNum_numeric']].rank(
            method='first',
            ascending=[False, False]
        ).min(axis=1).astype(int)
        
        # Sort by MIC_Type and Ranking for better readability
        selection_df = selection_df.sort_values(['MIC_Type', 'Ranking'])
        
        developed_market_df['Job_score_3Y_numeric'] = pd.to_numeric(
            developed_market_df['ISIN'].map(sesamm_df.set_index('ISIN')['Job_score_3Y']), 
            errors='coerce'
        ).fillna(0)
        developed_market_df['CRStaffRatingNum_numeric'] = pd.to_numeric(
            developed_market_df['ISIN'].map(analysis_df.set_index('ISIN')['CRStaffRatingNum']), 
            errors='coerce'
        ).fillna(3)
        
        # Create ranking within each MIC type for full universe
        developed_market_df['MIC_Type'] = developed_market_df['MIC'].apply(lambda x: 'XPAR' if x == 'XPAR' else 'Non-XPAR')
        developed_market_df['Ranking'] = developed_market_df.groupby('MIC_Type')[['Job_score_3Y_numeric', 'CRStaffRatingNum_numeric']].rank(
            method='first',
            ascending=[False, False]
        ).min(axis=1).astype(int)
        
        def select_top_stocks(df, mic_type, n_stocks):
            if mic_type == 'XPAR':
                filtered_df = df[df['MIC'] == 'XPAR'].copy()
            else:
                filtered_df = df[df['MIC'] != 'XPAR'].copy()
            
            filtered_df['Job_score_3Y'] = pd.to_numeric(filtered_df['Job_score_3Y'], errors='coerce')
            filtered_df['CRStaffRatingNum'] = pd.to_numeric(filtered_df['CRStaffRatingNum'], errors='coerce')
            
            sorted_df = filtered_df.sort_values(
                by=['Job_score_3Y', 'CRStaffRatingNum'],
                ascending=[False, False],
                na_position='last'
            )
            
            return sorted_df.head(n_stocks)

        xpar_selected_25 = select_top_stocks(selection_df, 'XPAR', 25)
        noxpar_selected_25 = select_top_stocks(selection_df, 'NOXPAR', 25)
        xpar_selected_20 = select_top_stocks(selection_df, 'XPAR', 20)
        noxpar_selected_20 = select_top_stocks(selection_df, 'NOXPAR', 20)

        full_selection_df = pd.concat([xpar_selected_25, noxpar_selected_25])
        final_selection_df = pd.concat([xpar_selected_20, noxpar_selected_20])


        def apply_capping(df, step, cap_threshold=0.2, final_step=False):
            current_step = step
            next_step = step + 1
            
            prev_mcap = f'Mcap {current_step-1}' if current_step > 1 else 'Original market cap'
            
            n_capping = (df[f'Capping {current_step}'] == 1).sum()
            perc_no_cap = 1 - (n_capping * cap_threshold)
            mcap_capping = df[df[f'Capping {current_step}'] == 1][prev_mcap].sum()
            new_mcap = (df[prev_mcap].sum() - mcap_capping) / perc_no_cap
            
            df[f'Mcap {current_step}'] = df.apply(
                lambda row: cap_threshold * new_mcap if row[f'Capping {current_step}'] == 1 else row[prev_mcap],
                axis=1
            )
            df[f'Weight {current_step}'] = df[f'Mcap {current_step}'] / new_mcap
            
            if not final_step:
                df[f'Capping {next_step}'] = df[f'Weight {current_step}'].apply(
                    lambda x: 1 if x > cap_threshold else 0
                )
            
            return df

        # Initial setup for capping
        logger.info(f"Checking IsinCode {isin} in index_eod_df")
        matching_rows = index_eod_df[index_eod_df['IsinCode'] == isin]
        logger.info(f"Found {len(matching_rows)} matching rows")
        if len(matching_rows) > 0:
            index_mkt_cap = matching_rows['Mkt Cap'].iloc[0]
        else:
            logger.error(f"No matching index found for ISIN {isin}")
            raise ValueError(f"No matching index found for ISIN {isin}")

        ffmc_world = noxpar_selected_20['Original market cap'].sum()
        ffmc_france = xpar_selected_20['Original market cap'].sum()
        ffmc_total = ffmc_france + ffmc_world

        # Initial weights and capping
        xpar_selected_20['Weight'] = xpar_selected_20['Original market cap'] / ffmc_france
        noxpar_selected_20['Weight'] = noxpar_selected_20['Original market cap'] / ffmc_world
        xpar_selected_20['Capping 1'] = xpar_selected_20['Weight'].apply(lambda x: 1 if x > 0.2 else 0)
        noxpar_selected_20['Capping 1'] = noxpar_selected_20['Weight'].apply(lambda x: 1 if x > 0.2 else 0)

        # Apply capping process
        for step in [1, 2]:
            xpar_selected_20 = apply_capping(xpar_selected_20, step)
            noxpar_selected_20 = apply_capping(noxpar_selected_20, step)

        # Final capping step
        xpar_selected_20 = apply_capping(xpar_selected_20, 3, final_step=True)
        noxpar_selected_20 = apply_capping(noxpar_selected_20, 3, final_step=True)

        # Calculate Final Capping
        xpar_selected_20['Final Capping'] = (xpar_selected_20['Weight 3'] * ffmc_total) / xpar_selected_20['Original market cap']
        noxpar_selected_20['Final Capping'] = (noxpar_selected_20['Weight 3'] * ffmc_total) / noxpar_selected_20['Original market cap']

        # Combine final selections
        final_selection_df = pd.concat([xpar_selected_20, noxpar_selected_20])

        # Remove the old get_stock_info and get_free_float functions and their calls
        # as they're now replaced by the chained merge approach above
        max_capping = final_selection_df['Final Capping'].max()
        final_selection_df['Final Capping'] = (final_selection_df['Final Capping'] / max_capping).round(14)
        final_selection_df['Effective Date of Review'] = effective_date

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
        # Save output files
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
                    final_selection_df.to_excel(writer, sheet_name='Selection', index=False)
                    full_selection_df.to_excel(writer, sheet_name='Full Selection', index=False)
                
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