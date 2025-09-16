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

def run_fri4p_review(date, co_date, effective_date, index="FRI4P", isin="FRIX00003643", 
                    area="US", area2="EU", type="STOCK", universe="Developed Market", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "FRI4P"
        isin (str, optional): ISIN code. Defaults to "FRIX00003643"
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
        Oekom_TrustCarbon_df = ref_data['oekom_trustcarbon']
        sesamm_df = ref_data['sesamm']
        
        if any(df is None for df in [ff_df, developed_market_df, icb_df, Oekom_TrustCarbon_df, sesamm_df]):
            raise ValueError("Failed to load one or more required reference data files")

        ff_df = ff_df.drop_duplicates(subset=['ISIN Code:'], keep='first')
        
        # Add Free Float data
        developed_market_df = developed_market_df.merge(
            ff_df[['ISIN Code:', 'Free Float Round:']],
            left_on='ISIN',
            right_on='ISIN Code:',
            how='left'
        ).drop('ISIN Code:', axis=1).rename(columns={'Free Float Round:': 'Free Float',
                                                     'Name': 'Company'})

        
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
        
        # Deduplicate sesamm_df before merging to prevent duplicate rows
        sesamm_df_clean = sesamm_df[['ISIN', 'Job_score_3Y']].drop_duplicates(subset=['ISIN'], keep='first')
        
        # Log the deduplication results
        logger.info(f"sesamm_df: {len(sesamm_df)} rows -> {len(sesamm_df_clean)} unique ISINs after deduplication")
        if len(sesamm_df) != len(sesamm_df_clean):
            logger.warning(f"Removed {len(sesamm_df) - len(sesamm_df_clean)} duplicate ISIN records from sesamm_df")
        
        selection_df = selection_df.merge(
            sesamm_df_clean,
            on='ISIN',
            how='left'
        ).merge(
            analysis_df[['ISIN', 'CRStaffRatingNum']],
            on='ISIN',
            how='left'
        )

        selection_df['Job_score_3Y'] = pd.to_numeric(selection_df['Job_score_3Y'], errors='coerce').fillna(0)
        
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
                    'Price': first_match['Close Prc'],
                    'FX Rate': fx_rate
                })
            return pd.Series({'Symbol': None, 'Price': None, 'FX Rate': None})

        # Add Symbol, Price, and FX Rate columns
        xpar_selected_20[['Symbol', 'Price', 'FX Rate']] = xpar_selected_20.apply(
            lambda row: get_stock_info(row, stock_eod_df, currency), axis=1
        )
        noxpar_selected_20[['Symbol', 'Price', 'FX Rate']] = noxpar_selected_20.apply(
            lambda row: get_stock_info(row, stock_eod_df, currency), axis=1
        )

        def get_free_float(row, ff_dataframe):
            mask = ff_dataframe['ISIN Code:'] == row['ISIN']
            matches = ff_dataframe[mask]
            if not matches.empty:
                return matches.iloc[0]['Free Float Round:']
            return None

        # Add Free Float columns
        xpar_selected_20['Free Float'] = xpar_selected_20.apply(
            lambda row: get_free_float(row, ff_df), axis=1
        )
        noxpar_selected_20['Free Float'] = noxpar_selected_20.apply(
            lambda row: get_free_float(row, ff_df), axis=1
        )

        # Calculate Price in Index Currency and Original market cap
        xpar_selected_20['Price in Index Currency'] = xpar_selected_20['Price'] * xpar_selected_20['FX Rate']
        noxpar_selected_20['Price in Index Currency'] = noxpar_selected_20['Price'] * noxpar_selected_20['FX Rate']
        xpar_selected_20['Original market cap'] = xpar_selected_20['Price in Index Currency'] * xpar_selected_20['NOSH'] * xpar_selected_20['Free Float']
        noxpar_selected_20['Original market cap'] = noxpar_selected_20['Price in Index Currency'] * noxpar_selected_20['NOSH'] * noxpar_selected_20['Free Float']

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
        max_capping = final_selection_df['Final Capping'].max()
        final_selection_df['Final Capping'] = (final_selection_df['Final Capping'] / max_capping).round(14)
        final_selection_df['Effective Date of Review'] = effective_date

        # Create final output DataFrame
        FRI4P_df = final_selection_df[[
            'Company', 
            'ISIN', 
            'MIC', 
            'NOSH', 
            'Free Float',
            'Final Capping',
            'Effective Date of Review',
            'Currency (Local)'
        ]].copy()

        # Rename columns and sort
        FRI4P_df = FRI4P_df.rename(columns={
            'Currency (Local)': 'Currency',
            'ISIN': 'ISIN Code',
            'NOSH': 'Number of Shares',
            'Final Capping': 'Capping Factor',
        })
        FRI4P_df = FRI4P_df.sort_values('Company')
        
        analysis_results = inclusion_exclusion_analysis(
            FRI4P_df, 
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
            fri4p_path = os.path.join(output_dir, f'FRI4P_df_{timestamp}.xlsx')
            
            # Save with timestamp to avoid any conflicts
            logger.info(f"Saving FRI4P output to: {fri4p_path}")
            with pd.ExcelWriter(fri4p_path) as writer:
                    # Write each DataFrame to a different sheet
                    FRI4P_df.to_excel(writer, sheet_name='Index Composition', index=False)
                    inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                    exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                    developed_market_df.to_excel(writer, sheet_name='Full Universe', index=False)
                    final_selection_df.to_excel(writer, sheet_name='Final Selection', index=False)                
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "fri4p_path": fri4p_path
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