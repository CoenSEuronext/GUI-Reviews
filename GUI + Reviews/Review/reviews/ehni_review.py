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

def run_ehni_review(date, co_date, effective_date, index="EHNI", isin="FRESG0002815", 
                   area="US", area2="EU", type="STOCK", universe="fixed_basket", 
                   feed="Reuters", currency="EUR", year=None):

    try:
        # Simplify year extraction
        year = year or str(datetime.strptime(date, '%Y%m%d').year)

        # Set data folder for current month
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        # Load data with error handling
        logger.info("Loading EOD data...")
        index_eod_df, stock_eod_df, stock_co_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)

        logger.info("Loading reference data...")
        
        ref_data = load_reference_data(
            current_data_folder,
            ['ff', 'sustainalytics']
        )

        # EHNI universe
        ehni_universe = [
            {'Company': 'IBERDROLA', 'ISIN': 'ES0144580Y14', 'MIC': 'XMAD', 'Currency': 'EUR'},
            {'Company': 'VESTAS WIND SYSTEMS', 'ISIN': 'DK0061539921', 'MIC': 'XCSE', 'Currency': 'DKK'},
            {'Company': 'SIEMENS ENERGY AG', 'ISIN': 'DE000ENER6Y0', 'MIC': 'XETR', 'Currency': 'EUR'},
            {'Company': 'RIO TINTO PLC', 'ISIN': 'GB0007188757', 'MIC': 'XLON', 'Currency': 'GBX'},
            {'Company': 'ANTOFAGASTA PLC', 'ISIN': 'GB0000456144', 'MIC': 'XLON', 'Currency': 'GBX'},
            {'Company': 'ASML HOLDING', 'ISIN': 'NL0010273215', 'MIC': 'XAMS', 'Currency': 'EUR'},
            {'Company': 'STMICROELECTRONICS', 'ISIN': 'NL0000226223', 'MIC': 'XPAR', 'Currency': 'EUR'},
            {'Company': 'NORSK HYDRO', 'ISIN': 'NO0005052605', 'MIC': 'XOSL', 'Currency': 'NOK'},
            {'Company': 'SCHNEIDER ELECTRIC', 'ISIN': 'FR0000121972', 'MIC': 'XPAR', 'Currency': 'EUR'},
            {'Company': 'NXP SEMICONDUCTORS', 'ISIN': 'NL0009538784', 'MIC': 'XNGS', 'Currency': 'USD'},
            {'Company': 'ADVANCED MICRO DEV.', 'ISIN': 'US0079031078', 'MIC': 'XNGS', 'Currency': 'USD'},
            {'Company': 'NVIDIA CORP', 'ISIN': 'US67066G1040', 'MIC': 'XNGS', 'Currency': 'USD'},
            {'Company': 'NEWMONT COR', 'ISIN': 'US6516391066', 'MIC': 'XNYS', 'Currency': 'USD'},
            {'Company': 'FREEPORT-MCMORAN', 'ISIN': 'US35671D8570', 'MIC': 'XNYS', 'Currency': 'USD'},
            {'Company': 'ACCIONA', 'ISIN': 'ES0125220311', 'MIC': 'XMAD', 'Currency': 'EUR'},
            {'Company': 'PAN AMERICAN SILVER', 'ISIN': 'CA6979001089', 'MIC': 'XNYS', 'Currency': 'USD'},
            {'Company': 'ALCOA CORP', 'ISIN': 'US0138721065', 'MIC': 'XNYS', 'Currency': 'USD'},
            {'Company': 'SOUTHERN COPPER CORP', 'ISIN': 'US84265V1052', 'MIC': 'XNYS', 'Currency': 'USD'},
            {'Company': 'ENPHASE ENERGY INC.', 'ISIN': 'US29355A1079', 'MIC': 'XNMS', 'Currency': 'USD'},
            {'Company': 'FIRST SOLAR INC', 'ISIN': 'US3364331070', 'MIC': 'XNGS', 'Currency': 'USD'}
        ]

        # Convert to DataFrame when needed
        ehni_df = pd.DataFrame(ehni_universe)
        ff_df = ref_data['ff']

        
        # Add the required columns to the combined dataframe
        ehni_df['Capping Factor'] = 1
        ehni_df['Effective Date of Review'] = effective_date

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')
        
        # Chain all data preparation operations using the combined dataframe
        selection_df = (ehni_df
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
        )
        
        # Validate data loading
        if selection_df is None:
            raise ValueError("Failed to load required reference data files")
    
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        
        # Use the merged Free Float Round value, fallback to original Free Float if not available
        selection_df["Free Float"] = selection_df["Free Float Round:"]

        # Merge Sustainalytics data
        logger.info("Merging Sustainalytics data...")
        
        sustainalytics_raw = ref_data.get('sustainalytics')

        # Check if sustainalytics data was loaded
        if sustainalytics_raw is None:
            logger.warning("Sustainalytics data not available. Skipping sustainalytics merge.")
        else:
            # The file has row 0 = headers (text) and row 1 = codes (numbers as text)
            # Define the required codes (these are in row 1 of the Excel file)
            required_codes = [
                '231112111799', '172911112999', '172915112999', '171025111199',
                '173012171899', '173211112999', '171114221199', '171114261199',
                '171114301199', '171114201199', '171114241199', '171114281199',
                '171025141199', '171114141199', '171611102999', '211010122999',
                '171613102999'
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
                    selection_df = selection_df.merge(
                        sustainalytics_filtered,
                        left_on='ISIN code',
                        right_on='ISIN',
                        how='left'
                    ).drop('ISIN', axis=1, errors='ignore')
                    
                    logger.info(f"Sustainalytics merge completed. Added {len(cols_to_keep)-1} columns.")
                else:
                    logger.warning("No matching sustainalytics columns found.")
            else:
                logger.warning("Sustainalytics dataframe is empty")
        
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
        
        # Exclusion 1: 231112111799 == 'Non-Compliant'
        col_231112111799 = find_column_by_code(selection_df, '231112111799')
        if col_231112111799:
            selection_df['exclusion_1_231112111799'] = selection_df[col_231112111799] == 'Non-Compliant'
        else:
            selection_df['exclusion_1_231112111799'] = False
        
        # Exclusion 2: 172911112999 > 0 OR 172915112999 >= 0.1
        col_172911112999 = find_column_by_code(selection_df, '172911112999')
        col_172915112999 = find_column_by_code(selection_df, '172915112999')
        exclusion_2 = False
        if col_172911112999:
            exclusion_2 = exclusion_2 | (safe_numeric(selection_df[col_172911112999]) > 0)
        if col_172915112999:
            exclusion_2 = exclusion_2 | (safe_numeric(selection_df[col_172915112999]) >= 10)
        selection_df['exclusion_2_172911112999_172915112999'] = exclusion_2
        
        # Exclusion 3: 171025111199 >= 0.01
        col_171025111199 = find_column_by_code(selection_df, '171025111199')
        if col_171025111199:
            selection_df['exclusion_3_171025111199'] = safe_numeric(selection_df[col_171025111199]) >= 1
        else:
            selection_df['exclusion_3_171025111199'] = False
        
        # Exclusion 4: 173012171899 > 0
        col_173012171899 = find_column_by_code(selection_df, '173012171899')
        if col_173012171899:
            selection_df['exclusion_4_173012171899'] = safe_numeric(selection_df[col_173012171899]) > 0
        else:
            selection_df['exclusion_4_173012171899'] = False
        
        # Exclusion 5: 173211112999 > 0
        col_173211112999 = find_column_by_code(selection_df, '173211112999')
        if col_173211112999:
            selection_df['exclusion_5_173211112999'] = safe_numeric(selection_df[col_173211112999]) > 0
        else:
            selection_df['exclusion_5_173211112999'] = False
        
        # Exclusion 6: Sum of (171114221199, 171114261199, 171114301199) >= 0.1
        col_171114221199 = find_column_by_code(selection_df, '171114221199')
        col_171114261199 = find_column_by_code(selection_df, '171114261199')
        col_171114301199 = find_column_by_code(selection_df, '171114301199')
        sum_exclusion_6 = 0
        if col_171114221199:
            sum_exclusion_6 += safe_numeric(selection_df[col_171114221199])
        if col_171114261199:
            sum_exclusion_6 += safe_numeric(selection_df[col_171114261199])
        if col_171114301199:
            sum_exclusion_6 += safe_numeric(selection_df[col_171114301199])
        selection_df['exclusion_6_171114221199_171114261199_171114301199'] = sum_exclusion_6 >= 10
        
        # Exclusion 7: Sum of (171114201199, 171114241199, 171114281199) >= 0.5
        col_171114201199 = find_column_by_code(selection_df, '171114201199')
        col_171114241199 = find_column_by_code(selection_df, '171114241199')
        col_171114281199 = find_column_by_code(selection_df, '171114281199')
        sum_exclusion_7 = 0
        if col_171114201199:
            sum_exclusion_7 += safe_numeric(selection_df[col_171114201199])
        if col_171114241199:
            sum_exclusion_7 += safe_numeric(selection_df[col_171114241199])
        if col_171114281199:
            sum_exclusion_7 += safe_numeric(selection_df[col_171114281199])
        selection_df['exclusion_7_171114201199_171114241199_171114281199'] = sum_exclusion_7 >= 50
        
        # Exclusion 8: Sum of (171025141199, 171114141199) >= 0.5
        col_171025141199 = find_column_by_code(selection_df, '171025141199')
        col_171114141199 = find_column_by_code(selection_df, '171114141199')
        sum_exclusion_8 = 0
        if col_171025141199:
            sum_exclusion_8 += safe_numeric(selection_df[col_171025141199])
        if col_171114141199:
            sum_exclusion_8 += safe_numeric(selection_df[col_171114141199])
        selection_df['exclusion_8_171025141199_171114141199'] = sum_exclusion_8 >= 50
        
        # Exclusion 9: 171611102999 > 0 OR 211010122999 > 0
        col_171611102999 = find_column_by_code(selection_df, '171611102999')
        col_211010122999 = find_column_by_code(selection_df, '211010122999')
        exclusion_9 = False
        if col_171611102999:
            exclusion_9 = exclusion_9 | (safe_numeric(selection_df[col_171611102999]) > 0)
        if col_211010122999:
            exclusion_9 = exclusion_9 | (safe_numeric(selection_df[col_211010122999]) > 0)
        selection_df['exclusion_9_171611102999_211010122999'] = exclusion_9
        
        # Exclusion 10: 171613102999 > 0 OR 211010122999 > 0
        col_171613102999 = find_column_by_code(selection_df, '171613102999')
        # col_211010122999 already defined above
        exclusion_10 = False
        if col_171613102999:
            exclusion_10 = exclusion_10 | (safe_numeric(selection_df[col_171613102999]) > 0)
        if col_211010122999:
            exclusion_10 = exclusion_10 | (safe_numeric(selection_df[col_211010122999]) > 0)
        selection_df['exclusion_10_171613102999_211010122999'] = exclusion_10
        
        # General Exclusion: Any exclusion criteria met
        selection_df['general_exclusion'] = (
            selection_df['exclusion_1_231112111799'] |
            selection_df['exclusion_2_172911112999_172915112999'] |
            selection_df['exclusion_3_171025111199'] |
            selection_df['exclusion_4_173012171899'] |
            selection_df['exclusion_5_173211112999'] |
            selection_df['exclusion_6_171114221199_171114261199_171114301199'] |
            selection_df['exclusion_7_171114201199_171114241199_171114281199'] |
            selection_df['exclusion_8_171025141199_171114141199'] |
            selection_df['exclusion_9_171611102999_211010122999'] |
            selection_df['exclusion_10_171613102999_211010122999']
        )
        
        logger.info(f"Exclusion criteria applied. {selection_df['general_exclusion'].sum()} companies excluded.")
        
        # Select top companies from each universe based on FFMC
        top_n = 20 

        # Calculate the target market cap per company (equal weighting across all 20 companies)
        target_mcap_per_company = index_mcap / top_n
        selection_df['Unrounded NOSH'] = target_mcap_per_company / (selection_df['Close Prc_EOD'] * selection_df['FX/Index Ccy'])
        selection_df['Rounded NOSH'] = selection_df['Unrounded NOSH'].round()
        selection_df['Free Float'] = 1
        selection_df['Effective Date of Review'] = effective_date
        selection_df['Unrounded NOSH'] = target_mcap_per_company / selection_df['Close Prc_EOD']
        
        # Prepare EHNI DataFrame
        EHNI_df = (
            selection_df[
                ['Company', 'ISIN code', 'MIC', 'Rounded NOSH', 'Free Float', 'Capping Factor', 
                'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'Rounded NOSH': 'Number of Shares'})
            .rename(columns={'ISIN code': 'ISIN Code'})
            .sort_values('Company')
        )

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
            ehni_path = os.path.join(output_dir, f'EHNI_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving EHNI output to: {ehni_path}")
            with pd.ExcelWriter(ehni_path) as writer:
                EHNI_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"ehni_path": ehni_path}
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