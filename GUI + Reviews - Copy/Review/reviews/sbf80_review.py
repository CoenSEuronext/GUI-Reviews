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

def run_sbf80_review(date, co_date, effective_date, index="SBF80", isin="FR0013017936", 
                   area="US", area2="EU", type="STOCK", universe="cac_family", 
                   feed="Reuters", currency="EUR", year=None):

    try:
        # Simplify year extraction
        year = year or str(datetime.strptime(date, '%Y%m%d').year)
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        # Load data with error handling
        logger.info("Loading EOD data...")
        index_eod_df, stock_eod_df, stock_co_df, fx_lookup_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)

        logger.info("Loading reference data...")
        ref_data = load_reference_data(
            current_data_folder,
            ['cac_family', 'ff', 'master_report'],
            sheet_names={'cac_family': 'PX4'}
        )
        
        selection_df = ref_data['cac_family']
        master_report_df = ref_data['master_report']

        # Check for duplicates in initial data
        logger.info(f"Initial selection_df shape: {selection_df.shape}")
        if selection_df.index.duplicated().any():
            logger.warning(f"Found {selection_df.index.duplicated().sum()} duplicate indices in initial selection_df")
            selection_df = selection_df.reset_index(drop=True)

        # Check for duplicate ISIN codes in the initial data
        duplicate_isins = selection_df[selection_df.duplicated(subset=['ISIN code'], keep=False)]
        if not duplicate_isins.empty:
            logger.warning(f"Found {len(duplicate_isins)} rows with duplicate ISIN codes:")
            logger.warning(f"{duplicate_isins[['ISIN code', 'Name']].to_string()}")
            selection_df = selection_df.drop_duplicates(subset=['ISIN code'], keep='first')
            logger.info(f"After removing ISIN duplicates, shape: {selection_df.shape}")

        # Get SBF80 constituents and check for missing companies
        sbf80_constituents = stock_eod_df[stock_eod_df['Index'] == 'SBF80']['Isin Code'].unique()

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() == 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        # ==================== MAIN CHAIN WITH SAFE FREE FLOAT FIX ====================
        selection_df = (selection_df
            .rename(columns={
                'Preliminary Capping Factor': 'Capping Factor',
                'Effective date of review': 'Effective Date of Review'
            })
            .merge(
                master_report_df[['ISIN', 'MIC of MoR', 'Number of issued shares']],
                left_on=['ISIN code', 'MIC'],
                right_on=['ISIN', 'MIC of MoR'],
                how='left'
            )
            .drop(['ISIN', 'MIC of MoR'], axis=1)
            .rename(columns={'Number of issued shares': 'Number of Shares'})

            # === SAFE FREE FLOAT MERGE (this prevents the crash) ===
            .merge(
                ref_data['ff'][['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset=['ISIN Code:'], keep='first'),
                left_on='ISIN code',
                right_on='ISIN Code:',
                how='left'
            )
            .drop('ISIN Code:', axis=1, errors='ignore')  # safe drop
        )

        # CRITICAL FIX: Ensure 'Free Float' column exists and is properly named
        if 'Free Float Round:' in selection_df.columns:
            selection_df['Free Float'] = selection_df['Free Float Round:']
            selection_df = selection_df.drop(columns=['Free Float Round:'], errors='ignore')
        else:
            logger.warning("'Free Float Round:' column not found in ff data → setting Free Float to NaN")
            selection_df['Free Float'] = np.nan

        # Ensure 'Company' column exists
        if 'Name' in selection_df.columns:
            selection_df = selection_df.rename(columns={'Name': 'Company'})

        # Continue exactly as your original code
        selection_df = (selection_df
            .merge(
                symbols_filtered,
                left_on='ISIN code',
                right_on='Isin Code',
                how='left'
            )
            .drop('Isin Code', axis=1)
            .merge(
                stock_eod_df[['#Symbol', 'FX/Index Ccy']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left'
            )
            .merge(
                stock_eod_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left',
                suffixes=('', '_EOD')
            )
            .merge(
                stock_co_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
                on='#Symbol',
                how='left',
                suffixes=('_EOD', '_CO')
            )
        )
        # ============================================================================

        # Check for duplicates after merges (your original logic)
        logger.info(f"After merges, selection_df shape: {selection_df.shape}")
        duplicate_isins_after = selection_df[selection_df.duplicated(subset=['ISIN code'], keep=False)]
        if not duplicate_isins_after.empty:
            logger.warning(f"Found {len(duplicate_isins_after)} duplicate ISINs after merges")
            selection_df = selection_df.drop_duplicates(subset=['ISIN code'], keep='first')

        selection_df = selection_df.reset_index(drop=True)

        # Handle missing SBF80 companies (unchanged)
        missing_isins = set(sbf80_constituents) - set(selection_df['ISIN code'])
        if missing_isins:
            logger.info(f"Found {len(missing_isins)} missing SBF80 companies → adding them")
            missing_companies = (
                stock_eod_df[
                    (stock_eod_df['Index'] == 'SBF80') & 
                    (stock_eod_df['Isin Code'].isin(missing_isins))
                ]
                .merge(master_report_df[['ISIN', 'MIC of MoR', 'Number of issued shares']],
                       left_on=['Isin Code', 'MIC'], right_on=['ISIN', 'MIC of MoR'], how='left')
                .merge(ref_data['ff'][['ISIN Code:', 'Free Float Round:']],
                       left_on='Isin Code', right_on='ISIN Code:', how='left')
                .merge(stock_co_df[['#Symbol', 'Close Prc']], on='#Symbol', how='left', suffixes=('_EOD', '_CO'))
            )

            append_df = pd.DataFrame({
                'ISIN code': missing_companies['Isin Code'],
                'Company': missing_companies.get('Name', 'Unknown'),
                'MIC': missing_companies['MIC'],
                'Number of Shares': missing_companies.get('Number of issued shares', np.nan),
                'Free Float': missing_companies.get('Free Float Round:', np.nan),
                '#Symbol': missing_companies['#Symbol'],
                'FX/Index Ccy': missing_companies.get('FX/Index Ccy', np.nan),
                'Close Prc_EOD': missing_companies.get('Close Prc_EOD', np.nan),
                'Close Prc_CO': missing_companies['Close Prc'],
                'Current SBF80': True
            })

            selection_df = pd.concat([selection_df, append_df], ignore_index=True)
            logger.info(f"After adding missing companies: {selection_df.shape}")

        # === SAFE NUMERIC CONVERSION (prevents the TypeError) ===
        if 'Free Float' not in selection_df.columns:
            selection_df['Free Float'] = np.nan

        selection_df = selection_df.rename(columns={'Free Float': 'Free Float Original'})

        # Convert to numeric safely
        selection_df['Number of Shares'] = pd.to_numeric(selection_df['Number of Shares'], errors='coerce')
        selection_df['Close Prc_CO'] = pd.to_numeric(selection_df['Close Prc_CO'], errors='coerce')
        selection_df['Close Prc_EOD'] = pd.to_numeric(selection_df['Close Prc_EOD'], errors='coerce')
        selection_df['Free Float Original'] = pd.to_numeric(selection_df['Free Float Original'], errors='coerce')

        # FFMC CO calculation
        selection_df['FFMC CO'] = (
            selection_df['Number of Shares'] * 
            selection_df['Free Float Original'] * 
            selection_df['Close Prc_CO']
        )
        
        logger.info("FFMC CO calculated successfully")
        
        selection_df['Rank'] = selection_df['FFMC CO'].rank(method='first', ascending=False)
        selection_df['Current SBF80'] = selection_df['ISIN code'].isin(sbf80_constituents)
        
        # Selection logic (top 75 + buffer)
        selection_df['Selected'] = selection_df['Rank'] <= 75

        buffer_companies = selection_df[
            (selection_df['Rank'] >= 76) & (selection_df['Rank'] <= 85)
        ].copy().sort_values(['Current SBF80', 'Rank'], ascending=[False, True])

        needed_from_buffer = 80 - selection_df['Selected'].sum()
        if needed_from_buffer > 0:
            selected_from_buffer = buffer_companies.head(needed_from_buffer)
            selection_df.loc[
                selection_df['ISIN code'].isin(selected_from_buffer['ISIN code']),
                'Selected'
            ] = True

        # Index market cap
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        
        selection_df['Unrounded NOSH'] = (index_mcap / 80) / selection_df['Close Prc_EOD']
        selection_df['Rounded NOSH'] = selection_df['Unrounded NOSH'].round()
        
        selection_df['Effective Date of Review'] = effective_date
        selection_df['Currency'] = currency
        selection_df['Capping Factor'] = 1
        selection_df['Free Float'] = 1  # final output requirement

        # Final SBF80 composition
        SBF80_df = (
            selection_df[selection_df['Selected']][
                ['Company', 'ISIN code', 'MIC', 'Rounded NOSH', 'Free Float', 'Capping Factor', 
                 'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'Rounded NOSH': 'Number of Shares'})
            .rename(columns={'ISIN code': 'ISIN Code'})
            .sort_values('Company')
            .reset_index(drop=True)
        )

        # Inclusion/Exclusion
        analysis_results = inclusion_exclusion_analysis(
            selection_df, stock_eod_df, index, isin_column='ISIN code'
        )
        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']

        # === SAVE EXACTLY AS BEFORE (so your GUI opens it ===
        output_dir = os.path.join(os.getcwd(), 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sbf80_path = os.path.join(output_dir, f'SBF80_df_{timestamp}.xlsx')
        
        logger.info(f"Saving SBF80 output to: {sbf80_path}")
        with pd.ExcelWriter(sbf80_path) as writer:
            SBF80_df.to_excel(writer, sheet_name='Index Composition', index=False)
            inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
            exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
            selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
            pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

        return {
            "status": "success",
            "message": "Review completed successfully",
            "data": {"sbf80_path": sbf80_path}   # ← exactly what your other code expects
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