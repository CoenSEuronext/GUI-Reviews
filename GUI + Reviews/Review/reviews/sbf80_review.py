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
        index_eod_df, stock_eod_df, stock_co_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)

        logger.info("Loading reference data...")
        ref_data = load_reference_data(
            current_data_folder,
            ['cac_family', 'ff', 'master_report'],
            sheet_names={'cac_family': 'PX4'}
        )
        
        selection_df = ref_data['cac_family']
        master_report_df = ref_data['master_report']

        # Get SBF80 constituents and check for missing companies
        sbf80_constituents = stock_eod_df[stock_eod_df['Index'] == 'SBF80']['Isin Code'].unique()

        # Now proceed with missing companies check
        missing_isins = set(sbf80_constituents) - set(selection_df['ISIN code'])

        if missing_isins:
            logger.info(f"Found {len(missing_isins)} companies in SBF80 not in selection_df")
            # ... rest of the missing companies code ...

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() == 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        # Chain all data preparation operations
        selection_df = (ref_data['cac_family']
           # Initial renaming
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
            .merge(
                ref_data['ff'][['ISIN Code:', 'Free Float Round:']].drop_duplicates(subset=['ISIN Code:'], keep='first'),
                left_on='ISIN code',
                right_on='ISIN Code:',
                how='left'
            )
            .drop('ISIN Code:', axis=1)
            .rename(columns={'Free Float Round:': 'Free Float', 'Name': 'Company'})
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
                stock_eod_df[['#Symbol', 'FX/Index Ccy']].drop_duplicates(subset='#Symbol', keep='first'),
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
        )

        # Handle missing SBF80 companies
        if missing_isins:
            logger.info(f"Found {len(missing_isins)} companies in SBF80 not in selection_df")
            
            # Get details of missing companies with all required information
            missing_companies = (
                stock_eod_df[
                    (stock_eod_df['Index'] == 'SBF80') & 
                    (stock_eod_df['Isin Code'].isin(missing_isins))
                ]
                .merge(
                    master_report_df[['ISIN', 'MIC of MoR', 'Number of issued shares']],
                    left_on=['Isin Code', 'MIC'],
                    right_on=['ISIN', 'MIC of MoR'],
                    how='left'
                )
                .merge(
                    ref_data['ff'][['ISIN Code:', 'Free Float Round:']],
                    left_on='Isin Code',
                    right_on='ISIN Code:',
                    how='left'
                )
                .merge(
                    stock_co_df[['#Symbol', 'Close Prc']],
                    on='#Symbol',
                    how='left',
                    suffixes=('_EOD', '_CO')
                )
            ).drop_duplicates(subset=['Isin Code'])
            
            # Create DataFrame for appending with all required columns
            append_df = pd.DataFrame({
                'ISIN code': missing_companies['Isin Code'],
                'Company': missing_companies['Name'],
                'MIC': missing_companies['MIC'],
                'Number of Shares': missing_companies['Number of issued shares'],
                'Free Float': missing_companies['Free Float Round:'],
                '#Symbol': missing_companies['#Symbol'],
                'FX/Index Ccy': missing_companies['FX/Index Ccy'],
                'Close Prc_EOD': missing_companies['Close Prc_EOD'],
                'Close Prc_CO': missing_companies['Close Prc_CO'],
                'Current SBF80': True
            })
            
            # Calculate FFMC CO for new rows
            append_df['FFMC CO'] = (
                append_df['Number of Shares'] * 
                append_df['Free Float'] * 
                append_df['Close Prc_CO']
            )
            
            # Append to selection_df
            selection_df = pd.concat([selection_df, append_df], ignore_index=True)
            
            # Log the appended companies
            for _, row in missing_companies.iterrows():
                logger.info(f"Added missing company: {row['Name']} ({row['Isin Code']})")

        # Calculate FFMC CO and rankings
        selection_df['FFMC CO'] = (
            selection_df['Number of Shares'] * 
            selection_df['Free Float'] * 
            selection_df['Close Prc_CO']
        )
        selection_df['Rank'] = selection_df['FFMC CO'].rank(method='first', ascending=False)
        selection_df['Current SBF80'] = selection_df['ISIN code'].isin(sbf80_constituents)
        # First select top 75 automatically
        selection_df['Selected'] = selection_df['Rank'] <= 75

        # Handle buffer zone (ranks 76-85)
        buffer_companies = selection_df[
            (selection_df['Rank'] >= 76) & 
            (selection_df['Rank'] <= 85)
        ].copy()

        # Sort buffer companies by rank and current constituent status
        buffer_companies = buffer_companies.sort_values(
            ['Current SBF80', 'Rank'],
            ascending=[False, True]
        )

        # Calculate how many more companies we need to reach 80
        needed_from_buffer = 80 - selection_df['Selected'].sum()

        if needed_from_buffer > 0:
            # Select the required number of companies from buffer zone
            selected_from_buffer = buffer_companies.head(needed_from_buffer)
            
            # Mark these companies as selected
            selection_df.loc[
                selection_df['ISIN code'].isin(selected_from_buffer['ISIN code']),
                'Selected'
            ] = True

        logger.info(f"Total companies selected: {selection_df['Selected'].sum()}")
        logger.info("Buffer zone selections:")
        for _, row in buffer_companies.iterrows():
            status = "Selected" if row['ISIN code'] in selected_from_buffer['ISIN code'].values else "Not Selected"
            logger.info(f"Rank {row['Rank']}: {row['Company']} - Current Constituent: {row['Current SBF80']} - {status}")

        # Find index market cap
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        selection_df['Unrounded NOSH'] = (index_mcap / 80)/ selection_df['Close Prc_EOD']
        selection_df['Rounded NOSH'] = selection_df['Unrounded NOSH'].round()
        # Add Effective Date and Currency
        selection_df['Effective Date of Review'] = effective_date
        selection_df['Currency'] = currency
        selection_df['Capping Factor'] = 1
        selection_df['Free Float'] = 1
        # Prepare SBF80 DataFrame
        SBF80_df = (
            selection_df[selection_df['Selected']][
                ['Company', 'ISIN code', 'MIC', 'Rounded NOSH', 'Free Float', 'Capping Factor', 
                'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'Rounded NOSH': 'Number of Shares'})
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
                "data": {"sbf80_path": sbf80_path}
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