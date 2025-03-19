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

def run_bnew_review(date, co_date, effective_date, index="BNEW", isin="NL0011376116", 
                    area="US", area2="EU", type="STOCK", universe="Developed Market", 
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
            ['ff', 'aex_bel', 'master_report']
        )
        
        # Validate and prepare reference DataFrames
        ff_df = ref_data['ff'].drop_duplicates(subset=['ISIN Code:'], keep='first')
        master_report_df = ref_data['master_report']
        aex_bel_df = ref_data['aex_bel'].drop(columns=['Effective date of review']).rename(
            columns={
                'ISIN code': 'ISIN', 
                'Preliminary number of shares': 'NOSH'
            }
        )
        
        # Validate data loading
        if any(df is None for df in [ff_df, aex_bel_df]):
            raise ValueError("Failed to load one or more required reference data files")
        
        # Define markets and filter stock data
        markets = ['XAMS', 'XPAR', 'ALXP', 'XBRU', 'ALXB', 'XLIS', 'ALXL', 'XMSM', 'XESM']
        stock_eod_filtered = stock_eod_df[
            (stock_eod_df['#Symbol'].str.len() == 12) & 
            (stock_eod_df['MIC'].isin(markets))
        ][['Isin Code', 'MIC', '#Symbol']].drop_duplicates(subset=['Isin Code', 'MIC'], keep='first')
        
        # Perform multi-stage merging with reduced redundancy
        aex_bel_df = (aex_bel_df
            # Merge filtered symbols
            .merge(stock_eod_filtered, left_on=['ISIN'], right_on=['Isin Code'], how='left')
            .drop('Isin Code', axis=1)
            
            # Merge Close Prices
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
            
            # Merge Currency Data
            .merge(
                stock_eod_df[['Isin Code', 'MIC', 'Currency', 'FX/Index Ccy']]
                .drop_duplicates(subset=['Isin Code', 'MIC'], keep='first'),
                left_on=['ISIN', 'MIC'],
                right_on=['Isin Code', 'MIC'],
                how='left'
            )
            
            # Merge Turnover Data
            .merge(
                master_report_df[['ISIN', 'MIC of MoR', 'Turnover in euro (Total)']],
                left_on=['ISIN', 'MIC'],
                right_on=['ISIN', 'MIC of MoR'],
                how='left',
                suffixes=('', '_MoR')
            )
            
            # Merge Free Float Data
            .merge(
                ff_df[['ISIN Code:', 'Free Float Round:']],
                left_on='ISIN',
                right_on='ISIN Code:',
                how='left'
            )
            .drop('ISIN Code:', axis=1)
            .rename(columns={'Free Float Round:': 'Free Float'})
        )
        
        # Calculate Price in Index Currency
        aex_bel_df['Price in index currency'] = aex_bel_df['Close Prc_EOD'] * (
            aex_bel_df['FX/Index Ccy'] if 'FX/Index Ccy' in aex_bel_df.columns else 1.0
        )
        
        # Ranking and Inclusion Logic
        aex_bel_df['Effective Date of Review'] = effective_date
        aex_bel_df['Rank'] = aex_bel_df['Turnover in euro (Total)'].rank(ascending=False, method='first')
        aex_bel_df['Inclusion'] = aex_bel_df['Rank'] <= 40
        
        # Calculate Number of Shares
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        new_mcap_per_share = index_mcap / 40
        
        aex_bel_df['unrounded nosh'] = new_mcap_per_share / aex_bel_df['Price in index currency']
        aex_bel_df['final nosh'] = aex_bel_df['unrounded nosh'].round()
        
        # Prepare Selection DataFrame
        selection_df = aex_bel_df[aex_bel_df['Inclusion']].copy()
        selection_df['Capping Factor'] = 1
        selection_df['Free Float'] = 1
        
        # Prepare BNEW DataFrame
        BNEW_df = selection_df[
            ['Company', 'ISIN', 'MIC', 'final nosh', 'Free Float', 'Capping Factor', 
             'Effective Date of Review', 'Currency']
        ].rename(columns={
            'final nosh': 'Number of Shares',
            'ISIN': 'ISIN Code'
        }).sort_values('Company')

        analysis_results = inclusion_exclusion_analysis(
            selection_df, 
            stock_eod_df, 
            index, 
            isin_column='ISIN'
        )

        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']
        # Save output files
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            # Create filename with timestamp to avoid conflicts
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            bnew_path = os.path.join(output_dir, f'BNEW_df_{timestamp}.xlsx')
            
            # Save with timestamp to avoid any conflicts
            logger.info(f"Saving BNEW output to: {bnew_path}")
            with pd.ExcelWriter(bnew_path) as writer:
                    # Write each DataFrame to a different sheet
                    BNEW_df.to_excel(writer, sheet_name='Index Composition', index=False)
                    inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                    exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                    selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                    index_mcap_df = pd.DataFrame({'Index Market Cap': [index_mcap]})
                    index_mcap_df.to_excel(writer, sheet_name='Index Market Cap', index=False)

                
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "bnew_path": bnew_path
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