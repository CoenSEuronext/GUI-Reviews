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

def run_ses5p_review(date, co_date, effective_date, index="SES5P", isin="NL0015000EF0", 
                    area="US", area2="EU", type="STOCK", universe="Eurozone 300", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "SES5P"
        isin (str, optional): ISIN code. Defaults to "NL0015000EF0"
        area (str, optional): Primary area. Defaults to "US"
        area2 (str, optional): Secondary area. Defaults to "EU"
        type (str, optional): Type of instrument. Defaults to "STOCK"
        universe (str, optional): Universe name. Defaults to "Eurozone 300"
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
        ref_data = load_reference_data(
            current_data_folder,
            ['ff', 'icb', 'eurozone_300']
        )
        
        # Extract the needed DataFrames
        ff_df = ref_data['ff']
        selection_df = ref_data['eurozone_300']
        icb_df = ref_data['icb']
                
        if any(df is None for df in [ff_df, selection_df, icb_df]):
            raise ValueError("Failed to load one or more required reference data files")

        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        ff_df = ff_df.drop_duplicates(subset=['ISIN Code:'], keep='first')
        
        # Add Free Float data
        selection_df = selection_df.merge(
            ff_df[['ISIN Code:', 'Free Float Round:']],
            left_on='ISIN',
            right_on='ISIN Code:',
            how='left'
        ).drop('ISIN Code:', axis=1).rename(columns={'Free Float Round:': 'Free Float'})
        
        # Merge with the filtered symbols
        selection_df = selection_df.merge(
            symbols_filtered,
            left_on='ISIN',
            right_on='Isin Code', 
            how='left'
        ).drop('Isin Code', axis=1)

        selection_df = selection_df.merge(
            stock_eod_df[['#Symbol', 'FX/Index Ccy']].drop_duplicates(subset='#Symbol', keep='first'),
            left_on='#Symbol', 
            right_on='#Symbol', 
            how='left'
        )

        selection_df = selection_df.merge(
            stock_eod_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
            left_on='#Symbol', 
            right_on='#Symbol', 
            how='left',
            suffixes=('', '_EOD')
        )

        # Merge CO Close Price
        selection_df = selection_df.merge(
            stock_co_df[['#Symbol', 'Close Prc']].drop_duplicates(subset='#Symbol', keep='first'),
            left_on='#Symbol', 
            right_on='#Symbol', 
            how='left',
            suffixes=('_EOD', '_CO')
        )
        
        icb_codes_stock = stock_eod_df[['Isin Code', 'ICBCode']].drop_duplicates(subset=['Isin Code'], keep='first')
        icb_codes_icb = icb_df[['ISIN Code', 'Subsector Code']].drop_duplicates(subset=['ISIN Code'], keep='first')

        # Add ICB codes to selection DataFrame
        selection_df = selection_df.merge(
        icb_codes_stock,
        left_on='ISIN',
        right_on='Isin Code',
        how='left'
        ).drop('Isin Code', axis=1)
        
        selection_df = selection_df.merge(
            icb_codes_icb,
            left_on='ISIN',
            right_on='ISIN Code',
            how='left'
        ).drop('ISIN Code', axis=1)
        
        selection_df['Uni_Supersector'] = selection_df['ICBCode'].astype(str).str[:4]
        selection_df['ICB_Supersector'] = selection_df['Subsector Code'].astype(str).str[:4]
                
        selection_df['FFMC CO'] = selection_df['NOSH'] * selection_df['Free Float'] * selection_df['Close Prc_CO']
        
        eligible_supersectors = ['3030', '1510', '6510', '6010']

        # Create proper Inclusion column - Change the column name to be clear
        selection_df['Inclusion_Sector'] = (
            (selection_df['Uni_Supersector'].isin(eligible_supersectors)) | 
            (selection_df['ICB_Supersector'].isin(eligible_supersectors))
        )

        # Rank only the truly included companies
        selection_df['Rank Universe'] = selection_df.loc[selection_df['Inclusion_Sector']]['FFMC CO'].rank(ascending=False, method='first')

        # Create Final Selection column - initialize as False
        selection_df['Final Selection'] = False

        # Set True for top 50 ranked companies (only among included companies)
        selection_df.loc[
            (selection_df['Inclusion_Sector']) & 
            (selection_df['Rank Universe'] <= 50), 
            'Final Selection'
        ] = True
         
        # Find index market cap
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]
        
        selection_df['Effective Date of Review'] = effective_date

        # Save to Excel for debugging

        selection_df['Final Capping'] = 1
        SES5P_df = selection_df[selection_df['Final Selection']][
            ['Name', 'ISIN', 'MIC', 'NOSH', 
            'Free Float', 'Final Capping', 
            'Effective Date of Review', 'Currency']
        ].rename(columns={
            'Currency': 'Currency (Local)'
        })

        SES5P_df = SES5P_df.sort_values('Name')
 
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
            ses5p_path = os.path.join(output_dir, f'SES5P_df_{timestamp}.xlsx')
            
            # Save with timestamp to avoid any conflicts
            logger.info(f"Saving SES5P output to: {ses5p_path}")
            with pd.ExcelWriter(ses5p_path) as writer:
                    # Write each DataFrame to a different sheet
                    SES5P_df.to_excel(writer, sheet_name='Index Composition', index=False)
                    inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                    exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                    selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                    index_mcap_df = pd.DataFrame({'Index Market Cap': [index_mcap]})
                    index_mcap_df.to_excel(writer, sheet_name='Index Market Cap', index=False)

                
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "ses5p_path": ses5p_path
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