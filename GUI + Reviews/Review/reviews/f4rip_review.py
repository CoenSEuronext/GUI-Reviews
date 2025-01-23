import pandas as pd
import numpy as np
from datetime import datetime
import os
import traceback
from Review.functions import read_semicolon_csv
from config import DLF_FOLDER, DATA_FOLDER, DATA_FOLDER2
from utils.logging_utils import setup_logging
from utils.data_loader import load_eod_data, load_reference_data

logger = setup_logging(__name__)

def run_f4rip_review(date, co_date, effective_date, index="F4RIP", isin="FR0013376209", 
                    area="US", area2="EU", type="STOCK", universe="Developed Market", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "F4RIP"
        isin (str, optional): ISIN code. Defaults to "FR0013376209"
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
        ref_data = load_reference_data(
            current_data_folder,
            ['ff', 'oekom_score', 'cac_family'],
            sheet_names={'cac_family': 'PX1'}
        )
        
        # Extract the needed DataFrames
        ff_df = ref_data['ff']
        selection_df = ref_data['cac_family'].drop(columns=['Effective Date of Review'])
        oekom_score_df = ref_data['oekom_score']
        
        if any(df is None for df in [ff_df, selection_df, oekom_score_df]):
            raise ValueError("Failed to load one or more required reference data files")

        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() == 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        # Merge with the filtered symbols
        selection_df = selection_df.merge(
            symbols_filtered,
            left_on='ISIN code',
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

        
        selection_df['FFMC CO'] = selection_df['Preliminary Number of shares'] * selection_df['Preliminary Free Float'] * selection_df['Preliminary Capping Factor'] * selection_df['Close Prc_CO']
        
        selection_df['Rank Universe'] = selection_df['FFMC CO'].rank(ascending=False, method='first')
        
        selection_df = selection_df.merge(
        oekom_score_df[['ISIN', 'New Sustainability Score']],
        left_on='ISIN code',
        right_on='ISIN',
        how='left'
        ).drop('ISIN', axis=1).rename(columns={'New Sustainability Score': 'Mirova/ISS-oekom score'})
        
        selection_df = selection_df.sort_values(['Mirova/ISS-oekom score', 'FFMC CO'], ascending=[False, False])

        # Assign rank based on the sorted values of 'Mirova/ISS-oekom score' and 'FFMC CO'
        selection_df['Rank Oekom'] = selection_df['Mirova/ISS-oekom score'].rank(
            method='first', 
            ascending=False
        )
        selection_df['Weight'] = pd.cut(selection_df['Rank Oekom'], 
                        bins=[0, 10, 20, 30, 40, float('inf')],
                        labels=[0.04, 0.03, 0.02, 0.01, 0],
                        include_lowest=True).astype(float)

        # Find index market cap
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == 'FR0013376209', 'Mkt Cap'].iloc[0]
        ff_df = ff_df.drop_duplicates(subset=['ISIN Code:'], keep='first')
        selection_df['Shares'] = index_mcap * selection_df['Weight'] / selection_df['Close Prc_EOD']
        
        # Add Free Float data
        selection_df = selection_df.merge(
            ff_df[['ISIN Code:', 'Free Float Round:']],
            left_on='ISIN code',
            right_on='ISIN Code:',
            how='left'
        ).drop('ISIN Code:', axis=1).rename(columns={'Free Float Round:': 'Free Float'})
        
        selection_df['Effective Date of Review'] = effective_date



        F4RIP_df = selection_df[[
            'Company', 
            'ISIN code', 
            'MIC', 
            'Preliminary Number of shares', 
            'Preliminary Free Float',
            'Preliminary Capping Factor',
            'Effective Date of Review',
            'Currency'
        ]].copy().rename(columns={
            'Company': 'Name',
            'ISIN code': 'ISIN', 
            'Preliminary Number of shares': 'NOSH',
            'Preliminary Free Float': 'Free Float', 
            'Preliminary Capping Factor': 'Final Capping',
            'Currency': 'Currency (Local)'
        })

        F4RIP_df = F4RIP_df.sort_values('Name')

        def inclusion(selection_df, stock_eod_df, index):
            """
            Add a flag to selection_df indicating whether the company is found in stock_eod_df.
            Flag is 1 if the company (based on ISIN code) is not found in stock_eod_df,
            and 0 if it is found.
            
            Args:
                selection_df (DataFrame): DataFrame with 'ISIN code' and company details.
                stock_eod_df (DataFrame): DataFrame with 'Isin Code' and other stock data.
                index (str): The index value to filter stock_eod_df by 'Index'.
            
            Returns:
                DataFrame: selection_df with an added flag indicating presence in stock_eod_df.
            """
            # Filter stock_eod_df based on the provided index
            filtered_stock_eod_df = stock_eod_df[stock_eod_df['Index'] == index]
            
            # Create a set of ISIN codes from the filtered stock_eod_df
            isin_set = set(filtered_stock_eod_df['Isin Code'])
            
            # Check if each ISIN in selection_df is in the isin_set, and assign a flag
            selection_df['Inclusion'] = selection_df['ISIN code'].apply(lambda x: 1 if x not in isin_set else 0)
            
            return selection_df
        
        index = "F4RIP"
        selection_df = inclusion(selection_df, stock_eod_df, index)
        inclusion_df = selection_df[selection_df['Inclusion'] == 1][[
            'Company', 
            'ISIN code', 
            'MIC'
        ]].rename(columns={
            'Company': 'Name',
            'ISIN code': 'ISIN'
        }).sort_values('Name')
        
        filtered_stock_eod_df = stock_eod_df[stock_eod_df['Index'] == index]

        # Find companies in stock_eod_df that are not in selection_df
        exclusion_df = filtered_stock_eod_df[
            ~filtered_stock_eod_df['Isin Code'].isin(selection_df['ISIN code'])
        ][['Name', 'Isin Code', 'MIC']].rename(columns={
            'Isin Code': 'ISIN'
        }).sort_values('Name')
        # Save output files
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            # Create filename with timestamp to avoid conflicts
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            f4rip_path = os.path.join(output_dir, f'F4RIP_df_{timestamp}.xlsx')
            
            # Save with timestamp to avoid any conflicts
            logger.info(f"Saving F4RIP output to: {f4rip_path}")
            with pd.ExcelWriter(f4rip_path) as writer:
                    # Write each DataFrame to a different sheet
                    F4RIP_df.to_excel(writer, sheet_name='Index Composition', index=False)
                    inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                    exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                    selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                    index_mcap_df = pd.DataFrame({'Index Market Cap': [index_mcap]})
                    index_mcap_df.to_excel(writer, sheet_name='Index Market Cap', index=False)

                
            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {
                    "f4rip_path": f4rip_path
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