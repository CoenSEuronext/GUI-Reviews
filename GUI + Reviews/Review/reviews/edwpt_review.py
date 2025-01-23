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

def run_edwpt_review(date, effective_date, index="EDWPT", isin="NLIX00001932", 
                    area="US", area2="EU", type="STOCK", universe="98% Universe", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "edwpt"
        isin (str, optional): ISIN code. Defaults to "NLIX00001932"
        area (str, optional): Primary area. Defaults to "US"
        area2 (str, optional): Secondary area. Defaults to "EU"
        type (str, optional): Type of instrument. Defaults to "STOCK"
        universe (str, optional): Universe name. Defaults to "98% Universe"
        feed (str, optional): Feed source. Defaults to "Reuters"
        currency (str, optional): Currency code. Defaults to "EUR"
        year (str, optional): Year for calculation. Defaults to None (extracted from date)

    Returns:
        dict: Result dictionary containing status, message, and data
    """
    
    try:
        logger.info("Starting EDWPT review calculation")
        # If year is not provided, get it from the date
        if year is None:
            year = str(datetime.strptime(date, '%Y%m%d').year)

        # Set data folder for current month
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        index_eod_df, stock_eod_df = load_eod_data(date, area, area2, DLF_FOLDER)

        ref_data = load_reference_data(
            current_data_folder, 
            required_files=['ff', 'universe'],
            universe_name=universe  # This will be "98% Universe" by default
        )

        # Get the DataFrames from ref_data
        ff_df = ref_data.get('ff')
        full_universe_df = ref_data.get('universe')

        # Add validation
        if ff_df is None or full_universe_df is None:
            raise ValueError("Failed to load required reference data files")
        
        full_universe_df['Mcap in EUR'] = full_universe_df['fx_rate'] * full_universe_df['cutoff_nosh'] * full_universe_df['cutoff_price'] * full_universe_df['free_float']
        
        # Column mapping dictionary
        column_mapping = {
            'Ticker': 'fs_ticker',           
            'Name': 'proper_name',           
            'ISIN': 'ISIN',         
            'MIC': 'MIC_GIS',                   
            'NOSH': 'cutoff_nosh',          
            'Price (EUR) ': 'cutoff_price',  
            'Currency (Local)': 'p_currency',
            'FFMC': 'free_float_market_cap' 
        }     
        
        # Create universe_df with selected and renamed columns
        universe_df = full_universe_df[list(column_mapping.values())]
        universe_df = universe_df.rename(columns={v: k for k, v in column_mapping.items()})

        # Sort by FFMC descending
        universe_df = universe_df.sort_values('FFMC', ascending=False)
        
        # Remove duplicates from ff_df
        ff_df = ff_df.drop_duplicates(subset=['ISIN Code:'], keep='first')
        
        # Add Free Float data
        universe_df = universe_df.merge(
            ff_df[['ISIN Code:', 'Free Float Round:']],
            left_on='ISIN',
            right_on='ISIN Code:',
            how='left'
        ).drop('ISIN Code:', axis=1).rename(columns={'Free Float Round:': 'Free Float'})
        
        universe_df['Final Capping'] = 1
        universe_df['Effective Date of Review'] = effective_date
        
        # Add cumulative count
        universe_df['Cumulative Count'] = range(1, len(universe_df) + 1)

        # Calculate universe-level statistics
        universe_df['Total Universe FFMC'] = universe_df['FFMC'].sum()
        universe_df['Universe Cumulative FFMC'] = universe_df['FFMC'].cumsum()
        universe_df['Universe Cumulative Percentage'] = (universe_df['Universe Cumulative FFMC'] / universe_df['Total Universe FFMC']) * 100

        # Create MIC grouping mapping
        mic_groups = {
            'US': ['XNYS', 'XNGS', 'BATS'],
            'SE': ['XSTO', 'XNGM'],
            'IT': ['XMIL', 'MTAA'],
            'IE': ['XESM', 'XMSM'],
            'AU': ['XASX'],
            'AT': ['WBAH'],
            'BE': ['XBRU'],
            'CA': ['XTSE'],
            'DK': ['XCSE'],
            'FI': ['XHEL'],
            'FR': ['XPAR', 'ALXP'],
            'DE': ['XETR'],
            'ES': ['XMAD'],
            'JP': ['XTKS'],
            'NL': ['XAMS'],
            'NZ': ['XNZE'],
            'NO': ['XOSL', 'MERK'],
            'PT': ['XLIS'],
            'SG': ['XSES'],
            'CH': ['XSWX'],
            'UK': ['XLON'],
            'HK': ['XHKG'],
            'IL': ['XTAE'],             
        }

        # Create mapping from MIC to country
        mic_to_country = {mic: country for country, mics in mic_groups.items() for mic in mics}

        def get_country_group(mic):
            return mic_to_country.get(mic, mic)

        # Add country group column
        universe_df['Country Group'] = universe_df['MIC'].apply(get_country_group)

        # Calculate MIC-level statistics
        universe_df['MIC Rank'] = universe_df.groupby('Country Group')['FFMC'].rank(method='first', ascending=False)
        universe_df['MIC Cumulative FFMC'] = universe_df.groupby('Country Group')['FFMC'].cumsum()
        universe_df['Total MIC FFMC'] = universe_df.groupby('Country Group')['FFMC'].transform('sum')
        universe_df['MIC Cumulative Percentage'] = (universe_df['MIC Cumulative FFMC'] / universe_df['Total MIC FFMC']) * 100
        
        # Define country groups
        country_groups = {
            'EDWPT': ['US', 'SE', 'IT', 'IE', 'AU', 'AT', 'BE', 'CA', 'DK', 
                    'FI', 'FR', 'DE', 'ES', 'JP', 'NL', 'NZ', 'NO', 'PT', 
                    'SG', 'CH', 'UK', 'HK', 'IL'],
            'DEUPT': ['NL', 'AT', 'BE', 'DK', 'DE', 'FI', 'PT', 'UK', 'ES', 'IT', 'IE', 'NO', 'FR', 'SE', 'CH'],
            'DEZPT': ['AT', 'NL', 'BE', 'DE', 'FI', 'PT', 'ES', 'IT', 'IE', 'FR'],
            'DAPPT': ['HK', 'JP', 'SG', 'AU', 'NZ'],
            'DNAPT': ['US', 'CA'],
            'DASPT': ['HK', 'JP', 'SG'],
            'DPAPT': ['NZ', 'AU'],
            'EUSPT': ['US'],
            'EJPPT': ['JP'],
            'ECHPT': ['CH'],
            'EUKPT': ['UK'],
            'CANPT': ['CA']
        }

        def analyze_changes(universe_df, stock_eod_df, group_name, selected_columns):
            """
            Analyze inclusions and exclusions for a given group
            
            Args:
                universe_df: DataFrame with selected companies
                stock_eod_df: DataFrame with current stock data
                group_name: Name of the index (e.g., 'EDWPT')
                selected_columns: List of columns to include
            """
            # Get ISINs of selected companies for this group from universe_df
            selected_isins = set(universe_df[universe_df[f'{group_name}_selection'] == 1]['ISIN'].tolist())
            
            # Get current constituents from stock_eod_df for this specific index
            current_isins = set(stock_eod_df[stock_eod_df['MIC'] == group_name]['Isin Code'].unique().tolist())
            
            # Find inclusions (in selected but not in current constituents)
            inclusion_isins = selected_isins - current_isins
            
            # Find exclusions (in current constituents but not in selected)
            exclusion_isins = current_isins - selected_isins
            
            # Create DataFrame for inclusions using universe_df data
            inclusions_df = universe_df[
                universe_df['ISIN'].isin(inclusion_isins)
            ][['ISIN', 'Name']].copy()
            inclusions_df['Change Type'] = 'Inclusion'
            
            # Create DataFrame for exclusions using stock_eod_df data
            exclusions_df = stock_eod_df[
                (stock_eod_df['MIC'] == group_name) & 
                (stock_eod_df['Isin Code'].isin(exclusion_isins))
            ][['Isin Code', 'Name']].copy()
            exclusions_df = exclusions_df.rename(columns={'Isin Code': 'ISIN'})
            exclusions_df['Change Type'] = 'Exclusion'
            
            return inclusions_df, exclusions_df

        # Function to calculate selection for a group
        def calculate_group_selection(df, group_name, countries):
            # Create mask for countries in this group
            group_mask = df['Country Group'].isin(countries)
            
            # Initialize selection column
            df[f'{group_name}_selection'] = 0
            
            # Calculate total FFMC for the group
            group_total_ffmc = df.loc[group_mask, 'FFMC'].sum()
            
            if group_total_ffmc > 0:
                # Sort group data by FFMC descending
                group_df = df[group_mask].sort_values('FFMC', ascending=False).copy()
                
                # Calculate group level cumulative stats
                group_df[f'{group_name}_Cumulative_FFMC'] = group_df['FFMC'].cumsum()
                group_df[f'{group_name}_Cumulative_Percentage'] = (
                    group_df[f'{group_name}_Cumulative_FFMC'] / group_total_ffmc * 100
                )
                
                # Add group percentage to main DataFrame
                df.loc[group_mask, f'{group_name}_Group_Percentage'] = group_df[f'{group_name}_Cumulative_Percentage']
                
                # Select companies up to 98% plus first one exceeding
                exceeds_98_group = group_df[f'{group_name}_Cumulative_Percentage'] > 98
                first_exceed_group = exceeds_98_group & ~exceeds_98_group.shift(1, fill_value=False)
                group_selection = (group_df[f'{group_name}_Cumulative_Percentage'] <= 98) | first_exceed_group
                
                # Process each country in the group
                for country in countries:
                    country_mask = group_df['Country Group'] == country
                    if country_mask.any():
                        country_df = group_df[country_mask].copy()
                        country_total = country_df['FFMC'].sum()
                        
                        if country_total > 0:
                            country_df['Country_Cumulative_FFMC'] = country_df['FFMC'].cumsum()
                            country_df['Country_Cumulative_Percentage'] = (
                                country_df['Country_Cumulative_FFMC'] / country_total * 100
                            )
                            
                            # Add country percentage to main DataFrame
                            df.loc[country_df.index, f'{group_name}_Country_Percentage'] = country_df['Country_Cumulative_Percentage']
                            
                            # Select companies up to 98% plus first one exceeding
                            exceeds_98_country = country_df['Country_Cumulative_Percentage'] > 98
                            first_exceed_country = exceeds_98_country & ~exceeds_98_country.shift(1, fill_value=False)
                            country_selection = (country_df['Country_Cumulative_Percentage'] <= 98) | first_exceed_country
                            
                            # Update group selection
                            group_selection.loc[country_df.index] |= country_selection
                
                # Update main DataFrame with selections
                df.loc[group_df.index, f'{group_name}_selection'] = group_selection.astype(int)
            
            return df

        # Apply selections for all groups
        for group_name, countries in country_groups.items():
            universe_df = calculate_group_selection(universe_df, group_name, countries)
            
            # Print group summary statistics
            group_mask = universe_df['Country Group'].isin(countries)
            logger.info(f"\n{group_name} Selection Summary:")
            logger.info(f"Total companies selected: {universe_df[universe_df[f'{group_name}_selection'] == 1].shape[0]}")
            logger.info(f"Total FFMC covered: {universe_df[universe_df[f'{group_name}_selection'] == 1]['FFMC'].sum() / universe_df.loc[group_mask, 'FFMC'].sum() * 100:.2f}%")

        # Keep DataFrame sorted by FFMC descending
        universe_df = universe_df.sort_values('FFMC', ascending=False)

        # Print global summary statistics
        logger.info("\nGlobal Selection Summary:")
        logger.info(f"Total companies selected: {universe_df['EDWPT_selection'].sum()}")
        logger.info(f"Total FFMC covered: {universe_df[universe_df['EDWPT_selection'] == 1]['FFMC'].sum() / universe_df['FFMC'].sum() * 100:.2f}%")

        # Create MIC-level summary
        mic_summary = universe_df.groupby('MIC').agg({
            'ISIN': 'count',
            'FFMC': 'sum',
            'EDWPT_selection': 'sum'
        }).round(2)

        logger.info("\nMIC-level Summary:")
        logger.info(mic_summary)
        
        # Create DataFrames for all country groups
        all_dfs = {}
        selected_columns = ['Name', 'ISIN', 'MIC', 'NOSH', 'Free Float', 'Final Capping', 
                        'Effective Date of Review', 'Currency (Local)']

        for group_name in country_groups.keys():
            all_dfs[group_name] = universe_df[universe_df[f'{group_name}_selection'] == 1][selected_columns].copy()


        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            edwpt_path = os.path.join(output_dir, f'EDWPT_df_{timestamp}.xlsx')
            
            logger.info(f"Saving output to: {edwpt_path}")
            
            with pd.ExcelWriter(edwpt_path) as writer:
                # Write each group's DataFrame to separate sheets
                for group_name, df in all_dfs.items():
                    # Write composition sheet
                    df.to_excel(writer, sheet_name=f'{group_name} Composition', index=False)
                    
                    # Generate and write inclusion/exclusion analysis
                    inclusions_df, exclusions_df = analyze_changes(
                        universe_df, 
                        stock_eod_df, 
                        group_name, 
                        selected_columns
                    )
                    
                    # Write changes to separate sheets
                    if not inclusions_df.empty:
                        inclusions_df.to_excel(writer, sheet_name=f'{group_name} Inclusions', index=False)
                    if not exclusions_df.empty:
                        exclusions_df.to_excel(writer, sheet_name=f'{group_name} Exclusions', index=False)
                    
                    # Log changes summary
                    logger.info(f"\n{group_name} Changes Summary:")
                    logger.info(f"Inclusions: {len(inclusions_df)}")
                    logger.info(f"Exclusions: {len(exclusions_df)}")
                
                # Write full universe sheet
                universe_df.to_excel(writer, sheet_name='Full Universe', index=False)
            
            # Verify file was saved
            # Replace the return statement in the try block with this:
            if os.path.exists(edwpt_path):
                logger.info(f"File successfully saved to: {edwpt_path}")
                return {
                    "status": "success",
                    "message": "Review completed successfully",
                    "data": {
                        "edwpt_path": edwpt_path,
                        "summary": {
                            "total_companies": int(universe_df['EDWPT_selection'].sum()),  # Convert to standard int
                            "total_ffmc_coverage": float(universe_df[universe_df['EDWPT_selection'] == 1]['FFMC'].sum() / universe_df['FFMC'].sum() * 100)  # Convert to standard float
                        }
                    }
                }
            else:
                error_msg = "File was not saved successfully"
                logger.error(error_msg)
                return {
                    "status": "error",
                    "message": error_msg,
                    "data": None
                }
                
        except Exception as e:
            error_msg = f"Error saving output file: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return {
                "status": "error", 
                "message": error_msg,
                "traceback": traceback.format_exc(),
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