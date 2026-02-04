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

def run_edwp_review(date, effective_date,co_date, index="EDWP", isin="NLIX00001577", 
                    area="US", area2="EU", type="STOCK", universe="98_universe", 
                    feed="Reuters", currency="EUR", year=None):
    """
    Run the index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        index (str, optional): Index name. Defaults to "edwp"
        isin (str, optional): ISIN code. Defaults to "NLIX00001577"
        area (str, optional): Primary area. Defaults to "US"
        area2 (str, optional): Secondary area. Defaults to "EU"
        type (str, optional): Type of instrument. Defaults to "STOCK"
        universe (str, optional): Universe name. Defaults to 85% Universe"
        feed (str, optional): Feed source. Defaults to "Reuters"
        currency (str, optional): Currency code. Defaults to "EUR"
        year (str, optional): Year for calculation. Defaults to None (extracted from date)

    Returns:
        dict: Result dictionary containing status, message, and data
    """
    
    try:
        logger.info("Starting EDWP review calculation")
        # If year is not provided, get it from the date
        if year is None:
            year = str(datetime.strptime(date, '%Y%m%d').year)

        # Set data folder for current month
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        index_eod_df, stock_eod_df, stock_co_df, fx_lookup_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)

        ref_data = load_reference_data(
            current_data_folder, 
            required_files=['ff', '98_universe'],
            universe_name=universe 
        )

        # Get the DataFrames from ref_data
        ff_df = ref_data.get('ff')
        full_universe_df = ref_data.get('98_universe')

        # Add validation
        if ff_df is None or full_universe_df is None:
            raise ValueError("Failed to load required reference data files")
        
        full_universe_df['Mcap in EUR'] = full_universe_df['fx_rate'] * full_universe_df['NOSH_final'] * full_universe_df['Price_final'] * full_universe_df['free_float']
        
        # Column mapping dictionary
        column_mapping = {
            'Ticker': 'fs_ticker',           
            'Name': 'entity_name',           
            'ISIN': 'ISIN',         
            'MIC': 'MIC_GIS',                   
            'Number of Shares': 'NOSH_final',
            'Free Float': 'free_float',            
            'Price (EUR) ': 'Price_final',  
            'Currency': 'p_currency',
            'FFMC': 'Mcap in EUR' 
        }     
        
        # Create universe_df with selected and renamed columns
        universe_df = full_universe_df[list(column_mapping.values())]
        universe_df = universe_df.rename(columns={v: k for k, v in column_mapping.items()})

        # Sort by FFMC descending
        universe_df = universe_df.sort_values('FFMC', ascending=False)
        
        
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
            'US': ['XNYS', 'XNGS', 'BATS', 'XNMS', 'XNCM', 'XASE', 'XNAS'],
            'SE': ['XSTO', 'SSME'],
            'IT': ['XMIL', 'MTAA'],
            'IE': ['XESM', 'XMSM'],
            'AU': ['XASX'],
            'AT': ['WBAH'],
            'BE': ['XBRU'],
            'CA': ['XTSE', 'NEOE'],
            'DK': ['XCSE', 'DSME'],
            'FI': ['XHEL', 'FSME'],
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
            'EDWP': ['US', 'SE', 'IT', 'IE', 'AU', 'AT', 'BE', 'CA', 'DK', 
                    'FI', 'FR', 'DE', 'ES', 'JP', 'NL', 'NZ', 'NO', 'PT', 
                    'SG', 'CH', 'UK', 'HK', 'IL'],
            'DEUP': ['NL', 'AT', 'BE', 'DK', 'DE', 'FI', 'PT', 'UK', 'ES', 'IT', 'IE', 'NO', 'FR', 'SE', 'CH'],
            'DEZP': ['AT', 'NL', 'BE', 'DE', 'FI', 'PT', 'ES', 'IT', 'IE', 'FR'],
            'DAPPR': ['HK', 'JP', 'SG', 'AU', 'NZ'],
            'DNAP': ['US', 'CA'],
            'DASP': ['HK', 'JP', 'SG'],
            'DPAP': ['NZ', 'AU'],
            'EUSP': ['US'],
            'EJPP': ['JP'],
            'ECHP': ['CH'],
            'EUKP': ['UK'],
            'CANP': ['CA']
        }

        # Function to calculate selection for a group
        def calculate_group_selection(df, group_name, countries):
            # Filter out CA and US ISINs for DEUP and DEZP only
            if group_name in ['DEUP', 'DEZP']:
                # Create a mask for non-CA/US ISINs
                isin_filter = ~df['ISIN'].str.startswith(('CA', 'US'))
                logger.info(f"{group_name}: Filtering out CA and US ISINs")
            else:
                # No filtering for other indices
                isin_filter = pd.Series(True, index=df.index)
            
            # Create mask for countries in this group AND apply ISIN filter
            group_mask = df['Country Group'].isin(countries) & isin_filter
            
            # Initialize selection column
            df[f'{group_name}_selection'] = 0
            
            # Calculate total FFMC for the group (with filters applied)
            group_total_ffmc = df.loc[group_mask, 'FFMC'].sum()
            
            if group_total_ffmc > 0:
                # Sort group data by FFMC descending
                group_df = df[group_mask].sort_values('FFMC', ascending=False).copy()
                
                logger.info(f"{group_name}: Processing {len(group_df)} companies after filtering")
                
                # Calculate group level cumulative stats
                group_df[f'{group_name}_Cumulative_FFMC'] = group_df['FFMC'].cumsum()
                group_df[f'{group_name}_Cumulative_Percentage'] = (
                    group_df[f'{group_name}_Cumulative_FFMC'] / group_total_ffmc * 100
                )
                
                # Add group percentage to main DataFrame using the group_df indices
                df.loc[group_df.index, f'{group_name}_Group_Percentage'] = group_df[f'{group_name}_Cumulative_Percentage'].values
                
                # Select companies up to 85% plus first one exceeding
                exceeds_85_group = group_df[f'{group_name}_Cumulative_Percentage'] > 85
                first_exceed_group = exceeds_85_group & ~exceeds_85_group.shift(1, fill_value=False)
                group_selection = (group_df[f'{group_name}_Cumulative_Percentage'] <= 85) | first_exceed_group
                
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
                            
                            # Add country percentage to main DataFrame using values
                            df.loc[country_df.index, f'{group_name}_Country_Percentage'] = country_df['Country_Cumulative_Percentage'].values
                            
                            # Select companies up to 85% plus first one exceeding
                            exceeds_85_country = country_df['Country_Cumulative_Percentage'] > 85
                            first_exceed_country = exceeds_85_country & ~exceeds_85_country.shift(1, fill_value=False)
                            country_selection = (country_df['Country_Cumulative_Percentage'] <= 85) | first_exceed_country
                            
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
        logger.info(f"Total companies selected: {universe_df['EDWP_selection'].sum()}")
        logger.info(f"Total FFMC covered: {universe_df[universe_df['EDWP_selection'] == 1]['FFMC'].sum() / universe_df['FFMC'].sum() * 100:.2f}%")

        # Create MIC-level summary
        mic_summary = universe_df.groupby('MIC').agg({
            'ISIN': 'count',
            'FFMC': 'sum',
            'EDWP_selection': 'sum'
        }).round(2)

        logger.info("\nMIC-level Summary:")
        logger.info(mic_summary)
        
        # Create DataFrames for all country groups
        all_dfs = {}
        selected_columns = ['Name', 'ISIN', 'MIC', 'Number of Shares', 'Free Float', 'Final Capping', 
                        'Effective Date of Review', 'Currency']

        for group_name in country_groups.keys():
            # Get selected companies and SORT ALPHABETICALLY BY NAME
            all_dfs[group_name] = universe_df[universe_df[f'{group_name}_selection'] == 1][selected_columns].copy()
            all_dfs[group_name] = all_dfs[group_name].sort_values('Name', ascending=True)


        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            edwp_path = os.path.join(output_dir, f'EDWP_df_{timestamp}.xlsx')
            
            logger.info(f"Saving output to: {edwp_path}")
            
            # Comment out this block to skip creating the main Excel file
            
            with pd.ExcelWriter(edwp_path) as writer:
                # Write each group's DataFrame to separate sheets
                for group_name in country_groups.keys():
                    # Get selected companies for this index
                    group_df = all_dfs[group_name].copy()
                    group_df = group_df.rename(columns={
                        'Name': 'Company',
                        'ISIN': 'ISIN Code'
                    })
                    group_df.to_excel(writer, sheet_name=f'{group_name} Composition', index=False)
                    
                    # Get current constituents for this specific index
                    current_index_df = stock_eod_df[
                        (stock_eod_df['Index'] == group_name)
                    ].copy()
                    
                    # Run inclusion/exclusion analysis for this specific index
                    analysis_results = inclusion_exclusion_analysis(
                        group_df,                # New selected companies
                        stock_eod_df,        # Current index constituents
                        group_name,
                        isin_column='ISIN Code'
                    )
                    
                    # Get results and write to sheets (SORTED ALPHABETICALLY BY COMPANY)
                    inclusions_df = analysis_results['inclusion_df']
                    if not inclusions_df.empty:
                        inclusions_df = inclusions_df.sort_values('Company', ascending=True)
                        inclusions_df.to_excel(writer, sheet_name=f'{group_name} Inclusions', index=False)
                        
                    exclusions_df = analysis_results['exclusion_df']
                    if not exclusions_df.empty:
                        exclusions_df = exclusions_df.sort_values('Company', ascending=True)
                        exclusions_df.to_excel(writer, sheet_name=f'{group_name} Exclusions', index=False)
                    
                    # Log changes summary
                    logger.info(f"\n{group_name} Changes Summary:")
                    logger.info(f"Inclusions: {len(inclusions_df)}")
                    logger.info(f"Exclusions: {len(exclusions_df)}")
                
                # Write full universe sheet
                universe_df.to_excel(writer, sheet_name='Full Universe', index=False)
            
            
            # Now create individual Excel files for each index with just the composition sheet
            individual_files = []
            for group_name, df in all_dfs.items():
                individual_path = os.path.join(output_dir, f'{group_name}_Composition_{timestamp}.xlsx')
                logger.info(f"Saving individual composition file for {group_name} to: {individual_path}")
                
                # Create a copy of the DataFrame with the renamed columns
                group_df = df.copy()
                group_df = group_df.rename(columns={
                    'Name': 'Company',
                    'ISIN': 'ISIN Code'
                })
                
                # Sort alphabetically by Company name
                group_df = group_df.sort_values('Company', ascending=True)
                
                # Create Excel file with just the composition sheet
                with pd.ExcelWriter(individual_path) as writer:
                    group_df.to_excel(writer, sheet_name=f'{group_name} Composition', index=False)
                
                individual_files.append(individual_path)
            
            # Verify files were saved (updated to check only individual files)
            if individual_files and all(os.path.exists(f) for f in individual_files):
                logger.info(f"Individual composition files saved: {len(individual_files)}")
                
                return {
                    "status": "success",
                    "message": "Review completed successfully - Individual files created",
                    "data": {
                        "individual_files": individual_files,
                        "summary": {
                            "total_companies": int(universe_df['EDWP_selection'].sum()),
                            "total_ffmc_coverage": float(universe_df[universe_df['EDWP_selection'] == 1]['FFMC'].sum() / universe_df['FFMC'].sum() * 100)
                        }
                    }
                }
            else:
                error_msg = "Individual files were not saved successfully"
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