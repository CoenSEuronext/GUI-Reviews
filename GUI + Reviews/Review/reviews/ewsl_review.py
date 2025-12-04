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
from utils.capping_proportional import apply_proportional_capping

logger = setup_logging(__name__)

def run_ewsl_review(date, effective_date, co_date, index="EWSL", isin="NLIX00008689", 
                    area="EU", area2="US", type="STOCK", universe="edwpt", 
                    feed="Reuters", currency="EUR", year=None, max_individual_weight=0.1, max_iterations=100):
    """
    Run the EWSL index review calculation

    Args:
        date (str): Calculation date in format YYYYMMDD
        effective_date (str): Effective date in format DD-MMM-YY
        co_date (str): Close-out date
        index (str, optional): Index name. Defaults to "EWSL"
        isin (str, optional): ISIN code. Defaults to "NLIX00008689"
        area (str, optional): Primary area. Defaults to "EU"
        area2 (str, optional): Secondary area. Defaults to None
        type (str, optional): Type of instrument. Defaults to "STOCK"
        universe (str, optional): Universe name. Defaults to "edwpt"
        feed (str, optional): Feed source. Defaults to "Reuters"
        currency (str, optional): Currency code. Defaults to "EUR"
        year (str, optional): Year for calculation. Defaults to None (extracted from date)
        max_individual_weight (float, optional): Maximum individual stock weight. Defaults to 0.1 (10%)
        max_iterations (int, optional): Maximum capping iterations. Defaults to 100

    Returns:
        dict: Result dictionary containing status, message, and data
    """
    
    try:
        logger.info("Starting EWSL review calculation")
        # If year is not provided, get it from the date
        if year is None:
            year = str(datetime.strptime(date, '%Y%m%d').year)

        # Set data folder for current month
        current_data_folder = os.path.join(DATA_FOLDER2, date[:6])

        index_eod_df, stock_eod_df, stock_co_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)

        ref_data = load_reference_data(
            current_data_folder, 
            required_files=['ff', universe],
            universe_name=universe
        )

        # Get the DataFrames from ref_data
        ff_df = ref_data.get('ff')
        full_universe_df = ref_data.get(universe)

        # Add validation
        if ff_df is None or full_universe_df is None:
            raise ValueError("Failed to load required reference data files")
        
        
        # Column mapping dictionary - include all necessary columns
        column_mapping = {
            'ISIN Code': 'ISIN',
            'Company': 'Name',
            'MIC': 'MIC',
            'Number of Shares': 'NOSH',
            'Currency': 'Currency (Local)',
            'Price (Eur) ': 'Price (EUR) ',
        }     
        
        # Create universe_df with selected and renamed columns
        universe_df = full_universe_df[list(column_mapping.values())].copy()
        universe_df = universe_df.rename(columns={v: k for k, v in column_mapping.items()})
        
        universe_df['Effective Date of Review'] = effective_date
        
        # Filter symbols once - keep only symbols with length < 12
        # Note: stock_eod_df uses 'Isin Code' (capital I, lowercase sin)
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')
        
        # Rename to match universe_df convention
        symbols_filtered = symbols_filtered.rename(columns={'Isin Code': 'ISIN Code'})
        
        # Add #Symbol, FX/Index Ccy, EOD prices, and CO prices
        universe_df = (universe_df
            # Merge symbols
            .merge(
                symbols_filtered,
                on='ISIN Code',
                how='left'
            )
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
                left_on='ISIN Code',
                right_on='ISIN Code:',
                how='left'
            )
            .drop('ISIN Code:', axis=1)
            .rename(columns={'Free Float Round:': 'Free Float'})
        )
        
        # Create MIC to Country mapping
        mic_to_country = {
            "XNGS": "United States",
            "XOSL": "Norway",
            "XLON": "Great Britain",
            "XNYS": "United States",
            "XTSE": "Canada",
            "XTKS": "Japan",
            "XNMS": "United States",
            "XASX": "Australia",
            "MTAA": "Italy",
            "XHKG": "Hong Kong",
            "XSTO": "Sweden",
            "XAMS": "Netherlands",
            "XBRU": "Belgium",
            "XSWX": "Switzerland",
            "XPAR": "France",
            "XMAD": "Spain",
            "XTAE": "Israel",
            "XETR": "Germany",
            "FSME": "Finland",
            "XMSM": "Ireland",
            "XSES": "Singapore",
            "XNZE": "New Zealand",
            "XHEL": "Finland",
            "XCSE": "Denmark",
            "XLIS": "Portugal",
            "WBAH": "Austria",
            "XNCM": "United States",
            "BATS": "United States",
            "SSME": "Sweden",
            "MERK": "Norway",
            "XESM": "Ireland",
            "XNGM": "Sweden",
            "ALXP": "France",
            "XASE": "United States"
        }
        
        # Add Country column based on MIC
        universe_df['Country'] = universe_df['MIC'].map(mic_to_country)
        
        # Log any unmapped MICs
        unmapped_mics = universe_df[universe_df['Country'].isna()]['MIC'].unique()
        if len(unmapped_mics) > 0:
            logger.warning(f"Unmapped MICs found: {unmapped_mics}")
        
        # Flag existing companies in the EWSL index
        # Look up ISIN codes from stock_eod_df where the Index column matches 'EWSL'
        # Note: stock_eod_df uses 'Isin Code' (capital I, lowercase sin)
        existing_isins = stock_eod_df[stock_eod_df['Index'] == index]['Isin Code'].unique()
        universe_df['Is_Existing'] = universe_df['ISIN Code'].isin(existing_isins).astype(int)
        logger.info(f"Found {universe_df['Is_Existing'].sum()} existing companies in {index}")
        
        # Calculate market cap
        universe_df['Mcap in EUR'] = universe_df['FX/Index Ccy'] * universe_df['Number of Shares'] * universe_df['Price (Eur) '] * universe_df['Free Float']
        
        # Sort by Country and then by Mcap descending within each country
        universe_df = universe_df.sort_values(['Country', 'Mcap in EUR'], ascending=[True, False])
        
        # Calculate country-level statistics
        universe_df['Country_Total_FFMC'] = universe_df.groupby('Country')['Mcap in EUR'].transform('sum')
        universe_df['Country_Cumulative_FFMC'] = universe_df.groupby('Country')['Mcap in EUR'].cumsum()
        universe_df['Country_Cumulative_Percentage'] = (universe_df['Country_Cumulative_FFMC'] / universe_df['Country_Total_FFMC']) * 100
        
        # Apply eligibility criteria per country
        # Existing companies: cumulative percentile >= 83%
        # New companies: cumulative percentile >= 87%
        universe_df['EWSL_selection'] = 0
        
        existing_mask = universe_df['Is_Existing'] == 1
        new_mask = universe_df['Is_Existing'] == 0
        
        universe_df.loc[existing_mask & (universe_df['Country_Cumulative_Percentage'] >= 83), 'EWSL_selection'] = 1
        universe_df.loc[new_mask & (universe_df['Country_Cumulative_Percentage'] >= 87), 'EWSL_selection'] = 1

        # Print overall summary statistics
        logger.info(f"\nOverall EWSL Selection Summary:")
        logger.info(f"Total companies selected: {universe_df['EWSL_selection'].sum()}")
        logger.info(f"  - Existing companies selected: {universe_df[existing_mask & (universe_df['EWSL_selection'] == 1)].shape[0]}")
        logger.info(f"  - New companies selected: {universe_df[new_mask & (universe_df['EWSL_selection'] == 1)].shape[0]}")
        logger.info(f"Total FFMC covered: {universe_df[universe_df['EWSL_selection'] == 1]['Mcap in EUR'].sum() / universe_df['Mcap in EUR'].sum() * 100:.2f}%")

        # ============================================================================
        # PROPORTIONAL CAPPING: Apply individual cap using utility function
        # ============================================================================
        logger.info("\nApplying proportional capping")
        
        # Filter to only selected companies
        selected_df = universe_df[universe_df['EWSL_selection'] == 1].copy()
        
        # Apply capping using utility function
        capped_df = apply_proportional_capping(
            selected_df,
            mcap_column='Mcap in EUR',
            max_weight=max_individual_weight,
            max_iterations=max_iterations
        )
        
        # Update universe_df with capping results
        capping_columns = ['Initial Weight', 'Current Weight', 'Capping Factor', 'Is Capped']
        for col in capping_columns:
            universe_df.loc[universe_df['EWSL_selection'] == 1, col] = capped_df[col].values
        
        # For non-selected companies, set default values
        universe_df.loc[universe_df['EWSL_selection'] == 0, 'Capping Factor'] = 1.0
        universe_df.loc[universe_df['EWSL_selection'] == 0, 'Is Capped'] = False
        
        # Get capping summary for reporting
        capped_companies = capped_df['Is Capped'].sum()

        # Create composition DataFrame - now including Symbol, FX, and prices
        selected_columns = ['Company', 'ISIN Code', 'MIC', 'Number of Shares', 'Free Float', 
                           'Capping Factor', 'Effective Date of Review', 'Currency']
        
        composition_df = universe_df[universe_df['EWSL_selection'] == 1][selected_columns].copy()
        composition_df = composition_df.sort_values('Company', ascending=True)

        # Analyze inclusions and exclusions
        selection_df = universe_df[universe_df['EWSL_selection'] == 1].copy()
        
        analysis_results = inclusion_exclusion_analysis(
            selection_df, 
            stock_eod_df, 
            index, 
            isin_column='ISIN Code'
        )
        
        # Extract DataFrames
        inclusions_df = analysis_results['inclusion_df'].copy()
        exclusions_df = analysis_results['exclusion_df'].copy()
        
        # Add Change Type column
        inclusions_df['Change Type'] = 'Inclusion'
        exclusions_df['Change Type'] = 'Exclusion'
        
        # Rename ISIN column if needed
        if 'ISIN code' in exclusions_df.columns:
            exclusions_df = exclusions_df.rename(columns={'ISIN code': 'ISIN Code'})

        # Log changes summary
        logger.info(f"\nEWSL Changes Summary:")
        logger.info(f"Inclusions: {len(inclusions_df)}")
        logger.info(f"Exclusions: {len(exclusions_df)}")

        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Path for the main Excel file with all sheets
            ewsl_path = os.path.join(output_dir, f'EWSL_Review_{timestamp}.xlsx')
            
            logger.info(f"Saving output to: {ewsl_path}")
            
            # Create the Excel file with all sheets
            with pd.ExcelWriter(ewsl_path) as writer:
                # Write composition sheet
                composition_df.to_excel(writer, sheet_name='EWSL Composition', index=False)

                # Write changes sheets (always write, even if empty)
                inclusions_df_sorted = inclusions_df.sort_values('Company', ascending=True) if not inclusions_df.empty else inclusions_df
                inclusions_df_sorted.to_excel(writer, sheet_name='EWSL Inclusions', index=False)

                exclusions_df_sorted = exclusions_df.sort_values('Company', ascending=True) if not exclusions_df.empty else exclusions_df
                exclusions_df_sorted.to_excel(writer, sheet_name='EWSL Exclusions', index=False)

                # Write full universe sheet
                universe_df.to_excel(writer, sheet_name='Full Universe', index=False)
            
            # Verify file was saved
            if os.path.exists(ewsl_path):
                logger.info(f"File successfully saved to: {ewsl_path}")
                
                return {
                    "status": "success",
                    "message": "Review completed successfully",
                    "data": {
                        "ewsl_path": ewsl_path,
                        "summary": {
                            "total_companies": int(universe_df['EWSL_selection'].sum()),
                            "existing_companies_selected": int(universe_df[existing_mask & (universe_df['EWSL_selection'] == 1)].shape[0]),
                            "new_companies_selected": int(universe_df[new_mask & (universe_df['EWSL_selection'] == 1)].shape[0]),
                            "total_ffmc_coverage": float(universe_df[universe_df['EWSL_selection'] == 1]['Mcap in EUR'].sum() / universe_df['Mcap in EUR'].sum() * 100),
                            "inclusions_count": len(inclusions_df),
                            "exclusions_count": len(exclusions_df),
                            "capped_companies": int(capped_companies)
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