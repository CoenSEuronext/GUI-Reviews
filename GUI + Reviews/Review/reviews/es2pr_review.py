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

def calculate_capping_factors(df, index_mcap, max_individual_weight=0.10, max_sector_weight=0.50, max_iterations=100):
    """
    Calculate capping factors with individual (10%) and sector (50%) caps.
    Once a company is capped at 10%, it remains at 10% and doesn't receive redistributed weight.
    Final capping factors are normalized so the highest is 1.
    Returns the dataframe with capping factors and a list of iteration steps for tracking.
    """
    # Initialize
    df = df.copy()
    df['Capping Factor'] = 1.0
    df['Initial Weight'] = (df['Mcap in EUR_EOD'] / index_mcap)
    df['Is Capped'] = False  # Track which companies are already capped at 10%
    
    # Track iterations
    iterations_data = []
    company_iteration_steps = []
    
    for iteration in range(max_iterations):
        # Calculate current weights with capping factors
        df['Capped Mcap'] = df['Mcap in EUR_EOD'] * df['Capping Factor']
        total_capped_mcap = df['Capped Mcap'].sum()
        df['Current Weight'] = df['Capped Mcap'] / total_capped_mcap
        
        # Calculate sector weights (using first 4 digits of Subsector Code for ICB Industry)
        df['ICB Industry'] = df['Subsector Code'].astype(str).str[:4]
        sector_weights = df.groupby('ICB Industry')['Current Weight'].sum()
        
        # Save company states for this iteration
        for idx, row in df.iterrows():
            company_iteration_steps.append({
                'Iteration': iteration + 1,
                'Company': row['Company'],
                'ISIN code': row['ISIN code'],
                'ICB Industry': row['ICB Industry'],
                'Capping Factor': row['Capping Factor'],
                'Current Weight': row['Current Weight'],
                'Is Capped': row['Is Capped'],
                'Above Individual Cap': row['Current Weight'] > max_individual_weight,
                'Sector Weight': sector_weights.loc[row['ICB Industry']]
            })
        
        # Track this iteration summary
        iteration_info = {
            'Iteration': iteration + 1,
            'Max Individual Weight': df['Current Weight'].max(),
            'Max Sector Weight': sector_weights.max(),
            'Companies Above 10%': (df['Current Weight'] > max_individual_weight).sum(),
            'Companies Capped at 10%': df['Is Capped'].sum(),
            'Sectors Above 50%': (sector_weights > max_sector_weight).sum(),
            'Total Weight': df['Current Weight'].sum()
        }
        iterations_data.append(iteration_info)
        
        # Check if both caps are satisfied
        individual_breach = (df['Current Weight'] > max_individual_weight) & (~df['Is Capped'])
        sector_breach = df['ICB Industry'].map(sector_weights) > max_sector_weight
        
        if not individual_breach.any() and not sector_breach.any():
            logger.info(f"Capping converged after {iteration + 1} iterations")
            break
        
        # Step 1: Apply individual cap (only to companies not already capped)
        if individual_breach.any():
            # Mark companies that need to be capped
            newly_capped = individual_breach
            
            # Calculate how much weight these companies currently have vs what they should have
            excess_weight = df.loc[newly_capped, 'Current Weight'].sum() - (newly_capped.sum() * max_individual_weight)
            
            # Cap these companies at exactly 10% of total index
            # Calculate capping factor needed to achieve exactly 10% weight
            for idx in df[newly_capped].index:
                target_mcap = max_individual_weight * total_capped_mcap
                df.loc[idx, 'Capping Factor'] = target_mcap / df.loc[idx, 'Mcap in EUR_EOD']
                df.loc[idx, 'Is Capped'] = True
            
            # Redistribute excess weight only to uncapped companies
            uncapped_mask = ~df['Is Capped']
            if uncapped_mask.any() and excess_weight > 0:
                uncapped_weight = df.loc[uncapped_mask, 'Current Weight'].sum()
                if uncapped_weight > 0:
                    # Proportionally increase weights of uncapped companies
                    redistribution_factor = 1 + (excess_weight / uncapped_weight)
                    df.loc[uncapped_mask, 'Capping Factor'] *= redistribution_factor
        
        # Recalculate after individual capping
        df['Capped Mcap'] = df['Mcap in EUR_EOD'] * df['Capping Factor']
        total_capped_mcap = df['Capped Mcap'].sum()
        df['Current Weight'] = df['Capped Mcap'] / total_capped_mcap
        sector_weights = df.groupby('ICB Industry')['Current Weight'].sum()
        
        # Step 2: Apply sector cap
        breached_sectors = sector_weights[sector_weights > max_sector_weight].index
        if len(breached_sectors) > 0:
            for sector in breached_sectors:
                sector_mask = df['ICB Industry'] == sector
                
                # Calculate sector excess
                sector_current_weight = df.loc[sector_mask, 'Current Weight'].sum()
                sector_excess = sector_current_weight - max_sector_weight
                
                # Scale down all companies in the breached sector proportionally
                # But respect the 10% cap for already-capped companies
                scale_factor = max_sector_weight / sector_current_weight
                df.loc[sector_mask, 'Capping Factor'] *= scale_factor
                
                # Redistribute excess only to companies NOT in breached sectors and NOT capped at 10%
                eligible_for_redistribution = (~df['ICB Industry'].isin(breached_sectors)) & (~df['Is Capped'])
                
                if eligible_for_redistribution.any():
                    eligible_weight = df.loc[eligible_for_redistribution, 'Current Weight'].sum()
                    if eligible_weight > 0:
                        redistribution_factor = 1 + (sector_excess / eligible_weight)
                        df.loc[eligible_for_redistribution, 'Capping Factor'] *= redistribution_factor
    
    else:
        logger.warning(f"Capping did not converge after {max_iterations} iterations")
    
    # NORMALIZE CAPPING FACTORS - highest should be 1
    max_capping_factor = df['Capping Factor'].max()
    if max_capping_factor > 0:
        df['Unnormalized Capping Factor'] = df['Capping Factor'].copy()
        df['Capping Factor'] = df['Capping Factor'] / max_capping_factor
        logger.info(f"Normalized capping factors by dividing by {max_capping_factor}")
    
    # Final calculations with normalized capping factors
    df['Capped Mcap'] = df['Mcap in EUR_EOD'] * df['Capping Factor']
    total_capped_mcap = df['Capped Mcap'].sum()
    df['Final Weight'] = df['Capped Mcap'] / total_capped_mcap
    
    # Recalculate final sector weights
    final_sector_weights = df.groupby('ICB Industry')['Final Weight'].sum()
    
    # Update final iteration info with normalized values
    final_iteration_info = {
        'Iteration': 'Final (Normalized)',
        'Max Individual Weight': df['Final Weight'].max(),
        'Max Sector Weight': final_sector_weights.max(),
        'Companies Above 10%': (df['Final Weight'] > max_individual_weight).sum(),
        'Companies Capped at 10%': df['Is Capped'].sum(),
        'Sectors Above 50%': (final_sector_weights > max_sector_weight).sum(),
        'Total Weight': df['Final Weight'].sum(),
        'Max Capping Factor': df['Capping Factor'].max(),
        'Normalization Factor': max_capping_factor if max_capping_factor > 0 else 1.0
    }
    iterations_data.append(final_iteration_info)
    
    # Create summary dataframes
    iterations_df = pd.DataFrame(iterations_data)
    company_iterations_df = pd.DataFrame(company_iteration_steps)
    
    # Create a sector summary with final normalized weights
    sector_summary = df.groupby('ICB Industry').agg({
        'Final Weight': 'sum',
        'Company': 'count'
    }).rename(columns={'Company': 'Number of Companies'}).reset_index()
    sector_summary['Within 50% Cap'] = sector_summary['Final Weight'] <= max_sector_weight
    
    return df, iterations_df, company_iterations_df, sector_summary

def run_es2pr_review(date, co_date, effective_date, index="ES2PR", isin="NLIX00005982", 
                   area="US", area2="EU", type="STOCK", universe="developed_market", 
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
            ['ff', 'icb', universe]
        )

        # Get the individual dataframes
        sustainable_universe = [
            {'Company': 'ASML HOLDING', 'ISIN': 'NL0010273215', 'MIC': 'XAMS', 'Currency': 'EUR'},
            {'Company': 'SCHNEIDER ELECTRIC', 'ISIN': 'FR0000121972', 'MIC': 'XPAR', 'Currency': 'EUR'},
            {'Company': 'BANCO SANTANDER CENT', 'ISIN': 'ES0113900J37', 'MIC': 'XMAD', 'Currency': 'EUR'},
            {'Company': 'UNICREDIT', 'ISIN': 'IT0005239360', 'MIC': 'MTAA', 'Currency': 'EUR'},
            {'Company': 'PRYSMIAN', 'ISIN': 'IT0004176001', 'MIC': 'MTAA', 'Currency': 'EUR'},
            {'Company': 'BBVA', 'ISIN': 'ES0113211835', 'MIC': 'XMAD', 'Currency': 'EUR'},
            {'Company': 'SIEMENS ENERGY AG', 'ISIN': 'DE000ENER6Y0', 'MIC': 'XETR', 'Currency': 'EUR'},
            {'Company': 'DEUTSCHE BANK', 'ISIN': 'DE0005140008', 'MIC': 'XETR', 'Currency': 'EUR'},
            {'Company': 'INFINEON TECHNOLOGIE', 'ISIN': 'DE0006231004', 'MIC': 'XETR', 'Currency': 'EUR'},
            {'Company': 'SOCIETE GENERALE', 'ISIN': 'FR0000130809', 'MIC': 'XPAR', 'Currency': 'EUR'},
            {'Company': 'CAIXABANK', 'ISIN': 'ES0140609019', 'MIC': 'XMAD', 'Currency': 'EUR'},
            {'Company': 'ASM INTERNATIONAL', 'ISIN': 'NL0000334118', 'MIC': 'XAMS', 'Currency': 'EUR'},
            {'Company': 'DAIMLER TRUCK HLDNG', 'ISIN': 'DE000DTR0CK8', 'MIC': 'XETR', 'Currency': 'EUR'},
            {'Company': 'STMICROELECTRONICS', 'ISIN': 'NL0000226223', 'MIC': 'XPAR', 'Currency': 'EUR'},
            {'Company': 'BANCO BPM', 'ISIN': 'IT0005218380', 'MIC': 'MTAA', 'Currency': 'EUR'},
            {'Company': 'COMMERZBANK AG', 'ISIN': 'DE000CBK1001', 'MIC': 'XETR', 'Currency': 'EUR'},
            {'Company': 'BE SEMICONDUCTOR', 'ISIN': 'NL0012866412', 'MIC': 'XAMS', 'Currency': 'EUR'},
            {'Company': 'ALSTOM', 'ISIN': 'FR0010220475', 'MIC': 'XPAR', 'Currency': 'EUR'},
            {'Company': 'BANCA MONTE PASCHI SIENA', 'ISIN': 'IT0005508921', 'MIC': 'MTAA', 'Currency': 'EUR'},
            {'Company': 'IVECO GROUP', 'ISIN': 'NL0015000LU4', 'MIC': 'MTAA', 'Currency': 'EUR'},
        ]

        # Convert to DataFrame when needed
        sustainable_df = pd.DataFrame(sustainable_universe)
        ff_df = ref_data['ff']
        icb_df = ref_data['icb']
        developed_market_df = ref_data[universe]
        
        # Add the required columns to the combined dataframe
        sustainable_df['Effective Date of Review'] = effective_date

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() < 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')
        
        # Chain all data preparation operations using the combined dataframe
        selection_df = (sustainable_df
           # Initial renaming
           .rename(columns={
               'ISIN': 'ISIN code'
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
            .merge(
                icb_df[['ISIN Code', 'Subsector Code']].drop_duplicates(subset='ISIN Code', keep='first'),
                left_on='ISIN code',
                right_on='ISIN Code',
                how='left'
            )
            .drop('ISIN Code', axis=1)
            .merge(
                developed_market_df[['ISIN', 'NOSH', 'Price (EUR) ', 'Mcap in EUR']].drop_duplicates(subset='ISIN', keep='first'),
                left_on='ISIN code',
                right_on='ISIN',
                how='left'
            )
            .drop('ISIN', axis=1)
        )

        # Validate data loading
        if selection_df is None:
            raise ValueError("Failed to load required reference data files")
    
        # Use the merged Free Float Round value, fallback to original Free Float if not available
        selection_df["Free Float"] = selection_df["Free Float Round:"]

        # Calculate market cap at EOD with free float
        selection_df['Mcap in EUR_EOD'] = selection_df['Close Prc_EOD'] * selection_df['FX/Index Ccy'] * selection_df['Free Float'] * selection_df['NOSH']
        
        # Calculate index market cap
        index_mcap = selection_df['Mcap in EUR_EOD'].sum() if 'Mcap in EUR_EOD' in selection_df.columns else 0
        
        # Calculate capping factors with iteration tracking
        logger.info("Calculating capping factors...")
        selection_df, iterations_df, company_iterations_df, sector_summary_df = calculate_capping_factors(
            selection_df, 
            index_mcap,
            max_individual_weight=0.10,
            max_sector_weight=0.50
        )
        
        selection_df['Effective Date of Review'] = effective_date
        selection_df['Rounded NOSH'] = selection_df['NOSH']  # Adjust as needed
        
        # Prepare ES2PR DataFrame
        ES2PR_df = (
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
            es2pr_path = os.path.join(output_dir, f'ES2PR_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving ES2PR output to: {es2pr_path}")
            with pd.ExcelWriter(es2pr_path) as writer:
                ES2PR_df.to_excel(writer, sheet_name='Index Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
                iterations_df.to_excel(writer, sheet_name='Capping Iterations', index=False)
                company_iterations_df.to_excel(writer, sheet_name='Company Iterations', index=False)
                sector_summary_df.to_excel(writer, sheet_name='Sector Summary', index=False)
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"es2pr_path": es2pr_path}
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