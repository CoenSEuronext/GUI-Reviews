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

def run_aetaw_review(date, co_date, effective_date, index="AETAW", isin="NL0010614525", 
                   area="US", area2="EU", type="STOCK", universe="aex_family", 
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

        # Load each sheet separately
        aex_data = load_reference_data(
            current_data_folder,
            ['aex_family'],
            sheet_names={'aex_family': 'AEX'}
        )

        amx_data = load_reference_data(
            current_data_folder,
            ['aex_family'],
            sheet_names={'aex_family': 'AMX'}
        )

        ascx_data = load_reference_data(
            current_data_folder,
            ['aex_family'],
            sheet_names={'aex_family': 'AScX'}
        )

        # Concatenate all three sheets into a single DataFrame
        selection_df = pd.concat([
            aex_data['aex_family'],
            amx_data['aex_family'],
            ascx_data['aex_family']
        ], ignore_index=True)

        logger.info(f"Loaded {len(selection_df)} companies from AEX, AMX, and AScX")

        # Filter symbols once
        symbols_filtered = stock_eod_df[
            stock_eod_df['#Symbol'].str.len() == 12
        ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'], keep='first')

        selection_df = (selection_df
            # Initial renaming
            .rename(columns={
                'Preliminary Number of shares': 'Number of Shares',
                'Preliminary Free Float': 'Free Float',
                'Preliminary Capping Factor': 'Capping Factor',
                'Effective date of review': 'Effective Date of Review'
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
            # Merge current AETAW Capping Factor
            .merge(
                stock_eod_df[stock_eod_df['Index'] == 'AETAW'][['Isin Code', 'Capping Factor-Coeff']].drop_duplicates(subset='Isin Code', keep='first'),
                left_on='ISIN code',
                right_on='Isin Code',
                how='left'
            )
            .drop('Isin Code', axis=1)
        )

        # Fill missing capping factors with 0
        selection_df['Capping Factor-Coeff'] = selection_df['Capping Factor-Coeff'].fillna(0)
                
        # Validate data loading
        if selection_df is None or len(selection_df) == 0:
            raise ValueError("Failed to load one or more required reference data files")

        # Find index market cap
        index_mcap = index_eod_df.loc[index_eod_df['#Symbol'] == isin, 'Mkt Cap'].iloc[0]

        # Calculate initial Free Float Market Cap for weighting
        selection_df['Initial FF Market Cap'] = (
            selection_df['Number of Shares'] * 
            selection_df['Free Float'] * 
            selection_df['Close Prc_EOD'] *
            selection_df['FX/Index Ccy']
        )

        # Calculate initial uncapped weights
        total_ffmc = selection_df['Initial FF Market Cap'].sum()
        selection_df['Initial Weight'] = selection_df['Initial FF Market Cap'] / total_ffmc

        logger.info(f"\nInitial weights calculated. Starting two-step capping process...")

        # ===== TWO-STEP CAPPING PROCESS =====
        individual_cap = 0.09
        collective_threshold = 0.045
        collective_cap = 0.36
        max_iterations = 100
        tolerance = 1e-8

        original_weights = selection_df['Initial Weight'].copy()
        current_weights = original_weights.copy()

        # ===== STEP 1: Cap all companies at 9% iteratively =====
        logger.info("\n========== STEP 1: Cap all companies at 9% ==========")
        
        step1_iteration = 0
        for iteration in range(max_iterations):
            step1_iteration = iteration + 1
            logger.info(f"\n--- Step 1, Iteration {iteration + 1} ---")
            
            # Track this iteration's data
            selection_df[f'Step1_Iter{iteration+1}_Weight'] = current_weights
            
            new_weights = current_weights.copy()
            changes_made = False
            
            # Cap all companies at 9%
            companies_over_9 = current_weights > individual_cap
            selection_df[f'Step1_Iter{iteration+1}_Capped'] = companies_over_9
            
            if companies_over_9.any():
                excess_weight = (current_weights[companies_over_9] - individual_cap).sum()
                new_weights[companies_over_9] = individual_cap
                changes_made = True
                
                logger.info(f"Capped {companies_over_9.sum()} companies at 9%")
                logger.info(f"Excess weight: {excess_weight*100:.2f}%")
                
                # Redistribute excess to companies below 9%
                can_receive = new_weights < individual_cap
                
                if can_receive.any() and excess_weight > tolerance:
                    eligible_weights = original_weights[can_receive]
                    total_eligible = eligible_weights.sum()
                    
                    if total_eligible > tolerance:
                        redistribution_shares = eligible_weights / total_eligible
                        additional_weight = excess_weight * redistribution_shares
                        new_weights[can_receive] += additional_weight
                        
                        logger.info(f"Redistributed to {can_receive.sum()} companies below 9%")
                        
                        # Check if any companies went above 9% after redistribution
                        newly_over_9 = (new_weights > individual_cap) & (current_weights <= individual_cap)
                        if newly_over_9.any():
                            logger.info(f"  WARNING: {newly_over_9.sum()} companies now ABOVE 9% after redistribution")
            
            # Save weights after redistribution
            selection_df[f'Step1_Iter{iteration+1}_Weight_After_Redist'] = new_weights
            
            # Check convergence
            max_change = abs(new_weights - current_weights).max()
            logger.info(f"Max weight change: {max_change*100:.4f}%")
            
            current_weights = new_weights
            
            if not changes_made or max_change < tolerance:
                logger.info(f"[SUCCESS] Step 1 converged after {iteration + 1} iterations")
                break
        else:
            logger.warning(f"[WARNING] Step 1 did not converge after {max_iterations} iterations")
        
        # Log Step 1 results
        selection_df['Step1_Final_Weight'] = current_weights
        at_9_pct_step1 = (current_weights >= individual_cap - tolerance).sum()
        logger.info(f"\nStep 1 complete: {at_9_pct_step1} companies at 9%")

        # ===== STEP 2: Apply collective constraint iteratively =====
        logger.info("\n========== STEP 2: Apply collective constraint (36% rule) ==========")
        
        # Identify top 4 companies by ORIGINAL weight (initial market cap) - FIXED
        top_4_indices = original_weights.nlargest(4).index
        selection_df['Top_4_Protected'] = selection_df.index.isin(top_4_indices)
        
        logger.info(f"Top 4 companies by initial market cap (protected at 9%):")
        for rank, idx in enumerate(top_4_indices, 1):
            company_name = selection_df.loc[idx, 'Company']
            logger.info(f"  {rank}. {company_name}: Initial {original_weights[idx]*100:.2f}%, After Step 1: {current_weights[idx]*100:.2f}%")
        
        # Track companies that have been capped at 4.5% (they stay locked)
        capped_at_45 = pd.Series(False, index=selection_df.index)
        
        step2_iteration = 0
        for iteration in range(max_iterations):
            step2_iteration = iteration + 1
            logger.info(f"\n--- Step 2, Iteration {iteration + 1} ---")
            
            # Track this iteration's data
            selection_df[f'Step2_Iter{iteration+1}_Weight'] = current_weights
            
            new_weights = current_weights.copy()
            changes_made = False
            
            # Cap all companies above 4.5% (except top 4) to exactly 4.5%
            needs_capping_to_45 = (current_weights > collective_threshold) & (~selection_df.index.isin(top_4_indices))
            selection_df[f'Step2_Iter{iteration+1}_Capped'] = needs_capping_to_45
            
            if needs_capping_to_45.any():
                excess_weight = (current_weights[needs_capping_to_45] - collective_threshold).sum()
                new_weights[needs_capping_to_45] = collective_threshold
                capped_at_45[needs_capping_to_45] = True  # Mark as capped
                changes_made = True
                
                logger.info(f"Capped {needs_capping_to_45.sum()} companies (outside top 4) to 4.5%")
                logger.info(f"Total companies locked at 4.5%: {capped_at_45.sum()}")
                logger.info(f"Excess weight: {excess_weight*100:.2f}%")
                
                # Redistribute excess ONLY to companies that:
                # 1. Are at or below 4.5% AND
                # 2. Have NOT been capped at 4.5% (not locked)
                can_receive = (new_weights <= collective_threshold) & (~capped_at_45)
                
                if can_receive.any() and excess_weight > tolerance:
                    eligible_weights = original_weights[can_receive]
                    total_eligible = eligible_weights.sum()
                    
                    if total_eligible > tolerance:
                        redistribution_shares = eligible_weights / total_eligible
                        additional_weight = excess_weight * redistribution_shares
                        new_weights[can_receive] += additional_weight
                        
                        logger.info(f"Redistributed to {can_receive.sum()} companies (not locked at 4.5%)")
                        
                        # Check if any companies (outside top 4) went above 4.5% after redistribution
                        newly_over_45 = ((new_weights > collective_threshold) & 
                                        (current_weights <= collective_threshold) & 
                                        (~selection_df.index.isin(top_4_indices)))
                        
                        if newly_over_45.any():
                            logger.info(f"  WARNING: {newly_over_45.sum()} companies (outside top 4) now ABOVE 4.5% after redistribution")
                    elif total_eligible <= tolerance:
                        logger.warning(f"  WARNING: Cannot redistribute {excess_weight*100:.2f}% - no eligible recipients")
                elif not can_receive.any():
                    logger.warning(f"  WARNING: Cannot redistribute {excess_weight*100:.2f}% - all companies either top 4 or locked at 4.5%")
            
            # Save weights after redistribution
            selection_df[f'Step2_Iter{iteration+1}_Weight_After_Redist'] = new_weights
            selection_df[f'Step2_Iter{iteration+1}_Locked_at_4.5'] = capped_at_45
            
            # Check convergence
            max_change = abs(new_weights - current_weights).max()
            logger.info(f"Max weight change: {max_change*100:.4f}%")
            
            current_weights = new_weights
            
            if not changes_made or max_change < tolerance:
                logger.info(f"[SUCCESS] Step 2 converged after {iteration + 1} iterations")
                break
        else:
            logger.warning(f"[WARNING] Step 2 did not converge after {max_iterations} iterations")

        # ===== Final Results =====
        selection_df['Final Weight'] = current_weights
        selection_df['Final Locked at 4.5%'] = capped_at_45
        
        # Calculate final market cap based on final weights
        selection_df['Final FF Market Cap'] = selection_df['Final Weight'] * total_ffmc
        
        # Calculate Capping Factor as ratio of final to initial weight
        selection_df['Capping Factor'] = selection_df['Final Weight'] / selection_df['Initial Weight']
        selection_df['Capping Factor'] = selection_df['Capping Factor'].fillna(1)  # Handle division by zero

        # Standardize capping factors so maximum is 1
        max_capping_factor = selection_df['Capping Factor'].max()
        selection_df['Capping Factor'] = selection_df['Capping Factor'] / max_capping_factor

        # Final verification
        above_threshold = selection_df['Final Weight'] > collective_threshold + tolerance
        final_collective = selection_df.loc[above_threshold, 'Final Weight'].sum()
        at_9_pct = (selection_df['Final Weight'] >= individual_cap - tolerance).sum()
        at_45_pct = (abs(selection_df['Final Weight'] - collective_threshold) < tolerance).sum()
        
        logger.info(f"\n========== Final Results ==========")
        logger.info(f"Companies at 9.00%: {at_9_pct}")
        logger.info(f"Companies locked at 4.50%: {at_45_pct}")
        logger.info(f"Total weight OVER 4.5%: {final_collective*100:.2f}% (limit: {collective_cap*100:.2f}%)")
        
        if final_collective > collective_cap + tolerance:
            logger.error(f"[ERROR] Collective cap VIOLATED by {(final_collective - collective_cap)*100:.2f}%!")
        else:
            logger.info(f"[SUCCESS] Collective cap satisfied")

        logger.info(f"AETAW capping complete. Companies with CF < 1: {(selection_df['Capping Factor'] < 1).sum()}")

        # Add Effective Date
        selection_df['Effective Date of Review'] = effective_date
        selection_df['Currency'] = currency

        # Perform Inclusion/Exclusion Analysis
        analysis_results = inclusion_exclusion_analysis(
            selection_df, 
            stock_eod_df, 
            index, 
            isin_column='ISIN code'
        )
        inclusion_df = analysis_results['inclusion_df']
        exclusion_df = analysis_results['exclusion_df']
        
        AETAW_df = (
            selection_df[
                ['Company', 'ISIN code', 'MIC', 'Number of Shares', 'Free Float', 'Capping Factor', 
                'Effective Date of Review', 'Currency']
            ]
            .rename(columns={'ISIN code': 'ISIN Code'})
            .sort_values('Company')
        )

        # Save output files
        try:
            output_dir = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_dir, exist_ok=True)
           
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            aetaw_path = os.path.join(output_dir, f'AETAW_df_{timestamp}.xlsx')
           
            # Save output with multiple sheets
            logger.info(f"Saving AETAW output to: {aetaw_path}")
            with pd.ExcelWriter(aetaw_path) as writer:
                AETAW_df.to_excel(writer, sheet_name=index + ' Composition', index=False)
                inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
                exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
                selection_df.to_excel(writer, sheet_name='Selection', index=False)
                
                pd.DataFrame({'Index Market Cap': [index_mcap]}).to_excel(writer, sheet_name='Index Market Cap', index=False)

            return {
                "status": "success",
                "message": "Review completed successfully",
                "data": {"aetaw_path": aetaw_path}
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