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
        index_eod_df, stock_eod_df, stock_co_df, fx_lookup_df = load_eod_data(date, co_date, area, area2, DLF_FOLDER)

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

        logger.info(f"\nInitial weights calculated. Starting three-step capping process...")

        # ===== THREE-STEP CAPPING PROCESS =====
        individual_cap = 0.09
        collective_threshold = 0.045
        collective_cap = 0.36
        max_iterations = 100
        tolerance = 1e-8

        original_weights = selection_df['Initial Weight'].copy()
        current_weights = original_weights.copy()

        # Track locked companies
        locked_at_9 = pd.Series(False, index=selection_df.index)
        locked_at_45 = pd.Series(False, index=selection_df.index)

        # ===== STEP 1: Cap all companies at 9% iteratively =====
        logger.info("\n========== STEP 1: Individual 9% Cap ==========")

        for iteration in range(max_iterations):
            logger.info(f"\n--- Step 1, Iteration {iteration + 1} ---")
            
            selection_df[f'Step1_Iter{iteration+1}_Weight'] = current_weights
            
            new_weights = current_weights.copy()
            changes_made = False
            
            # Identify companies over 9%
            companies_over_9 = current_weights > individual_cap + tolerance
            
            if companies_over_9.any():
                excess_weight = (current_weights[companies_over_9] - individual_cap).sum()
                new_weights[companies_over_9] = individual_cap
                locked_at_9[companies_over_9] = True
                changes_made = True
                
                logger.info(f"Capped {companies_over_9.sum()} companies at 9%")
                logger.info(f"Excess weight: {excess_weight*100:.4f}%")
                
                # Redistribute to ALL other companies (not locked at 9%)
                can_receive = ~locked_at_9
                
                if can_receive.any() and excess_weight > tolerance:
                    eligible_weights = original_weights[can_receive]
                    total_eligible = eligible_weights.sum()
                    
                    if total_eligible > tolerance:
                        redistribution_shares = eligible_weights / total_eligible
                        additional_weight = excess_weight * redistribution_shares
                        new_weights[can_receive] += additional_weight
                        
                        logger.info(f"Redistributed to {can_receive.sum()} companies")
            
            selection_df[f'Step1_Iter{iteration+1}_Weight_After_Redist'] = new_weights
            selection_df[f'Step1_Iter{iteration+1}_Locked_at_9'] = locked_at_9
            
            max_change = abs(new_weights - current_weights).max()
            logger.info(f"Max weight change: {max_change*100:.6f}%")
            
            current_weights = new_weights
            
            if not changes_made or max_change < tolerance:
                logger.info(f"[SUCCESS] Step 1 converged after {iteration + 1} iterations")
                break
        else:
            logger.warning(f"[WARNING] Step 1 did not converge after {max_iterations} iterations")

        logger.info(f"\nStep 1 complete: {locked_at_9.sum()} companies locked at 9%")
        for idx in selection_df[locked_at_9].index:
            logger.info(f"  {selection_df.loc[idx, 'Company']}: {current_weights[idx]*100:.4f}%")

        # ===== STEP 2: Cap all except top 4 at 4.5% iteratively =====
        logger.info("\n========== STEP 2: Collective 36% Constraint (Cap outside top 4 at 4.5%) ==========")

        for iteration in range(max_iterations):
            logger.info(f"\n--- Step 2, Iteration {iteration + 1} ---")
            
            selection_df[f'Step2_Iter{iteration+1}_Weight'] = current_weights
            
            new_weights = current_weights.copy()
            changes_made = False
            
            # Identify top 4 companies by current weight
            top_4_indices = current_weights.nlargest(4).index
            is_top_4 = pd.Series(False, index=selection_df.index)
            is_top_4[top_4_indices] = True
            
            logger.info(f"Top 4 companies:")
            for idx in top_4_indices:
                logger.info(f"  {selection_df.loc[idx, 'Company']}: {current_weights[idx]*100:.4f}%")
            
            # Cap all companies NOT in top 4 and above 4.5% at 4.5%
            needs_capping_to_45 = (~is_top_4) & (current_weights > collective_threshold + tolerance) & (~locked_at_45)
            
            if needs_capping_to_45.any():
                excess_weight = (current_weights[needs_capping_to_45] - collective_threshold).sum()
                new_weights[needs_capping_to_45] = collective_threshold
                locked_at_45[needs_capping_to_45] = True
                changes_made = True
                
                logger.info(f"Capped {needs_capping_to_45.sum()} companies (outside top 4) at 4.5%")
                logger.info(f"Total companies locked at 4.5%: {locked_at_45.sum()}")
                logger.info(f"Excess weight: {excess_weight*100:.4f}%")
                
                # Redistribute to companies NOT locked at 9% and NOT locked at 4.5%
                can_receive = (~locked_at_9) & (~locked_at_45)
                
                if can_receive.any() and excess_weight > tolerance:
                    eligible_weights = original_weights[can_receive]
                    total_eligible = eligible_weights.sum()
                    
                    if total_eligible > tolerance:
                        redistribution_shares = eligible_weights / total_eligible
                        additional_weight = excess_weight * redistribution_shares
                        new_weights[can_receive] += additional_weight
                        
                        logger.info(f"Redistributed to {can_receive.sum()} companies (not locked at 9% or 4.5%)")
            
            selection_df[f'Step2_Iter{iteration+1}_Weight_After_Redist'] = new_weights
            selection_df[f'Step2_Iter{iteration+1}_Locked_at_4.5'] = locked_at_45
            
            max_change = abs(new_weights - current_weights).max()
            logger.info(f"Max weight change: {max_change*100:.6f}%")
            
            current_weights = new_weights
            
            if not changes_made or max_change < tolerance:
                logger.info(f"[SUCCESS] Step 2 converged after {iteration + 1} iterations")
                break
        else:
            logger.warning(f"[WARNING] Step 2 did not converge after {max_iterations} iterations")

        logger.info(f"\nStep 2 complete: {locked_at_45.sum()} companies locked at 4.5%")

        # ===== STEP 3: Re-check 9% cap for top 4 =====
        logger.info("\n========== STEP 3: Re-check 9% Cap for Top 4 ==========")

        for iteration in range(max_iterations):
            logger.info(f"\n--- Step 3, Iteration {iteration + 1} ---")
            
            selection_df[f'Step3_Iter{iteration+1}_Weight'] = current_weights
            
            new_weights = current_weights.copy()
            changes_made = False
            
            # Check if any company (not already locked at 9%) went above 9%
            companies_over_9 = (current_weights > individual_cap + tolerance) & (~locked_at_9)
            
            if companies_over_9.any():
                excess_weight = (current_weights[companies_over_9] - individual_cap).sum()
                new_weights[companies_over_9] = individual_cap
                locked_at_9[companies_over_9] = True
                changes_made = True
                
                logger.info(f"Capped {companies_over_9.sum()} companies at 9%")
                logger.info(f"Excess weight: {excess_weight*100:.4f}%")
                
                # Redistribute to companies NOT locked at 9%, NOT locked at 4.5%, and below 4.5%
                can_receive = (~locked_at_9) & (~locked_at_45) & (new_weights < collective_threshold)
                
                if can_receive.any() and excess_weight > tolerance:
                    eligible_weights = original_weights[can_receive]
                    total_eligible = eligible_weights.sum()
                    
                    if total_eligible > tolerance:
                        redistribution_shares = eligible_weights / total_eligible
                        additional_weight = excess_weight * redistribution_shares
                        new_weights[can_receive] += additional_weight
                        
                        logger.info(f"Redistributed to {can_receive.sum()} companies (not locked, below 4.5%)")
            
            selection_df[f'Step3_Iter{iteration+1}_Weight_After_Redist'] = new_weights
            selection_df[f'Step3_Iter{iteration+1}_Locked_at_9'] = locked_at_9
            
            max_change = abs(new_weights - current_weights).max()
            logger.info(f"Max weight change: {max_change*100:.6f}%")
            
            current_weights = new_weights
            
            if not changes_made or max_change < tolerance:
                logger.info(f"[SUCCESS] Step 3 converged after {iteration + 1} iterations")
                break
        else:
            logger.warning(f"[WARNING] Step 3 did not converge after {max_iterations} iterations")

        # ===== Final Results =====
        selection_df['Final Weight'] = current_weights
        selection_df['Final Locked at 9%'] = locked_at_9
        selection_df['Final Locked at 4.5%'] = locked_at_45
        
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
        logger.info(f"Total weight OVER 4.5%: {final_collective*100:.4f}% (limit: {collective_cap*100:.2f}%)")
        logger.info(f"Sum of all final weights: {current_weights.sum()*100:.6f}% (should be 100%)")
        
        if final_collective > collective_cap + tolerance:
            logger.error(f"[ERROR] Collective cap VIOLATED by {(final_collective - collective_cap)*100:.4f}%!")
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