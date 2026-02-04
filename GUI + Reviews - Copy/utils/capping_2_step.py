import pandas as pd
import numpy as np
from utils.logging_utils import setup_logging

logger = setup_logging(__name__)

def calculate_2_step_capped_weights(weights, individual_cap=0.09, collective_threshold=0.045, 
                                   collective_cap=0.36, max_iterations=100, tolerance=1e-8):
    """
    Two-step iterative capping process for AETAW:
    
    STEP 1: Iteratively cap all companies at 9% until stable
    STEP 2: Iteratively keep only top 4 (by INITIAL weight) at 9%, cap all others at 4.5% until stable
            IMPORTANT: Once capped at 4.5%, companies stay at 4.5% (no further redistribution)
    
    This ensures:
    - Individual cap: Maximum 9% per company
    - Collective cap: Total weight of companies > 4.5% cannot exceed 36%
    """
    
    original_weights = weights.copy()
    current_weights = weights.copy()
    
    # ===== STEP 1: Cap all companies at 9% iteratively =====
    logger.info("\n========== STEP 1: Cap all companies at 9% ==========")
    
    for iteration in range(max_iterations):
        logger.info(f"\n--- Step 1, Iteration {iteration + 1} ---")
        
        new_weights = current_weights.copy()
        changes_made = False
        
        # Cap all companies at 9%
        companies_over_9 = current_weights > individual_cap
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
    at_9_pct_step1 = (current_weights >= individual_cap - tolerance).sum()
    logger.info(f"\nStep 1 complete: {at_9_pct_step1} companies at 9%")
    
    # ===== STEP 2: Apply collective constraint iteratively =====
    logger.info("\n========== STEP 2: Apply collective constraint (36% rule) ==========")
    
    # Identify top 4 companies by ORIGINAL weight (initial market cap) - FIXED, doesn't change
    top_4_indices = original_weights.nlargest(4).index
    logger.info(f"Top 4 companies by initial market cap (protected at 9%):")
    for rank, idx in enumerate(top_4_indices, 1):
        logger.info(f"  {rank}. {idx}: Initial {original_weights[idx]*100:.2f}%, After Step 1: {current_weights[idx]*100:.2f}%")
    
    # Track companies that have been capped at 4.5% (they stay locked)
    capped_at_45 = pd.Series(False, index=weights.index)
    
    # Iterative capping at 4.5% for non-top-4 companies
    for iteration in range(max_iterations):
        logger.info(f"\n--- Step 2, Iteration {iteration + 1} ---")
        
        new_weights = current_weights.copy()
        changes_made = False
        
        # Cap all companies above 4.5% (except top 4) to exactly 4.5%
        needs_capping_to_45 = (current_weights > collective_threshold) & (~current_weights.index.isin(top_4_indices))
        
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
                                    (~new_weights.index.isin(top_4_indices)))
                    
                    if newly_over_45.any():
                        logger.info(f"  WARNING: {newly_over_45.sum()} companies (outside top 4) now ABOVE 4.5% after redistribution")
                        for idx in new_weights[newly_over_45].sort_values(ascending=False).index:
                            logger.info(f"      {idx}: {current_weights[idx]*100:.2f}% -> {new_weights[idx]*100:.2f}%")
                elif total_eligible <= tolerance:
                    # No eligible recipients left - this shouldn't happen in practice
                    logger.warning(f"  WARNING: Cannot redistribute {excess_weight*100:.2f}% - no eligible recipients")
            elif not can_receive.any():
                logger.warning(f"  WARNING: Cannot redistribute {excess_weight*100:.2f}% - all companies either top 4 or locked at 4.5%")
        
        # Check convergence
        max_change = abs(new_weights - current_weights).max()
        logger.info(f"Max weight change: {max_change*100:.4f}%")
        
        current_weights = new_weights
        
        if not changes_made or max_change < tolerance:
            logger.info(f"[SUCCESS] Step 2 converged after {iteration + 1} iterations")
            break
    else:
        logger.warning(f"[WARNING] Step 2 did not converge after {max_iterations} iterations")
    
    # ===== Final Verification =====
    above_threshold = current_weights > collective_threshold + tolerance
    final_collective = current_weights[above_threshold].sum()
    at_9_pct = (current_weights >= individual_cap - tolerance).sum()
    at_45_pct = (abs(current_weights - collective_threshold) < tolerance).sum()
    between_45_and_9 = ((current_weights > collective_threshold + tolerance) & 
                        (current_weights < individual_cap - tolerance)).sum()
    
    logger.info(f"\n========== Final Results ==========")
    logger.info(f"Companies at 9.00%: {at_9_pct}")
    logger.info(f"Companies locked at 4.50%: {at_45_pct}")
    logger.info(f"Companies between 4.5% and 9%: {between_45_and_9}")
    logger.info(f"Companies below 4.5%: {(current_weights < collective_threshold - tolerance).sum()}")
    logger.info(f"Total weight OVER 4.5%: {final_collective*100:.2f}% (limit: {collective_cap*100:.2f}%)")
    
    if final_collective > collective_cap + tolerance:
        logger.error(f"[ERROR] Collective cap VIOLATED by {(final_collective - collective_cap)*100:.2f}%!")
    else:
        logger.info(f"[SUCCESS] Collective cap satisfied")
    
    # Log the top 4 final weights
    logger.info(f"\nTop 4 companies (by initial market cap) final weights:")
    for rank, idx in enumerate(top_4_indices, 1):
        logger.info(f"  {rank}. {idx}: {current_weights[idx]*100:.2f}%")
    
    logger.info(f"\nCompanies locked at 4.5%: {capped_at_45.sum()}")
    
    return current_weights