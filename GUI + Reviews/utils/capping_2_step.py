import pandas as pd
import numpy as np
from utils.capping_standard import calculate_capped_weights
from utils.logging_utils import setup_logging

logger = setup_logging(__name__)

def calculate_2_step_capped_weights(weights, individual_cap=0.09, collective_threshold=0.045, 
                                   collective_cap=0.36, max_iterations=100, tolerance=1e-8):
    """
    Calculate capped weights for AETAW with two constraints:
    1. Individual cap: No company can exceed 9%
    2. Collective cap: Total weight of companies > 4.5% cannot exceed 36%
    
    Args:
        weights: Series of uncapped weights
        individual_cap: Maximum weight for any single company (default 0.09 = 9%)
        collective_threshold: Threshold for collective cap (default 0.045 = 4.5%)
        collective_cap: Maximum total weight for companies above threshold (default 0.36 = 36%)
        max_iterations: Maximum iterations for convergence
        tolerance: Convergence tolerance
        
    Returns:
        Series of capped weights
    """
    
    # Step 1: Apply individual 9% cap using standard capping function
    original_weights = weights.copy()
    current_weights = calculate_capped_weights(
        weights, 
        cap_limit=individual_cap, 
        max_iterations=max_iterations, 
        tolerance=tolerance
    )
    
    logger.info(f"After 9% individual cap: {(current_weights >= individual_cap - tolerance).sum()} companies at cap")
    
    # Step 2: Check and apply collective constraint
    above_threshold = current_weights > collective_threshold
    total_above_threshold = current_weights[above_threshold].sum()
    
    logger.info(f"Companies above {collective_threshold*100}%: {above_threshold.sum()}, Total weight: {total_above_threshold*100:.2f}%")
    
    if total_above_threshold > collective_cap + tolerance:
        # Collective constraint violated, need to apply additional capping
        excess = total_above_threshold - collective_cap
        
        logger.info(f"Collective cap violated! Excess: {excess*100:.2f}%. Applying collective capping...")
        
        # Proportionally reduce weights above threshold to exactly collective_cap
        reduction_factor = collective_cap / total_above_threshold
        current_weights[above_threshold] = current_weights[above_threshold] * reduction_factor
        
        logger.info(f"Reduced {above_threshold.sum()} companies by factor {reduction_factor:.4f}")
        
        # Redistribute excess to companies at or below threshold
        at_or_below = current_weights <= collective_threshold
        
        if at_or_below.any():
            eligible_weights = original_weights[at_or_below]
            total_eligible = eligible_weights.sum()
            
            if total_eligible > tolerance:
                redistribution_shares = eligible_weights / total_eligible
                current_weights[at_or_below] += excess * redistribution_shares
                
                logger.info(f"Redistributed {excess*100:.2f}% to {at_or_below.sum()} companies below threshold")
    else:
        logger.info(f"Collective cap satisfied (36% limit not exceeded)")
    
    # Verify final constraints
    final_above_threshold = current_weights > collective_threshold
    final_total_above = current_weights[final_above_threshold].sum()
    logger.info(f"Final: {final_above_threshold.sum()} companies above {collective_threshold*100}%, Total: {final_total_above*100:.2f}%")
    
    return current_weights