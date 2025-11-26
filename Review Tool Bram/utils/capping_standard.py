import pandas as pd
import numpy as np
from utils.logging_utils import setup_logging

logger = setup_logging(__name__)

def calculate_capped_weights(weights, cap_limit=0.1, max_iterations=100, tolerance=1e-8):
    """
    Calculate capped weights with iterative redistribution of excess weight.
    Companies exceeding the cap will have exactly cap_limit weight (10%).
    Excess weight is redistributed proportionally to companies below the cap
    based on their ORIGINAL weights.
    """
    # Store original weights for proportional redistribution
    original_weights = weights.copy()
    current_weights = weights.copy()
    is_capped = pd.Series(False, index=weights.index)
    
    # Iterative redistribution
    for iteration in range(max_iterations):
        needs_capping = (current_weights > cap_limit) & (~is_capped)
        
        if not needs_capping.any():
            break
            
        excess_weight = (current_weights[needs_capping] - cap_limit).sum()
        current_weights[needs_capping] = cap_limit
        is_capped[needs_capping] = True
        
        can_receive = (~is_capped) & (current_weights < cap_limit)
        
        if not can_receive.any() or excess_weight <= tolerance:
            break
            
        eligible_original_weights = original_weights[can_receive]
        total_eligible_original = eligible_original_weights.sum()
        
        if total_eligible_original <= tolerance:
            break
            
        redistribution_shares = eligible_original_weights / total_eligible_original
        additional_weights = excess_weight * redistribution_shares
        current_weights[can_receive] += additional_weights
    
    return current_weights