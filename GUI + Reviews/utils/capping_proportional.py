# utils/capping.py

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def apply_proportional_capping(df, mcap_column='Mcap in EUR', max_weight=0.1, max_iterations=100):
    """
    Apply proportional capping to a DataFrame of stocks.
    
    Redistributes excess weight from capped stocks proportionally to uncapped stocks
    until all stocks are below the maximum weight threshold.
    
    Args:
        df (pd.DataFrame): DataFrame containing stocks to be capped
        mcap_column (str): Column name containing market capitalization values. Defaults to 'Mcap in EUR'
        max_weight (float): Maximum individual stock weight (e.g., 0.1 for 10%). Defaults to 0.1
        max_iterations (int): Maximum number of iterations to avoid infinite loops. Defaults to 100
    
    Returns:
        pd.DataFrame: Original DataFrame with added columns:
            - 'Initial Weight': Initial weight based on market cap
            - 'Current Weight': Final weight after capping
            - 'Capping Factor': Factor to apply (Current Weight * Total Mcap) / Original Mcap
            - 'Is Capped': Boolean indicating if stock is capped at max_weight
    
    Raises:
        ValueError: If mcap_column is not found in DataFrame
        Exception: If capping procedure doesn't converge within max_iterations
    """
    
    # Validate input
    if mcap_column not in df.columns:
        raise ValueError(f"Column '{mcap_column}' not found in DataFrame")
    
    if df[mcap_column].isna().any():
        logger.warning(f"NaN values found in '{mcap_column}' column. These rows will be excluded from capping.")
        df = df[df[mcap_column].notna()].copy()
    
    if len(df) == 0:
        raise ValueError("DataFrame is empty or all market cap values are NaN")
    
    # Calculate total market cap
    total_mcap = df[mcap_column].sum()
    
    if total_mcap <= 0:
        raise ValueError(f"Total market cap must be positive, got {total_mcap}")
    
    logger.info(f"Starting proportional capping with max weight: {max_weight * 100}%")
    
    # Calculate initial weights
    df = df.copy()
    df['Initial Weight'] = df[mcap_column] / total_mcap
    df['Current Weight'] = df['Initial Weight'].copy()
    df['Capping Factor'] = 1.0
    df['Is Capped'] = False
    
    # Iterative capping process
    for iteration in range(max_iterations):
        # Check if individual cap is satisfied (using rounding to 14 decimals for floating point precision)
        companies_above_cap = df[df['Current Weight'].round(14) > max_weight].shape[0]
        
        if companies_above_cap == 0:
            logger.info(f"Capping procedure converged after {iteration} iterations")
            break
        
        logger.debug(f"Iteration {iteration + 1}: {companies_above_cap} companies above {max_weight * 100}% cap")
        
        # Count how many companies need capping
        to_cap_count = df[df['Current Weight'].round(14) >= max_weight].shape[0]
        
        # Compute how much weight should be allocated to the non-capped companies
        final_weight_uncapped = 1.0 - (max_weight * to_cap_count)
        
        # Compute how much weight the non-capped companies currently have
        initial_weight_uncapped = df[df['Current Weight'].round(14) < max_weight]['Current Weight'].sum()
        
        # Calculate weight increase ratio
        if initial_weight_uncapped > 0:
            weight_increase_ratio = (final_weight_uncapped / initial_weight_uncapped) - 1
        else:
            # All companies need capping or edge case
            weight_increase_ratio = 0
        
        # Apply capping: cap at max_weight or increase proportionally
        df['Current Weight'] = np.where(
            df['Current Weight'].round(14) >= max_weight,
            max_weight,
            df['Current Weight'] * (1 + weight_increase_ratio)
        )
        
        # Mark companies that are now capped
        df['Is Capped'] = df['Current Weight'].round(14) >= max_weight
        
        # Update capping factors based on new weights
        # Capping Factor = (Current Weight * Total Market Cap) / Original Market Cap
        df['Capping Factor'] = (df['Current Weight'] * total_mcap) / df[mcap_column]
        
        # Normalize capping factors so the maximum is 1.0
        max_capping = df['Capping Factor'].max()
        if max_capping > 0 and np.isfinite(max_capping):
            df['Capping Factor'] = df['Capping Factor'] / max_capping
    else:
        raise Exception(f"Capping procedure did not converge after {max_iterations} iterations")
    
    # Log capping summary
    capped_companies = df['Is Capped'].sum()
    total_weight = df['Current Weight'].sum()
    max_final_weight = df['Current Weight'].max()
    
    logger.info(f"Capping Summary:")
    logger.info(f"  Total companies: {len(df)}")
    logger.info(f"  Companies capped: {capped_companies}")
    logger.info(f"  Weight sum after capping: {total_weight:.6f}")
    logger.info(f"  Max weight after capping: {max_final_weight:.6f}")
    
    return df