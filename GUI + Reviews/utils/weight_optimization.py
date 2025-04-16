"""
Weight optimization module for PAB indices.
This module contains functions to implement the PAB weighting procedure described in section 2.4.6.
"""

import numpy as np
import pandas as pd
from scipy.optimize import minimize
import logging

def optimize_weights(final_selection, universe_df, WACI, date):
    """
    Implement the PAB weighting optimization procedure as described in section 2.4.6
    
    Args:
        final_selection (pd.DataFrame): DataFrame with the final selection of companies
        universe_df (pd.DataFrame): DataFrame with the full universe
        WACI (float): Current WACI value
        date (str): Calculation date in format YYYYMMDD
        
    Returns:
        pd.DataFrame: DataFrame with the optimized weights
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting PAB weighting optimization procedure...")
    
    # Set a copy of final selection to work with
    portfolio = final_selection.copy()
    
    # 1. Get necessary variables for optimization
    # Total FFMC from universe for 70% constraint
    total_universe_ffmc = universe_df['FFMC_WD'].sum()
    total_portfolio_ffmc = portfolio['FFMC_WD'].sum()
    logger.info(f"Total FFMC in universe: {total_universe_ffmc:.2f}")
    logger.info(f"Total FFMC in portfolio: {total_portfolio_ffmc:.2f}")
    logger.info(f"Portfolio is {total_portfolio_ffmc/total_universe_ffmc*100:.2f}% of universe FFMC")
    
    # Number of companies
    n_companies = len(portfolio)
    logger.info(f"Optimizing weights for {n_companies} companies")
    
    # Get EU Taxonomy pocket size
    eu_taxonomy_count = portfolio['EU_Taxonomy'].sum()
    non_eu_taxonomy_count = n_companies - eu_taxonomy_count
    logger.info(f"EU Taxonomy companies: {eu_taxonomy_count}")
    logger.info(f"Non-EU Taxonomy companies: {non_eu_taxonomy_count}")
    
    # Get High Climate Impact section weights
    universe_hci = universe_df[universe_df['High_Climate_Impact'] == 1]
    universe_hci_weight = universe_hci['FFMC_WD'].sum() / total_universe_ffmc
    logger.info(f"Universe High Climate Impact weight: {universe_hci_weight:.4f}")
    
    # Get PAB plan ratio in universe
    pab_universe_ratio = universe_df[universe_df['PAB_Plan_Flag'] == 1]['FFMC_WD'].sum() / total_universe_ffmc
    logger.info(f"Universe Paris Agreement Plan ratio: {pab_universe_ratio:.4f}")
    
    # Get the market cap weights for the supersectors in the universe
    supersector_weights_universe = universe_df.groupby('Supersector Code')['FFMC_WD'].sum() / total_universe_ffmc
    logger.info(f"Calculated weights for {len(supersector_weights_universe)} supersectors in universe")
    
    # 2. Prepare optimization inputs
    # Create a matrix showing which company belongs to which supersector
    supersectors = sorted(portfolio['Supersector Code'].unique())
    n_sectors = len(supersectors)
    
    # Create a mapping of supersector code to index
    sector_to_idx = {sector: idx for idx, sector in enumerate(supersectors)}
    
    # Create the sector matrix (binary matrix indicating if company i belongs to sector j)
    sector_matrix = np.zeros((n_companies, n_sectors))
    for i, (_, company) in enumerate(portfolio.iterrows()):
        sector_idx = sector_to_idx.get(company['Supersector Code'])
        if sector_idx is not None:
            sector_matrix[i, sector_idx] = 1
    
    # Get the target supersector weights from universe
    target_sector_weights = np.zeros(n_sectors)
    for j, sector in enumerate(supersectors):
        if sector in supersector_weights_universe:
            target_sector_weights[j] = supersector_weights_universe[sector]
    
    # 3. Define optimization objective function
    def objective_function(weights):
        """
        Minimize the squared deviation between portfolio sector weights and target sector weights
        """
        portfolio_sector_weights = np.dot(weights, sector_matrix)
        return np.sum((portfolio_sector_weights - target_sector_weights) ** 2)
    
    # 4. Define constraints
    constraints = []
    
    # All weights sum to 1
    constraints.append({'type': 'eq', 'fun': lambda w: np.sum(w) - 1})
    
    # Free Float Market Cap constraint (<=70% of universe)
    # We'll implement this by ensuring the weighted sum of FFMC_WD is <= 70% of total_universe_ffmc
    ffmc_values = portfolio['FFMC_WD'].values
    constraints.append({
        'type': 'ineq', 
        'fun': lambda w: 0.7 * total_universe_ffmc - np.sum(w * ffmc_values)
    })
    
    # EU Taxonomy pocket constraints (5% <= weight <= 10%)
    eu_taxonomy_mask = portfolio['EU_Taxonomy'].values == 1
    constraints.append({'type': 'ineq', 'fun': lambda w: np.sum(w[eu_taxonomy_mask]) - 0.05})  # >= 5%
    constraints.append({'type': 'ineq', 'fun': lambda w: 0.10 - np.sum(w[eu_taxonomy_mask])})  # <= 10%
    
    # High Climate Impact section weight constraint
    # High Stake NACE Section weight in Index >= High Stake NACE Section weight in Universe
    hci_mask = portfolio['High_Climate_Impact'].values == 1
    constraints.append({
        'type': 'ineq', 
        'fun': lambda w: np.sum(w[hci_mask]) - universe_hci_weight
    })
    
    # WACI reduction constraint (>=50% reduction vs universe)
    ci_values = portfolio['CI'].values
    constraints.append({
        'type': 'ineq',
        'fun': lambda w: 0.5 * WACI - np.sum(w * ci_values)
    })
    
    # Year-on-year self-decarbonization trajectory constraint (7% annual reduction)
    # Note: We need to calculate base year WACI, which should be 254.12 as per documentation
    base_year_waci = 254.12  # As provided in the documentation
    
    # Calculate how many years have passed since base year (assuming base year is 2024)
    year = int(date[:4])
    base_year = 2024
    years_passed = max(0, year - base_year)
    
    # Target WACI for current year
    target_waci = base_year_waci * ((1 - 0.07) ** years_passed)
    
    logger.info(f"Base year WACI: {base_year_waci}")
    logger.info(f"Years passed: {years_passed}")
    logger.info(f"Target WACI for current year: {target_waci}")
    
    constraints.append({
        'type': 'ineq',
        'fun': lambda w: target_waci - np.sum(w * ci_values)
    })
    
    # Temperature improvement constraint
    # More companies with Paris Agreement Plan in index than in universe
    # Note: In our case PAB_Plan_Flag indicates companies with temperature score <= 2°C
    pab_mask = portfolio['PAB_Plan_Flag'].values == 1
    
    constraints.append({
        'type': 'ineq',
        'fun': lambda w: np.sum(w[pab_mask]) - pab_universe_ratio
    })
    
    # 35% of HCI companies must have plans in line with Paris Agreement
    hci_pab_mask = (portfolio['High_Climate_Impact'].values == 1) & (portfolio['PAB_Plan_Flag'].values == 1)
    constraints.append({
        'type': 'ineq',
        'fun': lambda w: np.sum(w[hci_pab_mask]) - 0.35 * np.sum(w[hci_mask])
    })
    
    # Liquidity constraints for each company
    for i in range(n_companies):
        # Weight <= Liquidity_Cap
        constraints.append({
            'type': 'ineq',
            'fun': lambda w, i=i: portfolio.iloc[i]['Liquidity_Cap'] - w[i]
        })
        
        # Weight >= Liquidity_Floor
        constraints.append({
            'type': 'ineq',
            'fun': lambda w, i=i: w[i] - portfolio.iloc[i]['Liquidity_Floor']
        })
    
    # Calculate total FFMC_WD for portfolio
    total_ffmc_wd = portfolio['FFMC_WD'].sum()
    
    # Calculate initial weights according to specific rules:
    # - For EU Taxonomy: equal weight within the pocket (10% pocket total)
    # - For Non-EU Taxonomy: based on FFMC weight within that pocket (90% pocket total)
    logger.info("Creating initial guess for optimization according to specified rules")
    
    # Identify EU Taxonomy and Non-EU Taxonomy companies
    eu_taxonomy_mask = portfolio['EU_Taxonomy'].values == 1
    non_eu_taxonomy_mask = ~eu_taxonomy_mask
    
    # Count companies in each pocket
    n_eu_taxonomy = eu_taxonomy_mask.sum()
    n_non_eu_taxonomy = non_eu_taxonomy_mask.sum()
    
    logger.info(f"EU Taxonomy pocket: {n_eu_taxonomy} companies (target: 10% of total weight)")
    logger.info(f"Non-EU Taxonomy pocket: {n_non_eu_taxonomy} companies (target: 90% of total weight)")
    
    # Create initial weights array
    initial_weights = np.zeros(n_companies)
    
    # For EU Taxonomy: equal weight within the pocket
    if n_eu_taxonomy > 0:
        eu_equal_weight = 1.0 / n_eu_taxonomy
        initial_weights[eu_taxonomy_mask] = eu_equal_weight
    
    # For Non-EU Taxonomy: based on FFMC weight within the pocket
    if n_non_eu_taxonomy > 0:
        # Calculate FFMC for non-EU companies
        non_eu_ffmc = portfolio.loc[non_eu_taxonomy_mask, 'FFMC_WD'].values
        non_eu_ffmc_sum = non_eu_ffmc.sum()
        
        # Set weights proportional to FFMC within the non-EU pocket
        if non_eu_ffmc_sum > 0:
            initial_weights[non_eu_taxonomy_mask] = non_eu_ffmc / non_eu_ffmc_sum
    
    # Normalize to ensure EU Taxonomy pocket = 10% and Non-EU Taxonomy pocket = 90%
    eu_sum = initial_weights[eu_taxonomy_mask].sum()
    non_eu_sum = initial_weights[non_eu_taxonomy_mask].sum()
    
    if eu_sum > 0:
        initial_weights[eu_taxonomy_mask] = initial_weights[eu_taxonomy_mask] * 0.10 / eu_sum
    
    if non_eu_sum > 0:
        initial_weights[non_eu_taxonomy_mask] = initial_weights[non_eu_taxonomy_mask] * 0.90 / non_eu_sum
    
    # Verify the initial weights
    total_weight = initial_weights.sum()
    eu_weight = initial_weights[eu_taxonomy_mask].sum()
    non_eu_weight = initial_weights[non_eu_taxonomy_mask].sum()
    
    logger.info(f"Initial weights - Total: {total_weight:.6f}, EU: {eu_weight:.6f} (10%), Non-EU: {non_eu_weight:.6f} (90%)")
    
    # Verify if initial weights satisfy basic constraints
    initial_waci = np.sum(initial_weights * ci_values)
    logger.info(f"Initial WACI: {initial_waci:.2f} (targets: < {0.5 * WACI:.2f} and < {target_waci:.2f})")
    
    # Try optimization with increasing factors until a solution is found
    max_factor = 20
    successful = False
    
    # Set initial factor
    factor = 2
    
    while factor <= max_factor and not successful:
        logger.info(f"Attempting optimization with factor = {factor}")
        
        # Calculate FFMC based weights for constraint bounds only (not as initial guess)
        ffmc_weights = portfolio['FFMC_WD'].values / total_ffmc_wd
        
        # Update the FFMC weight factor constraints
        ffmc_constraints = []
        for i in range(n_companies):
            # Weight <= FFMC_weight * factor
            ffmc_constraints.append({
                'type': 'ineq',
                'fun': lambda w, i=i: ffmc_weights[i] * factor - w[i]
            })
            
            # Weight >= FFMC_weight / factor
            ffmc_constraints.append({
                'type': 'ineq',
                'fun': lambda w, i=i: w[i] - ffmc_weights[i] / factor
            })
        
        # Combine all constraints
        all_constraints = constraints + ffmc_constraints
        
        # Set bounds for all weights (0 to 1)
        bounds = [(0, 1) for _ in range(n_companies)]
                
        try:
            # Perform the optimization using our specialized initial weights
            logger.info(f"Starting optimization with {len(all_constraints)} constraints")
            result = minimize(
                objective_function,
                initial_weights,
                method='SLSQP',
                bounds=bounds,
                constraints=all_constraints,
                options={'maxiter': 1000, 'disp': True}
            )
            
            if result.success:
                logger.info(f"Optimization successful with factor = {factor}")
                successful = True
                optimized_weights = result.x
                # Store the successful factor for reporting
                final_factor = factor
            else:
                logger.warning(f"Optimization failed with factor = {factor}: {result.message}")
                factor += 1
                
        except Exception as e:
            logger.error(f"Error during optimization with factor = {factor}: {str(e)}")
            factor += 1
    
    if not successful:
        logger.error("Failed to find optimal weights after trying all relaxation steps")
        # Set a default value for final_factor to indicate failure
        final_factor = None
        return portfolio
    
    # Add optimized weights to the portfolio DataFrame
    portfolio['Optimized_Weight'] = optimized_weights
    # Store the factor used for the successful optimization
    portfolio['Optimization_Factor'] = final_factor
    
    # Calculate new WACI with optimized weights
    portfolio_waci = np.sum(portfolio['CI'] * portfolio['Optimized_Weight'])
    logger.info(f"Portfolio WACI after optimization: {portfolio_waci}")
    logger.info(f"Achieved {(1 - portfolio_waci/WACI)*100:.2f}% reduction vs universe WACI")
    
    # Calculate sector weights with optimized weights
    portfolio_sector_weights = {}
    for sector in supersectors:
        sector_companies = portfolio[portfolio['Supersector Code'] == sector]
        sector_weight = sector_companies['Optimized_Weight'].sum()
        portfolio_sector_weights[sector] = sector_weight
        
    # Calculate EU taxonomy pocket weight
    eu_taxonomy_weight = portfolio[portfolio['EU_Taxonomy'] == 1]['Optimized_Weight'].sum()
    logger.info(f"EU Taxonomy pocket weight: {eu_taxonomy_weight*100:.2f}%")
    
    # Calculate High Climate Impact section weight
    hci_weight = portfolio[portfolio['High_Climate_Impact'] == 1]['Optimized_Weight'].sum()
    logger.info(f"High Climate Impact section weight: {hci_weight*100:.2f}%")
    
    # Calculate PAB plan weight
    pab_weight = portfolio[portfolio['PAB_Plan_Flag'] == 1]['Optimized_Weight'].sum()
    logger.info(f"Companies with Paris Agreement Plan weight: {pab_weight*100:.2f}%")
    
    # Calculate HCI with PAB plan weight
    hci_pab_weight = portfolio[(portfolio['High_Climate_Impact'] == 1) & (portfolio['PAB_Plan_Flag'] == 1)]['Optimized_Weight'].sum()
    logger.info(f"HCI companies with Paris Agreement Plan weight: {hci_pab_weight*100:.2f}%")
    logger.info(f"Percentage of HCI weight with Paris Agreement Plan: {hci_pab_weight/hci_weight*100:.2f}%")
    
    # Check if any weights are too small or too large
    min_weight = portfolio['Optimized_Weight'].min()
    max_weight = portfolio['Optimized_Weight'].max()
    logger.info(f"Minimum weight: {min_weight*100:.6f}%")
    logger.info(f"Maximum weight: {max_weight*100:.6f}%")
    
    return portfolio