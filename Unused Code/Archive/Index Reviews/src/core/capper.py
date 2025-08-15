import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple

class IndexCapper:
    """Handles capping of index weights."""
    
    def __init__(self, data: Dict[str, pd.DataFrame], config: Dict[str, Any]):
        self.data = data
        self.config = config
    
    def apply_capping(
        self, 
        xpar_df: pd.DataFrame, 
        noxpar_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Apply capping process to both XPAR and non-XPAR stocks.
        
        Args:
            xpar_df: Selected XPAR stocks
            noxpar_df: Selected non-XPAR stocks
            
        Returns:
            DataFrame with final capped weights
        """
        # Calculate initial market caps
        index_mkt_cap = self._get_index_market_cap()
        ffmc_world = noxpar_df['Original market cap'].sum()
        ffmc_france = xpar_df['Original market cap'].sum()
        ffmc_total = ffmc_france + ffmc_world
        
        # Calculate initial weights
        xpar_df['Weight'] = xpar_df['Original market cap'] / ffmc_france
        noxpar_df['Weight'] = noxpar_df['Original market cap'] / ffmc_world
        
        # Initialize capping
        xpar_df['Capping 1'] = xpar_df['Weight'].apply(lambda x: 1 if x > 0.2 else 0)
        noxpar_df['Capping 1'] = noxpar_df['Weight'].apply(lambda x: 1 if x > 0.2 else 0)
        
        # Apply iterative capping
        for step in [1, 2]:
            xpar_df = self._apply_capping_step(xpar_df, step)
            noxpar_df = self._apply_capping_step(noxpar_df, step)
        
        # Final capping step
        xpar_df = self._apply_capping_step(xpar_df, 3, final_step=True)
        noxpar_df = self._apply_capping_step(noxpar_df, 3, final_step=True)
        
        # Calculate final capping factors
        xpar_df, noxpar_df = self._calculate_final_capping(
            xpar_df, 
            noxpar_df, 
            ffmc_total
        )
        
        # Combine and format final output
        return self._prepare_final_output(xpar_df, noxpar_df)
    
    def _get_index_market_cap(self) -> float:
        """Get market cap for the index."""
        return self.data['index_eod'][
            self.data['index_eod']['IsinCode'] == self.config['isin']
        ]['Mkt Cap'].iloc[0]
    
    def _apply_capping_step(
        self, 
        df: pd.DataFrame, 
        step: int, 
        cap_threshold: float = 0.2, 
        final_step: bool = False
    ) -> pd.DataFrame:
        """Apply a single capping step."""
        current_step = step
        next_step = step + 1
        
        # Use previous Mcap if available, otherwise use Original market cap
        prev_mcap = f'Mcap {current_step-1}' if current_step > 1 else 'Original market cap'
        
        # Count capped items and calculate new market cap
        n_capping = (df[f'Capping {current_step}'] == 1).sum()
        perc_no_cap = 1 - (n_capping * cap_threshold)
        mcap_capping = df[df[f'Capping {current_step}'] == 1][prev_mcap].sum()
        new_mcap = (df[prev_mcap].sum() - mcap_capping) / perc_no_cap
        
        # Calculate new market cap and weight
        df[f'Mcap {current_step}'] = df.apply(
            lambda row: cap_threshold * new_mcap 
            if row[f'Capping {current_step}'] == 1 
            else row[prev_mcap],
            axis=1
        )
        df[f'Weight {current_step}'] = df[f'Mcap {current_step}'] / new_mcap
        
        # Only add next Capping if not the final step
        if not final_step:
            df[f'Capping {next_step}'] = df[f'Weight {current_step}'].apply(
                lambda x: 1 if x > cap_threshold else 0
            )
        
        return df
    
    def _calculate_final_capping(
        self, 
        xpar_df: pd.DataFrame, 
        noxpar_df: pd.DataFrame, 
        ffmc_total: float
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Calculate final capping factors."""
        # Calculate initial capping factors
        xpar_df['Final Capping'] = (
            xpar_df['Weight 3'] * ffmc_total
        ) / xpar_df['Original market cap']
        
        noxpar_df['Final Capping'] = (
            noxpar_df['Weight 3'] * ffmc_total
        ) / noxpar_df['Original market cap']
        
        # Combine and normalize
        combined_df = pd.concat([xpar_df, noxpar_df])
        max_capping = combined_df['Final Capping'].max()
        
        xpar_df['Final Capping'] = (
            xpar_df['Final Capping'] / max_capping
        ).round(14)
        
        noxpar_df['Final Capping'] = (
            noxpar_df['Final Capping'] / max_capping
        ).round(14)
        
        return xpar_df, noxpar_df
    
    def _prepare_final_output(
        self, 
        xpar_df: pd.DataFrame, 
        noxpar_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Prepare final output DataFrame."""
        # Combine dataframes
        final_df = pd.concat([xpar_df, noxpar_df])
        
        # Add effective date
        final_df['Effective Date of Review'] = self.config['effective_date']
        
        # Select and rename required columns
        output_df = final_df[[
            'Name',
            'ISIN',
            'MIC',
            'NOSH',
            'Free Float',
            'Final Capping',
            'Effective Date of Review',
            'Currency (Local)'
        ]].copy()
        
        # Rename columns
        output_df = output_df.rename(columns={
            'Currency (Local)': 'Currency'
        })
        
        # Sort by name
        output_df = output_df.sort_values('Name')
        
        return output_df