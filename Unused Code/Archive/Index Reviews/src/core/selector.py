import pandas as pd
import numpy as np
from typing import Dict, Tuple, Any, List, Optional

class IndexSelector:
    """Handles selection of index constituents after screening."""
    
    def __init__(self, data: Dict[str, pd.DataFrame], config: Dict[str, Any]):
        self.data = data
        self.config = config
    
    def select_stocks(self, screened_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Select stocks for both XPAR and non-XPAR categories.
        
        Args:
            screened_df: DataFrame after screening process
            
        Returns:
            Tuple of (XPAR selected stocks, non-XPAR selected stocks)
        """
        # Prepare selection dataframe
        selection_df = self._prepare_selection_df(screened_df)
        
        # Select stocks for each category
        xpar_selected = self._select_top_stocks(selection_df, 'XPAR', 20)
        noxpar_selected = self._select_top_stocks(selection_df, 'NOXPAR', 20)
        
        # Add price and other market data
        xpar_selected = self._add_market_data(xpar_selected)
        noxpar_selected = self._add_market_data(noxpar_selected)
        
        return xpar_selected, noxpar_selected
    
    def _prepare_selection_df(self, screened_df: pd.DataFrame) -> pd.DataFrame:
        """Prepare dataframe for selection process."""
        # Start with non-excluded companies
        selection_df = screened_df[screened_df['exclude'].isna()].copy()
        
        # Merge with job creation scores and staff ratings
        selection_df = selection_df.merge(
            self.data['sesamm'][['ISIN', 'Job_score_3Y']],
            on='ISIN',
            how='left'
        ).merge(
            self.data['oekom'][['ISIN', 'CRStaffRatingNum']],
            on='ISIN',
            how='left'
        )
        
        return selection_df
    
    def _select_top_stocks(self, df: pd.DataFrame, mic_type: str, n_stocks: int) -> pd.DataFrame:
        """
        Select top n stocks based on criteria for given MIC type.
        
        Args:
            df: Prepared selection DataFrame
            mic_type: 'XPAR' or 'NOXPAR'
            n_stocks: Number of stocks to select
            
        Returns:
            DataFrame with selected stocks
        """
        # Filter for MIC type
        if mic_type == 'XPAR':
            filtered_df = df[df['MIC'] == 'XPAR'].copy()
        else:  # NOXPAR
            filtered_df = df[df['MIC'] != 'XPAR'].copy()
        
        # Convert scoring columns to numeric
        filtered_df['Job_score_3Y'] = pd.to_numeric(filtered_df['Job_score_3Y'], errors='coerce')
        filtered_df['CRStaffRatingNum'] = pd.to_numeric(filtered_df['CRStaffRatingNum'], errors='coerce')
        
        # Sort by criteria
        sorted_df = filtered_df.sort_values(
            by=['Job_score_3Y', 'CRStaffRatingNum'],
            ascending=[False, False],
            na_position='last'
        )
        
        # Select top n stocks
        return sorted_df.head(n_stocks)
    
    def _add_market_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add price, FX rate, and free float data to selected stocks."""
        # Add Symbol, Price, and FX Rate
        df[['Symbol', 'Price', 'FX Rate']] = df.apply(
            lambda row: self._get_stock_info(row),
            axis=1
        )
        
        # Add Free Float
        df['Free Float'] = df.apply(
            lambda row: self._get_free_float(row),
            axis=1
        )
        
        # Calculate Price in Index Currency
        df['Price in Index Currency'] = df['Price'] * df['FX Rate']
        
        # Calculate Original Market Cap
        df['Original market cap'] = (
            df['Price in Index Currency'] * 
            df['NOSH'] * 
            df['Free Float']
        )
        
        return df
    
    def _get_stock_info(self, row: pd.Series) -> pd.Series:
        """Get stock information including FX rate."""
        stock_df = self.data['stock_eod']
        target_currency = self.config['currency']
        
        # Match for Symbol and Price
        mask = (
            (stock_df['Isin Code'] == row['ISIN']) & 
            (stock_df['MIC'] == row['MIC']) & 
            (stock_df['Reuters/Optiq'] == 'Reuters')
        )
        
        matches = stock_df[mask]
        
        if not matches.empty:
            first_match = matches.iloc[0]
            lookup_id5 = f"{first_match['#Symbol']}{target_currency}"
            
            # Find FX rate
            fx_mask = stock_df['id5'] == lookup_id5
            fx_matches = stock_df[fx_mask]
            
            fx_rate = fx_matches.iloc[0]['FX/Index Ccy'] if not fx_matches.empty else None
            
            return pd.Series({
                'Symbol': first_match['#Symbol'],
                'Price': first_match['Close Prc'],
                'FX Rate': fx_rate
            })
            
        return pd.Series({'Symbol': None, 'Price': None, 'FX Rate': None})
    
    def _get_free_float(self, row: pd.Series) -> Optional[float]:
        """Get Free Float value by matching ISIN."""
        mask = self.data['ff']['ISIN Code:'] == row['ISIN']
        matches = self.data['ff'][mask]
        
        if not matches.empty:
            return matches.iloc[0]['Free Float Round:']
        return None