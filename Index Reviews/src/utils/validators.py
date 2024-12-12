# src/utils/validators.py
from typing import Any, List, Dict, Union, Optional
import pandas as pd
import numpy as np
from pathlib import Path
from .constants import (
    ALLOWED_CURRENCIES,
    REQUIRED_COLUMNS,
    MIN_MARKET_CAP,
    MAX_WEIGHT,
    ALLOWED_AREAS,
    HIGH_IMPACT_NACE_CODES
)

class DataValidator:
    """Validates data inputs and configurations for index reviews."""
    
    @staticmethod
    def validate_input_files(paths: Dict[str, str]) -> bool:
        """
        Validate that all required input files exist.
        
        Args:
            paths: Dictionary of file paths to check
            
        Returns:
            bool: True if all files exist
            
        Raises:
            FileNotFoundError: If any required file is missing
        """
        for name, path in paths.items():
            if not Path(path).is_file():
                raise FileNotFoundError(f"Required file not found: {name} at {path}")
        return True
    
    @staticmethod
    def validate_dataframe_columns(df: pd.DataFrame, required_columns: List[str]) -> bool:
        """
        Validate that DataFrame contains all required columns.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            
        Returns:
            bool: True if all required columns exist
            
        Raises:
            ValueError: If any required columns are missing
        """
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        return True
    
    @staticmethod
    def validate_market_data(df: pd.DataFrame) -> bool:
        """
        Validate market data requirements.
        
        Args:
            df: DataFrame containing market data
            
        Returns:
            bool: True if data meets requirements
            
        Raises:
            ValueError: If data doesn't meet requirements
        """
        # Check for missing values
        if df['NOSH'].isna().any():
            raise ValueError("Missing values found in NOSH column")
            
        # Check for negative values
        if (df['NOSH'] < 0).any():
            raise ValueError("Negative values found in NOSH column")
            
        # Check market cap threshold
        market_cap = df['Price'] * df['NOSH']
        if (market_cap < MIN_MARKET_CAP).any():
            raise ValueError(f"Companies found with market cap below {MIN_MARKET_CAP}")
            
        return True
    
    @staticmethod
    def validate_currency(currency: str) -> bool:
        """
        Validate that currency is allowed.
        
        Args:
            currency: Currency code to validate
            
        Returns:
            bool: True if currency is allowed
            
        Raises:
            ValueError: If currency is not allowed
        """
        if currency not in ALLOWED_CURRENCIES:
            raise ValueError(f"Invalid currency: {currency}. Must be one of {ALLOWED_CURRENCIES}")
        return True
    
    @staticmethod
    def validate_weights(weights: pd.Series) -> bool:
        """
        Validate weight constraints.
        
        Args:
            weights: Series of weights to validate
            
        Returns:
            bool: True if weights meet requirements
            
        Raises:
            ValueError: If weights don't meet requirements
        """
        # Check sum of weights
        if not np.isclose(weights.sum(), 1.0, rtol=1e-5):
            raise ValueError("Weights do not sum to 1.0")
            
        # Check maximum weight
        if (weights > MAX_WEIGHT).any():
            raise ValueError(f"Weights found exceeding maximum allowed weight of {MAX_WEIGHT}")
            
        # Check for negative weights
        if (weights < 0).any():
            raise ValueError("Negative weights found")
            
        return True
    
    @staticmethod
    def validate_area(area: str) -> bool:
        """
        Validate geographical area.
        
        Args:
            area: Area code to validate
            
        Returns:
            bool: True if area is valid
            
        Raises:
            ValueError: If area is not valid
        """
        if area not in ALLOWED_AREAS:
            raise ValueError(f"Invalid area: {area}. Must be one of {ALLOWED_AREAS}")
        return True
    
    @staticmethod
    def validate_nace_code(code: str) -> bool:
        """
        Validate NACE code format and content.
        
        Args:
            code: NACE code to validate
            
        Returns:
            bool: True if code is valid
            
        Raises:
            ValueError: If code is not valid
        """
        if not code or not isinstance(code, str):
            raise ValueError("Invalid NACE code format")
            
        if code not in HIGH_IMPACT_NACE_CODES:
            raise ValueError(f"NACE code {code} is not in high impact sectors")
            
        return True