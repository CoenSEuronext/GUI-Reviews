import os
import logging
from datetime import datetime
import pandas as pd
from pathlib import Path
from typing import Optional

def read_semicolon_csv(filepath: str, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Read a semicolon-delimited CSV file into a pandas DataFrame.
    
    Args:
        filepath: Path to the CSV file
        encoding: File encoding (default: 'utf-8')
        
    Returns:
        pandas.DataFrame: The loaded data
    """
    try:
        return pd.read_csv(
            filepath,
            sep=';',
            encoding=encoding,
            decimal=',',
            thousands='.'
        )
    except Exception as e:
        logging.error(f"Error reading CSV file {filepath}: {str(e)}")
        raise

def setup_logging(logger_name: str) -> logging.Logger:
    """
    Set up logging configuration for the application.
    
    Args:
        logger_name: Name for the logger
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d')
    log_file = log_dir / f"{logger_name}_{timestamp}.log"
    
    # Configure logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def load_config(config_path: str) -> dict:
    """
    Load YAML configuration file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        dict: Configuration data
    """
    import yaml
    
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Error loading config file {config_path}: {str(e)}")
        raise

def ensure_directory_exists(path: str) -> None:
    """
    Ensure that a directory exists, creating it if necessary.
    
    Args:
        path: Directory path to check/create
    """
    Path(path).mkdir(parents=True, exist_ok=True)

def get_output_path(base_dir: str, index_name: str) -> Path:
    """
    Generate output path for index review results.
    
    Args:
        base_dir: Base directory for output
        index_name: Name of the index
        
    Returns:
        Path: Complete output path
    """
    current_date = datetime.now().strftime('%Y%m')
    output_path = Path(base_dir) / current_date / index_name
    ensure_directory_exists(output_path)
    return output_path

def format_number(value: float, decimals: int = 2) -> str:
    """
    Format number with thousands separator and fixed decimals.
    
    Args:
        value: Number to format
        decimals: Number of decimal places
        
    Returns:
        str: Formatted number string
    """
    return f"{value:,.{decimals}f}"

def validate_file_exists(filepath: str) -> bool:
    """
    Check if a file exists.
    
    Args:
        filepath: Path to the file to check
        
    Returns:
        bool: True if file exists, False otherwise
    """
    return Path(filepath).is_file()