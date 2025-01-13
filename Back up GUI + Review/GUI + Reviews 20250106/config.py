# config.py
import os
from datetime import datetime

# Network paths
DLF_FOLDER = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"
DATA_FOLDER = r"V:\PM-Indices-IndexOperations\Review Files"

# Index Configurations
INDEX_CONFIGS = {
    "FRI4P": {
        "index": "FRI4P",
        "isin": "FRIX00003643",
        "output_key": "fri4p_path"
    },
    "FRD4P": {
        "index": "FRD4P",
        "isin": "FRIX00003031",
        "output_key": "frd4p_path"
    }
    # Add new indices here following the same pattern
}

# Default values (can be derived from INDEX_CONFIGS if desired)
DEFAULT_INDEX = "FRI4P"
DEFAULT_ISIN = INDEX_CONFIGS["FRI4P"]["isin"]
DEFAULT_CURRENCY = "EUR"

def get_index_config(review_type):
    """Get configuration for a specific review type"""
    return INDEX_CONFIGS.get(review_type.upper(), INDEX_CONFIGS["FRI4P"])