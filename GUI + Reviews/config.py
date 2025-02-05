# config.py
# Network paths
DLF_FOLDER = r"V:\PM-Indices-IndexOperations\General\Daily downloadfiles\Monthly Archive"
DATA_FOLDER = r"V:\PM-Indices-IndexOperations\Review Files"
DATA_FOLDER2 = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\GUI + Reviews"

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
    },
    "EGSPP": {
        "index": "EGSPP",
        "isin": "FRIX00003031",
        "output_key": "egspp_path"
    },
    "GICP": {
        "index": "GICP",
        "isin": "NLIX00005321",
        "output_key": "gicp_path"
    },
    "EDWPT": {
        "index": "EDWPT",
        "isin": "NLIX00001932",
        "output_key": "edwpt_path"
    },
    "EDWP": {
        "index": "EDWP",
        "isin": "NLIX00001577",
        "output_key": "edwp_path"
    },
    "F4RIP": {
        "index": "F4RIP",
        "isin": "FR0013376209",
        "output_key": "f4rip_path"
    },
    "SES5P": {
        "index": "SES5P",
        "isin": "NL0015000EF0",
        "output_key": "ses5p_path"
    },
    "AERDP": {
        "index": "AERDP",
        "isin": "NLIX00003086",
        "output_key": "aerdp_path"
    },
    "BNEW": {
        "index": "BNEW",
        "isin": "NL0011376116",
        "output_key": "bnew_path"
    },
    "AEXEW": {
        "index": "AEXEW",
        "isin": "QS0011159744",
        "output_key": "aexew_path"
    },
    "CACEW": {
        "index": "CACEW",
        "isin": "QS0011159777",
        "output_key": "cacew_path"
    },
    "CLEW": {
        "index": "CLEW",
        "isin": "FR0012663292",
        "output_key": "clew_path"
    },
    "SBF80": {
        "index": "SBF80",
        "isin": "FR0013017936",
        "output_key": "sbf80_path"
    }
    # Add new indices here following the same pattern
}

def get_index_config(review_type):
    """Get configuration for a specific review type"""
    review_type = review_type.upper()
    if review_type not in INDEX_CONFIGS:
        raise ValueError(f"Unknown review type: {review_type}")
    return INDEX_CONFIGS[review_type]