#review_logic.py
import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging
import traceback
from Review.functions import read_semicolon_csv
from config import DLF_FOLDER, DATA_FOLDER
from .reviews.fri4p_review import run_fri4p_review
from .reviews.frd4p_review import run_frd4p_review

def run_review(review_type, **kwargs):
    """
    Route to appropriate review based on type
    """
    try:
        if review_type.upper() == "FRI4P":
            return run_fri4p_review(**kwargs)
        elif review_type.upper() == "FRD4P":
            return run_frd4p_review(**kwargs)
        else:
            raise ValueError(f"Unknown review type: {review_type}")
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error in {review_type} review: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None
        }