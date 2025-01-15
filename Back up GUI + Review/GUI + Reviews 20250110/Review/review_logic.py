#review_logic.py
import traceback
from config import DLF_FOLDER, DATA_FOLDER
from .reviews.fri4p_review import run_fri4p_review
from .reviews.frd4p_review import run_frd4p_review
from .reviews.egspp_review import run_egspp_review
from .reviews.gicp_review import run_gicp_review

def run_review(review_type, **kwargs):
    """
    Route to appropriate review based on type
    """
    try:
        if review_type.upper() == "FRI4P":
            return run_fri4p_review(**kwargs)
        elif review_type.upper() == "FRD4P":
            return run_frd4p_review(**kwargs)
        elif review_type.upper() == "EGSPP":
            return run_egspp_review(**kwargs)
        elif review_type.upper() == "GICP":
            return run_gicp_review(**kwargs)
        else:
            raise ValueError(f"Unknown review type: {review_type}")
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error in {review_type} review: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None
        }