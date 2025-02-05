# review_logic.py
import traceback
from .reviews.fri4p_review import run_fri4p_review
from .reviews.frd4p_review import run_frd4p_review
from .reviews.egspp_review import run_egspp_review
from .reviews.gicp_review import run_gicp_review
from .reviews.edwpt_review import run_edwpt_review
from .reviews.edwp_review import run_edwp_review
from .reviews.f4rip_review import run_f4rip_review
from .reviews.ses5p_review import run_ses5p_review
from .reviews.aerdp_review import run_aerdp_review
from .reviews.bnew_review import run_bnew_review
from .reviews.aexew_review import run_aexew_review
from .reviews.cacew_review import run_cacew_review
from .reviews.clew_review import run_clew_review

# Dictionary mapping review types to their corresponding functions
REVIEW_FUNCTIONS = {
    "FRI4P": run_fri4p_review,
    "FRD4P": run_frd4p_review,
    "EGSPP": run_egspp_review,
    "GICP": run_gicp_review,
    "EDWPT": run_edwpt_review,
    "EDWP": run_edwp_review,
    "F4RIP": run_f4rip_review,
    "SES5P": run_ses5p_review,
    "AERDP": run_aerdp_review,
    "BNEW": run_bnew_review,
    "AEXEW": run_aexew_review,
    "CACEW": run_cacew_review,
    "CLEW": run_clew_review
}

def run_review(review_type, **kwargs):
    """
    Route to appropriate review based on type

    Args:
        review_type (str): Type of review to run
        **kwargs: Arguments to pass to the review function

    Returns:
        dict: Result of the review containing status, message, and data
    """
    try:
        review_function = REVIEW_FUNCTIONS.get(review_type.upper())
        if review_function is None:
            raise ValueError(f"Unknown review type: {review_type}")
            
        return review_function(**kwargs)
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error in {review_type} review: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None
        }