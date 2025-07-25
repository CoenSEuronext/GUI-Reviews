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
from .reviews.sbf80_review import run_sbf80_review
from .reviews.wifrp_review import run_wifrp_review
from .reviews.lc100_review import run_lc100_review
from .reviews.lc3wp_review import run_lc3wp_review
from .reviews.lc1ep_review import run_lc1ep_review
from .reviews.frecp_review import run_frecp_review
from .reviews.frn4p_review import run_frn4p_review
from .reviews.fr20p_review import run_fr20p_review
from .reviews.ez40p_review import run_ez40p_review
from .reviews.ez60p_review import run_ez60p_review
from .reviews.ez15p_review import run_ez15p_review
from .reviews.ezn1p_review import run_ezn1p_review
from .reviews.efmep_review import run_efmep_review
from .reviews.eri5p_review import run_eri5p_review
from .reviews.be1p_review import run_be1p_review
from .reviews.eus5p_review import run_eus5p_review
from .reviews.edefp_review import run_edefp_review
from .reviews.etpfb_review import run_etpfb_review
from .reviews.eluxp_review import run_eluxp_review

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
    "CLEW": run_clew_review,
    "SBF80": run_sbf80_review,
    "WIFRP": run_wifrp_review,
    "LC100": run_lc100_review,
    "LC3WP": run_lc3wp_review,
    "LC1EP": run_lc1ep_review,
    "FRECP": run_frecp_review,
    "FRN4P": run_frn4p_review,
    "FR20P": run_fr20p_review,
    "EZ40P": run_ez40p_review,
    "EZ60P": run_ez60p_review,
    "EZ15P": run_ez15p_review,
    "EZN1P": run_ezn1p_review,
    "EFMEP": run_efmep_review,
    "ERI5P": run_eri5p_review,
    "BE1P": run_be1p_review,
    "EUS5P": run_eus5p_review,
    "EDEFP": run_edefp_review,
    "ETPFB": run_etpfb_review,
    "ELUXP": run_eluxp_review
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