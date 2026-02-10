# review_logic.py
import importlib
import threading
import traceback
import datetime as dt
from typing import Callable, Dict, Tuple

thread_local = threading.local()

REVIEW_TARGETS: Dict[str, Tuple[str, str]] = {
    "FRI4P": ("Review.reviews.fri4p_review", "run_fri4p_review"),
    "FRD4P": ("Review.reviews.frd4p_review", "run_frd4p_review"),
    "EGSPP": ("Review.reviews.egspp_review", "run_egspp_review"),
    "GICP": ("Review.reviews.gicp_review", "run_gicp_review"),
    "EDWPT": ("Review.reviews.edwpt_review", "run_edwpt_review"),
    "EDWP": ("Review.reviews.edwp_review", "run_edwp_review"),
    "F4RIP": ("Review.reviews.f4rip_review", "run_f4rip_review"),
    "SES5P": ("Review.reviews.ses5p_review", "run_ses5p_review"),
    "AERDP": ("Review.reviews.aerdp_review", "run_aerdp_review"),
    "BNEW": ("Review.reviews.bnew_review", "run_bnew_review"),
    "AEXEW": ("Review.reviews.aexew_review", "run_aexew_review"),
    "CACEW": ("Review.reviews.cacew_review", "run_cacew_review"),
    "CLEW": ("Review.reviews.clew_review", "run_clew_review"),
    "SBF80": ("Review.reviews.sbf80_review", "run_sbf80_review"),
    "WIFRP": ("Review.reviews.wifrp_review", "run_wifrp_review"),
    "LC100": ("Review.reviews.lc100_review", "run_lc100_review"),
    "LC3WP": ("Review.reviews.lc3wp_review", "run_lc3wp_review"),
    "LC1EP": ("Review.reviews.lc1ep_review", "run_lc1ep_review"),
    "FRECP": ("Review.reviews.frecp_review", "run_frecp_review"),
    "FRN4P": ("Review.reviews.frn4p_review", "run_frn4p_review"),
    "FR20P": ("Review.reviews.fr20p_review", "run_fr20p_review"),
    "EZ40P": ("Review.reviews.ez40p_review", "run_ez40p_review"),
    "EZ60P": ("Review.reviews.ez60p_review", "run_ez60p_review"),
    "EZ15P": ("Review.reviews.ez15p_review", "run_ez15p_review"),
    "EZN1P": ("Review.reviews.ezn1p_review", "run_ezn1p_review"),
    "EFMEP": ("Review.reviews.efmep_review", "run_efmep_review"),
    "ERI5P": ("Review.reviews.eri5p_review", "run_eri5p_review"),
    "BE1P": ("Review.reviews.be1p_review", "run_be1p_review"),
    "EUS5P": ("Review.reviews.eus5p_review", "run_eus5p_review"),
    "EDEFP": ("Review.reviews.edefp_review", "run_edefp_review"),
    "ETPFB": ("Review.reviews.etpfb_review", "run_etpfb_review"),
    "ELUXP": ("Review.reviews.eluxp_review", "run_eluxp_review"),
    "ESVEP": ("Review.reviews.esvep_review", "run_esvep_review"),
    "SECTORIAL": ("Review.reviews.sectorial_review", "run_sectorial_review"),
    "DWREP": ("Review.reviews.dwrep_review", "run_dwrep_review"),
    "DEREP": ("Review.reviews.derep_review", "run_derep_review"),
    "DAREP": ("Review.reviews.darep_review", "run_darep_review"),
    "EUREP": ("Review.reviews.eurep_review", "run_eurep_review"),
    "GSFBP": ("Review.reviews.gsfbp_review", "run_gsfbp_review"),
    "EESF": ("Review.reviews.eesf_review", "run_eesf_review"),
    "ETSEP": ("Review.reviews.etsep_review", "run_etsep_review"),
    "ELTFP": ("Review.reviews.eltfp_review", "run_eltfp_review"),
    "ELECP": ("Review.reviews.elecp_review", "run_elecp_review"),
    "EUADP": ("Review.reviews.euadp_review", "run_euadp_review"),
    "EEFAP": ("Review.reviews.eefap_review", "run_eefap_review"),
    "EES2": ("Review.reviews.ees2_review", "run_ees2_review"),
    "EFESP": ("Review.reviews.efesp_review", "run_efesp_review"),
    "AEXAT": ("Review.reviews.aexat_review", "run_aexat_review"),
    "AETAW": ("Review.reviews.aetaw_review", "run_aetaw_review"),
    "ES2PR": ("Review.reviews.es2pr_review", "run_es2pr_review"),
    "ENVB": ("Review.reviews.envb_review", "run_envb_review"),
    "EZSL": ("Review.reviews.ezsl_review", "run_ezsl_review"),
    "EWMS": ("Review.reviews.ewms_review", "run_ewms_review"),
    "EEMSC": ("Review.reviews.eemsc_review", "run_eemsc_review"),
    "EZMS": ("Review.reviews.ezms_review", "run_ezms_review"),
    "EESL": ("Review.reviews.eesl_review", "run_eesl_review"),
    "EUSL": ("Review.reviews.eusl_review", "run_eusl_review"),
    "EUMS": ("Review.reviews.eums_review", "run_eums_review"),
    "EWSL": ("Review.reviews.ewsl_review", "run_ewsl_review"),
    "ELUX": ("Review.reviews.elux_review", "run_elux_review"),
    "EZCLA": ("Review.reviews.ezcla_review", "run_ezcla_review"),
    "USCLE": ("Review.reviews.uscle_review", "run_uscle_review"),
    "TCAMP": ("Review.reviews.tcamp_review", "run_tcamp_review"),
    "GSCSP": ("Review.reviews.gscsp_review", "run_gscsp_review"),
    "WCAMP": ("Review.reviews.wcamp_review", "run_wcamp_review"),
    "EHNI": ("Review.reviews.ehni_review", "run_ehni_review"),
    "USCLA": ("Review.reviews.uscla_review", "run_uscla_review"),
    "USC3P": ("Review.reviews.usc3p_review", "run_usc3p_review"),
    "UC3PE": ("Review.reviews.uc3pe_review", "run_uc3pe_review"),
    "CLAMP": ("Review.reviews.clamp_review", "run_clamp_review"),
    "JPCLA": ("Review.reviews.jpcla_review", "run_jpcla_review"),
    "JPCLE": ("Review.reviews.jpcle_review", "run_jpcle_review"),
    "FCLSP": ("Review.reviews.fclsp_review", "run_fclsp_review"),
    "EAIB": ("Review.reviews.eaib_review", "run_eaib_review"),
    "EEDF": ("Review.reviews.eedf_review", "run_eedf_review"),
    "EHCF": ("Review.reviews.ehcf_review", "run_ehcf_review"),
    "EIAPR": ("Review.reviews.eiapr_review", "run_eiapr_review"),
    "EMLS": ("Review.reviews.emls_review", "run_emls_review"),
    "C6RIP": ("Review.reviews.c6rip_review", "run_c6rip_review"),
    "INFRP": ("Review.reviews.infrp_review", "run_infrp_review"),
    "ES2PR_BACKUP": ("Review.reviews.es2pr_review_backup", "run_es2pr_review_backup"),
    "ENVU": ("Review.reviews.envu_review", "run_envu_review"),
    "ENVUK": ("Review.reviews.envuk_review", "run_envuk_review"),
    "ENVEO": ("Review.reviews.enveo_review", "run_enveo_review"),
    "ENVEU": ("Review.reviews.enveu_review", "run_enveu_review"),
    "ENVF": ("Review.reviews.envf_review", "run_envf_review"),
    "ENVW": ("Review.reviews.envw_review", "run_envw_review"),
    "ENTP": ("Review.reviews.entp_review", "run_entp_review"),
    "ENZTP": ("Review.reviews.enztp_review", "run_enztp_review"),
    "EENS": ("Review.reviews.eens_review", "run_eens_review"),
    "FGINP": ("Review.reviews.fginp_review", "run_fginp_review"),
    "EZ3R": ("Review.reviews.ez3r_review", "run_ez3r_review"),
    "EBMFP": ("Review.reviews.ebmfp_review", "run_ebmfp_review"),
    "EZ20P": ("Review.reviews.ez20p_review", "run_ez20p_review"),
    "EZGP": ("Review.reviews.ezgp_review", "run_ezgp_review"),
    "ESF4P": ("Review.reviews.esf4p_review", "run_esf4p_review"),
    "ESG50": ("Review.reviews.esg50_review", "run_esg50_review"),
    "ESF5P": ("Review.reviews.esf5p_review", "run_esf5p_review"),
    "EEEPR": ("Review.reviews.eeepr_review", "run_eeepr_review"),
}

_RESOLVED: Dict[str, Callable] = {}

def _resolve_review_function(review_type: str) -> Callable:
    key = (review_type or "").upper().strip()
    if not key:
        raise ValueError("Missing review_type")

    cached = _RESOLVED.get(key)
    if cached is not None:
        return cached

    target = REVIEW_TARGETS.get(key)
    if target is None:
        raise ValueError(f"Unknown review type: {review_type}")

    module_path, func_name = target
    module = importlib.import_module(module_path)
    fn = getattr(module, func_name, None)
    if fn is None or not callable(fn):
        raise AttributeError(f"{module_path}.{func_name} not found or not callable")

    _RESOLVED[key] = fn
    return fn


def run_review(review_type, **kwargs):
    """
    Route to appropriate review based on type with enhanced error handling

    Args:
        review_type (str): Type of review to run
        **kwargs: Arguments to pass to the review function

    Returns:
        dict: Result of the review containing status, message, and data
    """
    start_time = dt.datetime.now()

    try:
        # Set thread-local info for debugging
        thread_local.current_review = review_type
        thread_local.start_time = start_time

        review_function = _resolve_review_function(review_type)

        print(f"Starting {review_type} review at {start_time}")
        result = review_function(**kwargs)

        # Add timing information
        end_time = dt.datetime.now()
        duration = (end_time - start_time).total_seconds()

        if isinstance(result, dict):
            result["duration_seconds"] = duration
            result["completed_at"] = end_time.isoformat()

        print(f"Completed {review_type} review in {duration:.2f} seconds")
        return result

    except Exception as e:
        end_time = dt.datetime.now()
        duration = (end_time - start_time).total_seconds()

        error_result = {
            "status": "error",
            "message": f"Error in {review_type} review: {str(e)}",
            "traceback": traceback.format_exc(),
            "data": None,
            "duration_seconds": duration,
            "completed_at": end_time.isoformat(),
        }

        print(f"Failed {review_type} review after {duration:.2f} seconds: {str(e)}")
        return error_result

    finally:
        # Clean up thread-local storage
        if hasattr(thread_local, "current_review"):
            delattr(thread_local, "current_review")
        if hasattr(thread_local, "start_time"):
            delattr(thread_local, "start_time")


def get_review_status():
    """Get current review status for debugging"""
    if hasattr(thread_local, "current_review"):
        return {
            "current_review": thread_local.current_review,
            "start_time": thread_local.start_time.isoformat() if hasattr(thread_local, "start_time") else None,
        }
    return None
