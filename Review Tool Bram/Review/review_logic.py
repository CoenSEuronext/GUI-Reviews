# review_logic.py
import threading
import traceback
import datetime as dt
from .reviews.f4rip_review import run_f4rip_review
from .reviews.wifrp_review import run_wifrp_review
from .reviews.eluxp_review import run_eluxp_review
from .reviews.eesf_review import run_eesf_review
from .reviews.elecp_review import run_elecp_review
from .reviews.aexat_review import run_aexat_review
from .reviews.aetaw_review import run_aetaw_review

# Add a thread-local storage for better error handling in concurrent scenarios
thread_local = threading.local()

# Dictionary mapping review types to their corresponding functions
REVIEW_FUNCTIONS = {
    "F4RIP": run_f4rip_review,
    "WIFRP": run_wifrp_review,
    "ELUXP": run_eluxp_review,
    "EESF": run_eesf_review,
    "ELECP": run_elecp_review,
    "AEXAT": run_aexat_review,
    "AETAW": run_aetaw_review,
}

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
        
        review_function = REVIEW_FUNCTIONS.get(review_type.upper())
        if review_function is None:
            raise ValueError(f"Unknown review type: {review_type}")
        
        print(f"Starting {review_type} review at {start_time}")
        result = review_function(**kwargs)
        
        # Add timing information
        end_time = dt.datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        if isinstance(result, dict):
            result['duration_seconds'] = duration
            result['completed_at'] = end_time.isoformat()
        
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
            "completed_at": end_time.isoformat()
        }
        
        print(f"Failed {review_type} review after {duration:.2f} seconds: {str(e)}")
        return error_result
    
    finally:
        # Clean up thread-local storage
        if hasattr(thread_local, 'current_review'):
            delattr(thread_local, 'current_review')
        if hasattr(thread_local, 'start_time'):
            delattr(thread_local, 'start_time')

def get_review_status():
    """Get current review status for debugging"""
    if hasattr(thread_local, 'current_review'):
        return {
            'current_review': thread_local.current_review,
            'start_time': thread_local.start_time.isoformat() if hasattr(thread_local, 'start_time') else None
        }
    return None