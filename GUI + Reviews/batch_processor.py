# batch_processor.py
import concurrent.futures
import threading
import datetime as dt
import os
import socket
from config import get_index_config
from Review.review_logic import run_review

class BatchProcessor:
    def __init__(self, max_workers=3):
        self.max_workers = max_workers
        self.results = {}
        self.lock = threading.Lock()
    
    def process_single_review(self, review_type, **kwargs):
        """Process a single review and return the result"""
        try:
            # Get index configuration
            index_config = get_index_config(review_type)
            
            # Run the review calculation
            result = run_review(
                review_type=review_type,
                index=index_config["index"],
                isin=index_config["isin"],
                **kwargs
            )
            
            return {
                "review_type": review_type,
                "status": result["status"],
                "message": result["message"],
                "data": result.get("data"),
                "duration_seconds": result.get("duration_seconds"),
                "completed_at": result.get("completed_at", dt.datetime.now().isoformat()),
                "timestamp": dt.datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "review_type": review_type,
                "status": "error",
                "message": f"Error in {review_type} review: {str(e)}",
                "data": None,
                "duration_seconds": None,
                "completed_at": dt.datetime.now().isoformat(),
                "timestamp": dt.datetime.now().isoformat()
            }

def run_batch_reviews(review_types, date, co_date, effective_date, auto_open=False):
    """
    Run multiple reviews sequentially (one after another) to avoid conflicts
    
    Args:
        review_types (list): List of review types to run
        date (str): Calculation date
        co_date (str): Cut off date
        effective_date (str): Effective date
        auto_open (bool): Whether to auto-open files
    
    Returns:
        list: Results from all reviews
    """
    # Remove duplicates while preserving order
    unique_review_types = list(dict.fromkeys(review_types))
    
    if len(unique_review_types) != len(review_types):
        print(f"Warning: Removed {len(review_types) - len(unique_review_types)} duplicate review types")
    
    processor = BatchProcessor()
    results = []
    processed_reviews = set()  # Track processed reviews
    
    # Common parameters for all reviews
    common_params = {
        'date': date,
        'co_date': co_date,
        'effective_date': effective_date
    }
    
    print(f"Starting sequential processing of {len(unique_review_types)} reviews...")
    
    # Process reviews one by one (sequentially)
    for i, review_type in enumerate(unique_review_types, 1):
        if review_type in processed_reviews:
            print(f"Warning: {review_type} already processed, skipping duplicate")
            continue
            
        processed_reviews.add(review_type)
        
        print(f"[{i}/{len(unique_review_types)}] Starting {review_type} review...")
        
        try:
            # Process single review
            result = processor.process_single_review(review_type, **common_params)
            results.append(result)
            
            print(f"[{i}/{len(unique_review_types)}] Completed {review_type} review with status: {result['status']}")
            
            # Handle auto-open for successful reviews
            if result["status"] == "success" and auto_open and result.get("data"):
                try:
                    index_config = get_index_config(result["review_type"])
                    output_path = result["data"].get(index_config["output_key"])
                    if output_path and os.path.exists(output_path):
                        os.startfile(output_path)
                        print(f"Auto-opened file for {result['review_type']}: {output_path}")
                except Exception as e:
                    print(f"Error auto-opening file for {result['review_type']}: {str(e)}")
                    
        except Exception as e:
            print(f"[{i}/{len(unique_review_types)}] Error processing {review_type}: {str(e)}")
            results.append({
                "review_type": review_type,
                "status": "error",
                "message": f"Processing error: {str(e)}",
                "data": None,
                "duration_seconds": None,
                "completed_at": dt.datetime.now().isoformat(),
                "timestamp": dt.datetime.now().isoformat()
            })
    
    # Sort results by review type for consistency
    results.sort(key=lambda x: x['review_type'])
    
    successful = len([r for r in results if r['status'] == 'success'])
    failed = len([r for r in results if r['status'] == 'error'])
    
    print(f"Sequential batch processing completed: {successful} successful, {failed} failed out of {len(unique_review_types)} unique reviews")
    
    return results

def run_batch_reviews_with_progress(review_types, date, co_date, effective_date, auto_open=False):
    """
    Run multiple reviews with progress updates (generator function for streaming)
    
    Yields progress updates for real-time feedback
    """
    processor = BatchProcessor()
    total = len(review_types)
    completed = 0
    
    # Common parameters for all reviews
    common_params = {
        'date': date,
        'co_date': co_date,
        'effective_date': effective_date
    }
    
    # Yield initial progress
    yield {
        'type': 'progress',
        'completed': 0,
        'total': total,
        'message': f'Starting batch processing of {total} reviews...'
    }
    
    results = []
    
    # Use ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=processor.max_workers) as executor:
        # Submit all reviews
        future_to_review = {
            executor.submit(processor.process_single_review, review_type, **common_params): review_type
            for review_type in review_types
        }
        
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_review):
            result = future.result()
            results.append(result)
            completed += 1
            
            # Yield progress update
            yield {
                'type': 'progress',
                'completed': completed,
                'total': total,
                'message': f'Completed {completed} of {total} reviews',
                'latest_result': result
            }
            
            # Handle auto-open for successful reviews
            if result["status"] == "success" and auto_open and result.get("data"):
                try:
                    index_config = get_index_config(result["review_type"])
                    output_path = result["data"].get(index_config["output_key"])
                    if output_path and os.path.exists(output_path):
                        os.startfile(output_path)
                except Exception as e:
                    print(f"Error auto-opening file for {result['review_type']}: {str(e)}")
    
    # Sort results and yield final summary
    results.sort(key=lambda x: x['review_type'])
    successful = len([r for r in results if r['status'] == 'success'])
    failed = len([r for r in results if r['status'] == 'error'])
    
    yield {
        'type': 'complete',
        'completed': total,
        'total': total,
        'successful': successful,
        'failed': failed,
        'message': f'Batch processing completed: {successful} successful, {failed} failed',
        'results': results
    }