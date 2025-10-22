# task_manager.py
import threading
import uuid
import datetime as dt
from enum import Enum
from dataclasses import dataclass, asdict
from typing import Dict, Optional, Any
import json
import os
from Review.review_logic import run_review
from config import get_index_config

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class TaskResult:
    task_id: str
    task_type: str  # "single" or "batch"
    status: TaskStatus
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    progress: int = 0  # 0-100
    message: str = ""
    error: Optional[str] = None
    result_data: Optional[Dict] = None
    review_type: Optional[str] = None
    parameters: Optional[Dict] = None
    duration_seconds: Optional[float] = None

class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, TaskResult] = {}
        self.lock = threading.Lock()
        self.worker_threads: Dict[str, threading.Thread] = {}
        self.max_concurrent_tasks = 3  # Configurable limit
        
    def create_task(self, task_type: str, review_type: str = None, **parameters) -> str:
        """Create a new task and return task ID"""
        task_id = str(uuid.uuid4())
        
        with self.lock:
            task = TaskResult(
                task_id=task_id,
                task_type=task_type,
                status=TaskStatus.PENDING,
                created_at=dt.datetime.now().isoformat(),
                review_type=review_type,
                parameters=parameters,
                message=f"Task {task_id} created and queued"
            )
            self.tasks[task_id] = task
            
        return task_id
    
    def start_single_review_task(self, task_id: str):
        """Start a single review task in background thread"""
        def run_single_review():
            try:
                with self.lock:
                    if task_id not in self.tasks:
                        return
                    
                    task = self.tasks[task_id]
                    task.status = TaskStatus.RUNNING
                    task.started_at = dt.datetime.now().isoformat()
                    task.progress = 10
                    task.message = f"Starting {task.review_type} review..."
                
                # Run the actual review
                result = run_review(
                    review_type=task.review_type,
                    date=task.parameters['date'],
                    co_date=task.parameters['co_date'],
                    effective_date=task.parameters['effective_date'],
                    index=task.parameters['index'],
                    isin=task.parameters['isin'],
                    currency=task.parameters['currency']
                )
                
                # Update task with results
                with self.lock:
                    if task_id in self.tasks:
                        task = self.tasks[task_id]
                        task.completed_at = dt.datetime.now().isoformat()
                        task.progress = 100
                        task.duration_seconds = result.get('duration_seconds')
                        
                        if result['status'] == 'success':
                            task.status = TaskStatus.COMPLETED
                            task.message = result['message']
                            task.result_data = result.get('data')
                            
                            # Handle auto-open if requested and local
                            if task.parameters.get('auto_open') and task.parameters.get('is_local_request'):
                                self._handle_auto_open(task.review_type, task.result_data)
                                
                        else:
                            task.status = TaskStatus.FAILED
                            task.error = result['message']
                            task.message = f"Review failed: {result['message']}"
                            
            except Exception as e:
                with self.lock:
                    if task_id in self.tasks:
                        task = self.tasks[task_id]
                        task.status = TaskStatus.FAILED
                        task.completed_at = dt.datetime.now().isoformat()
                        task.error = str(e)
                        task.message = f"Task execution failed: {str(e)}"
                        task.progress = 0
                        
            finally:
                # Clean up thread reference
                with self.lock:
                    if task_id in self.worker_threads:
                        del self.worker_threads[task_id]
        
        # Start the background thread
        thread = threading.Thread(target=run_single_review, daemon=True)
        
        with self.lock:
            self.worker_threads[task_id] = thread
            
        thread.start()
    
    def start_batch_review_task(self, task_id: str):
        """Start a batch review task in background thread"""
        def run_batch_reviews():
            try:
                with self.lock:
                    if task_id not in self.tasks:
                        return
                        
                    task = self.tasks[task_id]
                    task.status = TaskStatus.RUNNING
                    task.started_at = dt.datetime.now().isoformat()
                    task.progress = 5
                    task.message = "Starting batch review process..."
                
                review_types = task.parameters['review_types']
                total_reviews = len(review_types)
                completed_reviews = 0
                results = []
                
                # Process each review sequentially
                for i, review_type in enumerate(review_types, 1):
                    try:
                        # Update progress
                        with self.lock:
                            if task_id in self.tasks:
                                progress = int((i / total_reviews) * 90) + 5  # 5-95%
                                self.tasks[task_id].progress = progress
                                self.tasks[task_id].message = f"Processing {review_type} ({i}/{total_reviews})"
                        
                        # Get index config for this review
                        index_config = get_index_config(review_type)
                        
                        # Run individual review
                        review_result = run_review(
                            review_type=review_type,
                            date=task.parameters['date'],
                            co_date=task.parameters['co_date'],
                            effective_date=task.parameters['effective_date'],
                            index=index_config["index"],
                            isin=index_config["isin"],
                            currency=task.parameters['currency']
                        )
                        
                        # Format result for batch response
                        batch_result = {
                            "review_type": review_type,
                            "status": review_result["status"],
                            "message": review_result["message"],
                            "data": review_result.get("data"),
                            "duration_seconds": review_result.get("duration_seconds"),
                            "completed_at": review_result.get("completed_at", dt.datetime.now().isoformat())
                        }
                        results.append(batch_result)
                        
                        if review_result["status"] == "success":
                            completed_reviews += 1
                            
                        # Handle auto-open for successful reviews
                        if (review_result["status"] == "success" and 
                            task.parameters.get('auto_open') and 
                            task.parameters.get('is_local_request') and
                            review_result.get("data")):
                            self._handle_auto_open(review_type, review_result["data"])
                            
                    except Exception as review_error:
                        batch_result = {
                            "review_type": review_type,
                            "status": "error",
                            "message": f"Error processing {review_type}: {str(review_error)}",
                            "data": None,
                            "duration_seconds": None,
                            "completed_at": dt.datetime.now().isoformat()
                        }
                        results.append(batch_result)
                
                # Update final task status
                with self.lock:
                    if task_id in self.tasks:
                        task = self.tasks[task_id]
                        task.completed_at = dt.datetime.now().isoformat()
                        task.progress = 100
                        task.status = TaskStatus.COMPLETED
                        task.result_data = {
                            "results": results,
                            "summary": {
                                "total": total_reviews,
                                "successful": completed_reviews,
                                "failed": total_reviews - completed_reviews
                            }
                        }
                        task.message = f"Batch completed: {completed_reviews}/{total_reviews} successful"
                        
                        # Calculate total duration
                        if task.started_at:
                            start_time = dt.datetime.fromisoformat(task.started_at)
                            end_time = dt.datetime.fromisoformat(task.completed_at)
                            task.duration_seconds = (end_time - start_time).total_seconds()
                            
            except Exception as e:
                with self.lock:
                    if task_id in self.tasks:
                        task = self.tasks[task_id]
                        task.status = TaskStatus.FAILED
                        task.completed_at = dt.datetime.now().isoformat()
                        task.error = str(e)
                        task.message = f"Batch execution failed: {str(e)}"
                        task.progress = 0
                        
            finally:
                # Clean up thread reference
                with self.lock:
                    if task_id in self.worker_threads:
                        del self.worker_threads[task_id]
        
        # Start the background thread
        thread = threading.Thread(target=run_batch_reviews, daemon=True)
        
        with self.lock:
            self.worker_threads[task_id] = thread
            
        thread.start()
    
    def _handle_auto_open(self, review_type: str, result_data: dict):
        """Handle auto-opening files for successful reviews"""
        try:
            index_config = get_index_config(review_type)
            output_path = result_data.get(index_config["output_key"])
            if output_path and os.path.exists(output_path):
                os.startfile(output_path)
                print(f"Auto-opened file for {review_type}: {output_path}")
        except Exception as e:
            print(f"Error auto-opening file for {review_type}: {str(e)}")
    
    def get_task_status(self, task_id: str) -> Optional[TaskResult]:
        """Get current status of a task"""
        with self.lock:
            return self.tasks.get(task_id)
    
    def get_task_status_dict(self, task_id: str) -> Optional[Dict]:
        """Get task status as dictionary for JSON serialization"""
        task = self.get_task_status(task_id)
        if task:
            result = asdict(task)
            # Convert enum to string
            result['status'] = task.status.value
            return result
        return None
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending task"""
        with self.lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                if task.status == TaskStatus.PENDING:
                    task.status = TaskStatus.CANCELLED
                    task.message = "Task cancelled by user"
                    task.completed_at = dt.datetime.now().isoformat()
                    return True
        return False
    
    def cleanup_old_tasks(self, max_age_hours: int = 24):
        """Remove old completed/failed tasks"""
        cutoff_time = dt.datetime.now() - dt.timedelta(hours=max_age_hours)
        
        with self.lock:
            tasks_to_remove = []
            for task_id, task in self.tasks.items():
                if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    task_time = dt.datetime.fromisoformat(task.created_at)
                    if task_time < cutoff_time:
                        tasks_to_remove.append(task_id)
            
            for task_id in tasks_to_remove:
                del self.tasks[task_id]
                
        return len(tasks_to_remove)
    
    def get_running_tasks_count(self) -> int:
        """Get number of currently running tasks"""
        with self.lock:
            return sum(1 for task in self.tasks.values() if task.status == TaskStatus.RUNNING)
    
    def can_start_new_task(self) -> bool:
        """Check if we can start a new task based on concurrency limits"""
        return self.get_running_tasks_count() < self.max_concurrent_tasks

# Global task manager instance
task_manager = TaskManager()