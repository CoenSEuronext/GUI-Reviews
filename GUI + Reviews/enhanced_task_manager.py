# enhanced_task_manager.py
import threading
import uuid
import datetime as dt
from enum import Enum
from dataclasses import dataclass, asdict
from typing import Dict, Optional, Any
import json
import os
import pickle
from pathlib import Path
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

class PersistentTaskManager:
    def __init__(self, storage_dir="task_storage"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        self.tasks: Dict[str, TaskResult] = {}
        self.lock = threading.Lock()
        self.worker_threads: Dict[str, threading.Thread] = {}
        self.max_concurrent_tasks = 3
        
        # Load existing tasks from storage
        self._load_tasks()
        
        # Mark any previously running tasks as failed (since server restarted)
        self._mark_interrupted_tasks_as_failed()
    
    def _get_task_file_path(self, task_id: str) -> Path:
        """Get the file path for storing a specific task"""
        return self.storage_dir / f"{task_id}.json"
    
    def _save_task(self, task: TaskResult):
        """Save a single task to persistent storage"""
        task_data = asdict(task)
        task_data['status'] = task.status.value  # Convert enum to string
        
        with open(self._get_task_file_path(task.task_id), 'w') as f:
            json.dump(task_data, f, indent=2)
    
    def _load_tasks(self):
        """Load all tasks from persistent storage"""
        for task_file in self.storage_dir.glob("*.json"):
            try:
                with open(task_file, 'r') as f:
                    task_data = json.load(f)
                
                # Convert status back to enum
                task_data['status'] = TaskStatus(task_data['status'])
                
                task = TaskResult(**task_data)
                self.tasks[task.task_id] = task
                
            except Exception as e:
                print(f"Error loading task from {task_file}: {e}")
    
    def _mark_interrupted_tasks_as_failed(self):
        """Mark any running/pending tasks as failed due to server restart"""
        interrupted_count = 0
        with self.lock:
            for task in self.tasks.values():
                if task.status in [TaskStatus.RUNNING, TaskStatus.PENDING]:
                    task.status = TaskStatus.FAILED
                    task.error = "Server restarted during task execution"
                    task.message = "Task interrupted by server restart"
                    task.completed_at = dt.datetime.now().isoformat()
                    task.progress = 0
                    self._save_task(task)
                    interrupted_count += 1
        
        if interrupted_count > 0:
            print(f"Marked {interrupted_count} interrupted tasks as failed due to server restart")
    
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
            self._save_task(task)
            
        return task_id
    
    def _update_task_status(self, task_id: str, **updates):
        """Update task status and save to storage"""
        with self.lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                for key, value in updates.items():
                    setattr(task, key, value)
                self._save_task(task)
    
    def start_single_review_task(self, task_id: str):
        """Start a single review task in background thread"""
        def run_single_review():
            try:
                self._update_task_status(
                    task_id,
                    status=TaskStatus.RUNNING,
                    started_at=dt.datetime.now().isoformat(),
                    progress=10,
                    message=f"Starting {self.tasks[task_id].review_type} review..."
                )
                
                task = self.tasks[task_id]
                
                # Run the actual review
                result = run_review(
                    review_type=task.review_type,
                    date=task.parameters['date'],
                    co_date=task.parameters['co_date'],
                    effective_date=task.parameters['effective_date'],
                    index=task.parameters['index'],
                    isin=task.parameters['isin']
                )
                
                # Update task with results
                completed_at = dt.datetime.now().isoformat()
                duration_seconds = result.get('duration_seconds')
                
                if result['status'] == 'success':
                    self._update_task_status(
                        task_id,
                        status=TaskStatus.COMPLETED,
                        completed_at=completed_at,
                        progress=100,
                        message=result['message'],
                        result_data=result.get('data'),
                        duration_seconds=duration_seconds
                    )
                    
                    # Handle auto-open if requested and local
                    if task.parameters.get('auto_open') and task.parameters.get('is_local_request'):
                        self._handle_auto_open(task.review_type, result.get('data'))
                        
                else:
                    self._update_task_status(
                        task_id,
                        status=TaskStatus.FAILED,
                        completed_at=completed_at,
                        error=result['message'],
                        message=f"Review failed: {result['message']}",
                        duration_seconds=duration_seconds
                    )
                    
            except Exception as e:
                self._update_task_status(
                    task_id,
                    status=TaskStatus.FAILED,
                    completed_at=dt.datetime.now().isoformat(),
                    error=str(e),
                    message=f"Task execution failed: {str(e)}",
                    progress=0
                )
                        
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
                self._update_task_status(
                    task_id,
                    status=TaskStatus.RUNNING,
                    started_at=dt.datetime.now().isoformat(),
                    progress=5,
                    message="Starting batch review process..."
                )
                
                task = self.tasks[task_id]
                review_types = task.parameters['review_types']
                total_reviews = len(review_types)
                completed_reviews = 0
                results = []
                
                # Process each review sequentially
                for i, review_type in enumerate(review_types, 1):
                    try:
                        # Update progress
                        progress = int((i / total_reviews) * 90) + 5  # 5-95%
                        self._update_task_status(
                            task_id,
                            progress=progress,
                            message=f"Processing {review_type} ({i}/{total_reviews})"
                        )
                        
                        # Get index config for this review
                        index_config = get_index_config(review_type)
                        
                        # Run individual review
                        review_result = run_review(
                            review_type=review_type,
                            date=task.parameters['date'],
                            co_date=task.parameters['co_date'],
                            effective_date=task.parameters['effective_date'],
                            index=index_config["index"],
                            isin=index_config["isin"]
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
                completed_at = dt.datetime.now().isoformat()
                start_time = dt.datetime.fromisoformat(task.started_at)
                end_time = dt.datetime.fromisoformat(completed_at)
                duration_seconds = (end_time - start_time).total_seconds()
                
                self._update_task_status(
                    task_id,
                    status=TaskStatus.COMPLETED,
                    completed_at=completed_at,
                    progress=100,
                    result_data={
                        "results": results,
                        "summary": {
                            "total": total_reviews,
                            "successful": completed_reviews,
                            "failed": total_reviews - completed_reviews
                        }
                    },
                    message=f"Batch completed: {completed_reviews}/{total_reviews} successful",
                    duration_seconds=duration_seconds
                )
                        
            except Exception as e:
                self._update_task_status(
                    task_id,
                    status=TaskStatus.FAILED,
                    completed_at=dt.datetime.now().isoformat(),
                    error=str(e),
                    message=f"Batch execution failed: {str(e)}",
                    progress=0
                )
                        
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
                    self._update_task_status(
                        task_id,
                        status=TaskStatus.CANCELLED,
                        message="Task cancelled by user",
                        completed_at=dt.datetime.now().isoformat()
                    )
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
                # Remove from memory
                del self.tasks[task_id]
                
                # Remove from storage
                task_file = self._get_task_file_path(task_id)
                if task_file.exists():
                    task_file.unlink()
                
        return len(tasks_to_remove)
    
    def get_running_tasks_count(self) -> int:
        """Get number of currently running tasks"""
        with self.lock:
            return sum(1 for task in self.tasks.values() if task.status == TaskStatus.RUNNING)
    
    def can_start_new_task(self) -> bool:
        """Check if we can start a new task based on concurrency limits"""
        return self.get_running_tasks_count() < self.max_concurrent_tasks

# Global task manager instance
task_manager = PersistentTaskManager()