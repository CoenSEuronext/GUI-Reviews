# enhanced_task_manager.py
import threading
import uuid
import datetime as dt
from enum import Enum
from dataclasses import dataclass, asdict, fields
from typing import Dict, Optional, Any
import json
from pathlib import Path


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

    # Forward-compatible fields (task_runner/worker may write these)
    completed_at_review: Optional[str] = None
    traceback: Optional[str] = None
    runner_returncode: Optional[int] = None
    runner_stdout_tail: Optional[str] = None
    runner_stderr_tail: Optional[str] = None
    runner_log_path: Optional[str] = None


_ALLOWED_TASK_FIELDS = {f.name for f in fields(TaskResult)}


def _read_task_file_json(path: Path) -> Optional[Dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def _filter_task_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    # Drop unknown keys so old/new task files don't break app startup
    return {k: v for k, v in data.items() if k in _ALLOWED_TASK_FIELDS}


class PersistentTaskManager:
    """
    Persists tasks to disk. Execution is handled by an external worker process (worker.py).
    This process only creates/cancels tasks and serves status.
    """

    def __init__(self, storage_dir="task_storage"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        self.tasks: Dict[str, TaskResult] = {}
        self.lock = threading.Lock()
        self.max_concurrent_tasks = 3  # informational only (worker may enforce differently)

        self._load_tasks()
        # IMPORTANT:
        # Do NOT mark RUNNING tasks as failed here.
        # The worker is the only process that should handle "running -> failed" on restart.

    def _get_task_file_path(self, task_id: str) -> Path:
        return self.storage_dir / f"{task_id}.json"

    def _save_task(self, task: TaskResult):
        task_data = asdict(task)
        task_data["status"] = task.status.value
        with open(self._get_task_file_path(task.task_id), "w", encoding="utf-8") as f:
            json.dump(task_data, f, indent=2)

    def _load_tasks(self):
        for task_file in self.storage_dir.glob("*.json"):
            try:
                data = _read_task_file_json(task_file)
                if not data:
                    continue

                status_raw = data.get("status")
                if isinstance(status_raw, str):
                    data["status"] = TaskStatus(status_raw)
                elif isinstance(status_raw, TaskStatus):
                    data["status"] = status_raw
                else:
                    # Unknown status shape; skip
                    continue

                data = _filter_task_fields(data)
                task = TaskResult(**data)
                self.tasks[task.task_id] = task
            except Exception as e:
                print(f"Error loading task from {task_file}: {e}")

    def create_task(self, task_type: str, review_type: str = None, **parameters) -> str:
        task_id = str(uuid.uuid4())

        with self.lock:
            task = TaskResult(
                task_id=task_id,
                task_type=task_type,
                status=TaskStatus.PENDING,
                created_at=dt.datetime.now().isoformat(),
                review_type=review_type,
                parameters=parameters,
                message="Initializing...",
            )
            self.tasks[task_id] = task
            self._save_task(task)

        return task_id

    def _update_task_status(self, task_id: str, **updates):
        with self.lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                for key, value in updates.items():
                    if key in _ALLOWED_TASK_FIELDS:
                        setattr(task, key, value)
                self._save_task(task)

    def start_single_review_task(self, task_id: str):
        """
        No-op: execution is done by worker.py.
        Kept to avoid breaking callers; app.py is patched to not call this.
        """
        return

    def start_batch_review_task(self, task_id: str):
        """
        No-op: execution is done by worker.py.
        Kept to avoid breaking callers; app.py is patched to not call this.
        """
        return

    def get_task_status(self, task_id: str) -> Optional[TaskResult]:
        with self.lock:
            return self.tasks.get(task_id)

    def get_task_status_dict(self, task_id: str) -> Optional[Dict]:
        """
        Source of truth is disk because worker updates JSON files.
        Also updates in-memory cache best-effort.
        """
        path = self._get_task_file_path(task_id)
        data = _read_task_file_json(path)
        if data is None:
            return None

        # Best-effort: hydrate into TaskResult + keep cache in sync
        try:
            hydrated = dict(data)

            status_raw = hydrated.get("status")
            if isinstance(status_raw, str):
                hydrated["status"] = TaskStatus(status_raw)
            elif isinstance(status_raw, TaskStatus):
                hydrated["status"] = status_raw
            else:
                # Can't hydrate; return raw
                return data

            hydrated = _filter_task_fields(hydrated)
            task = TaskResult(**hydrated)

            with self.lock:
                self.tasks[task_id] = task

            out = asdict(task)
            out["status"] = task.status.value
            return out
        except Exception:
            # Fallback: return raw JSON (ensure status is string)
            if isinstance(data.get("status"), TaskStatus):
                data["status"] = data["status"].value
            return data

    def cancel_task(self, task_id: str) -> bool:
        with self.lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                if task.status == TaskStatus.PENDING:
                    self._update_task_status(
                        task_id,
                        status=TaskStatus.CANCELLED,
                        message="Task cancelled by user",
                        completed_at=dt.datetime.now().isoformat(),
                    )
                    return True
        return False

    def cleanup_old_tasks(self, max_age_hours: int = 24):
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
                task_file = self._get_task_file_path(task_id)
                if task_file.exists():
                    task_file.unlink()

        return len(tasks_to_remove)

    def get_running_tasks_count(self) -> int:
        with self.lock:
            return sum(1 for task in self.tasks.values() if task.status == TaskStatus.RUNNING)

    def can_start_new_task(self) -> bool:
        return self.get_running_tasks_count() < self.max_concurrent_tasks


task_manager = PersistentTaskManager()
