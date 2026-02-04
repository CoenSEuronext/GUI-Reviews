# task_runner.py
import json
import os
import sys
import time
import datetime as dt
from pathlib import Path
from typing import Dict, Optional

TASK_STORAGE_DIR = Path(os.environ.get("TASK_STORAGE_DIR", "task_storage"))


def _task_file_path(task_id: str) -> Path:
    return TASK_STORAGE_DIR / f"{task_id}.json"


def _read_json(path: Path) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json_atomic(path: Path, data: Dict) -> None:
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def _update_task(task_id: str, **updates) -> None:
    path = _task_file_path(task_id)
    if not path.exists():
        return
    data = _read_json(path)
    data.update(updates)
    _write_json_atomic(path, data)


def _run_review_from_task(task_id: str, task_data: Dict) -> Dict:
    """
    Import review code only here so we always get a fresh import on each task_runner invocation.
    """
    from Review.review_logic import run_review  # local import: critical for hot-reload via subprocess

    params = task_data.get("parameters") or {}
    review_type = task_data.get("review_type")
    if not review_type:
        raise ValueError("Task is missing 'review_type'")

    required = ["date", "co_date", "effective_date", "index", "isin"]
    missing = [k for k in required if k not in params]
    if missing:
        raise ValueError(f"Task parameters missing required fields: {missing}")

    return run_review(
        review_type=review_type,
        date=params["date"],
        co_date=params["co_date"],
        effective_date=params["effective_date"],
        index=params["index"],
        isin=params["isin"],
    )


def _ensure_unbuffered_stdio() -> None:
    # Make stdout/stderr line-buffered when possible (keeps logs real-time when worker streams).
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(line_buffering=True)
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(line_buffering=True)
    except Exception:
        pass


def main(argv: Optional[list] = None) -> int:
    _ensure_unbuffered_stdio()

    argv = argv or sys.argv[1:]
    if not argv:
        print("Usage: python task_runner.py <task_id>", file=sys.stderr, flush=True)
        return 2

    task_id = argv[0]
    TASK_STORAGE_DIR.mkdir(exist_ok=True)

    path = _task_file_path(task_id)
    if not path.exists():
        print(f"Task file not found: {path}", file=sys.stderr, flush=True)
        return 2

    task_data = _read_json(path)
    if task_data.get("status") not in ("pending", "running"):
        return 0

    started_at = task_data.get("started_at") or dt.datetime.now().isoformat()

    if task_data.get("status") == "pending":
        _update_task(
            task_id,
            status="running",
            started_at=started_at,
            progress=max(int(task_data.get("progress") or 0), 10),
            message=f"Starting {task_data.get('review_type')} review...",
            error=None,
        )

    t0 = time.perf_counter()
    try:
        result = _run_review_from_task(task_id, task_data)

        completed_at = dt.datetime.now().isoformat()
        duration_seconds = result.get("duration_seconds")
        if duration_seconds is None:
            duration_seconds = time.perf_counter() - t0

        if result.get("status") == "success":
            _update_task(
                task_id,
                status="completed",
                completed_at=completed_at,
                progress=100,
                message=result.get("message", "Completed"),
                result_data=result.get("data"),
                duration_seconds=duration_seconds,
                completed_at_review=result.get("completed_at"),
            )
            return 0

        _update_task(
            task_id,
            status="failed",
            completed_at=completed_at,
            progress=0,
            error=result.get("message", "Unknown error"),
            message=f"Review failed: {result.get('message', 'Unknown error')}",
            duration_seconds=duration_seconds,
            traceback=result.get("traceback"),
            completed_at_review=result.get("completed_at"),
        )
        return 1

    except Exception as e:
        completed_at = dt.datetime.now().isoformat()
        duration_seconds = time.perf_counter() - t0
        _update_task(
            task_id,
            status="failed",
            completed_at=completed_at,
            progress=0,
            error=str(e),
            message=f"Task failed: {str(e)}",
            duration_seconds=duration_seconds,
            traceback=None,
        )
        print(f"task_runner error for task_id={task_id}: {e}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
