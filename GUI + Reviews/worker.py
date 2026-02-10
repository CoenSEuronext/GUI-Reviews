# worker.py
import json
import os
import time
import datetime as dt
from pathlib import Path
from typing import Dict, Optional
import signal
import sys
import subprocess
from collections import deque


TASK_STORAGE_DIR = Path(os.environ.get("TASK_STORAGE_DIR", "task_storage"))
POLL_INTERVAL_SECONDS = float(os.environ.get("WORKER_POLL_INTERVAL", "0.5"))
LOCK_TIMEOUT_SECONDS = int(os.environ.get("WORKER_LOCK_TIMEOUT", "3600"))  # stale lock safety

# Path to the per-task runner (fresh interpreter per task)
TASK_RUNNER_PATH = os.environ.get("TASK_RUNNER_PATH", "task_runner.py")

# Streaming/logging controls
RUNNER_TAIL_LINES = int(os.environ.get("WORKER_RUNNER_TAIL_LINES", "300"))
WRITE_TASK_LOG_FILES = os.environ.get("WORKER_WRITE_TASK_LOGS", "1") == "1"

# NOTE: config is intentionally NOT imported at the top level.
# The worker process stays alive indefinitely, so any top-level import of config
# would be cached in sys.modules forever. Instead, use _get_index_config_fresh()
# which busts the cache before every import so config changes are always picked up
# without restarting the worker.


def handle_signal(sig, frame):
    print("\nWorker interrupted by Ctrl+C. Cleaning up and exiting...")
    sys.exit(0)


def _get_index_config_fresh(review_type: str):
    """
    Load config fresh from disk on every call by evicting it from sys.modules first.
    This ensures that changes to config.py are picked up without restarting the worker.
    """
    for key in list(sys.modules):
        if key == "config" or key.startswith("config."):
            del sys.modules[key]
    from config import get_index_config
    return get_index_config(review_type)


def _task_file_path(task_id: str) -> Path:
    return TASK_STORAGE_DIR / f"{task_id}.json"


def _lock_file_path(task_id: str) -> Path:
    return TASK_STORAGE_DIR / f"{task_id}.lock"


def _read_json(path: Path) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json_atomic(path: Path, data: Dict) -> None:
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def _acquire_lock(task_id: str) -> bool:
    lock_path = _lock_file_path(task_id)

    if lock_path.exists():
        try:
            age = time.time() - lock_path.stat().st_mtime
            if age > LOCK_TIMEOUT_SECONDS:
                lock_path.unlink(missing_ok=True)
        except Exception:
            pass

    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(str(os.getpid()))
        return True
    except FileExistsError:
        return False


def _release_lock(task_id: str) -> None:
    try:
        _lock_file_path(task_id).unlink(missing_ok=True)
    except Exception:
        pass


def _update_task(task_id: str, **updates) -> None:
    path = _task_file_path(task_id)
    if not path.exists():
        return
    data = _read_json(path)
    data.update(updates)
    _write_json_atomic(path, data)


def _mark_interrupted_running_tasks_as_failed() -> None:
    for task_file in TASK_STORAGE_DIR.glob("*.json"):
        try:
            data = _read_json(task_file)
            if data.get("status") == "running":
                now = dt.datetime.now().isoformat()
                data["status"] = "failed"
                data["error"] = "Worker restarted during task execution"
                data["message"] = "Task interrupted by worker restart"
                data["completed_at"] = now
                data["progress"] = 0
                _write_json_atomic(task_file, data)
        except Exception:
            continue


def _find_next_pending_task_id() -> Optional[str]:
    candidates = []
    for task_file in TASK_STORAGE_DIR.glob("*.json"):
        try:
            data = _read_json(task_file)
            if data.get("status") == "pending":
                created_at = data.get("created_at", "")
                candidates.append((created_at, data.get("task_id")))
        except Exception:
            continue

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _handle_auto_open(review_type: str, result_data: dict, is_local_request: bool, auto_open: bool) -> None:
    if not (auto_open and is_local_request and isinstance(result_data, dict)):
        return
    try:
        # Fresh import every call so config changes are picked up without a worker restart.
        index_config = _get_index_config_fresh(review_type)
        output_key = index_config.get("output_key")
        if not output_key:
            return
        output_path = result_data.get(output_key)
        if output_path and os.path.exists(output_path):
            os.startfile(output_path)
    except Exception:
        return


def _resolve_task_runner_path() -> Path:
    runner_path = Path(TASK_RUNNER_PATH)
    if runner_path.exists():
        return runner_path

    alt = Path(__file__).resolve().parent / TASK_RUNNER_PATH
    if alt.exists():
        return alt

    raise FileNotFoundError(
        f"task runner not found at '{TASK_RUNNER_PATH}'. "
        f"Set TASK_RUNNER_PATH env var or place task_runner.py next to worker.py."
    )


def _stream_task_runner(task_id: str, review_type: Optional[str]) -> Dict[str, Optional[str]]:
    """
    Run task_runner.py in a fresh interpreter and stream logs to worker terminal.
    Also captures tail for debugging and optional per-task log file.
    """
    runner_path = _resolve_task_runner_path()

    cmd = [sys.executable, "-u", str(runner_path), task_id]  # -u: unbuffered
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    tail = deque(maxlen=RUNNER_TAIL_LINES)
    log_path = TASK_STORAGE_DIR / f"{task_id}.log"
    log_f = None

    t0 = time.perf_counter()
    try:
        if WRITE_TASK_LOG_FILES:
            TASK_STORAGE_DIR.mkdir(exist_ok=True)
            log_f = open(log_path, "a", encoding="utf-8")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )

        assert proc.stdout is not None
        prefix = f"[{task_id}{'|' + review_type if review_type else ''}] "

        for line in proc.stdout:
            line = line.rstrip("\n")
            if not line:
                continue
            print(prefix + line, flush=True)
            tail.append(line)
            if log_f:
                log_f.write(line + "\n")
                log_f.flush()

        rc = proc.wait()
        dt_s = time.perf_counter() - t0

        return {
            "returncode": rc,
            "stdout_tail": "\n".join(tail),
            "stderr_tail": "",
            "duration_seconds_subprocess": dt_s,
            "log_path": str(log_path) if WRITE_TASK_LOG_FILES else "",
        }

    finally:
        if log_f:
            try:
                log_f.close()
            except Exception:
                pass


def _refresh_and_auto_open_if_completed(task_id: str) -> None:
    path = _task_file_path(task_id)
    if not path.exists():
        return

    data = _read_json(path)
    if data.get("status") != "completed":
        return

    params = data.get("parameters") or {}
    review_type = data.get("review_type")
    result_data = data.get("result_data") or {}

    _handle_auto_open(
        review_type=review_type,
        result_data=result_data,
        is_local_request=bool(params.get("is_local_request")),
        auto_open=bool(params.get("auto_open")),
    )


def _run_single(task_id: str, task_data: Dict) -> None:
    review_type = task_data.get("review_type")

    _update_task(
        task_id,
        status="running",
        started_at=dt.datetime.now().isoformat(),
        progress=10,
        message=f"Starting {review_type} review...",
        error=None,
    )

    exec_info = _stream_task_runner(task_id, review_type=review_type)

    # task_runner updates the task json; attach tail/debug if runner failed
    if exec_info["returncode"] != 0:
        try:
            data = _read_json(_task_file_path(task_id))
            if data.get("status") not in ("failed", "completed"):
                _update_task(
                    task_id,
                    status="failed",
                    completed_at=dt.datetime.now().isoformat(),
                    progress=0,
                    error=f"task_runner exit code {exec_info['returncode']}",
                    message="Task failed: task_runner crashed",
                )
        except Exception:
            pass

        _update_task(
            task_id,
            runner_returncode=exec_info["returncode"],
            runner_stdout_tail=exec_info["stdout_tail"],
            runner_log_path=exec_info.get("log_path", ""),
        )

    _refresh_and_auto_open_if_completed(task_id)


def _run_batch(task_id: str, task_data: Dict) -> None:
    params = task_data.get("parameters") or {}
    review_types = params.get("review_types") or []

    started_at_iso = dt.datetime.now().isoformat()
    t0 = time.perf_counter()

    _update_task(
        task_id,
        status="running",
        started_at=started_at_iso,
        progress=5,
        message="Starting batch review process...",
        error=None,
    )

    total_reviews = len(review_types)
    completed_reviews = 0
    results = []

    for i, review_type in enumerate(review_types, 1):
        progress = int((i / max(total_reviews, 1)) * 90) + 5  # 5-95%
        _update_task(
            task_id,
            progress=progress,
            message=f"Processing {review_type} ({i}/{total_reviews})",
        )

        child_task_id = f"{task_id}__{review_type}__{i}"
        child_path = _task_file_path(child_task_id)

        # Fresh config load on every iteration so batch jobs pick up config
        # changes between reviews without requiring a worker restart.
        index_config = _get_index_config_fresh(review_type)

        child_data = {
            "task_id": child_task_id,
            "task_type": "single",
            "status": "pending",
            "created_at": dt.datetime.now().isoformat(),
            "review_type": review_type,
            "parameters": {
                "date": params["date"],
                "co_date": params["co_date"],
                "effective_date": params["effective_date"],
                "index": index_config["index"],
                "isin": index_config["isin"],
                "auto_open": params.get("auto_open", False),
                "is_local_request": params.get("is_local_request", False),
            },
            "message": "Batch child task created",
            "progress": 0,
        }
        _write_json_atomic(child_path, child_data)

        try:
            exec_info = _stream_task_runner(child_task_id, review_type=review_type)
            final_child = _read_json(child_path)

            results.append(
                {
                    "review_type": review_type,
                    "status": final_child.get("status"),
                    "message": final_child.get("message"),
                    "data": final_child.get("result_data"),
                    "duration_seconds": final_child.get("duration_seconds"),
                    "completed_at": final_child.get("completed_at", dt.datetime.now().isoformat()),
                    "runner_returncode": exec_info["returncode"],
                    "runner_log_path": exec_info.get("log_path", ""),
                }
            )

            if final_child.get("status") == "completed":
                completed_reviews += 1
                _handle_auto_open(
                    review_type=review_type,
                    result_data=final_child.get("result_data") or {},
                    is_local_request=bool(params.get("is_local_request")),
                    auto_open=bool(params.get("auto_open")),
                )

            if exec_info["returncode"] != 0:
                _update_task(
                    child_task_id,
                    runner_returncode=exec_info["returncode"],
                    runner_stdout_tail=exec_info["stdout_tail"],
                    runner_log_path=exec_info.get("log_path", ""),
                )

        except Exception as review_error:
            results.append(
                {
                    "review_type": review_type,
                    "status": "error",
                    "message": f"Error processing {review_type}: {str(review_error)}",
                    "data": None,
                    "duration_seconds": None,
                    "completed_at": dt.datetime.now().isoformat(),
                }
            )

    completed_at_iso = dt.datetime.now().isoformat()
    duration_seconds = time.perf_counter() - t0
    avg_seconds_per_review = (duration_seconds / total_reviews) if total_reviews > 0 else None

    _update_task(
        task_id,
        status="completed",
        completed_at=completed_at_iso,
        progress=100,
        result_data={
            "results": results,
            "summary": {
                "total": total_reviews,
                "successful": completed_reviews,
                "failed": total_reviews - completed_reviews,
                "duration_seconds": duration_seconds,
                "avg_seconds_per_review": avg_seconds_per_review,
            },
        },
        message=f"Batch completed: {completed_reviews}/{total_reviews} successful",
        duration_seconds=duration_seconds,
    )


def main() -> None:
    signal.signal(signal.SIGINT, handle_signal)

    print("Worker activated :)")
    TASK_STORAGE_DIR.mkdir(exist_ok=True)
    _mark_interrupted_running_tasks_as_failed()

    while True:
        task_id = None
        lock_acquired = False

        try:
            task_id = _find_next_pending_task_id()
            if not task_id:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            lock_acquired = _acquire_lock(task_id)
            if not lock_acquired:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            task_path = _task_file_path(task_id)
            if not task_path.exists():
                continue

            task_data = _read_json(task_path)
            if task_data.get("status") != "pending":
                continue

            task_type = task_data.get("task_type")
            if task_type == "single":
                _run_single(task_id, task_data)
            elif task_type == "batch":
                _run_batch(task_id, task_data)
            else:
                _update_task(
                    task_id,
                    status="failed",
                    completed_at=dt.datetime.now().isoformat(),
                    progress=0,
                    error=f"Unknown task_type: {task_type}",
                    message=f"Task failed: unknown task_type {task_type}",
                )

        except KeyboardInterrupt:
            print("\nWorker interrupted. Shutting down gracefully...")
            break

        except Exception as e:
            if task_id:
                _update_task(
                    task_id,
                    status="failed",
                    completed_at=dt.datetime.now().isoformat(),
                    progress=0,
                    error=str(e),
                    message=f"Task execution failed: {str(e)}",
                )

        finally:
            if lock_acquired and task_id:
                _release_lock(task_id)


if __name__ == "__main__":
    main()