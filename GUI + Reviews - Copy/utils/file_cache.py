# utils/file_cache.py
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd
from Review.functions import read_semicolon_csv

_lock = threading.RLock()

# In-memory L1 cache (per process)
# (abs_path, variant) -> (signature, dataframe)
_df_cache: Dict[Tuple[str, str], Tuple[Tuple[int, int], pd.DataFrame]] = {}

# abs_path -> (signature, sheet_names)
_sheet_cache: Dict[str, Tuple[Tuple[int, int], Tuple[str, ...]]] = {}

# Disk cache (shared across subprocess runs)
# Default: .df_cache next to where you run the app (override via env)
_DISK_CACHE_DIR = Path(os.environ.get("DF_CACHE_DIR", ".df_cache"))
_DISK_CACHE_DIR.mkdir(exist_ok=True)

# Simple per-entry lock timeout (seconds)
_DISK_LOCK_TIMEOUT = int(os.environ.get("DF_CACHE_LOCK_TIMEOUT", "60"))


def _signature(path: str) -> Tuple[int, int]:
    st = os.stat(path)
    return (st.st_mtime_ns, st.st_size)


def make_variant(prefix: str, **kwargs: Any) -> str:
    if not kwargs:
        return prefix
    parts = [prefix]
    for k, v in sorted(kwargs.items(), key=lambda kv: kv[0]):
        parts.append(f"{k}={repr(v)}")
    return "|".join(parts)


def _cache_key(abs_path: str, variant: str) -> str:
    h = hashlib.sha256()
    h.update(abs_path.encode("utf-8", errors="ignore"))
    h.update(b"\n")
    h.update(variant.encode("utf-8", errors="ignore"))
    return h.hexdigest()


def _paths_for_key(key_hex: str) -> Tuple[Path, Path, Path]:
    # Prefer parquet; fallback to pickle if parquet engine missing.
    parquet_path = _DISK_CACHE_DIR / f"{key_hex}.parquet"
    pickle_path = _DISK_CACHE_DIR / f"{key_hex}.pkl"
    meta_path = _DISK_CACHE_DIR / f"{key_hex}.meta.json"
    return parquet_path, pickle_path, meta_path


def _read_meta(meta_path: Path) -> Optional[Dict[str, Any]]:
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_meta_atomic(meta_path: Path, meta: Dict[str, Any]) -> None:
    tmp = meta_path.with_suffix(".meta.json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    os.replace(tmp, meta_path)


def _acquire_disk_lock(lock_path: Path) -> bool:
    # Best-effort exclusive lock file, removed if stale.
    if lock_path.exists():
        try:
            age = time.time() - lock_path.stat().st_mtime
            if age > _DISK_LOCK_TIMEOUT:
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
    except Exception:
        return False


def _release_disk_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def _try_read_disk_cached(abs_path: str, variant: str, sig: Tuple[int, int]) -> Optional[pd.DataFrame]:
    key_hex = _cache_key(abs_path, variant)
    parquet_path, pickle_path, meta_path = _paths_for_key(key_hex)

    meta = _read_meta(meta_path)
    if not meta:
        return None

    if tuple(meta.get("signature", ())) != sig:
        return None

    # Try parquet first
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path)
        except Exception:
            # Corrupt/missing engine, fall through to pickle
            pass

    if pickle_path.exists():
        try:
            return pd.read_pickle(pickle_path)
        except Exception:
            return None

    return None


def _write_disk_cached(abs_path: str, variant: str, sig: Tuple[int, int], df: pd.DataFrame) -> None:
    key_hex = _cache_key(abs_path, variant)
    parquet_path, pickle_path, meta_path = _paths_for_key(key_hex)
    lock_path = _DISK_CACHE_DIR / f"{key_hex}.lock"

    if not _acquire_disk_lock(lock_path):
        # Another process is writing; skip disk write (in-memory still works for this process).
        return

    try:
        meta = {
            "abs_path": abs_path,
            "variant": variant,
            "signature": list(sig),
            "created_at": pd.Timestamp.utcnow().isoformat(),
            "format": "parquet",
        }

        # Write parquet atomically
        try:
            tmp_parquet = parquet_path.with_suffix(".parquet.tmp")
            df.to_parquet(tmp_parquet, index=False)
            os.replace(tmp_parquet, parquet_path)

            # If a previous pickle exists, leave it (harmless) or remove it (optional).
            meta["format"] = "parquet"
            _write_meta_atomic(meta_path, meta)
            return

        except Exception as parquet_err:
            # Fallback to pickle if parquet engine is not installed or write fails
            # (keeps functionality; still parquet-first when available)
            try:
                tmp_pkl = pickle_path.with_suffix(".pkl.tmp")
                df.to_pickle(tmp_pkl)
                os.replace(tmp_pkl, pickle_path)
                meta["format"] = "pickle"
                meta["parquet_error"] = str(parquet_err)
                _write_meta_atomic(meta_path, meta)
                return
            except Exception:
                return

    finally:
        _release_disk_lock(lock_path)


def load_df_cached(
    path: str,
    variant: str,
    loader: Callable[[], pd.DataFrame],
) -> Optional[pd.DataFrame]:
    """
    Two-level cache:
      - L1: in-memory (per process)
      - L2: disk (shared across task_runner subprocesses)
    Invalidated when file signature (mtime_ns, size) changes.
    """
    if not os.path.exists(path):
        return None

    abs_path = os.path.abspath(path)
    sig = _signature(abs_path)
    key = (abs_path, variant)

    with _lock:
        cached = _df_cache.get(key)
        if cached is not None:
            cached_sig, cached_df = cached
            if cached_sig == sig:
                return cached_df

    # Try disk cache (outside lock is fine; read is atomic)
    df_disk = _try_read_disk_cached(abs_path, variant, sig)
    if df_disk is not None:
        with _lock:
            _df_cache[key] = (sig, df_disk)
        return df_disk

    # Load from source
    df = loader()

    # Update caches
    with _lock:
        _df_cache[key] = (sig, df)

    _write_disk_cached(abs_path, variant, sig, df)
    return df


def read_semicolon_csv_cached(path: str, encoding: str = "utf-8") -> Optional[pd.DataFrame]:
    variant = make_variant("csv:semicolon", encoding=encoding)
    return load_df_cached(
        path=path,
        variant=variant,
        loader=lambda: read_semicolon_csv(path, encoding=encoding),
    )


def read_excel_cached(path: str, **read_excel_kwargs: Any) -> Optional[pd.DataFrame]:
    variant = make_variant("excel", **read_excel_kwargs)
    return load_df_cached(
        path=path,
        variant=variant,
        loader=lambda: pd.read_excel(path, **read_excel_kwargs),
    )


def get_excel_sheet_names_cached(path: str) -> Optional[Tuple[str, ...]]:
    if not os.path.exists(path):
        return None

    abs_path = os.path.abspath(path)
    sig = _signature(abs_path)

    with _lock:
        cached = _sheet_cache.get(abs_path)
        if cached is not None:
            cached_sig, sheet_names = cached
            if cached_sig == sig:
                return sheet_names

        xls = pd.ExcelFile(abs_path)
        sheet_names = tuple(xls.sheet_names)
        _sheet_cache[abs_path] = (sig, sheet_names)
        return sheet_names


def clear_cache() -> None:
    # Clears only in-memory cache. Disk cache is intentionally retained across runs.
    with _lock:
        _df_cache.clear()
        _sheet_cache.clear()
