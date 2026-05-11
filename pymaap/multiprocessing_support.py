# pymaap/multiprocessing_support.py
"""
Lazy multiprocessing support: a Manager-backed queue and a single writer thread
in the main process append timing/error results safely (spawn-safe).

Construct ``Timer(..., use_multiprocessing=True)`` or ``ErrorCatcher(..., use_multiprocessing=True)``
in the **main** process (usually at import time) so the Manager starts before workers run.

For ``multiprocessing.Pool``, pass a picklable wrapper returned from the decorator applied to a
**separate top-level** function (e.g. ``work = Timer(..., use_multiprocessing=True)(work_body)``) so
the inner ``work_body`` keeps a stable import name; see README Multiprocessing section.
"""

from __future__ import annotations

import atexit
import logging
import multiprocessing
import os
import threading
from typing import Any, Dict, List, Optional

_manager: Optional[multiprocessing.managers.SyncManager] = None
_queue: Optional[Any] = None  # manager.Queue proxy
_writer_thread: Optional[threading.Thread] = None
_started = False
_bootstrap_lock = threading.Lock()
_atexit_registered = False

_SENTINEL = object()


def ensure_mp_writer() -> None:
    """
    Start the Manager, shared results queue, and writer thread (main process only).

    Idempotent. Called from Timer / ErrorCatcher __init__ when use_multiprocessing=True.
    """
    global _manager, _queue, _writer_thread, _started, _atexit_registered

    if multiprocessing.current_process().name != "MainProcess":
        raise RuntimeError(
            "PyMAAP multiprocessing: construct @Timer(..., use_multiprocessing=True) or "
            "@ErrorCatcher(..., use_multiprocessing=True) in the main process (typically at "
            "import time) before starting worker processes. See README Multiprocessing section."
        )

    with _bootstrap_lock:
        if _started:
            return
        _manager = multiprocessing.Manager()
        _queue = _manager.Queue()
        _writer_thread = threading.Thread(target=_writer_loop, args=(_queue,), daemon=True)
        _writer_thread.start()
        _started = True
        if not _atexit_registered:
            atexit.register(_shutdown_mp_writer)
            _atexit_registered = True


def get_mp_queue():
    """Return the shared results queue proxy (after ensure_mp_writer)."""
    if not _started or _queue is None:
        raise RuntimeError("Multiprocessing writer not started; construct a Timer or ErrorCatcher with use_multiprocessing=True in the main process first.")
    return _queue


def _shutdown_mp_writer() -> None:
    global _manager, _queue, _writer_thread, _started
    if not _started or _queue is None:
        return
    try:
        _queue.put(_SENTINEL)
    except Exception:
        pass
    if _writer_thread is not None:
        _writer_thread.join(timeout=10.0)
    if _manager is not None:
        try:
            _manager.shutdown()
        except Exception:
            pass
    _manager = None
    _queue = None
    _writer_thread = None
    _started = False


def _writer_loop(q: Any) -> None:
    log = logging.getLogger("pymaap.multiprocessing_support")
    while True:
        try:
            item = q.get()
        except (EOFError, BrokenPipeError, OSError):
            break
        except Exception:
            log.debug("Writer queue get failed", exc_info=True)
            break
        if item is _SENTINEL:
            break
        if not isinstance(item, dict):
            continue
        kind = item.get("kind")
        try:
            if kind == "timing_csv":
                _append_timing_csv(item["path"], item["row"])
            elif kind == "timing_parquet":
                _append_timing_parquet(item["path"], item["row"])
            elif kind == "error_csv":
                _append_error_csv(item["path"], item["row"])
            elif kind == "error_parquet":
                _append_error_parquet(item["path"], item["row"])
        except Exception:
            log.exception("Writer thread failed processing %s", kind)


def _append_timing_csv(path: str, row: List[Any]) -> None:
    import pandas as pd
    from pymaap.monitoring import _TIMER_RESULT_COLUMNS

    df = pd.DataFrame([row], columns=list(_TIMER_RESULT_COLUMNS))
    header = not os.path.exists(path) or os.path.getsize(path) == 0
    df.to_csv(path, mode="a", header=header, index=False)


def _append_timing_parquet(path: str, row: List[Any]) -> None:
    import pandas as pd
    from pymaap.monitoring import _TIMER_RESULT_COLUMNS

    keys = list(_TIMER_RESULT_COLUMNS)
    row_dict = {keys[i]: row[i] for i in range(len(keys))}
    try:
        df_existing = pd.read_parquet(path)
        df_new = pd.DataFrame([row_dict])
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    except (FileNotFoundError, ValueError, OSError):
        df_combined = pd.DataFrame([row_dict])
    df_combined.to_parquet(path, index=False)


_ERROR_CSV_COLUMNS = ["Timestamp", "UUID", "Function Name", "Error Message", "Arguments"]


def _append_error_csv(path: str, row: List[Any]) -> None:
    import pandas as pd

    df = pd.DataFrame([row], columns=_ERROR_CSV_COLUMNS)
    header = not os.path.exists(path) or os.path.getsize(path) == 0
    df.to_csv(path, mode="a", header=header, index=False)


def _append_error_parquet(path: str, row: Dict[str, Any]) -> None:
    import pandas as pd

    try:
        df_existing = pd.read_parquet(path)
        df_new = pd.DataFrame([row])
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    except (FileNotFoundError, ValueError, OSError):
        df_combined = pd.DataFrame([row])
    df_combined.to_parquet(path, index=False)
