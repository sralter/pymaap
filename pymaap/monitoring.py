# pymaap/monitoring.py

import json
import logging
import os
import csv
import functools
import time
import psutil
import uuid
import multiprocessing
from datetime import datetime
import pandas as pd
import geopandas as gpd
from logging.handlers import RotatingFileHandler
import inspect

from pymaap.logging_backend import get_log_queue, log_event
from pymaap.logging_setup import init_general_logger
from pymaap.multiprocessing_support import ensure_mp_writer, get_mp_queue

logger = init_general_logger(__name__)

# Columns for Timer CSV/Parquet (seed file + every append must match).
_TIMER_RESULT_COLUMNS = (
    "Timestamp",
    "Process ID",
    "Thread Count",
    "UUID",
    "Function Name",
    "Execution Time (s)",
    "CPU Time (sec)",
    "Memory Change (MB)",
    "Final Memory Usage (MB)",
    "Arguments",
    "Log Message",
)

# --- Helpers ---

def sanitizer(arg_str):
    """
    Example sanitizer that replaces any digits with '*'.
    Usage:
      timer = Timer(max_arg_length=100, sanitize_func=sanitizer)
      error_handler = ErrorCatcher(sanitize_func=sanitizer)
    """
    return ''.join('*' if c.isdigit() else c for c in arg_str)

class JSONFormatter(logging.Formatter):
    """Formats JSON"""
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "function": record.__dict__.get("function_name", "N/A"),
            "uuid": record.__dict__.get("uuid", "N/A")
        }
        return json.dumps(log_record)


class _PicklableTimerWrapper:
    """
    Callable that embeds the Timer instance so ``multiprocessing.Pool`` pickles
    the Manager queue proxy (spawn-safe). Module-level decorated functions are
    otherwise unpickled by reference and would lose Timer state.
    """

    def __init__(self, timer: "Timer", func):
        self._timer = timer
        self._func = func
        functools.update_wrapper(
            self,
            func,
            assigned=("__module__", "__name__", "__qualname__", "__doc__", "__annotations__"),
            updated=(),
        )

    def __reduce_ex__(self, protocol):
        return (
            _restore_picklable_timer_wrapper,
            (self._timer.__dict__, self._func.__module__, self._func.__qualname__),
        )

    def __call__(self, *args, **kwargs):
        return self._timer._run_wrapped(self._func, args, kwargs)


def _restore_picklable_timer_wrapper(timer_dict, mod: str, qualname: str):
    import importlib

    t = Timer.__new__(Timer)
    t.__dict__.update(timer_dict)
    modobj = importlib.import_module(mod)
    func = getattr(modobj, qualname)
    return _PicklableTimerWrapper(t, func)


class _PicklableErrorCatcherWrapper:
    """Picklable ErrorCatcher wrapper for use with ``multiprocessing`` (see Timer)."""

    def __init__(self, catcher: "ErrorCatcher", func):
        self._catcher = catcher
        self._func = func
        functools.update_wrapper(
            self,
            func,
            assigned=("__module__", "__name__", "__qualname__", "__doc__", "__annotations__"),
            updated=(),
        )

    def __reduce_ex__(self, protocol):
        c = self._catcher
        state = (
            c.log_to_console,
            c.log_to_file,
            c.error_log_file,
            c.max_bytes,
            c.backup_count,
            c.sanitize_func,
            c.results_format,
            c.max_arg_length,
            c.use_multiprocessing,
            c.RESULTS_FILE,
            c._mp_results_queue,
        )
        return (
            _restore_picklable_error_catcher_wrapper,
            (state, self._func.__module__, self._func.__qualname__),
        )

    def __call__(self, *args, **kwargs):
        return self._catcher._run_error_wrapped(self._func, args, kwargs)


def _restore_picklable_error_catcher_wrapper(state, mod: str, qualname: str):
    import importlib

    c = ErrorCatcher.__new__(ErrorCatcher)
    (
        c.log_to_console,
        c.log_to_file,
        c.error_log_file,
        c.max_bytes,
        c.backup_count,
        c.sanitize_func,
        c.results_format,
        c.max_arg_length,
        c.use_multiprocessing,
        c.RESULTS_FILE,
        c._mp_results_queue,
    ) = state
    os.makedirs("logs", exist_ok=True)
    c._ensure_error_file()
    c._setup_error_logging()
    modobj = importlib.import_module(mod)
    func = getattr(modobj, qualname)
    return _PicklableErrorCatcherWrapper(c, func)


# --- Decorators ---

class Timer:
    """
    A decorator for timing and profiling function execution.

    By default, results are saved as a CSV file. With results_format="parquet",
    results are stored in a Parquet file, appending a new row for each call.

    With ``use_multiprocessing=True``, result rows are sent to a background writer
    thread via a ``multiprocessing.Manager`` queue (spawn-safe). The decorated callable
    is a picklable object so it can be used with ``multiprocessing.Pool``; define
    decorated functions in the main process (typically at import time) before
    starting worker pools.
    """
    def __init__(self, log_to_console=True, log_to_file=True, backup_count=5,
                 max_bytes=10*1024*1024, track_resources=True, max_arg_length=None, 
                 sanitize_func=None, results_format="csv", use_multiprocessing=False):
        self.log_to_console = log_to_console
        self.log_to_file = log_to_file
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.track_resources = track_resources
        self.max_arg_length = max_arg_length
        self.sanitize_func = sanitize_func
        self.results_format = results_format.lower()
        self.use_multiprocessing = use_multiprocessing  # flag for multiprocessing

        self.log_dir = "logs"
        os.makedirs(self.log_dir, exist_ok=True)
        
        if self.results_format == "csv":
            self.RESULTS_FILE = os.path.join(self.log_dir, "timing_results.csv")
        elif self.results_format == "parquet":
            self.RESULTS_FILE = os.path.join(self.log_dir, "timing_results.parquet")
        else:
            raise ValueError("results_format must be either 'csv' or 'parquet'")

        self.LOG_FILE = os.path.join(self.log_dir, "timing.log")

        self._mp_results_queue = None
        if self.use_multiprocessing:
            # Worker processes re-import user code under "spawn"; do not start the
            # Manager here. Pickled Timer instances carry the queue proxy without __init__.
            if multiprocessing.current_process().name == "MainProcess":
                ensure_mp_writer()
                self._mp_results_queue = get_mp_queue()

        self._ensure_files_exist()
        self._setup_logging()

    def _ensure_files_exist(self):
        """Ensure necessary files exist with proper headers (for CSV mode)."""
        self._create_files_if_needed()

    def _create_files_if_needed(self):
        """Create necessary files safely, avoiding race conditions."""
        
        # Ensure CSV file exists and has headers
        if self.results_format == "csv":
            try:
                with open(self.RESULTS_FILE, mode="x", newline="") as file:  # 'x' mode prevents overwriting
                    writer = csv.writer(file)
                    writer.writerow(list(_TIMER_RESULT_COLUMNS))
                    logger.info(f"Created fresh {self.RESULTS_FILE}")
            except FileExistsError:
                pass  # Another process has already created the file

        # Ensure Parquet file exists
        elif self.results_format == "parquet":
            if not os.path.exists(self.RESULTS_FILE):  # Avoid unnecessary reads
                try:
                    df_empty = pd.DataFrame(columns=list(_TIMER_RESULT_COLUMNS))
                    df_empty.to_parquet(self.RESULTS_FILE, index=False)
                    logger.info(f"Created fresh {self.RESULTS_FILE}")
                except FileExistsError:
                    pass  # Another process has already created the file

        # Ensure log file exists
        try:
            with open(self.LOG_FILE, mode="x") as file:
                logger.info(f"Created fresh {self.LOG_FILE}")
        except FileExistsError:
            pass  # Another process has already created the file
        
    def _setup_logging(self):
        """Configure logging with JSON formatting and log rotation, ensuring multiprocessing safety."""
        if self.use_multiprocessing:
            # Results are serialized via multiprocessing_support; root logger setup here
            # previously used a per-Timer queue/listener that is not spawn-safe. Module
            # ``logger`` (init_general_logger) is used for console-style messages in wrappers.
            return

        # Ensure each process configures its own logging independently
        root = logging.getLogger()
        if root.hasHandlers():  # Prevent duplicate handlers
            return

        root.setLevel(logging.INFO)

        rotating_handler = RotatingFileHandler(self.LOG_FILE, maxBytes=self.max_bytes,
                                               backupCount=self.backup_count)
        rotating_handler.setFormatter(JSONFormatter())

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        root.addHandler(rotating_handler)
        root.addHandler(console_handler)

    def _save_results(self, timestamp, call_uuid, function_name, elapsed_time, cpu_time, mem_change, final_mem, args_repr, log_message):
        """Save timing and resource results to the chosen file format in a multiprocessing-safe manner."""
        process_id = os.getpid()
        thread_count = multiprocessing.cpu_count()
        row = [
            timestamp, process_id, thread_count, call_uuid, function_name,
            elapsed_time, cpu_time, mem_change, final_mem, args_repr, log_message
        ]

        if self.use_multiprocessing:
            if self._mp_results_queue is None:
                raise RuntimeError(
                    "Timer multiprocessing queue is missing. Build @Timer(..., use_multiprocessing=True) "
                    "in the main process before using this callable in a worker pool."
                )
            self._mp_results_queue.put({
                "kind": f"timing_{self.results_format}",
                "path": self.RESULTS_FILE,
                "row": row,
            })
            return

        if self.results_format == "csv":
            self._write_csv(row)
        elif self.results_format == "parquet":
            self._write_parquet(row)

    def _write_csv(self, row):
        """Write a row to the CSV file safely using pandas for consistent headers."""
        df = pd.DataFrame([row], columns=list(_TIMER_RESULT_COLUMNS))
        header = not os.path.exists(self.RESULTS_FILE) or os.path.getsize(self.RESULTS_FILE) == 0
        df.to_csv(self.RESULTS_FILE, mode="a", header=header, index=False)

    def _write_parquet(self, row):
        """Write a row to the Parquet file safely."""
        row_dict = {
            "Timestamp": row[0],
            "Process ID": row[1],
            "Thread Count": row[2],
            "UUID": row[3],
            "Function Name": row[4],
            "Execution Time (s)": row[5],
            "CPU Time (sec)": row[6],
            "Memory Change (MB)": row[7],
            "Final Memory Usage (MB)": row[8],
            "Arguments": row[9],
            "Log Message": row[10]
        }

        # Read existing parquet file if it exists and append new data
        try:
            df_existing = pd.read_parquet(self.RESULTS_FILE)
            df_new = pd.DataFrame([row_dict])
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        except (FileNotFoundError, ValueError):
            # File does not exist or is empty, so create a new DataFrame
            df_combined = pd.DataFrame([row_dict])

        df_combined.to_parquet(self.RESULTS_FILE, index=False)

    def _run_wrapped(self, func, args, kwargs):
        """Execute ``func`` with timing, logging, and optional result persistence."""
        call_uuid = str(uuid.uuid4())
        start_time = time.perf_counter()
        process = psutil.Process(os.getpid())
        cpu_start = (process.cpu_times().user + process.cpu_times().system) if self.track_resources else None
        mem_start = (process.memory_info().rss / (1024 ** 2)) if self.track_resources else None

        try:
            result = func(*args, **kwargs)
        except Exception:
            log_event(logging.ERROR, f"Function `{func.__name__}` raised an exception",
                      extra={"function_name": func.__name__, "uuid": call_uuid})
            raise

        elapsed_time = time.perf_counter() - start_time
        cpu_end = (process.cpu_times().user + process.cpu_times().system) if self.track_resources else None
        mem_end = (process.memory_info().rss / (1024 ** 2)) if self.track_resources else None

        cpu_time = cpu_end - cpu_start if cpu_start is not None else None
        mem_change = mem_end - mem_start if mem_start is not None else None
        final_mem = mem_end if mem_end is not None else None

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        def safe_serialize(obj):
            """Convert args/kwargs to string with optional sanitization and truncation."""
            if isinstance(obj, (pd.DataFrame, gpd.GeoDataFrame)):
                return f"<DataFrame with {len(obj)} rows>"
            try:
                s = str(obj)
            except Exception:
                s = "<unserializable>"
            if self.sanitize_func:
                s = self.sanitize_func(s)
            if self.max_arg_length is not None and len(s) > self.max_arg_length:
                s = s[:self.max_arg_length] + "..."
            return s

        args_repr = json.dumps({
            "args": [safe_serialize(arg) for arg in args],
            "kwargs": {k: safe_serialize(v) for k, v in kwargs.items()}
        })

        log_message = f"Function `{func.__name__}` executed in {elapsed_time:.4f} sec"
        if self.track_resources:
            log_message += f", CPU Time: {cpu_time:.4f} sec, Memory Change: {mem_change:.4f} MB, Final Memory: {final_mem:.4f} MB"
        if self.log_to_console:
            logger.info(log_message)
        log_event(logging.INFO, log_message, extra={"function_name": func.__name__,
                                                     "uuid": call_uuid})

        if self.log_to_file:
            self._save_results(timestamp, call_uuid, func.__name__, elapsed_time, cpu_time, mem_change, final_mem, args_repr, log_message)

        return result

    def __call__(self, func):
        """Wrap the function call with timing and logging."""
        if func is None:  # For when no function is given
            return lambda f: self.__call__(f)

        if self.use_multiprocessing:
            return _PicklableTimerWrapper(self, func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return self._run_wrapped(func, args, kwargs)

        return wrapper

class ErrorCatcher:
    """
    A decorator for catching and logging exceptions.

    Logs error details with a unique UUID and function name to a dedicated error log file
    (using log rotation, default: 10 MB max size, 5 backups). Optionally sanitizes the exception
    message and saves error details to a results file in CSV or Parquet format.

    Set ``use_multiprocessing=True`` when the wrapped function runs in worker processes so
    error result rows are serialized through the same writer queue as :class:`Timer`.
    """

    def __init__(self, log_to_console=True, log_to_file=True,
                 error_log_file=None, max_bytes=10*1024*1024, backup_count=5,
                 sanitize_func=None, results_format="csv", max_arg_length=None,
                 use_multiprocessing=False):
        self.log_to_console = log_to_console
        self.log_to_file = log_to_file
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.sanitize_func = sanitize_func
        self.max_arg_length = max_arg_length
        self.results_format = results_format.lower()
        self.use_multiprocessing = use_multiprocessing
        self._mp_results_queue = None
        if self.use_multiprocessing:
            if multiprocessing.current_process().name == "MainProcess":
                ensure_mp_writer()
                self._mp_results_queue = get_mp_queue()

        if self.results_format == "csv":
            self.RESULTS_FILE = os.path.join("logs", "error_results.csv")
        elif self.results_format == "parquet":
            self.RESULTS_FILE = os.path.join("logs", "error_results.parquet")
        else:
            raise ValueError("results_format must be either 'csv' or 'parquet'")
            
        if error_log_file is None:
            self.error_log_file = os.path.join("logs", "error.log")
        else:
            self.error_log_file = error_log_file
        
        os.makedirs("logs", exist_ok=True)
        self._ensure_error_file()
        self._setup_error_logging()
    
    def _ensure_error_file(self):
        """Ensure the error results file exists (for CSV mode)."""
        if self.results_format == "csv":
            if not os.path.exists(self.RESULTS_FILE):
                with open(self.RESULTS_FILE, mode="w", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerow(["Timestamp", "UUID", "Function Name", "Error Message", "Arguments"])
                logger.info(f"Created fresh {self.RESULTS_FILE}")
    
    def _setup_error_logging(self):
        """Set up a dedicated logger for error catching with JSON formatting and log rotation."""
        self.logger = logging.getLogger("error_catcher")
        self.logger.setLevel(logging.ERROR)
        self.logger.handlers = []
        if self.log_to_file:
            rotating_handler = RotatingFileHandler(self.error_log_file, maxBytes=self.max_bytes, backupCount=self.backup_count)
            rotating_handler.setFormatter(JSONFormatter())
            self.logger.addHandler(rotating_handler)
        if self.log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
            self.logger.addHandler(console_handler)
    
    def _safe_serialize(self, obj):
        """Serialize an object to string with optional sanitization and truncation."""
        try:
            s = str(obj)
        except Exception:
            s = "<unserializable>"
        if self.sanitize_func:
            s = self.sanitize_func(s)
        if self.max_arg_length is not None and len(s) > self.max_arg_length:
            s = s[:self.max_arg_length] + "..."
        return s
    
    def _save_error(self, timestamp, call_uuid, function_name, error_msg, args_repr):
        """Save error details to the chosen results file format."""
        if self.use_multiprocessing:
            if self._mp_results_queue is None:
                raise RuntimeError(
                    "ErrorCatcher multiprocessing queue is missing. Build @ErrorCatcher(..., use_multiprocessing=True) "
                    "in the main process before using this callable in a worker pool."
                )
            if self.results_format == "csv":
                self._mp_results_queue.put({
                    "kind": "error_csv",
                    "path": self.RESULTS_FILE,
                    "row": [timestamp, call_uuid, function_name, error_msg, args_repr],
                })
            else:
                self._mp_results_queue.put({
                    "kind": "error_parquet",
                    "path": self.RESULTS_FILE,
                    "row": {
                        "Timestamp": timestamp,
                        "UUID": call_uuid,
                        "Function Name": function_name,
                        "Error Message": error_msg,
                        "Arguments": args_repr,
                    },
                })
            return

        if self.results_format == "csv":
            columns = ["Timestamp", "UUID", "Function Name", "Error Message", "Arguments"]
            row = [timestamp, call_uuid, function_name, error_msg, args_repr]
            df = pd.DataFrame([row], columns=columns)
            header = not os.path.exists(self.RESULTS_FILE) or os.path.getsize(self.RESULTS_FILE) == 0
            df.to_csv(self.RESULTS_FILE, mode="a", header=header, index=False)
        elif self.results_format == "parquet":
            row = {
                "Timestamp": timestamp,
                "UUID": call_uuid,
                "Function Name": function_name,
                "Error Message": error_msg,
                "Arguments": args_repr
            }
            # Append the new row to existing data (if any)
            try:
                df_existing = pd.read_parquet(self.RESULTS_FILE)
                df_new = pd.DataFrame([row])
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            except (FileNotFoundError, ValueError):
                df_combined = pd.DataFrame([row])
            df_combined.to_parquet(self.RESULTS_FILE, index=False)

    def _run_error_wrapped(self, func, args, kwargs):
        call_uuid = str(uuid.uuid4())
        try:
            return func(*args, **kwargs)
        except Exception as e:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_msg = str(e)
            if self.sanitize_func:
                error_msg = self.sanitize_func(error_msg)
            args_repr = json.dumps({
                "args": [self._safe_serialize(arg) for arg in args],
                "kwargs": {k: self._safe_serialize(v) for k, v in kwargs.items()}
            })
            log_event(
                logging.ERROR,
                f"Function `{func.__name__}` raised an exception: {error_msg}",
                extra={"function_name": func.__name__, "uuid": call_uuid}
            )
            self._save_error(timestamp, call_uuid, func.__name__, error_msg, args_repr)
            raise

    def __call__(self, func=None):
        """Wrap the function call to catch exceptions, log them, and save error details."""
        if func is None:
            return lambda f: self.__call__(f)

        if self.use_multiprocessing:
            return _PicklableErrorCatcherWrapper(self, func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return self._run_error_wrapped(func, args, kwargs)

        return wrapper

# --- Manual benchmarking tools ---

process = psutil.Process()
def get_caller_name():
    """Gets name of function that the get_metrics_* is in."""
    frame = inspect.currentframe()
    outer = inspect.getouterframes(frame)[2]  # skip self and calling helper
    return outer.function

def get_metrics_start(func_name: str = None):
    """Tracks initial performance metrics and adds a line in the log file."""
    if func_name is None:
        func_name = get_caller_name()

    call_id = str(uuid.uuid4())
    wall_start = time.time()
    perf_start = time.perf_counter()
    cpu_percent = process.cpu_percent(interval=None)
    mem_info = process.memory_info()
    mem_percent = process.memory_percent()
    cpu_times = process.cpu_times()
    num_threads = process.num_threads()
    num_fds = process.num_fds()

    metrics = {
        "func": func_name,
        "id": call_id,
        "wall_start": wall_start,
        "perf_start": perf_start,
        "cpu_start_percent": cpu_percent,
        "rss_start": mem_info.rss,
        "vms_start": mem_info.vms,
        "mem_percent_start": mem_percent,
        "cpu_times_start": cpu_times,
        "num_threads_start": num_threads,
        "num_fds_start": num_fds,
    }

    logging.info(
        "%s: start: wall=%.4f perf=%.4f id=%s cpu=%.2f%% rss=%d vms=%d mem%%=%.2f threads=%d fds=%d",
        func_name, wall_start, perf_start, call_id, cpu_percent,
        mem_info.rss, mem_info.vms, mem_percent, num_threads, num_fds
    )

    return metrics

def get_metrics_end(metrics_start: dict, func_name: str = None):
    """Tracks final performance metrics, including duration, and adds line in .log file."""
    if func_name is None:
        func_name = metrics_start["func"]

    call_id = metrics_start["id"]
    wall_end = time.time()
    perf_end = time.perf_counter()
    duration = perf_end - metrics_start["perf_start"]
    cpu_percent = process.cpu_percent(interval=None)
    mem_info = process.memory_info()
    mem_percent = process.memory_percent()
    cpu_times = process.cpu_times()
    num_threads = process.num_threads()
    num_fds = process.num_fds()

    logging.info(
        "%s: end: wall=%.4f perf=%.4f id=%s duration=%.4fsec cpu=%.2f%% rss=%d vms=%d mem%%=%.2f threads=%d fds=%d",
        func_name, wall_end, perf_end, call_id, duration, cpu_percent,
        mem_info.rss, mem_info.vms, mem_percent, num_threads, num_fds
    )

    metrics = {
        "func": func_name,
        "id": call_id,
        "wall_end": wall_end,
        "perf_end": perf_end,
        "duration": duration,
        "cpu_end_percent": cpu_percent,
        "rss_end": mem_info.rss,
        "vms_end": mem_info.vms,
        "mem_percent_end": mem_percent,
        "cpu_times_end": cpu_times,
        "num_threads_end": num_threads,
        "num_fds_end": num_fds,
    }

    return metrics
