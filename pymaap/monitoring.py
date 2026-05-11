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
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
import inspect

from pymaap.logging_backend import get_log_queue, log_event
from pymaap.logging_setup import init_general_logger
logger = init_general_logger(__name__)

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

# --- Decorators ---

class Timer:
    """
    A decorator for timing and profiling function execution.
    
    By default, results are saved as a CSV file. With results_format="parquet",
    results are stored in a Parquet file, appending a new row for each call.
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

        # Only use multiprocessing Lock if multiprocessing is enabled
        self.file_lock = multiprocessing.Lock() if self.use_multiprocessing else None

        self._ensure_files_exist()
        self._setup_logging()

    def _ensure_files_exist(self):
        """Ensure necessary files exist with proper headers (for CSV mode)."""
        if self.use_multiprocessing and self.file_lock:
            with self.file_lock:
                self._create_files_if_needed()
        else:
            self._create_files_if_needed()

    def _create_files_if_needed(self):
        """Create necessary files safely, avoiding race conditions."""
        
        # Ensure CSV file exists and has headers
        if self.results_format == "csv":
            try:
                with open(self.RESULTS_FILE, mode="x", newline="") as file:  # 'x' mode prevents overwriting
                    writer = csv.writer(file)
                    writer.writerow(["Timestamp", "UUID", "Function Name", "Execution Time (s)", 
                                    "CPU Time (sec)", "Memory Change (MB)", "Final Memory Usage (MB)", "Arguments", "Log Message"])
                    logger.info(f"Created fresh {self.RESULTS_FILE}")
            except FileExistsError:
                pass  # Another process has already created the file

        # Ensure Parquet file exists
        elif self.results_format == "parquet":
            if not os.path.exists(self.RESULTS_FILE):  # Avoid unnecessary reads
                try:
                    df_empty = pd.DataFrame(columns=["Timestamp", "UUID", "Function Name", "Execution Time (s)", 
                                                    "CPU Time (sec)", "Memory Change (MB)", "Final Memory Usage (MB)", "Arguments", "Log Message"])
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
    
        # Ensure each process configures its own logging independently
        logger = logging.getLogger()
        if logger.hasHandlers():  # Prevent duplicate handlers
            return  
    
        logger.setLevel(logging.INFO)
    
        # Create a log queue for multiprocessing safety
        log_queue = multiprocessing.Queue() if self.use_multiprocessing else None
    
        # Set up file logging with rotation
        rotating_handler = RotatingFileHandler(self.LOG_FILE, maxBytes=self.max_bytes, 
                                               backupCount=self.backup_count)
        rotating_handler.setFormatter(JSONFormatter())
    
        # Set up console logging
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    
        if self.use_multiprocessing and log_queue:
            # Use a queue-based logging handler for multiprocessing safety
            queue_handler = QueueHandler(log_queue)
            logger.addHandler(queue_handler)
    
            # Set up a listener in the parent process to write logs safely
            listener = QueueListener(log_queue, rotating_handler, console_handler, respect_handler_level=True)
            listener.start()
        else:
            # Standard single-process logging
            logger.addHandler(rotating_handler)
            logger.addHandler(console_handler)

    def _save_results(self, timestamp, call_uuid, function_name, elapsed_time, cpu_time, mem_change, final_mem, args_repr, log_message):
        """Save timing and resource results to the chosen file format in a multiprocessing-safe manner."""
        process_id = os.getpid()
        thread_count = multiprocessing.cpu_count()
        row = [
            timestamp, process_id, thread_count, call_uuid, function_name, 
            elapsed_time, cpu_time, mem_change, final_mem, args_repr, log_message
        ]

        if self.results_format == "csv":
            # Ensure only one process writes at a time if multiprocessing is enabled
            if self.use_multiprocessing and self.file_lock:
                with self.file_lock:
                    self._write_csv(row)
            else:
                self._write_csv(row)

        elif self.results_format == "parquet":
            # Parquet writes are atomic, but we'll still use the lock to prevent race conditions
            if self.use_multiprocessing and self.file_lock:
                with self.file_lock:
                    self._write_parquet(row)
            else:
                self._write_parquet(row)

    def _write_csv(self, row):
        """Write a row to the CSV file safely using pandas for consistent headers."""
        columns = [
            "Timestamp", "Process ID", "Thread Count", "UUID", "Function Name",
            "Execution Time (s)", "CPU Time (sec)", "Memory Change (MB)",
            "Final Memory Usage (MB)", "Arguments", "Log Message"
        ]
        df = pd.DataFrame([row], columns=columns)
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

    def __call__(self, func):
        """Wrap the function call with timing and logging."""
        if func is None:  # For when no function is given
            return lambda f: self.__call__(f)  # Return a decorator function
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            call_uuid = str(uuid.uuid4())
            start_time = time.perf_counter()
            process = psutil.Process(os.getpid())
            cpu_start = (process.cpu_times().user + process.cpu_times().system) if self.track_resources else None
            mem_start = (process.memory_info().rss / (1024 ** 2)) if self.track_resources else None
    
            try:
                result = func(*args, **kwargs)
            except Exception as e:
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
    
            # Ensure only one process writes to the file at a time
            if self.log_to_file:
                if self.use_multiprocessing and self.file_lock:
                    with self.file_lock:
                        self._save_results(timestamp, call_uuid, func.__name__, elapsed_time, cpu_time, mem_change, final_mem, args_repr, log_message)
                else:
                    self._save_results(timestamp, call_uuid, func.__name__, elapsed_time, cpu_time, mem_change, final_mem, args_repr, log_message)
    
            return result
    
        return wrapper

class ErrorCatcher:
    """
    A decorator for catching and logging exceptions.
    
    Logs error details with a unique UUID and function name to a dedicated error log file
    (using log rotation, default: 10 MB max size, 5 backups). Optionally sanitizes the exception
    message and saves error details to a results file in CSV or Parquet format.
    """
    
    def __init__(self, log_to_console=True, log_to_file=True,
                 error_log_file=None, max_bytes=10*1024*1024, backup_count=5,
                 sanitize_func=None, results_format="csv", max_arg_length=None):
        self.log_to_console = log_to_console
        self.log_to_file = log_to_file
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.sanitize_func = sanitize_func
        self.max_arg_length = max_arg_length
        self.results_format = results_format.lower()

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
    
    def __call__(self, func=None):
        """Wrap the function call to catch exceptions, log them, and save error details."""
        if func is None: # when no function argument is given
            # Returning a wrapper function so that @ErrorCatcher() works
            return lambda f: self.__call__(f)
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
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
