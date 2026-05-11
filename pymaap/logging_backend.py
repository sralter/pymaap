# pymaap/logging_backend.py

import multiprocessing
import atexit
import json
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

_log_queue = None
_writer_process = None
_log_file_path = None

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "function": getattr(record, "function_name", "N/A"),
            "uuid": getattr(record, "uuid", "N/A"),
        }
        return json.dumps(log_record)

def _writer_worker(queue, log_file):
    logger = logging.getLogger("mp_logger")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)

    while True:
        record = queue.get()
        if record == "STOP":
            break
        logger.handle(record)

def init_multiprocessing_logging(log_file="logs/timing.log"):
    global _log_queue, _writer_process, _log_file_path

    log_dir = os.path.dirname(log_file) or "."
    os.makedirs(log_dir, exist_ok=True)
    _log_queue = multiprocessing.Queue()
    _log_file_path = log_file
    _writer_process = multiprocessing.Process(target=_writer_worker, args=(_log_queue, log_file))
    _writer_process.start()

    # Ensure it shuts down cleanly
    atexit.register(shutdown_multiprocessing_logging)

def shutdown_multiprocessing_logging():
    global _log_queue, _writer_process
    if _log_queue and _writer_process and _writer_process.is_alive():
        _log_queue.put("STOP")
        _writer_process.join()
    # if _log_queue:
    #     _log_queue.put("STOP")
    # if _writer_process:
    #     _writer_process.join()

def get_log_queue():
    return _log_queue

def log_event(level, msg, extra=None):
    """
    Log an event to the appropriate backend: queue (if active) or std logging.
    """
    record = logging.LogRecord(
        name="pymaap",
        level=level,
        pathname=__file__,
        lineno=0,
        msg=msg,
        args=(),
        exc_info=None
    )
    if extra:
        for k, v in extra.items():
            setattr(record, k, v)

    log_queue = get_log_queue()
    # Only main process should push to the queue; workers log to their own stderr
    if log_queue and multiprocessing.current_process().name == "MainProcess":
        log_queue.put(record)
    else:
        logger = logging.getLogger("pymaap")
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
            logger.addHandler(handler)
        logger.handle(record)
