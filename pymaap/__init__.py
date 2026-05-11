from .logging_setup import init_general_logger
from .monitoring import Timer, ErrorCatcher, get_metrics_start, get_metrics_end
from .analysis import generate_plots, parse_log_lines, detect_recent_dense_block

from importlib.metadata import version

__version__ = version("pymaap")

__all__ = [
    "init_general_logger",
    "Timer",
    "ErrorCatcher",
    "get_metrics_start",
    "get_metrics_end",
    "generate_plots",
    "parse_log_lines",
    "detect_recent_dense_block",
]
