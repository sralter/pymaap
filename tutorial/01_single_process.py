"""
Single-process Timer + ErrorCatcher (default path).

Run from the repository root::

    uv run python tutorial/01_single_process.py
"""
from __future__ import annotations

import logging

import pymaap
from pymaap.monitoring import ErrorCatcher, Timer, sanitizer

LOG_DIR = "logs/tutorial_single_process"

timer = Timer(
    log_to_console=True,
    log_to_file=True,
    results_format="csv",
    max_arg_length=120,
    sanitize_func=sanitizer,
    use_multiprocessing=False,
)
errors = ErrorCatcher(
    log_to_console=True,
    log_to_file=True,
    results_format="csv",
    sanitize_func=sanitizer,
    use_multiprocessing=False,
)


@errors
@timer
def add(a: int, b: int) -> int:
    logging.info("adding %s + %s", a, b)
    return a + b


@errors
@timer
def divide(a: float, b: float) -> float:
    return a / b


def main() -> None:
    pymaap.init_general_logger(
        log_dir=LOG_DIR,
        general_log="general.log",
        json_log="general.json.log",
    )
    print("Single-process examples →", LOG_DIR, "(timing/error CSVs under logs/ per Timer defaults)")
    print("add(2, 3) =", add(2, 3))
    try:
        divide(1.0, 0.0)
    except ZeroDivisionError:
        print("divide(1, 0) raised as expected; error row written by ErrorCatcher")


if __name__ == "__main__":
    main()
