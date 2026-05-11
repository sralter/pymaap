"""
Same program: single-process Timer first, then a multiprocessing Pool with MP Timer.

Shows both code paths in one script (typical “warm up on main, fan out work” pattern).

Run from the repository root::

    uv run python tutorial/03_single_then_multiprocessing.py
"""
from __future__ import annotations

import logging
import time
from multiprocessing import Pool

import pymaap
from pymaap.monitoring import Timer

LOG_DIR = "logs/tutorial_single_then_mp"

# --- Single-process Timer (direct @ syntax is fine here) ---
single_timer = Timer(
    log_to_console=True,
    log_to_file=True,
    results_format="csv",
    use_multiprocessing=False,
)


@single_timer
def greet(name: str) -> str:
    logging.info("hello %s", name)
    time.sleep(0.05)
    return f"Hello, {name}"


# --- Multiprocessing section (body + wrapper for pickling) ---
def job_body(n: int) -> int:
    time.sleep(0.08)
    return n * n


job = Timer(
    log_to_console=False,
    log_to_file=True,
    results_format="csv",
    use_multiprocessing=True,
)(job_body)


def main() -> None:
    pymaap.init_general_logger(
        log_dir=LOG_DIR,
        general_log="general.log",
        json_log="general.json.log",
    )
    print("--- Single-process ---")
    print(greet("PyMAAP"))

    print("--- Multiprocessing Pool ---")
    with Pool(2) as pool:
        out = pool.map(job, [1, 2, 3])
    print("pool.map(job, ...) →", out)


if __name__ == "__main__":
    main()
