"""
Timer with ``use_multiprocessing=True`` and ``multiprocessing.Pool``.

Uses a top-level *body* function plus an explicit wrapper so the callable
pickles correctly under spawn (macOS / Windows default).

Run from the repository root::

    uv run python tutorial/02_multiprocessing_pool.py
"""
from __future__ import annotations

import logging
import time
from multiprocessing import Pool

import pymaap
from pymaap.monitoring import Timer

LOG_DIR = "logs/tutorial_multiprocessing"


def work_body(seconds: float) -> str:
    """Plain user code; must stay a top-level symbol for ``Pool.map``."""
    time.sleep(seconds)
    return f"slept {seconds}s"


# Picklable wrapper (holds Manager queue proxy) — pass *this* to ``Pool.map``.
work = Timer(
    log_to_console=False,
    log_to_file=True,
    results_format="csv",
    use_multiprocessing=True,
)(work_body)


def main() -> None:
    pymaap.init_general_logger(
        log_dir=LOG_DIR,
        general_log="general.log",
        json_log="general.json.log",
    )
    logging.getLogger(__name__).info("Starting pool (2 workers × short sleep)")
    with Pool(2) as pool:
        results = pool.map(work, [0.12, 0.12])
    print("Pool finished:", results)
    print("Timing rows appended via writer thread → logs/timing_results.csv (Timer defaults)")


if __name__ == "__main__":
    main()
