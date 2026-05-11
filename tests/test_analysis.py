# tests/test_analysis.py

import pandas as pd
from pathlib import Path
from unittest import mock
import pytest
from datetime import datetime, timedelta
from pymaap.analysis import detect_recent_dense_block, parse_log_lines, generate_plots, parse_log_timestamp

# Helper to generate fake log lines
def fake_log(ts, message):
    return {
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S,%f"),
        "message": message
    }

@pytest.fixture
def dense_cluster_logs():
    now = datetime.now()
    logs = [
        fake_log(now + timedelta(seconds=i), "func: start: wall=1.0 perf=1.0 id=abc cpu=1.0% rss=1000 vms=2000 mem%=5.0 threads=2 fds=10")
        for i in range(30)
    ] + [
        fake_log(now + timedelta(seconds=i), "func: end: wall=2.0 perf=2.0 id=abc duration=1.0sec cpu=2.0% rss=1100 vms=2100 mem%=6.0 threads=2 fds=10")
        for i in range(30)
    ]
    return logs

def test_detect_recent_dense_block_detects_cluster(dense_cluster_logs):
    start, end = detect_recent_dense_block(dense_cluster_logs)
    assert isinstance(start, datetime)
    assert isinstance(end, datetime)
    assert (end - start).total_seconds() > 25

def test_parse_log_lines_creates_dataframe(dense_cluster_logs):
    now = datetime.now()
    df, _ = parse_log_lines(dense_cluster_logs, now, now + timedelta(seconds=60))
    assert not df.empty
    assert "Function" in df.columns
    assert "Perf Duration (s)" in df.columns
    assert df.iloc[0]["Function"] == "func"


def test_parse_log_lines_unbounded_window_includes_all(dense_cluster_logs):
    """start_time/end_time None must not compare against None (regression)."""
    df, lines = parse_log_lines(dense_cluster_logs, None, None)
    assert len(lines) == len(dense_cluster_logs)
    assert not df.empty


def test_parse_log_timestamp_accepts_comma_and_dot_fraction():
    a = parse_log_timestamp("2025-06-01 12:00:00,123456")
    b = parse_log_timestamp("2025-06-01 12:00:00.123456")
    assert a == b
    # millisecond-style (still valid %f in Python 3.10+)
    c = parse_log_timestamp("2025-06-01 12:00:00.123")
    assert c.year == 2025 and c.month == 6 and c.microsecond == 123000


def test_parse_log_timestamp_rejects_garbage():
    with pytest.raises(ValueError, match="Unrecognized timestamp"):
        parse_log_timestamp("not-a-timestamp")


def test_detect_recent_dense_block_with_dot_timestamps():
    """Timestamps with a dot before fractional seconds (general JSON logs) must cluster correctly."""
    now = datetime(2025, 6, 1, 12, 0, 0)
    logs = []
    for i in range(30):
        ts = now + timedelta(seconds=i)
        dot = ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        logs.append({
            "timestamp": dot,
            "message": "func: start: wall=1.0 perf=1.0 id=x cpu=1.0% rss=1 vms=1 mem%=1.0 threads=1 fds=1",
        })
    for i in range(30):
        ts = now + timedelta(seconds=i)
        dot = ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        logs.append({
            "timestamp": dot,
            "message": "func: end: wall=2.0 perf=2.0 id=x duration=1.0sec cpu=2.0% rss=1 vms=1 mem%=1.0 threads=1 fds=1",
        })
    start, end = detect_recent_dense_block(logs)
    assert start is not None and end is not None
    assert (end - start).total_seconds() > 25


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "Function": ["func"] * 5,
        "Call ID": [f"id_{i}" for i in range(5)],
        "Start Time": pd.date_range("2025-01-01", periods=5, freq="1s"),
        "End Time": pd.date_range("2025-01-01 00:00:01", periods=5, freq="1s"),
        "Wall Duration (s)": [1.0] * 5,
        "Perf Duration (s)": [0.9 + i * 0.01 for i in range(5)],
        "Duration (from log)": [1.0] * 5,
        "Start CPU (%)": [10] * 5,
        "End CPU (%)": [20] * 5,
        "Start RSS": [1000] * 5,
        "End RSS": [2000] * 5,
        "Start Mem %": [5.0] * 5,
        "End Mem %": [6.0] * 5,
        "Start Threads": [2] * 5,
        "End Threads": [2] * 5,
        "Start FDs": [10] * 5,
        "End FDs": [10] * 5
    })

@mock.patch("matplotlib.pyplot.savefig")
def test_generate_plots_saves_figures(mock_savefig, sample_df, tmp_path):
    generate_plots(sample_df, tmp_path, subtitle="Unit Test Subtitle")
    assert mock_savefig.call_count >= 5  # boxplot, scatter, bars, etc.
