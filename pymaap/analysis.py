# pymaap/analysis.py

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pymaap.logging_setup import init_general_logger
logger = init_general_logger(__name__)


def parse_log_timestamp(ts: str) -> datetime:
    """
    Parse a ``timestamp`` string from JSON log lines.

    Accepts fractional seconds after a comma (default ``logging`` ``asctime`` style,
    e.g. ``timing.log`` from :class:`~pymaap.monitoring.Timer`) or after a dot
    (e.g. ``general.json.log`` from :class:`~pymaap.logging_setup.JSONFormatter`).
    """
    for fmt in ("%Y-%m-%d %H:%M:%S,%f", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized timestamp format: {ts!r}")


def load_all_log_lines(logdir: Path):
    """
    Gathers all .log file(s) contents into one object

    Args:
        logdir (Path): Location of the .log file(s)

    Returns:
        log_lines: Object containing all lines from the .log file(s)
    """
    log_files = sorted(logdir.glob("timing.log*"))
    log_lines = []
    for file in log_files:
        with open(file) as f:
            for line in f:
                try:
                    record = json.loads(line)
                    if "timestamp" in record and "message" in record:
                        log_lines.append(record)
                except json.JSONDecodeError:
                    continue
    return log_lines


def detect_recent_dense_block(log_lines, min_cluster_size=25, gap_seconds=30):
    """
    Detect the most recent cluster of densely occurring log entries.

    This function examines a list of log lines (each containing a timestamp) and identifies clusters
    of timestamps where the gap between consecutive timestamps is less than or equal to `gap_seconds`.
    If the gap_seconds are larger than 30 seconds, then the function will consider the clusters separate.
    Only clusters with at least `min_cluster_size` timestamps are considered valid. The function
    returns the earliest and latest timestamps of the most recent (i.e., latest in time) valid cluster.

    Parameters:
        log_lines (list): A list of dictionaries, each containing a ``timestamp`` string
        parseable by :func:`parse_log_timestamp` (comma or dot before fractional seconds).
        min_cluster_size (int, optional): Minimum number of timestamps required for a cluster to be considered valid. Default is 25.
        gap_seconds (float, optional): Maximum allowed gap in seconds between consecutive timestamps in a cluster. Default is 30.

    Returns:
        tuple: A tuple (start_time, end_time) where:
            - start_time (datetime): The earliest timestamp in the most recent valid cluster.
            - end_time (datetime): The latest timestamp in the most recent valid cluster.
            If no valid cluster is found, returns (None, None).
    """
    timestamps = sorted([parse_log_timestamp(line["timestamp"]) for line in log_lines])

    if not timestamps:
        return None, None

    clusters = []
    cluster = [timestamps[0]]

    for curr, nxt in zip(timestamps, timestamps[1:]):
        if (nxt - curr).total_seconds() <= gap_seconds:
            cluster.append(nxt)
        else:
            if len(cluster) >= min_cluster_size:
                clusters.append(cluster)
            cluster = [nxt]
    if len(cluster) >= min_cluster_size:
        clusters.append(cluster)

    if not clusters:
        return None, None

    most_recent = clusters[-1]
    return min(most_recent), max(most_recent)


def parse_log_lines(log_lines, start_time=None, end_time=None):
    """
    Parses log lines to identify the performance metrics of the function.

    Args:
        log_lines: Object containing all the log lines.
        start_time (optional): Start time of the run. If None, no lower bound is applied.
        end_time (optional): End time of the run. If None, no upper bound is applied.

    Returns:
        tuple: ``(pd.DataFrame, filtered_lines)`` — parsed metrics and the log lines
        retained after the time filter.
    """
    def in_window(line):
        ts = parse_log_timestamp(line["timestamp"])
        if start_time is not None and ts < start_time:
            return False
        if end_time is not None and ts > end_time:
            return False
        return True

    filtered_lines = [line for line in log_lines if in_window(line)]

    pattern = re.compile(
        r"(?P<func>[^\s:]+): (?P<type>start|end): wall=(?P<wall>[\d.]+) perf=(?P<perf>[\d.]+) id=(?P<id>[\w-]+)"
        r"(?: duration=(?P<duration>[\d.]+)sec)? cpu=(?P<cpu>[\d.]+)% rss=(?P<rss>\d+) vms=(?P<vms>\d+)"
        r" mem%=(?P<mem>[\d.]+) threads=(?P<threads>\d+) fds=(?P<fds>\d+)"
    )

    execution_data = defaultdict(dict)

    for line in filtered_lines:
        match = pattern.search(line["message"])
        if match:
            d = match.groupdict()
            key = (d["func"], d["id"])
            parsed = {
                "wall": float(d["wall"]),
                "perf": float(d["perf"]),
                "cpu": float(d["cpu"]),
                "rss": int(d["rss"]),
                "vms": int(d["vms"]),
                "mem_percent": float(d["mem"]),
                "threads": int(d["threads"]),
                "fds": int(d["fds"]),
                "timestamp": parse_log_timestamp(line["timestamp"])
            }
            if d["duration"]:
                parsed["duration"] = float(d["duration"])
            execution_data[key][d["type"]] = parsed

    records = []
    for (func, id_), event in execution_data.items():
        if "start" in event and "end" in event:
            records.append({
                "Function": func,
                "Call ID": id_,
                "Start Time": event["start"]["timestamp"],
                "End Time": event["end"]["timestamp"],
                "Wall Duration (s)": event["end"]["wall"] - event["start"]["wall"],
                "Perf Duration (s)": event["end"]["perf"] - event["start"]["perf"],
                "Duration (from log)": event["end"].get("duration", None),
                "Start CPU (%)": event["start"]["cpu"],
                "End CPU (%)": event["end"]["cpu"],
                "Start RSS": event["start"]["rss"],
                "End RSS": event["end"]["rss"],
                "Start Mem %": event["start"]["mem_percent"],
                "End Mem %": event["end"]["mem_percent"],
                "Start Threads": event["start"]["threads"],
                "End Threads": event["end"]["threads"],
                "Start FDs": event["start"]["fds"],
                "End FDs": event["end"]["fds"]
            })

    df = pd.DataFrame(records).sort_values("Start Time")
    return df, filtered_lines


def generate_plots(df: pd.DataFrame, output_dir: Path, subtitle: str):
    """
    Generates analytical plots and tables from extracted performance metrics.

    Args:
        df (pd.DataFrame): Table containing the parsed and organized performance metrics.
        output_dir (Path): Location of where the plots should be saved to.
        subtitle (str): Optional argument from script call to add informative subtitle to every plot.
                        If an empty string is passed, no subtitle will be added.
    """
    sns.set_theme(style="whitegrid")
    font = {
        'family': 'sans serif', 'color': 'grey',
        'weight': 'normal', 'size': 10, 'style': 'italic'
    }

    df["CPU Delta"] = df["End CPU (%)"] - df["Start CPU (%)"]
    df["Memory Delta (MB)"] = (df["End RSS"] - df["Start RSS"]) / 1e6
    df["Start Seconds"] = (df["Start Time"] - df["Start Time"].min()).dt.total_seconds()

    # Execution time per function
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df, x="Function", y="Perf Duration (s)", orientation='vertical')
    plt.suptitle("Execution Time per Function")
    if subtitle:
        plt.title(subtitle, fontdict=font, y=1.05)
    plt.xticks(rotation=45)
    plt.grid(visible=True, axis='y')
    plt.tight_layout()
    plt.savefig(output_dir / "execution_time_per_function.png", bbox_inches='tight')
    plt.close()

    # Function call timeline
    plt.figure(figsize=(12, 6))
    sns.scatterplot(data=df, x="Start Seconds", y="Function", size="Perf Duration (s)",
                    hue="Perf Duration (s)", palette="coolwarm", sizes=(20, 200))
    plt.suptitle("Function Calls Over Time")
    if subtitle:
        plt.title(subtitle, fontdict=font, y=1.05)
    plt.xlabel("Time since start (seconds)")
    plt.grid(visible=True, axis='x')
    plt.tight_layout()
    plt.savefig(output_dir / "function_calls_over_time.png", bbox_inches='tight')
    plt.close()

    # Memory delta per function
    plt.figure(figsize=(12, 6))
    sns.barplot(data=df, x="Function", y="Memory Delta (MB)")
    plt.suptitle("Memory Change per Function Call")
    if subtitle:
        plt.title(subtitle, fontdict=font, y=1.05)
    plt.xticks(rotation=45)
    plt.grid(visible=True, axis='y')
    plt.tight_layout()
    plt.savefig(output_dir / "memory_change_per_function_call.png", bbox_inches='tight')
    plt.close()

    # Top 10 functions by total time
    agg = df.groupby("Function").agg({
        "Perf Duration (s)": ["count", "sum", "mean", "max"],
        "CPU Delta": "mean",
        "Memory Delta (MB)": "mean"
    }).sort_values(("Perf Duration (s)", "sum"), ascending=False)
    agg.columns = ['_'.join(col).strip() for col in agg.columns.values]
    top_funcs = agg.head(10).index

    plt.figure(figsize=(12, 6))
    sns.barplot(data=df[df["Function"].isin(top_funcs)],
                x="Function", y="Perf Duration (s)")
    plt.suptitle("Top 10 Functions by Total Time")
    if subtitle:
        plt.title(subtitle, fontdict=font, y=1.05)
    plt.xticks(rotation=45)
    plt.grid(visible=True, axis='y')
    plt.tight_layout()
    plt.savefig(output_dir / "top10_functions_by_total_time.png", bbox_inches='tight')
    plt.close()

    # Histograms for each function
    for i, func in enumerate(df['Function'].unique()):
        plt.figure(figsize=(4, 4))
        sns.histplot(data=df[df["Function"] == func], x="Perf Duration (s)", bins=20)
        plt.suptitle(f"Perf Duration for '{func}'")
        if subtitle:
            plt.title(subtitle, fontdict=font, y=1.05)
        plt.tight_layout()
        plt.savefig(output_dir / f"hist_{i+1}_{func}_perf_duration.png", bbox_inches='tight')
        plt.close()

    # Save data
    df.to_csv(output_dir / "results.csv", index=False)
    agg.to_csv(output_dir / "results_aggregate.csv")


def write_metadata(output_dir, start_time, end_time, subtitle, log_files):
    """
    Metadata text file showing the parameters used in running the results.py script.

    Args:
        output_dir (Path): Output directory to save all files.
        start_time (datetime): Start time of run, user-defined or automatically determined.
        end_time (datetime): End time of run, user-defined or automatically determined.
        subtitle (str): Subtitle used in the plots. If empty, no subtitle was applied.
        log_files (iterable): List or iterable of log file paths used.
    """
    with open(output_dir / "README.txt", "w") as f:
        f.write("=== Timing Analysis Metadata ===\n")
        f.write("Log files used:\n")
        for log in log_files:
            f.write(f"  - {log}\n")
        f.write(f"\nTime window: {start_time} → {end_time}\n")
        f.write(f"Subtitle:    {subtitle}\n")


def analysis(args=None):
    """
    Function for argument handling when running the script.
    """
    parser = argparse.ArgumentParser(description="Parse timing logs and generate performance plots.")
    parser.add_argument("--logdir", type=str, required=True, help="Directory containing timing.log files")
    parser.add_argument("--subtitle", type=str, required=False, help='Subtitle for all plots. '
                                                                     'Pass "none" to disable subtitles, '
                                                                     'or omit to default to "start_time to end_time".')
    parser.add_argument("--tag", type=str, default="run", help="Folder name tag")
    parser.add_argument("--start-time", type=str, help="Optional override for start time (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--end-time", type=str, help="Optional override for end time (YYYY-MM-DD HH:MM:SS)")

    args = parser.parse_args(args)
    logdir = Path(args.logdir)

    log_lines = load_all_log_lines(logdir)
    if not log_lines:
        logger.warning("No valid log entries found in directory.")
        return

    # Parse manual time overrides
    start_time = datetime.strptime(args.start_time, "%Y-%m-%d %H:%M:%S") if args.start_time else None
    end_time = datetime.strptime(args.end_time, "%Y-%m-%d %H:%M:%S") if args.end_time else None

    # Auto-detect start and end time if not provided
    if not start_time or not end_time:
        detected_start, detected_end = detect_recent_dense_block(log_lines)
        if not detected_start:
            logger.warning("Could not detect a recent cluster of calls.")
            return
        start_time = start_time or detected_start
        end_time = end_time or detected_end
        logger.info("Using auto-detected time window for missing value(s).")

    # Determine subtitle mode:
    #   - If --subtitle is not provided: default to "start_time to end_time"
    #   - If --subtitle is provided as "none" (case insensitive): disable subtitle (empty string)
    #   - Otherwise, use the user-provided subtitle.
    if args.subtitle is None:
        subtitle = f"{start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}"
    elif args.subtitle.lower() == "none":
        subtitle = ""
    else:
        subtitle = args.subtitle

    timestamp_tag = end_time.strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = Path("figs") / f"{timestamp_tag}_{args.tag}"
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Time window: {start_time} → {end_time}")
    logger.info(f"Subtitle: {subtitle}")

    df, filtered_lines = parse_log_lines(log_lines, start_time, end_time)
    if df.empty:
        logger.warning("No function calls found in selected time window.")
        return

    # Save filtered raw logs
    raw_log_out = output_dir / "filtered_log_lines.log"
    with open(raw_log_out, "w") as f:
        for line in filtered_lines:
            f.write(json.dumps(line) + "\n")

    generate_plots(df, output_dir, subtitle)
    write_metadata(output_dir, start_time, end_time, subtitle, sorted(logdir.glob("timing.log*")))
    logger.info("Analysis complete. Results written to: %s", output_dir)

if __name__ == "__main__":
    analysis()
