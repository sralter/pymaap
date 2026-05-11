# PyMAAP: Python Monitoring and Analysis Package

![Tests](https://github.com/sralter/pymaap/actions/workflows/test.yml/badge.svg)
[![Coverage Report](https://img.shields.io/badge/coverage-html-blue)](https://sralter.github.io/pymaap/reports/pymaap/)

By Samuel Alter  
[Documentation](https://sralter.github.io/pymaap/)

## 0. Quickstart <a name='quickstart'></a>

[Back to TOC](#toc)

PyMAAP helps you monitor, log, and analyze the performance and behavior of your Python functions. It includes three main tools:
* `monitoring.py`: Decorators and tools to track performance, log errors, and save metrics.
* `analysis.py`: CLI script to analyze log files and generate insightful plots.
* `logging_setup.py`: Script to allow for logging statements to appear both in the console output and in a log file, allowing for the user to forgo the use of `print()`.

### Installation
```bash
uv pip install pymaap
```

To work on PyMAAP locally, clone the repo and install locked dev dependencies:

```bash
uv sync --all-extras
uv run pytest
```

The repo includes `uv.lock` so CI and contributors resolve the same dependency versions.

### Monitoring Functions

Import the decorators and wrap your functions:

```python
import pymaap
from pymaap.monitoring import Timer, ErrorCatcher, sanitizer

timer = Timer(results_format="csv", max_arg_length=200, sanitize_func=sanitizer)
error_handler = ErrorCatcher(results_format="csv", sanitize_func=sanitizer)

# initialize once (e.g. at program start)
pymaap.init_general_logger(
    log_dir="my_logs",
    general_log="general.log",
    json_log="general.json.log",
)

@error_handler
@timer
def my_function(x, y):
    logging.info(f"adding {x} and {y}…")
    result = x + y
    logging.debug(f"Result is {result}")
    return result
```

#### Options:

* `results_format`: `"csv"` or `"parquet"`
* `sanitize_func`: Custom sanitizer for sensitive args/logs
* `log_to_console`: Print logs to console (default `True`)
* `use_multiprocessing`: Use locks/log queues in multi-process apps

#### This creates:
* `logs/timing_results.csv` or `.parquet`
* `logs/error_results.csv` or `.parquet`
* `logs/timing.log`
* `logs/error.log`

#### Manual Metrics

You can also log timing manually using:

```python
from pymaap.monitoring import get_metrics_start, get_metrics_end

start_metrics = get_metrics_start("my_custom_block")
# ... your code ...
end_metrics = get_metrics_end(start_metrics)
```
### Analyzing Logs

Once you’ve collected logs, analyze them via CLI:

```python
python -m pymaap.analysis \
  --logdir logs \
  --subtitle "Post-deployment test" \
  --tag run1
```

#### CLI Options

* `--logdir`: Folder with `timing.log` files
* `--subtitle`: Subtitle for all plots (or `"none"` to disable)
* `--tag`: Folder name suffix for outputs (default: `"run"`)
* `--start-time` / `--end-time`: Optional override of time window

**Outputs** go to `figs/YYYY-MM-DD_HH-MM-SS_run1/:
* Execution plots
* Histograms
* Aggregate CSVs
* Metadata

#### Example output structure

```plaintext
figs/
└── 2025-04-01_14-00-01_run1/
    ├── execution_time_per_function.png
    ├── function_calls_over_time.png
    ├── memory_change_per_function_call.png
    ├── top10_functions_by_total_time.png
    ├── hist_*.png
    ├── results.csv
    ├── results_aggregate.csv
    └── README.txt
```

## 1. Overview <a name='overview'></a>

[Back to TOC](#toc)

A Python module for robust execution monitoring, error logging, and benchmarking analysis. This module provides two decorators, summarized below. **Note: these decorators currently do not work for functions using multiprocessing.**
* [**Timer**](#tp):
  * Logs function execution time, CPU usage, memory usage, and captures function arguments. Performance data is saved to a CSV file and logged in JSON format.
* [**ErrorCatcher**](#ep):
  * Catches and logs exceptions with a full traceback to a dedicated error log file using log rotation. Both decorators generate a unique UUID per function call for tracking.

This module also provides the following tools for implementing manual performance tracking and automated performance analysis. **Note: These tools are multiprocessing-safe.**
* [**`get_metrics_start()`**](#usage_metrics):
  * Logs initial CPU and memory state and starts timer for execution time measurement.
* [**`get_metrics_end()`**](#usage_metrics):
  * Logs final CPU and memory state and calculates execution time since `get_metrics_start()` call.
* [**results.py**](#resultspy_config):
  * Automatically grabs the most-recent, dense cluster of timestamps (i.e., the most recent run of your code) from the `.log` file to aggregate and plot benchmarking data into the following figures:
    * _Execution time per function_
    * _Function call timeline_
    * _Memory delta per function_
    * _Top-10 functions by total time_
    * _Histograms of execution time per function_
  * The script also creates three additional files:
    * _results.csv_: The full results file from the most-recent run
    * _filtered_log_lines.log_: The raw `.log` lines from the most-recent run
    * _results_aggregate_: The aggregate results showing:

| Column                      | Data Type | Description                                                     |
| --------------------------- | --------- | --------------------------------------------------------------- |
| **Function**                | String    | Function name                   |
| **Perf Duration (s)_count** | Integer   | Number of samples recorded for performance duration (s)          |
| **Perf Duration (s)_sum**   | Float     | Total sum of all performance durations (s)                      |
| **Perf Duration (s)_mean**  | Float     | Average performance duration (s)                                |
| **Perf Duration (s)_max**   | Float     | Maximum performance duration observed (s)                       |
| **CPU Delta_mean**          | Float     | Mean CPU change/delta                                             |
| **Memory Delta (MB)_mean**  | Float     | Mean change in memory (MB)                                        |

## 2. Table of Contents <a name='toc'></a>

- [PyMAAP: Python Monitoring and Analysis Package](#pymaap-python-monitoring-and-analysis-package)
  - [0. Quickstart](#quickstart)
    - [Installation](#installation)
    - [Monitoring Functions](#monitoring-functions)
      - [Options:](#options)
      - [This creates:](#this-creates)
      - [Manual Metrics](#manual-metrics)
    - [Analyzing Logs](#analyzing-logs)
      - [CLI Options](#cli-options)
      - [Example output structure](#example-output-structure)
  - [1. Overview](#overview)
  - [2. Table of Contents](#toc)
  - [3. Features](#features)
  - [4. Installation](#install)
  - [5. Configuration Options](#config)
    - [Timer Decorator Parameters](#tp)
    - [ErrorCatcher Decorator Parameters](#ep)
    - [`results.py` Parameters](#resultspy_config)
  - [6. Usage](#usage)
    - [Decorators](#decor)
      - [Example with the Timer decorator](#timer_example)
      - [Example with the ErrorCatcher decorator](#error_example)
      - [Example with both decorators combined](#combined)
    - [`results.py`](#resultspy)
      - [Applying metrics tracking](#usage_metrics)
      - [Running the analysis tool:](#running_results)
    - [General Logging Facility](#general_logging)
      - [Usage](#general-logging-usage)
  - [Customization](#custom)
    - [Performance Decorator Customization](#performance-decorator-customization)
  - [Contributing](#contribute)
   
## 3. Features <a name='features'></a>

[Back to TOC](#toc)

* **High-Resolution Timing**: Uses `time.perf_counter()` for precise measurements.
* **Process-Specific Metrics**: Tracks CPU time and memory usage for your process.
* **Structured Logging**: Outputs logs in JSON format for easy integration with log aggregation tools.
  * Saves log files in the working directory with the following structure:
  ```plaintext
  logs/
  ├── timing.log
  │   JSON-formatted performance logs
  │   (rotates at 10 MB, up to 5 backups)
  ├── timing_results.csv
  │   CSV file containing timing, CPU, and memory metrics
  │   for each function call
  └── error.log
      JSON-formatted error log capturing exceptions
      (rotates at 10 MB, up to 5 backups)
  ```
* **Error Logging**: Separates error logging from performance logs for clarity and monitoring.
* **Customizable Sanitation**: Optionally sanitize logged arguments or error messages to mask sensitive data.
* **Log Rotation**: Automatically rotates log files (default: 10 MB max size, 5 backups).
* **Performance Analysis**: Optional script can analyze the log files and output helpul charts for you to analyze the performance of your code, surfacing inefficiencies and areas for improvement.

## 4. Installation <a name='install'></a>

[Back to TOC](#toc)

Simply copy the monitoring.py file into your project. Ensure that you have the following Python packages installed:
* `psutil`
* `pandas`
* `geopandas` (if you plan to log dataframes)
* `matplotlib`
* `seaborn`
* (Standard library modules such as `logging`, `csv`, `json`, `functools`, etc., are included with Python.)

You can install these packages using `uv` and `pip`:
```bash
uv pip install psutil
```

## 5. Configuration Options <a name='config'></a>

[Back to TOC](#toc)

Both decorators accept several optional arguments to customize their behavior.

### Timer Decorator Parameters <a name='tp'></a>

* **`log_to_console (bool, default=True)`**:  
  Print log messages to the console.

* **`log_to_file (bool, default=True)`**:  
  Save log messages to a file (`timing.log` for performance logs).

* **`track_resources (bool, default=True)`**:  
  Track CPU and memory usage during function execution.

* **`max_arg_length (int or None, default=None)`**:  
  If set, function arguments are truncated to the specified maximum length when logged.

* **`sanitize_func (callable or None, default=None)`**:  
  A function to sanitize logged strings (e.g., masking sensitive data).  
  _Example:_  
  ```python
  def sanitizer(arg_str):
      return ''.join('*' if c.isdigit() else c for c in arg_str)
  ```

* **`results_format (str, default='csv')`**:
  Specify the format for saving timing results. Use `'csv'` (default) to save to a CSV file or `'parquet'` to save to a Parquet file.

### ErrorCatcher Decorator Parameters <a name='ep'></a>

* **`log_to_console (bool, default=True)`**:
  Print error logs to the console.

* **`log_to_file (bool, default=True)`**:
  Save error logs to a dedicated error log file (`error.log`).

* **`error_log_file (str or None, default=None)`**:
  Custom path for the error log file. If None, defaults to `logs/error.log`.

* **`max_bytes (int, default=10*1024*1024)`**:
  Maximum size in bytes for the error log file before rotation (default is 10 MB).

* **`backup_count (int, default=5)`**:
  Number of backup files to keep during log rotation.

* **`sanitize_func (callable or None, default=None)`**:
  A function to sanitize error messages before logging (useful for masking sensitive data).

* **`max_arg_length (int or None, default=None)`**:  
  If set, function arguments are truncated to the specified maximum length when logged in the error results.

### `results.py` Parameters <a name='resultspy_config'></a>

* **`--logdir`** (required):
  Points the script to the location of your `.log` files. It will automatically grab all log files and get the most-recent dense cluster of timestamps. They can be spread across your `.log` files.
* **`--subtitle`** (optional):
  Adds a common subtitle to every plot if the user wants to add helpful information. It defaults to the timestamp
* **`--tag`** (optional, default='run'):
  Optional name for the folder to add a short description. The folder will include the timestamp of the end of the run.
* **`--start-time`** (optional):
  Optional override for the start time of the run.
* **`--end-time`** (optional ):
  Optional override for the end time of the run.

## 6. Usage <a name='usage'></a>

[Back to TOC](#toc)

Import the decorators from the module and apply them to your functions. You can use them individually or together. 

Likewise with the `results.py` script.

### Decorators <a name='decor'></a>

#### Example with the Timer decorator <a name='timer_example'></a>

[Back to TOC](#toc)

```python
from monitoring import Timer
import logging
import time

@Timer(max_arg_length=100, sanitize_func=lambda s: s.replace("secret", "*****"))
def my_function(x, y):
    """Dummy function that simulates processing with the timer decorator."""
    logging.info("My function started")
    time.sleep(2)  # Simulate processing
    logging.info("My function completed")
    return x + y

result = my_function(5, 10)
print(f"Result: {result}")
```

**What Happens**:
* The function’s start and completion messages are logged.
* Execution time, CPU time, memory change, and final memory usage are recorded in `logs/timing_results.csv`.
* A JSON-formatted log is written to `logs/timing.log`, which rotates once it reaches 10 MB.
* Function arguments are sanitized and truncated as specified.

#### Example with the ErrorCatcher decorator <a name='error_example'></a>

[Back to TOC](#toc)

```python
from monitoring import ErrorCatcher
import logging

@ErrorCatcher(sanitize_func=lambda s: s.replace("password", "*****"))
def error_function():
    """Dummy function that simulates processing with an error and the error-catching decorator."""
    logging.info("Error function starting")
    # Deliberately raise an exception to test error logging.
    raise ValueError("This is a test error with a secret password!")

try:
    error_function()
except Exception as e:
    print(f"Caught an exception as expected: {e}")
```

**What Happens**:
* When `error_function` raises an exception, the ErrorCatcher logs a detailed error message (including a sanitized exception message) with a full traceback.
* The error log is written to `logs/error.log` with log rotation (default: 10 MB max, 5 backups).
* The exception is re‑raised so you can catch it in your code.

#### Example with both decorators combined <a name='combined'></a>

[Back to TOC](#toc)

You can stack the decorators to get both error logging and performance modeling. 

Note that `ErrorCatcher` should be the outer decorator (i.e., placed above `Timer`). This way, it catches any exception thrown by the function itself or even from within the `Timer` decorator:

```python
from monitoring import Timer, ErrorCatcher, sanitizer
import logging
import time

@ErrorCatcher(sanitize_func=sanitizer)
@Timer(max_arg_length=100, sanitize_func=sanitizer)
def combined_function(a, b):
    """Dummy function to demonstrate both uses of monitoring decorators."""
    logging.info("Combined function started")
    time.sleep(1)
    if a < 0:
        raise ValueError("Invalid value for a!")
    logging.info("Combined function completed")
    return a * b

# Successful run
print("Combined function result:", combined_function(3, 7))

# Exception case
try:
    combined_function(-1, 7)
except Exception as e:
    print(f"Caught an exception: {e}")
```

### `results.py` <a name='resultspy'></a>

[Back to TOC](#toc)

#### Applying metrics tracking <a name='usage_metrics'></a>

```python
def my_function():
    metrics_start = get_metrics_start()
    if early_exit:
        metrics_end = get_metrics_end(metrics_start=metrics_start)
        return my_early_result

    # function logic here...

    metrics_end = get_metrics_end(metrics_start=metrics_start)
    return my_result
```

#### Running the analysis tool: <a name='running_results'></a>

The script works in your CLI. After running your script that produces logs, incorporating the decorators and tracking hooks, the `.log` file will include benchmarking information in raw form. The `results.py` script will scan the `.log` file and grab the most recent run of your script

```bash
python results.py \
  --logdir ./logs \
  --subtitle "Run after memory tweaks" \
  --tag run_memory_tweak \
  --start-time "2025-01-01 12:00:00" \
  --end-time "2025-01-01 12:00:42"
```

### General Logging Facility <a name='general_logging'></a>

PyMAAP also provides a single entry point for all application-level messages, with:

- **Rotating file** (plain-text) at `<log_dir>/general.log`  
- **Rotating file** (JSON) at `<log_dir>/general.json.log`  
- **Console output** (identical format)  

All records include:
1. **Timestamp** (`YYYY-MM-DD HH:MM:SS,mmm`)  
2. **Log level** (INFO, WARNING, etc.)  
3. **UUID** (unique per record)  
4. **Logger name & function** (`[module.funcName]`)  
5. **Your message**  

#### Usage <a name='general_logging_usage'></a>

```python
import logging
import pymaap

# initialize once (e.g. at program start)
pymaap.init_general_logger(
    log_dir="my_logs",
    general_log="general.log",
    json_log="general.json.log",
)

def multiply(x, y):
    logging.info(f"multiplying {x} and {y}…")
    result = x * y
    logging.debug(f"Result is {result}")
    return x * y

if __name__ == "__main__":
    multiply(3, 4)
    # console:
    # 2025-04-28 12:00:00,123 INFO 123e4567-e89b-12d3-a456-426655440000 [__main__.multiply] multiplying 3 and 4…
    # 2025-04-28 12:00:00,123 INFO 123e4568-e89b-12d3-a456-426655440000 [__main__.multiply] Result is 12
    #
    # Files created under my_logs/:
    # • general.log        (plain-text, same format)  
    # • general.json.log   (JSON objects, one per line)
```

Be sure to call `init_general_logger()` before your first `logging.*` call so handlers are installed.

## Customization <a name='custom'></a>

[Back to TOC](#toc)

### Performance Decorator Customization

* **Sanitization**:  
  * Provide your own sanitizer function to remove or mask sensitive data from logged messages.
  
* **Log Rotation Parameters**:  
  * Both Timer and ErrorCatcher use rotating file handlers. You can adjust the max file size and backup count by passing parameters to the decorators if needed.

## Contributing <a name='contribute'></a>

[Back to TOC](#toc)

Feel free to fork this repository and submit pull requests if you have improvements or bug fixes.
