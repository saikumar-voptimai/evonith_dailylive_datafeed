"""
Utility functions for date parsing, range, and data checking.
"""

import logging
import logging.config
import os
from datetime import datetime, timedelta

import yaml

logger = logging.getLogger("pipeline")


def daterange(start_date, end_date):
    """
    Yields date strings from start_date to end_date (inclusive) in MM-DD-YYYY format.
    Args:
        start_date (str): Start date in MM-DD-YYYY format.
        end_date (str): End date in MM-DD-YYYY format.
    Yields:
        str: Date string in MM-DD-YYYY format for each day in the range.
    """
    logger.info("daterange called with start_date=%s, end_date=%s", start_date, end_date)
    start = datetime.strptime(start_date, "%m-%d-%Y")
    end = datetime.strptime(end_date, "%m-%d-%Y")
    delta = timedelta(days=1)
    count = 0
    while start <= end:
        date_str = start.strftime("%m-%d-%Y")
        logger.debug("Yielding date: %s", date_str)
        yield date_str
        start += delta
        count += 1
    logger.info("daterange generated %d dates", count)


def check_existing_data(cleaned_data):
    """
    Stub for checking if data already exists in InfluxDB and if value difference > 0.000001.
    Args:
        cleaned_data (list): List of cleaned records to check.
        args: CLI args or config for DB connection (unused in stub).
    Returns:
        bool: False (always writes; implement as needed).
    """
    logger.info("check_existing_data called (stub)")
    # TODO: implement existence and delta logic
    logger.debug("Received %d records for existence check", len(cleaned_data))
    return False


def setup_run_logging_yaml(
    date_str: str,
    time_str: str = None,
    mode: str = "live",
    range_param: str = None,
    pid: int = None,
    log_dir: str = "logs",
    yaml_path: str = "config/logging.yaml",
) -> str:
    """
    Sets up per-run logging using a YAML config, with a dynamic log file path.
    Args:
        date_str (str): Date string for the run (e.g. '05-24-2025').
        time_str (str, optional): Time string for the run (used in live mode).
        mode (str, optional): 'daily' or 'live'.
        range_param (str, optional): Range parameter as string.
        pid (int, optional): Process ID.
        log_dir (str, optional): Directory to store log files.
        yaml_path (str, optional): Path to the YAML logging config.
    Returns:
        str: The path to the log file for this run.
    """

    os.makedirs(log_dir, exist_ok=True)
    if mode == "live":
        log_path = os.path.join(log_dir, f"{mode}_{date_str}_{time_str}_{pid}.log")
    else:
        log_path = os.path.join(log_dir, f"{mode}_{date_str}_{range_param}_{pid}.log")
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)
    # Update the file handler's filename
    if "handlers" in config and "file" in config["handlers"]:
        config["handlers"]["file"]["filename"] = log_path
    logging.config.dictConfig(config)
    return log_path
