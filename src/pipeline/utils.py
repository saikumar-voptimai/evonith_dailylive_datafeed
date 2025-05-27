"""
Utility functions for date parsing, range, and data checking.
"""
from datetime import datetime, timedelta
import logging
import os
from typing import Optional
import yaml
import logging.config

logger = logging.getLogger('pipeline')

def daterange(start_date, end_date):
    """
    Yields date strings from start_date to end_date (inclusive) in MM-DD-YYYY format.
    """
    logger.info(f"daterange called with start_date={start_date}, end_date={end_date}")
    start = datetime.strptime(start_date, '%m-%d-%Y')
    end = datetime.strptime(end_date, '%m-%d-%Y')
    delta = timedelta(days=1)
    count = 0
    while start <= end:
        date_str = start.strftime('%m-%d-%Y')
        logger.debug(f"Yielding date: {date_str}")
        yield date_str
        start += delta
        count += 1
    logger.info(f"daterange generated {count} dates")

def check_existing_data(cleaned_data, args):
    """
    Stub for checking if data already exists in InfluxDB and if value difference > 0.000001.
    Returns False to always write (implement as needed).
    """
    logger.info("check_existing_data called (stub)")
    # TODO: implement existence and delta logic
    logger.debug(f"Received {len(cleaned_data)} records for existence check")
    return False

def setup_run_logging_yaml(date_str: str, time_str: str=None, mode: str='live', range_param: str=None, pid: int=None, log_dir: str = "logs", yaml_path: str = "config/logging.yaml") -> str:
    """
    Set up per-run logging using a YAML config, but with a dynamic log file path.
    Args:
        date_str: Date string for the run (e.g. '05-24-2025')
        mode: 'daily' or 'live'
        range_param: Range parameter as string
        pid: Process ID
        log_dir: Directory to store log files
        yaml_path: Path to the YAML logging config
    Returns:
        The path to the log file for this run.
    """
    import os
    os.makedirs(log_dir, exist_ok=True)
    if mode == 'live':
        log_path = os.path.join(log_dir, f"{mode}_{date_str}_{time_str}_{pid}.log")
    else:
        log_path = os.path.join(log_dir, f"{mode}_{date_str}_{range_param}_{pid}.log")
    with open(yaml_path, 'r') as f:
        config = yaml.safe_load(f)
    # Update the file handler's filename
    if 'handlers' in config and 'file' in config['handlers']:
        config['handlers']['file']['filename'] = log_path
    logging.config.dictConfig(config)
    return log_path
