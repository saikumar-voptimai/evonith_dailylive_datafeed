"""
API client for fetching data from the Blast Furnace API with retry logic.
"""

import logging
import os
from datetime import datetime
from time import sleep
from xml.etree import ElementTree

import requests
import yaml
from dotenv import load_dotenv

from src.pipeline.data_cleaner import clean_and_parse_data
from src.pipeline.influx_writer import process_and_write
from src.pipeline.run_tracker import log_run

logger = logging.getLogger("pipeline")

with open("config/config.yaml", "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f) or {}

load_dotenv()

USER_LIVE = os.getenv("API_USER_LIVE")
PASSWORD_LIVE = os.getenv("API_PASSWORD_LIVE")
API_URL_LIVE = os.getenv("API_URL_LIVE")
USER_DAILY = os.getenv("API_USER_DAILY")
PASSWORD_DAILY = os.getenv("API_PASSWORD_DAILY")
API_URL_DAILY = os.getenv("API_URL_DAILY")


def fetch_api_data(date_str, range_param, max_retries=3, delay=10):
    """
    Fetches raw XML data from the daily API for a given date and range, with retry logic.
    Args:
        date_str (str): Date in MM-DD-YYYY format.
        range_param (int): Range parameter for API (e.g., 1 or 2).
        max_retries (int): Maximum number of retry attempts on failure.
        delay (int): Delay in seconds between retries.
    Returns:
        str: Raw XML response as a string if successful.
    Raises:
        Exception: If all retries fail or response is empty/invalid.
    """
    logger.info(f"fetch_api_data(date={date_str}, range_param={range_param})")
    month, day, year = [int(x) for x in date_str.split("-")]
    params = {
        "user": USER_DAILY,
        "password": PASSWORD_DAILY,
        "month": month,
        "day": day,
        "year": year,
        "range": range_param,
    }
    logger.debug(f"Request params: {params}")
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(API_URL_DAILY, params=params, timeout=60)
            logger.info(f"API response status code: {response.status_code}")
            response.raise_for_status()
            root = ElementTree.fromstring(response.text)
            assert root.text is not None, "API response is empty"
            logger.debug(
                f"API response text: {root.text[:100]}..."
            )  # Log first 100 chars for brevity
            return root.text
        except Exception as e:
            logger.error(f"API fetch failed (attempt {attempt}): {e}")
            if attempt < max_retries:
                sleep(delay)
            else:
                raise


def fetch_api_data_live(max_retries=3, delay=5):
    """
    Fetches raw XML data from the live API for current data, with retry logic.
    Args:
        max_retries (int): Maximum number of retry attempts on failure.
        delay (int): Delay in seconds between retries.
    Returns:
        str: Raw XML response as a string if successful.
    Raises:
        Exception: If all retries fail or response is empty/invalid.
    """
    logger.info("fetch_api_data_live() called")
    params = {"user": USER_LIVE, "password": PASSWORD_LIVE}
    logger.debug(f"Live request params: {params}")
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(API_URL_LIVE, params=params, timeout=60)
            logger.info(f"API response status code: {response.status_code}")
            response.raise_for_status()
            # Extract the text inside the <string> tag
            root = ElementTree.fromstring(response.text)
            return root.text
        except Exception as e:
            logger.error(f"API fetch failed (attempt {attempt}): {e}")
            if attempt < max_retries:
                sleep(delay)
            else:
                raise


def process_datewise(
    dt: datetime.date,
    range_param: int,
    log_run_to_localdb: bool,
    args=None,
    log_path: str = None,
    variables_list=None,
):
    """
    Orchestrates fetching, cleaning, processing, and writing of data for a specific date and range.
    Args:
        dt (datetime.date): Date to process.
        range_param (int): Range parameter for API (e.g., 1 or 2).
        log_run_to_localdb (bool): Whether to log the run to the local SQLite DB.
        args: CLI arguments namespace (optional).
        log_path (str): Path to the log file for this run (optional).
        variables_list (list, optional): List of variables to filter and write.
    Returns:
        None. Logs results and optionally writes run metadata to DB.
    """
    num_records, success = 0, False
    dt_str = dt.strftime(CONFIG["DATE_FORMAT"])
    dt_str_file = dt.strftime(CONFIG["DATE_FORMAT_FILENAME"])
    points_file_path = None
    try:
        st = datetime.now()
        raw = fetch_api_data(dt_str, range_param)
        logger.info(
            f"Fetched raw data for {dt_str} in {(datetime.now() - st).total_seconds()} seconds"
        )
        logger.debug(f"Fetched historical raw data for {dt}")
        try:
            st = datetime.now()
            cleaned_list = clean_and_parse_data(raw)
            logger.info(
                f"Cleaned raw data for {dt_str} in {(datetime.now() - st).total_seconds()} seconds"
            )

            st = datetime.now()
            num_records, points_file_path, _ = process_and_write(
                cleaned_list, dt_str_file, range=range_param, mode="daily", args=args, variables_list=variables_list
            )
            logger.debug(
                f"Processed and wrote {num_records} records for {dt} in {(datetime.now() - st).total_seconds()} seconds"
            )
            success = True
        except Exception:
            logger.exception("process_and_write failed")
    except Exception:
        logger.exception(f"fetch_api_data failed for {dt_str}")

    if log_run_to_localdb:
        run_time = datetime.now().isoformat()
        log_run(
            run_time,
            dt_str,
            str(range_param),
            args.mode,
            vars(args) if args else {},
            os.getpid(),
            success,
            num_records,
            log_path,
            points_file_path,
        )
