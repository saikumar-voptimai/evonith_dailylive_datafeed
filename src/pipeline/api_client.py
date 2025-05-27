"""
API client for fetching data from the Blast Furnace API with retry logic.
"""
import os
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv
from pipeline.data_cleaner import clean_and_parse_data
from pipeline.influx_writer import process_and_write
from pipeline.run_tracker import log_run
from time import sleep
from xml.etree import ElementTree
import yaml

logger = logging.getLogger('pipeline')

with open('config/config.yaml', 'r', encoding='utf-8') as f:
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
    Fetch data from the API for a given date and range. Retries on failure.
    Args:
        date_str (str): Date in MM-DD-YYYY format
        range_param (int): Range parameter for API
        max_retries (int): Number of retries
        delay (int): Delay between retries (seconds)
    Returns:
        str: Raw XML response as string
    """
    logger.info(f"fetch_api_data(date={date_str}, range_param={range_param})")
    month, day, year = [int(x) for x in date_str.split('-')]
    params = {
        'user': USER_DAILY,
        'password': PASSWORD_DAILY,
        'month': month,
        'day': day,
        'year': year,
        'range': range_param
    }
    logger.debug(f"Request params: {params}")
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(API_URL_DAILY, params=params, timeout=60)
            logger.info(f"API response status code: {response.status_code}")
            response.raise_for_status()
            root = ElementTree.fromstring(response.text)
            assert root.text is not None, "API response is empty"
            logger.debug(f"API response text: {root.text[:100]}...")  # Log first 100 chars for brevity
            return root.text
        except Exception as e:
            logger.error(f"API fetch failed (attempt {attempt}): {e}")
            if attempt < max_retries:
                sleep(delay)
            else:
                raise

def fetch_api_data_live(max_retries=3, delay=5):
    """
    Fetch data from the LIVE API for CURRENT data. Retries on failure.
    Args:
        max_retries (int): Number of retries
        delay (int): Delay between retries (seconds)
    Returns:
        str: Raw XML response as string
    """
    logger.info("fetch_api_data_live() called")
    params = {
        'user': USER_LIVE,
        'password': PASSWORD_LIVE
    }
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


def process_datewise(dt: datetime.date, range_param: int, log_run_to_localdb: bool, args=None, log_path: str = None):
    """
    Process data for a specific date and range.
    Args:
        dt (datetime.date): Date to process
        range_param (int): Range parameter for API
        log_run_to_localdb (bool): Flag to log run to local DB
        args: CLI args
    """
    num_records, success = 0, False
    dt_str = dt.strftime(CONFIG['DATE_FORMAT'])
    dt_str_file = dt.strftime(CONFIG['DATE_FORMAT_FILENAME'])
    points_file_path = None
    try:
        st = datetime.now()
        raw = fetch_api_data(dt_str, range_param)
        logger.info(f"Fetched raw data for {dt_str} in {(datetime.now() - st).total_seconds()} seconds")
        logger.debug(f"Fetched historical raw data for {dt}")
        try:
            st = datetime.now()
            cleaned_list = clean_and_parse_data(raw)
            logger.info(f"Cleaned raw data for {dt_str} in {(datetime.now() - st).total_seconds()} seconds")

            st = datetime.now()
            num_records, points_file_path, _ = process_and_write(cleaned_list, dt_str_file, range=range_param, mode='daily', args=args)
            logger.debug(f"Processed and wrote {num_records} records for {dt} in {(datetime.now() - st).total_seconds()} seconds")
            success = True
        except Exception:
            logger.exception("process_and_write failed")
    except Exception:
        logger.exception(f"fetch_api_data failed for {dt_str}")

    if log_run_to_localdb:
        run_time = datetime.now().isoformat()
        log_run(run_time, dt_str, str(range_param), args.mode, vars(args) if args else {}, os.getpid(), success, num_records, log_path, points_file_path)
