"""
API client for fetching data from the Blast Furnace API with retry logic.
"""
import os
import requests
import logging
from dotenv import load_dotenv
from time import sleep
from xml.etree import ElementTree

logger = logging.getLogger('pipeline.api_client')

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
    month, day, year = [int(x) for x in date_str.split('-')]
    params = {
        'user': USER_DAILY,
        'password': PASSWORD_DAILY,
        'month': month,
        'day': day,
        'year': year,
        'range': range_param
    }
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(API_URL_DAILY, params=params, timeout=60)
            logger.info(f"API response status code: {response.status_code}")
            response.raise_for_status()
            root = ElementTree.fromstring(response.text)
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
    params = {
        'user': USER_LIVE,
        'password': PASSWORD_LIVE
    }
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
