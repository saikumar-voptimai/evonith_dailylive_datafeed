"""
Utility functions for date parsing, range, and data checking.
"""
from datetime import datetime, timedelta
import logging

logger = logging.getLogger('pipeline.utils')

def daterange(start_date, end_date):
    """
    Yields date strings from start_date to end_date (inclusive) in MM-DD-YYYY format.
    """
    start = datetime.strptime(start_date, '%m-%d-%Y')
    end = datetime.strptime(end_date, '%m-%d-%Y')
    delta = timedelta(days=1)
    while start <= end:
        yield start.strftime('%m-%d-%Y')
        start += delta

def check_existing_data(cleaned_data, args):
    """
    Stub for checking if data already exists in InfluxDB and if value difference > 0.000001.
    Returns False to always write (implement as needed).
    """
    logger.info("Checking for existing data (stub, always returns False)")
    return False
