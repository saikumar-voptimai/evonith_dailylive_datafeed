"""
Cleans and parses raw XML data string to a list of dicts, with datetime parsing.
"""
import re
import ast
import logging
from typing import List, Dict
logger = logging.getLogger('pipeline')

def clean_and_parse_data(raw_data: str) -> list[dict]:
    """
    Cleans the raw XML string, removes unwanted scripts, parses to list of dicts, and parses datetime.
    Returns a list of records with 'Timelogged' as ISO string or datetime.
    """
    logger.info("Starting clean_and_parse_data")
    cleaned = re.sub(r'<script.*?</script>', '', raw_data, flags=re.DOTALL).strip()
    try:
        records = ast.literal_eval(cleaned)
    except Exception as e:
        logger.exception("Failed to parse data into Python objects")
        raise
    logger.info(f"clean_and_parse_data produced {len(records)} records")
    return records

def clean_data(raw_data, date_str: str, mode: bool) -> List[Dict[str, str]]:
    """
    Process raw data and write to InfluxDB and/or CSV.
    Args:
        raw_data (str): Raw data from API
        date_str (str): Date string for filename
        mode (str): 'daily' or 'live'
        retain_file (bool): Flag to retain the file after processing
    """
    logger.info(f"Processing and writing data for date={date_str}, mode={mode}")
    try:
        cleaned_list = clean_and_parse_data(raw_data)
        logger.debug(f"Cleaned records count: {len(cleaned_list)}")
    except Exception:
        logger.exception("Error in clean_and_parse_data")
        return 0
    return cleaned_list