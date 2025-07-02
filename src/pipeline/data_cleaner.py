"""
Cleans and parses raw XML data string to a list of dicts, with datetime parsing.
"""

import ast
import logging
import re
from typing import Dict, List

logger = logging.getLogger("pipeline")


def clean_and_parse_data(raw_data: str) -> list[dict]:
    """
    Cleans a raw XML string by removing <script> tags, parses it into a list of dicts, and returns the result.
    Args:
        raw_data (str): Raw XML string from the API, possibly with embedded scripts.
    Returns:
        list[dict]: List of records parsed from the cleaned string.
    Raises:
        Exception: If parsing fails or the data is malformed.
    """
    logger.info("Starting clean_and_parse_data")
    cleaned = re.sub(r"<script.*?</script>", "", raw_data, flags=re.DOTALL).strip()
    try:
        records = ast.literal_eval(cleaned)
    except Exception as e:
        logger.exception("Failed to parse data into Python objects")
        raise Exception(
            f"Failed to parse data into Python objects: {e}"
        ) from e
    logger.info("clean_and_parse_data produced %d records", len(records))
    return records


def clean_data(raw_data, date_str: str, mode: bool) -> List[Dict[str, str]]:
    """
    Cleans and parses raw API data, returning a list of records or 0 on error.
    Args:
        raw_data (str): Raw data from API.
        date_str (str): Date string for filename.
        mode (str): 'daily' or 'live'.
    Returns:
        list[dict] or int: List of cleaned records, or 0 if parsing fails.
    """
    logger.info("Processing and writing data for date=%s, mode=%s", date_str, mode)
    try:
        cleaned_list = clean_and_parse_data(raw_data)
        logger.debug("Cleaned records count: %d", len(cleaned_list))
    except Exception:
        logger.exception("Error in clean_and_parse_data")
        return 0
    return cleaned_list
