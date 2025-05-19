"""
Cleans and parses raw XML data string to a list of dicts, with datetime parsing.
"""
import re
import ast
from datetime import datetime
import logging
import yaml
import pytz

logger = logging.getLogger('pipeline.data_cleaner')

with open('config/config.yaml', 'r', encoding='utf-8') as f:
    CONFIG = yaml.safe_load(f) or {}

LOCAL_TZ = pytz.timezone(CONFIG.get('timezone', 'UTC'))

def clean_and_parse_data(raw_data: str) -> list[dict]:
    """
    Cleans the raw XML string, removes unwanted scripts, parses to list of dicts, and parses datetime.
    Returns a list of records with 'Timelogged' as ISO string or datetime.
    """
    cleaned = re.sub(r'<script.*?</script>', '', raw_data, flags=re.DOTALL).strip()
    try:
        records = ast.literal_eval(cleaned)
    except Exception as e:
        logger.error(f"Failed to parse data: {e}")
        raise
    for rec in records:
        if 'Timelogged' in rec:
            try:
                dt_naive = datetime.strptime(rec['Timelogged'], '%m/%d/%Y %I:%M:%S %p')
                dt_local = LOCAL_TZ.localize(dt_naive)
                dt_utc = dt_local.astimezone(pytz.utc)
                rec['Timelogged'] = dt_utc
            except Exception as e:
                logger.warning(f"Failed to parse Timelogged: {rec.get('Timelogged')} - {e}")
    return records