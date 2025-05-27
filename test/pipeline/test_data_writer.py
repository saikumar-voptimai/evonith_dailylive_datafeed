import logging
import yaml
import pytz
from src.pipeline.influx_writer import write_to_influxdb

logger = logging.getLogger('pipeline.influx_writer')
with open('config/config.yaml', 'r', encoding='utf-8') as f:
    DB_CONFIG = yaml.safe_load(f) or {}
    LOCAL_TZ = pytz.timezone(DB_CONFIG.get('timezone', 'UTC'))


if __name__ == "__main__":
    # This is just a placeholder to prevent import errors in tests
    # Actual execution logic should be in the main script or CLI entry point
    write_to_influxdb('output/tmp_45340.txt', None, batch_size=1000)
