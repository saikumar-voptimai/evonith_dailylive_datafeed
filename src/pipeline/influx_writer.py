"""
InfluxDB writer for cleaned data, with overwrite logic.
"""
import logging
import yaml
from influxdb_client_3 import InfluxDBClient3
import os

logger = logging.getLogger('pipeline.influx_writer')
with open('config/config.yaml', 'r', encoding='utf-8') as f:
    DB_CONFIG = yaml.safe_load(f) or {}

def write_to_influxdb(points, args):
    """
    Writes a list of InfluxDB Point objects to InfluxDB.
    Args:
        points (list[Point]): Points to write
        args: CLI args with db connection info and flags
    """
    logger.info("Writing to InfluxDB using influxdb_client_3")
    if not args.db_write:
        logger.info("db_write is False; skipping InfluxDB write.")
        return
    try:
        client = InfluxDBClient3(
            host=DB_CONFIG['INFLUXDB']['HOST'],
            token=os.getenv("INFLUXDB_TOKEN_EVONITH_BF2_CREATE"),
            org=DB_CONFIG['INFLUXDB']['ORG'],
        )
    except Exception as e:
        logger.error(f"Error connecting to InfluxDB: {e}")
        raise
    try:
        client.write(database=DB_CONFIG['INFLUXDB']['BUCKET'], record=points)
        logger.info(f"Wrote {len(points)} points to InfluxDB")
    except Exception as e:
        logger.error(f"Error writing to InfluxDB: {e}")
        raise


def should_write_point(point, args):
    """
    Stub for deciding if a point should be written based on existing DB value and override flag.
    Returns True to write all points. Implement diff logic as needed.
    """
    #TODO: Implement logic to check if point already exists in DB
    return True
