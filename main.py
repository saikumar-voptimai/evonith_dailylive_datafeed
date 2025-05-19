import os
import sys
import argparse
import logging.config
import yaml
from dotenv import load_dotenv
from pipeline.api_client import fetch_api_data
from pipeline.data_cleaner import clean_and_parse_data
from pipeline.influx_writer import write_to_influxdb
from pipeline.write_to_file import write_to_csv
from pipeline.utils import daterange, check_existing_data


def setup_logging(default_path='config/logging.yaml'):
    with open(default_path, 'r') as f:
        config = yaml.safe_load(f)
    logging.config.dictConfig(config)


def main():
    """
    Main function to run the data pipeline.
    Parses command line arguments, sets up logging, and processes data."""
    parser = argparse.ArgumentParser(description="Blast Furnace Data Pipeline")
    parser.add_argument('--date', type=str, help='Date in DD-MM-YYYY format')
    parser.add_argument('--startdate', type=str, help='Start date in DD-MM-YYYY format')
    parser.add_argument('--enddate', type=str, help='End date in DD-MM-YYYY format')
    parser.add_argument('--range', type=int, default=2, help='Range parameter for API')
    parser.add_argument('--use-db-params', action='store_true', default=False, help='If set, uses the passed DB config. Else uses influx token from .env')
    parser.add_argument('--db-host', type=str, help='InfluxDB host')
    parser.add_argument('--db-url', type=str, help='InfluxDB URL')
    parser.add_argument('--db-user', type=str, help='InfluxDB username')
    parser.add_argument('--db-write', action='store_true', default=False, help='Write to InfluxDB (default: False)')
    parser.add_argument('--override', action='store_true', default=True, help='Override existing data (default: True)')
    parser.add_argument('--delay', type=int, default=120, help='Delay between API calls (seconds)')
    parser.add_argument('--mode', type=str, choices=['live', 'daily'], required=True, help='Mode: live (fetch from live API) or daily (fetch from daily data API)')
    args = parser.parse_args()

    load_dotenv()
    setup_logging()
    logger = logging.getLogger('pipeline')

    db_config_path = 'config/config.yaml'
    db_config = {}
    if os.path.exists(db_config_path):
        with open(db_config_path, 'r') as f:
            db_config = yaml.safe_load(f) or {}

    # Set DB params based on --use-db flag
    if args.use_db_params:
        args.db_host = args.db_host or db_config.get('db_host')
        args.db_url = args.db_url or db_config.get('db_url')
        args.db_user = args.db_user or db_config.get('db_user')
        args.db_password = args.db_password or db_config.get('db_password')
        args.db_token = None  # Explicitly not using token
    else:
        args.db_host = None
        args.db_url = None
        args.db_user = None
        args.db_password = None
        args.db_token = os.getenv('INFLUXDB_TOKEN')

    # Load table config
    with open('config/tables.yaml', 'r') as f:
        table_config = yaml.safe_load(f)

    # Mode logic
    if args.mode == 'live':
        logger.info('Running in LIVE mode.')
        try:
            raw_data = fetch_api_data_live()  # You need to implement fetch_api_data_live in api_client.py
            cleaned_data = clean_and_parse_data(raw_data)
            if args.db_write:
                if args.override:
                    if not check_existing_data(cleaned_data, args):
                        write_to_influxdb(cleaned_data, table_config, args)
                else:
                    write_to_influxdb(cleaned_data, table_config, args)
            else:
                write_to_csv(cleaned_data, table_config, 'live')
        except Exception as e:
            logger.error(f"Error processing live data: {e}")
    elif args.mode == 'daily':
        # Date logic
        if args.startdate and args.enddate:
            dates = list(daterange(args.startdate, args.enddate))
        elif args.date:
            dates = [args.date]
        else:
            logger.error('No date or date range provided for daily mode.')
            sys.exit(1)

        for date_str in dates:
            logger.info(f"Processing date: {date_str}")
            try:
                raw_data = fetch_api_data(date_str, args.range)
                cleaned_data = clean_and_parse_data(raw_data)
                if args.db_write:
                    if args.override:
                        if not check_existing_data(cleaned_data, args):
                            write_to_influxdb(cleaned_data, table_config, args)
                    else:
                        write_to_influxdb(cleaned_data, table_config, args)
                else:
                    write_to_csv(cleaned_data, table_config, date_str)
            except Exception as e:
                logger.error(f"Error processing {date_str}: {e}")
            logger.info(f"Waiting {args.delay} seconds before next call...")
            if date_str != dates[-1]:
                import time
                time.sleep(args.delay)

if __name__ == "__main__":
    main()
