import datetime
import sys
import argparse
import logging.config
import yaml
from dotenv import load_dotenv
from pipeline.api_client import fetch_api_data, fetch_api_data_live
from pipeline.data_cleaner import clean_and_parse_data
from pipeline.influx_writer import write_to_influxdb
from pipeline.write_to_file import write_points_to_txt
from pipeline.bf2_rename_map import build_points
from pipeline.utils import daterange

with open('config/config.yaml', 'r', encoding='utf-8') as f:
    CONFIG = yaml.safe_load(f) or {}

def setup_logging(default_path='config/logging.yaml'):
    with open(default_path, 'r') as f:
        config = yaml.safe_load(f)
    logging.config.dictConfig(config)


def main():
    parser = argparse.ArgumentParser(description="Blast Furnace Data Pipeline")
    parser.add_argument('--mode', type=str, choices=['live', 'daily'],
                        help='Mode: live (fetch latest) or daily (fetch by date)')
    parser.add_argument('--date', type=str, help='Date in MM-DD-YYYY format')
    parser.add_argument('--startdate', type=str, help='Start date in MM-DD-YYYY format')
    parser.add_argument('--enddate', type=str, help='End date in MM-DD-YYYY format')
    parser.add_argument('--db-write', type=lambda x: x.lower() in ('true','1','yes'), default=False,
                        help='Write to InfluxDB (True/False, default: False)')
    parser.add_argument('--override', type=lambda x: x.lower() in ('true','1','yes'), default=True,
                        help='Override existing data (True/False, default: True)')
    parser.add_argument('--write-to-file', type=lambda x: x.lower() in ('true','1','yes'), default=False,
                        help='Write to txt or csv File (True/False, default: False)')
    parser.add_argument('--delay', type=int, default=120,
                        help='Delay between API calls in seconds')
    parser.add_argument('--use-db-params', type=lambda x: x.lower() in ('true','1','yes'), default=False,
                        help='If True, uses CLI/config DB params; else uses token')
    parser.add_argument('--db-host', type=str, help='InfluxDB host')
    parser.add_argument('--db-org', type=str, help='InfluxDB org')
    args = parser.parse_args()

    load_dotenv()
    setup_logging()
    logger = logging.getLogger('pipeline')

    def process_and_write(raw_data, date_str, mode, write_to_file=True):
        """
        Process raw data and write to InfluxDB and/or CSV.
        Args:
            raw_data (str): Raw data from API
            date_str (str): Date string for filename
            mode (str): 'daily' or 'live'
        """
        cleaned_list = clean_and_parse_data(raw_data)
        all_points = []
        for record in cleaned_list:
            ts = record.get('Timelogged')
            points = build_points(record, ts)
            all_points.extend(points)
            if args.db_write:
                if args.override:
                    write_to_influxdb(points, args)
                else:
                    from pipeline.influx_writer import should_write_point
                    filtered = [pt for pt in points if should_write_point(pt, args)]
                    if filtered:
                        write_to_influxdb(filtered, args)
        if write_to_file:
            write_points_to_txt(all_points, date_str, mode)

    if args.mode == 'live':
        logger.info('Running in live mode')
        while True:
            st = datetime.datetime.now()
            raw = fetch_api_data_live()
            process_and_write(raw, 'live', 'live', write_to_file=args.write_to_file)
            et = datetime.datetime.now()
            wait_time = (datetime.timedelta(seconds=float(CONFIG['WAIT'])) - (et - st)).total_seconds()  # 10 seconds buffer
            if wait_time > 0:
                logger.info(f'Waiting {wait_time}s')
                import time; time.sleep(wait_time)
            else:
                logger.warning('API call took longer than expected, skipping wait time.')       
    else:  # daily mode
        if args.startdate and args.enddate:
            dates = list(daterange(args.startdate, args.enddate))
        elif args.date:
            dates = [args.date]
        else:
            logger.error('Daily mode requires --date or --startdate/--enddate')
            sys.exit(1)
        for dt in dates:
            logger.info(f'Processing date {dt}')
            raw = fetch_api_data(dt, args.range)
            process_and_write(raw, dt, 'daily')
            logger.info(f'Waiting {args.delay}s')
            if dt != dates[-1]:
                import time; time.sleep(args.delay)

if __name__ == "__main__":
    print("in main")
    main()
