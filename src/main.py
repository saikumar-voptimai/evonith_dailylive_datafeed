"""
Main entry point for the Blast Furnace Data Pipeline.

This module parses command-line arguments, sets up logging, and orchestrates
the fetching, cleaning, processing, and writing of blast furnace data in both
live and daily/range modes. It supports writing to InfluxDB, file retention,
debugging, and selective variable processing via a variable file.
"""

import argparse
import datetime
import logging.config
import os
import sys

import yaml
from dotenv import load_dotenv

from src.pipeline.api_client import (
    fetch_api_data_live,
    process_and_write,
    process_datewise,
)
from src.pipeline.data_cleaner import clean_data
from src.pipeline.run_tracker import init_db, log_run
from src.pipeline.utils import setup_run_logging_yaml


def setup_logging(default_path: str="config/logging.yaml"):
    """
    Setup logging configuration from a YAML file.
    Args:
        default_path (str): Path to the logging configuration YAML file.    
    """
    with open(default_path, "r") as logfile:
        config = yaml.safe_load(logfile)
    logging.config.dictConfig(config)


with open("config/config.yaml", "r", encoding="utf-8") as conffile:
    CONFIG = yaml.safe_load(conffile) or {}


def main():
    """
    Main function to parse command-line arguments and run the pipeline.
    It initializes the database, sets up logging, and processes data in either
    live or daily/range mode based on the provided arguments.
    """
    logger = logging.getLogger("pipeline")
    logger.debug("Initializing argument parser")
    parser = argparse.ArgumentParser(description="Blast Furnace Data Pipeline")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["live", "daily"],
        help="Mode: live (fetch latest) or daily (fetch by date)",
    )
    parser.add_argument("--date", type=str, help="Date in MM-DD-YYYY format")
    parser.add_argument("--startdate", type=str, help="Start date in MM-DD-YYYY format")
    parser.add_argument("--enddate", type=str, help="End date in MM-DD-YYYY format")
    parser.add_argument(
        "--range",
        type=str,
        help="Range 1 or 2 for daily mode - 1 is 0 to 12 hours, 2 is 12 to 24 hours data",
    )
    parser.add_argument(
        "--db-write",
        type=lambda x: x.lower() in ("true", "1", "yes"),
        default=False,
        help="Write to InfluxDB (True/False, default: False)",
    )
    parser.add_argument(
        "--override",
        type=lambda x: x.lower() in ("true", "1", "yes"),
        default=True,
        help="Override existing data (True/False, default: True)",
    )
    parser.add_argument(
        "--retain-file",
        type=lambda x: x.lower() in ("true", "1", "yes"),
        default=False,
        help="Write to txt or csv File (True/False, default: False)",
    )
    parser.add_argument(
        "--debug",
        type=lambda x: x.lower() in ("true", "1", "yes"),
        default=False,
        help="Debug Mode (True/False, default: False)",
    )
    parser.add_argument(
        "--delay", type=int, default=120, help="Delay between API calls in seconds"
    )
    parser.add_argument(
        "--use-db-params",
        type=lambda x: x.lower() in ("true", "1", "yes"),
        default=False,
        help="If True, uses CLI/config DB params; else uses token",
    )
    parser.add_argument("--db-host", type=str, help="InfluxDB host")
    parser.add_argument("--db-org", type=str, help="InfluxDB org")
    parser.add_argument(
        "--log-run",
        type=lambda x: x.lower() in ("true", "1", "yes"),
        default=False,
        help="To log the run-details on sqlitedb (True/False, default: False)",
    )
    parser.add_argument(
        "--variable-file",
        type=str,
        help="Processes only those variables in the variable-file passed as .txt file."
        "Only for range mode.",
    )

    args = parser.parse_args()

    init_db()
    logger.info("Parsed CLI args: %s", args)

    load_dotenv()
    log_config_file = (
        "config/logging_debug.yaml" if args.debug else "config/logging.yaml"
    )
    setup_logging(log_config_file)

    if (args.startdate == args.enddate) and args.startdate:
        logger.info(
            "Start date %s is the same as end date %s. Triggering daily mode.",
            args.startdate, args.enddate
        )
        args.date = args.startdate
        args.startdate = None
        args.enddate = None

    if args.mode == "live":
        pid = os.getpid()
        date_str_file = datetime.datetime.now(datetime.timezone.utc).strftime(
            CONFIG["DATE_FORMAT_FILENAME"]
        )
        time_str_file = datetime.datetime.now(datetime.timezone.utc).strftime(
            CONFIG["TIME_FORMAT_FILENAME"]
        )
        log_path = setup_run_logging_yaml(
            date_str_file, time_str_file, args.mode, "1", pid
        )
        logger.info("Running in live mode")
        st = datetime.datetime.now()
        logger.debug(
            "Live mode - Run for timestamp UTC at %s",
            datetime.datetime.now(datetime.timezone.utc),
        )
        try:
            raw = fetch_api_data_live()
            logger.debug("Fetched live raw data size: %d", len(raw))
        except Exception as e:
            logger.exception("fetch_api_data_live() failed - %s", e)
            sys.exit(1)
        cleaned_list = clean_data(raw, date_str=date_str_file, mode="live")
        num_records, points_file_path, time_str_file = process_and_write(
            cleaned_list,
            date_str_file=date_str_file,
            time_str_file=time_str_file,
            mode="live",
            args=args,
            log_path=log_path,
        )
        et = datetime.datetime.now()
        run_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
        if args.log_run:
            log_run(
                run_time,
                date_str_file,
                time_str_file,
                str(args.range or 1),
                args.mode,
                vars(args),
                pid,
                True,
                num_records,
                log_path,
                points_file_path,
            )
        excess_time = (
            datetime.timedelta(seconds=float(CONFIG["WAIT"])) - (et - st)
        ).total_seconds()
        if excess_time < 0:
            logger.warning(
                'API call & processing took longer than %s, by %.2fs',
                CONFIG["WAIT"], excess_time
            )
        else:
            logger.warning("API call took longer than expected, skipping wait time.")
    elif args.startdate and args.enddate:
        start_date = datetime.datetime.strptime(args.startdate, "%m-%d-%Y")
        end_date = datetime.datetime.strptime(args.enddate, "%m-%d-%Y")
        log_run_to_localdb = args.log_run
        logger.debug("Range mode - Processing date range: %s to %s", start_date, end_date)
        if start_date > end_date:
            logger.error("Start date %s is after end date %s", start_date, end_date)
            sys.exit(1)
        dates_list = [
            start_date + datetime.timedelta(days=i)
            for i in range((end_date - start_date).days + 1)
        ]
        log_run_to_localdb = (
            args.log_run
        )  # True if args.log_run else False. Also False if date is today.
        # Read variables_list from file if provided
        variables_list = None
        if hasattr(args, "variable_file") and args.variable_file:
            with open(args.variable_file, "r") as vf:
                variables_list = [line.strip() for line in vf if line.strip()]
        for dt in dates_list:
            logger.debug("Processing date %s", dt)
            if dt == datetime.datetime.now(datetime.timezone.utc).date():
                log_run_to_localdb = False
                logger.info(
                    "Skip logging the run details for %s as yet data needs to be \
                        loaded for today's date.",
                    dt
                )
            for i in range(1, 3):
                log_path = setup_run_logging_yaml(
                    dt.strftime(CONFIG["DATE_FORMAT_FILENAME"]),
                    range_param=str(i),
                    mode=args.mode,
                    pid=os.getpid(),
                )
                process_datewise(
                    dt,
                    i,
                    log_run_to_localdb=log_run_to_localdb,
                    args=args,
                    log_path=log_path,
                    variables_list=variables_list,
                )
    else:
        if not args.date:
            args.date = (
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(days=1)
            ).strftime("%m-%d-%Y")
        logger.debug("Daily mode - Processing date: %s", args.date)
        log_run_to_localdb = args.log_run
        if args.date:
            dt = datetime.datetime.strptime(args.date, "%m-%d-%Y")
            if dt == datetime.datetime.now(datetime.timezone.utc).date():
                log_run_to_localdb = False
                logger.info(
                    "Skip logging the run details for %s as yet data needs to be \
                        loaded for today's date.",
                    dt
                )
            for i in range(1, 3):
                log_path = setup_run_logging_yaml(
                    dt.strftime(CONFIG["DATE_FORMAT_FILENAME"]),
                    range_param=str(i),
                    mode=args.mode,
                    pid=os.getpid(),
                )
                process_datewise(
                    dt,
                    i,
                    log_run_to_localdb=log_run_to_localdb,
                    args=args,
                    log_path=log_path,
                )
        else:
            logger.error("Date is required in daily mode")
            sys.exit(1)


if __name__ == "__main__":

    print("in main")
    main()
