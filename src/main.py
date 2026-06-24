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
import time

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


def setup_logging(default_path: str = "config/logging.yaml"):
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

_TRUE = {"true", "1", "yes", "y", "t", "on"}
_FALSE = {"false", "0", "no", "n", "f", "off"}


def str2bool(v: str) -> bool:
    """
    Convert a string to a boolean.
    Args:
        v (str): Input string.
    Returns:
        bool: Converted boolean value.
    Raises:
        argparse.ArgumentTypeError: If the input string is not a valid boolean representation.
    """
    if isinstance(v, bool):
        return v
    v_lower = str(v).strip().lower()
    if v_lower in _TRUE:
        return True
    elif v_lower in _FALSE:
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def add_bool_flag(parser: argparse.ArgumentParser, name: str, default: bool, help: str):
    """
    Add a boolean flag to the argument parser with both --name and --no-name options.
    Args:
        parser (argparse.ArgumentParser): The argument parser to which the flag is added.
        name (str): The name of the flag.
        default (bool): The default value of the flag.
        help (str): The help description for the flag.
    """
    dest = name.replace("-", "_")
    parser.add_argument(
        f"--{name}",
        dest=dest,
        nargs="?",
        const=True,
        default=default,
        type=str2bool,
        help=help,
    )
    parser.add_argument(
        f"--no-{name}",
        dest=dest,
        action="store_false",
        help=f"Disable {help.lower()}",
    )


def main():
    """
    Main function to parse command-line arguments and run the pipeline.
    It initializes the database, sets up logging, and processes data in either
    live or daily/range mode based on the provided arguments.
    """
    parser = argparse.ArgumentParser(description="Blast Furnace Data Pipeline")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["live", "daily", "range"],
        required=True,
        help="Mode: live (fetch latest) or daily (fetch by date) or use dates for range mode.",
    )
    parser.add_argument("--date", type=str, help="Date in MM-DD-YYYY format")
    parser.add_argument("--startdate", type=str, help="Start date in MM-DD-YYYY format")
    parser.add_argument("--enddate", type=str, help="End date in MM-DD-YYYY format")
    parser.add_argument(
        "--range",
        type=str,
        help="Range 1 or 2 for daily mode - 1 is 0 to 12 hours, 2 is 12 to 24 hours data",
    )

    add_bool_flag(
        parser,
        "db-write",
        default=False,
        help="Write to InfluxDB (True/False, default: False)",
    )
    add_bool_flag(
        parser,
        "override",
        default=True,
        help="Override existing data (True/False, default: True)",
    )
    add_bool_flag(
        parser,
        "retain-file",
        default=False,
        help="Write to txt or csv File (True/False, default: False)",
    )
    add_bool_flag(
        parser, "debug", default=False, help="Debug Mode (True/False, default: False)"
    )
    add_bool_flag(
        parser,
        "use-db-params",
        default=False,
        help="If True, uses CLI/config DB params; else uses token",
    )
    add_bool_flag(
        parser,
        "log-run",
        default=False,
        help="To log the run-details on sqlitedb (True/False, default: False)",
    )
    parser.add_argument(
        "--variable-file",
        dest="variable_file",
        type=str,
        default=None,
        help="Processes only those variables in the variable-file passed as .txt file. Only for range mode backfill",
    )

    parser.add_argument(
        "--delay", type=int, default=120, help="Delay between API calls in seconds"
    )

    parser.add_argument("--db-host", type=str, help="InfluxDB host")
    parser.add_argument("--db-org", type=str, help="InfluxDB org")

    args = parser.parse_args()

    load_dotenv()
    log_config_file = (
        "config/logging_debug.yaml" if args.debug else "config/logging.yaml"
    )
    setup_logging(log_config_file)
    logger = logging.getLogger("pipeline")

    init_db()
    logger.info("Parsed CLI args: %s", args)

    if args.startdate and args.enddate and (args.startdate == args.enddate):
        logger.info(
            "Start date %s is the same as end date %s. Triggering daily mode.",
            args.startdate,
            args.enddate,
        )
        args.date = args.startdate
        args.startdate = None
        args.enddate = None

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    logger.debug("Current UTC time: %s", now_utc)

    if args.mode == "live":
        pid = os.getpid()
        date_str_file = now_utc.strftime(CONFIG["DATE_FORMAT_FILENAME"])
        time_str_file = now_utc.strftime(CONFIG["TIME_FORMAT_FILENAME"])
        log_path = None
        if args.log_run:
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
        )
        et = datetime.datetime.now()
        run_time = datetime.datetime.now(datetime.timezone.utc).isoformat()

        if args.log_run:
            logger.info
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
        wait_s = float(CONFIG.get("WAIT", args.delay))
        elapsed_s = (et - st).total_seconds()
        remaining_s = wait_s - elapsed_s

        if remaining_s > 0:
            logger.info("Sleeping %.2fs to maintain %ss cadence.", remaining_s, wait_s)
            time.sleep(remaining_s)
        else:
            logger.warning(
                "API call & processing exceeded target cadence (%ss) by %.2fs",
                wait_s,
                -remaining_s,
            )
    elif args.startdate and args.enddate:
        start_date = datetime.datetime.strptime(args.startdate, "%m-%d-%Y")
        end_date = datetime.datetime.strptime(args.enddate, "%m-%d-%Y")

        logger.debug(
            "Range mode - Processing date range: %s to %s", start_date, end_date
        )
        if start_date > end_date:
            logger.error("Start date %s is after end date %s", start_date, end_date)
            sys.exit(1)
        dates_list = [
            start_date + datetime.timedelta(days=i)
            for i in range((end_date - start_date).days + 1)
        ]

        # Read variables_list from file if provided
        variables_list = None
        variable_file = getattr(args, "variable_file", None)
        if variable_file:
            with open(variable_file, "r", encoding="utf-8") as vf:
                variables_list = [line.strip() for line in vf if line.strip()]
        for dt in dates_list:
            logger.debug("Processing date %s", dt)
            if dt.date() == now_utc.date():
                logger.info(
                    "Skip logging the run details for %s as yet data needs to be \
                        loaded for today's date.",
                    dt,
                )
            for i in range(1, 3):
                log_path = None
                if args.log_run:
                    log_path = setup_run_logging_yaml(
                        dt.strftime(CONFIG["DATE_FORMAT_FILENAME"]),
                        range_param=str(i),
                        mode=args.mode,
                        pid=os.getpid(),
                    )
                process_datewise(
                    dt,
                    i,
                    log_run_to_localdb=args.log_run,
                    args=args,
                    log_path=log_path,
                    variables_list=variables_list,
                )
    elif args.mode == "daily":  # Single date mode
        if not args.date:
            args.date = (now_utc - datetime.timedelta(days=1)).strftime("%m-%d-%Y")
        logger.debug("Daily mode - Processing date: %s", args.date)
        log_run_to_localdb = args.log_run
        if args.date:
            dt = datetime.datetime.strptime(args.date, "%m-%d-%Y")
            if dt.date() == now_utc.date():
                log_run_to_localdb = False
                logger.info(
                    "Skip logging the run details for %s as yet data needs to be \
                        loaded for today's date.",
                    dt,
                )
            for i in range(1, 3):
                log_path = None
                if args.log_run:
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
            sys.exit(2)


if __name__ == "__main__":
    main()
