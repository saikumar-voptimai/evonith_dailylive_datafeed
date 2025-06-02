"""
InfluxDB writer for cleaned data, with overwrite logic.
"""

import gzip
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Tuple

import pytz
import yaml
from influxdb_client_3 import (SYNCHRONOUS, InfluxDBClient3, InfluxDBError,
                               WriteOptions, write_client_options)

from src.pipeline.bf2_rename_map import build_points

logger = logging.getLogger("pipeline")
with open("config/config.yaml", "r", encoding="utf-8") as f:
    DB_CONFIG = yaml.safe_load(f) or {}
    LOCAL_TZ = pytz.timezone(DB_CONFIG.get("timezone", "UTC"))


def write_points_to_txt(
    line_input_per_rec,
    date_str_file: str = None,
    time_str: str = None,
    range: str = "1",
    mode="live",
    out_dir="output",
    filename="tmp",
):
    """
    Writes InfluxDB line protocol data to a .txt file for testing or auditing.
    Args:
        line_input_per_rec (str): InfluxDB line protocol string(s) to write.
        date_str_file (str, optional): Date string for filename context.
        time_str (str, optional): Time string for filename context (used in live mode).
        range (str, optional): Range parameter for Daily API. Default is '1'.
        mode (str, optional): 'daily' or 'live'.
        out_dir (str, optional): Output directory for files.
        filename (str, optional): Name of the file to write to.
    Returns:
        None. Writes data to file.
    """
    os.makedirs(out_dir, exist_ok=True)

    logger.debug(
        f"Starting write_points_to_txt: mode={mode}, date_str={date_str_file}, time_str={time_str} range={range}, filename={filename}"
    )
    if os.path.exists(filename) and mode != "live":
        logger.debug(f"File {filename} exists, appending data.")
        with open(filename, "a", encoding="utf-8") as f:
            f.write(line_input_per_rec)
    else:
        logger.info(f"Creating new file {filename}")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(line_input_per_rec)


def write_to_influxdb(filename, args, batch_size=1000):
    """
    Writes line protocol data from a file to InfluxDB in batches.
    Args:
        filename (str): Path to the line protocol file.
        args: CLI args with DB connection info and flags.
        batch_size (int, optional): Number of lines per batch. Default is 1000.
    Returns:
        None. Writes data to InfluxDB.
    Raises:
        Exception: If connection or write fails.
    """
    logger.info(
        f"write_to_influxdb called with filename={filename}, args={args}, batch_size={batch_size}"
    )

    try:
        host = DB_CONFIG["INFLUXDB"]["HOST"]
        org = DB_CONFIG["INFLUXDB"]["ORG"]
        logger.info(f"Connecting to InfluxDB at {host}, org={org}")
        write_options = WriteOptions(
            batch_size=1000,
            flush_interval=10_000,
            jitter_interval=2_000,
            retry_interval=5_000,
            max_retries=5,
            max_retry_delay=30_000,
            exponential_base=2,
        )

        def success(self, data: str):
            status = "Success writing batch: data: {data}"
            assert status.startswith("Success"), f"Expected {status} to be success"

        def error(self, data: str, exception: InfluxDBError):
            print(f"Failed writing batch: config: {self}, due: {exception}")

        def retry(self, data: str, exception: InfluxDBError):
            print(f"Failed retry writing batch: config: {self}, retry: {exception}")

        wco = write_client_options(
            success_callback=success,
            error_callback=error,
            retry_callback=retry,
            write_options=write_options,
        )

        try:
            bucket = DB_CONFIG["INFLUXDB"]["BUCKET"]
            logger.info(f"Writing to bucket: {bucket}")
            batch, wrote = [], 0
            with open(filename, "r") as file:
                for line in file:
                    if line.strip():
                        batch.append(line.rstrip("\n"))
                    if len(batch) >= batch_size:
                        points = "\n".join(batch)
                        wrote += len(batch)
                        client = InfluxDBClient3(
                            host=host,
                            token=os.getenv("INFLUXDB_TOKEN_EVONITH_BF2_CREATE"),
                            org=org,
                            write_client_options=wco,
                        )
                        logger.info("Successfully connected to InfluxDB")
                        client.write(
                            database=bucket, record=points, write_precision="s"
                        )
                        logger.info(
                            f"Wrote batch of {len(batch)} lines to InfluxDB. Total written: {wrote}."
                        )
                        batch = []
                        wait_time = DB_CONFIG.get("WRITE_DELAY", 5)
                        # time.sleep(wait_time)
                        client.close()
                        time.sleep(wait_time)
                # Write any remaining lines
                if batch:
                    points = "\n".join(batch)
                    wrote += len(batch)
                    client = InfluxDBClient3(
                        host=host,
                        token=os.getenv("INFLUXDB_TOKEN_EVONITH_BF2_CREATE"),
                        org=org,
                        write_client_options=wco,
                    )
                    logger.info("Successfully connected to InfluxDB")
                    client.write(database=bucket, record=points, write_precision="s")
                    wait_time = (
                        0.5 if len(batch) < 100 else DB_CONFIG.get("WRITE_DELAY", 0.5)
                    )
                    # time.sleep(wait_time)
                    client.close()
                    time.sleep(wait_time)
                    logger.info(f"Wrote final batch of {len(batch)} lines to InfluxDB")
            logger.info(f"Finished writing all lines from {filename} to InfluxDB")
        except Exception:
            logger.exception("Failed to write points to InfluxDB")
            raise
    except Exception as e:
        logger.error(f"Error connecting to InfluxDB: {e}")
        raise


def process_and_write(
    cleaned_list: List[Dict[str, str]],
    date_str_file: str,
    time_str_file: str = None,
    range: int = 1,
    mode: str = "live",
    args: Dict = None,
    ouput_dir="output",
    log_path: str = None,
) -> Tuple[int, str, str]:
    """
    Processes cleaned data, writes to InfluxDB or file, and optionally gzips output.
    Args:
        cleaned_list (list): List of cleaned records.
        date_str_file (str): Date string for filename.
        time_str (str, optional): Time string for filename (used in live mode).
        range (int, optional): Range parameter for Daily API. Default is 1.
        mode (str, optional): 'daily' or 'live'.
        args (dict, optional): CLI args with DB connection info and flags.
        ouput_dir (str, optional): Output directory for files.
        log_path (str, optional): Path to the log file for this run.
    Returns:
        tuple: (number of records processed, final points file path or None, time string or None)
    """
    record_count = len(cleaned_list)
    write_filename = os.path.join(ouput_dir, f"tmp_{os.getpid()}.txt")
    for record in cleaned_list:
        if "Timelogged" in record:
            try:
                dt_naive = datetime.strptime(
                    record["Timelogged"], "%m/%d/%Y %I:%M:%S %p"
                )
                dt_local = LOCAL_TZ.localize(dt_naive)
                dt_utc = dt_local.astimezone(pytz.utc)
                record["Timelogged"] = dt_utc
            except Exception as e:
                logger.warning(
                    f"Failed to parse Timelogged: {record.get('Timelogged')} - {e}"
                )
        ts = record.get("Timelogged")
        logger.debug(f"Building points for timestamp={ts}")
        line_input = build_points(record, ts)
        write_points_to_txt(
            line_input, date_str_file, range, mode=mode, filename=write_filename
        )

    if args and args.db_write:
        if args.override:
            st = datetime.now()
            write_to_influxdb(write_filename, args, batch_size=5000)
            logger.info(
                f"Write to influxdb took: {(datetime.now() - st).total_seconds()} seconds"
            )
        # TODO: Implement logic to check if point already exists in DB
        # and if value difference > 0.000001
    points_file_final = None
    if args and args.retain_file:
        points_file_final = (
            os.path.join("output", f"date_{date_str_file}_Range{range}.txt")
            if mode == "daily"
            else os.path.join("output", f"live_{date_str_file}_{time_str}.txt")
        )
        logger.info(f"Renaming file {write_filename} to {points_file_final}")
        os.rename(write_filename, points_file_final)
        gzipped_path = points_file_final + ".gz"
        with (
            open(points_file_final, "rb") as f_in,
            gzip.open(gzipped_path, "wb") as f_out,
        ):
            f_out.writelines(f_in)
        os.remove(points_file_final)
        logger.info(f"Gzipped points file to {gzipped_path}")
        points_file_final = gzipped_path
    time_str = None
    if mode == "live":
        time_str = dt_utc.strftime(DB_CONFIG["TIME_FORMAT_FILENAME"])
    return record_count, points_file_final, time_str


def should_write_point(point, args):
    """
    Determines whether a point should be written to InfluxDB (stub; always returns True).
    Args:
        point (dict): The data point to check.
        args: CLI args with override flag.
    Returns:
        bool: True if the point should be written (default), False otherwise.
    """
    logger.info(
        f"should_write_point called for point={point}, override={args.override}"
    )
    # TODO: Implement logic to check if point already exists in DB
    return True
