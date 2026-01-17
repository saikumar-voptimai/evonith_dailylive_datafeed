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
from influxdb_client_3 import (
    InfluxDBClient3,
    InfluxDBError,
    WriteOptions,
    write_client_options,
)

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
        "Starting write_points_to_txt: mode=%s, date_str=%s, time_str=%s range=%s, filename=%s",
        mode,
        date_str_file,
        time_str,
        range,
        filename,
    )
    if os.path.exists(filename):
        logger.debug("File %s exists, appending data.", filename)
        with open(filename, "a", encoding="utf-8") as f:
            f.write(line_input_per_rec)
    else:
        logger.info("Creating new file %s", filename)
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
        "write_to_influxdb called with filename=%s, args=%r, batch_size=%d",
        filename,
        args,
        batch_size,
    )

    try:
        host = DB_CONFIG["INFLUXDB"]["HOST"]
        org = DB_CONFIG["INFLUXDB"]["ORG"]
        logger.info("Connecting to InfluxDB at %s, org=%s", host, org)
        write_options = WriteOptions(
            batch_size=1000,
            flush_interval=10_000,
            jitter_interval=2_000,
            retry_interval=5_000,
            max_retries=5,
            max_retry_delay=30_000,
            exponential_base=2,
        )

        def success():
            status = "Success writing batch: data: {data}"
            assert status.startswith("Success"), f"Expected {status} to be success"

        def error(self, exception: InfluxDBError):
            print(f"Failed writing batch: config: {self}, due: {exception}")

        def retry(self, exception: InfluxDBError):
            print(f"Failed retry writing batch: config: {self}, retry: {exception}")

        wco = write_client_options(
            success_callback=success,
            error_callback=error,
            retry_callback=retry,
            write_options=write_options,
        )

        try:
            bucket = DB_CONFIG["INFLUXDB"]["BUCKET"]
            logger.info("Writing to bucket: %s", bucket)
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
                            "Wrote batch of %d lines to InfluxDB. Total written: %d.",
                            len(batch),
                            wrote,
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
                    logger.info("Wrote final batch of %d lines to InfluxDB", len(batch))
            logger.info("Finished writing all lines from %s to InfluxDB", filename)
        except Exception:
            logger.exception("Failed to write points to InfluxDB")
            raise
    except Exception as e:
        logger.error("Error connecting to InfluxDB: %s", e)
        raise


def process_and_write(
    cleaned_list: List[Dict[str, str]],
    date_str_file: str,
    time_str_file: str = None,
    range: int = 1,
    mode: str = "live",
    args: Dict = None,
    ouput_dir="output",
    variables_list=None,
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
        variables_list (list, optional): List of variables to filter and write.
    Returns:
        tuple: (number of records processed, final points file path or None, time string or None)
    """
    record_count = len(cleaned_list)
    write_filename = os.path.join(ouput_dir, f"tmp_{os.getpid()}.txt")
    dt_utc = None
    for record in cleaned_list:
        # Filter record if variables_list is provided
        if variables_list is not None:
            record = {
                k: v
                for k, v in record.items()
                if k in variables_list or k == "Timelogged"
            }
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
                    "Failed to parse Timelogged: %s - %s", record.get("Timelogged"), e
                )
        ts = record.get("Timelogged")
        logger.debug("Building points for timestamp=%s", ts)
        line_input = build_points(record, ts)
        write_points_to_txt(
            line_input_per_rec=line_input,
            date_str_file=date_str_file,
            time_str=time_str_file,
            range=str(range),
            mode=mode,
            out_dir=ouput_dir,
            filename=write_filename,
        )

    if args and args.db_write:
        if args.override:
            st = datetime.now()
            write_to_influxdb(write_filename, args, batch_size=5000)
            logger.info(
                "Write to influxdb took: %s seconds",
                (datetime.now() - st).total_seconds(),
            )
        # TODO: Implement logic to check if point already exists in DB
        # and if value difference > 0.000001
    points_file_final = None
    if args and args.retain_file:
        points_file_final = (
            os.path.join("output", f"date_{date_str_file}_Range{range}.txt")
            if mode == "daily"
            else os.path.join("output", f"live_{date_str_file}_{time_str_file}.txt")
        )
        logger.info("Renaming file %s to %s", write_filename, points_file_final)
        os.rename(write_filename, points_file_final)
        gzipped_path = points_file_final + ".gz"
        with (
            open(points_file_final, "rb") as f_in,
            gzip.open(gzipped_path, "wb") as f_out,
        ):
            f_out.writelines(f_in)
        os.remove(points_file_final)
        logger.info("Gzipped points file to %s", gzipped_path)
        points_file_final = gzipped_path
    else:
        try:
            if os.path.exists(write_filename):
                os.remove(write_filename)
                logger.info("Removed temporary file %s", write_filename)
        except Exception as e:
            logger.warning("Failed to remove temporary file %s: %s", write_filename, e)

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
        "should_write_point called for point=%r, override=%r", point, args.override
    )
    # TODO: Implement logic to check if point already exists in DB
    return True
