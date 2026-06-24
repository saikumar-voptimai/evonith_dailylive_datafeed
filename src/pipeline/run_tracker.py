"""
Run tracking utilities for the Blast Furnace data pipeline.

This module provides functions to initialize and update a local SQLite database
or tracking pipeline runs, including run metadata and upsert logic.
"""

import json
import logging
import os
import sqlite3
from typing import Optional

logger = logging.getLogger("pipeline")


def init_db(db_path="db/run_metadata.db"):
    """
    Initializes the local SQLite database and creates the 'runs' table if it does not exist.
    Args:
        db_path (str, optional): Path to the SQLite DB file. Default is 'db/run_metadata.db'.
    Returns:
        None. Creates DB and table if needed.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    logger.info("Resolved absolute DB path: %s", os.path.abspath(db_path))
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_time TEXT,
            date_run TEXT,
            range TEXT,
            mode TEXT,
            parameters TEXT,
            process_id INTEGER,
            success INTEGER,
            num_records INTEGER,
            log_path TEXT,
            points_file_path TEXT,
            UNIQUE(date_run, range, mode) ON CONFLICT REPLACE
        )
    """
    )
    conn.commit()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    logger.info("Tables in DB: %r", cur.fetchall())
    conn.close()
    logger.info("Initialized database at %s with runs table.", db_path)


def log_run(
    run_time: str,
    date_run: str,
    range_param: str,
    mode: str,
    parameters: dict,
    process_id: int,
    success: int,
    num_records: int,
    log_path: Optional[str],
    points_file_path: str,
    db_path: str = "db/run_metadata.db",
) -> None:
    """
    Logs a pipeline run to the SQLite DB, performing an upsert based on date_run, range, and mode.
    Args:
        run_time (str): Timestamp of the run.
        date_run (str): Date string for the run.
        range_param (str): Range parameter.
        mode (str): 'daily' or 'live'.
        parameters (dict): Parameters used for the run.
        process_id (int): PID of the process.
        success (int): 1 if the run succeeded, 0 otherwise.
        num_records (int): Number of records processed.
        log_path (str): Path to the log file.
        points_file_path (str): Path to the points file.
        db_path (str, optional): Path to the SQLite DB.
    Returns:
        None. Inserts or updates the run record in the DB.
    """
    logger.info("Resolved absolute DB path: %s", os.path.abspath(db_path))
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO runs (run_time, date_run, range, mode, parameters, process_id, success, num_records, log_path, points_file_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date_run, range, mode) DO UPDATE SET
            run_time=excluded.run_time,
            parameters=excluded.parameters,
            process_id=excluded.process_id,
            success=excluded.success,
            num_records=excluded.num_records,
            log_path=excluded.log_path,
            points_file_path=excluded.points_file_path
        """,
        (
            run_time,
            date_run,
            range_param,
            mode,
            json.dumps(parameters),
            process_id,
            int(success),
            num_records,
            log_path,
            points_file_path,
        ),
    )
    conn.commit()
    conn.close()
