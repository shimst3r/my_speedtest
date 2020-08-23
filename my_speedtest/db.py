#!/usr/bin/env python3
"""
SQL scripts used for creating and maintaining the measurements database.

If called from the command line, the necessary database tables will be created.
"""
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List


def get_db() -> sqlite3.Connection:
    """
    Establishes a connection and returns it.
    """
    database_path = f"{Path(__file__).parent}/measurements.db"
    connection = sqlite3.connect(database=database_path)

    return connection


def create_measurements_table(connection: sqlite3.Connection):
    """
    Creates the root table which will be used as a foreign key
    for download, upload, and latency measurements.
    """
    query = """
    CREATE TABLE IF NOT EXISTS measurements(
        measurement_id INTEGER PRIMARY KEY,
        timestamp TEXT NOT NULL
    );
    """
    connection.execute(query)


def create_downloads_table(connection: sqlite3.Connection):
    """
    Creates the downloads table which stores the download measurement in
    bytes per second.
    """
    query = """
    CREATE TABLE IF NOT EXISTS downloads(
        download_id INTEGER PRIMARY KEY,
        measurement_id INTEGER NOT NULL,
        download_rate REAL NOT NULL,
        FOREIGN KEY(measurement_id) REFERENCES measurement(measurement_id)
    );
    """
    connection.execute(query)


def create_uploads_table(connection: sqlite3.Connection):
    """
    Creates the uploads table which stores the upload measurement in
    bytes per second.
    """
    query = """
    CREATE TABLE IF NOT EXISTS uploads(
        upload_id INTEGER PRIMARY KEY,
        measurement_id INTEGER NOT NULL,
        upload_rate REAL NOT NULL,
        FOREIGN KEY(measurement_id) REFERENCES measurement(measurement_id)
    );
    """
    connection.execute(query)


def create_ping_table(connection: sqlite3.Connection):
    """
    Creates the ping table which stores the ping measurement in
    milliseconds.
    """
    query = """
    CREATE TABLE IF NOT EXISTS pings(
        ping_id INTEGER PRIMARY KEY,
        measurement_id INTEGER NOT NULL,
        ping REAL NOT NULL,
        FOREIGN KEY(measurement_id) REFERENCES measurement(measurement_id)
    );
    """
    connection.execute(query)


def create_error_log_table(connection: sqlite3.Connection):
    """
    Creates the error log table which stores error logs for measurement jobs.
    """
    query = """
    CREATE TABLE IF NOT EXISTS error_logs(
        error_log_id INTEGER PRIMARY KEY,
        measurement_id INTEGER NOT NULL,
        log_message TEXT NOT NULL,
        FOREIGN KEY(measurement_id) REFERENCES measurement(measurement_id)
    );
    """
    connection.execute(query)


def add_measurement(connection: sqlite3.Connection, timestamp: datetime) -> int:
    """
    Adds a new measurement for `timestamp` and returns its primary key.
    """
    query = """
    INSERT INTO measurements(timestamp)
    VALUES (?);
    """
    cursor = connection.execute(query, [timestamp])
    measurement_id = cursor.lastrowid

    return measurement_id


def add_download(
    connection: sqlite3.Connection, measurement_id: int, download_rate: float
):
    """
    Adds a download rate for `measurement_id`.
    """
    query = """
    INSERT INTO downloads(measurement_id, download_rate)
    VALUES (?, ?);
    """
    connection.execute(query, [measurement_id, download_rate])


def add_upload(connection: sqlite3.Connection, measurement_id: int, upload_rate: float):
    """
    Adds an upload rate for `measurement_id`.
    """
    query = """
    INSERT INTO uploads(measurement_id, upload_rate)
    VALUES (?, ?);
    """
    connection.execute(query, [measurement_id, upload_rate])


def add_ping(connection: sqlite3.Connection, measurement_id: int, ping: float):
    """
    Adds a ping for `measurement_id`.
    """
    query = """
    INSERT INTO pings(measurement_id, ping)
    VALUES (?, ?);
    """
    connection.execute(query, [measurement_id, ping])


def add_error_log(
    connection: sqlite3.Connection, measurement_id: int, log_message: str
):
    """
    Adds an error log for `measurement_id`.
    """
    query = """
    INSERT INTO error_logs(measurement_id, log_message)
    VALUES (?, ?);
    """
    connection.execute(query, [measurement_id, log_message])


def get_result_for_measurement_id(
    connection: sqlite3.Connection, measurement_id: int
) -> Dict:
    """
    Returns the download, upload, and ping measurements for `measurement_id`.
    """
    query = """
    SELECT m.timestamp, d.download_rate, u.upload_rate, p.ping
    FROM measurements m
    JOIN downloads d ON m.measurement_id = d.measurement_id
    JOIN uploads u ON m.measurement_id = u.measurement_id
    JOIN pings p ON m.measurement_id = p.measurement_id
    WHERE m.measurement_id = ?;
    """
    connection.row_factory = sqlite3.Row
    cursor = connection.execute(query, [measurement_id])
    return dict(cursor.fetchone())


def get_all_results(connection: sqlite3.Connection) -> List[Dict]:
    """
    Returns all measurements.
    """
    query = """
    SELECT m.timestamp, d.download_rate, u.upload_rate, p.ping
    FROM measurements m
    JOIN downloads d ON m.measurement_id = d.measurement_id
    JOIN uploads u ON m.measurement_id = u.measurement_id
    JOIN pings p ON m.measurement_id = p.measurement_id
    """
    connection.row_factory = sqlite3.Row
    cursor = connection.execute(query)

    return [dict(row) for row in cursor.fetchall()]


def get_results_for_start_and_end_date(
    connection: sqlite3.Connection, start_datetime: datetime, end_datetime: datetime
) -> List[Dict]:
    """
    Returns all measurements between `start_datetime` and `end_datetime`.
    """
    query = """
    SELECT m.timestamp, d.download_rate, u.upload_rate, p.ping
    FROM measurements m
    JOIN downloads d ON m.measurement_id = d.measurement_id
    JOIN uploads u ON m.measurement_id = u.measurement_id
    JOIN pings p ON m.measurement_id = p.measurement_id
    WHERE m.timestamp >= ? AND m.timestamp <= ?;
    """
    connection.row_factory = sqlite3.Row
    cursor = connection.execute(query, [start_datetime, end_datetime])

    return [dict(row) for row in cursor.fetchall()]


if __name__ == "__main__":
    with get_db() as connection:
        create_measurements_table(connection=connection)
        create_downloads_table(connection=connection)
        create_uploads_table(connection=connection)
        create_ping_table(connection=connection)
        create_error_log_table(connection=connection)
