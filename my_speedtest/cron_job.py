#!/usr/bin/env python3
"""
Collects speedtest results on a regular basis.

Copyright 2020-08-22 Nils Müller
"""
from datetime import datetime

import speedtest

from my_speedtest import db


connection = db.get_db()
with connection:
    timestamp = datetime.now().isoformat()
    measurement_id = db.add_measurement(connection=connection, timestamp=timestamp)

client = speedtest.Speedtest()
try:
    client.get_servers([])
    client.get_best_server()
    client.download()
    client.upload()
except Exception as exception:
    with connection:
        db.add_error_log(
            connection=connection,
            measurement_id=measurement_id,
            log_message=str(exception),
        )
else:
    results = client.results.dict()
    with connection:
        db.add_download(
            connection=connection,
            measurement_id=measurement_id,
            download_rate=results["download"],
        )
        db.add_upload(
            connection=connection,
            measurement_id=measurement_id,
            upload_rate=results["upload"],
        )
        db.add_ping(
            connection=connection, measurement_id=measurement_id, ping=results["ping"]
        )
