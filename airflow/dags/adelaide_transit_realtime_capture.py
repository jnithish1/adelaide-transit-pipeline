"""Capture a GTFS-Realtime snapshot every 15 minutes.

Writes to S3 only. Snowflake isn't touched, so this costs nothing in credits;
the daily DAG loads all of the day's snapshots in one COPY.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(
    dag_id="adelaide_transit_realtime_capture",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Australia/Adelaide"),
    catchup=False,           # a missed snapshot can't be recovered later anyway
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["transit", "realtime"],
    doc_md=__doc__,
)
def adelaide_transit_realtime_capture():

    @task(execution_timeout=timedelta(minutes=3))
    def capture_trip_updates() -> dict:
        from ingestion.extract_gtfs_realtime import run
        return run()

    capture_trip_updates()


adelaide_transit_realtime_capture()
