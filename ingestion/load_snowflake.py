"""Load new files from the S3 stage into Snowflake RAW tables.

COPY INTO tracks which files it has already loaded (for 64 days), so this is
safe to re-run: files already loaded are skipped, new ones are appended.
Row counts from every COPY are written to OPS.LOAD_LOG so you can quote real
volume numbers later (see scripts/metrics.sql).

Authentication uses a key pair, not a password. Snowflake is phasing out
password-only logins for service users.

    python -m ingestion.load_snowflake            # load everything new
    python -m ingestion.load_snowflake --dry-run  # print the SQL only
"""
from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

log = logging.getLogger("load_snowflake")

# (target table, stage sub-path). One RAW table per landed dataset.
LOADS: list[tuple[str, str]] = [
    ("RAW.GTFS_AGENCY", "gtfs_static/agency/"),
    ("RAW.GTFS_ROUTES", "gtfs_static/routes/"),
    ("RAW.GTFS_STOPS", "gtfs_static/stops/"),
    ("RAW.GTFS_TRIPS", "gtfs_static/trips/"),
    ("RAW.GTFS_STOP_TIMES", "gtfs_static/stop_times/"),
    ("RAW.GTFS_CALENDAR", "gtfs_static/calendar/"),
    ("RAW.GTFS_CALENDAR_DATES", "gtfs_static/calendar_dates/"),
    ("RAW.GTFS_RT_TRIP_UPDATES", "gtfs_rt/trip_updates/"),
    ("RAW.WEATHER_HOURLY", "weather/hourly/"),
]


def copy_sql(table: str, path: str) -> str:
    # Every RAW table has the same shape: the record as VARIANT + lineage.
    return f"""
COPY INTO {table} (record, _source_file, _source_row, _loaded_at)
FROM (
    SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
    FROM @RAW.LAKE_STAGE/{path}
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.NDJSON_GZ')
PATTERN = '.*[.]json[.]gz'
ON_ERROR = 'ABORT_STATEMENT'
""".strip()


def _connect():
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    with open(key_path, "rb") as fh:
        pkey = serialization.load_pem_private_key(
            fh.read(), password=passphrase.encode() if passphrase else None
        )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=pkey.private_bytes(
            serialization.Encoding.DER,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
        role=os.getenv("SNOWFLAKE_ROLE", "TRANSFORMER"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "TRANSIT_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "TRANSIT"),
        session_parameters={"QUERY_TAG": "transit_pipeline:load"},
    )


def run(dry_run: bool = False) -> dict:
    run_id = os.getenv("PIPELINE_RUN_ID", str(uuid.uuid4()))
    if dry_run:
        for table, path in LOADS:
            print(copy_sql(table, path), end=";\n\n")
        return {"run_id": run_id, "dry_run": True}

    totals: dict[str, int] = {}
    conn = _connect()
    try:
        cur = conn.cursor()
        for table, path in LOADS:
            started = datetime.now(timezone.utc)
            cur.execute(copy_sql(table, path))
            results = cur.fetchall()
            # COPY returns one row per file: (file, status, rows_parsed, rows_loaded, ...).
            # When there is nothing new it returns a single "Copy executed with 0 files" row.
            files = [r for r in results if len(r) > 3]
            rows_loaded = sum(int(r[3] or 0) for r in files)
            totals[table] = rows_loaded
            cur.execute(
                """INSERT INTO OPS.LOAD_LOG
                   (run_id, table_name, files_loaded, rows_loaded, started_at, finished_at)
                   VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP())""",
                (run_id, table, len(files), rows_loaded, started),
            )
            log.info("%-28s files=%-4d rows=%s", table, len(files), f"{rows_loaded:,}")
    finally:
        conn.close()
    return {"run_id": run_id, "rows_loaded": totals, "total_rows": sum(totals.values())}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    print(json.dumps(run(dry_run="--dry-run" in sys.argv), indent=2))
