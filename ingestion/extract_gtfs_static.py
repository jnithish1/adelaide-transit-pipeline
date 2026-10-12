"""Download the Adelaide Metro GTFS static timetable and land it in the lake.

Run daily. The timetable zip only changes every few weeks, so we hash it and
skip the upload when nothing changed. That keeps Snowflake from re-loading
~1.5M identical stop_times rows every day.

    python -m ingestion.extract_gtfs_static
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import sys
import zipfile
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ingestion.config import settings
from ingestion.lake import build_key, get_bytes, http_get, ndjson_gz, put_bytes

log = logging.getLogger("extract_gtfs_static")
ADELAIDE = ZoneInfo("Australia/Adelaide")

# GTFS files we model. Others in the zip (shapes, fare rules...) are archived
# in the raw zip but not converted.
TABLES = ["agency", "routes", "stops", "trips", "stop_times", "calendar", "calendar_dates"]
STATE_KEY = f"{settings.s3_prefix}/gtfs_static/_state/latest_manifest.json"


def csv_to_records(raw: bytes, feed_version: str, extracted_at: str):
    """Yield one dict per CSV row, tagged with lineage columns."""
    text = raw.decode("utf-8-sig")  # GTFS files often carry a BOM
    for row in csv.DictReader(io.StringIO(text)):
        clean = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items() if k}
        clean["_feed_version"] = feed_version
        clean["_extracted_at"] = extracted_at
        yield clean


def run(force: bool = False) -> dict:
    now_utc = datetime.now(timezone.utc)
    today_local = now_utc.astimezone(ADELAIDE).date()
    extracted_at = now_utc.isoformat()

    log.info("downloading %s", settings.gtfs_static_url)
    zip_bytes = http_get(settings.gtfs_static_url)
    sha = hashlib.sha256(zip_bytes).hexdigest()
    feed_version = sha[:16]

    previous = get_bytes(STATE_KEY)
    if previous and not force and json.loads(previous).get("sha256") == sha:
        log.info("feed unchanged (sha %s) - nothing to land", feed_version)
        return {"changed": False, "feed_version": feed_version, "rows": {}}

    # Archive the original zip first (bronze copy, useful for reprocessing).
    put_bytes(build_key("gtfs_static", "_zip", today_local, f"google_transit_{feed_version}.zip"),
              zip_bytes, "application/zip")

    rows: dict[str, int] = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = {n.rsplit("/", 1)[-1]: n for n in zf.namelist()}
        for table in TABLES:
            member = names.get(f"{table}.txt")
            if member is None:
                log.warning("%s.txt not in feed - skipping", table)
                continue
            body, n = ndjson_gz(csv_to_records(zf.read(member), feed_version, extracted_at))
            put_bytes(build_key("gtfs_static", table, today_local, f"{table}_{feed_version}.json.gz"),
                      body, "application/gzip")
            rows[table] = n

    manifest = {"sha256": sha, "feed_version": feed_version, "extracted_at": extracted_at,
                "partition_date": today_local.isoformat(), "rows": rows}
    put_bytes(STATE_KEY, json.dumps(manifest, indent=2).encode(), "application/json")
    log.info("landed feed %s: %s", feed_version, rows)
    return {"changed": True, **manifest}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    result = run(force="--force" in sys.argv)
    print(json.dumps(result))
