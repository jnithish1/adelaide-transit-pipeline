"""Offline tests for the extractors. No network, no AWS, no Snowflake.

    pytest -q
"""
from __future__ import annotations

import gzip
import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pytest
from google.transit import gtfs_realtime_pb2


@pytest.fixture(autouse=True)
def local_lake(tmp_path, monkeypatch):
    """Point every test at a temp folder instead of S3."""
    import ingestion.config as cfg
    import ingestion.lake as lake

    s = cfg.Settings(s3_bucket=None, local_lake_dir=str(tmp_path))
    monkeypatch.setattr(cfg, "settings", s)
    monkeypatch.setattr(lake, "settings", s)
    for mod in ("extract_gtfs_static", "extract_gtfs_realtime", "extract_weather"):
        m = __import__(f"ingestion.{mod}", fromlist=["settings"])
        monkeypatch.setattr(m, "settings", s)
    import ingestion.extract_gtfs_static as st
    monkeypatch.setattr(st, "STATE_KEY", "raw/gtfs_static/_state/latest_manifest.json")
    return tmp_path


def read_ndjson_gz(path: Path) -> list[dict]:
    return [json.loads(line) for line in gzip.decompress(path.read_bytes()).splitlines()]


# --- GTFS static -------------------------------------------------------------

def make_gtfs_zip() -> bytes:
    files = {
        "routes.txt": "﻿route_id,route_short_name,route_long_name,route_type\nG10,G10,Glenelg Tram,0\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n100,Victoria Square,-34.928,138.600\n",
        "trips.txt": "route_id,service_id,trip_id,direction_id\nG10,WK,T1,0\n",
        "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                          "T1,25:10:00,25:10:30,100,1\n",
        "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                        "WK,1,1,1,1,1,0,0,20261001,20261231\n",
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, body in files.items():
            zf.writestr(name, body)
    return buf.getvalue()


def test_gtfs_static_lands_tables_and_skips_unchanged(local_lake, monkeypatch):
    import ingestion.extract_gtfs_static as st

    monkeypatch.setattr(st, "http_get", lambda url: make_gtfs_zip())
    first = st.run()
    assert first["changed"] is True
    assert first["rows"] == {"routes": 1, "stops": 1, "trips": 1, "stop_times": 1, "calendar": 1}

    routes_file = next(local_lake.glob("raw/gtfs_static/routes/dt=*/*.json.gz"))
    rec = read_ndjson_gz(routes_file)[0]
    assert rec["route_id"] == "G10"            # BOM stripped from header
    assert rec["_feed_version"] == first["feed_version"]

    # GTFS times past midnight must survive untouched (parsed in dbt).
    st_rec = read_ndjson_gz(next(local_lake.glob("raw/gtfs_static/stop_times/dt=*/*.json.gz")))[0]
    assert st_rec["arrival_time"] == "25:10:00"

    second = st.run()
    assert second["changed"] is False          # same zip -> no new files


# --- GTFS realtime -----------------------------------------------------------

def make_feed() -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_791_000_000
    ent = feed.entity.add(id="e1")
    tu = ent.trip_update
    tu.trip.trip_id = "T1"
    tu.trip.route_id = "G10"
    tu.trip.start_date = "20261012"
    tu.vehicle.id = "V42"
    stu = tu.stop_time_update.add(stop_sequence=1, stop_id="100")
    stu.arrival.delay = 120
    stu.arrival.time = 1_791_000_120
    stu2 = tu.stop_time_update.add(stop_sequence=2, stop_id="101")
    stu2.departure.time = 1_791_000_300            # time only, no delay
    cancelled = feed.entity.add(id="e2").trip_update
    cancelled.trip.trip_id = "T2"
    cancelled.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED
    return feed.SerializeToString()


def test_gtfs_rt_flattens_one_row_per_stop_update(local_lake, monkeypatch):
    import ingestion.extract_gtfs_realtime as rt

    monkeypatch.setattr(rt, "http_get", lambda url: make_feed())
    result = rt.run()
    assert result == {**result, "rows": 3, "entities": 2}

    rows = read_ndjson_gz(Path(result["uri"]))
    first, second, cancelled = rows
    assert first["arrival_delay"] == 120 and first["vehicle_id"] == "V42"
    assert second["arrival_delay"] is None and second["departure_time"] == 1_791_000_300
    assert cancelled["trip_schedule_relationship"] == "CANCELED"
    assert cancelled["stop_sequence"] is None


# --- Weather (Open-Meteo) --------------------------------------------------------

def test_weather_keeps_past_hours_only(local_lake, monkeypatch):
    import ingestion.extract_weather as w

    now = int(datetime.now(timezone.utc).timestamp())
    past, future = now - 7200, now + 7200
    payload = {
        "latitude": -34.93, "longitude": 138.6,
        "hourly": {"time": [past, past + 3600, future],
                   "temperature_2m": [21.4, 20.9, 30.0],
                   "precipitation": [0.2, 0.0, 5.0]},
    }
    monkeypatch.setattr(w, "http_get", lambda url, **kw: json.dumps(payload).encode())
    result = w.run()
    assert result["rows"] == 2                     # forecast hour dropped
    rows = read_ndjson_gz(Path(result["uri"]))
    assert rows[0]["time_epoch_utc"] == past and rows[0]["precipitation"] == 0.2
    assert rows[0]["rain"] is None                 # variable missing from payload -> null, not a crash
    assert rows[0]["location_id"] == "adelaide_cbd"


def test_weather_requests_utc_epoch_times():
    from ingestion.extract_weather import request_params

    p = request_params()
    assert p["timezone"] == "GMT" and p["timeformat"] == "unixtime"


def test_copy_sql_is_idempotent_style():
    from ingestion.load_snowflake import LOADS, copy_sql

    sql = copy_sql(*LOADS[0])
    assert "COPY INTO RAW.GTFS_AGENCY" in sql and "METADATA$FILENAME" in sql
    assert "FORCE" not in sql  # never force: that would double-load files
