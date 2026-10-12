"""Capture one snapshot of Adelaide Metro GTFS-Realtime trip updates.

Each run reads the protobuf feed, flattens it to one row per
(trip, stop_time_update), and lands it as gzipped NDJSON. Schedule it every
15 minutes. It only writes to S3, so it never wakes the Snowflake warehouse;
the daily load picks up all the snapshots in one COPY.

    python -m ingestion.extract_gtfs_realtime

It also exposes `lambda_handler` so you can run it as an AWS Lambda on an
EventBridge schedule instead of Airflow/GitHub Actions.
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from google.transit import gtfs_realtime_pb2

from ingestion.config import settings
from ingestion.lake import build_key, http_get, ndjson_gz, put_bytes

log = logging.getLogger("extract_gtfs_realtime")
ADELAIDE = ZoneInfo("Australia/Adelaide")

_TRIP_REL = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship
_STU_REL = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship


def _event(stu, which: str) -> tuple[int | None, int | None]:
    """Return (delay_seconds, epoch_time) for arrival or departure, if present."""
    if not stu.HasField(which):
        return None, None
    ev = getattr(stu, which)
    return (ev.delay if ev.HasField("delay") else None,
            ev.time if ev.HasField("time") else None)


def flatten(feed: gtfs_realtime_pb2.FeedMessage, captured_at: str):
    """Yield one flat dict per stop_time_update in the feed."""
    feed_ts = feed.header.timestamp or None
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip = tu.trip
        base = {
            "feed_timestamp": feed_ts,
            "captured_at": captured_at,
            "entity_id": entity.id,
            "trip_id": trip.trip_id or None,
            "route_id": trip.route_id or None,
            "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
            "start_date": trip.start_date or None,  # YYYYMMDD service date
            "start_time": trip.start_time or None,
            "trip_schedule_relationship": _TRIP_REL.Name(trip.schedule_relationship),
            "vehicle_id": tu.vehicle.id if tu.HasField("vehicle") else None,
            "trip_update_timestamp": tu.timestamp or None,
        }
        if not tu.stop_time_update:  # e.g. a cancelled trip with no stop detail
            yield {**base, "stop_sequence": None, "stop_id": None}
            continue
        for stu in tu.stop_time_update:
            arr_delay, arr_time = _event(stu, "arrival")
            dep_delay, dep_time = _event(stu, "departure")
            yield {
                **base,
                "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
                "stop_id": stu.stop_id or None,
                "stop_schedule_relationship": _STU_REL.Name(stu.schedule_relationship),
                "arrival_delay": arr_delay,
                "arrival_time": arr_time,
                "departure_delay": dep_delay,
                "departure_time": dep_time,
            }


def run() -> dict:
    now_utc = datetime.now(UTC)
    captured_at = now_utc.isoformat()
    local_date = now_utc.astimezone(ADELAIDE).date()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(http_get(settings.gtfs_rt_trip_updates_url))

    body, n = ndjson_gz(flatten(feed, captured_at))
    stamp = now_utc.strftime("%Y%m%dT%H%M%SZ")
    uri = put_bytes(build_key("gtfs_rt", "trip_updates", local_date, f"trip_updates_{stamp}.json.gz"),
                    body, "application/gzip")
    log.info("captured %s stop updates from %s entities", n, len(feed.entity))
    return {"rows": n, "entities": len(feed.entity), "uri": uri}


def lambda_handler(event, context):  # pragma: no cover - AWS entry point
    logging.getLogger().setLevel(logging.INFO)
    return run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    print(json.dumps(run()))
