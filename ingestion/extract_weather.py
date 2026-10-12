"""Land hourly weather for Adelaide from Open-Meteo.

Open-Meteo is a free, documented weather API (no key needed for
non-commercial use). We ask for the last few days of hourly values in UTC
and keep only hours that have already happened, so no forecast values leak
into the warehouse.

Running daily with past_days=3 means each file overlaps the previous two.
That's deliberate: a failed run loses nothing, and recent hours that the
provider revises get the corrected value. Duplicates are removed in dbt
(int_weather__hourly_deduped).

    python -m ingestion.extract_weather

Why not BOM directly? The Bureau's website prohibits automated downloads and
blocks them (HTTP 403).
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from ingestion.config import settings
from ingestion.lake import build_key, http_get, ndjson_gz, put_bytes

log = logging.getLogger("extract_weather")
ADELAIDE = ZoneInfo("Australia/Adelaide")

HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
]


def request_params() -> dict:
    return {
        "latitude": settings.weather_lat,
        "longitude": settings.weather_lon,
        "hourly": ",".join(HOURLY_VARIABLES),
        "past_days": settings.weather_past_days,
        "forecast_days": 1,
        "timezone": "GMT",          # UTC avoids daylight-saving gaps and repeats
        "timeformat": "unixtime",   # epoch seconds, converted to Adelaide time in dbt
    }


def to_records(payload: dict, captured_at: str, now_epoch: int):
    """One record per past hour. Open-Meteo returns parallel arrays per variable."""
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    for i, t in enumerate(times):
        if t > now_epoch:  # skip forecast hours
            continue
        yield {
            "location_id": settings.weather_location_id,
            "location_name": settings.weather_location_name,
            "latitude": payload.get("latitude"),
            "longitude": payload.get("longitude"),
            "time_epoch_utc": t,
            **{var: (hourly.get(var) or [None] * len(times))[i] for var in HOURLY_VARIABLES},
            "_captured_at": captured_at,
            "_source": "open-meteo",
        }


def run() -> dict:
    now_utc = datetime.now(UTC)
    captured_at = now_utc.isoformat()

    payload = json.loads(http_get(settings.weather_api_url, params=request_params()))
    body, n = ndjson_gz(to_records(payload, captured_at, int(now_utc.timestamp())))

    stamp = now_utc.strftime("%Y%m%dT%H%M%SZ")
    uri = put_bytes(
        build_key("weather", "hourly", now_utc.astimezone(ADELAIDE).date(),
                  f"{settings.weather_location_id}_{stamp}.json.gz"),
        body, "application/gzip",
    )
    log.info("landed %s hourly weather rows for %s", n, settings.weather_location_name)
    return {"rows": n, "location": settings.weather_location_id, "uri": uri}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    print(json.dumps(run()))
