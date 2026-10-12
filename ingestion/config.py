"""Central configuration, read from environment variables.

Nothing secret lives in this file. Copy `.env.example` to `.env` and fill it in,
or set the variables in Airflow / GitHub Actions.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

try:  # optional: load a local .env when running by hand
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    return value if value not in ("", None) else default


@dataclass(frozen=True)
class Settings:
    # --- Landing zone -------------------------------------------------------
    # If S3_BUCKET is empty, files are written to LOCAL_LAKE_DIR instead.
    # That lets you run and test the extractors without an AWS account.
    s3_bucket: str | None = field(default_factory=lambda: _env("S3_BUCKET"))
    s3_prefix: str = field(default_factory=lambda: _env("S3_PREFIX", "raw"))
    aws_region: str = field(default_factory=lambda: _env("AWS_REGION", "ap-southeast-2"))
    local_lake_dir: str = field(default_factory=lambda: _env("LOCAL_LAKE_DIR", "./data_lake"))

    # --- Adelaide Metro GTFS ------------------------------------------------
    # Check https://gtfs.adelaidemetro.com.au/ if these ever move.
    gtfs_static_url: str = field(
        default_factory=lambda: _env(
            "GTFS_STATIC_URL",
            "https://gtfs.adelaidemetro.com.au/v1/static/latest/google_transit.zip",
        )
    )
    gtfs_rt_trip_updates_url: str = field(
        default_factory=lambda: _env(
            "GTFS_RT_TRIP_UPDATES_URL",
            "https://gtfs.adelaidemetro.com.au/v1/realtime/trip_updates",
        )
    )

    # --- Weather (Open-Meteo) -----------------------------------------------
    # Free, documented API, no key needed for non-commercial use.
    # (BOM's website prohibits automated downloads, so it isn't used.)
    weather_api_url: str = field(
        default_factory=lambda: _env("WEATHER_API_URL", "https://api.open-meteo.com/v1/forecast")
    )
    weather_location_id: str = field(default_factory=lambda: _env("WEATHER_LOCATION_ID", "adelaide_cbd"))
    weather_location_name: str = field(default_factory=lambda: _env("WEATHER_LOCATION_NAME", "Adelaide CBD"))
    weather_lat: float = float(_env("WEATHER_LAT", "-34.9285"))
    weather_lon: float = float(_env("WEATHER_LON", "138.6007"))
    weather_past_days: int = int(_env("WEATHER_PAST_DAYS", "3"))

    http_user_agent: str = field(
        default_factory=lambda: _env(
            "HTTP_USER_AGENT",
            "adelaide-transit-pipeline/1.0 (portfolio project; contact via GitHub)",
        )
    )
    http_timeout_s: int = int(_env("HTTP_TIMEOUT_S", "60"))


settings = Settings()
