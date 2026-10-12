"""Land Bureau of Meteorology half-hourly observations for Adelaide.

BOM publishes a rolling 72-hour JSON per station. Running daily gives ~48h of
overlap between files, which is deliberate: if one day's run fails, the next
day still covers the gap. Duplicates are removed in dbt
(int_bom__observations_deduped).

    python -m ingestion.extract_bom
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ingestion.config import settings
from ingestion.lake import build_key, http_get, ndjson_gz, put_bytes

log = logging.getLogger("extract_bom")
ADELAIDE = ZoneInfo("Australia/Adelaide")


def to_records(payload: dict, captured_at: str, source_url: str):
    """One record per half-hourly observation, with station header attached."""
    obs = payload.get("observations", {})
    header = (obs.get("header") or [{}])[0]
    for row in obs.get("data", []):
        yield {
            **row,
            "_station_name": header.get("name"),
            "_state": header.get("state"),
            "_captured_at": captured_at,
            "_source_url": source_url,
        }


def run() -> dict:
    now_utc = datetime.now(timezone.utc)
    captured_at = now_utc.isoformat()
    url = settings.bom_observations_url
    payload = json.loads(http_get(url, headers={"Accept": "application/json"}))

    body, n = ndjson_gz(to_records(payload, captured_at, url))
    station = url.rsplit("/", 1)[-1].removesuffix(".json")
    stamp = now_utc.strftime("%Y%m%dT%H%M%SZ")
    uri = put_bytes(
        build_key("bom", "observations", now_utc.astimezone(ADELAIDE).date(), f"{station}_{stamp}.json.gz"),
        body, "application/gzip",
    )
    log.info("landed %s observations for %s", n, station)
    return {"rows": n, "station": station, "uri": uri}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    print(json.dumps(run()))
