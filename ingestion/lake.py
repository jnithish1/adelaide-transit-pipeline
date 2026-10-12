"""Writing to the landing zone (S3, or a local folder for development).

Key layout (Hive-style partitions so Snowflake and Athena can both read them):

    raw/<source>/<table>/dt=YYYY-MM-DD/<file>

Every object is immutable: we never overwrite, we only add new files.
Snowflake's COPY INTO remembers which files it already loaded, so re-running a
load is safe.
"""
from __future__ import annotations

import gzip
import io
import json
import logging
from collections.abc import Iterable
from datetime import date
from pathlib import Path

import requests

from ingestion.config import settings

log = logging.getLogger(__name__)


def build_key(source: str, table: str, partition_date: date, filename: str) -> str:
    return f"{settings.s3_prefix}/{source}/{table}/dt={partition_date.isoformat()}/{filename}"


def put_bytes(key: str, body: bytes, content_type: str = "application/octet-stream") -> str:
    """Write bytes to S3 (or local disk). Returns the URI written."""
    if settings.s3_bucket:
        import boto3  # imported lazily so local runs don't need AWS libs

        s3 = boto3.client("s3", region_name=settings.aws_region)
        s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=body, ContentType=content_type)
        uri = f"s3://{settings.s3_bucket}/{key}"
    else:
        path = Path(settings.local_lake_dir) / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        uri = str(path)
    log.info("wrote %s (%s bytes)", uri, f"{len(body):,}")
    return uri


def get_bytes(key: str) -> bytes | None:
    """Read an object back, or None if it doesn't exist."""
    if settings.s3_bucket:
        import boto3
        from botocore.exceptions import ClientError

        s3 = boto3.client("s3", region_name=settings.aws_region)
        try:
            return s3.get_object(Bucket=settings.s3_bucket, Key=key)["Body"].read()
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
    path = Path(settings.local_lake_dir) / key
    return path.read_bytes() if path.exists() else None


def ndjson_gz(records: Iterable[dict]) -> tuple[bytes, int]:
    """Serialise records as gzipped newline-delimited JSON. Returns (bytes, row_count)."""
    buf = io.BytesIO()
    n = 0
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for rec in records:
            gz.write(json.dumps(rec, separators=(",", ":"), default=str).encode())
            gz.write(b"\n")
            n += 1
    return buf.getvalue(), n


def http_get(url: str, **kwargs) -> bytes:
    headers = {"User-Agent": settings.http_user_agent, **kwargs.pop("headers", {})}
    resp = requests.get(url, headers=headers, timeout=settings.http_timeout_s, **kwargs)
    resp.raise_for_status()
    return resp.content
