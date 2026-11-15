"""Fetch the inputs for a fraud evaluation run.

Ground-truth labels are read from S3 and alerts plus the processed-transaction
count come from Snowflake. Every function takes an already-open client or
connection, so the DAG owns credentials and these functions stay easy to test.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

ALERTS_TABLE = "ALERTS"
TRANSACTIONS_TABLE = "TRANSACTIONS_RECENT"


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/key`` into its bucket and key."""
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key:
        raise ValueError(f"Expected an s3://bucket/key URI, got {uri!r}")
    return parsed.netloc, key


def _fetch_dicts(connection: Any, query: str, params: tuple) -> list[dict[str, Any]]:
    """Run a query and return each row as a dict keyed by lower-case column name."""
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        columns = [column[0].lower() for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def read_ground_truth_lines(s3_client: Any, uri: str) -> list[str]:
    """Return the lines of a ground-truth JSONL file stored in S3."""
    bucket, key = _parse_s3_uri(uri)
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"]
    return [line.decode("utf-8") for line in body.iter_lines()]


def fetch_alerts(connection: Any, seconds_since_start: int) -> list[dict[str, Any]]:
    """Return alert rows ingested within the last ``seconds_since_start`` seconds."""
    query = (
        f"SELECT ALERT_ID, RULE_NAME, TRANSACTION_IDS FROM {ALERTS_TABLE} "
        "WHERE INGESTED_AT >= DATEADD(second, -%s, CURRENT_TIMESTAMP())"
    )
    return _fetch_dicts(connection, query, (seconds_since_start,))


def count_transactions(connection: Any, seconds_since_start: int) -> int:
    """Count transactions ingested within the last ``seconds_since_start`` seconds."""
    query = f"SELECT COUNT(*) FROM {TRANSACTIONS_TABLE} WHERE INGESTED_AT >= DATEADD(second, -%s, CURRENT_TIMESTAMP())"
    with connection.cursor() as cursor:
        cursor.execute(query, (seconds_since_start,))
        return cursor.fetchone()[0]
