# tests/test_geo_late_events.py
"""Late and out-of-order event tests for the impossible-travel rule in spark/geo.py.

Purpose: prove that a stale event never raises a false alert and never overwrites
the account's stored location, since a bad overwrite would corrupt every later
comparison for that account.
Design: events are written as JSON files and the real streaming query is run one
batch at a time with availableNow, reusing one checkpoint so per-account state
carries between batches. No Kafka broker is needed.
"""

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.geo import detect_impossible_travel

ACCOUNT = "C100"
NAIROBI = (-1.2921, 36.8219)
MOMBASA = (-4.0435, 39.6682)

_EVENT_SCHEMA = (
    "transaction_id string, event_time string, name_orig string, "
    "latitude double, longitude double"
)
_MAX_SPEED_KMH = 500.0
_MIN_DISTANCE_KM = 1.0
_WATERMARK_DELAY = "900 seconds"
_STATE_TTL_MINUTES = 60


@pytest.fixture(scope="module")
def spark_session():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("geo-late-event-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def stream_dirs(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    return input_dir, tmp_path / "checkpoint"


def _event(transaction_id: str, event_time: str, location: tuple) -> dict:
    latitude, longitude = location
    return {
        "transaction_id": transaction_id,
        "event_time": f"2026-01-01T{event_time}",
        "name_orig": ACCOUNT,
        "latitude": latitude,
        "longitude": longitude,
    }


def _write_batch(input_dir: Path, batch_name: str, events: list) -> None:
    lines = "\n".join(json.dumps(event) for event in events)
    (input_dir / f"{batch_name}.json").write_text(lines + "\n")


def _run_batch(spark, input_dir: Path, checkpoint_dir: Path) -> list:
    """Process every unread file once, reusing the checkpoint so state carries over."""
    alerts = []
    transactions = (
        spark.readStream.schema(_EVENT_SCHEMA)
        .json(str(input_dir))
        .withColumn("event_time", F.col("event_time").cast("timestamp"))
        .withColumn("is_valid_event", F.lit(True))
    )
    detected = detect_impossible_travel(
        transactions,
        max_speed_kmh=_MAX_SPEED_KMH,
        min_distance_km=_MIN_DISTANCE_KM,
        watermark_delay=_WATERMARK_DELAY,
        state_ttl_minutes=_STATE_TTL_MINUTES,
    )
    query = (
        detected.writeStream.foreachBatch(
            lambda batch, _batch_id: alerts.extend(batch.collect())
        )
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return alerts


def _alerted_pairs(alerts: list) -> list:
    """Return (flagged transaction, previous transaction) for each alert."""
    return [(alert.transaction_id, alert.previous_transaction_id) for alert in alerts]


def _process_batches(spark, stream_dirs, batches: list) -> list:
    input_dir, checkpoint_dir = stream_dirs
    alerts = []
    for index, events in enumerate(batches):
        _write_batch(input_dir, f"batch_{index}", events)
        alerts.extend(_run_batch(spark, input_dir, checkpoint_dir))
    return alerts


def test_late_event_is_skipped_without_raising_an_alert(spark_session, stream_dirs):
    alerts = _process_batches(
        spark_session,
        stream_dirs,
        [
            [_event("t1", "12:00:00", NAIROBI)],
            [_event("t2", "11:50:00", MOMBASA)],
            [_event("t3", "12:05:00", NAIROBI)],
        ],
    )

    assert alerts == []


def test_late_event_does_not_replace_the_stored_location(spark_session, stream_dirs):
    alerts = _process_batches(
        spark_session,
        stream_dirs,
        [
            [_event("t1", "12:00:00", NAIROBI)],
            [_event("t2", "11:50:00", MOMBASA)],
            [_event("t3", "12:30:00", MOMBASA)],
        ],
    )

    assert _alerted_pairs(alerts) == [("t3", "t1")]
    assert alerts[0].speed_kmh > _MAX_SPEED_KMH


def test_event_older_than_the_watermark_is_ignored(spark_session, stream_dirs):
    alerts = _process_batches(
        spark_session,
        stream_dirs,
        [
            [_event("t1", "12:00:00", NAIROBI)],
            [_event("t2", "11:00:00", MOMBASA)],
            [_event("t3", "12:05:00", NAIROBI)],
        ],
    )

    assert alerts == []


def test_rows_within_one_batch_are_compared_in_event_time_order(
    spark_session, stream_dirs
):
    alerts = _process_batches(
        spark_session,
        stream_dirs,
        [
            [
                _event("t2", "12:30:00", MOMBASA),
                _event("t1", "12:00:00", NAIROBI),
            ]
        ],
    )

    assert _alerted_pairs(alerts) == [("t2", "t1")]