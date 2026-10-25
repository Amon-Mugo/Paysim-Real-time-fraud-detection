"""Tests for the landing-parity tripwire."""

from __future__ import annotations

import logging

import pytest

from spark.landing_parity import check_landing_parity

TARGET_VIEW = "landed_rows_view"
KEY_COLUMNS = ["transaction_id"]


@pytest.fixture
def landed_view(spark):
    landed = spark.createDataFrame([("T1",), ("T2",)], ["transaction_id"])
    landed.createOrReplaceTempView(TARGET_VIEW)
    yield TARGET_VIEW
    spark.catalog.dropTempView(TARGET_VIEW)


def _batch(spark, ids):
    return spark.createDataFrame(
        [(transaction_id, 1.0) for transaction_id in ids],
        "transaction_id string, amount double",
    )


def test_returns_zero_when_every_key_landed(spark, landed_view):
    assert (
        check_landing_parity(_batch(spark, ["T1", "T2"]), landed_view, KEY_COLUMNS, 1)
        == 0
    )


def test_counts_keys_missing_from_the_target(spark, landed_view):
    batch = _batch(spark, ["T1", "T3", "T4"])
    assert check_landing_parity(batch, landed_view, KEY_COLUMNS, 2) == 2


def test_in_batch_duplicates_count_once(spark, landed_view):
    batch = _batch(spark, ["T3", "T3", "T3"])
    assert check_landing_parity(batch, landed_view, KEY_COLUMNS, 3) == 1


def test_null_keys_are_ignored(spark, landed_view):
    batch = _batch(spark, ["T1", None])
    assert check_landing_parity(batch, landed_view, KEY_COLUMNS, 4) == 0


def test_replayed_batch_stays_clean(spark, landed_view):
    batch = _batch(spark, ["T1", "T2"])
    assert check_landing_parity(batch, landed_view, KEY_COLUMNS, 5) == 0
    assert check_landing_parity(batch, landed_view, KEY_COLUMNS, 5) == 0


def test_logs_an_error_when_keys_are_missing(spark, landed_view, caplog):
    with caplog.at_level(logging.INFO, logger="spark.landing_parity"):
        check_landing_parity(_batch(spark, ["T9"]), landed_view, KEY_COLUMNS, 6)
    assert any(
        record.levelno == logging.ERROR and "missing_keys=1" in record.getMessage()
        for record in caplog.records
    )


def test_logs_info_when_everything_landed(spark, landed_view, caplog):
    with caplog.at_level(logging.INFO, logger="spark.landing_parity"):
        check_landing_parity(_batch(spark, ["T1"]), landed_view, KEY_COLUMNS, 7)
    assert all(record.levelno == logging.INFO for record in caplog.records)
    assert any("ok" in record.getMessage() for record in caplog.records)
