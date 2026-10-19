"""Tests for the common alert layout and its deterministic alert ids."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Callable

from pyspark.sql import DataFrame, Row, SparkSession

from spark.alert_layout import (
    ALERT_COLUMNS,
    ALERT_ID,
    ALERT_TIME,
    DETAILS,
    TRANSACTION_IDS,
    normalize_ml_alerts,
    normalize_structuring_alerts,
    normalize_travel_alerts,
    normalize_velocity_alerts,
)
from spark.schema import EVENT_TIME_FIELD

_WINDOW_START = datetime(2026, 1, 1, 0, 0)
_WINDOW_END = datetime(2026, 1, 1, 2, 0)
_LATER_WINDOW_END = datetime(2026, 1, 1, 2, 15)
_EVENT_TIME = datetime(2026, 1, 1, 0, 30)
_PREVIOUS_EVENT_TIME = datetime(2026, 1, 1, 0, 10)

_STRUCTURING_SCHEMA = (
    "rule_name string, name_orig string, window_start timestamp, window_end timestamp, "
    "band_transaction_count long, band_total_amount double, transaction_ids array<string>"
)
_VELOCITY_SCHEMA = (
    "rule_name string, name_orig string, window_start timestamp, window_end timestamp, "
    "transaction_count long, total_amount double, transaction_ids array<string>"
)
_TRAVEL_SCHEMA = (
    f"rule_name string, name_orig string, transaction_id string, {EVENT_TIME_FIELD} timestamp, "
    "previous_transaction_id string, previous_event_time timestamp, "
    "distance_km double, speed_kmh double"
)
_ML_SCHEMA = (
    f"transaction_id string, {EVENT_TIME_FIELD} timestamp, name_orig string, amount double, "
    "fraud_score double, model_version string, rule_name string"
)


def _structuring_alerts(
    spark: SparkSession,
    window_end: datetime = _WINDOW_END,
    total_amount: float = 2_700_000.0,
) -> DataFrame:
    row = (
        "structuring",
        "C1",
        _WINDOW_START,
        window_end,
        3,
        total_amount,
        ["T1", "T2", "T3"],
    )
    return spark.createDataFrame([row], schema=_STRUCTURING_SCHEMA)


def _velocity_alerts(
    spark: SparkSession, window_end: datetime = _WINDOW_END
) -> DataFrame:
    row = ("velocity", "C1", _WINDOW_START, window_end, 12, 5_000.0, ["T1", "T2"])
    return spark.createDataFrame([row], schema=_VELOCITY_SCHEMA)


def _travel_alerts(spark: SparkSession, transaction_id: str = "T2") -> DataFrame:
    row = (
        "impossible_travel",
        "C1",
        transaction_id,
        _EVENT_TIME,
        "T1",
        _PREVIOUS_EVENT_TIME,
        950.5,
        2851.5,
    )
    return spark.createDataFrame([row], schema=_TRAVEL_SCHEMA)


def _ml_alerts(
    spark: SparkSession, model_version: str = "v1", amount: float = 800.0
) -> DataFrame:
    row = ("T2", _EVENT_TIME, "C1", amount, 0.9, model_version, "ml_model")
    return spark.createDataFrame([row], schema=_ML_SCHEMA)


def _only_row(alerts: DataFrame) -> Row:
    rows = alerts.collect()
    assert len(rows) == 1
    return rows[0]


def _alert_id(alerts: DataFrame) -> str:
    return _only_row(alerts)[ALERT_ID]


def _all_normalized(spark: SparkSession) -> list[DataFrame]:
    return [
        normalize_structuring_alerts(_structuring_alerts(spark)),
        normalize_velocity_alerts(_velocity_alerts(spark)),
        normalize_travel_alerts(_travel_alerts(spark)),
        normalize_ml_alerts(_ml_alerts(spark)),
    ]


def test_every_normalizer_emits_the_common_columns(spark: SparkSession) -> None:
    for alerts in _all_normalized(spark):
        assert tuple(alerts.columns) == ALERT_COLUMNS


def test_structuring_alert_maps_window_fields(spark: SparkSession) -> None:
    row = _only_row(normalize_structuring_alerts(_structuring_alerts(spark)))
    details = json.loads(row[DETAILS])

    assert row[ALERT_TIME] == _WINDOW_END
    assert row[TRANSACTION_IDS] == ["T1", "T2", "T3"]
    assert details["transaction_count"] == 3
    assert details["total_amount"] == 2_700_000.0


def test_velocity_alert_maps_window_fields(spark: SparkSession) -> None:
    row = _only_row(normalize_velocity_alerts(_velocity_alerts(spark)))
    details = json.loads(row[DETAILS])

    assert row[ALERT_TIME] == _WINDOW_END
    assert row[TRANSACTION_IDS] == ["T1", "T2"]
    assert details["transaction_count"] == 12
    assert details["total_amount"] == 5_000.0


def test_travel_alert_maps_event_fields(spark: SparkSession) -> None:
    row = _only_row(normalize_travel_alerts(_travel_alerts(spark)))
    details = json.loads(row[DETAILS])

    assert row[ALERT_TIME] == _EVENT_TIME
    assert row[TRANSACTION_IDS] == ["T2"]
    assert details["previous_transaction_id"] == "T1"
    assert details["distance_km"] == 950.5
    assert details["speed_kmh"] == 2851.5


def test_ml_alert_maps_score_fields(spark: SparkSession) -> None:
    row = _only_row(normalize_ml_alerts(_ml_alerts(spark)))
    details = json.loads(row[DETAILS])

    assert row[ALERT_TIME] == _EVENT_TIME
    assert row[TRANSACTION_IDS] == ["T2"]
    assert details["fraud_score"] == 0.9
    assert details["model_version"] == "v1"


def test_alert_ids_are_stable_across_replays(spark: SparkSession) -> None:
    builders: list[Callable[[], DataFrame]] = [
        lambda: normalize_structuring_alerts(_structuring_alerts(spark)),
        lambda: normalize_velocity_alerts(_velocity_alerts(spark)),
        lambda: normalize_travel_alerts(_travel_alerts(spark)),
        lambda: normalize_ml_alerts(_ml_alerts(spark)),
    ]
    for build in builders:
        assert _alert_id(build()) == _alert_id(build())


def test_window_change_changes_the_windowed_alert_id(spark: SparkSession) -> None:
    first = normalize_structuring_alerts(_structuring_alerts(spark))
    shifted = normalize_structuring_alerts(
        _structuring_alerts(spark, window_end=_LATER_WINDOW_END)
    )

    assert _alert_id(first) != _alert_id(shifted)


def test_transaction_change_changes_the_travel_alert_id(spark: SparkSession) -> None:
    first = normalize_travel_alerts(_travel_alerts(spark, transaction_id="T2"))
    other = normalize_travel_alerts(_travel_alerts(spark, transaction_id="T9"))

    assert _alert_id(first) != _alert_id(other)


def test_model_version_change_changes_the_ml_alert_id(spark: SparkSession) -> None:
    first = normalize_ml_alerts(_ml_alerts(spark, model_version="v1"))
    retrained = normalize_ml_alerts(_ml_alerts(spark, model_version="v2"))

    assert _alert_id(first) != _alert_id(retrained)


def test_fields_outside_the_key_do_not_change_the_alert_id(
    spark: SparkSession,
) -> None:
    structuring = normalize_structuring_alerts(_structuring_alerts(spark))
    structuring_changed = normalize_structuring_alerts(
        _structuring_alerts(spark, total_amount=1.0)
    )
    ml = normalize_ml_alerts(_ml_alerts(spark))
    ml_changed = normalize_ml_alerts(_ml_alerts(spark, amount=1.0))

    assert _alert_id(structuring) == _alert_id(structuring_changed)
    assert _alert_id(ml) == _alert_id(ml_changed)


def test_rules_do_not_collide_on_the_same_account_and_window(
    spark: SparkSession,
) -> None:
    structuring = normalize_structuring_alerts(_structuring_alerts(spark))
    velocity = normalize_velocity_alerts(_velocity_alerts(spark))

    assert _alert_id(structuring) != _alert_id(velocity)
