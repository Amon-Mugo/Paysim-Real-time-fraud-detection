# tests/test_alert_landing.py
"""Tests for the rule-based alert landing writers on a local Iceberg catalog."""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.alert_landing import (
    build_alerts_writer,
    landing_query_name,
    start_geo_landing_query,
    start_structuring_landing_query,
    start_velocity_landing_query,
)
from spark.alert_layout import (
    normalize_structuring_alerts,
    normalize_travel_alerts,
    normalize_velocity_alerts,
)
from spark.config import DetectionConfig
from spark.geo import RULE_NAME as GEO_RULE_NAME
from spark.iceberg_session import iceberg_packages, iceberg_spark_conf
from spark.iceberg_tables import create_landing_tables
from spark.schema import EVENT_TIME_FIELD
from spark.structuring import RULE_NAME as STRUCTURING_RULE_NAME
from spark.velocity import RULE_NAME as VELOCITY_RULE_NAME

WINDOW_START = datetime(2026, 10, 1, 12, 0, 0)
WINDOW_END = datetime(2026, 10, 1, 12, 10, 0)
GEO_EVENT_TIME = datetime(2026, 10, 1, 12, 5, 0)
GEO_PREVIOUS_TIME = datetime(2026, 10, 1, 12, 0, 0)
ACCOUNT = "C1"

_STRING_ARRAY = ArrayType(StringType())

_STRUCTURING_SCHEMA = StructType(
    [
        StructField("rule_name", StringType()),
        StructField("name_orig", StringType()),
        StructField("window_start", TimestampType()),
        StructField("window_end", TimestampType()),
        StructField("band_transaction_count", LongType()),
        StructField("band_total_amount", DoubleType()),
        StructField("transaction_ids", _STRING_ARRAY),
    ]
)

_VELOCITY_SCHEMA = StructType(
    [
        StructField("rule_name", StringType()),
        StructField("name_orig", StringType()),
        StructField("window_start", TimestampType()),
        StructField("window_end", TimestampType()),
        StructField("transaction_count", LongType()),
        StructField("total_amount", DoubleType()),
        StructField("transaction_ids", _STRING_ARRAY),
    ]
)

_GEO_SCHEMA = StructType(
    [
        StructField("rule_name", StringType()),
        StructField("name_orig", StringType()),
        StructField("transaction_id", StringType()),
        StructField(EVENT_TIME_FIELD, TimestampType()),
        StructField("previous_transaction_id", StringType()),
        StructField("previous_event_time", TimestampType()),
        StructField("distance_km", DoubleType()),
        StructField("speed_kmh", DoubleType()),
    ]
)


def _table(config: DetectionConfig, table: str) -> str:
    return f"{config.iceberg_catalog_name}.{config.iceberg_database}.{table}"


def _structuring_batch(spark: SparkSession) -> DataFrame:
    row = (
        STRUCTURING_RULE_NAME,
        ACCOUNT,
        WINDOW_START,
        WINDOW_END,
        3,
        29000.0,
        ["t1", "t2", "t3"],
    )
    return spark.createDataFrame([row], schema=_STRUCTURING_SCHEMA)


def _velocity_batch(spark: SparkSession) -> DataFrame:
    row = (
        VELOCITY_RULE_NAME,
        ACCOUNT,
        WINDOW_START,
        WINDOW_END,
        2,
        300.0,
        ["t1", "t2"],
    )
    return spark.createDataFrame([row], schema=_VELOCITY_SCHEMA)


def _geo_batch(spark: SparkSession) -> DataFrame:
    row = (
        GEO_RULE_NAME,
        ACCOUNT,
        "t2",
        GEO_EVENT_TIME,
        "t1",
        GEO_PREVIOUS_TIME,
        550.0,
        6600.0,
    )
    return spark.createDataFrame([row], schema=_GEO_SCHEMA)


_RULE_CASES = [
    pytest.param(
        normalize_structuring_alerts,
        _structuring_batch,
        STRUCTURING_RULE_NAME,
        ["t1", "t2", "t3"],
        WINDOW_END,
        id="structuring",
    ),
    pytest.param(
        normalize_velocity_alerts,
        _velocity_batch,
        VELOCITY_RULE_NAME,
        ["t1", "t2"],
        WINDOW_END,
        id="velocity",
    ),
    pytest.param(
        normalize_travel_alerts,
        _geo_batch,
        GEO_RULE_NAME,
        ["t2"],
        GEO_EVENT_TIME,
        id="geo",
    ),
]

_START_CASES = [
    pytest.param(
        start_structuring_landing_query, STRUCTURING_RULE_NAME, id="structuring"
    ),
    pytest.param(start_velocity_landing_query, VELOCITY_RULE_NAME, id="velocity"),
    pytest.param(start_geo_landing_query, GEO_RULE_NAME, id="geo"),
]


@pytest.fixture(scope="module")
def base_config(tmp_path_factory: pytest.TempPathFactory) -> DetectionConfig:
    warehouse = tmp_path_factory.mktemp("alert-landing-warehouse")
    return DetectionConfig(
        iceberg_catalog_type="hadoop",
        iceberg_catalog_name="test_catalog",
        iceberg_warehouse=str(warehouse),
    )


@pytest.fixture(scope="module")
def iceberg_spark(base_config: DetectionConfig) -> Iterator[SparkSession]:
    # A session left running by another module would ignore the Iceberg settings.
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("test-alert-landing")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ",".join(iceberg_packages(base_config)))
    )
    for key, value in iceberg_spark_conf(base_config).items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def config(
    iceberg_spark: SparkSession, base_config: DetectionConfig
) -> DetectionConfig:
    """Return a config whose landing tables live in a fresh database per test."""
    isolated = replace(base_config, iceberg_database=f"landing_{uuid.uuid4().hex[:8]}")
    create_landing_tables(iceberg_spark, isolated)
    return isolated


@pytest.mark.parametrize(
    "normalize, make_batch, rule_name, expected_ids, expected_time", _RULE_CASES
)
def test_alert_lands_in_the_common_layout(
    iceberg_spark, config, normalize, make_batch, rule_name, expected_ids, expected_time
):
    write_batch = build_alerts_writer(normalize, config)

    write_batch(make_batch(iceberg_spark), 0)

    alerts = iceberg_spark.table(_table(config, config.alerts_table)).collect()
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.rule_name == rule_name
    assert alert.name_orig == ACCOUNT
    assert list(alert.transaction_ids) == expected_ids
    assert alert.alert_time == expected_time
    assert alert.alert_id


@pytest.mark.parametrize(
    "normalize, make_batch, rule_name, expected_ids, expected_time", _RULE_CASES
)
def test_replayed_batch_inserts_nothing_twice(
    iceberg_spark, config, normalize, make_batch, rule_name, expected_ids, expected_time
):
    write_batch = build_alerts_writer(normalize, config)
    batch = make_batch(iceberg_spark)

    write_batch(batch, 0)
    write_batch(batch, 0)

    assert iceberg_spark.table(_table(config, config.alerts_table)).count() == 1


def test_structuring_details_carry_count_and_total(iceberg_spark, config):
    write_batch = build_alerts_writer(normalize_structuring_alerts, config)

    write_batch(_structuring_batch(iceberg_spark), 0)

    alert = iceberg_spark.table(_table(config, config.alerts_table)).collect()[0]
    details = json.loads(alert.details)
    assert details["transaction_count"] == 3
    assert details["total_amount"] == 29000.0


def test_geo_details_carry_distance_and_speed(iceberg_spark, config):
    write_batch = build_alerts_writer(normalize_travel_alerts, config)

    write_batch(_geo_batch(iceberg_spark), 0)

    alert = iceberg_spark.table(_table(config, config.alerts_table)).collect()[0]
    details = json.loads(alert.details)
    assert details["previous_transaction_id"] == "t1"
    assert details["distance_km"] == 550.0
    assert details["speed_kmh"] == 6600.0


def test_all_three_rules_share_one_alerts_table(iceberg_spark, config):
    build_alerts_writer(normalize_structuring_alerts, config)(
        _structuring_batch(iceberg_spark), 0
    )
    build_alerts_writer(normalize_velocity_alerts, config)(
        _velocity_batch(iceberg_spark), 0
    )
    build_alerts_writer(normalize_travel_alerts, config)(_geo_batch(iceberg_spark), 0)

    alerts = iceberg_spark.table(_table(config, config.alerts_table)).collect()
    assert {alert.rule_name for alert in alerts} == {
        STRUCTURING_RULE_NAME,
        VELOCITY_RULE_NAME,
        GEO_RULE_NAME,
    }
    assert len({alert.alert_id for alert in alerts}) == 3


@pytest.mark.parametrize("start_query, rule_name", _START_CASES)
def test_start_query_wires_name_mode_checkpoint_and_writer(start_query, rule_name):
    alerts = MagicMock()
    config = DetectionConfig()
    query_name = landing_query_name(rule_name)

    named = alerts.writeStream.queryName
    mode = named.return_value.outputMode
    batch_sink = mode.return_value.foreachBatch
    option = batch_sink.return_value.option
    expected = option.return_value.trigger.return_value.start.return_value

    started = start_query(alerts, config)

    named.assert_called_once_with(query_name)
    mode.assert_called_once_with("append")
    assert callable(batch_sink.call_args[0][0])
    option.assert_called_once_with(
        "checkpointLocation", f"{config.checkpoint_location}/{query_name}"
    )
    assert started is expected


def test_landing_query_names_are_distinct_per_rule():
    names = {
        landing_query_name(rule)
        for rule in (STRUCTURING_RULE_NAME, VELOCITY_RULE_NAME, GEO_RULE_NAME)
    }

    assert len(names) == 3
