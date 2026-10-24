# tests/test_landing_transactions.py
"""Tests for the transactions landing writer on a local Hadoop Iceberg catalog."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BooleanType, StructField, StructType

from spark.config import DetectionConfig
from spark.iceberg_session import iceberg_packages, iceberg_spark_conf
from spark.iceberg_tables import (
    MODEL_VERSION_COLUMN,
    create_landing_tables,
    transactions_schema,
)
from spark.landing import (
    TRANSACTIONS_QUERY_NAME,
    build_transactions_writer,
    start_transactions_landing_query,
)
from spark.scoring import RULE_NAME as ML_RULE_NAME

EVENT_TIME = datetime(2026, 10, 1, 12, 0, 0)
MANIFEST = SimpleNamespace(model_version="stub-v1", alert_threshold=0.8)


def _table(config: DetectionConfig, table: str) -> str:
    return f"{config.iceberg_catalog_name}.{config.iceberg_database}.{table}"


def _scored_schema() -> StructType:
    """Return the scored stream's schema: table columns minus the model version."""
    fields = [
        field
        for field in transactions_schema().fields
        if field.name != MODEL_VERSION_COLUMN
    ]
    return StructType([*fields, StructField("is_valid_event", BooleanType())])


def _scored_row(transaction_id: str, score: float, offset: int = 0) -> dict:
    return {
        "transaction_id": transaction_id,
        "event_time": EVENT_TIME,
        "transaction_type": "TRANSFER",
        "amount": 100.0,
        "name_orig": "C1",
        "old_balance_orig": 500.0,
        "new_balance_orig": 400.0,
        "name_dest": "M1",
        "old_balance_dest": 0.0,
        "new_balance_dest": 100.0,
        "latitude": -1.28,
        "longitude": 36.82,
        "kafka_partition": 0,
        "kafka_offset": offset,
        "kafka_timestamp": EVENT_TIME,
        "fraud_score": score,
        "is_valid_event": True,
    }


def _batch(spark: SparkSession, *rows: dict) -> DataFrame:
    return spark.createDataFrame(list(rows), schema=_scored_schema())


@pytest.fixture(scope="module")
def base_config(tmp_path_factory: pytest.TempPathFactory) -> DetectionConfig:
    warehouse = tmp_path_factory.mktemp("landing-warehouse")
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
        .appName("test-landing-transactions")
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


def test_every_scored_row_lands_in_transactions(iceberg_spark, config):
    write_batch = build_transactions_writer(MANIFEST, config)

    write_batch(
        _batch(iceberg_spark, _scored_row("t1", 0.1), _scored_row("t2", 0.9, 1)), 0
    )

    rows = iceberg_spark.table(_table(config, config.transactions_table)).collect()
    assert {row.transaction_id for row in rows} == {"t1", "t2"}


def test_transactions_carry_the_model_version(iceberg_spark, config):
    write_batch = build_transactions_writer(MANIFEST, config)

    write_batch(_batch(iceberg_spark, _scored_row("t1", 0.1)), 0)

    row = iceberg_spark.table(_table(config, config.transactions_table)).collect()[0]
    assert row.model_version == MANIFEST.model_version


def test_only_scores_at_or_above_threshold_become_alerts(iceberg_spark, config):
    write_batch = build_transactions_writer(MANIFEST, config)

    write_batch(
        _batch(
            iceberg_spark,
            _scored_row("low", 0.2),
            _scored_row("at", 0.8, 1),
            _scored_row("high", 0.95, 2),
        ),
        0,
    )

    alerts = iceberg_spark.table(_table(config, config.alerts_table)).collect()
    assert {tuple(alert.transaction_ids) for alert in alerts} == {("at",), ("high",)}


def test_alerts_use_the_ml_rule_name(iceberg_spark, config):
    write_batch = build_transactions_writer(MANIFEST, config)

    write_batch(_batch(iceberg_spark, _scored_row("t1", 0.99)), 0)

    alert = iceberg_spark.table(_table(config, config.alerts_table)).collect()[0]
    assert alert.rule_name == ML_RULE_NAME
    assert alert.name_orig == "C1"
    assert alert.alert_time == EVENT_TIME


def test_batch_below_threshold_writes_no_alerts(iceberg_spark, config):
    write_batch = build_transactions_writer(MANIFEST, config)

    write_batch(_batch(iceberg_spark, _scored_row("t1", 0.1)), 0)

    assert iceberg_spark.table(_table(config, config.alerts_table)).count() == 0


def test_replayed_batch_inserts_nothing_twice(iceberg_spark, config):
    write_batch = build_transactions_writer(MANIFEST, config)
    batch = _batch(iceberg_spark, _scored_row("t1", 0.1), _scored_row("t2", 0.9, 1))

    write_batch(batch, 0)
    write_batch(batch, 0)

    transactions = iceberg_spark.table(_table(config, config.transactions_table))
    alerts = iceberg_spark.table(_table(config, config.alerts_table))
    assert transactions.count() == 2
    assert alerts.count() == 1


def test_duplicate_keys_inside_one_batch_land_once(iceberg_spark, config):
    write_batch = build_transactions_writer(MANIFEST, config)

    write_batch(
        _batch(iceberg_spark, _scored_row("t1", 0.9), _scored_row("t1", 0.9, 1)), 0
    )

    transactions = iceberg_spark.table(_table(config, config.transactions_table))
    assert transactions.count() == 1


def test_start_query_wires_name_checkpoint_and_writer():
    scored = MagicMock()
    config = DetectionConfig()
    query = (
        scored.writeStream.queryName.return_value.foreachBatch.return_value.option.return_value.trigger.return_value.start.return_value
    )

    started = start_transactions_landing_query(scored, MANIFEST, config)

    named = scored.writeStream.queryName
    named.assert_called_once_with(TRANSACTIONS_QUERY_NAME)
    option = named.return_value.foreachBatch.return_value.option
    option.assert_called_once_with(
        "checkpointLocation", f"{config.checkpoint_location}/{TRANSACTIONS_QUERY_NAME}"
    )
    assert callable(named.return_value.foreachBatch.call_args[0][0])
    assert started is query
