# tests/test_snowflake_landing.py
"""Tests for the Snowflake landing writers, run against fake staging and SQL runners."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.alert_landing import landing_query_name
from spark.alert_layout import (
    ALERT_COLUMNS,
    normalize_velocity_alerts,
)
from spark.config import DetectionConfig
from spark.geo import RULE_NAME as GEO_RULE_NAME
from spark.iceberg_tables import MODEL_VERSION_COLUMN, transactions_schema
from spark.landing import TRANSACTIONS_QUERY_NAME
from spark.schema import EVENT_TIME_FIELD
from spark.scoring import RULE_NAME as ML_RULE_NAME
from spark.snowflake_landing import (
    STAGING_ALERTS_GEO,
    STAGING_ALERTS_ML,
    STAGING_ALERTS_STRUCTURING,
    STAGING_ALERTS_VELOCITY,
    STAGING_TRANSACTIONS,
    TRANSACTION_QUERY_NAME,
    build_snowflake_alerts_writer,
    build_snowflake_transactions_writer,
    snowflake_query_name,
    start_snowflake_geo_landing_query,
    start_snowflake_structuring_landing_query,
    start_snowflake_transactions_landing_query,
    start_snowflake_velocity_landing_query,
)
from spark.structuring import RULE_NAME as STRUCTURING_RULE_NAME
from spark.velocity import RULE_NAME as VELOCITY_RULE_NAME

SETUP_SQL = Path(__file__).resolve().parents[1] / "snowflake" / "setup.sql"
WRITER_ROLE = "FRAUD_WRITER"
CONFIG = DetectionConfig()
MANIFEST = SimpleNamespace(model_version="stub-v1", alert_threshold=0.8)

EVENT_TIME = datetime(2026, 10, 1, 12, 0, 0)
WINDOW_START = datetime(2026, 10, 1, 12, 0, 0)
WINDOW_END = datetime(2026, 10, 1, 12, 10, 0)
GEO_EVENT_TIME = datetime(2026, 10, 1, 12, 5, 0)
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
        WINDOW_START,
        550.0,
        6600.0,
    )
    return spark.createDataFrame([row], schema=_GEO_SCHEMA)


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
        "name_orig": ACCOUNT,
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


def _transactions_batch(spark: SparkSession, *rows: dict) -> DataFrame:
    return spark.createDataFrame(list(rows), schema=_scored_schema())


def _make_recorder():
    """Return an event log plus fake stage and SQL callables that fill it."""
    events: list[tuple] = []

    def stage_batch(batch, staging):
        events.append(("stage", staging, sorted(batch.columns), batch.collect()))

    def run_sql(statement):
        events.append(("sql", statement))

    return events, stage_batch, run_sql


def _staged_events(events: list[tuple], staging: str) -> list[tuple]:
    return [event for event in events if event[0] == "stage" and event[1] == staging]


def _setup_sql() -> str:
    return SETUP_SQL.read_text()


def _table_columns(sql: str, table: str) -> list[str]:
    pattern = rf"TABLE\s+IF\s+NOT\s+EXISTS\s+{table}\s*\((.*?)\n\)\s*;"
    match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
    assert match, f"{table} definition not found in setup.sql"
    lines = [line.strip() for line in match.group(1).splitlines()]
    return [line.split()[0].lower() for line in lines if line]


_STAGING_TABLES = [
    STAGING_TRANSACTIONS,
    STAGING_ALERTS_STRUCTURING,
    STAGING_ALERTS_VELOCITY,
    STAGING_ALERTS_GEO,
    STAGING_ALERTS_ML,
]

_RULE_CASES = [
    pytest.param(
        start_snowflake_structuring_landing_query,
        _structuring_batch,
        STRUCTURING_RULE_NAME,
        STAGING_ALERTS_STRUCTURING,
        id="structuring",
    ),
    pytest.param(
        start_snowflake_velocity_landing_query,
        _velocity_batch,
        VELOCITY_RULE_NAME,
        STAGING_ALERTS_VELOCITY,
        id="velocity",
    ),
    pytest.param(
        start_snowflake_geo_landing_query,
        _geo_batch,
        GEO_RULE_NAME,
        STAGING_ALERTS_GEO,
        id="geo",
    ),
]


@pytest.fixture(autouse=True)
def _rows_arrive_pre_scored(monkeypatch):
    """Pass pre-scored rows straight through; scoring has its own tests."""
    monkeypatch.setattr(
        "spark.snowflake_landing.score_transactions",
        lambda batch, manifest, model: batch,
    )


def test_transactions_are_staged_merged_and_truncated(spark):
    events, stage_batch, run_sql = _make_recorder()
    write_batch = build_snowflake_transactions_writer(
        MANIFEST, "model", CONFIG, stage_batch, run_sql
    )

    write_batch(_transactions_batch(spark, _scored_row("t1", 0.1)), 0)

    assert events[0][:3] == (
        "stage",
        STAGING_TRANSACTIONS,
        sorted(transactions_schema().names),
    )
    assert events[1][1].startswith(
        f"MERGE INTO {CONFIG.snowflake_transactions_table} AS target"
    )
    assert events[2] == ("sql", f"TRUNCATE TABLE {STAGING_TRANSACTIONS}")
    assert len(events) == 3


def test_every_scored_row_is_staged_as_a_transaction(spark):
    events, stage_batch, run_sql = _make_recorder()
    write_batch = build_snowflake_transactions_writer(
        MANIFEST, "model", CONFIG, stage_batch, run_sql
    )

    write_batch(
        _transactions_batch(spark, _scored_row("t1", 0.1), _scored_row("t2", 0.9, 1)),
        0,
    )

    staged = _staged_events(events, STAGING_TRANSACTIONS)[0][3]
    assert {row.transaction_id for row in staged} == {"t1", "t2"}
    assert {row.model_version for row in staged} == {MANIFEST.model_version}


def test_only_scores_at_or_above_threshold_are_staged_as_ml_alerts(spark):
    events, stage_batch, run_sql = _make_recorder()
    write_batch = build_snowflake_transactions_writer(
        MANIFEST, "model", CONFIG, stage_batch, run_sql
    )

    write_batch(
        _transactions_batch(
            spark,
            _scored_row("low", 0.2),
            _scored_row("at", 0.8, 1),
            _scored_row("high", 0.95, 2),
        ),
        0,
    )

    alerts = _staged_events(events, STAGING_ALERTS_ML)[0][3]
    assert {tuple(json.loads(row.transaction_ids)) for row in alerts} == {
        ("at",),
        ("high",),
    }
    assert all(row.rule_name == ML_RULE_NAME for row in alerts)


def test_batch_below_threshold_stages_no_ml_alerts(spark):
    events, stage_batch, run_sql = _make_recorder()
    write_batch = build_snowflake_transactions_writer(
        MANIFEST, "model", CONFIG, stage_batch, run_sql
    )

    write_batch(_transactions_batch(spark, _scored_row("t1", 0.1)), 0)

    assert _staged_events(events, STAGING_ALERTS_ML) == []
    assert len(events) == 3


def test_a_failed_transactions_merge_skips_alerts_and_still_truncates(spark):
    events, stage_batch, _ = _make_recorder()

    def run_sql(statement):
        events.append(("sql", statement))
        if statement.startswith("MERGE"):
            raise RuntimeError("merge failed")

    write_batch = build_snowflake_transactions_writer(
        MANIFEST, "model", CONFIG, stage_batch, run_sql
    )

    with pytest.raises(RuntimeError, match="merge failed"):
        write_batch(_transactions_batch(spark, _scored_row("t1", 0.99)), 0)

    assert _staged_events(events, STAGING_ALERTS_ML) == []
    assert events[-1] == ("sql", f"TRUNCATE TABLE {STAGING_TRANSACTIONS}")


def test_alert_ids_are_staged_as_a_json_string(spark):
    events, stage_batch, run_sql = _make_recorder()
    write_batch = build_snowflake_alerts_writer(
        normalize_velocity_alerts, STAGING_ALERTS_VELOCITY, CONFIG, stage_batch, run_sql
    )

    write_batch(_velocity_batch(spark), 0)

    _, staging, columns, rows = events[0]
    assert staging == STAGING_ALERTS_VELOCITY
    assert columns == sorted(ALERT_COLUMNS)
    assert isinstance(rows[0].transaction_ids, str)
    assert json.loads(rows[0].transaction_ids) == ["t1", "t2"]


def test_alert_merge_parses_the_json_columns_and_keys_on_alert_id(spark):
    events, stage_batch, run_sql = _make_recorder()
    write_batch = build_snowflake_alerts_writer(
        normalize_velocity_alerts, STAGING_ALERTS_VELOCITY, CONFIG, stage_batch, run_sql
    )

    write_batch(_velocity_batch(spark), 0)

    merge_sql = events[1][1]
    assert merge_sql.startswith(f"MERGE INTO {CONFIG.snowflake_alerts_table} AS target")
    assert "ON target.alert_id = source.alert_id" in merge_sql
    assert "PARSE_JSON(transaction_ids) AS transaction_ids" in merge_sql
    assert "PARSE_JSON(details) AS details" in merge_sql
    assert events[2] == ("sql", f"TRUNCATE TABLE {STAGING_ALERTS_VELOCITY}")


def test_alert_writer_truncates_staging_even_when_the_merge_fails(spark):
    events, stage_batch, _ = _make_recorder()

    def run_sql(statement):
        events.append(("sql", statement))
        if statement.startswith("MERGE"):
            raise RuntimeError("merge failed")

    write_batch = build_snowflake_alerts_writer(
        normalize_velocity_alerts, STAGING_ALERTS_VELOCITY, CONFIG, stage_batch, run_sql
    )

    with pytest.raises(RuntimeError, match="merge failed"):
        write_batch(_velocity_batch(spark), 0)

    assert events[-1] == ("sql", f"TRUNCATE TABLE {STAGING_ALERTS_VELOCITY}")


def test_alert_writer_skips_empty_batches(spark):
    events, stage_batch, run_sql = _make_recorder()
    write_batch = build_snowflake_alerts_writer(
        normalize_velocity_alerts, STAGING_ALERTS_VELOCITY, CONFIG, stage_batch, run_sql
    )

    write_batch(spark.createDataFrame([], _VELOCITY_SCHEMA), 0)

    assert events == []


@pytest.mark.parametrize("start_query, make_batch, rule_name, staging", _RULE_CASES)
def test_alert_query_wires_name_checkpoint_and_staging_table(
    spark, start_query, make_batch, rule_name, staging
):
    alerts = MagicMock()
    events, stage_batch, run_sql = _make_recorder()
    query_name = snowflake_query_name(rule_name)

    named = alerts.writeStream.queryName
    mode = named.return_value.outputMode
    batch_sink = mode.return_value.foreachBatch
    option = batch_sink.return_value.option
    expected = option.return_value.trigger.return_value.start.return_value

    started = start_query(alerts, CONFIG, stage_batch, run_sql)

    named.assert_called_once_with(query_name)
    mode.assert_called_once_with("append")
    option.assert_called_once_with(
        "checkpointLocation", f"{CONFIG.checkpoint_location}/{query_name}"
    )
    assert started is expected

    batch_sink.call_args[0][0](make_batch(spark), 0)
    assert events[0][1] == staging


def test_transactions_query_wires_name_checkpoint_and_staging_table(spark):
    scored = MagicMock()
    events, stage_batch, run_sql = _make_recorder()

    named = scored.writeStream.queryName
    mode = named.return_value.outputMode
    batch_sink = mode.return_value.foreachBatch
    option = batch_sink.return_value.option
    expected = option.return_value.trigger.return_value.start.return_value

    started = start_snowflake_transactions_landing_query(
        scored, MANIFEST, "model", CONFIG, stage_batch, run_sql
    )

    named.assert_called_once_with(TRANSACTION_QUERY_NAME)
    mode.assert_called_once_with("append")
    option.assert_called_once_with(
        "checkpointLocation", f"{CONFIG.checkpoint_location}/{TRANSACTION_QUERY_NAME}"
    )
    assert started is expected

    batch_sink.call_args[0][0](_transactions_batch(spark, _scored_row("t1", 0.1)), 0)
    assert events[0][1] == STAGING_TRANSACTIONS


def test_snowflake_query_names_never_share_a_checkpoint_with_iceberg():
    rules = (STRUCTURING_RULE_NAME, VELOCITY_RULE_NAME, GEO_RULE_NAME)
    snowflake_names = {snowflake_query_name(rule) for rule in rules}
    snowflake_names.add(TRANSACTION_QUERY_NAME)
    iceberg_names = {landing_query_name(rule) for rule in rules}
    iceberg_names.add(TRANSACTIONS_QUERY_NAME)

    assert len(snowflake_names) == 4
    assert snowflake_names.isdisjoint(iceberg_names)


def test_transaction_columns_match_the_transactions_recent_table():
    table_columns = set(_table_columns(_setup_sql(), "TRANSACTIONS_RECENT"))

    assert table_columns - {"ingested_at"} == set(transactions_schema().names)


def test_alert_columns_exist_in_the_alerts_table():
    table_columns = set(_table_columns(_setup_sql(), "ALERTS"))

    assert set(ALERT_COLUMNS) <= table_columns


@pytest.mark.parametrize("staging", _STAGING_TABLES)
def test_staging_table_is_created_and_granted_in_setup_sql(staging):
    sql = _setup_sql()
    create = (
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TRANSIENT\s+)?TABLE\s+"
        rf"(?:IF\s+NOT\s+EXISTS\s+)?{staging}\b"
    )
    grant = (
        r"GRANT\s+SELECT,\s*INSERT,\s*TRUNCATE\s+ON\s+TABLE\s+"
        rf"{staging}\s+TO\s+ROLE\s+{WRITER_ROLE}\b"
    )

    assert re.search(create, sql, re.IGNORECASE)
    assert re.search(grant, sql, re.IGNORECASE)
