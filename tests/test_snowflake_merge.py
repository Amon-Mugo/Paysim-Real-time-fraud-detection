from __future__ import annotations

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from spark.snowflake_merge import build_merge_sql, build_merge_writer

_TARGET = "FRAUD_DETECTION.MONITORING.TRANSACTIONS_RECENT"
_STAGING = "STG_TRANSACTIONS_RECENT"
_COLUMNS = ["transaction_id", "amount", "fraud_score"]
_SCHEMA = StructType([StructField(name, StringType()) for name in (*_COLUMNS, "extra")])
_ROWS = [
    ("T1", "10", "0.1", "x"),
    ("T1", "10", "0.1", "x"),
    ("T2", "20", "0.2", "y"),
]


def _make_recorder():
    events: list = []

    def stage_batch(batch, staging):
        events.append(("stage", staging, sorted(batch.columns), batch.count()))

    def run_sql(statement):
        events.append(("sql", statement))

    return events, stage_batch, run_sql


def _build_writer(stage_batch, run_sql):
    return build_merge_writer(
        _TARGET, _STAGING, ["transaction_id"], _COLUMNS, stage_batch, run_sql
    )


def test_merge_sql_is_insert_only_on_the_key():
    sql = build_merge_sql(_TARGET, _STAGING, ["transaction_id"], _COLUMNS)

    assert sql.startswith(f"MERGE INTO {_TARGET} AS target USING (")
    assert "ON target.transaction_id = source.transaction_id" in sql
    assert "WHEN NOT MATCHED THEN INSERT (transaction_id, amount, fraud_score)" in sql
    assert "VALUES (source.transaction_id, source.amount, source.fraud_score)" in sql
    assert "WHEN MATCHED" not in sql
    assert "PARSE_JSON" not in sql


def test_merge_sql_keeps_one_row_per_key_in_the_source():
    sql = build_merge_sql(_TARGET, _STAGING, ["transaction_id"], _COLUMNS)

    assert f"FROM {_STAGING}" in sql
    assert "QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id" in sql


def test_merge_sql_parses_only_the_json_columns():
    sql = build_merge_sql(
        "ALERTS",
        "STG_ALERTS",
        ["alert_id"],
        ["alert_id", "rule_name", "transaction_ids", "details"],
        json_columns=["transaction_ids", "details"],
    )

    assert "PARSE_JSON(transaction_ids) AS transaction_ids" in sql
    assert "PARSE_JSON(details) AS details" in sql
    assert "PARSE_JSON(alert_id)" not in sql
    assert "PARSE_JSON(rule_name)" not in sql


def test_merge_sql_joins_composite_keys_with_and():
    sql = build_merge_sql(
        "ALERTS", "STG_ALERTS", ["rule_name", "name_orig"], ["rule_name", "name_orig"]
    )

    assert (
        "ON target.rule_name = source.rule_name AND target.name_orig = source.name_orig"
        in sql
    )


@pytest.mark.parametrize(
    "target, staging",
    [("bad-name; DROP TABLE x", _STAGING), (_TARGET, "stg name"), ("", _STAGING)],
)
def test_merge_sql_rejects_invalid_table_names(target, staging):
    with pytest.raises(ValueError):
        build_merge_sql(target, staging, ["transaction_id"], _COLUMNS)


def test_merge_sql_rejects_invalid_column_names():
    with pytest.raises(ValueError):
        build_merge_sql(
            _TARGET, _STAGING, ["transaction_id"], ["transaction_id", "a b"]
        )


def test_merge_sql_rejects_empty_columns_and_keys():
    with pytest.raises(ValueError, match="columns must not be empty"):
        build_merge_sql(_TARGET, _STAGING, ["transaction_id"], [])
    with pytest.raises(ValueError, match="key_columns must not be empty"):
        build_merge_sql(_TARGET, _STAGING, [], _COLUMNS)


def test_merge_sql_rejects_keys_and_json_columns_outside_the_column_list():
    with pytest.raises(ValueError, match="not in the column list"):
        build_merge_sql(_TARGET, _STAGING, ["missing_key"], _COLUMNS)
    with pytest.raises(ValueError, match="not in the column list"):
        build_merge_sql(
            _TARGET, _STAGING, ["transaction_id"], _COLUMNS, json_columns=["details"]
        )


def test_writer_stages_then_merges_then_truncates(spark):
    events, stage_batch, run_sql = _make_recorder()
    write_batch = _build_writer(stage_batch, run_sql)

    write_batch(spark.createDataFrame(_ROWS, _SCHEMA), 0)

    assert events[0] == ("stage", _STAGING, sorted(_COLUMNS), 2)
    assert events[1][0] == "sql" and events[1][1].startswith("MERGE INTO")
    assert events[2] == ("sql", f"TRUNCATE TABLE {_STAGING}")
    assert len(events) == 3


def test_writer_stages_only_the_selected_columns_without_duplicates(spark):
    events, stage_batch, run_sql = _make_recorder()
    write_batch = _build_writer(stage_batch, run_sql)

    write_batch(spark.createDataFrame(_ROWS, _SCHEMA), 0)

    _, _, staged_columns, staged_count = events[0]
    assert "extra" not in staged_columns
    assert staged_count == 2


def test_writer_truncates_staging_even_when_the_merge_fails(spark):
    statements: list[str] = []

    def run_sql(statement):
        statements.append(statement)
        if statement.startswith("MERGE"):
            raise RuntimeError("merge failed")

    write_batch = _build_writer(lambda batch, staging: None, run_sql)

    with pytest.raises(RuntimeError, match="merge failed"):
        write_batch(spark.createDataFrame(_ROWS, _SCHEMA), 0)

    assert statements[-1] == f"TRUNCATE TABLE {_STAGING}"


def test_writer_skips_empty_batches(spark):
    events, stage_batch, run_sql = _make_recorder()
    write_batch = _build_writer(stage_batch, run_sql)

    write_batch(spark.createDataFrame([], _SCHEMA), 0)

    assert events == []


def test_writer_rejects_an_invalid_staging_name_at_build_time():
    with pytest.raises(ValueError):
        build_merge_writer(
            _TARGET,
            "stg name",
            ["transaction_id"],
            _COLUMNS,
            lambda batch, staging: None,
            lambda statement: None,
        )
