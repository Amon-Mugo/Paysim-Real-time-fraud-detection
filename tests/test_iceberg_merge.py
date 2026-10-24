# tests/test_iceberg_merge.py
"""Unit tests for the Iceberg merge helper used by the landing queries."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from spark.iceberg_merge import (
    _insert_missing_rows,
    _validate_target,
    build_merge_writer,
    merge_batch,
)

TABLE = "local.fraud.transactions"
KEYS = ["transaction_id"]
CONDITION = "target.`transaction_id` = source.`transaction_id`"


def make_batch() -> MagicMock:
    """Return a mocked DataFrame exposing the calls the helper makes."""
    return MagicMock()


def test_validate_target_builds_table_and_condition() -> None:
    table, condition = _validate_target(TABLE, KEYS)

    assert table == TABLE
    assert condition == CONDITION


def test_validate_target_joins_multiple_keys_with_and() -> None:
    _, condition = _validate_target(TABLE, ["step", "transaction_id"])

    assert condition == (
        "target.`step` = source.`step` AND "
        "target.`transaction_id` = source.`transaction_id`"
    )


def test_validate_target_rejects_empty_key_columns() -> None:
    with pytest.raises(ValueError):
        _validate_target(TABLE, [])


def test_insert_missing_rows_deduplicates_on_key_columns() -> None:
    batch = make_batch()

    _insert_missing_rows(batch, TABLE, CONDITION, KEYS)

    batch.dropDuplicates.assert_called_once_with(KEYS)


def test_insert_missing_rows_runs_merge_against_registered_view() -> None:
    batch = make_batch()

    _insert_missing_rows(batch, TABLE, CONDITION, KEYS)

    view_name = batch.dropDuplicates.return_value.createOrReplaceTempView.call_args[0][
        0
    ]
    statement = batch.sparkSession.sql.call_args[0][0]
    assert f"MERGE INTO {TABLE} AS target" in statement
    assert f"USING {view_name} AS source" in statement
    assert f"ON {CONDITION}" in statement
    assert "WHEN NOT MATCHED THEN INSERT *" in statement


def test_insert_missing_rows_drops_view_after_success() -> None:
    batch = make_batch()

    _insert_missing_rows(batch, TABLE, CONDITION, KEYS)

    view_name = batch.dropDuplicates.return_value.createOrReplaceTempView.call_args[0][
        0
    ]
    batch.sparkSession.catalog.dropTempView.assert_called_once_with(view_name)


def test_insert_missing_rows_drops_view_when_merge_fails() -> None:
    batch = make_batch()
    batch.sparkSession.sql.side_effect = RuntimeError("merge failed")

    with pytest.raises(RuntimeError):
        _insert_missing_rows(batch, TABLE, CONDITION, KEYS)

    batch.sparkSession.catalog.dropTempView.assert_called_once()


def test_merge_batch_merges_into_validated_table() -> None:
    batch = make_batch()

    merge_batch(batch, TABLE, KEYS)

    statement = batch.sparkSession.sql.call_args[0][0]
    assert f"MERGE INTO {TABLE} AS target" in statement
    assert CONDITION in statement


def test_merge_batch_rejects_empty_key_columns() -> None:
    with pytest.raises(ValueError):
        merge_batch(make_batch(), TABLE, [])


def test_build_merge_writer_validates_when_built() -> None:
    with pytest.raises(ValueError):
        build_merge_writer(TABLE, [])


def test_build_merge_writer_returns_batch_callable() -> None:
    batch = make_batch()

    write_batch = build_merge_writer(TABLE, KEYS)
    write_batch(batch, 7)

    statement = batch.sparkSession.sql.call_args[0][0]
    assert f"MERGE INTO {TABLE} AS target" in statement
    assert CONDITION in statement
