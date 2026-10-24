# used to write the micro batch data into apache iceberg and check any duplicated files
# in the process

from __future__ import annotations
import uuid
from collections.abc import Callable, Sequence
from spark.iceberg_tables import validated_identifiers
from pyspark.sql import DataFrame

BatchWrite = Callable[
    [DataFrame, int], None
]  # used for forebatch used in spark streaming


# used to validate incoming data and checks for any duplicates
def _validate_target(target_tables: str, key_columns: Sequence[str]) -> tuple[str, str]:

    if not key_columns:
        raise ValueError("key columns must be present not empty")
    table = ".".join(validated_identifiers(part) for part in target_tables.split("."))
    keys = [validated_identifiers(column) for column in key_columns]
    condition = " AND ".join(f"target.`{key}` = source.`{key}`" for key in keys)
    return table, condition


# used to write the tables into iceberg safely and keeps duplicates in a temp table
def _insert_missing_rows(
    batch: DataFrame, table: str, condition: str, key_columns: Sequence[str]
) -> None:
    view_name = f"landing_batch_{uuid.uuid4().hex}"
    batch.dropDuplicates(list(key_columns)).createOrReplaceTempView(view_name)
    try:
        batch.sparkSession.sql(
            f"MERGE INTO {table} AS target USING {view_name} AS source "
            f"ON {condition} WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        batch.sparkSession.catalog.dropTempView(view_name)


# used to merge the 2 helper functions
def merge_batch(
    batch: DataFrame, target_tables: str, key_columns: Sequence[str]
) -> None:
    table, condition = _validate_target(target_tables, key_columns)
    _insert_missing_rows(batch, table, condition, key_columns)


# used to set up for spark structured streaming
def build_merge_writer(target_table: str, key_columns: Sequence[str]) -> BatchWrite:
    table, condition = _validate_target(target_table, key_columns)

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        _insert_missing_rows(batch, table, condition, key_columns)

    return write_batch
