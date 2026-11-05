# used for landing the incoming data from spark structed streaming into
# mico batches and performs three things that is staging data in a temoporary table
# merge the data and clean the data .also allows testing without live snowflake account

from __future__ import annotations
from collections.abc import (
    Callable,
    Sequence,
    Collection,
)  # sequence enables us to accept different data structures and an odered list

# collection enables us to pass an orderd list
from spark.iceberg_tables import validated_identifiers
from pyspark.sql import DataFrame

BatchWriter = Callable[[DataFrame, int], None]  # used for foreachbatch
StageBatch = Callable[
    [DataFrame, str], None
]  # for foreachbacth for the stage table name and the data frame
RunSql = Callable[[str], None]  # used for running sql in snowflake


# used in validation of the sql queries
def _validated_table(name: str) -> str:
    return ".".join(validated_identifiers(part) for part in name.split("."))


# ussed in validateing the columns
def _validated_columns(
    columns: Sequence[str], key_columns: Sequence[str], json_column: Collection[str]
) -> list[str]:
    # json_columns enables us to parse different columns into proper datatypes

    if not columns:
        raise ValueError("columns must not be empty")
    if not key_columns:
        raise ValueError("key_columns must not be empty")

    validated = [validated_identifiers(column) for column in columns]
    unknown = (set(key_columns) | set(json_column)) - set(validated)
    if unknown:
        raise ValueError(f"columns not in the column list :{sorted(unknown)}")
    return validated


# used in constructing a sql sudquey that reads from staging tables and removes duplicates and
# cast json string inot snowflake VARIANT data types


def _source_query(
    staging: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
    json_columns: Collection[str],
) -> str:

    select_list = ", ".join(
        f"PARSE_JSON({column}) AS {column}" if column in json_columns else column
        for column in columns
    )
    keys = ", ".join(key_columns)
    return (
        f"SELECT {select_list} FROM {staging} "
        f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {keys}) = 1"
    )


def build_merge_sql(
    target_table: str,
    staging_table: str,
    key_columns: Sequence[str],
    columns: Sequence[str],
    json_columns: Collection[str] = (),
) -> str:
    column_list = _validated_columns(columns, key_columns, json_columns)
    target = _validated_table(target_table)
    staging = _validated_table(staging_table)

    condition = " AND ".join(f"target.{key} = source.{key}" for key in key_columns)
    # thus we can join target.region = source.region if there is multiple primary keys
    source = _source_query(staging, column_list, key_columns, json_columns)
    insert_columns = ", ".join(column_list)
    # thus becomes col1,col2,col3
    insert_values = ", ".join(f"source.{column}" for column in column_list)
    # becomes source.col1,source.col2  used in matching the column names fo validation
    return (
        f"MERGE INTO {target} AS target USING ({source}) AS source "  # specifies main serving table
        f"ON {condition} "  # check if row is using primary keys
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"  # matches any missing column names
    )


# used for write batch to be streamed
def build_merge_writer(
    target_table: str,
    staging_table: str,
    key_columns: Sequence[str],
    columns: Sequence[str],
    stage_batch: StageBatch,
    run_sql: RunSql,
    json_columns: Collection[str] = (),
) -> BatchWriter:

    merge_sql = build_merge_sql(
        target_table, staging_table, key_columns, columns, json_columns
    )
    staging = _validated_table(staging_table)
    truncate_sql = f"TRUNCATE TABLE {staging}"
    selected = list(columns)  # make them inot a proper list for sql
    keys = list(key_columns)  # make them into proper python list

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        # staging data
        stage_batch(batch.select(*selected).dropDuplicates(keys), staging)
        try:
            run_sql(merge_sql)
        finally:
            run_sql(truncate_sql)

    return write_batch
