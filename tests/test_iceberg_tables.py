"""Integration tests for the Iceberg landing tables on a local Hadoop catalog."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace

import pytest
from pyspark.sql import SparkSession

from spark.config import DetectionConfig
from spark.iceberg_session import iceberg_packages, iceberg_spark_conf
from spark.iceberg_tables import (
    alerts_schema,
    create_landing_tables,
    transactions_schema,
)


def _namespace(config: DetectionConfig) -> str:
    return f"{config.iceberg_catalog_name}.{config.iceberg_database}"


def _table_name(config: DetectionConfig, table: str) -> str:
    return f"{_namespace(config)}.{table}"


@pytest.fixture(scope="module")
def landing_config(tmp_path_factory: pytest.TempPathFactory) -> DetectionConfig:
    warehouse = tmp_path_factory.mktemp("iceberg-warehouse")
    return DetectionConfig(
        iceberg_catalog_type="hadoop",
        iceberg_catalog_name="test_catalog",
        iceberg_warehouse=str(warehouse),
    )


@pytest.fixture(scope="module")
def iceberg_spark(landing_config: DetectionConfig) -> Iterator[SparkSession]:
    # A session left running by another module would ignore the Iceberg settings.
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("test-iceberg-tables")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ",".join(iceberg_packages(landing_config)))
    )
    for key, value in iceberg_spark_conf(landing_config).items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()
    yield session
    session.stop()


def test_creates_both_landing_tables(iceberg_spark, landing_config):
    create_landing_tables(iceberg_spark, landing_config)

    rows = iceberg_spark.sql(f"SHOW TABLES IN {_namespace(landing_config)}").collect()

    assert {row.tableName for row in rows} == {
        landing_config.transactions_table,
        landing_config.alerts_table,
    }


@pytest.mark.parametrize(
    ("table_field", "expected_schema"),
    [
        ("transactions_table", transactions_schema()),
        ("alerts_table", alerts_schema()),
    ],
)
def test_table_columns_match_the_schema(
    iceberg_spark, landing_config, table_field, expected_schema
):
    create_landing_tables(iceberg_spark, landing_config)
    table = _table_name(landing_config, getattr(landing_config, table_field))

    actual = [(f.name, f.dataType) for f in iceberg_spark.table(table).schema.fields]

    assert actual == [(f.name, f.dataType) for f in expected_schema.fields]


@pytest.mark.parametrize(
    ("table_field", "partition_column"),
    [("transactions_table", "event_time"), ("alerts_table", "alert_time")],
)
def test_tables_are_partitioned_by_day(
    iceberg_spark, landing_config, table_field, partition_column
):
    create_landing_tables(iceberg_spark, landing_config)
    table = _table_name(landing_config, getattr(landing_config, table_field))

    ddl = iceberg_spark.sql(f"SHOW CREATE TABLE {table}").collect()[0][0]

    assert f"days({partition_column})" in ddl


def test_creation_is_idempotent(iceberg_spark, landing_config):
    create_landing_tables(iceberg_spark, landing_config)
    create_landing_tables(iceberg_spark, landing_config)

    rows = iceberg_spark.sql(f"SHOW TABLES IN {_namespace(landing_config)}").collect()

    assert len(rows) == 2


def test_unsafe_table_name_is_rejected(iceberg_spark, landing_config):
    unsafe = replace(landing_config, transactions_table="bad name; DROP TABLE x")

    with pytest.raises(ValueError, match="Invalid Iceberg identifier"):
        create_landing_tables(iceberg_spark, unsafe)
