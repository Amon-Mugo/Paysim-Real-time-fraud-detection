# used to initialise apache iceberg landing tables for alerts and transactions of
# data streams in pyspark application automatically dervies database schema

from __future__ import annotations

import re

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.alert_layout import ACCOUNT, ALERT_ID, ALERT_TIME, DETAILS, RULE_NAME
from spark.alert_layout import TRANSACTION_IDS
from spark.config import DetectionConfig
from spark.schema import EVENT_TIME_FIELD, TRANSACTION_SCHEMA
from spark.scoring import SCORE_COLUMN

MODEL_VERSION_COLUMN = "model_version"
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TABLE_FORMAT_VERSION = "2"
_KAFKA_COLUMNS = (
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_timestamp", TimestampType()),
)


# used to validate sql sytanxes
def _validated_identifiers(name: str) -> str:
    if not _IDENTIFIER_PATTERN.match(name):
        raise ValueError(f"Invalid Iceberg Identifier {name!r}")
    return name


# used to enable apache iceberg to query different sql functions and database names
def _qualified_name(config: DetectionConfig, *parts: str) -> str:
    names = [config.iceberg_catalog_name, config.iceberg_database, *parts]
    return ".".join(_validated_identifiers(name) for name in names)


# used  to translate the StructType defined earlier into proper sql compatible column definations
def _column_definitions(schema: StructType) -> str:
    return ",".join(
        f"`{field.name}`{field.dataType.simpleString()}" for field in schema
    )


# used in executing data definition language (DDL) sql command to create apache icebrg table in spark
# catalog instead of writing manula sql commands everytime thus automating this process
def _create_table(
    spark: SparkSession, table_name: str, schema: StructType, partition_column: str
) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({_column_definitions(schema)}) "
        "USING iceberg "
        f"PARTITIONED BY (days({partition_column})) "
        f"TBLPROPERTIES ('format-version' = '{_TABLE_FORMAT_VERSION}')"
    )


# used to returns a scored transaction and processed table instead of hadcording a manual column names
# it takes the 3 kafka lineage for message that goes to kafka,model output and model version
def transactions_schema() -> StructType:
    parsed_fields = [
        (
            StructField(field.name, TimestampType())
            if field.name == EVENT_TIME_FIELD
            else field
        )
        for field in TRANSACTION_SCHEMA.fields
    ]

    return StructType(
        [
            *parsed_fields,
            *_KAFKA_COLUMNS,
            StructField(SCORE_COLUMN, DoubleType()),
            StructField(MODEL_VERSION_COLUMN, StringType()),
        ]
    )


# used in defining and determining pyspark schema for the fraud alerts transactions
# this ensures alerts are saved in iceberg tables
def alerts_schema() -> StructType:
    return StructType(
        [
            StructField(ALERT_ID, StringType()),
            StructField(RULE_NAME, StringType()),
            StructField(ACCOUNT, StringType()),
            StructField(ALERT_TIME, TimestampType()),
            StructField(TRANSACTION_IDS, ArrayType(StringType())),
            StructField(DETAILS, StringType()),
        ]
    )


# used as the main orchestration of the module it is called at the very begining only once
# used to create iceberg table ,create transactions table,creat alets table
def create_landing_tables(spark: SparkSession, config: DetectionConfig) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_qualified_name(config)}")
    _create_table(
        spark,
        _qualified_name(config, config.transactions_table),
        transactions_schema(),
        EVENT_TIME_FIELD,
    )
    _create_table(
        spark,
        _qualified_name(config, config.alerts_table),
        alerts_schema(),
        ALERT_TIME,
    )
