# used in orchestration of the data into snowflake tables after the data is written into them
# it enables to build alters tables  and staging tables that are needed by the stremas

from __future__ import annotations
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from spark.alert_landing import AlertNormalizer, landing_query_name
from spark.alert_layout import (
    ALERT_COLUMNS,
    DETAILS,
    TRANSACTION_IDS,
    normalize_structuring_alerts,
    normalize_travel_alerts,
    normalize_velocity_alerts,
)
from spark.config import DetectionConfig, spark_interval
from spark.geo import RULE_NAME as GEO_RULE_NAME
from spark.iceberg_tables import transactions_schema
from spark.landing import ALERT_KEY, TRANSACTION_KEY, _ml_alert_rows, _transaction_row
from spark.model_contract import FraudModel, ModelManifest
from spark.scoring import score_transactions
from spark.snowflake_merge import BatchWriter, RunSql, StageBatch, build_merge_writer
from spark.structuring import RULE_NAME as STRUCTURING_RULE_NAME
from spark.velocity import RULE_NAME as VELOCITY_RULE_NAME

TRANSACTION_QUERY_NAME = "snowflake_transactions_landing"
STAGING_TRANSACTIONS = "STG_TRANSACTIONS_RECENT"
STAGING_ALERTS_GEO = "STG_ALERTS_GEO"
STAGING_ALERTS_STRUCTURING = "STG_ALERTS_STRUCTURING"
STAGING_ALERTS_ML = "STG_ALERTS_ML"
STAGING_ALERTS_VELOCITY = "STG_ALERTS_VELOCITY"
_ALERT_JSON_COLUMN = (
    TRANSACTION_IDS,
    DETAILS,
)  # this can be serialsied into json so they can be queried by snowflake


# used to idenity the incoming data into snowflake
def snowflake_query_name(rule_name: str) -> str:
    return f"snowflake_{landing_query_name(rule_name)}"


# serialise the alerts into json string due to them being in an array
def _alerts_for_snowflake(alerts: DataFrame) -> DataFrame:
    return alerts.withColumn(TRANSACTION_IDS, F.to_json(F.col(TRANSACTION_IDS)))


# create a function that knows how alerts lands in snowflake tables
def _build_alerts_merge_writer(
    staging_table: str,
    config: DetectionConfig,
    stage_batch: StageBatch,
    run_sql: RunSql,
) -> BatchWriter:

    return build_merge_writer(
        config.snowflake_alerts_table,  # the table where the alerts will land
        staging_table,  # temporary table name
        ALERT_KEY,  # id
        ALERT_COLUMNS,  # holds the column names
        stage_batch,
        run_sql,
        json_columns=_ALERT_JSON_COLUMN,  # which conatins the serialised tranasaction_ids
    )


# used to write the data of alerts into snowflake tbales
def build_snowflake_alerts_writer(
    normalizer: AlertNormalizer,
    staging_table: str,
    config: DetectionConfig,
    stage_batch: StageBatch,
    run_sql: RunSql,
) -> BatchWriter:

    merge_alerts = _build_alerts_merge_writer(
        staging_table, config, stage_batch, run_sql
    )

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        merge_alerts(_alerts_for_snowflake(normalizer(batch)), batch_id)

    return write_batch


# used to merge the scored transactions and ML alerts they raise form such trnasactions
def build_snowflake_transactions_writer(
    manifest: ModelManifest,
    model: FraudModel,
    config: DetectionConfig,
    stage_batch: StageBatch,
    run_sql: RunSql,
) -> BatchWriter:

    merge_transactions = build_merge_writer(
        config.snowflake_transactions_table,
        STAGING_TRANSACTIONS,
        TRANSACTION_KEY,
        transactions_schema().names,  # ensure all trnasctions columns match the schema name derived
        stage_batch,
        run_sql,
    )

    merge_alerts = _build_alerts_merge_writer(
        STAGING_ALERTS_ML, config, stage_batch, run_sql
    )

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        scored = score_transactions(batch, manifest, model)
        rows = _transaction_row(scored, manifest).persist()
        # used to standardise the row rules needed to land in snowflake
        try:
            merge_transactions(rows, batch_id)  # merge the transactions into snowflake
            ml_alerts = _ml_alert_rows(rows, manifest)  # extract the fraud alerts data
            merge_alerts(_alerts_for_snowflake(ml_alerts), batch_id)

        finally:
            rows.unpersist()  # clear the cache

    return write_batch


# used to stream the batchwiter data  built
def _start_landing_query(
    frame: DataFrame, query_name: str, writer: BatchWriter, config: DetectionConfig
) -> StreamingQuery:
    return (
        frame.writeStream.queryName(query_name)
        .outputMode("append")
        .foreachBatch(writer)
        .option("checkpointLocation", f"{config.checkpoint_location}/{query_name}")
        .trigger(processingTime=spark_interval(config.trigger_interval))
        .start()
    )


# used to stream the alerts only
def _start_alerts_landing_query(
    alerts: DataFrame,
    rule_name: str,
    normalize: AlertNormalizer,
    staging_table: str,
    config: DetectionConfig,
    stage_batch: StageBatch,
    run_sql: RunSql,
) -> StreamingQuery:
    writer = build_snowflake_alerts_writer(
        normalize, staging_table, config, stage_batch, run_sql
    )

    return _start_landing_query(alerts, snowflake_query_name(rule_name), writer, config)


# main function to stream the transaction to snowflake
def start_snowflake_transactions_landing_query(
    transactions: DataFrame,
    manifest: ModelManifest,
    model: FraudModel,
    config: DetectionConfig,
    stage_batch: StageBatch,
    run_sql: RunSql,
) -> StreamingQuery:
    writer = build_snowflake_transactions_writer(
        manifest, model, config, stage_batch, run_sql
    )

    return _start_landing_query(transactions, TRANSACTION_QUERY_NAME, writer, config)


# used to land the structuring alerts(sumrfing pattern) for anti  money laudnering
def start_snowflake_structuring_landing_query(
    alerts: DataFrame, config: DetectionConfig, stage_batch: StageBatch, run_sql: RunSql
) -> StreamingQuery:

    return _start_alerts_landing_query(
        alerts,
        STRUCTURING_RULE_NAME,
        normalize_structuring_alerts,
        STAGING_ALERTS_STRUCTURING,
        config,
        stage_batch,
        run_sql,
    )


# used to land the velocity parmaeters in snowflake
def start_snowflake_velocity_landing_query(
    alerts: DataFrame,
    config: DetectionConfig,
    stage_batch: StageBatch,
    run_sql: RunSql,
) -> StreamingQuery:
    return _start_alerts_landing_query(
        alerts,
        VELOCITY_RULE_NAME,
        normalize_velocity_alerts,
        STAGING_ALERTS_VELOCITY,
        config,
        stage_batch,
        run_sql,
    )


# used to land the geo parmaters to snowflake
def start_snowflake_geo_landing_query(
    alerts: DataFrame,
    config: DetectionConfig,
    stage_batch: StageBatch,
    run_sql: RunSql,
) -> StreamingQuery:
    return _start_alerts_landing_query(
        alerts,
        GEO_RULE_NAME,
        normalize_travel_alerts,
        STAGING_ALERTS_GEO,
        config,
        stage_batch,
        run_sql,
    )
