# used to land each query and reads one stream and writes them in the foreachnbatch with merge
# and provides a real time streaming transactions data and machine learning alerts into
# Apache iceberg table and idempontently

from __future__ import annotations
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from spark.alert_layout import ALERT_ID
from spark.alert_layout import RULE_NAME as RULE_COLUMN
from spark.alert_layout import normalize_ml_alerts
from spark.config import DetectionConfig, spark_interval
from spark.iceberg_merge import BatchWrite, merge_batch
from spark.iceberg_tables import (
    MODEL_VERSION_COLUMN,
    _qualified_name,
    transactions_schema,
)
from spark.model_contract import ModelManifest
from spark.scoring import RULE_NAME as ML_RULE_NAME
from spark.scoring import SCORE_COLUMN

TRANSACTIONS_QUERY_NAME = "transactions_landing"  # used to label incoming data
TRANSACTION_KEY = ["transaction_id"]
ALERT_KEY = [ALERT_ID]


# used to standize and prepare the scored data so it matches the schema and the scored ml model
def _transaction_row(scored: DataFrame, manifest: ModelManifest) -> DataFrame:
    return scored.withColumn(
        MODEL_VERSION_COLUMN, F.lit(manifest.model_version)
    ).select(*transactions_schema().names)


# used to build the fraud ml alerts
def _ml_alert_rows(rows: DataFrame, manifest: ModelManifest) -> DataFrame:
    flagged = rows.filter(F.col(SCORE_COLUMN) >= F.lit(manifest.alert_threshold))
    return normalize_ml_alerts(flagged.withColumn(RULE_COLUMN, F.lit(ML_RULE_NAME)))


# used to build a factory functions that writes the data to be streamed
def build_transactions_writer(
    manifest: ModelManifest, config: DetectionConfig
) -> BatchWrite:

    # used to combine the table and databases
    transaction_tables = _qualified_name(config, config.transactions_table)
    alert_tables = _qualified_name(config, config.alerts_table)

    # used to execute the incoming data
    def write_batch(batch: DataFrame, batch_id: int) -> None:
        rows = _transaction_row(batch, manifest).persist()
        try:
            merge_batch(rows, transaction_tables, TRANSACTION_KEY)
            merge_batch(_ml_alert_rows(rows, manifest), alert_tables, ALERT_KEY)
        finally:
            rows.unpersist()

    return write_batch


# used to start the streaming
def start_transactions_landing_query(
    scored: DataFrame, manifest: ModelManifest, config: DetectionConfig
) -> StreamingQuery:
    return (
        scored.writeStream.queryName(TRANSACTIONS_QUERY_NAME)
        .foreachBatch(build_transactions_writer(manifest, config))
        .option(
            "checkpointLocation",
            f"{config.checkpoint_location}/{TRANSACTIONS_QUERY_NAME}",
        )
        .trigger(processingTime=spark_interval(config.trigger_interval))
        .start()
    )
