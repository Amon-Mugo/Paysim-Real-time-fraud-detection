# used to by spark structured streaming in order to build the alerts for velocity,geo
# into streaming them into apache iceberg

from __future__ import annotations
from collections.abc import Callable
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from spark.alert_layout import (
    normalize_structuring_alerts,
    normalize_travel_alerts,
    normalize_velocity_alerts,
)
from spark.config import DetectionConfig, spark_interval
from spark.geo import RULE_NAME as GEO_RULE_NAME
from spark.iceberg_merge import BatchWrite, merge_batch
from spark.iceberg_tables import _qualified_name
from spark.landing import ALERT_KEY
from spark.structuring import RULE_NAME as STRUCTURING_RULE_NAME
from spark.velocity import RULE_NAME as VELOCITY_RULE_NAME

AlertNormalizer = Callable[
    [DataFrame], DataFrame
]  # used for incoming data and the returned transformed data
_LANDING_SUFFIX = "landing"  # name icoming data


# used to generate a unique identifier id of incoming data
def landing_query_name(rule_name: str) -> str:
    return f"{rule_name}_{_LANDING_SUFFIX}"  # so it becomes geo_landing


# used to write the alerts into apche iceberg in a foreach method
def build_alerts_writer(
    normalize: AlertNormalizer, config: DetectionConfig
) -> BatchWrite:
    alert_table = _qualified_name(config, config.alerts_table)

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        merge_batch(normalize(batch), alert_table, ALERT_KEY)

    return write_batch


# used to start the streaming pipeline
def _start_alerts_landing_query(
    alerts: DataFrame,
    rule_name: str,
    normalize: AlertNormalizer,
    config: DetectionConfig,
) -> StreamingQuery:
    query_name = landing_query_name(rule_name)
    return (
        alerts.writeStream.queryName(query_name)
        .outputMode("append")
        .foreachBatch(build_alerts_writer(normalize, config))
        .option("checkpointLocation", f"{config.checkpoint_location}/{query_name}")
        .trigger(processingTime=spark_interval(config.trigger_interval))
        .start()
    )


# used to start structuring alerts velocity to be placed in iceberg tables
def start_structuring_landing_query(
    alerts: DataFrame, config: DetectionConfig
) -> StreamingQuery:
    return _start_alerts_landing_query(
        alerts, STRUCTURING_RULE_NAME, normalize_structuring_alerts, config
    )


# used for velocity
def start_velocity_landing_query(
    alerts: DataFrame, config: DetectionConfig
) -> StreamingQuery:
    return _start_alerts_landing_query(
        alerts, VELOCITY_RULE_NAME, normalize_velocity_alerts, config
    )


# used for geo
def start_geo_landing_query(
    alerts: DataFrame, config: DetectionConfig
) -> StreamingQuery:
    return _start_alerts_landing_query(
        alerts, GEO_RULE_NAME, normalize_travel_alerts, config
    )
