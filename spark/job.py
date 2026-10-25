# used as the main entry point for the orcherstartion for spark and process the streams one at a time

from pyspark.sql import DataFrame, SparkSession
from typing import List
from pyspark.sql.streaming import StreamingQuery
import pyspark
import logging
from pathlib import Path
from spark.alert_landing import (
    start_geo_landing_query,
    start_structuring_landing_query,
    start_velocity_landing_query,
)
from spark.config import (
    DetectionConfig,
    spark_interval,
)  # used to import the conigaration and time conversion
from spark.geo import detect_impossible_travel  # used to import detection logic
from spark.iceberg_session import iceberg_packages, iceberg_spark_conf
from spark.iceberg_tables import create_landing_tables
from spark.landing import start_transactions_landing_query
from spark.model_loader import load_model
from spark.schema import parse_transactions  # used in decrialisation of the json
from spark.scoring import score_transactions
from spark.structuring import (
    detect_structuring,
)  # used for catching structuring behavour for aml
from spark.tripwires import (
    MONITORING_QUERY_NAME,
    BatchParityLedger,  # holds the actual initialisation and batch_id and row count and config rules
    TripwireListener,  # background monitoring and the actual triperwires
    build_integrity_check,  # checking anly duplicates
)
from spark.velocity import (
    detect_velocity,
)  # any detections of accounts with suspisous behaviour

logger = logging.getLogger(__name__)
APP_NAME = "fraud-aml-detection"  # used to be displayed in UI in spark
SESSION_TIME_ZONE = "UTC"  # gloabal time to be used
LOCAL_SHUFFLE_PARTITION = 4


# set up gloabal logger message due to numerous imported files
def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )


# due to spark running on jvm it need proper configs
def _kafka_package() -> str:
    major_version = int(pyspark.__version__.split(".")[0])
    scala_version = "2.13" if major_version >= 4 else "2.12"
    return (
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{pyspark.__version__}"
    )


# used for structuring a bound for aml detection
def _structuring_band_fraction(config: DetectionConfig) -> float:
    return 1 - config.structuring_min_amount / config.structuring_max_amount


def _geo_state_ttl_minutes(config: DetectionConfig) -> int:
    return int(config.geo_state_ttl.total_seconds() // 60)


# set up spark engine and kafka connection
def build_spark_session(
    config: DetectionConfig, app_name: str = APP_NAME
) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            ",".join([_kafka_package(), *iceberg_packages(config)]),
        )
        .config("spark.sql.session.timeZone", SESSION_TIME_ZONE)
        .config("spark.sql.shuffle.partitions", str(LOCAL_SHUFFLE_PARTITION))
        .config(map=iceberg_spark_conf(config))
        .getOrCreate()
    )


# engine fo kafka connection and how to read the message
def _read_transactions(spark: SparkSession, config: DetectionConfig) -> DataFrame:
    raw_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", config.kafka_starting_offsets)
        .option("maxOffsetsPerTrigger", config.kafka_max_offset_per_trigger)
        .load()
    )
    return parse_transactions(raw_events)


# used in monitoring background data and for data interity and check any nulls
def _start_tripwire_query(
    transactions: DataFrame, ledger: BatchParityLedger, config: DetectionConfig
) -> StreamingQuery:
    return (
        transactions.writeStream.queryName(MONITORING_QUERY_NAME)
        .foreachBatch(build_integrity_check(ledger))
        .option(
            "checkpointLocation",
            f"{config.checkpoint_location}/{MONITORING_QUERY_NAME}",
        )
        .trigger(processingTime=spark_interval(config.trigger_interval))
        .start()
    )


# used as the main orchestration for the pipeline to bind all 4 helper functions
def start_detecion_queries(
    spark: SparkSession, config: DetectionConfig
) -> List[StreamingQuery]:

    watermark_delay = spark_interval(config.watermark_delay)
    # used to get proper watermark delay and tells saprk how long to wait

    create_landing_tables(spark, config)
    transactions = _read_transactions(spark, config)
    # used to connect kafka,load byte streams and parased into structured dataframe

    # used to connect the alerts config
    structuring_alerts = detect_structuring(
        transactions,
        reporting_threshold=config.structuring_max_amount,  # all based on the functions parameters
        band_fraction=_structuring_band_fraction(config),
        min_transaction=config.structuring_min_transfers,
        window_duration=spark_interval(config.structuring_window),
        slides_duration=spark_interval(config.structuring_slide),
        watermark_delay=watermark_delay,
    )

    # velocity used to flag suspisous accounts
    velocity_alerts = detect_velocity(
        transactions,
        min_transactions=config.velocity_min_transactions,
        window_duration=spark_interval(config.velocity_window),
        slide_duration=spark_interval(config.velocity_slide),
        watermark_delay=watermark_delay,
    )

    # used to ckeck any geo alerts based on the accounts
    travel_alerts = detect_impossible_travel(
        transactions,
        max_speed_kmh=config.geo_max_speed_kmh,
        min_distance_km=config.geo_min_distance_km,
        watermark_delay=watermark_delay,
        state_ttl_minutes=_geo_state_ttl_minutes(config),
    )
    # the model loads once at startup; a bad manifest or missing feature fails here
    manifest, model = load_model(Path(config.model_dir))
    logger.info("Loaded model %s", manifest.model_version)
    scored_transactions = score_transactions(transactions, manifest, model)
    ledger = (
        BatchParityLedger()
    )  # used to log the batch id and the row counts of the tripwire
    spark.streams.addListener(
        TripwireListener(ledger)
    )  # used for background monitoring of the pipeline health

    return [
        start_structuring_landing_query(structuring_alerts, config),
        start_velocity_landing_query(velocity_alerts, config),
        start_geo_landing_query(travel_alerts, config),
        start_transactions_landing_query(scored_transactions, manifest, config),
        _start_tripwire_query(transactions, ledger, config),
    ]


# orchestartion entry pint
def main() -> None:
    _configure_logging()
    config = DetectionConfig.from_env()
    logger.info("Starting detection jobs: %s", config)
    spark = build_spark_session(config)
    start_detecion_queries(spark, config)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
