# used to detect any accounts that originate an unusally high number of transactions within a
# short period of time

from pyspark.sql import DataFrame
from typing import Optional
from pyspark.sql import functions as F
from spark.schema import EVENT_TIME_FIELD

MINIMUM_ALLOWED_TRANSACTIONS = 2
RULE_NAME = "velocity"


def _validate_parameters(min_transactions: int) -> None:
    if min_transactions < MINIMUM_ALLOWED_TRANSACTIONS:
        raise ValueError(
            f"min transactions must be at least {MINIMUM_ALLOWED_TRANSACTIONS}"
        )


def detect_velocity(
    transactions: DataFrame,
    min_transactions: int,
    window_duration: str,
    watermark_delay: str,
    slide_duration: Optional[str] = None,
) -> DataFrame:

    _validate_parameters(min_transactions)
    valid_transactions = transactions.filter(F.col("is_valid_event")).withWatermark(
        EVENT_TIME_FIELD, watermark_delay
    )

    event_window = F.window(F.col(EVENT_TIME_FIELD), window_duration, slide_duration)
    return (
        valid_transactions.groupBy(F.col("name_orig"), event_window.alias("window"))
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.sort_array(F.collect_list("transaction_id")).alias("transaction_ids"),
        )
        .filter(F.col("transaction_count") >= min_transactions)
        .select(
            F.lit(RULE_NAME).alias("rule_name"),
            "name_orig",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "transaction_count",
            "total_amount",
            "transaction_ids",
        )
    )
