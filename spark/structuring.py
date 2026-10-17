# used for AML detection and used to detect a financlial pattern crime making multiple transactions
# below the threshold

from typing import Optional
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from spark.schema import EVENT_TIME_FIELD

RULE_NAME = "structuring"


# used to valodate the configuration input
def _validate_parameters(
    reposting_threshold: float, band_fraction: float, min_transactions: int
) -> None:
    if reposting_threshold < 0:
        raise ValueError("reposting_threshold must be positive")
    if not 0 < band_fraction < 1:
        raise ValueError("band_transaction must be between 0 and 1")
    if min_transactions < 2:
        raise ValueError("min_transactions must be atleast 2")


# used to infore the rule whether a transaction amount falls into
# the suspisous category
def _in_structuring_band(
    amount: Column, reporting_threshold: float, band_fraction: float
) -> Column:

    lower_bound = reporting_threshold * (1 - band_fraction)
    # enables us to set a lower bound for the threshold used to flag suspisous transactions and not daily transactions

    return (amount >= F.lit(lower_bound)) & (amount < F.lit(reporting_threshold))


# used as the main function for catching structuring behaviour
def detect_structuring(
    transactions: DataFrame,
    reporting_threshold: float,
    band_fraction: float,
    min_transaction: int,
    window_duration: str,
    watermark_delay: str,
    slides_duration: Optional[str] = None,
) -> DataFrame:

    _validate_parameters(reporting_threshold, band_fraction, min_transaction)

    band_transactions = (
        transactions.filter(
            F.col("is_valid_event")
        )  # used to filter out bad tranactions
        .filter(
            _in_structuring_band(F.col("amount"), reporting_threshold, band_fraction)
        )  # apply band rule
        .withWatermark(EVENT_TIME_FIELD, watermark_delay)
    )

    event_window = F.window(F.col(EVENT_TIME_FIELD), window_duration, slides_duration)

    return (
        band_transactions.groupBy(F.col("name_orig"), event_window.alias("window"))
        .agg(
            F.count("*").alias("band_transaction_count"),
            F.sum("amount").alias("band_total_amount"),
            F.sort_array(F.collect_list("transaction_id")).alias("transaction_ids"),
        )
        .filter(F.col("band_transaction_count") >= min_transaction)
        .select(
            F.lit(RULE_NAME).alias("rule_name"),
            "name_orig",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "band_transaction_count",
            "band_total_amount",
            "transaction_ids",
        )
    )
