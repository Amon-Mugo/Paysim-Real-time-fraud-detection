# used as a fraud alert for the velocity detectors (structuring velocity,travel ml) and reshapes them into a single
# layout alert_id and maps the output onto one common alert layout

from __future__ import annotations
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from spark.schema import EVENT_TIME_FIELD

ALERT_ID = "alert_id"
RULE_NAME = "rule_name"
ACCOUNT = "name_orig"
ALERT_TIME = "alert_time"
TRANSACTION_IDS = "transaction_ids"
DETAILS = "details"

ALERT_COLUMNS = (ALERT_ID, RULE_NAME, ACCOUNT, ALERT_TIME, TRANSACTION_IDS, DETAILS)

_KEY_SEPARATIONS = "|"  # separator for the concertination process
_HASH_BITS = 256  # length of crytograpgh which is 256 bit


# used to cast the timezones into microsends so it the timezone varries it all the same
def _epoch_micros(timestamp: Column) -> Column:
    return F.unix_micros(timestamp).cast("string")


# applies hashing to each column to create a unique identifier due to spark streaming can have duplicated data or itteration
# this prevents that
def _build_alert_id(*key_parts: Column) -> Column:

    sting_parts = [
        part.cast("string") for part in key_parts
    ]  # this cast sting to each column
    # this ensures concertination

    return F.sha2(
        F.concat_ws(_KEY_SEPARATIONS, *sting_parts), _HASH_BITS
    )  # performs concatination and cryptography


# used as a standerdisation wrapper foradding incoming data and new column names
def _to_common_layout(
    alerts: DataFrame,
    alert_id: Column,
    alert_time: Column,
    transaction_ids: Column,
    details: Column,
) -> DataFrame:

    return alerts.select(
        alert_id.alias(ALERT_ID),
        F.col(RULE_NAME),
        F.col(ACCOUNT),
        alert_time.alias(ALERT_TIME),
        transaction_ids.alias(TRANSACTION_IDS),
        F.to_json(details).alias(DETAILS),
    )


# used for reusable normalization logic for time windowed rules and account
def _normalize_windowed(
    alerts: DataFrame, count_column: str, total_column: str
) -> DataFrame:

    return _to_common_layout(
        alerts,
        alert_id=_build_alert_id(  # passes the cryptogrphic rule
            F.col(RULE_NAME),
            F.col(ACCOUNT),
            _epoch_micros(F.col("window_start")),
            _epoch_micros(F.col("window_end")),
        ),
        alert_time=F.col("window_end"),
        transaction_ids=F.col(TRANSACTION_IDS),
        details=F.struct(
            F.col(count_column).alias("transaction_count"),
            F.col(total_column).alias("total_amount"),
            F.col("window_start"),
            F.col("window_end"),
        ),
    )


# used as a function for structuring alerts for one account per window
def normalize_structuring_alerts(alerts: DataFrame) -> DataFrame:
    return _normalize_windowed(alerts, "band_transaction_count", "band_total_amount")


# used for velocty function
def normalize_velocity_alerts(alerts: DataFrame) -> DataFrame:
    return _normalize_windowed(alerts, "transaction_count", "total_amount")


# ued normalise the ml alerts
def normalize_ml_alerts(alerts: DataFrame) -> DataFrame:
    return _to_common_layout(
        alerts,
        alert_id=_build_alert_id(
            F.col(RULE_NAME), F.col("transaction_id"), F.col("model_version")
        ),
        alert_time=F.col(EVENT_TIME_FIELD),
        transaction_ids=F.array(F.col("transaction_id")),
        details=F.struct(
            F.col("amount"),
            F.col("fraud_score"),
            F.col("model_version"),
        ),
    )


# used to noramlise the geo rule
def normalize_travel_alerts(alerts: DataFrame) -> DataFrame:
    return _to_common_layout(
        alerts,
        alert_id=_build_alert_id(F.col(RULE_NAME), F.col("transaction_id")),
        alert_time=F.col(EVENT_TIME_FIELD),
        transaction_ids=F.array(F.col("transaction_id")),
        details=F.struct(
            F.col("previous_transaction_id"),
            F.col("previous_event_time"),
            F.col("distance_km"),
            F.col("speed_kmh"),
        ),
    )
