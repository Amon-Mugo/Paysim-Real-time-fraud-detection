# used in detecting fraudulant accounts based on user location

from typing import Iterator, NamedTuple, Optional

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.rule_logic import haversine_km, implied_speed_kmh, is_impossible_travel
from spark.schema import EVENT_TIME_FIELD

RULE_NAME = "impossible_travel"
_TIMEOUT_CONF = "EventTimeTimeout"
_MILLIS_PER_MINUTE = 60_000

_STATE_SCHEMA = StructType(
    [
        StructField("last_transaction_id", StringType()),
        StructField("last_event_time", TimestampType()),
        StructField("last_latitude", DoubleType()),
        StructField("last_longitude", DoubleType()),
    ]
)

_ALERT_SCHEMA = StructType(
    [
        StructField("rule_name", StringType()),
        StructField("name_orig", StringType()),
        StructField("transaction_id", StringType()),
        StructField(EVENT_TIME_FIELD, TimestampType()),
        StructField("previous_transaction_id", StringType()),
        StructField("previous_event_time", TimestampType()),
        StructField("distance_km", DoubleType()),
        StructField("speed_kmh", DoubleType()),
    ]
)

_ALERT_COLUMNS = [field.name for field in _ALERT_SCHEMA.fields]


class LastSeen(NamedTuple):
    """The most recent location recorded for an account."""

    transaction_id: str
    event_time: pd.Timestamp
    latitude: float
    longitude: float


def _validate_parameters(
    max_speed_kmh: float, min_distance_km: float, state_ttl_minutes: int
) -> None:
    """Reject settings that would make the rule meaningless."""
    if max_speed_kmh <= 0:
        raise ValueError("max_speed_kmh must be positive")
    if min_distance_km < 0:
        raise ValueError("min_distance_km must not be negative")
    if state_ttl_minutes <= 0:
        raise ValueError("state_ttl_minutes must be positive")


def _epoch_millis(timestamp: pd.Timestamp) -> int:
    """Convert a timestamp to epoch milliseconds, treating naive values as UTC."""
    moment = pd.Timestamp(timestamp)
    if moment.tzinfo is None:
        moment = moment.tz_localize("UTC")
    return int(moment.timestamp() * 1000)


def _read_last_seen(state: GroupState) -> Optional[LastSeen]:
    """Return the stored location for the account, or None if there is none."""
    if not state.exists:
        return None
    transaction_id, event_time, latitude, longitude = state.get
    return LastSeen(transaction_id, pd.Timestamp(event_time), latitude, longitude)


def _write_last_seen(
    state: GroupState, last_seen: LastSeen, state_ttl_minutes: int
) -> None:
    """Store the latest location and schedule eviction of idle accounts."""
    state.update(tuple(last_seen))
    timeout_ms = _epoch_millis(last_seen.event_time) + (
        state_ttl_minutes * _MILLIS_PER_MINUTE
    )
    state.setTimeoutTimestamp(max(timeout_ms, state.getCurrentWatermarkMs() + 1))


def _build_alert(account: str, current: LastSeen, previous: LastSeen) -> dict:
    """Describe a flagged movement between two consecutive transactions."""
    distance_km = haversine_km(
        previous.latitude, previous.longitude, current.latitude, current.longitude
    )
    elapsed_seconds = (current.event_time - previous.event_time).total_seconds()
    return {
        "rule_name": RULE_NAME,
        "name_orig": account,
        "transaction_id": current.transaction_id,
        EVENT_TIME_FIELD: current.event_time,
        "previous_transaction_id": previous.transaction_id,
        "previous_event_time": previous.event_time,
        "distance_km": distance_km,
        "speed_kmh": implied_speed_kmh(distance_km, elapsed_seconds),
    }


def _make_travel_tracker(
    max_speed_kmh: float, min_distance_km: float, state_ttl_minutes: int
):
    """Build the per-account function that Spark runs against grouped events."""

    def track_account(
        key: tuple, batches: Iterator[pd.DataFrame], state: GroupState
    ) -> Iterator[pd.DataFrame]:
        if state.hasTimedOut:
            state.remove()
            return

        account = key[0]
        previous = _read_last_seen(state)
        alerts = []

        for batch in batches:
            for row in batch.sort_values(EVENT_TIME_FIELD).itertuples(index=False):
                current = LastSeen(
                    row.transaction_id,
                    pd.Timestamp(getattr(row, EVENT_TIME_FIELD)),
                    row.latitude,
                    row.longitude,
                )
                if previous is not None and current.event_time < previous.event_time:
                    continue
                if previous is not None and is_impossible_travel(
                    previous.latitude,
                    previous.longitude,
                    previous.event_time,
                    current.latitude,
                    current.longitude,
                    current.event_time,
                    max_speed_kmh,
                    min_distance_km,
                ):
                    alerts.append(_build_alert(account, current, previous))
                previous = current

        if previous is not None:
            _write_last_seen(state, previous, state_ttl_minutes)
        yield pd.DataFrame(alerts, columns=_ALERT_COLUMNS)

    return track_account


def detect_impossible_travel(
    transactions: DataFrame,
    max_speed_kmh: float,
    min_distance_km: float,
    watermark_delay: str,
    state_ttl_minutes: int,
) -> DataFrame:
    """Return one alert row per transaction that implies impossible travel.

    Each valid event with a location is compared with the previous event for
    the same originating account. Events older than the last one already seen
    for the account are skipped rather than compared. Accounts idle for longer
    than state_ttl_minutes of event time are dropped from state.
    """
    _validate_parameters(max_speed_kmh, min_distance_km, state_ttl_minutes)

    located = (
        transactions.filter(
            F.col("is_valid_event")
            & F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
        )
        .select(
            "name_orig", "transaction_id", EVENT_TIME_FIELD, "latitude", "longitude"
        )
        .withWatermark(EVENT_TIME_FIELD, watermark_delay)
    )

    return located.groupBy("name_orig").applyInPandasWithState(
        _make_travel_tracker(max_speed_kmh, min_distance_km, state_ttl_minutes),
        outputStructType=_ALERT_SCHEMA,
        stateStructType=_STATE_SCHEMA,
        outputMode="append",
        timeoutConf=_TIMEOUT_CONF,
    )
