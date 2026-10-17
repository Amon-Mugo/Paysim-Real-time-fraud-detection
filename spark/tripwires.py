# used as a monitoring tool where we check if there is any data loss or
# duplicates in the pipeline that is always running and checks stream health

import logging
import threading
from pyspark.sql import Window
from typing import Dict, Optional, Callable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from datetime import datetime

logger = logging.getLogger(__name__)

MONITORING_QUERY_NAME = "tripwire_monitor"
TRANSACTION_ID_FIELD = "transaction_id"  # primary column for uniqueness


# used to track the watermark logs for every query and checks event parity
class BatchParityLedger:
    def __init__(self, max_history: int = 1000) -> None:
        self._lock = threading.Lock()  # used to prevent multiple backgqround threads
        self._parsed_rows: Dict[int, int] = (
            {}
        )  # used to hold the batch_id and row_count
        self._max_history = max_history

    # usde to record the batch_id and row_count
    def record(self, batch_id: int, row_count: int) -> None:
        with self._lock:  # ensures threads are evaluated at a time
            self._parsed_rows[batch_id] = row_count

            if len(self._parsed_rows) > self._max_history:
                oldest_keys = sorted(self._parsed_rows)[
                    : -self._max_history
                ]  # enables us to know the exeeded the limit

                for key in oldest_keys:
                    self._parsed_rows.pop(
                        key, None
                    )  # this drops the exceed batch_id key

    # used to delete row_count and fetch the batch_id

    def pop(self, batch_id: int) -> Optional[int]:
        with self._lock:
            return self._parsed_rows.pop(
                batch_id, None
            )  # know we have only the batch_id no row count


# used to convert the pyspark time into proper python time
def _parse_progress_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# used to count duplicates transaction_ids and count the exceeded rows
def _count_duplicate_transaction_ids(batch: DataFrame) -> int:
    window_spec = Window.partitionBy(TRANSACTION_ID_FIELD).orderBy(
        F.monotonically_increasing_id()
    )
    return (
        batch.filter(F.col(TRANSACTION_ID_FIELD).isNotNull())
        .withColumn("row_n", F.row_number().over(window_spec))
        .filter(F.col("row_n") > 1)
        .count()
    )


# used as a factory function that creates and returns another function due to .foreBatch method reuires
# (Dataframe,int ) for distributed cmputing
def build_integrity_check(
    ledger: BatchParityLedger,
) -> Callable[[DataFrame, int], None]:
    # function that records counts and flags duplicates rows

    def check_batch(batch: DataFrame, batch_id: int) -> None:
        # used for forebatch signature
        batch.persist()  # enable spark not to re-read everything to overuse compute
        try:
            row_count = batch.count()  # count the micro-batch
            duplicate_rows = _count_duplicate_transaction_ids(batch)
        finally:
            batch.unpersist()  # clean spark memory

        ledger.record(batch_id, row_count)

        if duplicate_rows:
            logger.error(
                "Tripwire duplicate_transaction_ids: batch=%s redundant_rows=%s",
                batch_id,
                duplicate_rows,
            )
        else:
            logger.info(
                "Tripwire duplicate_transaction_ids: batch=%s ok rows=%s",
                batch_id,
                row_count,
            )

    return check_batch


# used as background monitoring agent that inheritts streamquery listerner that runs in the background and acts as an observer and does
# not stop the pipeline is an error in adds a watermark for any logs created and even count parity check
class TripwireListener(StreamingQueryListener):
    def __init__(self, ledger: BatchParityLedger) -> None:
        self._ledger = ledger  # accepts the class

    # parity checks
    def _check_parity(self, batch_id: int, source_rows: int) -> None:
        parsed_rows = self._ledger.pop(batch_id)  # know we are left with batch_id

        if parsed_rows is None:
            logger.warning(
                "Triperwire event_count_parity: batch =%s no parsed_count recorded",
                batch_id,
            )

        elif parsed_rows != source_rows:
            logger.warning(
                "Tripwire event_count_parity:batch =%s source_rows=%s parsed_rows=%s",
                batch_id,
                source_rows,
                parsed_rows,
            )
        else:
            logger.info(
                "Tripwire event_count_parity batch=%s ok rows=%s", batch_id, source_rows
            )

    # used to add watermark lag for message delay and memory management
    def _log_watermark_delay(
        self, query_name: str, batch_id: int, event_time: dict
    ) -> None:
        max_event_time = event_time.get(
            "max"
        )  # this is the time of the newest stream in the micro batch
        watermark = event_time.get("watermark")

        if (
            not max_event_time or not watermark or watermark.startswith("1970-01-01")
        ):  # the first micro batch must wait for the next micro batch to get watermark
            return

        lag_seconds = (
            _parse_progress_timestamp(max_event_time)
            - _parse_progress_timestamp(watermark)
        ).total_seconds()  # convert into seconds
        logger.info(
            "Tripwire watermark_lag: query=%s batch=%s max_event_time=%s watermark=%s lag_seconds=%.0f",
            query_name,
            batch_id,
            max_event_time,
            watermark,
            lag_seconds,
        )

    # used to automatically tiggered by spark the exact moment any streaming query kicks in
    def onQueryStarted(self, event):
        logger.info("Query started:name=%s id=%s", event.name, event.id)

    # used to automatically trigged every evnt single time a micro batch finishes processing across any query
    def onQueryProgress(self, event):
        progress = event.progress
        if progress.name == MONITORING_QUERY_NAME:
            self._check_parity(progress.batchId, progress.numInputRows)
        self._log_watermark_delay(progress.name, progress.batchId, progress.eventTime)
        # used to track watermark lag health

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        if event.exception:
            logger.error(
                "Query terminated with error: id=%s %s", event.id, event.exception
            )

        else:
            logger.info("Query terminated:id=%s", event.id)
