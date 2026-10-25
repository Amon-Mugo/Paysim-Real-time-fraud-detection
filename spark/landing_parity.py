# used in the etl pipeline in batch processing in identifying the duplicated data
# used in cheking the primary keys only

from __future__ import annotations
from collections.abc import Sequence
import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


# used in checking any missing columsn and count them and group them
def _count_missing_keys(
    rows: DataFrame, target_table: str, key_columns: Sequence[str]
) -> int:
    keys = list(key_columns)
    batch_keys = rows.select(*keys).dropna().dropDuplicates()
    landed_keys = rows.sparkSession.table(target_table).select(*keys)
    return batch_keys.join(landed_keys, on=keys, how="left_anti").count()


# used in verification of the orchestration
def check_landing_parity(
    rows: DataFrame, target_table: str, key_columns: Sequence[str], batch_id: int
) -> int:
    missing = _count_missing_keys(rows, target_table, key_columns)
    if missing:
        logger.error(
            "Tripwire landing_parity: table=%s batch=%s missing_keys=%s",
            target_table,
            batch_id,
            missing,
        )

    else:
        logger.info(
            "Tripwire landing_parity: table=%s batch=%s ok", target_table, batch_id
        )

    return missing
