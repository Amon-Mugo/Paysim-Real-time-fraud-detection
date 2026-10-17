# used to define the kafka schema and convert it into spark and the json
# any wrong events are flagged and kept rather than dropped

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

EVENT_TIME_FIELD = (
    "event_time"  # used to hold a timestamp actual field name  for tracking
)
_PARSED_COLUMN = "parsed"  # hold incoming json to be converted to dataframe

# used to map the json of kafka into proper spark based form generator/schema.py
TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField(EVENT_TIME_FIELD, StringType()),
        StructField("transaction_type", StringType()),
        StructField("amount", DoubleType()),
        StructField("name_orig", StringType()),
        StructField("old_balance_orig", DoubleType()),
        StructField("new_balance_orig", DoubleType()),
        StructField("name_dest", StringType()),
        StructField("old_balance_dest", DoubleType()),
        StructField("new_balance_dest", DoubleType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
    ]
)


# used to loop throug the transaction_schema and build a proper column with proper dates casted into it and
# also a temprary column
def _event_columns() -> list[Column]:
    columns = []
    for field in TRANSACTION_SCHEMA.fields:
        column = F.col(f"{_PARSED_COLUMN}.{field.name}")
        # have  a temporary spark column we can cast dates event_time field

        if field.name == EVENT_TIME_FIELD:
            column = F.expr(
                f"try_cast({_PARSED_COLUMN}.{field.name} as timestamp)"
            )  # this converts the evnt_time into proper timestamp

        columns.append(column.alias(field.name))
    return columns


# used as the main function to convert into dataframe and deserialisation
def parse_transactions(kafka_df: DataFrame) -> DataFrame:
    parsed = kafka_df.select(
        F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias(
            _PARSED_COLUMN
        ),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    return parsed.select(
        *_event_columns(), "kafka_partition", "kafka_offset", "kafka_timestamp"
    ).withColumn(
        "is_valid_event",
        F.col("transaction_id").isNotNull() & F.col(EVENT_TIME_FIELD).isNotNull(),
    )
