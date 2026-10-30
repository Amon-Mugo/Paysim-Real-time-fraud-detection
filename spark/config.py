# used for bulding a centralised configuration management system for spark structured streaming and
# fraud detection jobs

from __future__ import annotations
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
import os  # used to read environment varriables and import operating system

PROJECT_ROOT = (
    Path(__file__).resolve().parents[1]
)  # used to read the project root of the file of spark in order to read and write files

_DEFAULT_KAFKA_BOOTSTRAP_SERVER = "localhost:9092"  # kafka server for communication
_DEFAULT_KAFKA_TOPIC = "transactions"  # used to specify which message to stream
_DEFAULT_KAFKA_STARTING_OFFSETS = (
    "earliest"  # tells spark when to spark to read the message
)
_DEFAULT_KAFKA_MAX_OFFSETS_PER_TRIGGER = (
    10_000  # used for rate limiting for spark to read the message
)
_DEFAULT_CHECKPOINT_LOCATION = str(
    PROJECT_ROOT / "data" / "checkpoints" / "detection"
)  # specify where spark will store the data internal state and its progress
_DEFAULT_TRIGGER_INTERVAL_SECONDS = 5  # used to process micro batch every 5 seconds
_DEFAULT_WATERMARK_DELAY_MINUTES = (
    15  # watermark any message that delayed for 15 minutes past
)
_DEFAULT_STRUCTURING_MIN_AMOUNT = 850_000.0  # threshholds not depoits
_DEFAULT_STRUCTURING_MAX_AMOUNT = 1_000_000.0
_DEFAULT_STRUCTURING_MIN_TRANSFERS = 3
_DEFAULT_STRUCTURING_WINDOW_MINUTES = (
    120  # how many transfers in a specified amount of time
)
_DEFAULT_STRUCTURING_SLIDE_MINUTES = 15  # step between overlapping structuring windows

_DEFAULT_GEO_MAX_SPEED_KMH = 500.0
_DEFAULT_GEO_MIN_DISTANCE_KM = 1.0
_DEFAULT_GEO_STATE_TTL_MINUTES = 60

# flagging accounts making 10 or more transactions in a 10 mininute window
_DEFAULT_VELOCITY_MIN_TRANSACTIONS = 10
_DEFAULT_VELOCITY_WINDOW_MINUTES = 10
_DEFAULT_VELOCITY_SLIDE_MINUTES = 1  # step between overlapping velocity windows
_DEFAULT_MODEL_DIR = str(PROJECT_ROOT / "data" / "models" / "fraud" / "stub-v1")
_DEFAULT_ICEBERG_CATALOG_NAME = "glue_catalog"
_DEFAULT_ICEBERG_CATALOG_TYPE = "hadoop"
_ICEBERG_CATALOG_TYPES = ("hadoop", "glue")
_DEFAULT_ICEBERG_WAREHOUSE = str(PROJECT_ROOT / "data" / "iceberg" / "warehouse")
_DEFAULT_ICEBERG_DATABASE = "fraud_detection"
_DEFAULT_TRANSACTIONS_TABLE = "transactions"
_DEFAULT_ALERTS_TABLE = "alerts"
_DEFAULT_SNOWFLAKE_ACCOUNT = ""
_DEFAULT_SNOWFLAKE_WAREHOUSE = "FRAUD_WH"
_DEFAULT_SNOWFLAKE_DATABASE = "FRAUD_DETECTION"
_DEFAULT_SNOWFLAKE_SCHEMA = "MONITORING"
_DEFAULT_SNOWFLAKE_ROLE = "FRAUD_WRITER"
_DEFAULT_SNOWFLAKE_SECRET_NAME = "paysim-fraud-pipeline/snowflake"
_DEFAULT_SNOWFLAKE_TRANSACTIONS_TABLE = "TRANSACTIONS_RECENT"
_DEFAULT_SNOWFLAKE_ALERTS_TABLE = "ALERTS"
_SNOWFLAKE_REQUIRED_SETTINGS = (
    "snowflake_warehouse",
    "snowflake_database",
    "snowflake_schema",
    "snowflake_role",
    "snowflake_secret_name",
    "snowflake_transactions_table",
    "snowflake_alerts_table",
)


# used to parse the date into proper time
def spark_interval(duration: timedelta) -> str:
    return f"{int(duration.total_seconds())} seconds"


# we define our config into a proper class
@dataclass(frozen=True)
class DetectionConfig:
    kafka_bootstrap_servers: str = _DEFAULT_KAFKA_BOOTSTRAP_SERVER
    kafka_topic: str = _DEFAULT_KAFKA_TOPIC
    kafka_starting_offsets: str = _DEFAULT_KAFKA_STARTING_OFFSETS
    kafka_max_offset_per_trigger: int = _DEFAULT_KAFKA_MAX_OFFSETS_PER_TRIGGER
    checkpoint_location: str = _DEFAULT_CHECKPOINT_LOCATION
    trigger_interval: timedelta = timedelta(seconds=_DEFAULT_TRIGGER_INTERVAL_SECONDS)
    watermark_delay: timedelta = timedelta(minutes=_DEFAULT_WATERMARK_DELAY_MINUTES)
    structuring_min_amount: float = _DEFAULT_STRUCTURING_MIN_AMOUNT
    structuring_max_amount: float = _DEFAULT_STRUCTURING_MAX_AMOUNT
    structuring_min_transfers: int = _DEFAULT_STRUCTURING_MIN_TRANSFERS
    structuring_window: timedelta = timedelta(
        minutes=_DEFAULT_STRUCTURING_WINDOW_MINUTES
    )
    structuring_slide: timedelta = timedelta(minutes=_DEFAULT_STRUCTURING_SLIDE_MINUTES)

    geo_max_speed_kmh: float = _DEFAULT_GEO_MAX_SPEED_KMH
    geo_min_distance_km: float = _DEFAULT_GEO_MIN_DISTANCE_KM
    geo_state_ttl: timedelta = timedelta(minutes=_DEFAULT_GEO_STATE_TTL_MINUTES)
    velocity_min_transactions: int = _DEFAULT_VELOCITY_MIN_TRANSACTIONS
    velocity_window: timedelta = timedelta(minutes=_DEFAULT_VELOCITY_WINDOW_MINUTES)
    velocity_slide: timedelta = timedelta(minutes=_DEFAULT_VELOCITY_SLIDE_MINUTES)
    model_dir: str = _DEFAULT_MODEL_DIR
    iceberg_catalog_name: str = _DEFAULT_ICEBERG_CATALOG_NAME
    iceberg_catalog_type: str = _DEFAULT_ICEBERG_CATALOG_TYPE
    iceberg_warehouse: str = _DEFAULT_ICEBERG_WAREHOUSE
    iceberg_database: str = _DEFAULT_ICEBERG_DATABASE
    transactions_table: str = _DEFAULT_TRANSACTIONS_TABLE
    alerts_table: str = _DEFAULT_ALERTS_TABLE
    snowflake_account: str = _DEFAULT_SNOWFLAKE_ACCOUNT
    snowflake_warehouse: str = _DEFAULT_SNOWFLAKE_WAREHOUSE
    snowflake_database: str = _DEFAULT_SNOWFLAKE_DATABASE
    snowflake_schema: str = _DEFAULT_SNOWFLAKE_SCHEMA
    snowflake_role: str = _DEFAULT_SNOWFLAKE_ROLE
    snowflake_secret_name: str = _DEFAULT_SNOWFLAKE_SECRET_NAME
    snowflake_transactions_table: str = _DEFAULT_SNOWFLAKE_TRANSACTIONS_TABLE
    snowflake_alerts_table: str = _DEFAULT_SNOWFLAKE_ALERTS_TABLE

    # used for automatic validations of configurations not for the fraud rules implementations
    def __post_init__(self) -> None:
        if not self.kafka_bootstrap_servers.strip():
            raise ValueError("Kafka_bootstrap_servers must not be empty")

        if not self.kafka_topic.strip():
            raise ValueError("Kafka_topic must not be empty")
        if self.kafka_starting_offsets not in ("earliest", "latest"):
            raise ValueError("Kafka_starting_offsets must be 'earliest' or 'latest'")
        if self.kafka_max_offset_per_trigger <= 0:
            raise ValueError("Kafka_max_offset_per_tigger must be positive")
        if not self.checkpoint_location.strip():
            raise ValueError("Checkpoint_location must not be empty")
        if self.trigger_interval <= timedelta(0):
            raise ValueError("trigger_interval must be positive")
        if self.watermark_delay <= timedelta(0):
            raise ValueError("watermark_delay must be positive")
        if self.structuring_window <= timedelta(0):
            raise ValueError("structuring_window must be positive")
        if self.velocity_window <= timedelta(0):
            raise ValueError("velocity_window must be positive")
        if not timedelta(0) < self.structuring_slide <= self.structuring_window:
            raise ValueError(
                "structuring_slide must be positive and no longer than structuring_window"
            )
        if not timedelta(0) < self.velocity_slide <= self.velocity_window:
            raise ValueError(
                "velocity_slide must be positive and no longer than velocity_window"
            )
        if not 0 < self.structuring_min_amount < self.structuring_max_amount:
            raise ValueError("structuring amount must satisy 0<min_amount <max_amount")
        if self.structuring_min_transfers < 2:
            raise ValueError("structuring_min_transfer must be at least 2")
        if self.geo_max_speed_kmh <= 0:
            raise ValueError("geo_max_speed must be positive")
        if self.geo_min_distance_km < 0:
            raise ValueError("geo_min_distance_km must not be negative")
        if self.geo_state_ttl <= timedelta(0):
            raise ValueError("geo_state_ttl must be positive")
        if not self.model_dir.strip():
            raise ValueError("model_dir must not be empty")
        if not self.iceberg_catalog_name.strip():
            raise ValueError("iceberg_catalog_name must not be empty")
        if self.iceberg_catalog_type not in _ICEBERG_CATALOG_TYPES:
            raise ValueError("iceberg_catalog_type must be one of hadoop, glue")
        if not self.iceberg_warehouse.strip():
            raise ValueError("iceberg_warehouse must not be empty")
        if not self.iceberg_database.strip():
            raise ValueError("iceberg_database must not be empty")
        if not self.transactions_table.strip():
            raise ValueError("transactions_table must not be empty")
        if not self.alerts_table.strip():
            raise ValueError("alerts_table must not be empty")
        for name in _SNOWFLAKE_REQUIRED_SETTINGS:
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must not be empty")
        if self.transactions_table == self.alerts_table:
            raise ValueError("transactions_table and alerts_table must differ")
        if self.velocity_min_transactions < 2:
            raise ValueError("velocity_min_transactions must be at least 2")

    @classmethod
    def from_env(cls) -> DetectionConfig:
        return cls(
            kafka_bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", _DEFAULT_KAFKA_BOOTSTRAP_SERVER
            ),
            kafka_topic=os.environ.get("KAFKA_TOPIC", _DEFAULT_KAFKA_TOPIC),
            model_dir=os.environ.get("DETECTION_MODEL_DIR", _DEFAULT_MODEL_DIR),
            snowflake_account=os.environ.get(
                "SNOWFLAKE_ACCOUNT", _DEFAULT_SNOWFLAKE_ACCOUNT
            ),
            snowflake_warehouse=os.environ.get(
                "SNOWFLAKE_WAREHOUSE", _DEFAULT_SNOWFLAKE_WAREHOUSE
            ),
            snowflake_database=os.environ.get(
                "SNOWFLAKE_DATABASE", _DEFAULT_SNOWFLAKE_DATABASE
            ),
            snowflake_schema=os.environ.get(
                "SNOWFLAKE_SCHEMA", _DEFAULT_SNOWFLAKE_SCHEMA
            ),
            snowflake_role=os.environ.get("SNOWFLAKE_ROLE", _DEFAULT_SNOWFLAKE_ROLE),
            snowflake_secret_name=os.environ.get(
                "SNOWFLAKE_SECRET_NAME", _DEFAULT_SNOWFLAKE_SECRET_NAME
            ),
            snowflake_transactions_table=os.environ.get(
                "SNOWFLAKE_TRANSACTIONS_TABLE", _DEFAULT_SNOWFLAKE_TRANSACTIONS_TABLE
            ),
            snowflake_alerts_table=os.environ.get(
                "SNOWFLAKE_ALERTS_TABLE", _DEFAULT_SNOWFLAKE_ALERTS_TABLE
            ),
            iceberg_catalog_name=os.environ.get(
                "ICEBERG_CATALOG_NAME", _DEFAULT_ICEBERG_CATALOG_NAME
            ),
            iceberg_catalog_type=os.environ.get(
                "ICEBERG_CATALOG_TYPE", _DEFAULT_ICEBERG_CATALOG_TYPE
            ),
            iceberg_warehouse=os.environ.get(
                "ICEBERG_WAREHOUSE", _DEFAULT_ICEBERG_WAREHOUSE
            ),
            iceberg_database=os.environ.get(
                "ICEBERG_DATABASE", _DEFAULT_ICEBERG_DATABASE
            ),
            transactions_table=os.environ.get(
                "TRANSACTIONS_TABLE", _DEFAULT_TRANSACTIONS_TABLE
            ),
            alerts_table=os.environ.get("ALERTS_TABLE", _DEFAULT_ALERTS_TABLE),
            kafka_starting_offsets=os.environ.get(
                "KAFKA_STARTING_OFFSETS", _DEFAULT_KAFKA_STARTING_OFFSETS
            ),
            kafka_max_offset_per_trigger=int(
                os.environ.get(
                    "KAFKA_MAX_OFFSETS_PER_TRIGGER",
                    _DEFAULT_KAFKA_MAX_OFFSETS_PER_TRIGGER,
                )
            ),
            checkpoint_location=os.environ.get(
                "DETECTION_CHECKPOINT_LOCATION", _DEFAULT_CHECKPOINT_LOCATION
            ),
            trigger_interval=timedelta(
                seconds=int(
                    os.environ.get(
                        "DETECTION_TRIGGER_INTERVAL_SECONDS",
                        _DEFAULT_TRIGGER_INTERVAL_SECONDS,
                    )
                )
            ),
            watermark_delay=timedelta(
                minutes=int(
                    os.environ.get(
                        "DETECTION_WATERMARK_DELAY_MINUTES",
                        _DEFAULT_WATERMARK_DELAY_MINUTES,
                    )
                )
            ),
            structuring_min_amount=float(
                os.environ.get(
                    "DETECTION_STRUCTURING_MIN_AMOUNT", _DEFAULT_STRUCTURING_MIN_AMOUNT
                )
            ),
            structuring_max_amount=float(
                os.environ.get(
                    "DETECTION_STRUCTURING_MAX_AMOUNT", _DEFAULT_STRUCTURING_MAX_AMOUNT
                )
            ),
            structuring_min_transfers=int(
                os.environ.get(
                    "DETECTION_STRUCTURING_MIN_TRANSFERS",
                    _DEFAULT_STRUCTURING_MIN_TRANSFERS,
                )
            ),
            structuring_window=timedelta(
                minutes=int(
                    os.environ.get(
                        "DETECTION_STRUCTURING_WINDOW_MINUTES",
                        _DEFAULT_STRUCTURING_WINDOW_MINUTES,
                    )
                )
            ),
            structuring_slide=timedelta(
                minutes=int(
                    os.environ.get(
                        "DETECTION_STRUCTURING_SLIDE_MINUTES",
                        _DEFAULT_STRUCTURING_SLIDE_MINUTES,
                    )
                )
            ),
            geo_max_speed_kmh=float(
                os.environ.get(
                    "DETECTION_GEO_MAX_SPEED_KMH", _DEFAULT_GEO_MAX_SPEED_KMH
                )
            ),
            geo_min_distance_km=float(
                os.environ.get(
                    "DETECTION_GEO_MIN_DISTANCE_KM", _DEFAULT_GEO_MIN_DISTANCE_KM
                )
            ),
            geo_state_ttl=timedelta(
                minutes=int(
                    os.environ.get(
                        "DETECTION_GEO_STATE_TTL_MINUTES",
                        _DEFAULT_GEO_STATE_TTL_MINUTES,
                    )
                )
            ),
            velocity_min_transactions=int(
                os.environ.get(
                    "DETECTION_VELOCITY_MIN_TRANSACTIONS",
                    _DEFAULT_VELOCITY_MIN_TRANSACTIONS,
                )
            ),
            velocity_slide=timedelta(
                minutes=int(
                    os.environ.get(
                        "DETECTION_VELOCITY_SLIDE_MINUTES",
                        _DEFAULT_VELOCITY_SLIDE_MINUTES,
                    )
                )
            ),
            velocity_window=timedelta(
                minutes=int(
                    os.environ.get(
                        "DETECTION_VELOCITY_WINDOW_MINUTES",
                        _DEFAULT_VELOCITY_WINDOW_MINUTES,
                    )
                )
            ),
        )
