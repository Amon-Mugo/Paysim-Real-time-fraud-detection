from __future__ import annotations
from datetime import timedelta,timezone,datetime
import os
from pathlib import Path
from dataclasses import dataclass

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PAYSIM_STEP_DURATION = timedelta(hours=1)

_DEFAULT_CSV_PATH = PROJECT_ROOT/"data"/"paysim_dataset.csv"
_DEFAULT_GROUND_TRUTH_PATH = PROJECT_ROOT/"data"/"ground_truth.jsonl"
_DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
_DEFAULT_KAFKA_TOPIC = "transactions"
_DEFAULT_SIMULATION_START= datetime(2026,1,1,tzinfo=timezone.utc)
_DEFAULT_REPLAY_SPEEDUP = 3600.0

@dataclass(frozen=True)
class GeneratorConfig:
    csv_path: Path=_DEFAULT_CSV_PATH
    kafka_bootstrap_servers: str=_DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    kafka_topic: str=_DEFAULT_KAFKA_TOPIC
    simulation_start: datetime=_DEFAULT_SIMULATION_START
    replay_speedup: float=_DEFAULT_REPLAY_SPEEDUP
    ground_truth_path: Path=_DEFAULT_GROUND_TRUTH_PATH

    def __post_init__(self)-> None:
        if self.simulation_start.tzinfo is None:
            raise ValueError("simulation_start must be in timezone-aware")

        if self.replay_speedup <=0:
            raise ValueError("replay_speed_up must be greater than zero")
        if not self.kafka_bootstrap_servers.strip():
            raise ValueError("kafka_bootstrap_servers must not be empty")
        if not self.kafka_topic.strip():
            raise ValueError("kafka_topic must not be empty")

    @classmethod
    def from_env(cls)-> GeneratorConfig:
        start_raw = os.environ.get("GENERATOR_SIMULATION_START")
        if start_raw:
           parsed = datetime.fromisoformat(start_raw) #used to read the whole time specified
           simulation_start = (parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed)
        else:
            simulation_start =_DEFAULT_SIMULATION_START

        return cls(
                csv_path=Path(os.environ.get("GENERATOR_CSV_PATH", _DEFAULT_CSV_PATH)),
                kafka_bootstrap_servers=os.environ.get(
                    "KAFKA_BOOTSTRAP_SERVERS", _DEFAULT_KAFKA_BOOTSTRAP_SERVERS
                ),
                kafka_topic=os.environ.get("KAFKA_TOPIC", _DEFAULT_KAFKA_TOPIC),
                simulation_start=simulation_start,
                replay_speedup=float(
                    os.environ.get("GENERATOR_REPLAY_SPEEDUP", _DEFAULT_REPLAY_SPEEDUP)
                ),
                ground_truth_path=Path(
                    os.environ.get(
                        "GENERATOR_GROUND_TRUTH_PATH", _DEFAULT_GROUND_TRUTH_PATH
                    )
                ),
            )

config = GeneratorConfig.from_env()


