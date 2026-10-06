# used as a ground of truth to isolate the fraudulant data from being streamed by kafka 
# helps us to evaluate the real time detection performance accurately

from __future__ import annotations
from collections.abc import Iterator  
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json
from pathlib import Path
from types import TracebackType  # used for type hints in the exit function
from typing import TextIO

# enum 
class FraudPattern(str, Enum):
    PAYSIM_FRAUD = "paysim_fraud"
    STRUCTURING = "structuring"
    GEO_ANOMALY = "geo_anomaly"

@dataclass(frozen=True)
class GroundTruthEntry:  
    transaction_id: str  # identify transactions
    pattern: FraudPattern  # to catogorise fraud
    event_time: datetime  # time when fraud occured
    scenario_id: str 

    # used to serialise the data into json to be sored into disk and not for kafka to use 
    def to_json(self) -> str: 
        return json.dumps(
            {
                "transaction_id": self.transaction_id,
                "pattern": self.pattern.value,
                "event_time": self.event_time.isoformat(),
                "scenario_id": self.scenario_id,
            },
            separators=(",", ":"),
        )
    
    @classmethod  # used for deserialisation to build a new dataclass
    def from_json(cls, line: str) -> GroundTruthEntry:
        data = json.loads(line)  # FIXED: json.loads() for string parsing (was json.load)

        return cls(
            transaction_id=data["transaction_id"],
            pattern=FraudPattern(data["pattern"]),  
            scenario_id=data["scenario_id"],
            event_time=datetime.fromisoformat(data["event_time"]),  # FIXED: datetime.fromisoformat() for ISO strings
        )

# file management for to_json
class GroundTruthWriter:  
    def __init__(self, path: Path) -> None:
        self._path = path
        self._handle: TextIO | None = None

    def __enter__(self) -> GroundTruthWriter:  
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self._path.open("w", encoding="utf-8", buffering=1)  # enables to display the message
        return self

    def __exit__(
        self, 
        exc_type: type[BaseException] | None, 
        exc_value: BaseException | None, 
        traceback: TracebackType | None
    ) -> None:
        if self._handle is not None:  # checks if the open file is open and still exists or not
            self._handle.close()  # closes the file 
            self._handle = None  # resets the file back to none so class knows it is no longer active 

    # used to write the data in the serialised raw json
    def write(self, entry: GroundTruthEntry) -> None:
        if self._handle is None:
            raise RuntimeError("GroundTruthWriter must be used as a context manager")

        self._handle.write(entry.to_json() + "\n")


# this is used to read and deserialsie the groundtruth dataset from disk to python object and loads 
# data one at a time

def read_ground_truth(path: Path) -> Iterator[GroundTruthEntry]:
    if not path.is_file():
        return

    with path.open(encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                yield GroundTruthEntry.from_json(stripped)