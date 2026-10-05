# used to process paysim dataset from streaming fraud-detection pipeline
# used also to load transaction records from one raw to paysim csv file and transfrom them in to orderd 
# streams ,plus added transaction id

from __future__ import annotations

import csv
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from itertools import groupby
from generator.config import PAYSIM_STEP_DURATION, GeneratorConfig
from generator.schema import PAYSIM_ID_PREFIX, build_transaction_id

REQUIRED_COLUMNS = frozenset(
    {
        "step",
        "type",
        "amount",
        "nameOrig",
        "oldbalanceOrg",
        "newbalanceOrig",
        "nameDest",
        "oldbalanceDest",
        "newbalanceDest",
        "isFraud",
    }
)


@dataclass(frozen=True)
class PaySimRecord:
    transaction_id: str
    step: int
    event_time: datetime
    transaction_type: str
    amount: float
    name_orig: str
    old_balance_orig: float
    new_balance_orig: float
    name_dest: str
    old_balance_dest: float
    new_balance_dest: float
    is_fraud: bool


def _validate_header(fieldnames: Sequence[str] | None) -> None:
    missing = REQUIRED_COLUMNS - set(fieldnames or ())
    if missing:
        raise ValueError(f"PaySim CSV is missing columns: {sorted(missing)}")


def _to_record(
    row: dict[str, str], sequence: int, step: int, event_time: datetime
) -> PaySimRecord:
    return PaySimRecord(
        transaction_id=build_transaction_id(PAYSIM_ID_PREFIX, sequence),
        step=step,
        event_time=event_time,
        transaction_type=row["type"],
        amount=float(row["amount"]),
        name_orig=row["nameOrig"],
        old_balance_orig=float(row["oldbalanceOrg"]),
        new_balance_orig=float(row["newbalanceOrig"]),
        name_dest=row["nameDest"],
        old_balance_dest=float(row["oldbalanceDest"]),
        new_balance_dest=float(row["newbalanceDest"]),
        is_fraud=bool(int(row["isFraud"])),
    )


def read_paysim(settings: GeneratorConfig) -> Iterator[PaySimRecord]:
    sequence = 0
    previous_step = -1

    with settings.csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _validate_header(reader.fieldnames)

        for step, rows in groupby(reader, key=lambda row: int(row["step"])):
            if step <= previous_step:
                raise ValueError(f"paysimstep must be in ascending order")
            previous_step = step

            step_rows = list(rows)
            step_start = settings.simulation_start + (step - 1) * PAYSIM_STEP_DURATION #gives paysim a real word duration time 
            spacing = PAYSIM_STEP_DURATION / len(step_rows) #calculates the duration delay of the number of transaction in 1 hour

            for position, row in enumerate(step_rows):
                sequence += 1
                yield _to_record(row, sequence, step, step_start + position * spacing)


def _validate_header (fieldnames: Sequence[str]| None)-> None:
    missing = REQUIRED_COLUMNS - set(fieldnames or ())
    if missing:
        raise ValueError(f"PaySim CSV is missing columns")


def _to_record(
    row: dict[str, str], sequence: int, step: int, event_time: datetime) -> PaySimRecord:
    return PaySimRecord(
        transaction_id=build_transaction_id(PAYSIM_ID_PREFIX, sequence),
        step=step,
        event_time=event_time,
        transaction_type=row["type"],
        amount=float(row["amount"]),
        name_orig=row["nameOrig"],
        old_balance_orig=float(row["oldbalanceOrg"]),
        new_balance_orig=float(row["newbalanceOrig"]),
        name_dest=row["nameDest"],
        old_balance_dest=float(row["oldbalanceDest"]),
        new_balance_dest=float(row["newbalanceDest"]),
        is_fraud=bool(int(row["isFraud"])),
    )






