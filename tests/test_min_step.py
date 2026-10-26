"""Tests for the generator's --min-step support."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from generator.config import PAYSIM_STEP_COUNT, PAYSIM_STEP_DURATION, GeneratorConfig
from generator.injectors import plan_injections
from generator.main import _build_parser, _reslove_settings
from generator.paysim_reader import read_paysim

CSV_COLUMNS = [
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
]


def _write_paysim_csv(path: Path, steps: list[int], rows_per_step: int) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for step in steps:
            for index in range(rows_per_step):
                writer.writerow(
                    {
                        "step": step,
                        "type": "TRANSFER",
                        "amount": 100.0,
                        "nameOrig": f"C{step}{index}",
                        "oldbalanceOrg": 500.0,
                        "newbalanceOrig": 400.0,
                        "nameDest": f"M{step}{index}",
                        "oldbalanceDest": 0.0,
                        "newbalanceDest": 100.0,
                        "isFraud": 0,
                    }
                )


def test_default_min_step_replays_from_simulation_start() -> None:
    config = GeneratorConfig()

    assert config.min_step == 1
    assert config.replay_start == config.simulation_start


def test_replay_start_is_offset_by_skipped_steps() -> None:
    config = GeneratorConfig(min_step=3)

    assert config.replay_start == config.simulation_start + 2 * PAYSIM_STEP_DURATION


@pytest.mark.parametrize("invalid", [0, -1, PAYSIM_STEP_COUNT + 1])
def test_min_step_outside_valid_range_is_rejected(invalid: int) -> None:
    with pytest.raises(ValueError, match="min_step"):
        GeneratorConfig(min_step=invalid)


def test_reader_skips_early_steps_and_keeps_full_run_records(tmp_path: Path) -> None:
    csv_path = tmp_path / "paysim.csv"
    _write_paysim_csv(csv_path, steps=[1, 2, 3], rows_per_step=2)

    full = list(read_paysim(GeneratorConfig(csv_path=csv_path)))
    partial = list(read_paysim(GeneratorConfig(csv_path=csv_path, min_step=2)))

    assert [record.step for record in partial] == [2, 2, 3, 3]
    assert partial == full[2:]


def test_injected_events_never_precede_the_replay_start() -> None:
    config = GeneratorConfig(
        min_step=500,
        structuring_scenarios=5,
        geo_anomaly_scenarios=5,
        velocity_scenarios=5,
    )

    events = plan_injections(config)

    assert events
    assert all(event.event_time >= config.replay_start for event in events)


def test_parser_reads_min_step() -> None:
    args = _build_parser().parse_args(["--min-step", "521"])

    assert args.min_step == 521


def test_parser_rejects_zero_min_step() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["--min-step", "0"])


def test_settings_carry_min_step_from_the_flag() -> None:
    args = _build_parser().parse_args(["--min-step", "521"])

    assert _reslove_settings(args).min_step == 521


def test_late_min_step_with_scenarios_is_rejected() -> None:
    config = GeneratorConfig(min_step=PAYSIM_STEP_COUNT, structuring_scenarios=1)

    with pytest.raises(ValueError, match="min_step"):
        plan_injections(config)
