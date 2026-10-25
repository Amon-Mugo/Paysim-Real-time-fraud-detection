"""Wiring tests for spark.job.start_detecion_queries, run with fakes only."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from spark import job

SCORED_FRAME = object()
MANIFEST = SimpleNamespace(model_version="test-v1")


def _build_config() -> SimpleNamespace:
    return SimpleNamespace(
        structuring_min_amount=9_000.0,
        structuring_max_amount=10_000.0,
        structuring_min_transfers=3,
        structuring_window=timedelta(hours=1),
        structuring_slide=timedelta(minutes=15),
        velocity_min_transactions=5,
        velocity_window=timedelta(minutes=5),
        velocity_slide=timedelta(minutes=1),
        geo_max_speed_kmh=900.0,
        geo_min_distance_km=1.0,
        geo_state_ttl=timedelta(minutes=60),
        watermark_delay=timedelta(minutes=10),
        model_dir="unused",
    )


@pytest.fixture
def wiring(monkeypatch):
    events: list[str] = []
    received: dict[str, tuple] = {}

    def _record_start(name: str):
        def start(*args):
            events.append(name)
            received[name] = args
            return f"query:{name}"

        return start

    def _create_tables(spark, config):
        events.append("create_landing_tables")

    monkeypatch.setattr(job, "spark_interval", str)
    monkeypatch.setattr(job, "create_landing_tables", _create_tables)
    monkeypatch.setattr(job, "_read_transactions", lambda spark, config: "stream")
    monkeypatch.setattr(job, "detect_structuring", lambda *a, **k: "structuring")
    monkeypatch.setattr(job, "detect_velocity", lambda *a, **k: "velocity")
    monkeypatch.setattr(job, "detect_impossible_travel", lambda *a, **k: "geo")
    monkeypatch.setattr(job, "load_model", lambda path: (MANIFEST, "model"))
    monkeypatch.setattr(job, "score_transactions", lambda *a: SCORED_FRAME)
    monkeypatch.setattr(job, "TripwireListener", lambda ledger: "listener")
    for name in (
        "start_structuring_landing_query",
        "start_velocity_landing_query",
        "start_geo_landing_query",
        "start_transactions_landing_query",
        "_start_tripwire_query",
    ):
        monkeypatch.setattr(job, name, _record_start(name))

    spark = MagicMock()
    config = _build_config()
    queries = job.start_detecion_queries(spark, config)
    return SimpleNamespace(
        events=events, received=received, queries=queries, spark=spark, config=config
    )


def test_starts_four_landing_queries_and_the_tripwire_monitor(wiring):
    assert wiring.queries == [
        "query:start_structuring_landing_query",
        "query:start_velocity_landing_query",
        "query:start_geo_landing_query",
        "query:start_transactions_landing_query",
        "query:_start_tripwire_query",
    ]


def test_landing_tables_are_created_before_any_query_starts(wiring):
    assert wiring.events[0] == "create_landing_tables"
    assert wiring.events.count("create_landing_tables") == 1


def test_transactions_landing_query_receives_the_scored_frame(wiring):
    scored, manifest, config = wiring.received["start_transactions_landing_query"]
    assert scored is SCORED_FRAME
    assert manifest is MANIFEST
    assert config is wiring.config


def test_tripwire_listener_is_registered_once(wiring):
    wiring.spark.streams.addListener.assert_called_once_with("listener")
