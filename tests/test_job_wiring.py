"""Wiring tests for spark.job.start_detecion_queries, run with fakes only."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from spark import job

STREAM_FRAME = SimpleNamespace(columns=[])
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
        snowflake_account="",
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
    monkeypatch.setattr(job, "_read_transactions", lambda spark, config: STREAM_FRAME)
    monkeypatch.setattr(job, "detect_structuring", lambda *a, **k: "structuring")
    monkeypatch.setattr(job, "detect_velocity", lambda *a, **k: "velocity")
    monkeypatch.setattr(job, "detect_impossible_travel", lambda *a, **k: "geo")
    monkeypatch.setattr(job, "load_model", lambda path: (MANIFEST, "model"))
    monkeypatch.setattr(job, "check_feature_compatibility", lambda *a: None)
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


def test_transactions_landing_query_receives_the_stream_and_model(wiring):
    transactions, manifest, model, config = wiring.received[
        "start_transactions_landing_query"
    ]
    assert transactions is STREAM_FRAME
    assert model == "model"
    assert manifest is MANIFEST
    assert config is wiring.config


def test_tripwire_listener_is_registered_once(wiring):
    wiring.spark.streams.addListener.assert_called_once_with("listener")


_SNOWFLAKE_STARTERS = (
    "start_snowflake_structuring_landing_query",
    "start_snowflake_velocity_landing_query",
    "start_snowflake_geo_landing_query",
    "start_snowflake_transactions_landing_query",
)


def _config_with_account(account: str) -> SimpleNamespace:
    config = _build_config()
    config.snowflake_account = account
    return config


def _start_snowflake(config: SimpleNamespace) -> list:
    return job._start_snowflake_queries(
        "structuring", "velocity", "geo", STREAM_FRAME, MANIFEST, "model", config
    )


@pytest.fixture
def snowflake_starts(monkeypatch):
    received: dict[str, tuple] = {}
    build_calls: list = []
    sinks = ("stage_batch", "run_sql")

    def _build_sinks(config):
        build_calls.append(config)
        return sinks

    def _record_start(name: str):
        def start(*args):
            received[name] = args
            return f"query:{name}"

        return start

    monkeypatch.setattr(job, "build_snowflake_sinks", _build_sinks)
    for name in _SNOWFLAKE_STARTERS:
        monkeypatch.setattr(job, name, _record_start(name))
    return SimpleNamespace(received=received, build_calls=build_calls, sinks=sinks)


def test_snowflake_queries_are_skipped_without_an_account(snowflake_starts):
    queries = _start_snowflake(_config_with_account(""))

    assert queries == []
    assert snowflake_starts.build_calls == []
    assert snowflake_starts.received == {}


def test_snowflake_queries_start_in_landing_order_with_an_account(snowflake_starts):
    queries = _start_snowflake(_config_with_account("xy12345.us-east-1"))

    assert queries == [f"query:{name}" for name in _SNOWFLAKE_STARTERS]


def test_snowflake_sinks_are_built_once_and_shared(snowflake_starts):
    config = _config_with_account("xy12345.us-east-1")

    _start_snowflake(config)

    assert snowflake_starts.build_calls == [config]
    for args in snowflake_starts.received.values():
        assert args[-2:] == snowflake_starts.sinks


def test_each_snowflake_alert_query_receives_its_own_frame(snowflake_starts):
    config = _config_with_account("xy12345.us-east-1")
    sinks = snowflake_starts.sinks

    _start_snowflake(config)

    received = snowflake_starts.received
    assert received["start_snowflake_structuring_landing_query"] == (
        "structuring",
        config,
        *sinks,
    )
    assert received["start_snowflake_velocity_landing_query"] == (
        "velocity",
        config,
        *sinks,
    )
    assert received["start_snowflake_geo_landing_query"] == ("geo", config, *sinks)


def test_snowflake_transactions_query_receives_the_stream_and_model(snowflake_starts):
    config = _config_with_account("xy12345.us-east-1")

    _start_snowflake(config)

    assert snowflake_starts.received["start_snowflake_transactions_landing_query"] == (
        STREAM_FRAME,
        MANIFEST,
        "model",
        config,
        *snowflake_starts.sinks,
    )
