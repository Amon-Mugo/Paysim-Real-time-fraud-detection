"""Tests for the Kafka producer configuration in local and MSK IAM modes."""

from __future__ import annotations

from typing import Any

import pytest

from generator import producer as producer_module
from generator.config import GeneratorConfig
from generator.producer import (
    _build_oauth_callback,
    _build_producer_config,
    create_producer,
)

_REGION = "us-east-1"
_EXPIRY_MS = 1_800_000_000_000
_SASL_KEYS = {"security.protocol", "sasl.mechanism", "oauth_cb"}


class _FakeProducer:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.poll_calls: list[float] = []

    def poll(self, timeout: float) -> int:
        self.poll_calls.append(timeout)
        return 0


def _iam_settings() -> GeneratorConfig:
    return GeneratorConfig(kafka_iam_auth=True, aws_region=_REGION)


def test_local_config_has_no_sasl_settings() -> None:
    config = _build_producer_config(GeneratorConfig())

    assert not _SASL_KEYS & config.keys()


def test_iam_config_enables_sasl_oauthbearer() -> None:
    config = _build_producer_config(_iam_settings())

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "OAUTHBEARER"
    assert callable(config["oauth_cb"])


def test_oauth_callback_returns_token_with_expiry_in_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    regions: list[str] = []

    def fake_generate_auth_token(region: str) -> tuple[str, int]:
        regions.append(region)
        return "token-value", _EXPIRY_MS

    monkeypatch.setattr(
        producer_module.MSKAuthTokenProvider,
        "generate_auth_token",
        fake_generate_auth_token,
    )

    token, expiry_seconds = _build_oauth_callback(_REGION)("")

    assert token == "token-value"
    assert expiry_seconds == _EXPIRY_MS / 1000
    assert regions == [_REGION]


def test_create_producer_fetches_first_token_when_iam_is_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(producer_module, "Producer", _FakeProducer)

    producer = create_producer(_iam_settings())

    assert producer.poll_calls == [0]


def test_create_producer_skips_token_fetch_when_iam_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(producer_module, "Producer", _FakeProducer)

    producer = create_producer(GeneratorConfig())

    assert producer.poll_calls == []
