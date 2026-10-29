"""Tests for the topic creation helper in local and MSK IAM modes."""

from __future__ import annotations

from concurrent.futures import Future
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import NewTopic

from generator import topic_admin as topic_admin_module
from generator.config import GeneratorConfig
from generator.topic_admin import (
    TOPIC_PARTITIONS,
    _build_admin_config,
    _create_admin_client,
    ensure_topic,
)

_REGION = "us-east-1"
_TOPIC = "transactions"
_SASL_KEYS = {"security.protocol", "sasl.mechanism", "oauth_cb"}


class _FakeAdminClient:
    def __init__(
        self, config: dict[str, Any] | None = None, error: KafkaException | None = None
    ) -> None:
        self.config = config
        self.error = error
        self.poll_calls: list[float] = []
        self.created: list[NewTopic] = []

    def poll(self, timeout: float) -> int:
        self.poll_calls.append(timeout)
        return 0

    def create_topics(
        self, new_topics: list[NewTopic], request_timeout: float
    ) -> dict[str, Future[None]]:
        self.created.extend(new_topics)
        futures: dict[str, Future[None]] = {}
        for new_topic in new_topics:
            future: Future[None] = Future()
            if self.error is None:
                future.set_result(None)
            else:
                future.set_exception(self.error)
            futures[new_topic.topic] = future
        return futures


def _settings() -> GeneratorConfig:
    return GeneratorConfig(kafka_topic=_TOPIC)


def _iam_settings() -> GeneratorConfig:
    return GeneratorConfig(kafka_topic=_TOPIC, kafka_iam_auth=True, aws_region=_REGION)


def test_local_admin_config_has_no_sasl_settings() -> None:
    config = _build_admin_config(_settings())

    assert not _SASL_KEYS & config.keys()


def test_iam_admin_config_enables_sasl_oauthbearer() -> None:
    config = _build_admin_config(_iam_settings())

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "OAUTHBEARER"
    assert callable(config["oauth_cb"])


def test_iam_client_fetches_first_token_up_front(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(topic_admin_module, "AdminClient", _FakeAdminClient)

    admin = _create_admin_client(_iam_settings())

    assert admin.poll_calls == [0]


def test_local_client_does_not_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(topic_admin_module, "AdminClient", _FakeAdminClient)

    admin = _create_admin_client(_settings())

    assert admin.poll_calls == []


def test_ensure_topic_creates_missing_topic() -> None:
    admin = _FakeAdminClient()

    created = ensure_topic(_settings(), admin=admin)

    assert created is True
    assert len(admin.created) == 1
    assert admin.created[0].topic == _TOPIC
    assert admin.created[0].num_partitions == TOPIC_PARTITIONS


def test_ensure_topic_skips_existing_topic() -> None:
    error = KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS))
    admin = _FakeAdminClient(error=error)

    created = ensure_topic(_settings(), admin=admin)

    assert created is False


def test_ensure_topic_reraises_other_kafka_errors() -> None:
    error = KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
    admin = _FakeAdminClient(error=error)

    with pytest.raises(KafkaException):
        ensure_topic(_settings(), admin=admin)
