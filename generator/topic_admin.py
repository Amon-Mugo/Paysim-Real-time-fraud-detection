from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from generator.config import GeneratorConfig
from generator.producer import _build_oauth_callback

TOPIC_PARTITIONS = 6
BROKER_DEFAULT_REPLICATION = -1  # let the cluster choose its own replication factor
REQUEST_TIMEOUT_SECONDS = 30.0

logger = logging.getLogger(__name__)


def _build_admin_config(settings: GeneratorConfig) -> dict[str, Any]:
    admin_config: dict[str, Any] = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "paysim-generator-admin",
    }
    if settings.kafka_iam_auth:
        admin_config.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "OAUTHBEARER",
                "oauth_cb": _build_oauth_callback(settings.aws_region),
            }
        )
    return admin_config


def _create_admin_client(settings: GeneratorConfig) -> AdminClient:
    admin = AdminClient(_build_admin_config(settings))
    if settings.kafka_iam_auth:
        admin.poll(0)
    return admin


def _is_topic_exists_error(error: KafkaException) -> bool:
    kafka_error = error.args[0]
    return (
        isinstance(kafka_error, KafkaError)
        and kafka_error.code() == KafkaError.TOPIC_ALREADY_EXISTS
    )


def ensure_topic(settings: GeneratorConfig, admin: AdminClient | None = None) -> bool:
    """Create the configured topic if missing; return True when it was created."""
    client = admin if admin is not None else _create_admin_client(settings)
    topic = settings.kafka_topic
    new_topic = NewTopic(
        topic,
        num_partitions=TOPIC_PARTITIONS,
        replication_factor=BROKER_DEFAULT_REPLICATION,
    )
    futures = client.create_topics([new_topic], request_timeout=REQUEST_TIMEOUT_SECONDS)
    try:
        futures[topic].result()
    except KafkaException as error:
        if _is_topic_exists_error(error):
            logger.info("Topic %s already exists", topic)
            return False
        raise
    logger.info("Created topic %s with %d partitions", topic, TOPIC_PARTITIONS)
    return True
