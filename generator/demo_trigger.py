"""Fire a single fraud scenario into Kafka on demand.

Usage:
    python -m generator.demo_trigger structuring
    python -m generator.demo_trigger geo-anomaly --start 2026-01-05T09:00:00
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import datetime, timezone

from confluent_kafka import KafkaError, Message, Producer

from generator.config import PROJECT_ROOT, GeneratorConfig
from generator.injectors import InjectedEvent, ScenarioBuilder
from generator.producer import create_producer
from generator.schema import build_transaction_id

EXIT_OK = 0
EXIT_TRIGGER_FAILED = 1
EXIT_INVALID_CONFIG = 2
EXIT_INTERRUPTED = 130

FLUSH_TIMEOUT_SECONDS = 30.0
MAX_LOGGED_DELIVERY_ERRORS = 5

DEMO_ID_PREFIX = "DEMO"
DEMO_ACCOUNT_PREFIX = "CDEMO"
DEFAULT_DEMO_GROUND_TRUTH_PATH = PROJECT_ROOT / "data" / "demo_ground_truth.jsonl"

ScenarioFactory = Callable[[ScenarioBuilder, datetime], list[InjectedEvent]]

SCENARIO_FACTORIES: dict[str, ScenarioFactory] = {
    "structuring": ScenarioBuilder.structuring,
    "geo-anomaly": ScenarioBuilder.geo_anomaly,
    "velocity": ScenarioBuilder.velocity,
}

logger = logging.getLogger("generator.demo_trigger")


class _DeliveryTracker:
    """Counts failed Kafka deliveries and logs the first few."""

    def __init__(self) -> None:
        self.failed = 0

    def on_delivery(self, error: KafkaError | None, message: Message) -> None:
        if error is None:
            return
        self.failed += 1
        if self.failed <= MAX_LOGGED_DELIVERY_ERRORS:
            logger.error("Delivery failed for key %s: %s", message.key(), error)


def _parse_start(raw: str) -> datetime:
    parsed = datetime.fromisoformat(raw)
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m generator.demo_trigger",
        description="Fire one fraud scenario into Kafka on demand",
    )
    parser.add_argument(
        "scenario",
        choices=sorted(SCENARIO_FACTORIES),
        help="which scenario to fire",
    )
    parser.add_argument("--bootstrap-servers", help="comma-separated Kafka bootstrap servers")
    parser.add_argument("--topic", help="Kafka topic to publish to")
    parser.add_argument(
        "--start",
        type=_parse_start,
        help="ISO-8601 event time of the first transaction (default: simulation start)",
    )
    parser.add_argument(
        "--speedup",
        type=float,
        help="simulated seconds per wall-clock second between events",
    )
    parser.add_argument("--seed", type=int, help="seed for the scenario contents")
    parser.add_argument(
        "--ground-truth-path",
        type=_path,
        default=DEFAULT_DEMO_GROUND_TRUTH_PATH,
        help="label log for demo scenarios (default: data/demo_ground_truth.jsonl)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="logging verbosity (default: INFO)",
    )
    return parser


def _path(raw: str):
    from pathlib import Path

    return Path(raw)


def _resolve_settings(args: argparse.Namespace) -> GeneratorConfig:
    overrides = {
        "kafka_bootstrap_servers": args.bootstrap_servers,
        "kafka_topic": args.topic,
        "simulation_start": args.start,
        "replay_speedup": args.speedup,
        "random_seed": args.seed,
        "ground_truth_path": args.ground_truth_path,
    }
    provided = {name: value for name, value in overrides.items() if value is not None}
    return replace(GeneratorConfig.from_env(), **provided)


def _isolate_events(
    events: list[InjectedEvent], run_id: str
) -> list[InjectedEvent]:
    """Give a scenario IDs and accounts that cannot collide with other streams.

    ScenarioBuilder numbers transactions and accounts from 1, which would clash
    with the main generator's injected events and break duplicate-ID checks and
    transaction-ID idempotency downstream.
    """
    accounts: dict[str, str] = {}

    def remap_account(name: str) -> str:
        if name not in accounts:
            accounts[name] = f"{DEMO_ACCOUNT_PREFIX}{run_id}{len(accounts) + 1:03d}"
        return accounts[name]

    isolated: list[InjectedEvent] = []
    for number, event in enumerate(events, start=1):
        transaction_id = build_transaction_id(f"{DEMO_ID_PREFIX}{run_id}", number)
        transaction = replace(
            event.transaction,
            transaction_id=transaction_id,
            name_orig=remap_account(event.transaction.name_orig),
            name_dest=remap_account(event.transaction.name_dest),
        )
        label = (
            None
            if event.label is None
            else replace(
                event.label,
                transaction_id=transaction_id,
                scenario_id=f"{DEMO_ID_PREFIX}-{run_id}-{event.label.scenario_id}",
            )
        )
        isolated.append(replace(event, transaction=transaction, label=label))
    return isolated


def _fire(
    producer: Producer, settings: GeneratorConfig, events: list[InjectedEvent]
) -> int:
    """Publish the events, spaced by their event-time gaps, and log their labels.

    Returns the number of labels written.
    """
    tracker = _DeliveryTracker()
    labelled = 0
    previous: datetime | None = None

    settings.ground_truth_path.parent.mkdir(parents=True, exist_ok=True)
    with settings.ground_truth_path.open("a", encoding="utf-8", buffering=1) as labels:
        try:
            for event in events:
                if previous is not None:
                    gap = (event.event_time - previous).total_seconds()
                    time.sleep(gap / settings.replay_speedup)
                previous = event.event_time

                producer.produce(
                    settings.kafka_topic,
                    key=event.transaction.message_key(),
                    value=event.transaction.message_value(),
                    on_delivery=tracker.on_delivery,
                )
                producer.poll(0)

                if event.label is not None:
                    labels.write(event.label.to_json() + "\n")
                    labelled += 1
        finally:
            undelivered = producer.flush(FLUSH_TIMEOUT_SECONDS)

    if undelivered:
        raise RuntimeError(f"{undelivered} messages were not delivered before timeout")
    if tracker.failed:
        raise RuntimeError(f"{tracker.failed} messages failed delivery")
    return labelled


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        settings = _resolve_settings(args)
    except ValueError as error:
        logger.error("Invalid configuration: %s", error)
        return EXIT_INVALID_CONFIG

    run_id = datetime.now(timezone.utc).strftime("%y%m%d%H%M%S")
    builder = ScenarioBuilder(random.Random(settings.random_seed))
    events = _isolate_events(
        SCENARIO_FACTORIES[args.scenario](builder, settings.simulation_start),
        run_id,
    )

    logger.info(
        "Firing %s: %d events to topic '%s' on %s",
        args.scenario,
        len(events),
        settings.kafka_topic,
        settings.kafka_bootstrap_servers,
    )

    try:
        labelled = _fire(create_producer(settings), settings, events)
    except KeyboardInterrupt:
        logger.warning("Interrupted; queued messages were flushed before exit")
        return EXIT_INTERRUPTED
    except (RuntimeError, OSError) as error:
        logger.error("Trigger failed: %s", error)
        return EXIT_TRIGGER_FAILED

    logger.info(
        "Done: %d events published, %d labelled. Labels: %s",
        len(events),
        labelled,
        settings.ground_truth_path,
    )
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())