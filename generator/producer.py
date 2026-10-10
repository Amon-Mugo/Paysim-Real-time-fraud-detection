# used for real time streaming kafka .main purpose is to read baseline transactions from datasets
# and alsoe inject the synthetic  records ,merge them chronologically in event time order
# publish payload to kafka topic and write the fraud labels

from __future__ import annotations

import heapq
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from typing import Protocol

from confluent_kafka import KafkaError, Message, Producer

from generator.config import GeneratorConfig
from generator.geo import home_location
from generator.ground_truth import FraudPattern, GroundTruthEntry, GroundTruthWriter
from generator.injectors import plan_injections
from generator.paysim_reader import PaySimRecord, read_paysim
from generator.schema import Transaction, format_event_time

LOG_INTERNAL_EVENTS = 100_000  # logs processes every 100000 messages
MIN_SLEEP_SECONDS = 0.002  # 2 miliseconds sleep threshold
QUEUE_FULL_POLL_SECONDS =  1.0  # wait time till messages buffer n/b flush out next messages (buffer= 1)

FLUSH_TIMEOUT_SECONDS = 60.0  # maximum wait to flush messages remained at queue upon shutdown
MAX_LOGGED_DELIVERY_ERRORS = 5  # error messages capped at 5

logger = logging.getLogger(__name__)


# Used for publishing messages to kafka
class StreamEvent(Protocol):
    @property
    def event_time(self) -> datetime: ...

    @property
    def transaction(self) -> Transaction: ...

    @property
    def label(self) -> GroundTruthEntry | None: ...


# used to take the paysim row and turns them into a publishable unit
@dataclass(frozen=True)
class PaysimEvent:
    event_time: datetime
    transaction: Transaction
    label: GroundTruthEntry | None


# used to publish a summary of the whole kafka message executon
@dataclass(frozen=True)
class ReplayStats:
    published: int
    labelled: int
    failed: int
    elapsed_seconds: float


# used for tracking error failure which is always continus
class _DeliveryTracker:
    def __init__(self) -> None:  # a starter functions that always runs first
        self.failed = 0

    def on_delivery(self, error: KafkaError | None, message: Message) -> None:
        if error is None:
            return
        self.failed += 1
        if self.failed <= MAX_LOGGED_DELIVERY_ERRORS:
            logger.error("Delivery failed for log %s: %s", message.key(), error)


class _Pacer:
    def __init__(self, first_event_time: datetime, speedup: float) -> None:
        self._origin = first_event_time
        self._speedup = speedup
        self._started = time.monotonic()  # current wall clock of real time

    def wait_until(self, event_time: datetime) -> None:

        offset = (event_time - self._origin).total_seconds() / self._speedup
        delay = self._started + offset - time.monotonic()
        if delay > MIN_SLEEP_SECONDS:
            time.sleep(delay)


# used in creating a producer to ensure reliability and performance settings
def create_producer(settings: GeneratorConfig) -> Producer:
    return Producer(
        {
            "bootstrap.servers": (
                settings.kafka_bootstrap_servers
            ),  # for cluster connections and the address
            "client.id": (
                "paysim-generator"
            ),  
            "acks": (
                "all"
            ),  # tells kafka to use isr to ensure all messages are received before retuing success
            "enable.idempotence": True,  # sequence the messages in order
            "compression.type": "lz4",  # compression algorithms
            "linger.ms": 5,  # waith 5 miliseconds before sending another message
        }
    )


# used in combing the fraid data and actual data
def merge_events(settings: GeneratorConfig) -> Iterator[StreamEvent]:
    paysim = (_to_stream_event(record) for record in read_paysim(settings))
    return heapq.merge(paysim, plan_injections(settings), key=_event_time)


# used as the main function that couples everything together
def replay(
    settings: GeneratorConfig,
    producer: Producer | None = None,
    max_events: int | None = None,
) -> ReplayStats:
    if producer is None:
        producer = create_producer(settings)

    # FIXED: Added () to instantiate _DeliveryTracker class
    tracker = _DeliveryTracker()
    pacer: _Pacer | None = (
        None  # set none initilay we need to set the first transaction
    )
    published = 0  # messages sent
    labelled = 0  # groundtruth labels logged
    started = time.monotonic()
    
    with GroundTruthWriter(settings.ground_truth_path) as ground_truth:
        try:
            for event in islice(merge_events(settings), max_events):
                if pacer is None:
                    pacer = _Pacer(
                        event.event_time, settings.replay_speedup
                    )  # for the first event we get the timestamp

                pacer.wait_until(
                    event.event_time
                )  # calculates the delay and sleep if neccessary

              
                _publish(
                    producer, settings.kafka_topic, event, tracker
                )  # sends current transaction to kafka

               
                if event.label is not None:
                    ground_truth.write(event.label)
                    labelled += 1

                published += 1
                producer.poll(0)
                if published % LOG_INTERNAL_EVENTS == 0:
                    logger.info("Published %d events", published)

        finally:
            undelivered = producer.flush(FLUSH_TIMEOUT_SECONDS)

    if undelivered:
        raise RuntimeError(
            f"{undelivered} messages were not delivered before timeout"
        )
    if tracker.failed:
        raise RuntimeError(f"{tracker.failed} messages failed delivery")

    return ReplayStats(
        published=published,
        labelled=labelled,
        failed=tracker.failed,
        elapsed_seconds=time.monotonic() - started,
    )


# used for safely queue message into kafka producer clinet side  ,memory buffer
# and aslo handling back pressure
def _publish(
    producer: Producer,
    topic: str,
    event: StreamEvent,
    tracker: _DeliveryTracker,
) -> None:
    while True:
        try:
            producer.produce(
                topic,
                key=event.transaction.message_key(),
                # FIXED: Added missing comma after message_value()
                value=event.transaction.message_value(),
                on_delivery=tracker.on_delivery,
            )
            return
        except BufferError:
            producer.poll(
                QUEUE_FULL_POLL_SECONDS
            )  # used to flash out the queued messages


# used to compare between the paysim and fraud data injections which came first
def _event_time(event: StreamEvent) -> datetime:
    return event.event_time


# used to transform the row dataset into normalised stream event (PaysimEvent) while performing
# geogrphicla enrichments ,serialization formating and groundtruth isolation fraud is always offline
def _to_stream_event(record: PaySimRecord) -> PaysimEvent:
    location = home_location(
        record.name_orig
    )  # used to determine origin and the location

    transaction = Transaction(
        transaction_id=record.transaction_id,
        event_time=format_event_time(record.event_time),
        transaction_type=record.transaction_type,
        amount=record.amount,
        name_orig=record.name_orig,
        old_balance_orig=record.old_balance_orig,
        new_balance_orig=record.new_balance_orig,
        name_dest=record.name_dest,
        old_balance_dest=record.old_balance_dest,
        new_balance_dest=record.new_balance_dest,
        latitude=location.latitude,
        longitude=location.longitude,
    )

    label = (
        GroundTruthEntry(
            transaction_id=record.transaction_id,
            pattern=FraudPattern.PAYSIM_FRAUD,
            event_time=record.event_time,
        )
        if record.is_fraud
        else None
    )
    return PaysimEvent(
        event_time=record.event_time, transaction=transaction, label=label
    )