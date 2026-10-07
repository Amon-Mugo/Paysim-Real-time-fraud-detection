#used to generate synthetic transactions and aslo geo anomalies and fraud scenarios 

from __future__ import annotations
from datetime import datetime,timedelta
from dataclasses import dataclass
import itertools
import random
from generator.config import PAYSIM_STEP_COUNT,PAYSIM_STEP_DURATION,GeneratorConfig
from generator.geo import (
    CITIES,COORDINATE_PRECISION,
    JITTER_DEGREES,
    Location,  haversine_km, home_location,
)

from generator.ground_truth import FraudPattern,GroundTruthEntry
from generator.schema import INJECTED_ID_PREFIX, Transaction, build_transaction_id,format_event_time


ACCOUNT_PREFIX = "CINJ" #injected account id perfix
DESTINATION_BALANCE_MAX = 500_000.0  #maximum initila balance of a generated account randimly account openning

REPORTING_THRESHOLD = 1_000_000.0 #legal threshold for transfaring money
STRUCTURING_MIN_FRACTION = 0.85 #lower legal threshold
STRUCTURING_MAX_FRACTION = 0.99 #maximum perc threshold
STRUCTURING_MIN_TRANSFERS = 6 
STRUCTURING_MAX_TRANSFERS = 10
STRUCTURING_MIN_GAP = timedelta(minutes=10)
STRUCTURING_MAX_GAP = timedelta(minutes=40)

GEO_BASELINE_EVENTS = 3
GEO_MIN_BASELINE_GAP = timedelta(hours=1)
GEO_MAX_BASELINE_GAP = timedelta(hours=3)
GEO_MIN_ANOMALY_GAP = timedelta(minutes=5)
GEO_MAX_ANOMALY_GAP = timedelta(minutes=30)
GEO_MIN_DISTANCE_KM = 300.0
GEO_MIN_AMOUNT = 1_000.0
GEO_MAX_AMOUNT = 50_000.0
GEO_MIN_OPENING_BALANCE = 500_000.0
GEO_MAX_OPENING_BALANCE = 2_000_000.0

# minutes is 6 hours, and 2 baseline gaps of 3 hours plus the anomaly gap is
# 6.5 hours. Scenarios start early enough to finish inside the replay window.
MAX_SCENARIO_SPAN = timedelta(hours=12)


@dataclass(frozen=True)
class InjectedEvent:
    """A fully formed injected transaction and, if fraudulent, its label.

    `event_time` repeats the transaction's timestamp as a datetime so the
    producer can merge injected events with PaySim events in time order.
    Legitimate baseline events carry no label.
    """

    transaction: Transaction
    event_time: datetime
    label: GroundTruthEntry | None


class ScenarioBuilder:
    """Build individual scenarios; also used to trigger one on demand."""

    def __init__(self, rng: random.Random) -> None:
        self._rng = rng
        self._transaction_numbers = itertools.count(1)
        self._account_numbers = itertools.count(1)
        self._structuring_numbers = itertools.count(1)
        self._geo_numbers = itertools.count(1)

    def structuring(self, start: datetime) -> list[InjectedEvent]:
        """Build one structuring scenario beginning at `start`.

        Every transfer is labelled, since the pattern only becomes detectable
        as the transfers accumulate and is scored per scenario.
        """
        scenario_id = f"STR-{next(self._structuring_numbers):05d}"
        origin = self._new_account()
        location = home_location(origin)

        amounts = [
            round(
                REPORTING_THRESHOLD
                * self._rng.uniform(STRUCTURING_MIN_FRACTION, STRUCTURING_MAX_FRACTION),
                2,
            )
            for _ in range(
                self._rng.randint(STRUCTURING_MIN_TRANSFERS, STRUCTURING_MAX_TRANSFERS)
            )
        ]
        balance = round(sum(amounts) * self._rng.uniform(1.05, 1.5), 2)

        events: list[InjectedEvent] = []
        moment = start
        for index, amount in enumerate(amounts):
            if index > 0:
                moment += self._gap(STRUCTURING_MIN_GAP, STRUCTURING_MAX_GAP)
            event = self._event(
                event_time=moment,
                transaction_type="TRANSFER",
                amount=amount,
                origin=origin,
                old_balance=balance,
                location=location,
                pattern=FraudPattern.STRUCTURING,
                scenario_id=scenario_id,
            )
            balance = event.transaction.new_balance_orig
            events.append(event)
        return events

    def geo_anomaly(self, start: datetime) -> list[InjectedEvent]:
        """Build one geo-anomaly scenario beginning at `start`.

        The baseline events at the home location are unlabelled; only the
        final event, from a distant city, is labelled.
        """
        scenario_id = f"GEO-{next(self._geo_numbers):05d}"
        origin = self._new_account()
        home = home_location(origin)
        balance = round(
            self._rng.uniform(GEO_MIN_OPENING_BALANCE, GEO_MAX_OPENING_BALANCE), 2
        )

        events: list[InjectedEvent] = []
        moment = start
        for index in range(GEO_BASELINE_EVENTS + 1):
            is_anomaly = index == GEO_BASELINE_EVENTS
            if index > 0:
                moment += (
                    self._gap(GEO_MIN_ANOMALY_GAP, GEO_MAX_ANOMALY_GAP)
                    if is_anomaly
                    else self._gap(GEO_MIN_BASELINE_GAP, GEO_MAX_BASELINE_GAP)
                )
            event = self._event(
                event_time=moment,
                transaction_type="CASH_OUT",
                amount=round(self._rng.uniform(GEO_MIN_AMOUNT, GEO_MAX_AMOUNT), 2),
                origin=origin,
                old_balance=balance,
                location=self._distant_location(home) if is_anomaly else home,
                pattern=FraudPattern.GEO_ANOMALY if is_anomaly else None,
                scenario_id=scenario_id,
            )
            balance = event.transaction.new_balance_orig
            events.append(event)
        return events

    def _event(
        self,
        *,
        event_time: datetime,
        transaction_type: str,
        amount: float,
        origin: str,
        old_balance: float,
        location: Location,
        pattern: FraudPattern | None,
        scenario_id: str,
    ) -> InjectedEvent:
        old_balance_dest = round(self._rng.uniform(0.0, DESTINATION_BALANCE_MAX), 2)
        transaction = Transaction(
            transaction_id=build_transaction_id(
                INJECTED_ID_PREFIX, next(self._transaction_numbers)
            ),
            event_time=format_event_time(event_time),
            transaction_type=transaction_type,
            amount=amount,
            name_orig=origin,
            old_balance_orig=old_balance,
            new_balance_orig=round(old_balance - amount, 2),
            name_dest=self._new_account(),
            old_balance_dest=old_balance_dest,
            new_balance_dest=round(old_balance_dest + amount, 2),
            latitude=location.latitude,
            longitude=location.longitude,
        )
        label = (
            GroundTruthEntry(
                transaction_id=transaction.transaction_id,
                pattern=pattern,
                event_time=event_time,
                scenario_id=scenario_id,
            )
            if pattern is not None
            else None
        )
        return InjectedEvent(transaction=transaction, event_time=event_time, label=label)

    def _new_account(self) -> str:
        return f"{ACCOUNT_PREFIX}{next(self._account_numbers):07d}"

    def _gap(self, minimum: timedelta, maximum: timedelta) -> timedelta:
        return minimum + (maximum - minimum) * self._rng.random()

    def _distant_location(self, home: Location) -> Location:
        """Pick a spot near a city centre at least GEO_MIN_DISTANCE_KM from home."""
        candidates = [
            city
            for city in CITIES
            if haversine_km(home, city.centre) >= GEO_MIN_DISTANCE_KM
        ]
        if not candidates:
            raise ValueError("no city is far enough from the account's home")
        city = self._rng.choice(candidates)
        return Location(
            latitude=round(
                city.centre.latitude + self._rng.uniform(-JITTER_DEGREES, JITTER_DEGREES),
                COORDINATE_PRECISION,
            ),
            longitude=round(
                city.centre.longitude
                + self._rng.uniform(-JITTER_DEGREES, JITTER_DEGREES),
                COORDINATE_PRECISION,
            ),
        )


def plan_injections(settings: GeneratorConfig) -> list[InjectedEvent]:
    """Build every configured scenario and return the events in time order.

    Scenario start times are spread uniformly across the replay window, with
    enough margin that each scenario finishes before the window ends.
    """
    rng = random.Random(settings.random_seed)
    builder = ScenarioBuilder(rng)

    window_start = settings.simulation_start
    window_end = window_start + PAYSIM_STEP_COUNT * PAYSIM_STEP_DURATION
    latest_start = window_end - MAX_SCENARIO_SPAN

    events: list[InjectedEvent] = []
    for _ in range(settings.structuring_scenarios):
        events.extend(builder.structuring(_random_start(rng, window_start, latest_start)))
    for _ in range(settings.geo_anomaly_scenarios):
        events.extend(builder.geo_anomaly(_random_start(rng, window_start, latest_start)))

    events.sort(key=lambda event: (event.event_time, event.transaction.transaction_id))
    return events


def _random_start(rng: random.Random, earliest: datetime, latest: datetime) -> datetime:
    return earliest + timedelta(seconds=rng.uniform(0, (latest - earliest).total_seconds()))