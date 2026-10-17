# tests/test_rule_logic.py
"""Unit tests for the pure detection logic in spark/rule_logic.py.

Purpose: pin down the distance, speed, impossible-travel and structuring-band
behaviour, including the exact boundaries, before the Spark modules depend on it.
Design: no Spark session is needed, so these run in milliseconds. The cross-check
against the Spark band condition lives in its own test file.
"""

import math
from datetime import datetime, timedelta

import pytest

from spark.rule_logic import (
    EARTH_RADIUS_KM,
    haversine_km,
    implied_speed_kmh,
    is_impossible_travel,
    is_within_structuring_band,
)

NAIROBI = (-1.2921, 36.8219)
MOMBASA = (-4.0435, 39.6682)
LONDON = (51.5074, -0.1278)
PARIS = (48.8566, 2.3522)
NOON = datetime(2026, 1, 1, 12, 0, 0)
KM_PER_DEGREE = 2 * math.pi * EARTH_RADIUS_KM / 360


def _travel(origin, destination, elapsed, **kwargs):
    return is_impossible_travel(
        *origin, NOON, *destination, NOON + elapsed, max_speed_kmh=500.0, **kwargs
    )


class TestHaversineKm:
    def test_identical_points_are_zero_km_apart(self):
        assert haversine_km(*NAIROBI, *NAIROBI) == 0.0

    def test_distance_is_symmetric(self):
        assert haversine_km(*NAIROBI, *MOMBASA) == pytest.approx(
            haversine_km(*MOMBASA, *NAIROBI)
        )

    def test_nairobi_to_mombasa(self):
        assert haversine_km(*NAIROBI, *MOMBASA) == pytest.approx(440.0, abs=1.0)

    def test_london_to_paris(self):
        assert haversine_km(*LONDON, *PARIS) == pytest.approx(343.6, abs=1.0)

    def test_one_degree_of_latitude(self):
        assert haversine_km(0, 0, 1, 0) == pytest.approx(KM_PER_DEGREE)

    def test_antipodal_points_are_half_the_circumference(self):
        assert haversine_km(0, 0, 0, 180) == pytest.approx(math.pi * EARTH_RADIUS_KM)


class TestImpliedSpeedKmh:
    def test_distance_over_one_hour(self):
        assert implied_speed_kmh(100.0, 3600.0) == pytest.approx(100.0)

    def test_distance_over_half_an_hour(self):
        assert implied_speed_kmh(100.0, 1800.0) == pytest.approx(200.0)

    def test_zero_elapsed_with_distance_is_infinite(self):
        assert implied_speed_kmh(10.0, 0.0) == math.inf

    def test_negative_elapsed_with_distance_is_infinite(self):
        assert implied_speed_kmh(10.0, -5.0) == math.inf

    def test_zero_elapsed_without_distance_is_zero(self):
        assert implied_speed_kmh(0.0, 0.0) == 0.0


class TestIsImpossibleTravel:
    def test_fast_long_movement_is_flagged(self):
        assert _travel(NAIROBI, MOMBASA, timedelta(minutes=10))

    def test_plausible_movement_is_not_flagged(self):
        assert not _travel(NAIROBI, MOMBASA, timedelta(hours=2))

    def test_two_places_at_the_same_instant_is_flagged(self):
        assert _travel(NAIROBI, MOMBASA, timedelta(0))

    def test_same_place_at_the_same_instant_is_not_flagged(self):
        assert not _travel(NAIROBI, NAIROBI, timedelta(0))

    def test_movement_below_min_distance_is_ignored(self):
        assert not _travel(
            (0, 0), (1, 0), timedelta(seconds=1), min_distance_km=KM_PER_DEGREE + 1
        )

    def test_movement_above_min_distance_is_evaluated(self):
        assert _travel(
            (0, 0), (1, 0), timedelta(seconds=1), min_distance_km=KM_PER_DEGREE - 1
        )

    def test_speed_exactly_at_the_limit_is_not_flagged(self):
        distance_km = haversine_km(*NAIROBI, *MOMBASA)
        assert not is_impossible_travel(
            *NAIROBI,
            NOON,
            *MOMBASA,
            NOON + timedelta(hours=1),
            max_speed_kmh=distance_km,
        )


class TestIsWithinStructuringBand:
    THRESHOLD = 10_000.0
    FRACTION = 0.1

    @pytest.mark.parametrize(
        ("amount", "expected"),
        [
            (8_999.99, False),
            (9_000.0, True),
            (9_500.0, True),
            (9_999.99, True),
            (10_000.0, False),
            (12_000.0, False),
        ],
    )
    def test_band_boundaries(self, amount, expected):
        assert (
            is_within_structuring_band(amount, self.THRESHOLD, self.FRACTION)
            is expected
        )

    @pytest.mark.parametrize("threshold", [0.0, -1.0])
    def test_non_positive_threshold_is_rejected(self, threshold):
        with pytest.raises(ValueError, match="reporting_threshold"):
            is_within_structuring_band(100.0, threshold, self.FRACTION)

    @pytest.mark.parametrize("fraction", [0.0, 1.0, -0.1, 1.5])
    def test_fraction_outside_open_unit_interval_is_rejected(self, fraction):
        with pytest.raises(ValueError, match="band_fraction"):
            is_within_structuring_band(100.0, self.THRESHOLD, fraction)
