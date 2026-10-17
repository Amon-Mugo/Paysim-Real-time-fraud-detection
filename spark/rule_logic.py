"""Pure detection logic shared by the streaming rule modules.

Contains the geographic and amount checks as plain Python functions with no
Spark dependency, so they can be unit-tested in isolation. Thresholds are
always passed in by the caller rather than read from configuration here.
"""

import math
from datetime import datetime

EARTH_RADIUS_KM = 6371.0088
SECONDS_PER_HOUR = 3600.0


def haversine_km(
    latitude_a: float, longitude_a: float, latitude_b: float, longitude_b: float
) -> float:
    """Return the great-circle distance in kilometres between two points."""
    phi_a = math.radians(latitude_a)
    phi_b = math.radians(latitude_b)
    delta_phi = phi_b - phi_a
    delta_lambda = math.radians(longitude_b - longitude_a)

    half_chord = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi_a) * math.cos(phi_b) * math.sin(delta_lambda / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(half_chord)))


def implied_speed_kmh(distance_km: float, elapsed_seconds: float) -> float:
    """Return the speed in km/h needed to cover a distance in the elapsed time.

    A non-positive elapsed time with a positive distance is treated as
    infinitely fast, since the account appears in two places at once.
    """
    if elapsed_seconds <= 0:
        return math.inf if distance_km > 0 else 0.0
    return distance_km / (elapsed_seconds / SECONDS_PER_HOUR)


def is_impossible_travel(
    previous_latitude: float,
    previous_longitude: float,
    previous_time: datetime,
    latitude: float,
    longitude: float,
    event_time: datetime,
    max_speed_kmh: float,
    min_distance_km: float = 0.0,
) -> bool:
    """Return True when two consecutive events imply an implausible speed.

    Movements shorter than min_distance_km are ignored so that small location
    jitter between nearby events is not reported as travel.
    """
    distance_km = haversine_km(
        previous_latitude, previous_longitude, latitude, longitude
    )
    if distance_km < min_distance_km:
        return False

    elapsed_seconds = (event_time - previous_time).total_seconds()
    return implied_speed_kmh(distance_km, elapsed_seconds) > max_speed_kmh


def is_within_structuring_band(
    amount: float, reporting_threshold: float, band_fraction: float
) -> bool:
    """Return True when an amount sits just below the reporting threshold.

    The band covers [threshold * (1 - band_fraction), threshold). For a
    threshold of 10,000 and a fraction of 0.1, amounts from 9,000 up to but
    excluding 10,000 qualify.
    """
    if reporting_threshold <= 0:
        raise ValueError("reporting_threshold must be positive")
    if not 0 < band_fraction < 1:
        raise ValueError("band_fraction must be between 0 and 1 exclusive")

    lower_bound = reporting_threshold * (1 - band_fraction)
    return lower_bound <= amount < reporting_threshold
