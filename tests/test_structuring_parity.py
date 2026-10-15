# tests/test_structuring_parity.py
"""Parity test between the Python and Spark structuring band conditions.

Purpose: structuring.py applies the band as a Spark column expression while
rule_logic.py exposes the same rule as plain Python. If the two ever drift apart,
the unit tests and the streaming job would disagree about which amounts count.
Design: boundary amounts, including the floating-point neighbours of each edge,
run through both implementations on a local Spark session and must match exactly.
"""

import math

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.config import DetectionConfig
from spark.rule_logic import is_within_structuring_band
from spark.structuring import _in_structuring_band

_DEFAULTS = DetectionConfig()
_DEFAULT_FRACTION = (
    1 - _DEFAULTS.structuring_min_amount / _DEFAULTS.structuring_max_amount
)

BAND_SCENARIOS = [
    pytest.param(10_000.0, 0.1, id="documented-example"),
    pytest.param(1_000_000.0, 0.15, id="round-fraction"),
    pytest.param(
        _DEFAULTS.structuring_max_amount, _DEFAULT_FRACTION, id="config-defaults"
    ),
    pytest.param(333_333.33, 0.07, id="awkward-values"),
]


@pytest.fixture(scope="module")
def spark_session():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("structuring-parity-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _boundary_amounts(threshold: float, band_fraction: float) -> list:
    """Return amounts on, just below and just above each band edge, plus interior points."""
    lower_bound = threshold * (1 - band_fraction)
    edges = []
    for edge in (lower_bound, threshold):
        edges.extend(
            [math.nextafter(edge, -math.inf), edge, math.nextafter(edge, math.inf)]
        )
    interior = [0.0, lower_bound / 2, (lower_bound + threshold) / 2, threshold * 2]
    return edges + interior


def _spark_band_flags(spark, amounts, threshold, band_fraction) -> list:
    """Evaluate the Spark band condition for each amount, preserving input order."""
    frame = spark.createDataFrame(
        [(index, amount) for index, amount in enumerate(amounts)],
        "row_id int, amount double",
    )
    rows = (
        frame.select(
            "row_id",
            _in_structuring_band(F.col("amount"), threshold, band_fraction).alias(
                "in_band"
            ),
        )
        .orderBy("row_id")
        .collect()
    )
    return [row.in_band for row in rows]


@pytest.mark.parametrize(("threshold", "band_fraction"), BAND_SCENARIOS)
def test_spark_band_matches_python_band(spark_session, threshold, band_fraction):
    amounts = _boundary_amounts(threshold, band_fraction)

    expected = [
        is_within_structuring_band(amount, threshold, band_fraction)
        for amount in amounts
    ]
    actual = _spark_band_flags(spark_session, amounts, threshold, band_fraction)

    assert actual == expected


def test_configured_amount_range_maps_onto_the_band():
    threshold = _DEFAULTS.structuring_max_amount

    assert is_within_structuring_band(
        _DEFAULTS.structuring_min_amount, threshold, _DEFAULT_FRACTION
    )
    assert not is_within_structuring_band(threshold, threshold, _DEFAULT_FRACTION)
    assert not is_within_structuring_band(
        math.nextafter(_DEFAULTS.structuring_min_amount, 0.0),
        threshold,
        _DEFAULT_FRACTION,
    )