"""Shared pytest fixtures for the Spark tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Provide a small local Spark session, stopped when the test module ends."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-spark")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
