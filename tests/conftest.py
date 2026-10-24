"""Shared pytest fixtures for the Spark tests."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from spark.config import DetectionConfig
from spark.iceberg_session import iceberg_packages


def _configure_jvm_packages() -> None:
    """Add the Iceberg runtime to the arguments the Spark JVM launches with.

    Spark reads ``--packages`` only when the JVM starts, and every later
    session in the pytest process reuses that JVM. Setting it here, before any
    test module runs, makes the Iceberg classes available to all sessions.
    """
    packages = ",".join(
        iceberg_packages(DetectionConfig(iceberg_catalog_type="hadoop"))
    )
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {packages} pyspark-shell"


_configure_jvm_packages()


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
