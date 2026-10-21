"""Unit tests for the Iceberg session settings."""

from __future__ import annotations

import pytest

from spark import iceberg_session
from spark.config import DetectionConfig
from spark.iceberg_session import iceberg_packages, iceberg_spark_conf

_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
_SPARK_CATALOG = "org.apache.iceberg.spark.SparkCatalog"
_GLUE_CATALOG = "org.apache.iceberg.aws.glue.GlueCatalog"
_S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO"


def _config(**overrides: str) -> DetectionConfig:
    return DetectionConfig(**overrides)


def _set_spark_line(monkeypatch: pytest.MonkeyPatch, minor: str) -> None:
    monkeypatch.setattr(iceberg_session, "_spark_minor_version", lambda: minor)


def test_hadoop_catalog_adds_the_runtime_package(monkeypatch):
    _set_spark_line(monkeypatch, "4.0")

    packages = iceberg_packages(_config(iceberg_catalog_type="hadoop"))

    assert packages == [
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13"
        f":{iceberg_session.ICEBERG_VERSION}"
    ]


def test_runtime_package_follows_the_spark_line(monkeypatch):
    _set_spark_line(monkeypatch, "4.1")

    packages = iceberg_packages(_config(iceberg_catalog_type="hadoop"))

    assert "iceberg-spark-runtime-4.1_2.13" in packages[0]


def test_unsupported_spark_line_is_rejected(monkeypatch):
    _set_spark_line(monkeypatch, "4.2")

    with pytest.raises(ValueError, match="runtime for Spark 4.2"):
        iceberg_packages(_config(iceberg_catalog_type="hadoop"))


def test_glue_catalog_adds_no_package():
    config = _config(
        iceberg_catalog_type="glue", iceberg_warehouse="s3://fraud-curated/warehouse"
    )

    assert iceberg_packages(config) == []


def test_hadoop_conf_registers_extensions_and_catalog():
    config = _config(
        iceberg_catalog_type="hadoop",
        iceberg_catalog_name="lake",
        iceberg_warehouse="/tmp/warehouse",
    )

    assert iceberg_spark_conf(config) == {
        "spark.sql.extensions": _EXTENSIONS,
        "spark.sql.catalog.lake": _SPARK_CATALOG,
        "spark.sql.catalog.lake.type": "hadoop",
        "spark.sql.catalog.lake.warehouse": "/tmp/warehouse",
    }


def test_glue_conf_registers_glue_catalog_on_s3():
    config = _config(
        iceberg_catalog_type="glue",
        iceberg_catalog_name="lake",
        iceberg_warehouse="s3://fraud-curated/warehouse",
    )

    assert iceberg_spark_conf(config) == {
        "spark.sql.extensions": _EXTENSIONS,
        "spark.sql.catalog.lake": _SPARK_CATALOG,
        "spark.sql.catalog.lake.catalog-impl": _GLUE_CATALOG,
        "spark.sql.catalog.lake.io-impl": _S3_FILE_IO,
        "spark.sql.catalog.lake.warehouse": "s3://fraud-curated/warehouse",
    }


def test_glue_catalog_requires_an_s3_warehouse():
    config = _config(iceberg_catalog_type="glue", iceberg_warehouse="/tmp/warehouse")

    with pytest.raises(ValueError, match="s3://"):
        iceberg_spark_conf(config)
