"""Tests for the Spark scoring step, run on a small batch DataFrame."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from spark.model_contract import FeatureSchemaError, ModelManifest
from spark.model_loader import load_model
from spark.scoring import RULE_NAME, SCORE_COLUMN, detect_ml_fraud, score_transactions
from spark.stub_model import write_stub_artifact

FEATURE_NAMES = ("amount", "old_balance_orig")
TRANSACTION_COLUMNS = (
    "transaction_id string, event_time string, name_orig string, "
    "amount double, old_balance_orig double, is_valid_event boolean"
)
ALERT_THRESHOLD = 0.8


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-scoring")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def loaded_model(tmp_path: Path):
    """Publish a stub artifact and load it through the real loader."""
    model_dir = tmp_path / "v1"
    write_stub_artifact(
        model_dir,
        model_version="v1",
        feature_names=FEATURE_NAMES,
        score_feature="amount",
        scale=1000.0,
        alert_threshold=ALERT_THRESHOLD,
    )
    return load_model(model_dir)


def _transactions(spark: SparkSession) -> DataFrame:
    rows = [
        ("T1", "2026-01-01 00:00:00", "C1", 100.0, 0.0, True),
        ("T2", "2026-01-01 00:01:00", "C2", 800.0, 0.0, True),
        ("T3", "2026-01-01 00:02:00", "C3", 900.0, 0.0, True),
        ("T4", "2026-01-01 00:03:00", "C4", 950.0, 0.0, False),
    ]
    return spark.createDataFrame(rows, TRANSACTION_COLUMNS)


def _scores_by_id(frame: DataFrame) -> dict[str, float]:
    return {row["transaction_id"]: row[SCORE_COLUMN] for row in frame.collect()}


def test_scores_are_added_for_valid_events_only(
    spark: SparkSession, loaded_model: tuple[ModelManifest, object]
) -> None:
    manifest, model = loaded_model
    scored = score_transactions(_transactions(spark), manifest, model)

    assert _scores_by_id(scored) == pytest.approx({"T1": 0.1, "T2": 0.8, "T3": 0.9})


def test_alerts_include_scores_at_or_above_the_threshold(
    spark: SparkSession, loaded_model: tuple[ModelManifest, object]
) -> None:
    manifest, model = loaded_model
    alerts = detect_ml_fraud(_transactions(spark), manifest, model)

    assert set(_scores_by_id(alerts)) == {"T2", "T3"}


def test_alerts_carry_model_version_and_rule(
    spark: SparkSession, loaded_model: tuple[ModelManifest, object]
) -> None:
    manifest, model = loaded_model
    alerts = detect_ml_fraud(_transactions(spark), manifest, model)

    assert {row["model_version"] for row in alerts.collect()} == {"v1"}
    assert {row["rule"] for row in alerts.collect()} == {RULE_NAME}


def test_missing_feature_fails_when_the_query_is_built(
    spark: SparkSession, loaded_model: tuple[ModelManifest, object]
) -> None:
    _, model = loaded_model
    manifest_needing_extra_column = ModelManifest(
        schema_version=1,
        model_version="v1",
        framework="stub",
        model_file="stub_model.json",
        feature_names=("amount", "not_a_stream_column"),
        alert_threshold=ALERT_THRESHOLD,
        trained_at="2026-01-01T00:00:00+00:00",
    )

    with pytest.raises(FeatureSchemaError, match="not_a_stream_column"):
        detect_ml_fraud(_transactions(spark), manifest_needing_extra_column, model)
