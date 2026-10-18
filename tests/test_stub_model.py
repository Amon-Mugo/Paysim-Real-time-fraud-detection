"""Tests for the stub fraud model and its artifact writer."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from spark.model_contract import FraudModel, load_manifest
from spark.stub_model import (
    STUB_FRAMEWORK,
    STUB_MODEL_FILENAME,
    StubFraudModel,
    StubModelError,
    write_stub_artifact,
)

FEATURE_NAMES = ("amount", "oldbalanceOrg")


def _publish(model_dir: Path):
    """Publish a valid stub artifact into ``model_dir``."""
    return write_stub_artifact(
        model_dir,
        model_version="v1",
        feature_names=FEATURE_NAMES,
        score_feature="amount",
        scale=1000.0,
        alert_threshold=0.8,
    )


def _write_model_file(tmp_path: Path, content: str) -> Path:
    """Write raw text to a stub model file and return its path."""
    path = tmp_path / STUB_MODEL_FILENAME
    path.write_text(content, encoding="utf-8")
    return path


def test_scores_scale_with_feature_value() -> None:
    model = StubFraudModel(score_feature="amount", scale=1000.0)
    frame = pd.DataFrame({"amount": [0.0, 250.0, 1000.0]})
    np.testing.assert_allclose(model.predict_fraud_probability(frame), [0.0, 0.25, 1.0])


def test_scores_are_clipped_to_unit_interval() -> None:
    model = StubFraudModel(score_feature="amount", scale=1000.0)
    frame = pd.DataFrame({"amount": [-5.0, 2000.0]})
    np.testing.assert_allclose(model.predict_fraud_probability(frame), [0.0, 1.0])


def test_missing_values_score_as_zero() -> None:
    model = StubFraudModel(score_feature="amount", scale=1000.0)
    frame = pd.DataFrame({"amount": [np.nan, 500.0]})
    np.testing.assert_allclose(model.predict_fraud_probability(frame), [0.0, 0.5])


def test_stub_satisfies_the_fraud_model_interface() -> None:
    assert isinstance(StubFraudModel(score_feature="amount", scale=1.0), FraudModel)


@pytest.mark.parametrize("scale", [0.0, -1.0])
def test_non_positive_scale_is_rejected(scale: float) -> None:
    with pytest.raises(StubModelError, match="scale"):
        StubFraudModel(score_feature="amount", scale=scale)


def test_blank_score_feature_is_rejected() -> None:
    with pytest.raises(StubModelError, match="score_feature"):
        StubFraudModel(score_feature="  ", scale=1.0)


def test_from_file_round_trip(tmp_path: Path) -> None:
    path = _write_model_file(tmp_path, '{"score_feature": "amount", "scale": 1000}')
    assert StubFraudModel.from_file(path) == StubFraudModel("amount", 1000.0)


def test_from_file_missing_file(tmp_path: Path) -> None:
    with pytest.raises(StubModelError, match="No stub model file"):
        StubFraudModel.from_file(tmp_path / STUB_MODEL_FILENAME)


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("{not json", "not valid JSON"),
        ("[1, 2]", "JSON object"),
        ('{"scale": 1}', "missing field"),
        ('{"score_feature": "amount", "scale": "abc"}', "must be a number"),
    ],
)
def test_from_file_rejects_bad_content(
    tmp_path: Path, content: str, message: str
) -> None:
    path = _write_model_file(tmp_path, content)
    with pytest.raises(StubModelError, match=message):
        StubFraudModel.from_file(path)


def test_write_stub_artifact_publishes_a_valid_manifest(tmp_path: Path) -> None:
    model_dir = tmp_path / "v1"
    manifest = _publish(model_dir)

    assert load_manifest(model_dir) == manifest
    assert manifest.framework == STUB_FRAMEWORK
    assert manifest.feature_names == FEATURE_NAMES
    assert json.loads(manifest.model_path(model_dir).read_text(encoding="utf-8")) == {
        "score_feature": "amount",
        "scale": 1000.0,
    }


def test_write_stub_artifact_never_overwrites_a_version(tmp_path: Path) -> None:
    model_dir = tmp_path / "v1"
    _publish(model_dir)
    with pytest.raises(FileExistsError):
        _publish(model_dir)


def test_write_stub_artifact_rejects_unknown_score_feature(tmp_path: Path) -> None:
    model_dir = tmp_path / "v1"
    with pytest.raises(StubModelError, match="one of feature_names"):
        write_stub_artifact(
            model_dir,
            model_version="v1",
            feature_names=FEATURE_NAMES,
            score_feature="step",
            scale=1000.0,
            alert_threshold=0.8,
        )
    assert not model_dir.exists()
