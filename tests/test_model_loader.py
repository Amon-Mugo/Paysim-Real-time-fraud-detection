"""Tests for the model loader."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from spark import model_loader
from spark.model_contract import MANIFEST_FILENAME, ManifestError
from spark.model_loader import ModelLoadError, load_model
from spark.stub_model import StubFraudModel, write_stub_artifact

FEATURE_NAMES = ("amount", "oldbalanceOrg")


def _publish(model_dir: Path, model_version: str = "v1"):
    """Publish a valid stub artifact into ``model_dir``."""
    return write_stub_artifact(
        model_dir,
        model_version=model_version,
        feature_names=FEATURE_NAMES,
        score_feature="amount",
        scale=1000.0,
        alert_threshold=0.8,
    )


def _rewrite_manifest_field(model_dir: Path, field: str, value: str) -> None:
    """Change one field of a published manifest in place."""
    manifest_path = model_dir / MANIFEST_FILENAME
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw[field] = value
    manifest_path.write_text(json.dumps(raw), encoding="utf-8")


def test_load_model_returns_manifest_and_working_model(tmp_path: Path) -> None:
    model_dir = tmp_path / "v1"
    published = _publish(model_dir)

    manifest, model = load_model(model_dir)

    assert manifest == published
    assert isinstance(model, StubFraudModel)
    frame = pd.DataFrame({"amount": [500.0], "oldbalanceOrg": [0.0]})
    np.testing.assert_allclose(model.predict_fraud_probability(frame), [0.5])


def test_directory_name_must_match_model_version(tmp_path: Path) -> None:
    model_dir = tmp_path / "v1"
    _publish(model_dir)
    renamed = model_dir.rename(tmp_path / "other")

    with pytest.raises(ModelLoadError, match="does not match"):
        load_model(renamed)


def test_missing_model_file_is_rejected(tmp_path: Path) -> None:
    model_dir = tmp_path / "v1"
    manifest = _publish(model_dir)
    manifest.model_path(model_dir).unlink()

    with pytest.raises(ModelLoadError, match="Model file not found"):
        load_model(model_dir)


def test_unregistered_framework_is_rejected(tmp_path: Path) -> None:
    model_dir = tmp_path / "v1"
    _publish(model_dir)
    _rewrite_manifest_field(model_dir, "framework", "scikit-learn")

    with pytest.raises(ModelLoadError, match="No loader registered"):
        load_model(model_dir)


def test_missing_manifest_is_rejected(tmp_path: Path) -> None:
    model_dir = tmp_path / "v1"
    model_dir.mkdir()

    with pytest.raises(ManifestError, match="No manifest found"):
        load_model(model_dir)


def test_factory_result_must_satisfy_the_model_interface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    model_dir = tmp_path / "v1"
    _publish(model_dir)
    monkeypatch.setitem(model_loader._MODEL_FACTORIES, "stub", lambda path: object())

    with pytest.raises(ModelLoadError, match="does not satisfy"):
        load_model(model_dir)
