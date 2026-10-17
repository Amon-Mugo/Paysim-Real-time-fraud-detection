"""Unit tests for the model-loading contract in spark/model_contract.py."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from spark.model_contract import (
    ContractError,
    FeatureSchemaError,
    FraudModel,
    ManifestError,
    ModelManifest,
    ModelOutputError,
    check_feature_compatibility,
    load_manifest,
    parse_manifest,
    score_batch,
    select_features,
    validate_scores,
)

FEATURES = ("amount", "oldbalanceOrg", "newbalanceOrig")


class StubModel:
    """Minimal model satisfying the FraudModel protocol, with call tracking."""

    def __init__(self, scores: Any = None) -> None:
        self._scores = scores
        self.calls = 0
        self.received: pd.DataFrame | None = None

    def predict_fraud_probability(self, features: pd.DataFrame) -> Any:
        self.calls += 1
        self.received = features
        if self._scores is None:
            return np.full(len(features), 0.5)
        return self._scores


def build_manifest_dict(**overrides: Any) -> dict[str, Any]:
    """Return a valid manifest as a dict, with optional field overrides."""
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "model_version": "2026-10-20-01",
        "framework": "scikit-learn",
        "model_file": "model.joblib",
        "feature_names": list(FEATURES),
        "alert_threshold": 0.8,
        "trained_at": "2026-10-20T09:30:00+00:00",
    }
    manifest.update(overrides)
    return manifest


def build_manifest_json(**overrides: Any) -> str:
    """Return a valid manifest serialized as JSON text."""
    return json.dumps(build_manifest_dict(**overrides))


def build_manifest() -> ModelManifest:
    """Return a parsed, valid manifest."""
    return parse_manifest(build_manifest_json())


def build_features() -> pd.DataFrame:
    """Return three transactions with shuffled columns and one extra column."""
    return pd.DataFrame(
        {
            "type": ["TRANSFER", "CASH_OUT", "TRANSFER"],
            "oldbalanceOrg": [1000.0, 50.0, 0.0],
            "amount": [900.0, 50.0, 10.0],
            "newbalanceOrig": [100.0, 0.0, 0.0],
        }
    )


def describe_overrides(overrides: dict[str, Any]) -> str:
    """Build a readable pytest id for a parametrized override dict."""
    return ",".join(f"{key}={value!r}" for key, value in overrides.items())


# --- Manifest parsing -------------------------------------------------------


def test_valid_manifest_is_parsed() -> None:
    manifest = build_manifest()

    assert manifest.schema_version == 1
    assert manifest.model_version == "2026-10-20-01"
    assert manifest.framework == "scikit-learn"
    assert manifest.model_file == "model.joblib"
    assert manifest.feature_names == FEATURES
    assert isinstance(manifest.feature_names, tuple)
    assert manifest.alert_threshold == 0.8
    assert manifest.trained_at == "2026-10-20T09:30:00+00:00"


def test_manifest_is_immutable() -> None:
    manifest = build_manifest()

    with pytest.raises(AttributeError):
        manifest.model_version = "other"  # type: ignore[misc]


def test_model_path_is_inside_the_model_directory() -> None:
    manifest = build_manifest()

    assert manifest.model_path(Path("/models/fraud/v1")) == Path(
        "/models/fraud/v1/model.joblib"
    )


def test_load_manifest_reads_manifest_from_directory(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(build_manifest_json(), encoding="utf-8")

    manifest = load_manifest(tmp_path)

    assert manifest.model_version == "2026-10-20-01"


def test_load_manifest_reports_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ManifestError, match="No manifest found"):
        load_manifest(tmp_path)


def test_invalid_json_is_rejected() -> None:
    with pytest.raises(ManifestError, match="not valid JSON"):
        parse_manifest("{not json")


def test_non_object_json_is_rejected() -> None:
    with pytest.raises(ManifestError, match="JSON object"):
        parse_manifest("[]")


@pytest.mark.parametrize("field", list(build_manifest_dict()))
def test_missing_required_field_is_rejected(field: str) -> None:
    raw = build_manifest_dict()
    del raw[field]

    with pytest.raises(ManifestError, match=field):
        parse_manifest(json.dumps(raw))


@pytest.mark.parametrize(
    "overrides",
    [
        {"schema_version": "1"},
        {"schema_version": True},
        {"model_version": 5},
        {"framework": None},
        {"model_file": ["model.joblib"]},
        {"feature_names": "amount"},
        {"alert_threshold": "0.8"},
        {"alert_threshold": True},
        {"trained_at": 20261020},
    ],
    ids=describe_overrides,
)
def test_wrong_field_type_is_rejected(overrides: dict[str, Any]) -> None:
    with pytest.raises(ManifestError, match="unexpected type"):
        parse_manifest(build_manifest_json(**overrides))


def test_non_string_feature_name_is_rejected() -> None:
    with pytest.raises(ManifestError, match="only strings"):
        parse_manifest(build_manifest_json(feature_names=["amount", 3]))


def test_unsupported_schema_version_is_rejected() -> None:
    with pytest.raises(ManifestError, match="Unsupported manifest schema_version"):
        parse_manifest(build_manifest_json(schema_version=2))


@pytest.mark.parametrize("field", ["model_version", "framework", "model_file"])
def test_blank_text_field_is_rejected(field: str) -> None:
    with pytest.raises(ManifestError, match="must not be empty"):
        parse_manifest(build_manifest_json(**{field: "  "}))


def test_model_version_containing_a_slash_is_rejected() -> None:
    with pytest.raises(ManifestError, match="must not contain '/'"):
        parse_manifest(build_manifest_json(model_version="a/b"))


@pytest.mark.parametrize(
    "model_file", ["../model.joblib", "sub/model.joblib", "..", "."]
)
def test_model_file_that_is_not_a_bare_name_is_rejected(model_file: str) -> None:
    with pytest.raises(ManifestError, match="bare file name"):
        parse_manifest(build_manifest_json(model_file=model_file))


def test_empty_feature_list_is_rejected() -> None:
    with pytest.raises(ManifestError, match="must not be empty"):
        parse_manifest(build_manifest_json(feature_names=[]))


def test_blank_feature_name_is_rejected() -> None:
    with pytest.raises(ManifestError, match="blanks"):
        parse_manifest(build_manifest_json(feature_names=["amount", " "]))


def test_duplicate_feature_names_are_rejected() -> None:
    with pytest.raises(ManifestError, match="duplicates"):
        parse_manifest(build_manifest_json(feature_names=["amount", "amount"]))


@pytest.mark.parametrize("threshold", [0, -0.1, 1.5])
def test_alert_threshold_outside_range_is_rejected(threshold: float) -> None:
    with pytest.raises(ManifestError, match="range"):
        parse_manifest(build_manifest_json(alert_threshold=threshold))


@pytest.mark.parametrize("threshold", [0.01, 0.5, 1, 1.0])
def test_alert_threshold_inside_range_is_accepted(threshold: float) -> None:
    manifest = parse_manifest(build_manifest_json(alert_threshold=threshold))

    assert manifest.alert_threshold == float(threshold)


def test_invalid_trained_at_is_rejected() -> None:
    with pytest.raises(ManifestError, match="ISO 8601"):
        parse_manifest(build_manifest_json(trained_at="yesterday"))


@pytest.mark.parametrize(
    "error_class", [ManifestError, FeatureSchemaError, ModelOutputError]
)
def test_contract_errors_share_a_common_base(error_class: type) -> None:
    assert issubclass(error_class, ContractError)
    assert issubclass(error_class, ValueError)


# --- Feature compatibility --------------------------------------------------


def test_compatibility_check_accepts_extra_columns() -> None:
    check_feature_compatibility([*FEATURES, "type"], build_manifest())


def test_compatibility_check_lists_missing_features_in_manifest_order() -> None:
    with pytest.raises(FeatureSchemaError) as excinfo:
        check_feature_compatibility(["oldbalanceOrg"], build_manifest())

    message = str(excinfo.value)
    assert "['amount', 'newbalanceOrig']" in message
    assert "2026-10-20-01" in message


def test_select_features_orders_columns_and_drops_extras() -> None:
    features = build_features()

    selected = select_features(features, build_manifest())

    assert list(selected.columns) == list(FEATURES)
    assert selected["amount"].tolist() == [900.0, 50.0, 10.0]
    assert "type" in features.columns


def test_select_features_rejects_missing_columns() -> None:
    features = build_features().drop(columns=["amount"])

    with pytest.raises(FeatureSchemaError):
        select_features(features, build_manifest())


# --- Score validation -------------------------------------------------------


def test_valid_scores_are_returned_as_a_float_array() -> None:
    result = validate_scores([0, 0.5, 1], 3)

    np.testing.assert_array_equal(result, [0.0, 0.5, 1.0])
    assert result.dtype == float


def test_two_dimensional_scores_are_rejected() -> None:
    with pytest.raises(ModelOutputError, match="one-dimensional"):
        validate_scores(np.array([[0.1], [0.2]]), 2)


def test_score_count_mismatch_is_rejected() -> None:
    with pytest.raises(ModelOutputError, match="2 scores for 3 rows"):
        validate_scores([0.1, 0.2], 3)


@pytest.mark.parametrize("scores", [[0.1, float("nan")], [float("inf"), 0.2]])
def test_non_finite_scores_are_rejected(scores: list[float]) -> None:
    with pytest.raises(ModelOutputError, match="finite"):
        validate_scores(scores, 2)


@pytest.mark.parametrize("scores", [[-0.01, 0.5], [0.5, 1.01]])
def test_scores_outside_probability_range_are_rejected(scores: list[float]) -> None:
    with pytest.raises(ModelOutputError, match="within"):
        validate_scores(scores, 2)


def test_non_numeric_scores_are_rejected() -> None:
    with pytest.raises(ModelOutputError, match="numeric"):
        validate_scores(["high", "low"], 2)


# --- Model protocol and score_batch -----------------------------------------


def test_protocol_recognises_conforming_models_only() -> None:
    assert isinstance(StubModel(), FraudModel)
    assert not isinstance(object(), FraudModel)


def test_score_batch_passes_ordered_features_and_returns_scores() -> None:
    model = StubModel([0.1, 0.9, 0.4])

    result = score_batch(model, build_manifest(), build_features())

    np.testing.assert_array_equal(result, [0.1, 0.9, 0.4])
    assert model.received is not None
    assert list(model.received.columns) == list(FEATURES)


def test_score_batch_returns_empty_array_without_calling_the_model() -> None:
    model = StubModel()

    result = score_batch(model, build_manifest(), build_features().iloc[0:0])

    assert result.shape == (0,)
    assert model.calls == 0


def test_score_batch_rejects_missing_features_before_scoring() -> None:
    model = StubModel()
    features = build_features().drop(columns=["amount"])

    with pytest.raises(FeatureSchemaError):
        score_batch(model, build_manifest(), features)

    assert model.calls == 0


def test_score_batch_rejects_wrong_number_of_scores() -> None:
    with pytest.raises(ModelOutputError):
        score_batch(StubModel([0.1]), build_manifest(), build_features())


def test_score_batch_rejects_out_of_range_scores() -> None:
    with pytest.raises(ModelOutputError):
        score_batch(StubModel([0.1, 1.4, 0.2]), build_manifest(), build_features())
