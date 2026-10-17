# Model-loading contract between the streaming job and the ML artifact.


from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Protocol, runtime_checkable

import numpy as np
import pandas as pd

MANIFEST_FILENAME = "manifest.json"
SUPPORTED_SCHEMA_VERSION = 1


class ContractError(ValueError):
    """Base class for every violation of the model-loading contract."""


class ManifestError(ContractError):
    """The manifest is missing, malformed, or internally inconsistent."""


class FeatureSchemaError(ContractError):
    """The available features do not satisfy the manifest's feature list."""


class ModelOutputError(ContractError):
    """The model returned scores that break the output contract."""


def _require_field(
    raw: Mapping[str, Any],
    key: str,
    expected_type: type | tuple[type, ...],
) -> Any:
    """Return ``raw[key]`` after checking it exists and has the expected type."""
    if key not in raw:
        raise ManifestError(f"Manifest is missing required field '{key}'.")
    value = raw[key]
    if isinstance(value, bool) or not isinstance(value, expected_type):
        raise ManifestError(
            f"Manifest field '{key}' has unexpected type {type(value).__name__}."
        )
    return value


@dataclass(frozen=True)
class ModelManifest:
    """Validated description of one published model version."""

    schema_version: int
    model_version: str
    framework: str
    model_file: str
    feature_names: tuple[str, ...]
    alert_threshold: float
    trained_at: str

    def __post_init__(self) -> None:
        if self.schema_version != SUPPORTED_SCHEMA_VERSION:
            raise ManifestError(
                f"Unsupported manifest schema_version {self.schema_version}; "
                f"expected {SUPPORTED_SCHEMA_VERSION}."
            )
        for field_name in ("model_version", "framework", "model_file"):
            if not getattr(self, field_name).strip():
                raise ManifestError(f"Manifest field '{field_name}' must not be empty.")
        if "/" in self.model_version:
            raise ManifestError("Manifest field 'model_version' must not contain '/'.")
        if Path(self.model_file).name != self.model_file or self.model_file in {
            ".",
            "..",
        }:
            raise ManifestError(
                "Manifest field 'model_file' must be a bare file name inside "
                "the model directory."
            )
        if not self.feature_names:
            raise ManifestError("Manifest field 'feature_names' must not be empty.")
        if any(not name.strip() for name in self.feature_names):
            raise ManifestError("Manifest 'feature_names' must not contain blanks.")
        if len(set(self.feature_names)) != len(self.feature_names):
            raise ManifestError("Manifest 'feature_names' must not contain duplicates.")
        if not 0.0 < self.alert_threshold <= 1.0:
            raise ManifestError(
                "Manifest field 'alert_threshold' must be in the range (0, 1]."
            )
        try:
            datetime.fromisoformat(self.trained_at)
        except ValueError as exc:
            raise ManifestError(
                "Manifest field 'trained_at' must be an ISO 8601 timestamp."
            ) from exc

    def model_path(self, model_dir: Path) -> Path:
        """Return the location of the serialized model inside ``model_dir``."""
        return model_dir / self.model_file


def parse_manifest(raw_text: str) -> ModelManifest:
    """Parse and validate manifest JSON text."""
    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ManifestError(f"Manifest is not valid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise ManifestError("Manifest must be a JSON object.")

    feature_names = _require_field(raw, "feature_names", list)
    if not all(isinstance(name, str) for name in feature_names):
        raise ManifestError("Manifest 'feature_names' must contain only strings.")

    return ModelManifest(
        schema_version=_require_field(raw, "schema_version", int),
        model_version=_require_field(raw, "model_version", str),
        framework=_require_field(raw, "framework", str),
        model_file=_require_field(raw, "model_file", str),
        feature_names=tuple(feature_names),
        alert_threshold=float(_require_field(raw, "alert_threshold", (int, float))),
        trained_at=_require_field(raw, "trained_at", str),
    )


def load_manifest(model_dir: Path) -> ModelManifest:
    """Read and validate ``manifest.json`` from a local model directory."""
    manifest_path = model_dir / MANIFEST_FILENAME
    try:
        raw_text = manifest_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ManifestError(f"No manifest found at {manifest_path}.") from exc
    return parse_manifest(raw_text)


@runtime_checkable
class FraudModel(Protocol):
    """Scoring interface every model artifact must satisfy once loaded.

    ``predict_fraud_probability`` receives a DataFrame whose columns are exactly
    ``manifest.feature_names``, in that order, and returns a one-dimensional
    array holding one fraud probability in [0, 1] per input row.
    """

    def predict_fraud_probability(self, features: pd.DataFrame) -> np.ndarray:
        """Return one fraud probability per row of ``features``."""
        ...


def _missing_features(
    available_columns: Iterable[str], manifest: ModelManifest
) -> list[str]:
    """Return manifest features absent from ``available_columns``, in order."""
    available = set(available_columns)
    return [name for name in manifest.feature_names if name not in available]


def check_feature_compatibility(
    available_columns: Iterable[str], manifest: ModelManifest
) -> None:
    """Fail fast at startup if the stream cannot supply the model's features."""
    missing = _missing_features(available_columns, manifest)
    if missing:
        raise FeatureSchemaError(
            f"Model {manifest.model_version} requires features that the stream "
            f"does not provide: {missing}."
        )


def select_features(features: pd.DataFrame, manifest: ModelManifest) -> pd.DataFrame:
    """Return ``features`` restricted and ordered to the manifest's feature list."""
    check_feature_compatibility(features.columns, manifest)
    return features.loc[:, list(manifest.feature_names)]


def validate_scores(scores: Any, expected_rows: int) -> np.ndarray:
    """Check the model output and return it as a one-dimensional float array."""
    try:
        array = np.asarray(scores, dtype=float)
    except (TypeError, ValueError) as exc:
        raise ModelOutputError("Model scores must be numeric.") from exc
    if array.ndim != 1:
        raise ModelOutputError(
            f"Model scores must be one-dimensional, got {array.ndim} dimensions."
        )
    if array.shape[0] != expected_rows:
        raise ModelOutputError(
            f"Model returned {array.shape[0]} scores for {expected_rows} rows."
        )
    if not np.all(np.isfinite(array)):
        raise ModelOutputError("Model scores must be finite.")
    if np.any((array < 0.0) | (array > 1.0)):
        raise ModelOutputError("Model scores must lie within [0, 1].")
    return array


def score_batch(
    model: FraudModel, manifest: ModelManifest, features: pd.DataFrame
) -> np.ndarray:
    """Score a batch of transactions and enforce the output contract.

    This is the single entry point the streaming job calls from inside its
    pandas UDF.
    """
    if features.empty:
        return np.empty(0, dtype=float)
    selected = select_features(features, manifest)
    return validate_scores(model.predict_fraud_probability(selected), len(selected))
