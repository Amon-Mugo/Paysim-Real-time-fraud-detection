"""Stub fraud model used to build M6 before the real model artifact exists.

The stub scores each row by scaling one numeric feature into [0, 1]. It is
published in the same directory layout as a real model (manifest plus model
file), so the loader, feature check and scoring step run against the real
contract. Replacing it with the trained model only changes the loader.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from spark.model_contract import (
    MANIFEST_FILENAME,
    SUPPORTED_SCHEMA_VERSION,
    ContractError,
    ModelManifest,
)

STUB_FRAMEWORK = "stub"
STUB_MODEL_FILENAME = "stub_model.json"


class StubModelError(ContractError):
    """The stub model file or its settings are invalid."""


def _read_json_object(path: Path) -> dict[str, Any]:
    """Read ``path`` and return its content, which must be a JSON object."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise StubModelError(f"No stub model file found at {path}.") from exc
    except json.JSONDecodeError as exc:
        raise StubModelError(f"Stub model file is not valid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise StubModelError("Stub model file must contain a JSON object.")
    return raw


@dataclass(frozen=True)
class StubFraudModel:
    """Scores a row as ``score_feature / scale``, clipped to [0, 1]."""

    score_feature: str
    scale: float

    def __post_init__(self) -> None:
        if not isinstance(self.score_feature, str) or not self.score_feature.strip():
            raise StubModelError("score_feature must be a non-empty string.")
        if not self.scale > 0:
            raise StubModelError("scale must be positive.")

    @classmethod
    def from_file(cls, path: Path) -> StubFraudModel:
        """Load the stub model from its JSON file."""
        raw = _read_json_object(path)
        try:
            score_feature = raw["score_feature"]
            scale = float(raw["scale"])
        except KeyError as exc:
            raise StubModelError(f"Stub model file is missing field {exc}.") from exc
        except (TypeError, ValueError) as exc:
            raise StubModelError("Stub model field 'scale' must be a number.") from exc
        return cls(score_feature=score_feature, scale=scale)

    def predict_fraud_probability(self, features: pd.DataFrame) -> np.ndarray:
        """Return one probability per row; missing values score as zero."""
        values = features[self.score_feature].to_numpy(dtype=float)
        return np.clip(np.nan_to_num(values) / self.scale, 0.0, 1.0)


def write_stub_artifact(
    model_dir: Path,
    *,
    model_version: str,
    feature_names: tuple[str, ...],
    score_feature: str,
    scale: float,
    alert_threshold: float,
) -> ModelManifest:
    """Publish a stub model version into ``model_dir`` and return its manifest.

    The model file is written first and the manifest last, so a directory with
    a manifest is always complete. An existing version is never overwritten.
    """
    manifest_path = model_dir / MANIFEST_FILENAME
    if manifest_path.exists():
        raise FileExistsError(f"Model version already published at {model_dir}.")
    if score_feature not in feature_names:
        raise StubModelError("score_feature must be one of feature_names.")

    model = StubFraudModel(score_feature=score_feature, scale=scale)
    manifest = ModelManifest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        model_version=model_version,
        framework=STUB_FRAMEWORK,
        model_file=STUB_MODEL_FILENAME,
        feature_names=tuple(feature_names),
        alert_threshold=alert_threshold,
        trained_at=datetime.now(timezone.utc).isoformat(),
    )

    model_dir.mkdir(parents=True, exist_ok=True)
    model_payload = {"score_feature": model.score_feature, "scale": model.scale}
    manifest.model_path(model_dir).write_text(
        json.dumps(model_payload, indent=2), encoding="utf-8"
    )
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": manifest.schema_version,
                "model_version": manifest.model_version,
                "framework": manifest.framework,
                "model_file": manifest.model_file,
                "feature_names": list(manifest.feature_names),
                "alert_threshold": manifest.alert_threshold,
                "trained_at": manifest.trained_at,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return manifest
