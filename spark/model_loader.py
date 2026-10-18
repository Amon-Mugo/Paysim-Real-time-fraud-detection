"""Load a published model version from a local directory.

The loader reads and validates the manifest, then builds the model with the
factory registered for the manifest's framework. Adding the trained model's
framework later means registering one more factory here and nothing else.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from spark.model_contract import (
    ContractError,
    FraudModel,
    ModelManifest,
    load_manifest,
)
from spark.stub_model import STUB_FRAMEWORK, StubFraudModel

ModelFactory = Callable[[Path], FraudModel]


class ModelLoadError(ContractError):
    """The model artifact could not be turned into a usable model."""


def _load_stub_model(model_path: Path) -> FraudModel:
    """Build the stub model from its JSON file."""
    return StubFraudModel.from_file(model_path)


_MODEL_FACTORIES: dict[str, ModelFactory] = {
    STUB_FRAMEWORK: _load_stub_model,
}


def _factory_for(manifest: ModelManifest) -> ModelFactory:
    """Return the factory registered for the manifest's framework."""
    try:
        return _MODEL_FACTORIES[manifest.framework]
    except KeyError as exc:
        supported = sorted(_MODEL_FACTORIES)
        raise ModelLoadError(
            f"No loader registered for framework '{manifest.framework}'; "
            f"supported frameworks: {supported}."
        ) from exc


def _check_directory_matches_version(manifest: ModelManifest, model_dir: Path) -> None:
    """Enforce the contract rule that the directory name is the model version."""
    if model_dir.name != manifest.model_version:
        raise ModelLoadError(
            f"Directory '{model_dir.name}' does not match manifest "
            f"model_version '{manifest.model_version}'."
        )


def _check_model_file_exists(manifest: ModelManifest, model_dir: Path) -> Path:
    """Return the model file path, failing if the file is missing."""
    model_path = manifest.model_path(model_dir)
    if not model_path.is_file():
        raise ModelLoadError(f"Model file not found at {model_path}.")
    return model_path


def load_model(model_dir: Path) -> tuple[ModelManifest, FraudModel]:
    """Load and validate one published model version from ``model_dir``."""
    manifest = load_manifest(model_dir)
    _check_directory_matches_version(manifest, model_dir)
    model_path = _check_model_file_exists(manifest, model_dir)

    model = _factory_for(manifest)(model_path)
    if not isinstance(model, FraudModel):
        raise ModelLoadError(
            f"Loaded object {type(model).__name__} does not satisfy the "
            "FraudModel interface."
        )
    return manifest, model
