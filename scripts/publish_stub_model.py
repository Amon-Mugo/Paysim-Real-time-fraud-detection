"""Publish the stub fraud model into the configured model directory.

Run from the repository root with ``python -m scripts.publish_stub_model``.
The stub stands in for the trained model until the ML artifact exists. It
writes a real manifest and model file, so the job loads it through the same
path as the final model.
"""

from __future__ import annotations

import logging
from pathlib import Path

from spark.config import DetectionConfig
from spark.stub_model import write_stub_artifact

logger = logging.getLogger(__name__)

STUB_FEATURE_NAMES = ("amount", "old_balance_orig", "new_balance_orig")
STUB_SCORE_FEATURE = "amount"
STUB_SCALE = 1_000_000.0
STUB_ALERT_THRESHOLD = 0.8


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    model_dir = Path(DetectionConfig.from_env().model_dir)
    try:
        manifest = write_stub_artifact(
            model_dir,
            model_version=model_dir.name,
            feature_names=STUB_FEATURE_NAMES,
            score_feature=STUB_SCORE_FEATURE,
            scale=STUB_SCALE,
            alert_threshold=STUB_ALERT_THRESHOLD,
        )
    except FileExistsError as exc:
        logger.error("%s Delete the directory to publish again.", exc)
        raise SystemExit(1) from exc
    logger.info("Published stub model %s to %s", manifest.model_version, model_dir)


if __name__ == "__main__":
    main()
