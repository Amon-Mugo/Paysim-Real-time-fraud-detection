"""Score the transaction stream with the published fraud model.

The scoring step wraps the contract's ``score_batch`` in a pandas UDF, so every
batch is validated against the manifest and the output contract. The feature
check runs when the query is built, which makes the job fail at startup, not
mid-stream, if the model needs a column the stream does not provide.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from spark.model_contract import (
    FraudModel,
    ModelManifest,
    check_feature_compatibility,
    score_batch,
)

RULE_NAME = "ml_model"
SCORE_COLUMN = "fraud_score"


def _feature_struct(manifest: ModelManifest) -> Column:
    """Pack the manifest's feature columns into one struct, in manifest order."""
    return F.struct(*[F.col(name) for name in manifest.feature_names])


def _build_score_udf(manifest: ModelManifest, model: FraudModel):
    """Return a pandas UDF that scores a struct of features with ``model``."""

    @F.pandas_udf(DoubleType())
    def score_features(features: pd.DataFrame) -> pd.Series:
        return pd.Series(score_batch(model, manifest, features))

    return score_features


def score_transactions(
    transactions: DataFrame, manifest: ModelManifest, model: FraudModel
) -> DataFrame:
    """Add a fraud score to every valid transaction."""
    check_feature_compatibility(transactions.columns, manifest)
    score_udf = _build_score_udf(manifest, model)
    return transactions.filter(F.col("is_valid_event")).withColumn(
        SCORE_COLUMN, score_udf(_feature_struct(manifest))
    )


def detect_ml_fraud(
    transactions: DataFrame, manifest: ModelManifest, model: FraudModel
) -> DataFrame:
    """Return ML alerts: transactions scoring at or above the alert threshold."""
    scored = score_transactions(transactions, manifest, model)
    return scored.filter(F.col(SCORE_COLUMN) >= F.lit(manifest.alert_threshold)).select(
        "transaction_id",
        "event_time",
        "name_orig",
        "amount",
        SCORE_COLUMN,
        F.lit(manifest.model_version).alias("model_version"),
        F.lit(RULE_NAME).alias("rule"),
    )
