# Model-Loading Contract

This document defines what the ML training pipeline must publish and what the
streaming job (`spark/`) expects when it loads and scores with a model. The
rules are enforced in code by `spark/model_contract.py` and covered by
`tests/test_model_contract.py`.

## Scope

- The ML side owns training, evaluation of the model itself, and publishing
  artifacts.
- The pipeline side owns loading, validating, and scoring with the artifact
  inside the Spark Structured Streaming job.
- Anything not stated here is not guaranteed. Changes to this contract are
  agreed by both sides before either side relies on them.

## Artifact layout

Each trained model version is published as its own directory under the
`models/fraud/` prefix of the curated S3 bucket:

```text
models/fraud/<model_version>/
    manifest.json
    <model_file>
```

Rules:

- A published version is immutable. A retrained model is published as a new
  version and never overwrites an existing directory.
- `<model_version>` is a unique identifier such as `2026-10-20-01`. It must not
  contain `/`.
- `manifest.json` and the model file sit side by side in the same directory.

## Manifest

`manifest.json` is a single JSON object with these fields, all required:

| Field             | Type            | Rules                                                          |
| ----------------- | --------------- | -------------------------------------------------------------- |
| `schema_version`  | integer         | Must be `1`.                                                   |
| `model_version`   | string          | Non-empty, no `/`. Matches the directory name.                 |
| `framework`       | string          | Non-empty, for example `scikit-learn`.                         |
| `model_file`      | string          | Bare file name inside the directory, no path components.       |
| `feature_names`   | list of strings | Non-empty, no blanks, no duplicates. Order is significant.     |
| `alert_threshold` | number          | In the range (0, 1]. Scores at or above it raise an ML alert.  |
| `trained_at`      | string          | ISO 8601 timestamp.                                            |

Example:

```json
{
  "schema_version": 1,
  "model_version": "2026-10-20-01",
  "framework": "scikit-learn",
  "model_file": "model.joblib",
  "feature_names": ["amount", "oldbalanceOrg", "newbalanceOrig"],
  "alert_threshold": 0.8,
  "trained_at": "2026-10-20T09:30:00+00:00"
}
```

## Features

- `feature_names` lists exactly the columns the model was trained on, in the
  order it was trained on them.
- Every feature must be a column of the streaming job's transaction schema
  (`spark/schema.py`), or a derived column that the streaming job has been
  changed to produce. Derived features are agreed with the pipeline side
  before training, because the streaming job has to compute them.
- Labels, `isFraud`, `isFlaggedFraud`, and any value that depends on future
  events must never be used as features. The streaming job never receives
  labels.
- Features that require per-account state, such as velocity counts, are
  produced by the rule layer in Spark. A model may use them only if the
  pipeline side agrees to expose them as columns.

## Scoring interface

Once loaded, the model must satisfy this interface:

```python
def predict_fraud_probability(self, features: pd.DataFrame) -> np.ndarray:
    ...
```

- `features` has exactly the columns in `feature_names`, in that order.
- The return value is a one-dimensional array with one fraud probability per
  input row, in input order.
- Every probability is finite and within [0, 1].
- Scoring is a pure function of the input rows. It must not read external
  state or depend on call order.

## Validation performed by the streaming job

The job fails loudly instead of scoring with a model that breaks the contract.

| Check                                                    | When       | Failure raised       |
| -------------------------------------------------------- | ---------- | -------------------- |
| Manifest is valid JSON and every field passes its rules  | Startup    | `ManifestError`      |
| Every name in `feature_names` exists in the stream       | Startup    | `FeatureSchemaError` |
| Scores are numeric, one-dimensional, one per row         | Every batch | `ModelOutputError`  |
| Scores are finite and within [0, 1]                      | Every batch | `ModelOutputError`  |

## Compatibility and versioning

- `schema_version` changes only when the manifest format changes in a way that
  old readers cannot handle. The streaming job rejects any value other than the
  one it supports.
- Adding or removing a feature is a new `model_version` and may also need a
  pipeline change, so it is agreed in advance.

## Open items

- **Serialization format.** The model file format and the code that turns it
  into an object satisfying the scoring interface depend on the framework used
  for training. They are agreed with the ML side and implemented in M6.
- **Derived features.** If the model needs features beyond the raw transaction
  columns, the list is agreed before the first training run.
- **Model selection at runtime.** How the streaming job chooses which published
  version to load (a configured version string, for example) is decided in M6.

## Checklist for publishing a model version

- The directory name equals `model_version` in the manifest.
- `manifest.json` passes the rules above.
- `feature_names` matches the training columns, in training order.
- The model returns probabilities in [0, 1], one per row, for a sample batch
  built from the listed features.
- No label or future-dependent column is among the features.