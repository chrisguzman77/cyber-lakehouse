# ADR-0009: MLflow tracking and Unity Catalog model registry

- **Status:** Accepted
- **Date:** 2026-08-24

## Context

Three models are benchmarked on each training run. Results need to be
comparable across runs, and the selected model should be versioned and
governed, not just a file on a Volume.

## Decision

On Databricks, notebook 04:

- Logs one **MLflow run per model** with params (model, seed, sample size,
  feature count) and metrics (accuracy, precision, recall, F1, ROC-AUC).
- Writes the comparison to `gold.model_metrics`.
- Registers the best model in the **Unity Catalog model registry** as
  `cyber_lakehouse.gold.attack_classifier` (signature inferred from an input
  example, required for UC registration).

Locally, the same metrics go to `reports/metrics.json` (committed) and the
model to `models/trained_model.pkl` (gitignored).

## Consequences

- Run-over-run comparison in the MLflow UI; versioned, governed model.
- Scoring still loads the joblib artifact from the Volume, keeping the local
  and cloud scoring paths identical.
- The registered model does not embed the frozen frequency encodings
  ([ADR-0013](0013-freeze-frequency-encodings-at-training.md)); a pyfunc
  wrapper would make it self-contained.
