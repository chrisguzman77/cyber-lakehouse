# ADR-0006: Class weighting and F1-based model selection

- **Status:** Accepted (model-selection rule superseded by [ADR-0017](0017-validation-tuned-alert-threshold.md))
- **Date:** 2026-08-19

## Context

After deduplication only ~4% of flows are attacks. A model that always
predicts "benign" scores ~96% accuracy, so accuracy says almost nothing about
detection quality.

## Decision

- Train every model with `class_weight="balanced"`.
- Report accuracy, precision, recall, F1, and ROC-AUC for every model.
- Select the saved model by **F1** on the held-out split.

## Alternatives considered

- **Select by accuracy** — rewards ignoring the minority class.
- **Select by ROC-AUC** — threshold-independent, but all three models are
  ≥0.996 and it doesn't reflect the operating point actually used (0.5).
- **Resampling (SMOTE/undersampling)** — more moving parts; class weights get
  the same effect for these models.

## Consequences

- RandomForest is selected (F1 0.8646, recall 0.9922, precision 0.7660).
- Precision of ~0.77 means roughly 1 in 4 alerts is a false positive at the
  default threshold; threshold tuning on a validation set is planned work.
