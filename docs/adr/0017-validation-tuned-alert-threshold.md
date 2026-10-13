# ADR-0017: Validation-tuned alert threshold at ≥95% recall

- **Status:** Accepted (supersedes the model-selection rule of [ADR-0006](0006-imbalance-handling-and-f1-model-selection.md))
- **Date:** 2026-10-12

## Context

The alerting model labeled a flow an attack when its probability was ≥ 0.5.
On the test set that gave recall 0.992 but precision 0.766: about 1 in 4
alerts was a false alarm, which is costly for a SOC triaging alerts by hand.
Nothing about 0.5 is special; it was just the default. The model was also
chosen by test-set F1, so the reported numbers were slightly optimistic,
because the same data was used to pick the model and to report it.

## Decision

- Split the stratified sample **60/20/20 train/validation/test**, stratified
  by attack family (`split_train_val_test` in `src/utils/modeling.py`).
- For each candidate model, choose its alert threshold on **validation** as
  the **highest-precision threshold with recall ≥ 95%** (`choose_threshold`,
  `TARGET_RECALL = 0.95`); ties go to higher recall.
- Keep the model with the **highest validation precision at that floor**.
- Report test metrics at the tuned threshold (and at 0.5 for comparison).
  The test split is never used for a choice.
- Persist the threshold in `model_meta.json` next to the model (and as an
  MLflow param on Databricks); scoring reads it, so `pred_is_attack` uses the
  same threshold as the evaluation.

## Alternatives considered

- **Maximize F1** — standard, but it picks whatever balance happens to
  maximize F1, which may cost more recall than a SOC accepts, and is harder
  to explain as a security trade-off.
- **Report several thresholds, keep 0.5** — analysis without changing
  behavior.
- **Cost-weighted threshold** — needs real costs for a missed attack vs. a
  false alarm, which this dataset doesn't provide.

## Consequences

- Selected model changed from RandomForest to **HistGradientBoosting**
  (threshold 0.912): test precision **0.839** (was 0.766), recall 0.945 (was
  0.992), F1 0.889 (was 0.865). False alarms drop from ~1 in 4 alerts to
  ~1 in 6, at the cost of missing ~5.5% of attacks instead of ~1%.
- Test recall (0.945) lands just under the 0.95 validation floor, as expected
  when a threshold tuned on one sample is applied to another.
- Reproduced on Databricks (300k-row sample, 2026-10-13): HistGradientBoosting
  selected again at threshold 0.880, test precision 0.824, recall 0.953,
  F1 0.884.
- Training data shrinks from 80% to 60% of the sample to fund the validation
  split.
- The threshold is now a model artifact that must stay in sync with the
  model, handled the same way as the frozen encodings
  ([ADR-0013](0013-freeze-frequency-encodings-at-training.md)).
