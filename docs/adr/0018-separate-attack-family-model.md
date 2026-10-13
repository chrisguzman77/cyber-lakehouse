# ADR-0018: Separate 10-class attack-family model

- **Status:** Accepted
- **Date:** 2026-10-12

## Context

The pipeline answered only "attack or not?". An analyst also needs "what kind
of attack?" to triage: a reconnaissance scan and an exploit warrant different
responses. Silver already carries the normalized family label
(`attack_cat_norm`: normal + 9 families), but it was unused by the models.

## Decision

- Train a **separate multiclass model** on the same 54 features to predict
  `attack_cat_norm` (10 classes), benchmarking the same three model families
  with `class_weight="balanced"` (`benchmark_family` in
  `src/utils/modeling.py`).
- Select by **validation macro-F1**; report per-class precision, recall, F1,
  support, and the confusion matrix on test.
- Save it as `attack_family_model.pkl` beside the binary model. Scoring adds a
  `pred_attack_cat` column for every flow; on Databricks it is also
  registered as `cyber_lakehouse.gold.attack_family_classifier`.
- The **binary model remains the detector**. The family label describes alerts;
  it is not a second detector.

## Alternatives considered

- **Two-stage** (family model only on flagged flows) — more realistic but
  more moving parts, and its evaluation depends on the binary model's errors.
- **One 10-class model for everything** — derive "attack" from "not normal";
  loses the tunable alert threshold of
  [ADR-0017](0017-validation-tuned-alert-threshold.md).
- **Evaluation only, no saved model** — less useful downstream (dashboard).

## Consequences

- RandomForest selected: test macro-F1 **0.585**, accuracy 0.980.
  Accuracy is dominated by `normal`; macro-F1 and the per-class table are the
  honest summary.
- Strong on `generic` (F1 0.94), `reconnaissance` (0.88), `exploits` (0.82);
  weak on `dos` (0.36, about half labeled `exploits`), `analysis` (0.21),
  `backdoor` (0.40), and `worms` (0 of 7 test flows). 1,245 benign flows are
  labeled `fuzzers`, which drags fuzzers' precision to 0.43.
- The two models can disagree (e.g. a flow labeled `analysis` but below the
  alert threshold), which is why the family label is only meaningful on alerts.
- Each scoring worker now loads both models; the family RandomForest is 94 MB
  on disk, so local scoring runs 2 Python workers instead of 4
  ([ADR-0015](0015-bound-batch-size-and-scoring-concurrency.md)).
- Very rare families can't be stratified on their own in small samples;
  families with fewer than 5 rows share one stratum (`MIN_STRATUM`), while
  the models still train on real labels.
- Multiclass LogisticRegression is slow (~200 s of the ~6 min local training)
  and scores worst; it stays in the benchmark as a baseline.
