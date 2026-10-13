# ADR-0005: scikit-learn on a stratified sample, not Spark MLlib

- **Status:** Accepted
- **Date:** 2026-08-19

## Context

The gold table has 2,013,620 rows × 54 features. Training must fit in a 2 GB
Spark driver locally and stay within Databricks Free Edition compute quotas,
and the goal is to benchmark several model families.

## Decision

- Take a **stratified sample** in Spark (`sampleBy` on `is_attack`, same
  fraction per class; 500k rows locally, 300k on Databricks), convert to
  pandas as `float32`, and train with **scikit-learn**.
- Benchmark RandomForest (150 trees, depth 20), LogisticRegression (with
  StandardScaler), and HistGradientBoosting; 80/20 stratified split, seed 42.

## Alternatives considered

- **Spark MLlib** — trains on all rows, but its model set and tuning ergonomics
  are narrower, and distributed training buys little at ~2M rows.
- **Full-data pandas** — does not fit in the 2 GB driver.

## Consequences

- Training sees ~25% of gold; the stratified sample preserves the class ratio.
- Scoring still covers every flow ([ADR-0007](0007-distributed-inference-mapinpandas.md)).
- The model is a portable pickle (joblib) and an MLflow sklearn flavor.
- Sample size is a parameter (`--max_rows` / `MAX_ROWS`).
