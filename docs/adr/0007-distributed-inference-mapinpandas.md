# ADR-0007: Distributed inference with `mapInPandas`

- **Status:** Accepted
- **Date:** 2026-08-19

## Context

The model is a scikit-learn estimator, but every one of the ~2M gold flows
must be scored, and the full table doesn't fit in one pandas DataFrame on a
2 GB driver.

## Decision

Score with Spark's `mapInPandas`: each partition streams Arrow batches into a
Python function that loads the model **once per partition** and yields
`flow_hash`, `pred_proba`, `pred_is_attack` (threshold 0.5). Feature columns
are selected by the model's `feature_names_in_` so order always matches
training.

## Alternatives considered

- **`toPandas` + `predict`** — doesn't fit in memory.
- **Row-at-a-time Python UDF** — orders of magnitude slower (per-row
  serialization).
- **`mlflow.pyfunc.spark_udf`** — viable on Databricks, but adds an MLflow
  dependency to the local path.

## Consequences

- Memory is bounded by partition size, not table size.
- Each concurrent Python worker holds its own copy of the model (~230 MB
  resident with sklearn), so worker concurrency must be bounded on small
  machines ([ADR-0015](0015-bound-batch-size-and-scoring-concurrency.md)).
- The scoring closure must not import `src` modules, since executors on
  serverless may not have the Git folder on their path.
