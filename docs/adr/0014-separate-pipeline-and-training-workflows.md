# ADR-0014: Separate pipeline and training Workflows

- **Status:** Accepted (Databricks Workflows to be recreated)
- **Date:** 2026-10-12

## Context

With incremental scoring, only new flows are scored. If training ran on every
pipeline run, older predictions would come from older models, and frequency
encodings would change under already-scored rows. Retraining is also far more
expensive than ingesting a few new chunks.

## Decision

Two orchestration units:

| Workflow | Tasks | Cadence |
|---|---|---|
| `cyber-lakehouse-pipeline` | 00 replay → 01 bronze → 02 silver → 03 gold → 05 score (incremental) | repeatedly |
| `cyber-lakehouse-train` | 04 train → 05 score with `full_refresh=true` | on demand |

`--full_refresh` / `full_refresh=true` drops the predictions table and its
checkpoint so the Change Data Feed stream re-snapshots gold and every flow is
rescored with the new model. Scoring exits cleanly ("no model yet") when no
model exists, so the pipeline can run before the first training.

## Alternatives considered

- **One Workflow that always retrains** — simpler, but mixes model versions
  in predictions and wastes compute.
- **Never rescore** — stale predictions after a retrain.

## Consequences

- Predictions always come from a single model version.
- Mirrors the common production split between data pipelines and model
  lifecycle jobs.
- Locally the same split is a run order: jobs 01–03 and 05 repeatedly; 04 and
  05 `--full_refresh` on demand.
