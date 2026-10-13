# Architecture Decision Records

Each ADR records one significant design decision: the context that forced it,
the decision, the alternatives considered, and the consequences. Format after
Michael Nygard's ADR template. ADRs are immutable once accepted; a changed
decision gets a new ADR that supersedes the old one.

ADRs 0001–0009 were written retrospectively on 2026-10-12 from the code and
commit history; their dates are when the decision was implemented.

| # | Decision | Status | Date |
|---|---|---|---|
| [0001](0001-medallion-lakehouse-on-delta-lake.md) | Medallion lakehouse on Delta Lake | Accepted | 2026-01-30 |
| [0002](0002-full-dataset-with-explicit-schema.md) | Full UNSW-NB15 dataset with an explicit schema | Accepted | 2026-08-19 |
| [0003](0003-sha256-flow-identity-key.md) | SHA-256 flow-identity key for deduplication | Accepted | 2026-08-19 |
| [0004](0004-shared-code-local-and-databricks.md) | One shared code base for local Docker and Databricks | Accepted | 2026-08-19 |
| [0005](0005-scikit-learn-on-stratified-sample.md) | scikit-learn on a stratified sample, not Spark MLlib | Accepted | 2026-08-19 |
| [0006](0006-imbalance-handling-and-f1-model-selection.md) | Class weighting and F1-based model selection | Accepted (selection rule superseded by 0017) | 2026-08-19 |
| [0007](0007-distributed-inference-mapinpandas.md) | Distributed inference with `mapInPandas` | Accepted | 2026-08-19 |
| [0008](0008-try-cast-under-ansi-mode.md) | `try_cast` for dirty string columns under ANSI mode | Accepted | 2026-08-24 |
| [0009](0009-mlflow-tracking-and-uc-registry.md) | MLflow tracking and Unity Catalog model registry | Accepted | 2026-08-24 |
| [0010](0010-incremental-streaming-ingestion.md) | Incremental ingestion with checkpointed streams (`availableNow`) | Accepted | 2026-10-12 |
| [0011](0011-insert-only-merge-first-seen-wins.md) | Insert-only MERGE on `flow_hash` — first seen wins | Accepted | 2026-10-12 |
| [0012](0012-change-data-feed-between-layers.md) | Change Data Feed between layers | Accepted | 2026-10-12 |
| [0013](0013-freeze-frequency-encodings-at-training.md) | Freeze frequency encodings at training time | Accepted | 2026-10-12 |
| [0014](0014-separate-pipeline-and-training-workflows.md) | Separate pipeline and training Workflows | Accepted | 2026-10-12 |
| [0015](0015-bound-batch-size-and-scoring-concurrency.md) | Bound micro-batch size and scoring concurrency | Accepted | 2026-10-12 |
| [0016](0016-testing-strategy.md) | Delta-enabled pytest plus a Docker end-to-end check | Accepted | 2026-10-12 |
| [0017](0017-validation-tuned-alert-threshold.md) | Validation-tuned alert threshold at ≥95% recall | Accepted | 2026-10-12 |
| [0018](0018-separate-attack-family-model.md) | Separate 10-class attack-family model | Accepted | 2026-10-12 |
| [0019](0019-persisted-quality-gate.md) | Data-quality checks as a persisted pipeline gate | Accepted | 2026-10-13 |
| [0020](0020-dashboard-as-code.md) | AI/BI dashboard kept as code, model quality from the test split only | Accepted | 2026-10-13 |

## Template

```markdown
# ADR-NNNN: Title

- **Status:** Proposed | Accepted | Superseded by ADR-XXXX
- **Date:** YYYY-MM-DD

## Context
## Decision
## Alternatives considered
## Consequences
```
