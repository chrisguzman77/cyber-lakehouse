# Changelog

What changed in Cyber Lakehouse and when, newest first. Dates are when the
work was merged to `main`. The reasoning behind each change is in the linked
ADRs ([`docs/adr/`](docs/adr/README.md)).

## 2026-10-14

- **README architecture is now a Mermaid diagram** that GitHub renders. Training
  is drawn as the on-demand branch it is, not a step between gold and
  predictions (PR #8).
- **Cross-dataset test on CIC-IDS2017.**
  - The detector is retrained on 19 features that both datasets share and
    scored on a second dataset.
  - It does not transfer: CIC F1 0.028, ROC-AUC 0.47. A model trained on CIC
    with the same features reaches F1 0.977. The README now states that the
    0.89 F1 is a within-dataset result.
  - Adds `scripts/fetch_cic.sh`, `src/jobs/evaluate_cross_dataset.py`, and
    `reports/cross_dataset.json` (PR #7,
    [ADR-0021](docs/adr/0021-cross-dataset-evaluation-on-portable-features.md)).
- **Dashboard published on Databricks**, with screenshots in the README (PR #6).
- **Dashboard polish** after the first import: separated headings,
  per-capture-session time charts, 3-decimal model tables, and tables for
  run history (PR #5).

## 2026-10-13

- **Data-quality gate:** 11 checks after every pipeline run. Results are
  appended to `gold.quality_results`, and any failure fails the run
  ([ADR-0019](docs/adr/0019-persisted-quality-gate.md)).
- **Three-page Databricks AI/BI dashboard** (threat overview, model quality,
  pipeline health), kept in the repo as code (PR #4,
  [ADR-0020](docs/adr/0020-dashboard-as-code.md)).
- **Evaluation run recorded on Databricks:** MLflow and prediction screenshots
  (PR #3).
- **Honest evaluation protocol:**
  - 60/20/20 train/validation/test split stratified by attack family.
  - The alert threshold is tuned on validation for ≥95% recall: precision
    0.766 → 0.839, F1 0.865 → 0.889.
  - A separate 10-class attack-family model (PR #2,
    [ADR-0017](docs/adr/0017-validation-tuned-alert-threshold.md),
    [ADR-0018](docs/adr/0018-separate-attack-family-model.md)).

## 2026-10-12

- **Incremental, idempotent pipeline** (PR #1):
  - A replay script delivers the data as chunk files.
  - Checkpointed Structured Streaming / Auto Loader ingestion.
  - Insert-only Delta MERGE on `flow_hash`, with Change Data Feed between
    layers.
  - Frequency encodings frozen at training time.
  - Separate pipeline and training Workflows.
  - ADRs 0010–0016.
- **Documentation suite:** architecture overview, data model, operations
  runbook, and ADRs 0001–0016 (0001–0009 written retrospectively).
- Scoring skips until both a model and its frozen encodings exist. This came
  from a failed Databricks run against a model left over from the batch
  version.

## 2026-08-24

- **MLflow tracking and Unity Catalog model registration** on Databricks
  ([ADR-0009](docs/adr/0009-mlflow-tracking-and-uc-registry.md)).
- `try_cast` for dirty string columns under ANSI mode, the Databricks
  serverless default ([ADR-0008](docs/adr/0008-try-cast-under-ansi-mode.md)).
- Databricks deployment evidence (Workflow, MLflow, Unity Catalog screenshots)
  in the README.

## 2026-08-19

- **Full UNSW-NB15 dataset** (2,540,047 flows) with an explicit 49-column
  schema.
- SHA-256 flow-identity deduplication.
- 54 features, and a three-model benchmark (RandomForest, LogisticRegression,
  HistGradientBoosting).
- **Databricks port** sharing one code base with the local pipeline
  (ADRs [0002](docs/adr/0002-full-dataset-with-explicit-schema.md)–[0007](docs/adr/0007-distributed-inference-mapinpandas.md)).

## 2026-01-30 – 2026-02-01

- **First version:**
  - Local Bronze → Silver → Gold pipeline on Spark + Delta Lake in Docker.
  - A RandomForest model with Delta predictions.
  - Spark SQL analytics and quality checks.
  - ([ADR-0001](docs/adr/0001-medallion-lakehouse-on-delta-lake.md))
