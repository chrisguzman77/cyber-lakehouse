# Architecture

This document describes what Cyber Lakehouse is, how it is built, and why it is
built that way. Individual design decisions are recorded as ADRs in
[`adr/`](adr/README.md); the table and column reference is in
[`data-model.md`](data-model.md); how to run and troubleshoot it is in
[`operations.md`](operations.md).

## 1. Purpose and scope

Cyber Lakehouse is a data-engineering and machine-learning pipeline for network
intrusion detection. It ingests the full UNSW-NB15 dataset (2,540,047 network
flow records), cleans and deduplicates it, engineers model features, trains and
benchmarks classifiers that label each flow as attack or benign, and scores
every flow.

**Goals**

- Process the complete dataset (not a sample) through a layered lakehouse.
- Be **incremental and idempotent**: new data is processed once; re-running a
  job or re-delivering a file never creates duplicates.
- Run identically on a laptop (Docker) and in the cloud (Databricks serverless).
- Evaluate models honestly on heavily imbalanced data.
- Be reproducible: pinned versions, fixed seeds, committed metrics, CI.

**Non-goals**

- Real-time (sub-second) detection. Streams run with an `availableNow`
  trigger, i.e. as scheduled incremental batches.
- Production alerting/SIEM integration, model serving endpoints, or a UI.
- Generalization claims beyond UNSW-NB15 (see §9).

## 2. System context

```mermaid
flowchart LR
    K[(Kaggle mirror of<br/>UNSW-NB15)] -->|scripts/fetch_data.sh| R[data/raw<br/>4 headerless CSVs]
    R -->|src/utils/replay.py| L[landing/incoming<br/>chunk files]
    L --> P[Lakehouse pipeline<br/>Spark + Delta Lake]
    P --> T[(Delta tables<br/>bronze / silver / gold)]
    P --> M[Model artifact +<br/>reports/metrics.json]
    T --> Q[Quality gate<br/>gold.quality_results]
    T --> D[AI/BI dashboard]
    Q --> D
    P -. Databricks .-> U[(Unity Catalog tables,<br/>MLflow, UC model registry)]
```

## 3. Data flow

```mermaid
flowchart TD
    raw[Raw CSVs<br/>2,540,047 rows] -->|replay: 250k-row chunks,<br/>atomic rename| landing[Landing<br/>incoming/]
    landing -->|01 file-source stream / Auto Loader<br/>explicit schema, availableNow| bronze[(Bronze<br/>append-only)]
    bronze -->|02 stream → clean_traffic →<br/>first_per_key → insert-only MERGE| silver[(Silver<br/>2,013,620 unique flows<br/>CDF on)]
    silver -->|03 CDF inserts → derived features<br/>→ insert-only MERGE| gold[(Gold features<br/>CDF on)]
    silver -->|03 batch recompute| analytics[(Gold analytics)]
    gold -->|04 train: compute + persist<br/>frequency encodings| enc[(freq_encodings)]
    gold -->|04 stratified 500k sample, 60/20/20 split| model[Binary model + tuned threshold,<br/>attack-family model, metrics]
    gold -->|05 CDF inserts → frozen encodings<br/>→ mapInPandas → insert-only MERGE| pred[(Predictions)]
    enc --> pred
    model --> pred
    pred -->|06 11 checks over silver, gold, predictions| dq[(Quality results<br/>append per run)]
    pred --> dash[AI/BI dashboard]
    dq --> dash
```

Each arrow labeled 01–05 is a job in `src/jobs/` (and a matching notebook in
`databricks/`). Jobs 01, 02, 03 and 05 are **checkpointed streams**: each run
processes everything new since the previous run and then stops. Job 04
(training) is a batch job.

## 4. Components

| Component | Location | Responsibility |
|---|---|---|
| Data fetch | `scripts/fetch_data.sh` | Download the 4 full-dataset CSVs (~600 MB) via the Kaggle CLI into `data/raw/`. |
| Replay | `src/utils/replay.py`, `databricks/00_replay_landing.py` | Split raw CSVs into 250k-row chunk files and release the next N into `landing/incoming/`, simulating data arriving over time. |
| Bronze ingest | `src/jobs/01_ingest_bronze.py` | Stream new landing files into an append-only Delta table with an explicit 49-column schema plus `ingest_ts` and `source_file`. |
| Silver clean | `src/jobs/02_clean_silver.py`, `src/utils/cleaning.py` | Normalize labels and attack categories, fix CSV quirks, drop corrupt rows, compute `flow_hash`, dedupe per micro-batch, insert-only MERGE. |
| Gold features | `src/jobs/03_build_gold_features.py`, `src/utils/features.py` | Stream silver's newly inserted rows, add 13 derived features, insert-only MERGE; recompute the analytics aggregate. |
| Training | `src/jobs/04_train_model.py`, `src/utils/modeling.py` | Freeze frequency encodings; sample and split 60/20/20; benchmark binary models with a validation-tuned alert threshold and 10-class attack-family models; save both best models, `model_meta.json` (threshold, feature order), and the metrics report. |
| Scoring | `src/jobs/05_score_predictions.py` | Stream gold's newly inserted rows, apply frozen encodings, score with `mapInPandas` (probability vs. the saved threshold, plus the attack family), insert-only MERGE; `--full_refresh` rescores everything. |
| Incremental helpers | `src/utils/incremental.py` | `first_per_key`, `ensure_delta_table`, `insert_new_keys`, `cdf_inserts`, `append_stream`, `run_insert_stream` — shared by local jobs and notebooks. |
| Schema | `src/utils/schema.py` | The 49-column UNSW-NB15 schema, canonical attack categories, quality rule metadata. |
| Paths | `src/utils/io.py` | Local data/model/report directories. |
| Data quality | `src/jobs/06_quality_checks.py`, `src/utils/quality.py` | 11 checks over silver, gold, and predictions; append one row per check per run to `quality_results`; fail the job if any check fails (missing tables are recorded, not failed). |
| Dashboard | `databricks/dashboards/cyber_lakehouse.lvdash.json` | Three-page Databricks AI/BI dashboard (threat overview, model quality, pipeline health) kept in version control. |
| SQL | `src/sql/analytics.sql` | Ad-hoc analytics queries. |
| Verification | `scripts/verify_incremental.sh`, `src/jobs/table_counts.py` | End-to-end idempotency check in Docker. |
| Runtime | `docker/`, `scripts/run_job.sh`, `run_spark_job.ps1` | Containerized Spark + Delta; `spark-submit` wrapper with Delta config and 2 GB driver memory. |

## 5. Deployment views

### Local (Docker)

- One container (`cyber-spark`) built from `docker/spark.Dockerfile`:
  Python 3.11, OpenJDK 17, PySpark 3.5.3, delta-spark 3.3.1, pandas, pyarrow,
  scikit-learn, joblib. The repo is bind-mounted at `/opt/project`.
- Jobs run via `scripts/run_job.sh` → `spark-submit` in `local[*]` mode with
  `--driver-memory 2g` (larger values are OOM-killed on the 8 GB development
  Mac, whose Docker VM has ~3.8 GB).
- Tables are path-based Delta tables under `data/`; stream checkpoints under
  `data/checkpoints/<layer>`; the model at `models/trained_model.pkl`.

### Cloud (Databricks Free Edition, serverless)

- The repo is attached as a **Git folder**; notebooks in `databricks/` import
  `src/utils` via a relative `sys.path` append, so the transformation code is
  the same code that runs locally ([ADR-0004](adr/0004-shared-code-local-and-databricks.md)).
- Raw CSVs in the UC Volume `cyber_lakehouse.bronze.raw`; landing files in
  `cyber_lakehouse.bronze.landing`; checkpoints in
  `cyber_lakehouse.bronze.checkpoints`; model artifact in
  `cyber_lakehouse.gold.artifacts`.
- Tables are Unity Catalog managed Delta tables: `cyber_lakehouse.bronze.traffic`,
  `silver.traffic_clean`, `gold.features`, `gold.analytics`,
  `gold.freq_encodings`, `gold.model_metrics`, `gold.family_model_metrics`,
  `gold.predictions`.
- Bronze uses **Auto Loader** (`cloudFiles`) instead of the plain file source.
- Training logs one MLflow run per model (binary and family) and registers
  the best of each in the UC model registry as
  `cyber_lakehouse.gold.attack_classifier` (with its alert threshold) and
  `cyber_lakehouse.gold.attack_family_classifier`.
- Orchestrated as two Workflows ([ADR-0014](adr/0014-separate-pipeline-and-training-workflows.md)):
  - `cyber-lakehouse-pipeline`: 00 replay → 01 → 02 → 03 → 05 → 06 quality gate (run repeatedly)
  - `cyber-lakehouse-train`: 04 → 05 with `full_refresh=true` (on demand)

> **Status:** verified on Databricks Free Edition on 2026-10-12. Four
> incremental pipeline runs delivered all 11 chunks (2–3 min each); a fifth
> run with nothing new left every row count unchanged. Final counts match the
> local run exactly: bronze 2,540,047; silver = gold = predictions =
> 2,013,620; 0 duplicate keys. The training Workflow ran in 4m23s.
> The evaluation upgrade (ADR-0017/0018) was then trained on Databricks on
> 2026-10-13: HistGradientBoosting alert model at threshold 0.880 (test
> precision 0.824, recall 0.953), HistGradientBoosting family model (test
> macro-F1 0.648); all 2,013,620 flows rescored with `pred_attack_cat`.

## 6. Technology stack

| Area | Technology | Version | Why |
|---|---|---|---|
| Processing | Apache Spark (PySpark) | 3.5.3 | Distributed DataFrame engine; same API locally and on Databricks; Structured Streaming for incremental ingestion. |
| Storage | Delta Lake | 3.3.1 | ACID tables on Parquet, MERGE, Change Data Feed, streaming source/sink, schema enforcement ([ADR-0001](adr/0001-medallion-lakehouse-on-delta-lake.md)). |
| Cloud | Databricks Free Edition | serverless | Managed Spark, Unity Catalog governance, Auto Loader, Workflows, MLflow — at no cost. |
| Governance | Unity Catalog | — | Catalog/schema/table/Volume namespace, model registry. |
| ML | scikit-learn | ≥1.5 | RandomForest, LogisticRegression, HistGradientBoosting on a stratified sample ([ADR-0005](adr/0005-scikit-learn-on-stratified-sample.md)). |
| Experiment tracking | MLflow | Databricks-provided | Per-model runs, metric comparison, UC model registration ([ADR-0009](adr/0009-mlflow-tracking-and-uc-registry.md)). |
| Data interchange | pandas, PyArrow | pandas ≥2.2 | `toPandas` for training, Arrow-backed `mapInPandas` for scoring. |
| Runtime | Docker, Docker Compose | — | Reproducible local Spark + Java 17 environment. |
| Language | Python | 3.11 | Pinned in `.python-version` and the Docker image. |
| Packaging | uv | — | Dependency management and lockfile (`uv.lock`). |
| Quality | pytest, ruff, GitHub Actions | pytest ≥8, ruff ≥0.6 | Unit + Delta integration tests, lint/format, CI on every push. |
| Dataset | UNSW-NB15 | full (4 CSVs) | Real benign traffic plus 9 synthetic attack families; 49 columns. |

## 7. Cross-cutting concerns

### Idempotency and incrementality

- **Bronze** is append-only; the stream checkpoint records which file paths
  were ingested, so re-running ingests nothing new.
- **Silver, gold features, predictions** are written only through
  `insert_new_keys`: an insert-only MERGE on `flow_hash`
  ([ADR-0011](adr/0011-insert-only-merge-first-seen-wins.md)). A file
  re-delivered under a new name lands in bronze again (bronze keeps raw
  history) but adds nothing downstream.
- Each layer reads only the rows the previous layer inserted, via Change Data
  Feed ([ADR-0012](adr/0012-change-data-feed-between-layers.md)).
- Verified by `scripts/verify_incremental.sh` and by a full-data no-op rerun
  (every count unchanged).

### Data quality

- Explicit schema on read; no inference ([ADR-0002](adr/0002-full-dataset-with-explicit-schema.md)).
- Cleaning rules in `clean_traffic`: null labels and negative durations
  dropped, blank/null counters coerced to 0, attack category normalized to 10
  canonical values.
- A quality gate (`06_quality_checks`, [ADR-0019](adr/0019-persisted-quality-gate.md)) runs
  11 checks after every pipeline run: null labels, negative durations,
  duplicate keys in silver and gold, `is_attack` domain, key-feature nulls,
  attack-share sanity band (2–25%), probability bounds, missing family labels,
  unscored or double-scored flows. Results are appended to `quality_results`
  so the dashboard shows the latest status and the history; any failure fails
  the run. Latest full-data run: all violations 0, attack share 4.06%.

### Train/serve consistency

- Frequency encodings are computed once at training time, persisted, and
  applied by both training and scoring ([ADR-0013](adr/0013-freeze-frequency-encodings-at-training.md)).
- The alert threshold and feature order are saved in `model_meta.json` at
  training time and read by scoring, so `pred_is_attack` uses exactly the
  threshold that was evaluated ([ADR-0017](adr/0017-validation-tuned-alert-threshold.md)).

### Reproducibility

- Pinned PySpark/Delta versions in the Docker image and dev dependencies.
- Fixed seed (42) for sampling, splitting, and model training; stratified
  sampling and splits.
- Every model choice (threshold, which model to keep) is made on the
  validation split; the test split is reporting-only.
- Metrics committed in `reports/metrics.json`.

### Resource constraints

The development machine has 8 GB RAM. Stream readers cap micro-batches at
128 MB, training collects its sample with Arrow, and scoring limits concurrent
Python workers to 2
([ADR-0015](adr/0015-bound-batch-size-and-scoring-concurrency.md)).

## 8. Results and performance

**Model results** — stratified 500k sample of the 2,013,620-flow gold table
(~4% attacks), 60/20/20 train/validation/test stratified by attack family,
seed 42. Choices are made on validation; numbers below are test.

*Attack detection* — threshold = highest validation precision at ≥95% recall:

| Model | Threshold | Precision | Recall | F1 | ROC-AUC |
|---|---|---|---|---|---|
| **HistGradientBoosting** (selected) | 0.912 | 0.8394 | 0.9453 | 0.8892 | 0.9988 |
| RandomForest | 0.766 | 0.8167 | 0.9546 | 0.8803 | 0.9985 |
| LogisticRegression | 0.948 | 0.7692 | 0.9500 | 0.8501 | 0.9964 |

Compared with the previous fixed 0.5 threshold (precision 0.766, recall
0.992), false alarms fall from ~1 in 4 alerts to ~1 in 6 while 94.5% of
attacks are still caught.

*Attack family* (10 classes) — RandomForest selected by validation macro-F1:
test macro-F1 0.585, accuracy 0.980. Strong on generic, reconnaissance, and
exploits (F1 0.82–0.94); weak on dos, analysis, backdoor, and worms. Per-class
results are in the README and `reports/metrics.json`
([ADR-0018](adr/0018-separate-attack-family-model.md)).

**Local run times** (Docker, 2 GB driver, full dataset, 2026-10-12):

| Step | First run (all data) | No-op rerun |
|---|---|---|
| 01 bronze | 189 s | 9 s |
| 02 silver | 153 s | 27 s |
| 03 gold | 135 s | 28 s |
| 04 training (binary + family) | 371 s | — |
| 05 scoring (full refresh, both models) | 265 s | 18 s |

## 9. Known limitations

- **Dataset realism.** UNSW-NB15 attacks are synthetic and some features
  (e.g. TTLs) are known to be unusually predictive on this dataset; scores are
  likely optimistic for real traffic. Cross-dataset evaluation is future work.
- **Precision 0.84 at 94.5% recall**: about 1 in 6 alerts is still a false
  positive, and ~5.5% of attacks are missed. The 95% recall floor is a
  judgment call, not derived from real costs.
- **Attack-family labels are unreliable for rare families** (dos, analysis,
  backdoor, worms), and 1.3% of benign flows are labeled `fuzzers`. The family
  label is meant for alerts only; the two models can disagree.
- **Training uses a 500k sample** (25% of gold) to fit in local memory;
  scoring covers every flow.
- **Encodings are not embedded in the registered MLflow model**; the
  `freq_encodings` table must travel with it (a pyfunc wrapper would fix this).
- **Replay rename atomicity on UC Volumes** (FUSE) is not guaranteed by the
  platform; in practice Auto Loader never read a partial chunk (counts were
  exact across all runs).
- After upgrading an existing deployment, run the training Workflow before
  expecting predictions: scoring skips until both a model and its frozen
  encodings exist. (The first Databricks run failed here because a model from
  the batch-era deployment existed without encodings; the check now requires
  both.)
- `run_spark_job.ps1` does not forward extra arguments (e.g. `--full_refresh`).

## 10. Roadmap

1. ~~Streaming ingestion + incremental MERGE~~ (done, 2026-10-12)
2. ~~Evaluation: train/validation/test split, threshold tuning, attack-family
   model~~ (done, 2026-10-12); cross-dataset test on CIC-IDS2017 (next)
3. ~~Dashboard (Databricks AI/BI) over predictions, model metrics, and
   persisted quality-check results~~ (built 2026-10-13, published 2026-10-14, [ADR-0020](adr/0020-dashboard-as-code.md))
