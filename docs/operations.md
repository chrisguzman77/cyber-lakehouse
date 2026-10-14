# Operations Runbook

How to set up, run, verify, reset, and troubleshoot the pipeline. For the
design behind these steps see [`architecture.md`](architecture.md).

## Prerequisites

| Need | Why |
|---|---|
| Docker Desktop, VM memory ≥ 4 GB | Runs Spark + Delta locally |
| [uv](https://docs.astral.sh/uv/) | Python environment, tests, lint, replay script |
| Kaggle CLI + API token (`~/.kaggle/kaggle.json`) | `scripts/fetch_data.sh` |
| Java 17 (optional) | Only to run the Spark tests on the host; otherwise run them in Docker |

## First-time setup

```bash
uv sync --dev                                   # dev deps (pytest, ruff, pyspark, delta-spark)
bash scripts/fetch_data.sh                      # 4 CSVs (~600 MB) → data/raw/
docker compose -f docker/docker-compose.yml up -d --build
```

## Running the pipeline locally

All Spark jobs run in the container through `scripts/run_job.sh`, which adds
the Delta Lake package/config and `--driver-memory 2g`
(override with `SPARK_DRIVER_MEMORY`).

### Incremental cycle (repeat as often as you like)

```bash
uv run python -m src.utils.replay --files 3          # deliver the next 3 chunks
./scripts/run_job.sh src/jobs/01_ingest_bronze.py
./scripts/run_job.sh src/jobs/02_clean_silver.py
./scripts/run_job.sh src/jobs/03_build_gold_features.py
./scripts/run_job.sh src/jobs/05_score_predictions.py   # prints [SKIP] until a model exists
```

`--files 0` releases every remaining chunk (11 chunks for the full dataset).

### Training (on demand)

```bash
./scripts/run_job.sh src/jobs/04_train_model.py
./scripts/run_job.sh src/jobs/05_score_predictions.py --full_refresh
```

Training overwrites `models/trained_model.pkl`, `models/attack_family_model.pkl`,
`models/model_meta.json` (alert threshold, feature order),
`reports/metrics.json`, and `data/gold/freq_encodings`. Always follow it with
a `--full_refresh` scoring run so predictions come from a single model. This
is also required after the predictions schema changes (e.g. the
`pred_attack_cat` column added in ADR-0018), since `--full_refresh` recreates
the table.

### Job parameters

| Job | Parameter | Default | Purpose |
|---|---|---|---|
| replay | `--files` | 3 | chunks to release (0 = all) |
| replay | `--chunk_rows` | 250000 | rows per chunk file |
| 01 | `--max_files_per_trigger` | 2 | landing files per micro-batch |
| 02, 03, 05 | `--max_bytes_per_trigger` | 128m | upstream data per micro-batch |
| 04 | `--max_rows` | 500000 | stratified training sample size |
| 04 | `--seed` | 42 | sampling/split/model seed |
| 05 | `--score_partitions` | 2 | concurrent Python scoring workers (each loads both models) |
| 05 | `--model_version` | v1 | label stored on each prediction |
| 05 | `--full_refresh` | off | drop predictions + checkpoint, rescore all |
| all | `--checkpoint`, `--in_path`, `--out_path`, … | under `data/` | override locations |

### Data-quality gate

```bash
./scripts/run_job.sh src/jobs/06_quality_checks.py
```

Runs the 11 checks in `src/utils/quality.py`, prints `PASS`/`FAIL`/`MISSING`
per check, appends the results to `data/gold/quality_results`, and exits
non-zero if any check fails. Run it after scoring (it is the last task of the
Databricks pipeline Workflow).

### Ad-hoc analytics

```bash
docker compose -f docker/docker-compose.yml exec spark \
  /usr/local/lib/python3.11/site-packages/pyspark/bin/spark-sql \
  --packages io.delta:delta-spark_2.12:3.3.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.jars.ivy=/root/.ivy2 \
  -f src/sql/analytics.sql
```

Quick row counts per layer (JSON):

```bash
./scripts/run_job.sh src/jobs/table_counts.py | grep COUNTS
```

Expected after all chunks are processed: bronze 2,540,047; silver, gold,
predictions 2,013,620; all `*_dup_keys` 0.

### Cross-dataset evaluation (optional, standalone)

```bash
bash scripts/fetch_cic.sh                              # CIC-IDS2017, 8 CSVs (~850 MB) → data/raw/cic/
./scripts/run_job.sh src/jobs/evaluate_cross_dataset.py
```

Needs a built gold table. `fetch_cic.sh` downloads anonymously with
`kagglehub`, so no Kaggle token is needed. The job trains a portable UNSW
model, scores CIC with it, trains a CIC ceiling, prints a results table plus
per-attack detection rates, and writes `reports/cross_dataset.json`
(about 4–5 minutes; `--max_rows` sets the sample size per dataset, default
500k). See [ADR-0021](adr/0021-cross-dataset-evaluation-on-portable-features.md).

## Verification

| What | Command | Where |
|---|---|---|
| Lint / format | `uv run ruff check . && uv run ruff format --check .` | host |
| Unit + Delta integration tests | `uv run pytest` (Spark tests skip without Java) | host / CI |
| Same, with Spark | `docker compose -f docker/docker-compose.yml exec -T spark sh -c "pip install -q pytest && python -m pytest -q -p no:cacheprovider"` | container |
| End-to-end idempotency | `./scripts/verify_incremental.sh --reset` (**wipes local pipeline data**) | host → Docker |

CI (`.github/workflows/ci.yml`) runs ruff and pytest with Java 17 on every
push and pull request; the first run downloads the Delta jars from Maven.

## Resetting

Local (raw CSVs are untouched):

```bash
rm -rf data/{landing,bronze,silver,gold,checkpoints}
```

A reset is required when upgrading from the old batch pipeline (its tables
have no Change Data Feed) or after changing a stream's query shape, since a
checkpoint is tied to its query.

Databricks: see "Resetting" in [`databricks/README.md`](../databricks/README.md)
(DROP the five tables, then delete the checkpoint and `landing/incoming`
directories).

## Databricks

Setup, Workflow definitions, and the verification checklist live in
[`databricks/README.md`](../databricks/README.md). Summary:

1. Create the catalog, 3 schemas, and 4 Volumes (`raw`, `landing`,
   `checkpoints`, `artifacts`); upload the CSVs to `raw`.
2. Attach the repo as a Git folder.
3. Create `cyber-lakehouse-pipeline` (00 → 01 → 02 → 03 → 05) and
   `cyber-lakehouse-train` (04 → 05 with `full_refresh=true`).
4. Run pipeline → train → pipeline.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `06_quality_checks` fails (`[FAIL] N data-quality check(s) failed`) | A check found violations; its rows in `quality_results` have `status = 'fail'` and the violation count | Inspect the failing check's query in `src/utils/quality.py` against the named layer; fix the data or the upstream job, then rerun. |
| Dashboard widget shows an error | The table it reads doesn't exist yet (model metrics before training, quality results before the first `06` run) | Run the train Workflow and one pipeline run with `06_quality_checks`. |
| Job exits with code **137** | Container killed by the kernel OOM killer (Docker VM ~3.8 GB) | Keep `--driver-memory 2g` (6g is OOM-killed); lower `--max_bytes_per_trigger` or `--score_partitions`. Streams resume from their checkpoint, so just rerun. |
| Scoring prints `[SKIP] No trained model + frequency encodings yet` | Training hasn't run yet, or a model from an older deployment exists without its encodings table | Run job 04 (or `cyber-lakehouse-train`), then 05 with `--full_refresh`. |
| Spark tests all skipped on the host | No Java runtime | Install Java 17, or run tests in the container. |
| `CAST_INVALID_INPUT` on Databricks | ANSI mode rejects blank-string casts | Use `try_cast` for dirty string columns ([ADR-0008](adr/0008-try-cast-under-ansi-mode.md)). |
| Stream fails after a code change mentioning checkpoint/offsets | Checkpoint incompatible with the new query | Reset that layer and its checkpoint (and downstream layers). |
| Gold or scoring error about missing CDF / table | Upstream table created by the old batch pipeline or not yet created | Reset; run jobs in order. |
| `ModuleNotFoundError: src` inside `foreachBatch` on serverless | Batch function runs in a process without the Git folder on `sys.path` | Notebooks register `src.utils` modules with `cloudpickle.register_pickle_by_value`; keep new helpers in those modules. |
| Windows: `--full_refresh` ignored | `run_spark_job.ps1` doesn't forward extra arguments | Use WSL/Git Bash with `scripts/run_job.sh`, or extend the script. |
| Very slow first run of 02/03 | One huge micro-batch | Ensure `--max_bytes_per_trigger` is set (default 128m). |
| Job crawls, log shows `Removing executor driver with no recent heartbeats` | Docker VM out of memory and swap (other containers running alongside Spark) | Stop other containers or raise Docker Desktop's memory; `docker restart cyber-spark` clears swap. Streams resume from their checkpoint, so just rerun. |
