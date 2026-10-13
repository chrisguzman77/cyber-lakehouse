# Running the Lakehouse on Databricks (Free Edition)

The `databricks/` notebooks mirror the local `src/jobs/` pipeline on Databricks
serverless compute with Unity Catalog managed Delta tables. Cleaning, feature
engineering, the incremental MERGE helpers, and the replay script are imported
from the **same** `src/utils/` modules the local jobs use, so the two
deployments cannot drift.

The pipeline is incremental: Auto Loader ingests only new landing files, and
every layer below bronze is an insert-only MERGE keyed by `flow_hash`, fed by
the upstream table's Change Data Feed. Reruns and re-delivered files never add
rows.

## One-time setup

1. **Sign up** for [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
   (serverless compute + Unity Catalog included).

2. **Create the catalog, schemas, and Volumes** — run in a SQL editor:

   ```sql
   CREATE CATALOG IF NOT EXISTS cyber_lakehouse;
   CREATE SCHEMA IF NOT EXISTS cyber_lakehouse.bronze;
   CREATE SCHEMA IF NOT EXISTS cyber_lakehouse.silver;
   CREATE SCHEMA IF NOT EXISTS cyber_lakehouse.gold;
   CREATE VOLUME IF NOT EXISTS cyber_lakehouse.bronze.raw;
   CREATE VOLUME IF NOT EXISTS cyber_lakehouse.bronze.landing;
   CREATE VOLUME IF NOT EXISTS cyber_lakehouse.bronze.checkpoints;
   CREATE VOLUME IF NOT EXISTS cyber_lakehouse.gold.artifacts;
   ```

3. **Upload the 4 full-dataset CSVs** (`UNSW-NB15_1.csv` … `UNSW-NB15_4.csv`,
   fetched locally via `scripts/fetch_data.sh`) into the raw Volume:
   - UI: Catalog Explorer → `cyber_lakehouse.bronze.raw` → Upload, or
   - CLI: `databricks fs cp data/raw/UNSW-NB15_1.csv dbfs:/Volumes/cyber_lakehouse/bronze/raw/`
     (repeat for files 2–4; requires the Databricks CLI configured with a PAT).

4. **Add this repo as a Git folder** (Workspace → Create → Git folder →
   this repo's GitHub URL). The notebooks live in `databricks/` inside it and
   import shared code from `src/` via a relative `sys.path` append.

5. **Create two Workflows** (Jobs & Pipelines → Create job), each task a
   notebook from the Git folder on serverless compute, chained with
   sequential dependencies:

   | Workflow | Tasks | When to run |
   |---|---|---|
   | `cyber-lakehouse-pipeline` | `00_replay_landing` → `01_ingest_bronze` → `02_clean_silver` → `03_build_gold_features` → `05_score_predictions` | Repeatedly — each run delivers and processes the next chunks |
   | `cyber-lakehouse-train` | `04_train_model` → `05_score_predictions` with parameter `full_refresh` = `true` | On demand — retrain, then rescore every flow |

   `00_replay_landing` takes a `files` parameter (default 3 chunks of 250k
   rows per run; `0` releases everything).

## Run order

1. Run `cyber-lakehouse-pipeline` once. The scoring task exits early with
   "No trained model + frequency encodings yet" — expected before the first
   training (including when upgrading from the batch pipeline, whose old model
   has no encodings table).
2. Run `cyber-lakehouse-train` to train and score everything ingested so far.
3. Run `cyber-lakehouse-pipeline` again (as often as you like): new chunks flow
   through every layer and only the new flows get scored.

**Upgrading an existing deployment to new model artifacts** (e.g. the
attack-family model and tuned threshold): pull the Git folder, then run
`cyber-lakehouse-train` once. Until then the pipeline's scoring task skips,
because `attack_family_model.pkl` and `model_meta.json` don't exist yet; the
train Workflow's `full_refresh` recreates `gold.predictions` with the new
`pred_attack_cat` column. No other tables need resetting.

| Notebook | Writes |
|---|---|
| `00_replay_landing` | next chunk files in `/Volumes/cyber_lakehouse/bronze/landing/incoming/` |
| `01_ingest_bronze` | `cyber_lakehouse.bronze.traffic` (append, Auto Loader) |
| `02_clean_silver` | `cyber_lakehouse.silver.traffic_clean` (insert-only MERGE, CDF on) |
| `03_build_gold_features` | `gold.features` (insert-only MERGE, CDF on), `gold.analytics` |
| `04_train_model` | `gold.freq_encodings`, `gold.model_metrics` (3 binary models, tuned thresholds), `gold.family_model_metrics` (3 family models), `model.pkl` + `attack_family_model.pkl` + `model_meta.json` in the artifacts Volume, UC models `attack_classifier` and `attack_family_classifier` |
| `05_score_predictions` | `gold.predictions` (insert-only MERGE): `pred_is_attack` at the tuned threshold, `pred_proba`, `pred_attack_cat` |

## Verification checklist

- [ ] Rerunning `cyber-lakehouse-pipeline` with no new chunks (`files` = 0
      after everything is released) leaves every table's row count unchanged
- [ ] `SELECT COUNT(*) - COUNT(DISTINCT flow_hash) FROM cyber_lakehouse.silver.traffic_clean` → 0
- [ ] After all chunks are released: `SELECT COUNT(*) FROM cyber_lakehouse.bronze.traffic` ≈ 2,540,047
      and `silver.traffic_clean` = 2,013,620
- [ ] `SELECT DISTINCT attack_cat_norm FROM cyber_lakehouse.silver.traffic_clean` → 10 categories
- [ ] `SELECT COUNT(*) FROM cyber_lakehouse.gold.model_metrics` → 3 rows; `gold.family_model_metrics` → 3 rows; exactly one `selected = true` in each
- [ ] `SELECT COUNT(*) FROM cyber_lakehouse.gold.predictions WHERE pred_attack_cat IS NULL` → 0
- [ ] `gold.predictions` row count equals `gold.features` row count

## Resetting

The incremental tables need Change Data Feed and their checkpoints in sync, so
start fresh when upgrading from the old batch pipeline or to replay from
scratch:

```sql
DROP TABLE IF EXISTS cyber_lakehouse.bronze.traffic;
DROP TABLE IF EXISTS cyber_lakehouse.silver.traffic_clean;
DROP TABLE IF EXISTS cyber_lakehouse.gold.features;
DROP TABLE IF EXISTS cyber_lakehouse.gold.predictions;
DROP TABLE IF EXISTS cyber_lakehouse.gold.freq_encodings;
```

then, in a notebook cell:

```python
dbutils.fs.rm("/Volumes/cyber_lakehouse/bronze/checkpoints/", True)
dbutils.fs.rm("/Volumes/cyber_lakehouse/bronze/landing/incoming/", True)
```

## Serverless (Free Edition) caveats

- **No cluster libraries** — notebooks 04/05 start with `%pip install scikit-learn joblib`.
  On first run, confirm the install cell succeeds; serverless base environments
  change over time and may already ship sklearn.
- **`input_file_name()` is restricted** on serverless — notebook 01 uses the
  `_metadata.file_path` column instead.
- **`availableNow` trigger only** — serverless streaming doesn't support
  continuous or processing-time triggers; every stream processes what's
  available and stops, so each notebook behaves like a batch task.
- **`foreachBatch` runs in a separate Python process** — notebooks 02/03/05
  register the shared `src/utils` modules with `cloudpickle.register_pickle_by_value`
  so they don't need the Git folder on that process's path, and notebook 05
  reads the encodings table inside the batch function rather than capturing a
  DataFrame.
- **Ephemeral local disk** — the model artifact is saved to the FUSE-mounted
  Volume path `/Volumes/cyber_lakehouse/gold/artifacts/model.pkl`, not `/tmp`.
- **Compute quotas** — notebook 04 trains on a 300k stratified sample
  (`MAX_ROWS`); raise it if your quota allows.
