# Running the Lakehouse on Databricks (Free Edition)

The `databricks/` notebooks mirror the local `src/jobs/` pipeline on Databricks
serverless compute with Unity Catalog managed Delta tables. The feature
engineering imports the **same** `src/utils/features.py` module the local jobs
use, so the two deployments cannot drift.

## One-time setup

1. **Sign up** for [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
   (serverless compute + Unity Catalog included).

2. **Create the catalog, schemas, and raw Volume** — run in a SQL editor:

   ```sql
   CREATE CATALOG IF NOT EXISTS cyber_lakehouse;
   CREATE SCHEMA IF NOT EXISTS cyber_lakehouse.bronze;
   CREATE SCHEMA IF NOT EXISTS cyber_lakehouse.silver;
   CREATE SCHEMA IF NOT EXISTS cyber_lakehouse.gold;
   CREATE VOLUME IF NOT EXISTS cyber_lakehouse.bronze.raw;
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

## Run order

Open each notebook in the Git folder and Run All, in order:

| Notebook | Writes |
|---|---|
| `01_ingest_bronze` | `cyber_lakehouse.bronze.traffic` (~2.54M rows) |
| `02_clean_silver` | `cyber_lakehouse.silver.traffic_clean` |
| `03_build_gold_features` | `gold.features` (50+ features), `gold.analytics` |
| `04_train_model` | `gold.model_metrics` (3 models), model artifact in the Volume |
| `05_score_predictions` | `gold.predictions` |

## Verification checklist

- [ ] `SELECT COUNT(*) FROM cyber_lakehouse.bronze.traffic` ≈ 2,540,044
- [ ] `SELECT DISTINCT attack_cat_norm FROM cyber_lakehouse.silver.traffic_clean` → 10 categories
- [ ] `SELECT COUNT(*) FROM cyber_lakehouse.gold.model_metrics` → 3 rows
- [ ] `gold.predictions` row count equals `gold.features` row count

## Serverless (Free Edition) caveats

- **No cluster libraries** — notebooks 04/05 start with `%pip install scikit-learn joblib`.
  On first run, confirm the install cell succeeds; serverless base environments
  change over time and may already ship sklearn.
- **`input_file_name()` is restricted** on serverless — notebook 01 uses the
  `_metadata.file_path` column instead.
- **Ephemeral local disk** — the model artifact is saved to the FUSE-mounted
  Volume path `/Volumes/cyber_lakehouse/gold/artifacts/model.pkl`, not `/tmp`.
- **Compute quotas** — notebook 04 trains on a 300k stratified sample
  (`MAX_ROWS`); raise it if your quota allows.
