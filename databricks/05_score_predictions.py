# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Score Predictions (incremental)
# MAGIC Scores only the flows gold newly inserted (Change Data Feed) via `mapInPandas`:
# MAGIC the binary model's probability is compared with the alert threshold tuned at
# MAGIC training time (`model_meta.json`), and the attack-family model labels each flow.
# MAGIC MERGEs insert-only into `cyber_lakehouse.gold.predictions`.
# MAGIC Set the `full_refresh` parameter to `true` after retraining to rescore every flow.

# COMMAND ----------

# MAGIC %pip install scikit-learn joblib

# COMMAND ----------

import json
import os
import sys
from collections.abc import Iterator

sys.path.append(os.path.abspath(".."))

import pandas as pd
from pyspark import cloudpickle
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import src.utils.features
import src.utils.incremental
from src.utils.features import apply_frequency_encodings
from src.utils.incremental import cdf_inserts, run_insert_stream

# Serverless runs foreachBatch in a separate Python process that may not see the
# Git folder, so ship the shared helpers by value instead of by module reference.
cloudpickle.register_pickle_by_value(src.utils.incremental)
cloudpickle.register_pickle_by_value(src.utils.features)

FEATURES_TABLE = "cyber_lakehouse.gold.features"
ENCODINGS_TABLE = "cyber_lakehouse.gold.freq_encodings"
PREDICTIONS_TABLE = "cyber_lakehouse.gold.predictions"
ARTIFACTS = "/Volumes/cyber_lakehouse/gold/artifacts"
MODEL_PATH = f"{ARTIFACTS}/model.pkl"
FAMILY_MODEL_PATH = f"{ARTIFACTS}/attack_family_model.pkl"
META_PATH = f"{ARTIFACTS}/model_meta.json"
CHECKPOINTS = "/Volumes/cyber_lakehouse/bronze/checkpoints"
MODEL_VERSION = "v1"

dbutils.widgets.text("full_refresh", "false")  # noqa: F821
FULL_REFRESH = dbutils.widgets.get("full_refresh").lower() == "true"  # noqa: F821

# COMMAND ----------

# Scoring needs every training output; artifacts left over from an older
# deployment (e.g. a model without its frozen encodings) must not be used.
artifacts_ready = all(os.path.exists(p) for p in (MODEL_PATH, FAMILY_MODEL_PATH, META_PATH))
if not (artifacts_ready and spark.catalog.tableExists(ENCODINGS_TABLE)):  # noqa: F821
    dbutils.notebook.exit(  # noqa: F821
        "No trained model + frequency encodings yet; run the train workflow (notebook 04) first."
    )

if FULL_REFRESH:
    spark.sql(f"DROP TABLE IF EXISTS {PREDICTIONS_TABLE}")  # noqa: F821
    dbutils.fs.rm(f"{CHECKPOINTS}/predictions", True)  # noqa: F821

# Column order and alert threshold must match training exactly.
with open(META_PATH, encoding="utf-8") as f:
    meta = json.load(f)
feature_cols = meta["features"]
threshold = meta["threshold"]


def score(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    import joblib

    # Safe pickle loads: the artifacts are produced by our own 04_train_model notebook
    model = joblib.load(MODEL_PATH)
    family = joblib.load(FAMILY_MODEL_PATH)
    for pdf in batches:
        X = pdf[feature_cols]
        proba = model.predict_proba(X)[:, 1]
        yield pd.DataFrame(
            {
                "flow_hash": pdf["flow_hash"],
                "pred_is_attack": (proba >= threshold).astype("int32"),
                "pred_proba": proba,
                "pred_attack_cat": family.predict(X),
            }
        )


def score_batch(batch_df: DataFrame) -> DataFrame:
    # Read the encodings through the batch's session: a DataFrame captured from
    # the notebook can't be pickled into serverless foreachBatch.
    encodings = batch_df.sparkSession.table(ENCODINGS_TABLE)
    return (
        apply_frequency_encodings(batch_df, encodings)
        .select("flow_hash", *feature_cols)
        .mapInPandas(
            score,
            schema="flow_hash string, pred_is_attack int, pred_proba double, "
            "pred_attack_cat string",
        )
        .withColumn("model_version", F.lit(MODEL_VERSION))
        .withColumn("scored_ts", F.current_timestamp())
    )


# COMMAND ----------

changes = spark.readStream.option("readChangeFeed", "true").table(FEATURES_TABLE)  # noqa: F821
run_insert_stream(
    cdf_inserts(changes),
    f"{CHECKPOINTS}/predictions",
    batch_fn=score_batch,
    name=PREDICTIONS_TABLE,
)

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        "SELECT pred_attack_cat, COUNT(*) AS flows, SUM(pred_is_attack) AS alerts "
        f"FROM {PREDICTIONS_TABLE} GROUP BY pred_attack_cat ORDER BY flows DESC"
    )
)
