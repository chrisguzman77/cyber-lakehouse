# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Score Predictions (incremental)
# MAGIC Scores only the flows gold newly inserted (Change Data Feed) with the saved
# MAGIC model via `mapInPandas`, MERGEing insert-only into `cyber_lakehouse.gold.predictions`.
# MAGIC Set the `full_refresh` parameter to `true` after retraining to rescore every flow.

# COMMAND ----------

# MAGIC %pip install scikit-learn joblib

# COMMAND ----------

import os
import sys
from collections.abc import Iterator

sys.path.append(os.path.abspath(".."))

import joblib
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
MODEL_PATH = "/Volumes/cyber_lakehouse/gold/artifacts/model.pkl"
CHECKPOINTS = "/Volumes/cyber_lakehouse/bronze/checkpoints"
MODEL_VERSION = "v1"

dbutils.widgets.text("full_refresh", "false")  # noqa: F821
FULL_REFRESH = dbutils.widgets.get("full_refresh").lower() == "true"  # noqa: F821

# COMMAND ----------

if not os.path.exists(MODEL_PATH):
    dbutils.notebook.exit("No model yet; run the train workflow (notebook 04) first.")  # noqa: F821

if FULL_REFRESH:
    spark.sql(f"DROP TABLE IF EXISTS {PREDICTIONS_TABLE}")  # noqa: F821
    dbutils.fs.rm(f"{CHECKPOINTS}/predictions", True)  # noqa: F821

# Column order must match training exactly; the model records it.
feature_cols = list(joblib.load(MODEL_PATH).feature_names_in_)


def score(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    import joblib

    # Safe pickle load: the artifact is produced by our own 04_train_model notebook
    model = joblib.load(MODEL_PATH)
    for pdf in batches:
        proba = model.predict_proba(pdf[feature_cols])[:, 1]
        yield pd.DataFrame(
            {
                "flow_hash": pdf["flow_hash"],
                "pred_is_attack": (proba >= 0.5).astype("int32"),
                "pred_proba": proba,
            }
        )


def score_batch(batch_df: DataFrame) -> DataFrame:
    # Read the encodings through the batch's session: a DataFrame captured from
    # the notebook can't be pickled into serverless foreachBatch.
    encodings = batch_df.sparkSession.table(ENCODINGS_TABLE)
    return (
        apply_frequency_encodings(batch_df, encodings)
        .select("flow_hash", *feature_cols)
        .mapInPandas(score, schema="flow_hash string, pred_is_attack int, pred_proba double")
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
        f"SELECT pred_is_attack, COUNT(*) AS n FROM {PREDICTIONS_TABLE} GROUP BY pred_is_attack"
    )
)
