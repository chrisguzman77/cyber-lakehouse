# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Score Predictions
# MAGIC Batch-scores every flow in the gold features table with the saved model via
# MAGIC `mapInPandas` and writes `cyber_lakehouse.gold.predictions`.

# COMMAND ----------

# MAGIC %pip install scikit-learn joblib

# COMMAND ----------

from collections.abc import Iterator

import pandas as pd
from pyspark.sql import functions as F

FEATURES_TABLE = "cyber_lakehouse.gold.features"
PREDICTIONS_TABLE = "cyber_lakehouse.gold.predictions"
MODEL_PATH = "/Volumes/cyber_lakehouse/gold/artifacts/model.pkl"
MODEL_VERSION = "v1"

# COMMAND ----------

df = spark.table(FEATURES_TABLE)  # noqa: F821

ignore = {"flow_hash", "attack_cat_norm", "ingest_ts", "is_attack"}
feature_cols = [
    c for c, t in df.dtypes if c not in ignore and t in ("double", "float", "int", "bigint")
]


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


scored = (
    df.select("flow_hash", *feature_cols)
    .mapInPandas(score, schema="flow_hash string, pred_is_attack int, pred_proba double")
    .withColumn("model_version", F.lit(MODEL_VERSION))
    .withColumn("scored_ts", F.current_timestamp())
)

scored.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    PREDICTIONS_TABLE
)

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"SELECT pred_is_attack, COUNT(*) AS n FROM {PREDICTIONS_TABLE} GROUP BY pred_is_attack"
    )
)
