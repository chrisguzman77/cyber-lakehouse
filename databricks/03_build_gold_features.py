# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Build Gold Features
# MAGIC Builds the 50+ feature ML table and the analytics aggregates using the SAME
# MAGIC `src/utils/features.py` functions as the local pipeline.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.features import (
    FREQ_ENCODED_COLS,
    RAW_FEATURE_COLS,
    add_derived_features,
    add_frequency_encodings,
)

SILVER_TABLE = "cyber_lakehouse.silver.traffic_clean"
FEATURES_TABLE = "cyber_lakehouse.gold.features"
ANALYTICS_TABLE = "cyber_lakehouse.gold.analytics"

# COMMAND ----------

df = spark.table(SILVER_TABLE)  # noqa: F821

base = df.select(
    "flow_hash",
    "is_attack",
    "attack_cat_norm",
    *FREQ_ENCODED_COLS,
    *[c for c in RAW_FEATURE_COLS if c in df.columns],
    "ingest_ts",
)
features = add_frequency_encodings(add_derived_features(base)).drop(*FREQ_ENCODED_COLS)

features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    FEATURES_TABLE
)

# COMMAND ----------

analytics = (
    add_derived_features(df)
    .groupBy("attack_cat_norm")
    .agg(
        F.count("*").alias("rows"),
        F.avg("dur").alias("avg_dur"),
        F.avg("pkts_per_sec").alias("avg_pkts_per_sec"),
        F.avg("sbytes").alias("avg_sbytes"),
    )
    .withColumn("pct_of_total", F.col("rows") / F.sum("rows").over(Window.partitionBy()))
    .orderBy(F.col("rows").desc())
)
analytics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    ANALYTICS_TABLE
)

display(spark.table(ANALYTICS_TABLE))  # noqa: F821
