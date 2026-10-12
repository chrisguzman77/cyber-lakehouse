# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Build Gold Features
# MAGIC Streams only the rows silver newly inserted (Change Data Feed) into the gold
# MAGIC feature table, using the SAME `src/utils/features.py` functions as the local
# MAGIC pipeline, then recomputes the small analytics aggregate.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from pyspark import cloudpickle
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import src.utils.incremental
from src.utils.features import FREQ_ENCODED_COLS, RAW_FEATURE_COLS, add_derived_features
from src.utils.incremental import cdf_inserts, run_insert_stream

# Serverless runs foreachBatch in a separate Python process that may not see the
# Git folder, so ship the shared helpers by value instead of by module reference.
cloudpickle.register_pickle_by_value(src.utils.incremental)

SILVER_TABLE = "cyber_lakehouse.silver.traffic_clean"
FEATURES_TABLE = "cyber_lakehouse.gold.features"
ANALYTICS_TABLE = "cyber_lakehouse.gold.analytics"
CHECKPOINTS = "/Volumes/cyber_lakehouse/bronze/checkpoints"

# COMMAND ----------

silver = cdf_inserts(
    spark.readStream.option("readChangeFeed", "true").table(SILVER_TABLE)  # noqa: F821
)
base = silver.select(
    "flow_hash",
    "is_attack",
    "attack_cat_norm",
    *FREQ_ENCODED_COLS,
    *[c for c in RAW_FEATURE_COLS if c in silver.columns],
    "ingest_ts",
)
run_insert_stream(add_derived_features(base), f"{CHECKPOINTS}/gold_features", name=FEATURES_TABLE)

# COMMAND ----------

analytics = (
    add_derived_features(spark.table(SILVER_TABLE))  # noqa: F821
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
