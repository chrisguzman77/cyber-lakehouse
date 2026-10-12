# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Clean Silver (incremental MERGE)
# MAGIC Streams new bronze rows, normalizes labels/attack categories, fixes CSV quirks,
# MAGIC dedupes each micro-batch by a SHA-256 flow key, and MERGEs insert-only into
# MAGIC `cyber_lakehouse.silver.traffic_clean` (Change Data Feed on). Uses the same
# MAGIC `src/utils/cleaning.py` and `src/utils/incremental.py` as the local pipeline.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from pyspark import cloudpickle

import src.utils.incremental
from src.utils.cleaning import clean_traffic
from src.utils.incremental import first_per_key, run_insert_stream

# Serverless runs foreachBatch in a separate Python process that may not see the
# Git folder, so ship the shared helpers by value instead of by module reference.
cloudpickle.register_pickle_by_value(src.utils.incremental)

BRONZE_TABLE = "cyber_lakehouse.bronze.traffic"
SILVER_TABLE = "cyber_lakehouse.silver.traffic_clean"
CHECKPOINTS = "/Volumes/cyber_lakehouse/bronze/checkpoints"

# COMMAND ----------

bronze = spark.readStream.table(BRONZE_TABLE)  # noqa: F821
run_insert_stream(
    clean_traffic(bronze), f"{CHECKPOINTS}/silver", batch_fn=first_per_key, name=SILVER_TABLE
)

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"SELECT attack_cat_norm, COUNT(*) AS rows FROM {SILVER_TABLE} "
        "GROUP BY attack_cat_norm ORDER BY rows DESC"
    )
)
