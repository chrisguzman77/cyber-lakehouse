# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest Bronze
# MAGIC Reads the full UNSW-NB15 CSVs (headerless, 49 columns) from a Unity Catalog
# MAGIC Volume and writes the managed Delta table `cyber_lakehouse.bronze.traffic`.
# MAGIC
# MAGIC Prereqs: see `databricks/README.md` (catalog/schemas/volume created, CSVs uploaded,
# MAGIC repo added as a Git folder so `src/` is importable).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from pyspark.sql import functions as F

from src.utils.schema import build_full_schema

RAW_GLOB = "/Volumes/cyber_lakehouse/bronze/raw/UNSW-NB15_*.csv"
BRONZE_TABLE = "cyber_lakehouse.bronze.traffic"

# COMMAND ----------

df = (
    spark.read.option("header", False)  # noqa: F821
    .schema(build_full_schema())
    .csv(RAW_GLOB)
    .withColumn("ingest_ts", F.current_timestamp())
    # input_file_name() is restricted on serverless; _metadata.file_path replaces it
    .withColumn("source_file", F.col("_metadata.file_path"))
)

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    BRONZE_TABLE
)

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS rows FROM {BRONZE_TABLE}"))  # noqa: F821
