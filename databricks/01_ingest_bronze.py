# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest Bronze (Auto Loader)
# MAGIC Incrementally ingests new CSV files from `landing/incoming/` (headerless, 49
# MAGIC columns) into the managed Delta table `cyber_lakehouse.bronze.traffic`. The
# MAGIC checkpoint records which files were already ingested, so reruns are no-ops.
# MAGIC
# MAGIC Prereqs: see `databricks/README.md` (catalog/schemas/volumes created, raw CSVs
# MAGIC uploaded, repo added as a Git folder so `src/` is importable).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from pyspark.sql import functions as F

from src.utils.incremental import append_stream
from src.utils.schema import build_full_schema

LANDING_INCOMING = "/Volumes/cyber_lakehouse/bronze/landing/incoming/"
CHECKPOINTS = "/Volumes/cyber_lakehouse/bronze/checkpoints"
BRONZE_TABLE = "cyber_lakehouse.bronze.traffic"

# COMMAND ----------

raw = (
    spark.readStream.format("cloudFiles")  # noqa: F821
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.maxFilesPerTrigger", 2)
    .option("header", False)
    .schema(build_full_schema())
    .load(LANDING_INCOMING)
    .withColumn("ingest_ts", F.current_timestamp())
    # input_file_name() is restricted on serverless; _metadata.file_path replaces it
    .withColumn("source_file", F.col("_metadata.file_path"))
)
append_stream(raw, f"{CHECKPOINTS}/bronze", name=BRONZE_TABLE)

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS rows FROM {BRONZE_TABLE}"))  # noqa: F821
