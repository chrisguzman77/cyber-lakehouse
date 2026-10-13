# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Replay Landing
# MAGIC Simulates network logs arriving over time: releases the next chunk files from
# MAGIC the raw Volume into `landing/incoming/`, which Auto Loader (notebook 01) watches.
# MAGIC Uses the same `src/utils/replay.py` as the local pipeline.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from src.utils.replay import release_chunks

RAW_GLOB = "/Volumes/cyber_lakehouse/bronze/raw/UNSW-NB15_*.csv"
LANDING_DIR = "/Volumes/cyber_lakehouse/bronze/landing"

dbutils.widgets.text("files", "3")  # noqa: F821  (0 = release everything)

# COMMAND ----------

released = release_chunks(RAW_GLOB, LANDING_DIR, n_files=int(dbutils.widgets.get("files")))  # noqa: F821
print(f"Released {len(released)} chunk(s): {released}")
