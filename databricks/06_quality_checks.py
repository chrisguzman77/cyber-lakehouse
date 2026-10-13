# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Data-Quality Checks
# MAGIC Runs every check in `src/utils/quality.py` against silver, gold, and
# MAGIC predictions, appends one row per check to `cyber_lakehouse.gold.quality_results`
# MAGIC (the dashboard's pipeline-health page reads it), then fails the task if any
# MAGIC check failed, a quality gate at the end of the pipeline Workflow. Checks on a
# MAGIC table that doesn't exist yet (e.g. predictions before the first training) are
# MAGIC recorded as `missing`, not failed.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from src.utils.quality import run_checks

TABLES = {
    "silver": "cyber_lakehouse.silver.traffic_clean",
    "gold": "cyber_lakehouse.gold.features",
    "predictions": "cyber_lakehouse.gold.predictions",
}
RESULTS_TABLE = "cyber_lakehouse.gold.quality_results"

# COMMAND ----------

existing = {layer: name for layer, name in TABLES.items() if spark.catalog.tableExists(name)}  # noqa: F821
results = run_checks(spark, existing)  # noqa: F821
results.write.format("delta").mode("append").saveAsTable(RESULTS_TABLE)
display(results)  # noqa: F821

# COMMAND ----------

failed = [r.check_name for r in results.collect() if r.status == "fail"]
if failed:
    raise Exception(f"{len(failed)} data-quality check(s) failed: {failed}")
print("All data-quality checks passed")
