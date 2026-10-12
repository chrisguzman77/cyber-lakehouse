"""Silver-layer cleaning, shared by the local job and the Databricks notebook.

Row-wise only: deduplication happens per micro-batch in the silver job
(src.utils.incremental.first_per_key + insert_new_keys), not here.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Columns whose CSV quirks need fixing beyond the explicit bronze schema:
# ct_ftp_cmd arrives as string (blanks in raw data); the two others have nulls
# where the dataset means 0.
NULLABLE_COUNT_COLS = ["ct_flw_http_mthd", "is_ftp_login"]

# Identity of a flow: endpoints + protocol + time window + byte counts.
FLOW_KEY_COLS = ["srcip", "sport", "dstip", "dsport", "proto", "stime", "ltime", "sbytes", "dbytes"]

# Dataset uses both "Backdoor" and "Backdoors"; null/empty attack_cat means benign.
# Mirrors src.utils.schema.normalize_attack_cat_value for Spark columns.
ATTACK_CAT_FIXES = {"backdoors": "backdoor"}


def clean_traffic(df: DataFrame) -> DataFrame:
    """Normalize labels/categories, fix CSV quirks, drop corrupt rows, add flow_hash."""
    df = df.withColumn("label_int", F.col("label").cast("int"))

    # Normalize attack category: trim/lower, fix known variants, null/empty -> "normal"
    cat = F.lower(F.trim(F.col("attack_cat")))
    for raw, fixed in ATTACK_CAT_FIXES.items():
        cat = F.when(cat == raw, F.lit(fixed)).otherwise(cat)
    df = df.withColumn(
        "attack_cat_norm",
        F.when(
            F.col("attack_cat").isNull() | (F.trim(F.col("attack_cat")) == ""), "normal"
        ).otherwise(cat),
    )

    # CSV quirks: ct_ftp_cmd has blank strings; two count columns use null for 0.
    # try_cast, not cast: blank -> NULL -> 0 everywhere, and plain cast throws
    # under ANSI mode (default on Databricks serverless)
    df = df.withColumn(
        "ct_ftp_cmd", F.coalesce(F.expr("try_cast(trim(ct_ftp_cmd) AS BIGINT)"), F.lit(0))
    )
    for c in NULLABLE_COUNT_COLS:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0)))

    # Filter out corrupt rows
    df = df.filter(F.col("label_int").isNotNull()).filter(F.col("dur") >= 0)

    # Dedupe key from the flow's identity fields
    df = df.withColumn(
        "flow_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in FLOW_KEY_COLS]), 256),
    )

    return df.withColumn("is_attack", (F.col("label_int") == F.lit(1)).cast("int"))
