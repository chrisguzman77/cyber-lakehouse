from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.io import BRONZE_DIR, SILVER_DIR, ensure_dirs

# Columns whose CSV quirks need fixing beyond the explicit bronze schema:
# ct_ftp_cmd arrives as string (blanks in raw data); the two others have nulls
# where the dataset means 0.
NULLABLE_COUNT_COLS = ["ct_flw_http_mthd", "is_ftp_login"]

# Identity of a flow: endpoints + protocol + time window + byte counts.
FLOW_KEY_COLS = ["srcip", "sport", "dstip", "dsport", "proto", "stime", "ltime", "sbytes", "dbytes"]

# Dataset uses both "Backdoor" and "Backdoors"; null/empty attack_cat means benign.
# Mirrors src.utils.schema.normalize_attack_cat_value for Spark columns.
ATTACK_CAT_FIXES = {"backdoors": "backdoor"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--in_path", default=str(BRONZE_DIR / "traffic_delta"))
    p.add_argument("--out_path", default=str(SILVER_DIR / "traffic_clean"))
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()
    spark = SparkSession.builder.appName("02_clean_silver").getOrCreate()

    df = spark.read.format("delta").load(args.in_path)

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

    # CSV quirks: ct_ftp_cmd has blank strings; two count columns use null for 0
    df = df.withColumn("ct_ftp_cmd", F.coalesce(F.trim(F.col("ct_ftp_cmd")).cast("long"), F.lit(0)))
    for c in NULLABLE_COUNT_COLS:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0)))

    # Filter out corrupt rows
    df = df.filter(F.col("label_int").isNotNull()).filter(F.col("dur") >= 0)

    # Dedupe key from the flow's identity fields
    df = df.withColumn(
        "flow_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in FLOW_KEY_COLS]), 256),
    )

    # Deduplicate by flow_hash (keep most recent ingestion)
    w = F.row_number().over(Window.partitionBy("flow_hash").orderBy(F.col("ingest_ts").desc()))
    df = df.withColumn("rn", w).filter(F.col("rn") == 1).drop("rn")

    df = df.withColumn("is_attack", (F.col("label_int") == F.lit(1)).cast("int"))

    # Overwrite Silver (deterministic rebuild)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(args.out_path)
    )

    print(f"[OK] Wrote Silver Delta table to: {args.out_path}")
    spark.stop()


if __name__ == "__main__":
    main()
