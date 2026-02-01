from __future__ import annotations

import argparse

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from src.utils.io import BRONZE_DIR, SILVER_DIR, ensure_dirs


NUMERIC_COLS = [
    "dur",
    "spkts",
    "dpkts",
    "sbytes",
    "dbytes",
    "rate",
    "sttl",
    "dttl",
    "sload",
    "dload",
]


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

    # Standardize label columns:
    # - many UNSW files have label as 0/1 string/int
    # - some have attack_cat
    df = df.withColumn("label_int", F.col("label").cast("int"))

    # If attack_cat exists, normalize it; else create a default.
    df = df.withColumn(
        "attack_cat_norm",
        F.when(F.col("attack_cat").isNull(), F.lit("unknown"))
        .otherwise(F.lower(F.trim(F.col("attack_cat")))),
    )

    # Basic numeric casting + null handling
    for c in NUMERIC_COLS:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    # Filter out corrupt rows (missing label)
    df = df.filter(F.col("label_int").isNotNull())

    # Create surrogate dedupe key from a subset of fields
    dedupe_cols = [c for c in ["dur", "spkts", "dpkts", "sbytes", "dbytes", "rate"] if c in df.columns]
    df = df.withColumn(
        "flow_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in dedupe_cols]), 256),
    )

    # Deduplicate by flow_hash (keep most recent ingestion)
    w = (
        F.row_number()
        .over(
            __import__("pyspark.sql.window").sql.window.Window.partitionBy("flow_hash")
            .orderBy(F.col("ingest_ts").desc())
        )
    )
    df = df.withColumn("rn", w).filter(F.col("rn") == 1).drop("rn")

    # Create is_attack as boolean/int
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
