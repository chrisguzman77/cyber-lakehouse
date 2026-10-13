from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.features import FREQ_ENCODED_COLS, RAW_FEATURE_COLS, add_derived_features
from src.utils.incremental import cdf_inserts, run_insert_stream
from src.utils.io import CHECKPOINTS_DIR, GOLD_DIR, SILVER_DIR, ensure_dirs


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--in_path", default=str(SILVER_DIR / "traffic_clean"))
    p.add_argument("--features_out", default=str(GOLD_DIR / "features"))
    p.add_argument("--analytics_out", default=str(GOLD_DIR / "analytics"))
    p.add_argument("--checkpoint", default=str(CHECKPOINTS_DIR / "gold_features"))
    p.add_argument("--max_bytes_per_trigger", default="128m")
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()
    spark = SparkSession.builder.appName("03_build_gold_features").getOrCreate()

    # Features table: stream only the rows silver newly inserted (Change Data
    # Feed). Every gold feature is row-wise; categoricals stay as strings and
    # are frequency-encoded with the map frozen at training time. The size cap
    # splits a first run's full-table snapshot into memory-sized micro-batches.
    changes = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("maxBytesPerTrigger", args.max_bytes_per_trigger)
        .load(args.in_path)
    )
    silver = cdf_inserts(changes)
    present = [c for c in RAW_FEATURE_COLS if c in silver.columns]
    base = silver.select(
        "flow_hash",
        "is_attack",
        "attack_cat_norm",
        *FREQ_ENCODED_COLS,
        *present,
        "ingest_ts",
    )
    run_insert_stream(add_derived_features(base), args.checkpoint, path=args.features_out)

    # Analytics aggregates: small, so recompute from all of silver each run
    df = spark.read.format("delta").load(args.in_path)
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

    (
        analytics.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(args.analytics_out)
    )

    print(f"[OK] Gold features up to date: {args.features_out}")
    print(f"[OK] Wrote Gold analytics to: {args.analytics_out}")
    spark.stop()


if __name__ == "__main__":
    main()
