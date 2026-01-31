from __future__ import annotations

import argparse

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from src.utils.io import SILVER_DIR, GOLD_DIR, ensure_dirs


FEATURE_COLS = [
    "dur",
    "spkts",
    "dpkts",
    "sbytes",
    "dbytes",
    "rate",
    "sttl",
    "dttl",
    "sload",
    "dload"
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--in_path", default=str(SILVER_DIR / "traffic_clean"))
    p.add_argument("--features_out", default=str(GOLD_DIR / "features"))
    p.add_argument("--analytics_out", default=str(GOLD_DIR / "analytics"))
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()
    spark = SparkSession.builder.appName("03_build_gold_features").getOrCreate()

    df = spark.read.format("delta").load((args.in_path))

    # Build features table
    present = [c for c in FEATURE_COLS if c in df.columns]
    features = df.select(
        "flow_hash",
        "is_attack",
        "attack_cat_norm",
        *present,
        "ingest_ts",
    )

    (
        features.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(args.features_out)
    )

    # Build analytics aggregates
    analytics = (
        df.groupBy("attack_cat_norm")
        .agg(
            F.count("*").alias("rows"),
            F.avg("dur").alias("avg_dur"),
            F.avg("rate").alias("avg_rate"),
        )
        .orderBy(F.col("rows").desc())
    )

    (
        analytics.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(args.analytics_out)
    )

    # Compact small files by rewriting partition count (simple compaction pattern)
    # This is a practical demo
    compacted = features.coalesce(4)
    (
        compacted.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(args.features_out)
    )

    print(f"[OK] Wrote Gold features to: {args.features_out}")
    print(f"[OK] Wrote Gold analytics to: {args.analytics_out}")
    spark.stop()


if __name__ == "__main__":
    main()