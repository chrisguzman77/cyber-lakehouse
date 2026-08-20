from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.features import (
    FREQ_ENCODED_COLS,
    RAW_FEATURE_COLS,
    add_derived_features,
    add_frequency_encodings,
)
from src.utils.io import GOLD_DIR, SILVER_DIR, ensure_dirs


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

    df = spark.read.format("delta").load(args.in_path)

    # Build features table: raw numerics + derived features + frequency encodings
    present = [c for c in RAW_FEATURE_COLS if c in df.columns]
    base = df.select(
        "flow_hash",
        "is_attack",
        "attack_cat_norm",
        *FREQ_ENCODED_COLS,
        *present,
        "ingest_ts",
    )
    features = add_frequency_encodings(add_derived_features(base))
    # Categorical strings are represented by their *_freq encodings in gold
    features = features.drop(*FREQ_ENCODED_COLS)

    (
        features.coalesce(8)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(args.features_out)
    )

    # Build analytics aggregates
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

    print(f"[OK] Wrote Gold features to: {args.features_out}")
    print(f"[OK] Wrote Gold analytics to: {args.analytics_out}")
    spark.stop()


if __name__ == "__main__":
    main()
