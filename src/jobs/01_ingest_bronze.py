from __future__ import annotations

import argparse

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from src.utils.io import ensure_dirs, RAW_DIR, BRONZE_DIR


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input_glob", default=str(RAW_DIR / "*.csv"))
    p.add_argument("--out_path", default=str(BRONZE_DIR / "traffic_delta"))
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()

    spark = SparkSession.builder.appName("01_ingest_bronze").getOrCreate()

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(args.input_glob)
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )

    # Light sanity: must have at least 1 row
    if df.rdd.isEmpty():
        raise RuntimeError("No input rows found. check data/raw/*.csv")
    
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true") # enables schema evolution at ingest time
        .save(args.out_path)
    )

    print(f"[OK] Wrote Bronze Delta table to: {args.out_path}")
    spark.stop()

if __name__ == "__main__":
    main()