from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.io import BRONZE_DIR, RAW_DIR, ensure_dirs
from src.utils.schema import build_full_schema


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input_glob", default=str(RAW_DIR / "UNSW-NB15_*.csv"))
    p.add_argument("--out_path", default=str(BRONZE_DIR / "traffic_delta"))
    p.add_argument(
        "--append",
        action="store_true",
        help="Append instead of overwrite (default overwrites so re-runs are idempotent)",
    )
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()

    spark = SparkSession.builder.appName("01_ingest_bronze").getOrCreate()

    # Full-dataset CSVs have no header row; explicit schema avoids a 600MB inference pass.
    df = (
        spark.read.option("header", False)
        .schema(build_full_schema())
        .csv(args.input_glob)
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )

    # Light sanity: must have at least 1 row
    if df.limit(1).count() == 0:
        raise RuntimeError("No input rows found. check data/raw/UNSW-NB15_*.csv")

    (
        df.write.format("delta")
        .mode("append" if args.append else "overwrite")
        .option("mergeSchema" if args.append else "overwriteSchema", "true")
        .save(args.out_path)
    )

    print(f"[OK] Wrote Bronze Delta table to: {args.out_path}")
    spark.stop()


if __name__ == "__main__":
    main()
