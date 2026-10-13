from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.incremental import append_stream
from src.utils.io import BRONZE_DIR, CHECKPOINTS_DIR, LANDING_DIR, ensure_dirs
from src.utils.schema import build_full_schema


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--incoming_dir", default=str(LANDING_DIR / "incoming"))
    p.add_argument("--out_path", default=str(BRONZE_DIR / "traffic_delta"))
    p.add_argument("--checkpoint", default=str(CHECKPOINTS_DIR / "bronze"))
    p.add_argument("--max_files_per_trigger", type=int, default=2)
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()
    Path(args.incoming_dir).mkdir(parents=True, exist_ok=True)

    spark = SparkSession.builder.appName("01_ingest_bronze").getOrCreate()

    # Stream new landing files only; the checkpoint records which files were
    # already ingested. Headerless CSVs + explicit schema (no inference pass).
    raw = (
        spark.readStream.schema(build_full_schema())
        .option("header", False)
        .option("maxFilesPerTrigger", args.max_files_per_trigger)
        .csv(args.incoming_dir)
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )
    append_stream(raw, args.checkpoint, path=args.out_path)

    print(f"[OK] Bronze Delta table up to date: {args.out_path}")
    spark.stop()


if __name__ == "__main__":
    main()
