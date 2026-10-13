from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from src.utils.cleaning import clean_traffic
from src.utils.incremental import first_per_key, run_insert_stream
from src.utils.io import BRONZE_DIR, CHECKPOINTS_DIR, SILVER_DIR, ensure_dirs


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--in_path", default=str(BRONZE_DIR / "traffic_delta"))
    p.add_argument("--out_path", default=str(SILVER_DIR / "traffic_clean"))
    p.add_argument("--checkpoint", default=str(CHECKPOINTS_DIR / "silver"))
    p.add_argument("--max_bytes_per_trigger", default="128m")
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()
    spark = SparkSession.builder.appName("02_clean_silver").getOrCreate()

    # Bronze is append-only, so new rows stream straight through. Each
    # micro-batch is deduped by flow_hash, then MERGEd insert-only: flows
    # silver already holds (re-deliveries, replays) are never written again.
    # The size cap keeps a large backlog from becoming one oversized MERGE.
    bronze = (
        spark.readStream.format("delta")
        .option("maxBytesPerTrigger", args.max_bytes_per_trigger)
        .load(args.in_path)
    )
    run_insert_stream(
        clean_traffic(bronze), args.checkpoint, batch_fn=first_per_key, path=args.out_path
    )

    print(f"[OK] Silver Delta table up to date: {args.out_path}")
    spark.stop()


if __name__ == "__main__":
    main()
