"""Print row counts per lakehouse layer as JSON (used by scripts/verify_incremental.sh)."""

from __future__ import annotations

import json

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from src.utils.io import BRONZE_DIR, GOLD_DIR, SILVER_DIR

TABLES = {
    "bronze": BRONZE_DIR / "traffic_delta",
    "silver": SILVER_DIR / "traffic_clean",
    "gold": GOLD_DIR / "features",
    "predictions": GOLD_DIR / "predictions",
}


def main() -> None:
    spark = SparkSession.builder.appName("table_counts").getOrCreate()
    counts = {}
    for layer, path in TABLES.items():
        if not DeltaTable.isDeltaTable(spark, str(path)):
            counts[layer] = None
            continue
        df = spark.read.format("delta").load(str(path))
        counts[layer] = df.count()
        if layer != "bronze":
            counts[f"{layer}_dup_keys"] = counts[layer] - df.select("flow_hash").distinct().count()
    print("COUNTS " + json.dumps(counts))
    spark.stop()


if __name__ == "__main__":
    main()
