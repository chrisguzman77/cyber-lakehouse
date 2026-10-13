from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from src.utils.io import GOLD_DIR, SILVER_DIR, ensure_dirs
from src.utils.quality import run_checks

TABLES = {
    "silver": SILVER_DIR / "traffic_clean",
    "gold": GOLD_DIR / "features",
    "predictions": GOLD_DIR / "predictions",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--out_path", default=str(GOLD_DIR / "quality_results"))
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()
    spark = SparkSession.builder.appName("06_quality_checks").getOrCreate()

    existing = {
        layer: f"delta.`{path}`"
        for layer, path in TABLES.items()
        if DeltaTable.isDeltaTable(spark, str(path))
    }
    results = run_checks(spark, existing)
    # Append, so the history of every run is kept for the dashboard.
    results.write.format("delta").mode("append").save(args.out_path)

    rows = results.collect()
    for r in rows:
        print(f"[{r.status.upper():7}] {r.check_name:28} violations={r.violations}")
    failed = [r.check_name for r in rows if r.status == "fail"]
    spark.stop()

    # Quality gate: results are saved first, then a failure fails the job.
    if failed:
        raise SystemExit(f"[FAIL] {len(failed)} data-quality check(s) failed: {failed}")
    print(f"[OK] Data-quality checks passed; results appended to {args.out_path}")


if __name__ == "__main__":
    main()
