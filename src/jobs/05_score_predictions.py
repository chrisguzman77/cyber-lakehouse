from __future__ import annotations

import argparse
import shutil
from collections.abc import Iterator
from pathlib import Path

import joblib
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.features import apply_frequency_encodings
from src.utils.incremental import cdf_inserts, run_insert_stream
from src.utils.io import CHECKPOINTS_DIR, GOLD_DIR, MODELS_DIR, ensure_dirs


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--features_path", default=str(GOLD_DIR / "features"))
    p.add_argument("--encodings_path", default=str(GOLD_DIR / "freq_encodings"))
    p.add_argument("--model_path", default=str(MODELS_DIR / "trained_model.pkl"))
    p.add_argument("--pred_out", default=str(GOLD_DIR / "predictions"))
    p.add_argument("--checkpoint", default=str(CHECKPOINTS_DIR / "predictions"))
    p.add_argument("--model_version", default="v1")
    p.add_argument("--max_bytes_per_trigger", default="128m")
    p.add_argument(
        "--score_partitions",
        type=int,
        default=4,
        help="Concurrent Python scoring workers (~230MB each with the RF model loaded)",
    )
    p.add_argument(
        "--full_refresh",
        action="store_true",
        help="Drop predictions + checkpoint and rescore every flow (run after retraining)",
    )
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()

    if not Path(args.model_path).exists():
        print(f"[SKIP] No model at {args.model_path} yet; run 04_train_model first.")
        return

    if args.full_refresh:
        shutil.rmtree(args.pred_out, ignore_errors=True)
        shutil.rmtree(args.checkpoint, ignore_errors=True)

    spark = SparkSession.builder.appName("05_score_predictions").getOrCreate()

    # Column order must match training exactly; the model records it.
    feature_cols = list(joblib.load(args.model_path).feature_names_in_)
    encodings = spark.read.format("delta").load(args.encodings_path)
    model_path = args.model_path

    # Score partition-by-partition so a large micro-batch never has to fit in
    # one pandas frame; the model is loaded once per worker.
    def score(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        import joblib

        # Safe pickle load: the artifact is produced by our own 04_train_model job
        model = joblib.load(model_path)
        for pdf in batches:
            proba = model.predict_proba(pdf[feature_cols])[:, 1]
            yield pd.DataFrame(
                {
                    "flow_hash": pdf["flow_hash"],
                    "pred_is_attack": (proba >= 0.5).astype("int32"),
                    "pred_proba": proba,
                }
            )

    def score_batch(batch_df: DataFrame) -> DataFrame:
        return (
            apply_frequency_encodings(batch_df, encodings)
            .select("flow_hash", *feature_cols)
            # Caps Python workers alive at once: one per core (8) on top of the
            # 2g driver gets OOM-killed in a ~4GB Docker VM.
            .coalesce(args.score_partitions)
            .mapInPandas(score, schema="flow_hash string, pred_is_attack int, pred_proba double")
            .withColumn("model_version", F.lit(args.model_version))
            .withColumn("scored_ts", F.current_timestamp())
        )

    # Only flows gold newly inserted get scored; after a full refresh the
    # checkpoint is gone, so the change feed starts from a full snapshot, which
    # the size cap splits into memory-sized micro-batches.
    changes = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("maxBytesPerTrigger", args.max_bytes_per_trigger)
        .load(args.features_path)
    )
    run_insert_stream(
        cdf_inserts(changes), args.checkpoint, batch_fn=score_batch, path=args.pred_out
    )

    print(f"[OK] Predictions up to date: {args.pred_out}")
    spark.stop()


if __name__ == "__main__":
    main()
