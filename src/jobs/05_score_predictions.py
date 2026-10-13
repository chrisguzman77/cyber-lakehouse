from __future__ import annotations

import argparse
import json
import shutil
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.features import apply_frequency_encodings
from src.utils.incremental import cdf_inserts, run_insert_stream
from src.utils.io import CHECKPOINTS_DIR, GOLD_DIR, MODELS_DIR, ensure_dirs, model_artifacts


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--features_path", default=str(GOLD_DIR / "features"))
    p.add_argument("--encodings_path", default=str(GOLD_DIR / "freq_encodings"))
    p.add_argument(
        "--model_path",
        default=str(MODELS_DIR / "trained_model.pkl"),
        help="Binary model; the family model and model_meta.json are read from beside it",
    )
    p.add_argument("--pred_out", default=str(GOLD_DIR / "predictions"))
    p.add_argument("--checkpoint", default=str(CHECKPOINTS_DIR / "predictions"))
    p.add_argument("--model_version", default="v1")
    p.add_argument("--max_bytes_per_trigger", default="128m")
    p.add_argument(
        "--score_partitions",
        type=int,
        default=2,
        help="Concurrent Python scoring workers (each loads both models, ~380MB)",
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

    # Scoring needs every training output; artifacts left over from an older
    # deployment (e.g. a model without its frozen encodings) must not be used.
    paths = model_artifacts(args.model_path)
    if not (
        all(p.exists() for p in paths.values()) and Path(args.encodings_path, "_delta_log").exists()
    ):
        print("[SKIP] No trained model + frequency encodings yet; run 04_train_model first.")
        return

    if args.full_refresh:
        shutil.rmtree(args.pred_out, ignore_errors=True)
        shutil.rmtree(args.checkpoint, ignore_errors=True)

    spark = SparkSession.builder.appName("05_score_predictions").getOrCreate()

    # Column order and alert threshold must match training exactly.
    meta = json.loads(paths["meta"].read_text())
    feature_cols = meta["features"]
    threshold = meta["threshold"]
    encodings = spark.read.format("delta").load(args.encodings_path)
    model_path, family_path = str(paths["binary"]), str(paths["family"])

    # Score partition-by-partition so a large micro-batch never has to fit in
    # one pandas frame; the models are loaded once per worker.
    def score(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        import joblib

        # Safe pickle loads: the artifacts are produced by our own 04_train_model job
        model = joblib.load(model_path)
        family = joblib.load(family_path)
        for pdf in batches:
            X = pdf[feature_cols]
            proba = model.predict_proba(X)[:, 1]
            yield pd.DataFrame(
                {
                    "flow_hash": pdf["flow_hash"],
                    "pred_is_attack": (proba >= threshold).astype("int32"),
                    "pred_proba": proba,
                    "pred_attack_cat": family.predict(X),
                }
            )

    def score_batch(batch_df: DataFrame) -> DataFrame:
        return (
            apply_frequency_encodings(batch_df, encodings)
            .select("flow_hash", *feature_cols)
            # Caps Python workers alive at once: one per core (8), each holding
            # both models, on top of the 2g driver gets OOM-killed in a ~4GB VM.
            .coalesce(args.score_partitions)
            .mapInPandas(
                score,
                schema="flow_hash string, pred_is_attack int, pred_proba double, "
                "pred_attack_cat string",
            )
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
