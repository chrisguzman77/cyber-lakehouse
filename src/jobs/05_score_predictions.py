from __future__ import annotations

import argparse
from collections.abc import Iterator

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.io import GOLD_DIR, MODELS_DIR, ensure_dirs

NUMERIC_SPARK_TYPES = ("double", "float", "int", "bigint")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--features_path", default=str(GOLD_DIR / "features"))
    p.add_argument("--model_path", default=str(MODELS_DIR / "trained_model.pkl"))
    p.add_argument("--pred_out", default=str(GOLD_DIR / "predictions"))
    p.add_argument("--model_version", default="v1")
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()

    spark = SparkSession.builder.appName("05_score_predictions").getOrCreate()
    df = spark.read.format("delta").load(args.features_path)

    ignore = {"flow_hash", "attack_cat_norm", "ingest_ts", "is_attack"}
    feature_cols = [c for c, t in df.dtypes if c not in ignore and t in NUMERIC_SPARK_TYPES]
    model_path = args.model_path

    # Score partition-by-partition so the full 2.5M-row table never has to fit
    # in one pandas frame; the model is loaded once per worker.
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

    scored = (
        df.select("flow_hash", *feature_cols)
        .mapInPandas(score, schema="flow_hash string, pred_is_attack int, pred_proba double")
        .withColumn("model_version", F.lit(args.model_version))
        .withColumn("scored_ts", F.current_timestamp())
    )

    (
        scored.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(args.pred_out)
    )

    print(f"[OK] Predictions written to: {args.pred_out}")
    spark.stop()


if __name__ == "__main__":
    main()
