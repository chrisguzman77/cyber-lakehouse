from __future__ import annotations

import argparse
from datetime import UTC, datetime

import joblib
from pyspark.sql import SparkSession

from src.utils.io import GOLD_DIR, MODELS_DIR, ensure_dirs


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--features_path", default=str(GOLD_DIR / "features"))
    p.add_argument("--model_path", default=str(MODELS_DIR / "trained_model.pk1"))
    p.add_argument("--pred_out", default=str(GOLD_DIR / "predictions"))
    p.add_argument("--model_version", default="v1")
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()

    spark = SparkSession.builder.appName("05_score_predictions").getOrCreate()
    df = spark.read.format("delta").load(args.features_path)

    model = joblib.load(args.model_path)

    ignore = {"flow_hash", "attack_cat_norm", "ingest_ts", "is_attack"}
    numeric_cols = [c for c, t in df.dtypes if c not in ignore and t in ("double", "int")]
    features = df.select("flow_hash", *numeric_cols)

    # Spark -> Pandas for scoring (fast enough for this project)
    pdf = features.toPandas()
    X = pdf[numeric_cols]

    proba = model.predict_proba(X)[:, 1]
    pred = (proba >= 0.5).astype(int)

    scored_pdf = pdf[["flow_hash"]].copy()
    scored_pdf["pred_is_attack"] = pred
    scored_pdf["pred_proba"] = proba
    scored_pdf["model_version"] = args.model_version
    scored_pdf["scored_ts"] = datetime.now(UTC).isoformat()

    scored = spark.createDataFrame(scored_pdf)

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
