from __future__ import annotations

import argparse
import json

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split

from src.utils.io import GOLD_DIR, MODELS_DIR, REPORTS_DIR, ensure_dirs

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--features_path", default=str(GOLD_DIR / "features"))
    p.add_argument("--model_out", default=str(MODELS_DIR / "trained_model.pk1"))
    p.add_argument("--metrics_out", default=str(REPORTS_DIR / "metrics.json"))
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()

    # Read Delta features through Spark -> Pandas, so it's conistent with the lakehouse.
    from pyspark.sql import SparkSession # imported here so local unit tests don't need pyspark

    spark = SparkSession.builder.appName("04_train_model").getOrCreate()
    sdf = spark.read.format("delta").load(args.features_path)

    # Keep only numeric columns for sklearn
    ignore = {"flow_hash", "attack_cat_norm", "ingest_ts"}
    cols = [c for c, t in sdf.dtypes if c not in ignore and t in ("double", "int")]
    pdf = sdf.select(*cols).toPandas()
    spark.stop()

    if "is_attack" not in pdf.columns:
        raise RuntimeError("Expected is_attack in features table.")
    
    X = pdf.drop(columns=["is_attack"])
    y = pdf["is_attack"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=args.seed, stratify=y
    )

    model = RandomForestClassifier(
        n_estimators=200,
        random_state=args.seed,
        n_jobs=-1,
        class_weight="balanced",
    )
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    metrics = {
        "accuracy": float(accuracy_score(y_test, preds)),
        "precision": float(precision_score(y_test, preds, zero_division=0)),
        "recall": float(recall_score(y_test, preds, zero_division=0)),
        "f1": float(f1_score(y_test, preds, zero_division=0)),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "features_used": list(X.columns),
        "seed": args.seed,
    }

    joblib.dump(model, args.model_out)
    with open(args.metrics_out, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print("[OK] Model saved to:", args.model_out)
    print("[OK] Metrics saved to:", args.metrics_out)
    print(metrics)

if __name__ == "__main__":
    main() 