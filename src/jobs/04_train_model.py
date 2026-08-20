from __future__ import annotations

import argparse
import json
import time

import joblib
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.utils.io import GOLD_DIR, MODELS_DIR, REPORTS_DIR, ensure_dirs

NUMERIC_SPARK_TYPES = ("double", "float", "int", "bigint")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--features_path", default=str(GOLD_DIR / "features"))
    p.add_argument("--model_out", default=str(MODELS_DIR / "trained_model.pkl"))
    p.add_argument("--metrics_out", default=str(REPORTS_DIR / "metrics.json"))
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--max_rows",
        type=int,
        default=500_000,
        help="Stratified sample size taken in Spark before pandas conversion",
    )
    return p.parse_args()


def build_models(seed: int) -> dict:
    return {
        "random_forest": RandomForestClassifier(
            n_estimators=150,
            max_depth=20,
            random_state=seed,
            n_jobs=-1,
            class_weight="balanced",
        ),
        "logistic_regression": Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "clf",
                    LogisticRegression(max_iter=1000, class_weight="balanced", random_state=seed),
                ),
            ]
        ),
        "hist_gradient_boosting": HistGradientBoostingClassifier(
            random_state=seed, class_weight="balanced"
        ),
    }


def main() -> None:
    ensure_dirs()
    args = parse_args()

    # Read Delta features through Spark -> Pandas, so it's consistent with the lakehouse.
    from pyspark.sql import SparkSession  # imported here so local unit tests don't need pyspark

    spark = SparkSession.builder.appName("04_train_model").getOrCreate()
    sdf = spark.read.format("delta").load(args.features_path)

    n_total = sdf.count()
    # Stratified downsample in Spark: identical fraction per class preserves the
    # attack/benign ratio while keeping the pandas frame memory-bounded.
    fraction = min(1.0, args.max_rows / n_total)
    if fraction < 1.0:
        sdf = sdf.sampleBy("is_attack", {0: fraction, 1: fraction}, seed=args.seed)

    ignore = {"flow_hash", "attack_cat_norm", "ingest_ts"}
    cols = [c for c, t in sdf.dtypes if c not in ignore and t in NUMERIC_SPARK_TYPES]
    pdf = sdf.select(*cols).toPandas()
    spark.stop()

    if "is_attack" not in pdf.columns:
        raise RuntimeError("Expected is_attack in features table.")

    # float32 halves memory; precision is irrelevant for these models
    X = pdf.drop(columns=["is_attack"]).astype("float32")
    y = pdf["is_attack"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=args.seed, stratify=y
    )

    results = {}
    fitted = {}
    for name, model in build_models(args.seed).items():
        start = time.monotonic()
        model.fit(X_train, y_train)
        train_seconds = time.monotonic() - start

        preds = model.predict(X_test)
        proba = model.predict_proba(X_test)[:, 1]
        results[name] = {
            "accuracy": float(accuracy_score(y_test, preds)),
            "precision": float(precision_score(y_test, preds, zero_division=0)),
            "recall": float(recall_score(y_test, preds, zero_division=0)),
            "f1": float(f1_score(y_test, preds, zero_division=0)),
            "roc_auc": float(roc_auc_score(y_test, proba)),
            "train_seconds": round(train_seconds, 2),
        }
        fitted[name] = model
        print(f"[OK] Trained {name} in {train_seconds:.1f}s: {results[name]}")

    best_name = max(results, key=lambda n: results[n]["f1"])
    metrics = {
        "dataset": {
            "n_rows_total": int(n_total),
            "n_rows_sampled": int(len(pdf)),
            "sample_strategy": f"stratified sampleBy is_attack, max_rows={args.max_rows}",
            "n_features": int(X.shape[1]),
            "n_train": int(len(X_train)),
            "n_test": int(len(X_test)),
        },
        "models": results,
        "best_model": best_name,
        "features_used": list(X.columns),
        "seed": args.seed,
    }

    joblib.dump(fitted[best_name], args.model_out)
    with open(args.metrics_out, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    # Markdown comparison table (copy-paste source for the README)
    print("\n| Model | Accuracy | Precision | Recall | F1 | ROC-AUC |")
    print("|---|---|---|---|---|---|")
    for name, m in results.items():
        star = " (saved)" if name == best_name else ""
        print(
            f"| {name}{star} | {m['accuracy']:.4f} | {m['precision']:.4f} "
            f"| {m['recall']:.4f} | {m['f1']:.4f} | {m['roc_auc']:.4f} |"
        )

    print("\n[OK] Best model:", best_name)
    print("[OK] Model saved to:", args.model_out)
    print("[OK] Metrics saved to:", args.metrics_out)


if __name__ == "__main__":
    main()
