from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

import joblib

from src.utils.io import GOLD_DIR, MODELS_DIR, REPORTS_DIR, ensure_dirs, model_artifacts
from src.utils.modeling import (
    TARGET_RECALL,
    benchmark_binary,
    benchmark_family,
    build_models,
    split_train_val_test,
)

NUMERIC_SPARK_TYPES = ("double", "float", "int", "bigint")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--features_path", default=str(GOLD_DIR / "features"))
    p.add_argument("--encodings_out", default=str(GOLD_DIR / "freq_encodings"))
    p.add_argument(
        "--model_out",
        default=str(MODELS_DIR / "trained_model.pkl"),
        help="Binary model; the family model and model_meta.json are written beside it",
    )
    p.add_argument("--metrics_out", default=str(REPORTS_DIR / "metrics.json"))
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--max_rows",
        type=int,
        default=500_000,
        help="Stratified sample size taken in Spark before pandas conversion",
    )
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()

    # Read Delta features through Spark -> Pandas, so it's consistent with the lakehouse.
    # imported here so local unit tests don't need pyspark
    from pyspark.sql import SparkSession

    from src.utils.features import apply_frequency_encodings, compute_frequency_encodings

    # Arrow keeps toPandas columnar: without it the driver JVM materializes the
    # whole sample as Java objects and keeps that heap while sklearn trains.
    spark = (
        SparkSession.builder.appName("04_train_model")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    sdf = spark.read.format("delta").load(args.features_path)

    # Freeze the frequency-encoding map over all gold rows and persist it;
    # 05_score_predictions applies this exact table, so train and serve match.
    (
        compute_frequency_encodings(sdf)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(args.encodings_out)
    )
    sdf = apply_frequency_encodings(sdf, spark.read.format("delta").load(args.encodings_out))

    n_total = sdf.count()
    # Stratified downsample in Spark: identical fraction per class preserves the
    # attack/benign ratio while keeping the pandas frame memory-bounded.
    fraction = min(1.0, args.max_rows / n_total)
    if fraction < 1.0:
        sdf = sdf.sampleBy("is_attack", {0: fraction, 1: fraction}, seed=args.seed)

    ignore = {"flow_hash", "attack_cat_norm", "ingest_ts", "is_attack"}
    feature_cols = [c for c, t in sdf.dtypes if c not in ignore and t in NUMERIC_SPARK_TYPES]
    pdf = sdf.select(*feature_cols, "is_attack", "attack_cat_norm").toPandas()
    spark.stop()
    n_sampled = len(pdf)

    # float32 halves memory; precision is irrelevant for these models
    splits = split_train_val_test(
        pdf[feature_cols].astype("float32"),
        pdf["is_attack"].astype(int),
        pdf["attack_cat_norm"],
        seed=args.seed,
    )
    del pdf  # the splits hold their own copies

    # Binary alerting model: threshold tuned on validation, selected by
    # validation precision at the recall floor; test is reporting-only.
    bin_results, bin_fitted, best = benchmark_binary(
        build_models(args.seed), splits, target_recall=TARGET_RECALL
    )
    for name, r in bin_results.items():
        print(f"[OK] Binary {name} ({r['train_seconds']}s): threshold={r['threshold']:.4f} {r}")
    bin_model = bin_fitted[best]
    del bin_fitted  # free the losing models before training the family models

    # Attack-family model: 10 classes (normal + 9 families), selected by
    # validation macro-F1.
    fam_results, fam_fitted, best_fam = benchmark_family(build_models(args.seed), splits)
    for name, r in fam_results.items():
        print(
            f"[OK] Family {name} ({r['train_seconds']}s): "
            f"val macro-F1={r['validation_macro_f1']:.4f} test macro-F1={r['test']['macro_f1']:.4f}"
        )

    paths = model_artifacts(args.model_out)
    threshold = bin_results[best]["threshold"]
    joblib.dump(bin_model, paths["binary"])
    joblib.dump(fam_fitted[best_fam], paths["family"])
    meta = {
        "binary_model": best,
        "threshold": threshold,
        "target_recall": TARGET_RECALL,
        "family_model": best_fam,
        "family_labels": fam_results[best_fam]["test"]["confusion_matrix"]["labels"],
        "features": feature_cols,
        "trained_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    with open(paths["meta"], "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    metrics = {
        "dataset": {
            "n_rows_total": int(n_total),
            "n_rows_sampled": n_sampled,
            "sample_strategy": f"stratified sampleBy is_attack, max_rows={args.max_rows}",
            "split": "60/20/20 train/validation/test, stratified by attack_cat_norm",
            "n_features": len(feature_cols),
            "n_train": len(splits["train"]["X"]),
            "n_val": len(splits["val"]["X"]),
            "n_test": len(splits["test"]["X"]),
        },
        "threshold_policy": {
            "target_recall": TARGET_RECALL,
            "rule": "highest validation precision with validation recall >= target_recall",
        },
        "models": bin_results,
        "best_model": best,
        "threshold": threshold,
        "family_models": fam_results,
        "best_family_model": best_fam,
        "features_used": feature_cols,
        "seed": args.seed,
    }
    with open(args.metrics_out, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    # Markdown tables (copy-paste source for the README)
    print("\n| Model | Threshold | Precision | Recall | F1 | ROC-AUC | F1 @ 0.5 |")
    print("|---|---|---|---|---|---|---|")
    for name, r in bin_results.items():
        t, d = r["test_at_threshold"], r["test_at_0_5"]
        star = " (saved)" if name == best else ""
        print(
            f"| {name}{star} | {r['threshold']:.4f} | {t['precision']:.4f} | {t['recall']:.4f} "
            f"| {t['f1']:.4f} | {t['roc_auc']:.4f} | {d['f1']:.4f} |"
        )
    print("\n| Family model | Val macro-F1 | Test macro-F1 | Test accuracy |")
    print("|---|---|---|---|")
    for name, r in fam_results.items():
        star = " (saved)" if name == best_fam else ""
        print(
            f"| {name}{star} | {r['validation_macro_f1']:.4f} | {r['test']['macro_f1']:.4f} "
            f"| {r['test']['accuracy']:.4f} |"
        )

    print("\n[OK] Best binary model:", best, f"(threshold {threshold:.4f})")
    print("[OK] Best family model:", best_fam)
    print("[OK] Models saved to:", paths["binary"], "and", paths["family"])
    print("[OK] Model metadata saved to:", paths["meta"])
    print("[OK] Frequency encodings saved to:", args.encodings_out)
    print("[OK] Metrics saved to:", args.metrics_out)


if __name__ == "__main__":
    main()
