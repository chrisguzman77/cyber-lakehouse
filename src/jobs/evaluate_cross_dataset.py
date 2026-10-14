"""Cross-dataset evaluation: does a detector trained on UNSW-NB15 transfer to
CIC-IDS2017, a different lab, attack set, and flow tool?

1. Portable UNSW model: train on UNSW gold using only PORTABLE_FEATURES (same
   60/20/20 protocol and >=95%-recall threshold as 04_train_model).
2. Transfer: score a CIC test split with that model at its UNSW threshold.
3. Ceiling: train the same model families on CIC itself.

Writes reports/cross_dataset.json. Standalone (not part of the pipeline);
needs data/raw/cic/ (scripts/fetch_cic.sh) and a built gold table.
"""

from __future__ import annotations

import argparse
import json

from src.utils.cross_dataset import (
    CORE_COLS,
    PORTABLE_FEATURES,
    load_cic,
    portable_features,
    transfer_report,
)
from src.utils.io import GOLD_DIR, RAW_DIR, REPORTS_DIR, ensure_dirs
from src.utils.modeling import TARGET_RECALL, benchmark_binary, build_models, split_train_val_test


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--features_path", default=str(GOLD_DIR / "features"))
    p.add_argument("--cic_glob", default=str(RAW_DIR / "cic" / "*.csv"))
    p.add_argument("--reference_metrics", default=str(REPORTS_DIR / "metrics.json"))
    p.add_argument("--out", default=str(REPORTS_DIR / "cross_dataset.json"))
    p.add_argument("--max_rows", type=int, default=500_000, help="Sample size per dataset")
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def best_summary(results: dict, best: str) -> dict:
    r = results[best]
    return {"model": best, "threshold": r["threshold"], "test": r["test_at_threshold"]}


def main() -> None:
    ensure_dirs()
    args = parse_args()

    # imported here so the module's helpers don't need pyspark
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("evaluate_cross_dataset")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    sdf = spark.read.format("delta").load(args.features_path)
    n_unsw = sdf.count()
    fraction = min(1.0, args.max_rows / n_unsw)
    if fraction < 1.0:
        sdf = sdf.sampleBy("is_attack", {0: fraction, 1: fraction}, seed=args.seed)
    unsw = sdf.select(*CORE_COLS, "is_attack", "attack_cat_norm").toPandas()
    spark.stop()

    # 1) Portable UNSW model, same protocol as 04_train_model
    unsw_splits = split_train_val_test(
        portable_features(unsw).astype("float32"),
        unsw["is_attack"].astype(int),
        unsw["attack_cat_norm"],
        seed=args.seed,
    )
    unsw_results, unsw_fitted, unsw_best = benchmark_binary(
        build_models(args.seed), unsw_splits, TARGET_RECALL
    )
    model, threshold = unsw_fitted[unsw_best], unsw_results[unsw_best]["threshold"]
    unsw_test_rows = len(unsw_splits["test"]["X"])
    del unsw_fitted, unsw_splits, unsw
    print(f"[OK] Portable UNSW model: {unsw_best} (threshold {threshold:.4f})")

    # 2) Transfer to a CIC test split
    cic, cic_stats = load_cic(args.cic_glob, args.max_rows, args.seed)
    cic_splits = split_train_val_test(
        portable_features(cic).astype("float32"), cic["is_attack"], cic["label"], seed=args.seed
    )
    del cic
    te = cic_splits["test"]
    transfer = transfer_report(
        te["y_bin"], model.predict_proba(te["X"])[:, 1], te["y_cat"], threshold
    )
    print(f"[OK] Transfer to CIC: {transfer['at_source_threshold']}")

    # 3) Ceiling: the same model families trained on CIC itself
    cic_results, cic_fitted, cic_best = benchmark_binary(
        build_models(args.seed), cic_splits, TARGET_RECALL
    )
    ceiling_model, ceiling_threshold = cic_fitted[cic_best], cic_results[cic_best]["threshold"]
    ceiling = transfer_report(
        te["y_bin"], ceiling_model.predict_proba(te["X"])[:, 1], te["y_cat"], ceiling_threshold
    )
    print(f"[OK] CIC-trained ceiling: {cic_best} {ceiling['at_source_threshold']}")

    with open(args.reference_metrics, encoding="utf-8") as f:
        ref = json.load(f)
    report = {
        "portable_features": PORTABLE_FEATURES,
        "protocol": "60/20/20 split per dataset; threshold = highest validation precision "
        f"at recall >= {TARGET_RECALL}; transfer scored on the CIC test split",
        "data": {
            "unsw_gold_rows": int(n_unsw),
            "unsw_test_rows": unsw_test_rows,
            "cic": cic_stats,
            "cic_test_rows": len(te["X"]),
            "cic_test_attack_share": float(te["y_bin"].mean()),
        },
        "unsw_full_feature_reference": best_summary(
            {ref["best_model"]: ref["models"][ref["best_model"]]}, ref["best_model"]
        ),
        "unsw_portable": {"models": unsw_results, "best": best_summary(unsw_results, unsw_best)},
        "transfer_unsw_to_cic": transfer,
        "cic_ceiling": {
            "models": cic_results,
            "best": best_summary(cic_results, cic_best),
            "on_cic_test": ceiling,
        },
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    rows = [
        (
            "UNSW, all 54 features (reference)",
            "UNSW",
            report["unsw_full_feature_reference"]["test"],
        ),
        ("UNSW, portable features", "UNSW", report["unsw_portable"]["best"]["test"]),
        ("UNSW portable -> CIC (UNSW threshold)", "CIC", transfer["at_source_threshold"]),
        ("CIC-trained ceiling", "CIC", report["cic_ceiling"]["best"]["test"]),
    ]
    print("\n| Model | Tested on | Precision | Recall | F1 | ROC-AUC |")
    print("|---|---|---|---|---|---|")
    for name, where, m in rows:
        print(
            f"| {name} | {where} | {m['precision']:.4f} | {m['recall']:.4f} "
            f"| {m['f1']:.4f} | {m['roc_auc']:.4f} |"
        )
    print("\nCIC detection rate by attack type (UNSW model vs CIC-trained ceiling):")
    for label, d in transfer["detection_rate_by_label"].items():
        c = ceiling["detection_rate_by_label"][label]
        print(f"  {label:28} n={d['flows']:6}  transfer={d['rate']:.3f}  ceiling={c['rate']:.3f}")
    print(f"\n[OK] Report saved to {args.out}")


if __name__ == "__main__":
    main()
