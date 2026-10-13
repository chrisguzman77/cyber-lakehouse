# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Train Models
# MAGIC Freezes the frequency-encoding map into `cyber_lakehouse.gold.freq_encodings`,
# MAGIC splits a stratified sample 60/20/20 (train/validation/test), and benchmarks:
# MAGIC - **binary** alerting models (RandomForest, LogisticRegression,
# MAGIC   HistGradientBoosting): each threshold is tuned on validation as the highest
# MAGIC   precision with recall ≥ 95%; the best model has the highest validation precision.
# MAGIC - **attack-family** models (normal + 9 families), selected by validation macro-F1.
# MAGIC
# MAGIC Test metrics are reporting-only. Logs every model to MLflow, writes
# MAGIC `gold.model_metrics` / `gold.family_model_metrics`, saves both models and
# MAGIC `model_meta.json` (threshold, features) to the artifacts Volume, and registers
# MAGIC both best models in Unity Catalog. Uses the same `src/utils/modeling.py` as
# MAGIC the local pipeline.

# COMMAND ----------

# MAGIC %pip install scikit-learn joblib mlflow

# COMMAND ----------

import json
import os
import sys
from datetime import UTC, datetime

sys.path.append(os.path.abspath(".."))

import joblib
import mlflow

from src.utils.features import apply_frequency_encodings, compute_frequency_encodings
from src.utils.modeling import (
    TARGET_RECALL,
    benchmark_binary,
    benchmark_family,
    build_models,
    split_train_val_test,
)

FEATURES_TABLE = "cyber_lakehouse.gold.features"
ENCODINGS_TABLE = "cyber_lakehouse.gold.freq_encodings"
METRICS_TABLE = "cyber_lakehouse.gold.model_metrics"
FAMILY_METRICS_TABLE = "cyber_lakehouse.gold.family_model_metrics"
ARTIFACTS = "/Volumes/cyber_lakehouse/gold/artifacts"
MODEL_PATH = f"{ARTIFACTS}/model.pkl"
FAMILY_MODEL_PATH = f"{ARTIFACTS}/attack_family_model.pkl"
META_PATH = f"{ARTIFACTS}/model_meta.json"
SEED = 42
MAX_ROWS = 300_000  # keep serverless compute quota-friendly

# COMMAND ----------

sdf = spark.table(FEATURES_TABLE)  # noqa: F821

# Freeze the frequency-encoding map over all gold rows; notebook 05 applies
# this exact table, so train and serve encodings match.
compute_frequency_encodings(sdf).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(ENCODINGS_TABLE)
sdf = apply_frequency_encodings(sdf, spark.table(ENCODINGS_TABLE))  # noqa: F821

n_total = sdf.count()
fraction = min(1.0, MAX_ROWS / n_total)
if fraction < 1.0:
    sdf = sdf.sampleBy("is_attack", {0: fraction, 1: fraction}, seed=SEED)

ignore = {"flow_hash", "attack_cat_norm", "ingest_ts", "is_attack"}
feature_cols = [
    c for c, t in sdf.dtypes if c not in ignore and t in ("double", "float", "int", "bigint")
]
pdf = sdf.select(*feature_cols, "is_attack", "attack_cat_norm").toPandas()

splits = split_train_val_test(
    pdf[feature_cols].astype("float32"),
    pdf["is_attack"].astype(int),
    pdf["attack_cat_norm"],
    seed=SEED,
)

# COMMAND ----------

bin_results, bin_fitted, best = benchmark_binary(build_models(SEED), splits, TARGET_RECALL)
fam_results, fam_fitted, best_fam = benchmark_family(build_models(SEED), splits)
threshold = bin_results[best]["threshold"]
print("Best binary model:", best, f"(threshold {threshold:.4f})")
print("Best family model:", best_fam)

# COMMAND ----------

common = {"seed": SEED, "max_rows": MAX_ROWS, "n_features": len(feature_cols)}
for name, r in bin_results.items():
    with mlflow.start_run(run_name=f"binary_{name}"):
        mlflow.log_params(
            {**common, "task": "binary", "model": name, "target_recall": TARGET_RECALL}
        )
        mlflow.log_metric("threshold", r["threshold"])
        mlflow.log_metrics({f"val_{k}": v for k, v in r["validation"].items()})
        mlflow.log_metrics({f"test_{k}": v for k, v in r["test_at_threshold"].items()})
        mlflow.log_metrics({f"test_{k}_at_0_5": v for k, v in r["test_at_0_5"].items()})
for name, r in fam_results.items():
    with mlflow.start_run(run_name=f"family_{name}"):
        mlflow.log_params({**common, "task": "family", "model": name})
        mlflow.log_metrics(
            {
                "val_macro_f1": r["validation_macro_f1"],
                "test_macro_f1": r["test"]["macro_f1"],
                "test_weighted_f1": r["test"]["weighted_f1"],
                "test_accuracy": r["test"]["accuracy"],
            }
        )
        mlflow.log_dict(r["test"], "family_test_metrics.json")

metrics_rows = [
    {
        "model": name,
        "selected": name == best,
        "threshold": r["threshold"],
        "val_precision": r["validation"]["precision"],
        "val_recall": r["validation"]["recall"],
        **{f"test_{k}": v for k, v in r["test_at_threshold"].items()},
        "test_f1_at_0_5": r["test_at_0_5"]["f1"],
        "train_seconds": r["train_seconds"],
    }
    for name, r in bin_results.items()
]
family_rows = [
    {
        "model": name,
        "selected": name == best_fam,
        "val_macro_f1": r["validation_macro_f1"],
        "test_macro_f1": r["test"]["macro_f1"],
        "test_accuracy": r["test"]["accuracy"],
        "train_seconds": r["train_seconds"],
    }
    for name, r in fam_results.items()
]
for rows, table in ((metrics_rows, METRICS_TABLE), (family_rows, FAMILY_METRICS_TABLE)):
    spark.createDataFrame(rows).write.format("delta").mode("overwrite").option(  # noqa: F821
        "overwriteSchema", "true"
    ).saveAsTable(table)
display(spark.table(METRICS_TABLE))  # noqa: F821
display(spark.table(FAMILY_METRICS_TABLE))  # noqa: F821

# COMMAND ----------

# UC Volumes are FUSE-mounted, so joblib and open() can write to the paths directly
joblib.dump(bin_fitted[best], MODEL_PATH)
joblib.dump(fam_fitted[best_fam], FAMILY_MODEL_PATH)
with open(META_PATH, "w", encoding="utf-8") as f:
    json.dump(
        {
            "binary_model": best,
            "threshold": threshold,
            "target_recall": TARGET_RECALL,
            "family_model": best_fam,
            "family_labels": fam_results[best_fam]["test"]["confusion_matrix"]["labels"],
            "features": feature_cols,
            "trained_at": datetime.now(UTC).isoformat(timespec="seconds"),
        },
        f,
        indent=2,
    )
print("Saved:", MODEL_PATH, FAMILY_MODEL_PATH, META_PATH)

# COMMAND ----------

# Register both best models in the Unity Catalog model registry. Signatures are
# inferred from input_example (required for UC registration). The alert
# threshold is logged with the binary model; scoring reads it from META_PATH.
mlflow.set_registry_uri("databricks-uc")
example = splits["test"]["X"].head(5)
with mlflow.start_run(run_name=f"register_{best}"):
    mlflow.log_params({"threshold": threshold, "target_recall": TARGET_RECALL})
    mlflow.sklearn.log_model(
        bin_fitted[best],
        "model",
        input_example=example,
        registered_model_name="cyber_lakehouse.gold.attack_classifier",
    )
with mlflow.start_run(run_name=f"register_family_{best_fam}"):
    mlflow.sklearn.log_model(
        fam_fitted[best_fam],
        "model",
        input_example=example,
        registered_model_name="cyber_lakehouse.gold.attack_family_classifier",
    )
print(
    "Registered: cyber_lakehouse.gold.attack_classifier, "
    "cyber_lakehouse.gold.attack_family_classifier"
)
