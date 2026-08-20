# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Train Models
# MAGIC Trains RandomForest, LogisticRegression, and HistGradientBoosting on a
# MAGIC stratified sample of the gold features, writes a comparison table to
# MAGIC `cyber_lakehouse.gold.model_metrics`, and saves the best model (by F1)
# MAGIC to a Unity Catalog Volume.

# COMMAND ----------

# MAGIC %pip install scikit-learn joblib

# COMMAND ----------

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

FEATURES_TABLE = "cyber_lakehouse.gold.features"
METRICS_TABLE = "cyber_lakehouse.gold.model_metrics"
MODEL_PATH = "/Volumes/cyber_lakehouse/gold/artifacts/model.pkl"
SEED = 42
MAX_ROWS = 300_000  # keep serverless compute quota-friendly

# COMMAND ----------

sdf = spark.table(FEATURES_TABLE)  # noqa: F821
n_total = sdf.count()
fraction = min(1.0, MAX_ROWS / n_total)
if fraction < 1.0:
    sdf = sdf.sampleBy("is_attack", {0: fraction, 1: fraction}, seed=SEED)

ignore = {"flow_hash", "attack_cat_norm", "ingest_ts"}
cols = [c for c, t in sdf.dtypes if c not in ignore and t in ("double", "float", "int", "bigint")]
pdf = sdf.select(*cols).toPandas()

X = pdf.drop(columns=["is_attack"])
y = pdf["is_attack"].astype(int)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=SEED, stratify=y
)

# COMMAND ----------

models = {
    "random_forest": RandomForestClassifier(
        n_estimators=150, max_depth=20, random_state=SEED, n_jobs=-1, class_weight="balanced"
    ),
    "logistic_regression": Pipeline(
        [
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(max_iter=1000, class_weight="balanced", random_state=SEED)),
        ]
    ),
    "hist_gradient_boosting": HistGradientBoostingClassifier(
        random_state=SEED, class_weight="balanced"
    ),
}

rows = []
fitted = {}
for name, model in models.items():
    start = time.monotonic()
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    proba = model.predict_proba(X_test)[:, 1]
    rows.append(
        {
            "model": name,
            "accuracy": float(accuracy_score(y_test, preds)),
            "precision": float(precision_score(y_test, preds, zero_division=0)),
            "recall": float(recall_score(y_test, preds, zero_division=0)),
            "f1": float(f1_score(y_test, preds, zero_division=0)),
            "roc_auc": float(roc_auc_score(y_test, proba)),
            "train_seconds": round(time.monotonic() - start, 2),
            "n_rows_sampled": int(len(pdf)),
            "n_features": int(X.shape[1]),
            "seed": SEED,
        }
    )
    fitted[name] = model

metrics_df = spark.createDataFrame(rows)  # noqa: F821
metrics_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    METRICS_TABLE
)
display(metrics_df)  # noqa: F821

# COMMAND ----------

best = max(rows, key=lambda r: r["f1"])["model"]
# UC Volumes are FUSE-mounted, so joblib can write to the path directly
joblib.dump(fitted[best], MODEL_PATH)
print("Best model:", best, "->", MODEL_PATH)
