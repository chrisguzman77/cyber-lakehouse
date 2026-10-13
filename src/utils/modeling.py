"""Model training and evaluation, shared by the local training job and the
Databricks notebook. Pure pandas/scikit-learn (no Spark), so it is unit-tested
on any machine.

Evaluation protocol:
- 60/20/20 train/validation/test split, stratified by attack family.
- Binary (attack vs benign): each model's alert threshold is tuned on
  validation as the highest-precision threshold with recall >= TARGET_RECALL;
  the best model is the one with the highest validation precision at that
  floor. The test split is only used for reporting.
- Family (10 classes: normal + 9 attack families): best model by validation
  macro-F1; per-class metrics and a confusion matrix reported on test.
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# SOC operating point: catch at least this share of attacks, then cut false alarms.
TARGET_RECALL = 0.95

# Smallest family that gets its own stratum: a 60/20/20 split needs >= 1 row
# per split, and the two-stage split needs >= 2 per stage.
MIN_STRATUM = 5


def build_models(seed: int) -> dict:
    """Fresh, unfitted candidates; used for both the binary and family tasks."""
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


def split_train_val_test(
    X: pd.DataFrame, y_bin: pd.Series, y_cat: pd.Series, seed: int
) -> dict[str, dict]:
    """60/20/20 split stratified by attack family (which also stratifies the
    binary label). Returns {"train"|"val"|"test": {"X", "y_bin", "y_cat"}}.

    Families too rare to stratify on their own (< MIN_STRATUM rows, e.g. worms
    in a small sample) share one stratum; the models still see real labels."""
    counts = y_cat.value_counts()
    rare = counts.index[counts < MIN_STRATUM]
    strata = y_cat.where(~y_cat.isin(rare), "_rare").to_numpy()
    if (strata == "_rare").sum() < MIN_STRATUM:
        # Too few rare rows even pooled: fold them into the largest stratum.
        strata[strata == "_rare"] = counts.index[0]

    idx = np.arange(len(X))
    rest, test = train_test_split(idx, test_size=0.2, random_state=seed, stratify=strata)
    train, val = train_test_split(rest, test_size=0.25, random_state=seed, stratify=strata[rest])
    return {
        name: {"X": X.iloc[part], "y_bin": y_bin.iloc[part], "y_cat": y_cat.iloc[part]}
        for name, part in (("train", train), ("val", val), ("test", test))
    }


def choose_threshold(y_true, proba, target_recall: float = TARGET_RECALL) -> dict:
    """Highest-precision threshold whose recall is >= target_recall (ties go to
    higher recall). A flow is an attack when proba >= threshold."""
    precision, recall, thresholds = precision_recall_curve(y_true, proba)
    # The curve's last point (recall 0) has no threshold.
    precision, recall = precision[:-1], recall[:-1]
    candidates = np.flatnonzero(recall >= target_recall)
    best = max(candidates, key=lambda i: (precision[i], recall[i]))
    return {
        "threshold": float(thresholds[best]),
        "precision": float(precision[best]),
        "recall": float(recall[best]),
    }


def binary_metrics(y_true, proba, threshold: float) -> dict:
    preds = (np.asarray(proba) >= threshold).astype(int)
    return {
        "accuracy": float(accuracy_score(y_true, preds)),
        "precision": float(precision_score(y_true, preds, zero_division=0)),
        "recall": float(recall_score(y_true, preds, zero_division=0)),
        "f1": float(f1_score(y_true, preds, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_true, proba)),
    }


def multiclass_metrics(y_true, y_pred, labels: list[str]) -> dict:
    report = classification_report(y_true, y_pred, labels=labels, output_dict=True, zero_division=0)
    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "macro_f1": float(report["macro avg"]["f1-score"]),
        "weighted_f1": float(report["weighted avg"]["f1-score"]),
        "per_class": {
            label: {
                "precision": float(report[label]["precision"]),
                "recall": float(report[label]["recall"]),
                "f1": float(report[label]["f1-score"]),
                "support": int(report[label]["support"]),
            }
            for label in labels
        },
        "confusion_matrix": {
            "labels": labels,
            "matrix": confusion_matrix(y_true, y_pred, labels=labels).tolist(),
        },
    }


def benchmark_binary(
    models: dict, splits: dict, target_recall: float = TARGET_RECALL
) -> tuple[dict, dict, str]:
    """Fit each model on train, tune its threshold on val, report on test.
    Returns (results, fitted, best_name)."""
    tr, va, te = splits["train"], splits["val"], splits["test"]
    results, fitted = {}, {}
    for name, model in models.items():
        start = time.monotonic()
        model.fit(tr["X"], tr["y_bin"])
        train_seconds = round(time.monotonic() - start, 2)

        tuned = choose_threshold(va["y_bin"], model.predict_proba(va["X"])[:, 1], target_recall)
        test_proba = model.predict_proba(te["X"])[:, 1]
        results[name] = {
            "threshold": tuned["threshold"],
            "validation": {"precision": tuned["precision"], "recall": tuned["recall"]},
            "test_at_threshold": binary_metrics(te["y_bin"], test_proba, tuned["threshold"]),
            "test_at_0_5": binary_metrics(te["y_bin"], test_proba, 0.5),
            "train_seconds": train_seconds,
        }
        fitted[name] = model
    best = max(
        results,
        key=lambda n: (results[n]["validation"]["precision"], results[n]["validation"]["recall"]),
    )
    return results, fitted, best


def benchmark_family(models: dict, splits: dict) -> tuple[dict, dict, str]:
    """Fit each model on train to predict the attack family, select by val
    macro-F1, report on test. Returns (results, fitted, best_name)."""
    tr, va, te = splits["train"], splits["val"], splits["test"]
    labels = sorted(tr["y_cat"].unique())
    results, fitted = {}, {}
    for name, model in models.items():
        start = time.monotonic()
        model.fit(tr["X"], tr["y_cat"])
        train_seconds = round(time.monotonic() - start, 2)

        results[name] = {
            "validation_macro_f1": float(
                f1_score(va["y_cat"], model.predict(va["X"]), labels=labels, average="macro")
            ),
            "test": multiclass_metrics(te["y_cat"], model.predict(te["X"]), labels),
            "train_seconds": train_seconds,
        }
        fitted[name] = model
    best = max(results, key=lambda n: results[n]["validation_macro_f1"])
    return results, fitted, best
