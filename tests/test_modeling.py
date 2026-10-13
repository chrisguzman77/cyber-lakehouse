from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier

from src.utils.modeling import (
    benchmark_binary,
    benchmark_family,
    binary_metrics,
    choose_threshold,
    multiclass_metrics,
    split_train_val_test,
)


def test_choose_threshold_maximizes_precision_subject_to_recall():
    y = np.array([0, 0, 0, 0, 1, 1, 1, 1])
    proba = np.array([0.1, 0.2, 0.3, 0.8, 0.4, 0.7, 0.9, 0.95])
    # At 0.7: TP=3 FP=1 (p=.75, r=.75). At 0.4: TP=4 FP=1 (p=.8, r=1.0) -> better on both.
    # At 0.3: an extra FP drops precision. So 0.4 wins for any target <= 1.0.
    out = choose_threshold(y, proba, target_recall=0.75)
    assert out == {"threshold": 0.4, "precision": 0.8, "recall": 1.0}


def test_choose_threshold_respects_recall_floor():
    y = np.array([0, 0, 1, 1, 1, 1])
    proba = np.array([0.5, 0.6, 0.55, 0.9, 0.95, 0.99])
    # 0.9 gives p=1.0 but r=0.75; the 0.95 floor forces 0.55 (r=1.0, p=4/5).
    assert choose_threshold(y, proba, target_recall=0.7)["threshold"] == 0.9
    out = choose_threshold(y, proba, target_recall=0.95)
    assert out["threshold"] == 0.55
    assert out["recall"] == 1.0
    assert out["precision"] == pytest.approx(0.8)


def test_binary_metrics_counts_proba_equal_to_threshold_as_attack():
    y = np.array([0, 1, 1])
    proba = np.array([0.2, 0.5, 0.9])
    m = binary_metrics(y, proba, threshold=0.5)
    assert m["recall"] == 1.0
    assert m["precision"] == 1.0
    assert set(m) == {"accuracy", "precision", "recall", "f1", "roc_auc"}


def test_multiclass_metrics_report():
    labels = ["normal", "dos", "worms"]
    y_true = ["normal", "normal", "dos", "dos", "worms"]
    y_pred = ["normal", "dos", "dos", "dos", "normal"]
    m = multiclass_metrics(y_true, y_pred, labels)

    assert m["accuracy"] == pytest.approx(3 / 5)
    assert m["per_class"]["dos"] == pytest.approx(
        {"precision": 2 / 3, "recall": 1.0, "f1": 0.8, "support": 2}
    )
    assert m["per_class"]["worms"]["recall"] == 0.0
    assert m["confusion_matrix"]["labels"] == labels
    # rows = true, cols = predicted
    assert m["confusion_matrix"]["matrix"] == [[1, 1, 0], [0, 2, 0], [1, 0, 0]]
    # normal: p=1/2 r=1/2 f1=0.5; dos: f1=0.8; worms: f1=0
    assert m["macro_f1"] == pytest.approx(np.mean([0.5, 0.8, 0.0]))


def make_dataset(n=600, seed=0):
    rng = np.random.default_rng(seed)
    cats = rng.choice(["normal", "dos", "exploits"], size=n, p=[0.7, 0.15, 0.15])
    shift = pd.Series(cats).map({"normal": 0.0, "dos": 2.0, "exploits": -2.0}).to_numpy()
    X = pd.DataFrame({"a": rng.normal(shift, 1.0), "b": rng.normal(-shift, 1.0)})
    y_cat = pd.Series(cats)
    y_bin = (y_cat != "normal").astype(int)
    return X, y_bin, y_cat


def test_split_is_60_20_20_disjoint_stratified_and_deterministic():
    X, y_bin, y_cat = make_dataset()
    s1 = split_train_val_test(X, y_bin, y_cat, seed=42)
    s2 = split_train_val_test(X, y_bin, y_cat, seed=42)

    sizes = [len(s1[part]["X"]) for part in ("train", "val", "test")]
    assert sizes == [360, 120, 120]
    idx = [set(s1[part]["X"].index) for part in ("train", "val", "test")]
    assert not (idx[0] & idx[1] or idx[0] & idx[2] or idx[1] & idx[2])
    for part in ("train", "val", "test"):
        assert s1[part]["X"].index.equals(s2[part]["X"].index)
        share = s1[part]["y_cat"].value_counts(normalize=True)
        assert share["normal"] == pytest.approx(
            y_cat.value_counts(normalize=True)["normal"], abs=0.02
        )
        assert (s1[part]["y_bin"] == (s1[part]["y_cat"] != "normal")).all()


def test_benchmark_binary_tunes_on_validation_and_picks_best():
    X, y_bin, y_cat = make_dataset()
    splits = split_train_val_test(X, y_bin, y_cat, seed=42)
    models = {
        "tree": DecisionTreeClassifier(max_depth=1, random_state=0),
        "logreg": LogisticRegression(),
    }
    results, fitted, best = benchmark_binary(models, splits, target_recall=0.9)

    assert best in results
    for r in results.values():
        assert r["validation"]["recall"] >= 0.9
        assert 0.0 <= r["threshold"] <= 1.0
        assert set(r) == {
            "threshold",
            "validation",
            "test_at_threshold",
            "test_at_0_5",
            "train_seconds",
        }
    # best = highest validation precision at the recall floor
    assert results[best]["validation"]["precision"] == max(
        r["validation"]["precision"] for r in results.values()
    )
    assert set(fitted) == set(models)


def test_benchmark_family_picks_best_macro_f1():
    X, y_bin, y_cat = make_dataset()
    splits = split_train_val_test(X, y_bin, y_cat, seed=42)
    models = {"logreg": LogisticRegression(max_iter=500)}
    results, fitted, best = benchmark_family(models, splits)

    assert best == "logreg"
    r = results["logreg"]
    assert set(r) == {"validation_macro_f1", "test", "train_seconds"}
    assert set(r["test"]["confusion_matrix"]["labels"]) == {"normal", "dos", "exploits"}
    assert r["test"]["macro_f1"] > 0.5  # separable synthetic data


def test_split_tolerates_very_rare_families():
    # A family with a single row can't be stratified on its own; it must not crash.
    X, y_bin, y_cat = make_dataset()
    y_cat = y_cat.copy()
    y_cat.iloc[0] = "worms"
    y_bin = (y_cat != "normal").astype(int)
    splits = split_train_val_test(X, y_bin, y_cat, seed=42)
    total = sum(len(splits[part]["X"]) for part in ("train", "val", "test"))
    assert total == len(X)
    assert sum((splits[part]["y_cat"] == "worms").sum() for part in ("train", "val", "test")) == 1
