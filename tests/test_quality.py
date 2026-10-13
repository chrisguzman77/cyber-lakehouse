from __future__ import annotations

import string

import pytest

pytest.importorskip("pyspark")

from src.utils.quality import CHECKS, LAYERS, run_checks  # noqa: E402


def make_views(spark, *, dirty: bool = False, with_predictions: bool = True) -> dict[str, str]:
    # 10 flows, 1 attack (10% share, inside the 2-25% band)
    silver = [(f"h{i}", 1 if i == 0 else 0, 0.5) for i in range(10)]
    gold = [(f"h{i}", 1 if i == 0 else 0, 0.5, 10, 2.0, "tcp") for i in range(10)]
    preds = [(f"h{i}", 0.1, "normal") for i in range(10)]
    if dirty:
        silver[1] = ("h1", None, 0.5)  # null label
        silver[2] = ("h1", 0, 0.5)  # duplicate key
        preds[3] = ("h3", 1.5, "dos")  # probability out of bounds
        preds = preds[:-1]  # one gold flow left unscored
    spark.createDataFrame(
        silver, "flow_hash string, label_int int, dur double"
    ).createOrReplaceTempView("q_silver")
    spark.createDataFrame(
        gold,
        "flow_hash string, is_attack int, dur double, sbytes long, pkts_per_sec double, proto string",
    ).createOrReplaceTempView("q_gold")
    tables = {"silver": "q_silver", "gold": "q_gold"}
    if with_predictions:
        spark.createDataFrame(
            preds, "flow_hash string, pred_proba double, pred_attack_cat string"
        ).createOrReplaceTempView("q_predictions")
        tables["predictions"] = "q_predictions"
    return tables


def statuses(df) -> dict[str, str]:
    return {r.check_name: r.status for r in df.collect()}


def test_checks_are_well_formed():
    names = [c.name for c in CHECKS]
    assert len(names) == len(set(names))
    for c in CHECKS:
        placeholders = {f for _, f, _, _ in string.Formatter().parse(c.sql) if f}
        assert placeholders and placeholders <= set(c.needs) <= set(LAYERS), c.name
        assert c.layer in LAYERS


def test_clean_tables_pass_every_check(spark):
    out = run_checks(spark, make_views(spark))
    assert set(statuses(out).values()) == {"pass"}
    assert out.count() == len(CHECKS)
    assert all(r.violations == 0 for r in out.collect())


def test_dirty_tables_fail_exactly_the_broken_checks(spark):
    out = statuses(run_checks(spark, make_views(spark, dirty=True)))
    failed = {name for name, status in out.items() if status == "fail"}
    assert failed == {
        "null_label",
        "duplicate_flow_hash_silver",
        "pred_proba_out_of_bounds",
        "unscored_flows",
    }


def test_missing_layer_marks_its_checks_missing(spark):
    out = run_checks(spark, make_views(spark, with_predictions=False)).collect()
    for r in out:
        check = next(c for c in CHECKS if c.name == r.check_name)
        if "predictions" in check.needs:
            assert r.status == "missing" and r.violations is None
        else:
            assert r.status == "pass"


def test_results_share_one_run_timestamp(spark):
    out = run_checks(spark, make_views(spark)).collect()
    assert len({r.run_ts for r in out}) == 1
    assert {"run_ts", "check_name", "layer", "description", "violations", "status"} == set(
        out[0].asDict()
    )
