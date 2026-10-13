"""Data-quality checks, shared by the local job and the Databricks notebook.

Each check is a SQL query returning one number: how many violations it found
(0 = pass). Queries use {layer} placeholders so the same checks run against
local Delta paths (delta.`/path`) or Unity Catalog table names. Results are
appended to a quality_results table, one row per check per run, so the
dashboard can show the latest status and the history.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession

LAYERS = ("silver", "gold", "predictions")

RESULT_SCHEMA = (
    "run_ts timestamp, check_name string, layer string, description string, "
    "violations bigint, status string"
)


@dataclass(frozen=True)
class Check:
    name: str
    layer: str  # the layer the check is about, for display
    description: str
    sql: str  # one row, one column: the violation count
    needs: tuple[str, ...]  # layers the query reads


CHECKS = [
    Check(
        "null_label",
        "silver",
        "label_int is null",
        "SELECT COUNT(*) FROM {silver} WHERE label_int IS NULL",
        ("silver",),
    ),
    Check(
        "negative_duration",
        "silver",
        "dur < 0",
        "SELECT COUNT(*) FROM {silver} WHERE dur < 0",
        ("silver",),
    ),
    Check(
        "duplicate_flow_hash_silver",
        "silver",
        "flow_hash is not unique",
        "SELECT COUNT(*) - COUNT(DISTINCT flow_hash) FROM {silver}",
        ("silver",),
    ),
    Check(
        "duplicate_flow_hash_gold",
        "gold",
        "flow_hash is not unique",
        "SELECT COUNT(*) - COUNT(DISTINCT flow_hash) FROM {gold}",
        ("gold",),
    ),
    Check(
        "is_attack_domain",
        "gold",
        "is_attack is null or not 0/1",
        "SELECT COUNT(*) FROM {gold} WHERE is_attack IS NULL OR is_attack NOT IN (0, 1)",
        ("gold",),
    ),
    Check(
        "key_feature_nulls",
        "gold",
        "null dur, sbytes, pkts_per_sec, or proto",
        "SELECT COUNT(*) FROM {gold} "
        "WHERE dur IS NULL OR sbytes IS NULL OR pkts_per_sec IS NULL OR proto IS NULL",
        ("gold",),
    ),
    Check(
        # The raw data is ~13% attacks, but attack flows are heavily duplicated,
        # so after flow-level dedupe the share sits around 4%.
        "attack_share_out_of_band",
        "gold",
        "attack share outside 2-25% (1 = out of band)",
        "SELECT CASE WHEN AVG(is_attack) BETWEEN 0.02 AND 0.25 THEN 0 ELSE 1 END FROM {gold}",
        ("gold",),
    ),
    Check(
        "pred_proba_out_of_bounds",
        "predictions",
        "pred_proba is null or outside [0, 1]",
        "SELECT COUNT(*) FROM {predictions} "
        "WHERE pred_proba IS NULL OR pred_proba < 0 OR pred_proba > 1",
        ("predictions",),
    ),
    Check(
        "pred_attack_cat_null",
        "predictions",
        "attack-family prediction is null",
        "SELECT COUNT(*) FROM {predictions} WHERE pred_attack_cat IS NULL",
        ("predictions",),
    ),
    Check(
        "unscored_flows",
        "predictions",
        "gold flows without a prediction",
        "SELECT (SELECT COUNT(*) FROM {gold}) - (SELECT COUNT(*) FROM {predictions})",
        ("gold", "predictions"),
    ),
    Check(
        "duplicate_predictions",
        "predictions",
        "a flow scored more than once",
        "SELECT COUNT(*) - COUNT(DISTINCT flow_hash) FROM {predictions}",
        ("predictions",),
    ),
]


def run_checks(spark: SparkSession, tables: dict[str, str]) -> DataFrame:
    """Run every check. tables maps layer -> SQL table reference, for the
    layers that exist; checks needing an absent layer are marked "missing"
    (e.g. predictions before the first training run)."""
    run_ts = datetime.now(UTC)
    rows = []
    for c in CHECKS:
        if not set(c.needs) <= set(tables):
            violations, status = None, "missing"
        else:
            violations = int(spark.sql(c.sql.format(**tables)).first()[0])
            status = "pass" if violations == 0 else "fail"
        rows.append((run_ts, c.name, c.layer, c.description, violations, status))
    return spark.createDataFrame(rows, RESULT_SCHEMA)
