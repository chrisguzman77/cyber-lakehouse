from __future__ import annotations

import csv
import datetime as dt
import shutil

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from src.utils.cleaning import clean_traffic  # noqa: E402
from src.utils.incremental import (  # noqa: E402
    append_stream,
    cdf_inserts,
    first_per_key,
    insert_new_keys,
    run_insert_stream,
)
from src.utils.schema import build_full_schema  # noqa: E402

T1 = dt.datetime(2026, 1, 1, 0, 0, 1)
T2 = dt.datetime(2026, 1, 1, 0, 0, 2)


def test_first_per_key_keeps_earliest(spark):
    df = spark.createDataFrame(
        [("k1", T2, "b.csv", "late"), ("k1", T1, "a.csv", "early"), ("k2", T2, "a.csv", "only")],
        ["flow_hash", "ingest_ts", "source_file", "v"],
    )
    out = {r.flow_hash: r.v for r in first_per_key(df).collect()}
    assert out == {"k1": "early", "k2": "only"}


def test_first_per_key_breaks_ingest_ts_ties_by_source_file(spark):
    df = spark.createDataFrame(
        [("k1", T1, "b.csv", "b"), ("k1", T1, "a.csv", "a")],
        ["flow_hash", "ingest_ts", "source_file", "v"],
    )
    assert [r.v for r in first_per_key(df).collect()] == ["a"]


def test_insert_new_keys_is_idempotent_and_first_seen_wins(spark, tmp_path):
    path = str(tmp_path / "t")
    batch = spark.createDataFrame([("k1", "first"), ("k2", "first")], ["flow_hash", "v"])

    insert_new_keys(spark, batch, path=path)
    insert_new_keys(spark, batch, path=path)  # replayed batch
    assert spark.read.format("delta").load(path).count() == 2

    later = spark.createDataFrame([("k1", "second"), ("k3", "second")], ["flow_hash", "v"])
    insert_new_keys(spark, later, path=path)
    out = {r.flow_hash: r.v for r in spark.read.format("delta").load(path).collect()}
    assert out == {"k1": "first", "k2": "first", "k3": "second"}


def test_insert_new_keys_enables_change_data_feed(spark, tmp_path):
    path = str(tmp_path / "t")
    insert_new_keys(spark, spark.createDataFrame([("k1", 1)], ["flow_hash", "v"]), path=path)
    insert_new_keys(spark, spark.createDataFrame([("k2", 2)], ["flow_hash", "v"]), path=path)

    changes = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .load(path)
    )
    out = cdf_inserts(changes)
    assert sorted(r.flow_hash for r in out.collect()) == ["k1", "k2"]
    assert out.columns == ["flow_hash", "v"]


def test_cdf_inserts_drops_non_inserts(spark):
    df = spark.createDataFrame(
        [("k1", "insert", 1, T1), ("k2", "update_preimage", 2, T1), ("k3", "delete", 2, T1)],
        ["flow_hash", "_change_type", "_commit_version", "_commit_timestamp"],
    )
    assert [r.flow_hash for r in cdf_inserts(df).collect()] == ["k1"]


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(["" if v is None else v for v in row] for row in rows)


def run_layers(spark, incoming, root):
    """Bronze -> silver -> gold exactly as the local jobs wire it."""
    raw = (
        spark.readStream.schema(build_full_schema())
        .option("header", False)
        .option("maxFilesPerTrigger", 1)
        .csv(str(incoming))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )
    append_stream(raw, str(root / "chk_bronze"), path=str(root / "bronze"))

    bronze = spark.readStream.format("delta").load(str(root / "bronze"))
    run_insert_stream(
        clean_traffic(bronze),
        str(root / "chk_silver"),
        batch_fn=first_per_key,
        path=str(root / "silver"),
    )

    silver_changes = (
        spark.readStream.format("delta").option("readChangeFeed", "true").load(str(root / "silver"))
    )
    run_insert_stream(
        cdf_inserts(silver_changes).select("flow_hash", "is_attack"),
        str(root / "chk_gold"),
        path=str(root / "gold"),
    )

    def count(layer):
        return spark.read.format("delta").load(str(root / layer)).count()

    return count("bronze"), count("silver"), count("gold")


def test_pipeline_is_idempotent_across_reruns_and_redelivery(spark, raw_row, tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    write_csv(incoming / "a.csv", [raw_row(srcip="r1"), raw_row(srcip="r2")])
    # r2 again (same flow identity) in a different file
    write_csv(incoming / "b.csv", [raw_row(srcip="r2"), raw_row(srcip="r3", label=1)])

    assert run_layers(spark, incoming, tmp_path) == (4, 3, 3)

    # Rerun with no new files: checkpoints mean nothing is reprocessed
    assert run_layers(spark, incoming, tmp_path) == (4, 3, 3)

    # Re-delivery under a new name: bronze keeps the raw copy, silver/gold don't change
    shutil.copy(incoming / "a.csv", incoming / "a-redelivered.csv")
    assert run_layers(spark, incoming, tmp_path) == (6, 3, 3)

    # Genuinely new flow flows all the way through
    write_csv(incoming / "c.csv", [raw_row(srcip="r4")])
    assert run_layers(spark, incoming, tmp_path) == (7, 4, 4)
