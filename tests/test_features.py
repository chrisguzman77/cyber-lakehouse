from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from src.utils.features import (  # noqa: E402
    DERIVED_FEATURE_COLS,
    FREQ_ENCODED_COLS,
    RAW_FEATURE_COLS,
    add_derived_features,
    apply_frequency_encodings,
    compute_frequency_encodings,
)

ROW_COLS = [
    "sbytes",
    "dbytes",
    "spkts",
    "dpkts",
    "sload",
    "dload",
    "sttl",
    "dttl",
    "sloss",
    "dloss",
    "dur",
]


def make_df(spark, rows):
    return spark.createDataFrame(rows, ROW_COLS)


def test_feature_count_meets_resume_claim():
    total = len(RAW_FEATURE_COLS) + len(DERIVED_FEATURE_COLS) + len(FREQ_ENCODED_COLS)
    assert total >= 40


def test_derived_feature_values(spark):
    df = make_df(spark, [(100, 50, 4, 2, 10.0, 30.0, 64, 32, 1, 0, 2.0)])
    row = add_derived_features(df).collect()[0]

    assert row.bytes_per_pkt_src == 25.0
    assert row.bytes_per_pkt_dst == 25.0
    assert row.byte_ratio_src_dst == pytest.approx(100 / 51)
    assert row.pkt_ratio_src_dst == pytest.approx(4 / 3)
    assert row.load_asymmetry == pytest.approx((10.0 - 30.0) / (10.0 + 30.0 + 1))
    assert row.ttl_delta == 32.0
    assert row.loss_rate_src == pytest.approx(0.25)
    assert row.loss_rate_dst == 0.0
    assert row.pkts_per_sec == pytest.approx(3.0)
    assert row.dur_bucket == 4.0


def test_derived_features_zero_division_guards(spark):
    # spkts=0, dpkts=0, dur=0 must not produce nulls, NaN, or infinities
    df = make_df(spark, [(100, 50, 0, 0, 0.0, 0.0, 64, 64, 0, 0, 0.0)])
    row = add_derived_features(df).collect()[0]

    assert row.bytes_per_pkt_src == 100.0
    assert row.bytes_per_pkt_dst == 50.0
    assert row.pkts_per_sec == 0.0
    assert row.loss_rate_src == 0.0
    assert row.dur_bucket == 0.0


def test_all_derived_columns_added(spark):
    df = make_df(spark, [(1, 1, 1, 1, 1.0, 1.0, 1, 1, 0, 0, 1.0)])
    out = add_derived_features(df)
    for c in DERIVED_FEATURE_COLS:
        assert c in out.columns


def test_compute_frequency_encodings(spark):
    df = spark.createDataFrame(
        [("tcp", "http"), ("tcp", "-"), ("udp", "-"), ("arp", "-")],
        ["proto", "service"],
    )
    enc = compute_frequency_encodings(df, cols=("proto", "service"))
    freqs = {(r.column, r.value): r.freq for r in enc.collect()}
    assert freqs[("proto", "tcp")] == pytest.approx(0.5)
    assert freqs[("proto", "udp")] == pytest.approx(0.25)
    assert freqs[("service", "-")] == pytest.approx(0.75)
    assert freqs[("service", "http")] == pytest.approx(0.25)


def test_apply_frequency_encodings_uses_frozen_map(spark):
    # The map comes from "training" data; a new batch must not recompute it.
    train = spark.createDataFrame([("tcp",), ("tcp",), ("udp",), ("arp",)], ["proto"])
    enc = compute_frequency_encodings(train, cols=("proto",))

    batch = spark.createDataFrame(
        [("tcp", 1), ("tcp", 2), ("tcp", 3), ("icmp", 4)], ["proto", "id"]
    )
    out = apply_frequency_encodings(batch, enc, cols=("proto",))
    freqs = {r.id: r.proto_freq for r in out.collect()}

    assert freqs[1] == pytest.approx(0.5)  # training share, not the batch's 0.75
    assert freqs[4] == 0.0  # unseen value
    assert out.count() == 4
    assert "proto" in out.columns
