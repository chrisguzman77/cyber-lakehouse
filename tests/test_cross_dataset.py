from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.utils.cross_dataset import (
    CORE_COLS,
    PORTABLE_FEATURES,
    cic_to_core,
    normalize_cic_label,
    portable_features,
    transfer_report,
)


def cic_frame(**overrides):
    row = {
        " Flow Duration": 2_000_000,  # µs -> 2.0 s
        " Total Fwd Packets": 4,
        " Total Backward Packets": 2,
        "Total Length of Fwd Packets": 300,  # payload bytes
        " Total Length of Bwd Packets": 1000,
        " Fwd Header Length": 100,  # header bytes
        " Bwd Header Length": 40,
        " Fwd IAT Mean": 5000.0,  # µs -> 5 ms
        " Bwd IAT Mean": 0.0,
        " Label": "BENIGN",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_cic_mapping_converts_units_and_adds_headers():
    core = cic_to_core(cic_frame()).iloc[0]
    assert list(cic_to_core(cic_frame()).columns) == [*CORE_COLS, "label", "is_attack"]
    assert core["dur"] == pytest.approx(2.0)
    assert core["sbytes"] == 400  # payload + header, like UNSW byte counts
    assert core["dbytes"] == 1040
    assert core["spkts"] == 4 and core["dpkts"] == 2
    assert core["sintpkt"] == pytest.approx(5.0)
    assert core["is_attack"] == 0


def test_cic_mapping_cleans_corrupt_values():
    df = pd.concat(
        [
            cic_frame(),
            cic_frame(**{" Flow Duration": -1}),  # negative duration: dropped
            cic_frame(**{" Fwd IAT Mean": np.inf}),  # infinite: dropped
            cic_frame(**{" Fwd Header Length": -20}),  # bad header: clipped to 0
        ],
        ignore_index=True,
    )
    core = cic_to_core(df)
    assert len(core) == 2
    assert core["sbytes"].tolist() == [400, 300]


def test_cic_label_normalization():
    assert normalize_cic_label("BENIGN") == "benign"
    assert normalize_cic_label("Web Attack � Brute Force") == "web attack - brute force"
    assert normalize_cic_label("Web Attack ï¿½ XSS") == "web attack - xss"
    assert normalize_cic_label(" DoS Hulk ") == "dos hulk"
    out = cic_to_core(cic_frame(**{" Label": "PortScan"}))
    assert out["label"].iloc[0] == "portscan" and out["is_attack"].iloc[0] == 1


def test_portable_features_formulas():
    core = pd.DataFrame(
        [
            {
                "dur": 2.0,
                "sbytes": 400,
                "dbytes": 1040,
                "spkts": 4,
                "dpkts": 2,
                "sintpkt": 5.0,
                "dintpkt": 0.0,
            }
        ]
    )
    f = portable_features(core).iloc[0]
    assert list(portable_features(core).columns) == PORTABLE_FEATURES
    assert f["bytes_per_pkt_src"] == 100.0
    assert f["byte_ratio_src_dst"] == pytest.approx(400 / 1041)
    assert f["pkt_ratio_src_dst"] == pytest.approx(4 / 3)
    assert f["pkts_per_sec"] == pytest.approx(3.0)
    assert f["sload"] == pytest.approx(8 * 400 / 2.0)
    assert f["load_asymmetry"] == pytest.approx((1600 - 4160) / (1600 + 4160 + 1))
    assert f["log1p_dur"] == pytest.approx(np.log1p(2.0))
    assert f["dur_bucket"] == 4.0


def test_portable_features_zero_guards():
    core = pd.DataFrame(
        [
            {
                "dur": 0.0,
                "sbytes": 60,
                "dbytes": 0,
                "spkts": 0,
                "dpkts": 0,
                "sintpkt": 0.0,
                "dintpkt": 0.0,
            }
        ]
    )
    f = portable_features(core)
    assert np.isfinite(f.to_numpy()).all()
    assert f["pkts_per_sec"].iloc[0] == 0.0 and f["sload"].iloc[0] == 0.0


def test_portable_features_match_the_spark_gold_derivations(spark):
    # The shared derived features must be computed exactly as gold does.
    from src.utils.features import add_derived_features

    rows = [
        (400, 1040, 4, 2, 1.0, 1.0, 64, 32, 0, 0, 2.0),
        (60, 0, 0, 0, 0.0, 0.0, 0, 0, 0, 0, 0.0),
    ]
    cols = [
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
    gold = add_derived_features(spark.createDataFrame(rows, cols)).toPandas()
    core = gold[["dur", "sbytes", "dbytes", "spkts", "dpkts"]].assign(sintpkt=0.0, dintpkt=0.0)
    ours = portable_features(core)
    shared = [
        "bytes_per_pkt_src",
        "bytes_per_pkt_dst",
        "byte_ratio_src_dst",
        "pkt_ratio_src_dst",
        "pkts_per_sec",
        "log1p_sbytes",
        "log1p_dbytes",
        "log1p_dur",
        "dur_bucket",
    ]
    pd.testing.assert_frame_equal(ours[shared].astype(float), gold[shared].astype(float))


def test_transfer_report():
    y = np.array([0, 0, 0, 0, 1, 1, 1, 1])
    proba = np.array([0.1, 0.2, 0.3, 0.9, 0.2, 0.7, 0.95, 0.99])
    labels = np.array(["benign"] * 4 + ["dos", "dos", "portscan", "portscan"])
    r = transfer_report(y, proba, labels, threshold=0.6)

    assert r["at_source_threshold"]["recall"] == pytest.approx(0.75)  # 3 of 4 attacks
    assert r["at_source_threshold"]["precision"] == pytest.approx(0.75)  # 1 false alarm
    assert r["false_positive_rate"] == pytest.approx(0.25)
    assert r["detection_rate_by_label"] == {
        "dos": {"flows": 2, "detected": 1, "rate": 0.5},
        "portscan": {"flows": 2, "detected": 2, "rate": 1.0},
    }
    # oracle: best threshold on the target itself (not deployable, for reference)
    assert r["oracle_threshold_on_target"]["recall"] >= 0.95
    assert 0.0 <= r["average_precision"] <= 1.0


def test_load_cic_dedupes_and_samples_by_label(tmp_path):
    from src.utils.cross_dataset import load_cic

    base = cic_frame()
    rows = [base] * 3  # three identical benign flows -> one after dedupe
    rows += [cic_frame(**{" Flow Duration": 1000 * i}) for i in range(1, 41)]  # 40 distinct benign
    rows += [cic_frame(**{" Flow Duration": 7 * i, " Label": "DoS Hulk"}) for i in range(1, 21)]
    df = pd.concat(rows, ignore_index=True)
    df.iloc[:30].to_csv(tmp_path / "a.csv", index=False)
    df.iloc[30:].to_csv(tmp_path / "b.csv", index=False)

    full, stats = load_cic(str(tmp_path / "*.csv"), max_rows=1_000, seed=0)
    assert stats == {"rows_read": 63, "rows_valid": 63, "rows_unique": 61, "rows_sampled": 61}
    assert full["label"].value_counts().to_dict() == {"benign": 41, "dos hulk": 20}

    half, stats = load_cic(str(tmp_path / "*.csv"), max_rows=30, seed=0)
    assert stats["rows_sampled"] == len(half) == 30
    # same fraction per label, so the attack share is preserved
    assert half["is_attack"].mean() == pytest.approx(20 / 61, abs=0.05)
