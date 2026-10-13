from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from src.utils.cleaning import clean_traffic  # noqa: E402
from src.utils.schema import build_full_schema  # noqa: E402


def cleaned(spark, rows):
    return {
        r.srcip: r
        for r in clean_traffic(spark.createDataFrame(rows, build_full_schema())).collect()
    }


def test_attack_cat_normalization(spark, raw_row):
    out = cleaned(
        spark,
        [
            raw_row(srcip="a", attack_cat=None),
            raw_row(srcip="b", attack_cat="  "),
            raw_row(srcip="c", attack_cat=" Backdoors ", label=1),
            raw_row(srcip="d", attack_cat="Exploits", label=1),
        ],
    )
    assert out["a"].attack_cat_norm == "normal"
    assert out["b"].attack_cat_norm == "normal"
    assert out["c"].attack_cat_norm == "backdoor"
    assert out["d"].attack_cat_norm == "exploits"
    assert out["d"].is_attack == 1
    assert out["a"].is_attack == 0


def test_csv_quirks_fixed(spark, raw_row):
    out = cleaned(
        spark,
        [
            raw_row(srcip="a", ct_ftp_cmd=" ", ct_flw_http_mthd=None, is_ftp_login=None),
            raw_row(srcip="b", ct_ftp_cmd="3"),
        ],
    )
    assert out["a"].ct_ftp_cmd == 0
    assert out["a"].ct_flw_http_mthd == 0
    assert out["a"].is_ftp_login == 0
    assert out["b"].ct_ftp_cmd == 3


def test_corrupt_rows_filtered(spark, raw_row):
    out = cleaned(
        spark,
        [raw_row(srcip="ok"), raw_row(srcip="nolabel", label=None), raw_row(srcip="neg", dur=-1.0)],
    )
    assert set(out) == {"ok"}


def test_flow_hash_depends_only_on_identity(spark, raw_row):
    # sttl is not part of the flow identity; srcip is. No dedupe happens here.
    rows = clean_traffic(
        spark.createDataFrame(
            [raw_row(srcip="a", sttl=10), raw_row(srcip="a", sttl=99), raw_row(srcip="b")],
            build_full_schema(),
        )
    ).collect()
    by_ip: dict[str, set[str]] = {}
    for r in rows:
        by_ip.setdefault(r.srcip, set()).add(r.flow_hash)

    assert len(rows) == 3
    assert len(by_ip["a"]) == 1
    assert by_ip["a"] != by_ip["b"]
    assert len(next(iter(by_ip["a"]))) == 64
