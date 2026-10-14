"""Cross-dataset evaluation: map UNSW-NB15 (Argus/Bro) and CIC-IDS2017
(CICFlowMeter) flows onto one portable feature set, so a model trained on one
can be scored on the other.

Only 7 core measurements mean the same thing in both datasets; everything the
portable model sees is derived from them with identical formulas:

  core      UNSW-NB15 (gold)        CIC-IDS2017 (MachineLearningCVE CSVs)
  dur       dur (s)                 Flow Duration (µs) / 1e6
  sbytes    sbytes (incl. headers)  Total Length of Fwd Packets (payload) + Fwd Header Length
  dbytes    dbytes                  Total Length of Bwd Packets + Bwd Header Length
  spkts     spkts                   Total Fwd Packets
  dpkts     dpkts                   Total Backward Packets
  sintpkt   sintpkt (ms)            Fwd IAT Mean (µs) / 1000
  dintpkt   dintpkt (ms)            Bwd IAT Mean (µs) / 1000

Deliberately excluded: TTLs, protocol/service/state, and the ct_* connection
counters (no CIC equivalent), and TCP window sizes (UNSW stores a scaled value,
mostly 255; CIC stores raw bytes such as 29200, so the numbers don't
correspond).

Pure pandas/numpy/scikit-learn, so it is unit-tested without Spark.
"""

from __future__ import annotations

import glob
import re

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score

from src.utils.modeling import binary_metrics, choose_threshold

CORE_COLS = ["dur", "sbytes", "dbytes", "spkts", "dpkts", "sintpkt", "dintpkt"]

PORTABLE_FEATURES = [
    *CORE_COLS,
    "bytes_per_pkt_src",
    "bytes_per_pkt_dst",
    "byte_ratio_src_dst",
    "pkt_ratio_src_dst",
    "pkts_per_sec",
    "sload",
    "dload",
    "load_asymmetry",
    "log1p_sbytes",
    "log1p_dbytes",
    "log1p_dur",
    "dur_bucket",
]

# CIC column names as they appear in the CSV headers (note the leading spaces).
CIC_USECOLS = [
    " Flow Duration",
    " Total Fwd Packets",
    " Total Backward Packets",
    "Total Length of Fwd Packets",
    " Total Length of Bwd Packets",
    " Fwd Header Length",
    " Bwd Header Length",
    " Fwd IAT Mean",
    " Bwd IAT Mean",
    " Label",
]


def normalize_cic_label(label: str) -> str:
    """'Web Attack ï¿½ XSS' -> 'web attack - xss'. The CSVs carry a mojibake'd
    separator in the web-attack labels."""
    label = re.sub(r"[^\x00-\x7f]+", "-", label)
    return re.sub(r"\s+", " ", label).strip().lower()


def cic_to_core(df: pd.DataFrame) -> pd.DataFrame:
    """CIC rows -> CORE_COLS + label + is_attack, in UNSW units. Rows with a
    negative duration or any non-finite core value are dropped; negative
    header lengths (a known CICFlowMeter artifact) are clipped to 0."""
    out = pd.DataFrame(
        {
            "dur": df[" Flow Duration"] / 1e6,
            "sbytes": df["Total Length of Fwd Packets"] + df[" Fwd Header Length"].clip(lower=0),
            "dbytes": df[" Total Length of Bwd Packets"] + df[" Bwd Header Length"].clip(lower=0),
            "spkts": df[" Total Fwd Packets"],
            "dpkts": df[" Total Backward Packets"],
            "sintpkt": df[" Fwd IAT Mean"] / 1000,
            "dintpkt": df[" Bwd IAT Mean"] / 1000,
        }
    )
    valid = np.isfinite(out.to_numpy(dtype="float64")).all(axis=1) & (out["dur"] >= 0)
    out = out[valid].copy()
    out["label"] = df.loc[valid, " Label"].map(normalize_cic_label)
    out["is_attack"] = (out["label"] != "benign").astype(int)
    return out.reset_index(drop=True)


def load_cic(path_glob: str, max_rows: int, seed: int) -> tuple[pd.DataFrame, dict]:
    """Read the CIC CSVs (only the columns needed), map to core units, drop
    exact duplicates in core features + label, then take the same fraction of
    every label so class proportions are preserved. Returns (frame, counts)."""
    parts, rows_read = [], 0
    for path in sorted(glob.glob(path_glob)):
        raw = pd.read_csv(path, usecols=CIC_USECOLS, encoding="latin-1")
        rows_read += len(raw)
        parts.append(cic_to_core(raw))
    core = pd.concat(parts, ignore_index=True)
    rows_valid = len(core)
    core = core.drop_duplicates(subset=[*CORE_COLS, "label"]).reset_index(drop=True)
    rows_unique = len(core)
    fraction = min(1.0, max_rows / rows_unique)
    if fraction < 1.0:
        core = (
            core.groupby("label", group_keys=False)
            .sample(frac=fraction, random_state=seed)
            .reset_index(drop=True)
        )
    stats = {
        "rows_read": rows_read,
        "rows_valid": rows_valid,
        "rows_unique": rows_unique,
        "rows_sampled": len(core),
    }
    return core, stats


def portable_features(core: pd.DataFrame) -> pd.DataFrame:
    """Derived features from CORE_COLS, using the same formulas as the gold
    layer (src/utils/features.py) where they overlap."""
    c = core[CORE_COLS].astype("float64")
    dur = c["dur"]
    sload = np.where(dur > 0, 8 * c["sbytes"] / dur.where(dur > 0, 1), 0.0)
    dload = np.where(dur > 0, 8 * c["dbytes"] / dur.where(dur > 0, 1), 0.0)
    f = c.copy()
    f["bytes_per_pkt_src"] = c["sbytes"] / np.maximum(c["spkts"], 1)
    f["bytes_per_pkt_dst"] = c["dbytes"] / np.maximum(c["dpkts"], 1)
    f["byte_ratio_src_dst"] = c["sbytes"] / (c["dbytes"] + 1)
    f["pkt_ratio_src_dst"] = c["spkts"] / (c["dpkts"] + 1)
    f["pkts_per_sec"] = np.where(dur > 0, (c["spkts"] + c["dpkts"]) / dur.where(dur > 0, 1), 0.0)
    f["sload"] = sload
    f["dload"] = dload
    f["load_asymmetry"] = (sload - dload) / (sload + dload + 1)
    f["log1p_sbytes"] = np.log1p(c["sbytes"])
    f["log1p_dbytes"] = np.log1p(c["dbytes"])
    f["log1p_dur"] = np.log1p(dur)
    f["dur_bucket"] = np.select(
        [dur < 0.001, dur < 0.01, dur < 0.1, dur < 1.0], [0.0, 1.0, 2.0, 3.0], default=4.0
    )
    return f[PORTABLE_FEATURES]


def transfer_report(y_true, proba, labels, threshold: float) -> dict:
    """How a model behaves on a dataset it wasn't trained on, at the threshold
    chosen on its source dataset; plus the target-tuned ("oracle") threshold
    for reference, which a real deployment wouldn't have."""
    y_true, proba, labels = np.asarray(y_true), np.asarray(proba), np.asarray(labels)
    preds = proba >= threshold
    benign = y_true == 0
    by_label = {}
    for label in sorted(set(labels[~benign])):
        mask = labels == label
        detected = int(preds[mask].sum())
        by_label[label] = {
            "flows": int(mask.sum()),
            "detected": detected,
            "rate": detected / int(mask.sum()),
        }
    return {
        "at_source_threshold": binary_metrics(y_true, proba, threshold),
        "source_threshold": float(threshold),
        "false_positive_rate": float(preds[benign].mean()),
        "alert_rate": float(preds.mean()),
        "average_precision": float(average_precision_score(y_true, proba)),
        "detection_rate_by_label": by_label,
        "oracle_threshold_on_target": choose_threshold(y_true, proba),
    }
