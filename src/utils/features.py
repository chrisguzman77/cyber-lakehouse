"""Feature engineering for the gold layer.

Pure DataFrame-in/DataFrame-out functions so the same logic runs in the local
Spark jobs and the Databricks notebooks, and unit tests can exercise them with
a tiny local SparkSession.

Derived features (all double):
  bytes_per_pkt_src   sbytes / max(spkts, 1)
  bytes_per_pkt_dst   dbytes / max(dpkts, 1)
  byte_ratio_src_dst  sbytes / (dbytes + 1)
  pkt_ratio_src_dst   spkts / (dpkts + 1)
  load_asymmetry      (sload - dload) / (sload + dload + 1)
  ttl_delta           sttl - dttl
  loss_rate_src       sloss / max(spkts, 1)
  loss_rate_dst       dloss / max(dpkts, 1)
  pkts_per_sec        (spkts + dpkts) / dur when dur > 0 else 0
  log1p_sbytes        log(1 + sbytes)
  log1p_dbytes        log(1 + dbytes)
  log1p_dur           log(1 + dur)
  dur_bucket          fixed thresholds: <1ms=0, <10ms=1, <100ms=2, <1s=3, else 4

Frequency encodings: for each categorical column c, c_freq = count(c) / total
rows — the share of the dataset carrying that category value.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Raw numeric columns from the full UNSW-NB15 schema used as model features.
RAW_FEATURE_COLS = [
    "dur",
    "sbytes",
    "dbytes",
    "sttl",
    "dttl",
    "sloss",
    "dloss",
    "sload",
    "dload",
    "spkts",
    "dpkts",
    "swin",
    "dwin",
    "stcpb",
    "dtcpb",
    "smeansz",
    "dmeansz",
    "trans_depth",
    "res_bdy_len",
    "sjit",
    "djit",
    "sintpkt",
    "dintpkt",
    "tcprtt",
    "synack",
    "ackdat",
    "is_sm_ips_ports",
    "ct_state_ttl",
    "ct_flw_http_mthd",
    "is_ftp_login",
    "ct_ftp_cmd",
    "ct_srv_src",
    "ct_srv_dst",
    "ct_dst_ltm",
    "ct_src_ltm",
    "ct_src_dport_ltm",
    "ct_dst_sport_ltm",
    "ct_dst_src_ltm",
]

DERIVED_FEATURE_COLS = [
    "bytes_per_pkt_src",
    "bytes_per_pkt_dst",
    "byte_ratio_src_dst",
    "pkt_ratio_src_dst",
    "load_asymmetry",
    "ttl_delta",
    "loss_rate_src",
    "loss_rate_dst",
    "pkts_per_sec",
    "log1p_sbytes",
    "log1p_dbytes",
    "log1p_dur",
    "dur_bucket",
]

FREQ_ENCODED_COLS = ("proto", "service", "state")


def add_derived_features(df: DataFrame) -> DataFrame:
    """Add the derived feature columns documented in the module docstring."""
    d = F.col("dur").cast("double")
    return (
        df.withColumn(
            "bytes_per_pkt_src",
            (F.col("sbytes") / F.greatest(F.col("spkts"), F.lit(1))).cast("double"),
        )
        .withColumn(
            "bytes_per_pkt_dst",
            (F.col("dbytes") / F.greatest(F.col("dpkts"), F.lit(1))).cast("double"),
        )
        .withColumn("byte_ratio_src_dst", (F.col("sbytes") / (F.col("dbytes") + 1)).cast("double"))
        .withColumn("pkt_ratio_src_dst", (F.col("spkts") / (F.col("dpkts") + 1)).cast("double"))
        .withColumn(
            "load_asymmetry",
            ((F.col("sload") - F.col("dload")) / (F.col("sload") + F.col("dload") + 1)).cast(
                "double"
            ),
        )
        .withColumn("ttl_delta", (F.col("sttl") - F.col("dttl")).cast("double"))
        .withColumn(
            "loss_rate_src", (F.col("sloss") / F.greatest(F.col("spkts"), F.lit(1))).cast("double")
        )
        .withColumn(
            "loss_rate_dst", (F.col("dloss") / F.greatest(F.col("dpkts"), F.lit(1))).cast("double")
        )
        .withColumn(
            "pkts_per_sec",
            F.when(d > 0, (F.col("spkts") + F.col("dpkts")) / d)
            .otherwise(F.lit(0.0))
            .cast("double"),
        )
        .withColumn("log1p_sbytes", F.log1p(F.col("sbytes")).cast("double"))
        .withColumn("log1p_dbytes", F.log1p(F.col("dbytes")).cast("double"))
        .withColumn("log1p_dur", F.log1p(d).cast("double"))
        .withColumn(
            "dur_bucket",
            F.when(d < 0.001, 0)
            .when(d < 0.01, 1)
            .when(d < 0.1, 2)
            .when(d < 1.0, 3)
            .otherwise(4)
            .cast("double"),
        )
    )


def add_frequency_encodings(df: DataFrame, cols: tuple[str, ...] = FREQ_ENCODED_COLS) -> DataFrame:
    """Add <col>_freq for each categorical column: share of rows with that value."""
    total = df.count()
    for c in cols:
        freqs = df.groupBy(c).agg((F.count("*") / F.lit(total)).cast("double").alias(f"{c}_freq"))
        df = df.join(freqs, on=c, how="left")
    return df
