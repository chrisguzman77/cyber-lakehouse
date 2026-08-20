from __future__ import annotations

from dataclasses import dataclass

# Full UNSW-NB15 dataset schema (UNSW-NB15_1.csv .. UNSW-NB15_4.csv).
# The CSVs have NO header row; column order comes from NUSW-NB15_features.csv.
# Quirks handled at the type level:
#   - sport/dsport contain hex values (e.g. "0x000b") and "-" -> read as string
#   - ct_ftp_cmd contains blanks -> read as string, cast in silver
#   - attack_cat is null/empty for benign flows and has variants ("Backdoors",
#     padded whitespace) -> normalized in silver via normalize_attack_cat_value
FULL_SCHEMA_FIELDS: list[tuple[str, str]] = [
    ("srcip", "string"),
    ("sport", "string"),
    ("dstip", "string"),
    ("dsport", "string"),
    ("proto", "string"),
    ("state", "string"),
    ("dur", "double"),
    ("sbytes", "long"),
    ("dbytes", "long"),
    ("sttl", "long"),
    ("dttl", "long"),
    ("sloss", "long"),
    ("dloss", "long"),
    ("service", "string"),
    ("sload", "double"),
    ("dload", "double"),
    ("spkts", "long"),
    ("dpkts", "long"),
    ("swin", "long"),
    ("dwin", "long"),
    ("stcpb", "long"),
    ("dtcpb", "long"),
    ("smeansz", "long"),
    ("dmeansz", "long"),
    ("trans_depth", "long"),
    ("res_bdy_len", "long"),
    ("sjit", "double"),
    ("djit", "double"),
    ("stime", "long"),
    ("ltime", "long"),
    ("sintpkt", "double"),
    ("dintpkt", "double"),
    ("tcprtt", "double"),
    ("synack", "double"),
    ("ackdat", "double"),
    ("is_sm_ips_ports", "long"),
    ("ct_state_ttl", "long"),
    ("ct_flw_http_mthd", "long"),
    ("is_ftp_login", "long"),
    ("ct_ftp_cmd", "string"),
    ("ct_srv_src", "long"),
    ("ct_srv_dst", "long"),
    ("ct_dst_ltm", "long"),
    ("ct_src_ltm", "long"),
    ("ct_src_dport_ltm", "long"),
    ("ct_dst_sport_ltm", "long"),
    ("ct_dst_src_ltm", "long"),
    ("attack_cat", "string"),
    ("label", "long"),
]


def build_full_schema():
    """Return the full-dataset StructType. pyspark imported lazily so pure-python
    tests don't need it."""
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    type_map = {"string": StringType(), "double": DoubleType(), "long": LongType()}
    return StructType(
        [StructField(name, type_map[t], nullable=True) for name, t in FULL_SCHEMA_FIELDS]
    )


# The 10 canonical attack categories after normalization (null -> normal).
CANONICAL_ATTACK_CATS = [
    "normal",
    "generic",
    "exploits",
    "fuzzers",
    "dos",
    "reconnaissance",
    "analysis",
    "backdoor",
    "shellcode",
    "worms",
]

# Raw-value fixes beyond trim/lower (dataset uses both "Backdoor" and "Backdoors").
_ATTACK_CAT_FIXES = {"backdoors": "backdoor"}


def normalize_attack_cat_value(value: str | None) -> str:
    """Normalize a raw attack_cat value: null/empty -> "normal", else trim+lower
    with known variant fixes."""
    if value is None or not value.strip():
        return "normal"
    v = value.strip().lower()
    return _ATTACK_CAT_FIXES.get(v, v)


# Key fields every silver row must carry.
REQUIRED_COLUMNS = [
    "dur",
    "spkts",
    "dpkts",
    "sbytes",
    "dbytes",
    "sttl",
    "dttl",
    "sload",
    "dload",
    "label",
]

LABEL_COLUMNS = ["label", "attack_cat"]


@dataclass(frozen=True)
class QualityRule:
    name: str
    description: str


QUALITY_RULES = [
    QualityRule("non_negative_dur", "dur must be >= 0"),
    QualityRule("non_null_label", "label must not be null"),
]
