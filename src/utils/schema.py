from __future__ import annotations

from dataclasses import dataclass

# Minimal column set commonly preset in UNSW-NB15 CSV variants.
# If CSV has more columns, we keep them; this is just for key fields.
REQUIRED_COLUMNS = [
    "dur",
    "spkts",
    "dpkts",
    "sbytes",
    "dbytes",
    "rate",
    "sttl",
    "sload",
    "dload",
    "label",  # often 0/1
]

LABEL_COLUMNS = ["label", "attack_cat"]  # some files include attack category


@dataclass(frozen=True)
class QualityRule:
    name: str
    description: str


QUALITY_RULES = [
    QualityRule("non_negative_dur", "dur must be >= 0"),
    QualityRule("non_null_label", "label must not be null"),
]
