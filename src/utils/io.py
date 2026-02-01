from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = REPO_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
CHECKPOINTS_DIR = DATA_DIR / "checkpoints"

MODELS_DIR = REPO_ROOT / "models"
REPORTS_DIR = REPO_ROOT / "reports"


def ensure_dirs() -> None:
    for p in [
        RAW_DIR,
        BRONZE_DIR,
        SILVER_DIR,
        GOLD_DIR,
        CHECKPOINTS_DIR,
        MODELS_DIR,
        REPORTS_DIR,
    ]:
        p.mkdir(parents=True, exist_ok=True)
