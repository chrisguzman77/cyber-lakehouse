from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = REPO_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
LANDING_DIR = DATA_DIR / "landing"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
CHECKPOINTS_DIR = DATA_DIR / "checkpoints"

MODELS_DIR = REPO_ROOT / "models"
REPORTS_DIR = REPO_ROOT / "reports"


def model_artifacts(model_path: str | Path) -> dict[str, Path]:
    """Training outputs live together: the binary model at model_path, plus the
    attack-family model and model_meta.json (threshold etc.) beside it."""
    binary = Path(model_path)
    return {
        "binary": binary,
        "family": binary.with_name("attack_family_model.pkl"),
        "meta": binary.with_name("model_meta.json"),
    }


def ensure_dirs() -> None:
    for p in [
        RAW_DIR,
        LANDING_DIR,
        BRONZE_DIR,
        SILVER_DIR,
        GOLD_DIR,
        CHECKPOINTS_DIR,
        MODELS_DIR,
        REPORTS_DIR,
    ]:
        p.mkdir(parents=True, exist_ok=True)
