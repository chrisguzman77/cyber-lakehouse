#!/usr/bin/env bash
# Download CIC-IDS2017 (the 8 "MachineLearningCVE" CSVs, ~2.83M flows, ~885MB)
# into data/raw/cic/ for the cross-dataset evaluation
# (src/jobs/evaluate_cross_dataset.py). Public Kaggle mirror, fetched
# anonymously with kagglehub (no Kaggle account or token needed).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$REPO_ROOT/data/raw/cic"
mkdir -p "$OUT_DIR"

uv run --with kagglehub python - "$OUT_DIR" <<'EOF'
import glob, shutil, sys
import kagglehub

out = sys.argv[1]
src = kagglehub.dataset_download("chethuhn/network-intrusion-dataset")
files = sorted(glob.glob(f"{src}/*.csv"))
for f in files:
    shutil.copy(f, out)
print(f"Copied {len(files)} CSVs to {out}")
EOF
