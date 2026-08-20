#!/usr/bin/env bash
# Download the full UNSW-NB15 dataset (~2.54M flow records, 4 CSVs, ~600MB)
# into data/raw/ via the Kaggle CLI.
#
# Requires: `kaggle` CLI (`uv tool install kaggle` or `pip install kaggle`)
# and an API token at ~/.kaggle/kaggle.json (https://www.kaggle.com/settings).
#
# Manual fallback: download "UNSW-NB15_{1..4}.csv" from the UNSW Canberra
# research portal (https://research.unsw.edu.au/projects/unsw-nb15-dataset)
# and place them in data/raw/ yourself.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAW_DIR="$REPO_ROOT/data/raw"
mkdir -p "$RAW_DIR"

kaggle datasets download -d mrwellsdavid/unsw-nb15 -p "$RAW_DIR" --unzip

# Keep the 4 full-dataset CSVs and the feature dictionary; drop everything else
# the mirror ships (train/test splits, ground-truth xlsx, etc.).
find "$RAW_DIR" -maxdepth 1 -type f \
  ! -name 'UNSW-NB15_[1-4].csv' \
  ! -name 'NUSW-NB15_features.csv' \
  ! -name 'UNSW_NB15_training-set*.csv' \
  ! -name 'UNSW_NB15_testing-set*.csv' \
  -delete

echo "Row counts (incl. no header rows):"
wc -l "$RAW_DIR"/UNSW-NB15_[1-4].csv
