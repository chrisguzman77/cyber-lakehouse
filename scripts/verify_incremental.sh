#!/usr/bin/env bash
# End-to-end idempotency check for the incremental pipeline, run in Docker on
# small replay chunks: reruns and re-delivered files must not add rows below
# bronze, new files must flow through, and every gold flow is scored once.
#
# WIPES local pipeline data: data/{landing,bronze,silver,gold,checkpoints,e2e}.
# The e2e model/metrics go to data/e2e/, so models/ and reports/ are untouched.
# Usage: scripts/verify_incremental.sh --reset
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ "${1:-}" != "--reset" ]]; then
  echo "Pass --reset to confirm wiping data/{landing,bronze,silver,gold,checkpoints,e2e}"
  exit 2
fi

CHUNK_ROWS=50000
MODEL=data/e2e/model.pkl

replay() { uv run python -m src.utils.replay --chunk_rows "$CHUNK_ROWS" --files "$1"; }
job() { ./scripts/run_job.sh "src/jobs/$1.py" "${@:2}" >/dev/null 2>&1; }
pipeline() { job 01_ingest_bronze && job 02_clean_silver && job 03_build_gold_features; }
counts() { ./scripts/run_job.sh src/jobs/table_counts.py 2>/dev/null | sed -n 's/^COUNTS //p'; }
field() { uv run python -c 'import json,sys; print(json.loads(sys.argv[1])[sys.argv[2]])' "$1" "$2"; }
expect() {
  if [[ "$2" != "$3" ]]; then
    echo "[FAIL] $1: got $2, expected $3"
    exit 1
  fi
  echo "[PASS] $1 ($2)"
}

rm -rf data/landing data/bronze data/silver data/gold data/checkpoints data/e2e
mkdir -p data/e2e

echo "== 1) First delivery: 2 chunks"
replay 2
pipeline
c1=$(counts)
echo "$c1"
b1=$(field "$c1" bronze)
s1=$(field "$c1" silver)
expect "bronze holds both chunks" "$b1" $((2 * CHUNK_ROWS))
expect "silver has no duplicate keys" "$(field "$c1" silver_dup_keys)" 0
expect "gold matches silver" "$(field "$c1" gold)" "$s1"

echo "== 2) Rerun with no new files"
pipeline
expect "counts unchanged on rerun" "$(counts)" "$c1"

echo "== 3) Re-deliver a chunk under a new name"
cp data/landing/incoming/UNSW-NB15_1.part-0000.csv \
  data/landing/incoming/UNSW-NB15_1.part-0000-redelivered.csv
pipeline
c3=$(counts)
expect "bronze keeps the raw copy" "$(field "$c3" bronze)" $((b1 + CHUNK_ROWS))
expect "silver unchanged" "$(field "$c3" silver)" "$s1"
expect "gold unchanged" "$(field "$c3" gold)" "$s1"

echo "== 4) New delivery flows through"
replay 1
pipeline
c4=$(counts)
s4=$(field "$c4" silver)
[[ "$s4" -gt "$s1" ]] || { echo "[FAIL] silver did not grow: $s4 <= $s1"; exit 1; }
echo "[PASS] silver grew ($s1 -> $s4)"
expect "silver has no duplicate keys" "$(field "$c4" silver_dup_keys)" 0
expect "gold matches silver" "$(field "$c4" gold)" "$s4"

echo "== 5) Train, full-refresh score, then incremental rescore is a no-op"
job 04_train_model --model_out "$MODEL" --metrics_out data/e2e/metrics.json
job 05_score_predictions --model_path "$MODEL" --full_refresh
c5=$(counts)
expect "every gold flow scored" "$(field "$c5" predictions)" "$s4"
expect "no duplicate predictions" "$(field "$c5" predictions_dup_keys)" 0
job 05_score_predictions --model_path "$MODEL"
expect "rescore adds nothing" "$(counts)" "$c5"

echo "[OK] Incremental pipeline is idempotent"
