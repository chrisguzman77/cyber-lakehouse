#!/usr/bin/env bash
# Run a Spark job inside the cyber-spark container with Delta Lake configured.
# Usage: scripts/run_job.sh src/jobs/01_ingest_bronze.py [job args...]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$REPO_ROOT/docker/docker-compose.yml"

if ! docker compose -f "$COMPOSE" ps --status running --services 2>/dev/null | grep -q '^spark$'; then
  echo "[INFO] spark service not running. Starting..."
  docker compose -f "$COMPOSE" up -d --build
fi

docker compose -f "$COMPOSE" exec -T spark \
  spark-submit \
  --packages io.delta:delta-spark_2.12:3.3.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.jars.ivy=/root/.ivy2 \
  --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
  "$@"
