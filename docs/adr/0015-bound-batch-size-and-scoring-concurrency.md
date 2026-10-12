# ADR-0015: Bound micro-batch size and scoring concurrency

- **Status:** Accepted
- **Date:** 2026-10-12

## Context

The development machine has 8 GB RAM; the Docker VM has ~3.8 GB and the Spark
driver 2 GB (6 GB is OOM-killed). The first full-data incremental run showed:

- The Delta streaming source defaults to up to 1,000 files per micro-batch,
  so silver read all of bronze, and gold all of silver, in **one** batch:
  silver took 410 s and gold 1,128 s.
- Full-refresh scoring was OOM-killed (exit 137). Measured: 8 concurrent
  Python workers (one per core) at ~230 MB each (sklearn plus the
  RandomForest) peaked at ~1.85 GB, on top of a JVM swinging up to ~2 GB.

## Decision

- Delta stream readers in jobs 02, 03, and 05 set
  `maxBytesPerTrigger` (default **128 MB**, `--max_bytes_per_trigger`). A byte
  cap adapts to each layer's file sizes (48 small bronze files vs. 8 × 45 MB
  gold files), unlike a file-count cap.
- Bronze keeps `maxFilesPerTrigger=2` on landing files.
- Scoring coalesces each batch to **4 partitions** before `mapInPandas`
  (`--score_partitions`), capping concurrent Python workers at 4.

## Alternatives considered

- **More driver memory** — the VM can't give it.
- **File-count cap on Delta readers** — 8 large gold files would still be one
  batch.
- **Load the model once per worker process** — reduces churn but not the
  per-worker baseline.

## Consequences

| Step | Before | After |
|---|---|---|
| 02 silver (first run) | 410 s, 1 batch | 153 s, 2 batches |
| 03 gold (first run) | 1,128 s, 1 batch | 135 s, 3 batches |
| 05 full refresh | OOM (exit 137) | 136 s, peak 2.7 GB (JVM 1.7 + Python 1.0) |

- Databricks notebooks don't apply these caps: serverless has ample memory,
  and smaller batches would only add MERGE overhead there.
- If memory errors return, these are the first knobs to turn
  (see [`operations.md`](../operations.md)).
