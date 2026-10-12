# ADR-0010: Incremental ingestion with checkpointed streams (`availableNow`)

- **Status:** Accepted (verified locally; Databricks run pending)
- **Date:** 2026-10-12

## Context

Every job used to overwrite its whole table on each run. Real network logs
arrive continuously; reprocessing everything each time doesn't scale and
hides whether the pipeline handles late, repeated, or re-delivered data.

## Decision

- **Simulate arrival** with `src/utils/replay.py`: split raw CSVs into
  250k-row chunk files and release the next N into `landing/incoming/`.
  Chunks are written to `landing/_staging/` and moved with `os.replace`, so a
  stream never sees a half-written file.
- **Bronze** reads `landing/incoming/` as a stream — Spark's file source
  locally, **Auto Loader** (`cloudFiles`) on Databricks — with the explicit
  schema and a **checkpoint** recording which files were ingested.
- Every stream (01, 02, 03, 05) uses **`trigger(availableNow=True)`**: process
  everything available, then stop. Jobs behave like scheduled batch tasks.

## Alternatives considered

- **Continuous / processing-time triggers** — need an always-on cluster, and
  serverless supports only `availableNow`.
- **Manual high-water marks** (e.g. "max `ingest_ts` processed") — reinvents
  checkpointing and is fragile.
- **Keep full overwrites** — simplest, but the gap this change closes.

## Consequences

- Reruns with no new files are near no-ops (9–28 s per job on the full dataset).
- Checkpoints become state: they must be reset together with their tables,
  and changing a stream's query shape can require a reset.
- Bronze is raw and append-only: a file re-delivered under a new name is
  ingested again; deduplication is silver's job
  ([ADR-0011](0011-insert-only-merge-first-seen-wins.md)).
- Rename atomicity on FUSE-mounted UC Volumes is unverified.
