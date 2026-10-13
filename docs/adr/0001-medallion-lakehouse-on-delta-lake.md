# ADR-0001: Medallion lakehouse on Delta Lake

- **Status:** Accepted
- **Date:** 2026-01-30

## Context

The pipeline turns raw, messy CSV flow records into model-ready features and
predictions. Each stage needs to be inspectable on its own, re-runnable, and
queryable with SQL, and the same storage format must work locally and on
Databricks.

## Decision

Organize data in three layers stored as Delta Lake tables:

- **Bronze** — raw records as delivered, plus ingest metadata. Never modified.
- **Silver** — cleaned, normalized, deduplicated flows.
- **Gold** — ML feature table, analytics aggregates, and predictions.

## Alternatives considered

- **Plain Parquet files** — no ACID commits, no MERGE, no Change Data Feed, no
  streaming source semantics; partial writes would be visible.
- **A relational database** — doesn't scale to the dataset the way Spark does
  and diverges from the Databricks target.
- **A single wide table** — no raw history to reprocess from, and cleaning
  bugs would be unrecoverable.

## Consequences

- Any layer can be rebuilt from the one below it; bronze preserves raw history.
- Delta provides ACID writes, schema enforcement, MERGE, time travel, and
  Change Data Feed, which later enabled incremental processing
  ([ADR-0010](0010-incremental-streaming-ingestion.md),
  [ADR-0012](0012-change-data-feed-between-layers.md)).
- Storage roughly triples (bronze ≈ 196 MB, silver ≈ 316 MB, gold ≈ 345 MB
  locally) — acceptable at this scale.
