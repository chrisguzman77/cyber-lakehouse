# ADR-0012: Change Data Feed to propagate increments between layers

- **Status:** Accepted (verified locally; Databricks run pending)
- **Date:** 2026-10-12

## Context

Gold and predictions must process only the rows their upstream table newly
inserted. Silver and gold are written by MERGE, and a plain Delta streaming
source is designed for append-only tables.

## Decision

Enable **Change Data Feed** (`delta.enableChangeDataFeed=true`) on silver,
gold features, and predictions at creation. Jobs 03 and 05 read the upstream
table with `readStream ... option("readChangeFeed", "true")` and keep only
`_change_type = 'insert'` rows (`cdf_inserts`), dropping the CDF metadata
columns. With no checkpoint, a CDF stream starts from a full snapshot, which
is how `--full_refresh` rescoring works.

## Alternatives considered

- **Plain Delta streaming source** — works for append-only tables but breaks
  if a MERGE ever rewrites files; CDF states intent explicitly.
- **Timestamp watermark queries** — manual state, and `ingest_ts` is not
  strictly monotonic across re-deliveries.
- **Recompute gold from all of silver** — not incremental.

## Consequences

- Each layer touches only new rows; downstream work is proportional to new
  data.
- Tables created by the old batch pipeline have no CDF, so upgrading requires
  a reset.
- Because writes are insert-only ([ADR-0011](0011-insert-only-merge-first-seen-wins.md)),
  update/delete change types never occur; filtering on `insert` is complete.
