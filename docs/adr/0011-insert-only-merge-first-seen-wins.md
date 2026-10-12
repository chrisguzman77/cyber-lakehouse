# ADR-0011: Insert-only MERGE on `flow_hash` — first seen wins

- **Status:** Accepted (verified locally; Databricks run pending)
- **Date:** 2026-10-12

## Context

With incremental processing, a flow already in silver can arrive again (a
re-delivered file, a replayed batch after a failure, a duplicate record in a
later file). Previously, a window over the whole table kept the copy with the
latest `ingest_ts`.

## Decision

Write silver, gold features, and predictions only through `insert_new_keys`
(`src/utils/incremental.py`): a Delta MERGE that **inserts keys the target
doesn't hold and never updates** (`whenNotMatchedInsertAll`). Before the MERGE,
`first_per_key` deduplicates each micro-batch by `flow_hash`, keeping the
earliest `ingest_ts`, then `source_file`. Tables are created on first write
with Change Data Feed enabled (`ensure_delta_table`).

## Alternatives considered

- **Latest wins** (`whenMatchedUpdateAll` when newer) — re-deliveries cause
  update writes that ripple through gold and predictions for no new
  information.
- **Append + periodic dedupe** — downstream sees duplicates in between.
- **Overwrite each run** — not incremental.

## Consequences

- Re-delivery causes **zero writes** below bronze; every layer below bronze is
  append-only, which keeps Change Data Feed simple (inserts only).
- In-batch deduplication is mandatory: an insert-only MERGE would insert two
  copies of a key that appears twice in one batch.
- Row counts are unchanged versus the old rule (same key); which duplicate is
  kept can differ. Together with a different stratified sample, this moved
  RandomForest F1 from 0.8652 to 0.8646.
- Verified: rerun → counts unchanged; re-delivered chunk → bronze +50k, silver
  and gold unchanged; a full-data run has 0 duplicate keys in every layer.
