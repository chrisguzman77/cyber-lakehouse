# ADR-0003: SHA-256 flow-identity key for deduplication

- **Status:** Accepted
- **Date:** 2026-08-19

## Context

The full dataset contains many repeated flow records (concentrated in attack
traffic, especially `generic`). Duplicates inflate the attack share (~13% raw
vs ~4% unique) and leak identical rows across train/test splits. There is no
natural primary key.

## Decision

Define a flow's identity as
`srcip, sport, dstip, dsport, proto, stime, ltime, sbytes, dbytes` and key every
silver/gold row by `flow_hash = sha2(concat_ws("||", <identity cols>), 256)`.
Keep exactly one row per `flow_hash`.

## Alternatives considered

- **Full-row distinct** — misses duplicates that differ only in derived
  counters or labels.
- **Concatenated string key** — long and awkward to join on; the hash is a
  fixed 64 characters.
- **No deduplication** — inflated, misleading metrics.

## Consequences

- Silver shrinks from 2,540,047 to 2,013,620 rows; the attack share drops to
  ~4%, which made honest metrics (F1, ROC-AUC) essential
  ([ADR-0006](0006-imbalance-handling-and-f1-model-selection.md)).
- `flow_hash` is the join and MERGE key for every downstream table
  ([ADR-0011](0011-insert-only-merge-first-seen-wins.md)).
- The `||` separator prevents accidental collisions between adjacent fields.
- If two records share an identity but differ in label, only one survives;
  which one is defined by the first-seen rule.
