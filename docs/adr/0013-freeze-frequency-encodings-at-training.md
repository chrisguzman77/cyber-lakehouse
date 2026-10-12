# ADR-0013: Freeze frequency encodings at training time

- **Status:** Accepted (verified locally and on Databricks, 2026-10-12)
- **Date:** 2026-10-12

## Context

`proto`, `service`, and `state` are frequency-encoded (`value → share of rows
with that value`). The old `add_frequency_encodings` computed the shares over
whatever DataFrame it was given. In an incremental pipeline that DataFrame is
a micro-batch, so the same protocol would get a different encoding in every
batch, and a different one again from what the model was trained on — a
**train/serve skew** bug.

## Decision

- `compute_frequency_encodings(df)` returns the map as rows of
  (`column`, `value`, `freq`). Training (job 04) computes it over the full gold
  table and persists it to `gold/freq_encodings`.
- `apply_frequency_encodings(df, encodings)` broadcast-joins the map; values
  absent from the map (unseen at training time) encode as **0.0**.
- Training and scoring both call `apply_frequency_encodings` with the same
  persisted table. Gold keeps the raw category strings, so every gold feature
  stays row-wise.

## Alternatives considered

- **Recompute per batch** — the skew bug itself.
- **Recompute over all of gold at scoring time** — encodings drift as data
  grows while the model stays fixed.
- **One-hot encoding** — high cardinality in `service`/`state`, and unseen
  values still need a policy.
- **Embed the encoder in an sklearn Pipeline** — self-contained model, but
  then executors must import the custom transformer class (fragile on
  serverless).

## Consequences

- Encodings change only when the model is retrained; train and serve match.
- The encodings table is a model artifact that must stay in sync with the
  model: retrain → full rescore ([ADR-0014](0014-separate-pipeline-and-training-workflows.md)).
- Values over the full gold table are identical to the old global computation,
  so model inputs did not change meaning.
- Unit test: encoding a batch uses the training share, not the batch share,
  and unseen values get 0.0.
