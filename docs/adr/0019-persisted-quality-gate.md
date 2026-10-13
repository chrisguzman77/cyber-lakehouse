# ADR-0019: Data-quality checks as a persisted pipeline gate

- **Status:** Accepted
- **Date:** 2026-10-13

## Context

The checks lived in `src/sql/quality_checks.sql`, run by hand with
`spark-sql`. Nothing recorded the results, so there was no history, nothing
stopped a run when a check failed, and a dashboard had nothing to read. The
SQL file also hard-coded local Delta paths, so it couldn't run on Databricks
as written.

## Decision

- Define the checks once, as data, in `src/utils/quality.py`: each is a SQL
  query returning a violation count, with `{silver}` / `{gold}` /
  `{predictions}` placeholders filled with local paths (`` delta.`/path` ``) or
  Unity Catalog names.
- `06_quality_checks` (local job and Databricks notebook) runs them after
  scoring and **appends** one row per check per run to `quality_results`
  (`run_ts`, `check_name`, `layer`, `description`, `violations`, `status`).
- A check needing a table that doesn't exist yet (predictions before the first
  training run) is recorded as `missing`, not failed.
- After saving, the job **fails if any check failed**, so a bad run turns the
  Workflow red instead of silently feeding the dashboard.
- Replace `src/sql/quality_checks.sql`; ad-hoc analytics stay in
  `src/sql/analytics.sql`.

## Alternatives considered

- **Dashboard runs the checks live** — no history, and the checks would exist
  in two places.
- **Delta Live Tables / Lakeflow expectations** — the idiomatic Databricks
  feature, but it requires rewriting the pipeline as a declarative pipeline
  and doesn't run locally.
- **Great Expectations** — a dependency and configuration surface out of
  proportion to 11 checks.

## Consequences

- Quality has a history: the dashboard shows the latest status and the
  pass/fail trend per run.
- The pipeline Workflow gains a sixth task; a failure is visible in the
  Workflow UI.
- The two previously "informational" summaries became checks: key-feature
  null rates (now `key_feature_nulls`, must be 0) and the attack-share band
  (now `attack_share_out_of_band`, 0/1). Row counts per layer moved to the
  dashboard.
- Checks are unit-tested against clean, dirty, and missing-table fixtures
  (`tests/test_quality.py`).
- Locally the 11 small queries take a few minutes because of Delta and Spark
  startup costs per query; acceptable for a gate that runs once per pipeline
  run.
