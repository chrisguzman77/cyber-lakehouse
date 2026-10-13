# ADR-0004: One shared code base for local Docker and Databricks

- **Status:** Accepted
- **Date:** 2026-08-19 (extended 2026-10-12)

## Context

The pipeline runs in two places: a Docker container on a laptop (fast, free
iteration) and Databricks serverless (cloud deployment, Unity Catalog,
Workflows). Copy-pasted logic in two places drifts.

## Decision

- Put all transformation logic in pure DataFrame-in/DataFrame-out functions
  under `src/utils/` (`schema`, `cleaning`, `features`, `incremental`,
  `replay`).
- Local jobs (`src/jobs/`) and Databricks notebooks (`databricks/`) are thin
  wiring around those functions; they differ only in I/O (paths vs. Unity
  Catalog names, file source vs. Auto Loader, local disk vs. Volumes).
- The repo is attached to Databricks as a Git folder; notebooks import
  `src/` via `sys.path.append(os.path.abspath(".."))`.
- Locally, everything runs in one Docker image with pinned PySpark/Delta and
  Java 17.

## Consequences

- A cleaning or feature change is made once and is covered by the unit tests.
- Path vs. table-name targets are handled by `path=` / `name=` keyword
  arguments on the shared helpers.
- On serverless, `foreachBatch` functions run in a separate Python process that
  may not see the Git folder; notebooks therefore register the shared modules
  with `cloudpickle.register_pickle_by_value`. Verified working on serverless
  (2026-10-12); a traceback from that run showed the worker also resolving
  `src.utils` from the Git folder path, so the registration is a safeguard
  rather than strictly required on the current runtime.
- Some platform differences remain in notebook code (e.g. `_metadata.file_path`
  instead of `input_file_name()`, which serverless restricts).
