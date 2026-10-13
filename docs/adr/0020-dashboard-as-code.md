# ADR-0020: AI/BI dashboard kept as code, model quality from the test split only

- **Status:** Accepted (import into Databricks pending)
- **Date:** 2026-10-13

## Context

The pipeline's outputs (alerts, attack families, model quality, data quality,
incremental ingestion) were visible only through SQL queries and README
screenshots. A dashboard makes them visible at a glance to analysts and
reviewers alike. It has to read the Unity Catalog tables the pipeline already
writes, stay reproducible, and not overstate model quality.

## Decision

- A **Databricks AI/BI dashboard** stored in the repo as
  `databricks/dashboards/cyber_lakehouse.lvdash.json`. Inside the Git folder
  it appears as a dashboard; it can also be imported from file.
- Three pages, 16 SQL datasets, 27 widgets:
  - **Threat overview:** flows, alerts, alert rate, targeted hosts and
    sources; alerts per hour by family, one chart per capture session (a
    single time axis would be a month of empty space); alerts by family; most-targeted
    hosts; the 100 highest-confidence alerts with their 5-tuple.
  - **Model quality:** the selected threshold, test precision and recall;
    every binary and family candidate from `gold.model_metrics` /
    `gold.family_model_metrics`.
  - **Pipeline health:** latest data-quality results; recent runs (passed /
    failed / missing); raw rows vs. new unique flows per micro-batch, with
    totals and the share removed; rows per layer.
- **Model quality comes only from the saved test-split metrics.** The
  dashboard does not compare predictions with true labels across all flows,
  because that population includes the training sample and would overstate
  quality ([ADR-0017](0017-validation-tuned-alert-threshold.md)).
- Every dataset query was validated against real Delta tables locally (UC
  names swapped for paths) before committing.

## Alternatives considered

- **Build in the UI only** — not reviewable or reproducible.
- **Streamlit app** — a separate service to host; duplicates what the
  platform provides.
- **"Alerts vs. actual labels" over all flows** — more visual, but
  optimistic in a way a reviewer would rightly question.

## Consequences

- The dashboard is versioned with the code that produces its tables. Edits
  made in the UI should be exported back over the `.lvdash.json` file.
- It depends on tables from both Workflows: the model page needs a training
  run, the health page a run of `06_quality_checks`.
- The first import (2026-10-13) worked but needed polish: headings ran into
  their text, one time axis spanning both capture sessions was mostly empty,
  numbers right-aligned under left-aligned headers and ROC-AUC 0.9988 rounded
  to 1.00, and thin per-run charts were hard to read. Revised to per-session
  charts, left-aligned 3-decimal model tables, and tables for run history and
  micro-batches.
- The file was written without access to the dashboard editor; if the import
  rejects a widget, the error names it and the fix is local to that widget.
