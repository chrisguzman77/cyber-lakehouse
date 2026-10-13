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
- Three pages, 14 SQL datasets, 23 widgets:
  - **Threat overview:** flows, alerts, alert rate, targeted hosts and
    sources; alerts over time by family; alerts by family; most-targeted
    hosts; the 100 highest-confidence alerts with their 5-tuple.
  - **Model quality:** the selected threshold, test precision and recall;
    every binary and family candidate from `gold.model_metrics` /
    `gold.family_model_metrics`.
  - **Pipeline health:** latest data-quality results, pass/fail per run,
    raw rows vs. new unique flows per micro-batch, rows per layer.
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
- The overview's time axis spans the dataset's two capture sessions (Jan 22
  and Feb 18, 2015), so it shows two clusters with a gap, which is noted on
  the widget.
- The file was written without access to the dashboard editor; if the import
  rejects a widget, the error names it and the fix is local to that widget.
