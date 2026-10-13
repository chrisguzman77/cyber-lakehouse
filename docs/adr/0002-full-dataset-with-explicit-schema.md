# ADR-0002: Full UNSW-NB15 dataset with an explicit schema

- **Status:** Accepted
- **Date:** 2026-08-19

## Context

The first version processed the pre-split training/testing CSVs (~258k rows,
10 raw features). The full dataset is 4 headerless CSVs with 2,540,047 rows and
49 columns, ~600 MB. Several columns are dirty: ports contain hex strings
(`0x000b`) and `-`, `ct_ftp_cmd` contains blanks, and `attack_cat` has
inconsistent spellings and padding.

## Decision

- Ingest the **full** 4-file dataset, not the pre-split subsets.
- Read with an **explicit 49-column `StructType`** (`src/utils/schema.py`),
  never schema inference.
- Type dirty columns as strings in bronze (`sport`, `dsport`, `ct_ftp_cmd`)
  and fix them in silver.

## Alternatives considered

- **Schema inference** — a full extra pass over ~600 MB, and non-deterministic
  types for dirty columns (one bad value flips a column to string).
- **Pre-split train/test CSVs** — an order of magnitude smaller and not
  representative of a real data-engineering workload.

## Consequences

- Deterministic types on every run; no inference pass.
- Column names come from the dataset's feature list, since the files have no
  header; the schema file documents the quirks.
- Bronze stays a faithful raw copy; all type repair is visible in one place
  (`src/utils/cleaning.py`).
