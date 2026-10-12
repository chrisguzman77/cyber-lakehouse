# ADR-0008: `try_cast` for dirty string columns under ANSI mode

- **Status:** Accepted
- **Date:** 2026-08-24

## Context

`ct_ftp_cmd` arrives as a string containing blanks. Locally (Spark 3.5,
ANSI off) `cast("")` returns NULL. Databricks serverless enables **ANSI mode
by default**, where the same cast throws `CAST_INVALID_INPUT` and the silver
job fails.

## Decision

Cast dirty string columns with `try_cast`, which returns NULL on invalid input
under any ANSI setting, then coalesce to 0:
`coalesce(try_cast(trim(ct_ftp_cmd) AS BIGINT), 0)`.

## Alternatives considered

- **Disable ANSI mode** — hides other real data errors and isn't a stable
  setting on serverless.
- **Regex-filter before casting** — more code for the same result.

## Consequences

- Identical behavior locally and on Databricks.
- Rule of thumb for this codebase: never use bare `.cast()` on columns that
  may contain dirty strings.
