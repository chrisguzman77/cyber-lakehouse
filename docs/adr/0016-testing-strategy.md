# ADR-0016: Testing strategy — Delta-enabled pytest plus a Docker end-to-end check

- **Status:** Accepted
- **Date:** 2026-10-12

## Context

The pipeline's main correctness claim is idempotency, which depends on real
Delta behavior (MERGE semantics, Change Data Feed, stream checkpoints). Pure
functions can be tested with plain Spark, but MERGE cannot.

## Decision

Three layers of verification:

1. **pytest with Delta** — `delta-spark==3.3.1` is a dev dependency (pinned to
   the Docker image) and `tests/conftest.py` builds a Delta-enabled local
   SparkSession. The tests (33 as of ADR-0018) cover schema, attack-category normalization,
   cleaning, derived and frozen features, `first_per_key`, insert-only MERGE
   idempotency, CDF reads, the replay script, a mini streaming pipeline
   (bronze → silver → gold) run repeatedly with a re-delivered file, and the
   model-evaluation helpers (threshold choice, split, metrics, benchmarks),
   which need no Spark and so also run on hosts without Java.
2. **End-to-end in Docker** — `scripts/verify_incremental.sh --reset` runs the
   real jobs on small replay chunks and fails on any count mismatch: rerun,
   re-delivery, new delivery, train, full refresh, and rescoring.
3. **SQL quality gates** on the full dataset (`src/sql/quality_checks.sql`).

CI (GitHub Actions) runs ruff and pytest with Java 17 on every push.

## Alternatives considered

- **Unit tests without Delta** — would leave MERGE and CDF behavior
  untested in CI.
- **Mocking Delta** — tests the mock, not the semantics that matter.

## Consequences

- CI downloads the Delta jars on first run (slower); local Delta tests take
  ~1 min (`spark.databricks.delta.snapshotPartitions=1` halves that).
- Hosts without Java skip the Spark tests; they run in the container instead.
- The end-to-end script wipes local pipeline data, so it requires an explicit
  `--reset` flag and writes its model to `data/e2e/` so committed metrics are
  untouched.
