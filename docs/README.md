# Cyber Lakehouse Documentation

*Last updated: 2026-10-14*

| Document | What it covers |
|---|---|
| [Architecture](architecture.md) | Purpose, system context, data flow, components, local and Databricks deployments, technology stack, cross-cutting concerns (idempotency, data quality, train/serve consistency), results, limitations, roadmap |
| [Data model](data-model.md) | Every table: location, writer, write pattern, key, columns, cleaning rules, feature definitions |
| [Operations runbook](operations.md) | Setup, running the incremental and training cycles, job parameters, verification, resetting, troubleshooting |
| [Architecture Decision Records](adr/README.md) | 21 ADRs: why each design choice was made and what it costs |
| [Databricks setup](../databricks/README.md) | Unity Catalog objects, Workflows, verification checklist, serverless caveats |
| [Changelog](../CHANGELOG.md) | What changed and when, newest first, with links to the ADRs |

Start with the architecture overview; follow ADR links for the reasoning
behind any part of it.

Every document carries a *Last updated* date, each ADR records the date of
its decision, and the changelog gives the project's dated history.
