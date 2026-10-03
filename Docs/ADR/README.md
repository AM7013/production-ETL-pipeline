# Architecture Decision Records

This folder documents significant architectural and design decisions made
during the development of this pipeline — what was decided, why, and what
trade-offs were accepted. ADRs are not updated after the fact; if a decision
changes later, a new ADR supersedes the old one rather than editing it.

| # | Title | Status |
|---|-------|--------|
| 0001 | [Modularize the monolithic ETL script](0001-modularize-monolithic-script.md) | Accepted |
| 0002 | [SCD2 snapshot strategy: check vs timestamp](0002-dbt-snapshot-check-strategy.md) | Accepted |
| 0003 | [BigQuery load strategy: load jobs, not DML](0003-bigquery-load-jobs-not-dml.md) | Accepted |
| 0004 | [Idempotent loads via run_id tagging and delete-then-insert](0004-idempotent-loads-run-id.md) | Accepted |
| 0005 | [Scope boundaries: what this pipeline deliberately does not include](0005-scope-boundaries.md) | Accepted |
| 0006 | [Staging environment: manual-only, schema-only branch](0006-staging-manual-schema-only.md) | Accepted |
| 0007 | [No real data in the public repository](0007-no-real-data-in-repo.md) | Accepted |
