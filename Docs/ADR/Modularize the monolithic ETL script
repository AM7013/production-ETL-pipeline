# 0001: Modularize the monolithic ETL script

## Status
Accepted

## Context
The original pipeline was a single ~290-line script that ran top-to-bottom
on import: Spark session setup, extraction, quality checks, and loads to
both Postgres and BigQuery all lived in one file with no clear boundaries
between stages. This made the code hard to test in isolation, hid real
bugs (a silent `to_csv()` call with no destination path, a tracking
function immediately shadowed by its own variable name), and meant any
change risked breaking unrelated logic.

## Decision
Split the pipeline into single-responsibility modules under `tasks/`:
`ingestion.py` (extraction), `quality.py` (validation/scoring),
`storage.py` (loads), `tracking.py` (run history), `schema_validation.py`
(pre-flight column checks). Each module exposes Prefect `@task`-decorated
functions that can be imported and tested independently of the full flow.

## Consequences
- Each module now has its own unit tests, callable via `.fn()` to bypass
  the Prefect task wrapper — this surfaced and fixed real bugs the
  monolithic version had carried silently for months.
- The orchestration layer (`etl_flow.py`) was later reconciled to import
  from these modules instead of maintaining its own parallel
  implementation, eliminating duplicate business logic entirely.
- Tradeoff: more files to navigate than a single script, but each file is
  small enough to hold in your head completely.
