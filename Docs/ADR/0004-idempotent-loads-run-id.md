# 0004: Idempotent loads via run_id tagging and delete-then-insert

## Status
Accepted

## Context
The original load logic used `if_exists="replace"`, which destroyed all
history on every run, and provided no way to trace which pipeline run
produced which rows — a real gap for debugging a downstream dbt test
failure back to its source.

## Decision
Every load tags each row with a generated `run_id` (UUID) and `loaded_at`
timestamp. For Postgres, loads are made idempotent on the natural key
(`OrderID`): existing rows matching incoming keys are deleted via a
temp-table join before the new batch is inserted via `COPY`, so re-running
the same batch twice doesn't duplicate rows.

## Consequences
- Every row is traceable to the exact pipeline run that loaded it, and
  failed/successful runs are both recorded in a `pipeline_runs` audit
  table.
- Re-running a failed or partial load is now safe — no manual cleanup
  required before retrying.
- Tradeoff: the delete-then-insert pattern costs an extra round trip per
  load compared to a blind append; acceptable at current data volumes.
