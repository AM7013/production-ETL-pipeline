# 0002: SCD2 snapshot strategy — check, not timestamp

## Status
Accepted

## Context
dbt snapshots support two change-detection strategies: `timestamp`
(compares an `updated_at` column) and `check` (compares the actual values
of specified columns). The source data has an `OrderDate` column, which
looks like a natural fit for `timestamp` — but `OrderDate` is set once at
order creation and never changes again for a given order. Using it as
`updated_at` meant the snapshot could never detect a real change (e.g. a
`Status` transition from Pending to Shipped), because the one column it
was watching never moved.

## Decision
Use `strategy: check` with `check_cols: ['"Status"']` instead. This
compares the actual column value between runs, with no dependency on a
timestamp column the source data doesn't genuinely have.

## Consequences
- The snapshot now correctly captures real status-transition history
  (e.g. time spent in Pending before shipping becomes queryable).
- `check_cols` can be widened later (e.g. to include `UnitPrice` or
  `Discount`) if price-correction history becomes worth tracking.
- Tradeoff: `check` strategy does more comparison work per run than a
  pure timestamp check, though at this data volume it's not measurable.
