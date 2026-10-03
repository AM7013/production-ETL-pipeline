# 0003: BigQuery loads use load jobs, not DML

## Status
Accepted

## Context
An earlier version of the BigQuery load implemented idempotency via a
scripted `DELETE` + `INSERT` + `DROP TABLE` sequence run as a query. This
works on a billing-enabled BigQuery project, but fails outright on the
free/sandbox tier, which disallows standalone DML entirely — only load
jobs (`to_gbq`, `load_table_from_dataframe`) are permitted without
billing enabled.

## Decision
Load to BigQuery via `google.cloud.bigquery.Client.load_table_from_dataframe`
using `WriteDisposition.WRITE_APPEND` and
`SchemaUpdateOption.ALLOW_FIELD_ADDITION`, rather than any DML-based
upsert. Deduplication/idempotency is handled on the Postgres side (which
has no such restriction); BigQuery is treated as an append-only sink, with
"latest version per key" left to a dbt model doing `ROW_NUMBER()` at
query time rather than enforced at load time.

## Consequences
- Works identically whether or not billing is enabled on the GCP project.
- Partitioning (`loaded_at`, daily) and clustering (`run_id`, natural key)
  are set automatically on first table creation, since BigQuery rejects
  changing them on an existing table.
- Tradeoff: the raw BigQuery table accumulates every historical load
  rather than reflecting only current state — intentional, and the
  dedup-at-query-time model is the documented way to get "current state"
  back out when needed.
