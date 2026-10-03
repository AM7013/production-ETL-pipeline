# 0007: No real production data in the repository

## Status
Accepted

## Context
The pipeline processes real order data that may contain sensitive
information (customer names, emails). CI/CD needs *some* data to
exercise the pipeline end-to-end, but committing real data to a public
repository is never acceptable, regardless of file size.

## Decision
Real data files (`cleaned_data_v1.csv`, etc.) are excluded via
`.gitignore` and never committed. CI/CD and manual testing use a small,
separate, synthetic sample file (`samples/cleaned_data_test.csv`),
checked into the repo deliberately, that exercises the same schema and
includes known-bad rows to verify the quarantine path actually fires.

## Consequences
- The public repo is safe to share without any data-exposure risk.
- The sample file's deliberately-included bad rows (invalid email,
  malformed numeric values) give CI/CD a reliable, repeatable way to
  confirm the quality engine's quarantine logic works, rather than
  depending on production data happening to contain a bad row on a given
  day.
