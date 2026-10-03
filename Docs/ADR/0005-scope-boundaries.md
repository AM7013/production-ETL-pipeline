# 0005: Scope boundaries — what this pipeline deliberately excludes

## Status
Accepted

## Context
A batch ETL pipeline sits adjacent to a large number of related
disciplines — streaming, infrastructure-as-code, container orchestration,
data lakehouse formats, formal data contracts, lineage tooling. Each adds
real complexity and operational overhead. For a solo-maintained pipeline
processing batch e-commerce order data, several of these solve problems
that don't exist in this context.

## Decision
Explicitly scope out, for now:
- **Streaming** — no sub-batch latency requirement exists for this data.
- **Delta Lake / table-format versioning** — `run_id`/`loaded_at` tagging
  already provides lightweight version tracking without the operational
  cost of a lakehouse format.
- **Formal data contracts** — exist to coordinate between teams that
  don't trust or can't see each other's changes; the quality/schema
  validation layer already performs the equivalent check at the one
  boundary that exists (solo maintainer, single producer/consumer).
- **Kubernetes / Terraform** — not yet introduced; current scale doesn't
  require orchestrated infrastructure provisioning.

## Consequences
- Keeps the system's operational surface area matched to its actual
  requirements rather than its theoretical maximum.
- These exclusions are revisited, not permanent — e.g. Terraform is
  planned as a deliberate, isolated future addition once there's a
  genuine multi-environment provisioning need to justify it.
