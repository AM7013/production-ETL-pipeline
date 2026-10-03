# 0006: Staging environment — manual-only, schema-only branch

## Status
Accepted

## Context
A staging environment was added using a Neon database branch. Two
decisions had to be made: (1) how much data the branch should carry
(full data copy, point-in-time copy, schema-only, or anonymized data),
and (2) whether staging should be wired into automated CI/CD.

## Decision
The staging branch was created as **schema-only** — no production data is
copied into it at all. Staging is also **not** wired into any automated
CI/CD trigger; it's invoked manually (`dbt build --target staging`) when
deliberately testing a change before promoting it toward `ci`/production
targets.

## Consequences
- Schema-only means there is no possible path for real production data
  (which may contain genuinely sensitive information) to exist in a
  second environment — stronger than anonymization, since there's simply
  nothing there to anonymize.
- Keeping staging manual avoids adding a third automated target's worth
  of secrets/config wiring to CI/CD while the pipeline's automation
  surface is still being stabilized; revisit automating it once manual
  use proves it's genuinely load-bearing, not just available.
