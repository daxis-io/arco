# Arco Flow vs Dagster Exception Planning

**Status:** Active
**Scope:** Deliberate parity exceptions and deferrals for orchestration capabilities.

## Purpose

This document defines capability exceptions to Dagster OSS semantics that are either:

- permanently out of scope, or
- explicitly deferred to a later milestone.

Any public parity claim must reference this file for declared exceptions.

## Exception Matrix

| ID | Capability | Decision | Type | Revisit Trigger |
|---|---|---|---|---|
| EX-01 | Op/Job authoring model | Asset-first model only | Permanent | Revisit only if asset-first blocks critical adoption |
| EX-02 | Customer-operated daemon model | Managed control-plane model only | Permanent | Revisit if control-plane SLOs cannot be met |
| EX-03 | Full orchestration web UI parity | Deferred | Deferral | Revisit at M2 |
| EX-04 | IO manager plugin parity | Deferred | Deferral | Revisit at M2 |
| EX-05a | Multi-asset execution parity | Deferred | Deferral | Revisit at M2 |
| EX-05b | Observable/external asset parity | Deferred | Deferral | Revisit at M3 |
| EX-06 | Multi-code-location workspace model | Simplified single-location model | Simplified | Revisit on enterprise demand |
| EX-07 | GraphQL API parity | REST/gRPC only | Permanent | No planned revisit |
| EX-08 | Runtime launcher plugin parity | Deferred | Deferral | Revisit at M3 |
| EX-09 | Reconciliation automation parity | Phased delivery | Deferral | Revisit at M2/M3 |

## Rationale Baseline

- Arco prioritizes deterministic execution semantics, replay safety, and CI-provable behavior.
- Architecture decisions are captured in ADRs and implemented via test-gated code paths.
- Product scope favors reliability and operability over one-to-one framework surface matching.

## Canonical Evidence Policy

For exception governance, valid references are:

- ADRs in `docs/adr/**`
- implementation code in `crates/**` and `python/**`
- CI-gated tests and CI workflow commands in `.github/workflows/ci.yml`

Historical planning artifacts are background context only and are not normative proof.

## Cross-References

- Parity charter: `docs/parity/dagster-parity-01-charter.md`
- Parity matrix: `docs/parity/dagster-parity-matrix.md`
- Related ADRs:
  - `docs/adr/adr-020-orchestration-domain.md`
  - `docs/adr/adr-024-schedule-sensor-automation.md`
  - `docs/adr/adr-025-backfill-controller.md`
  - `docs/adr/adr-026-partition-status-tracking.md`

## Review Cadence

- Review exceptions when parity matrix status changes, or at each release milestone touching orchestration semantics.
