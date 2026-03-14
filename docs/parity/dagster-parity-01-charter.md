# Dagster Parity Charter (OSS Semantics)

## Purpose
This document locks the **scope** and the **definition of “done”** for Dagster parity work in this repo.

This repo uses serverless, event-sourced projections and will not match Dagster’s implementation details. We target parity at the level of **user-facing semantics**.

## Scope (Locked)
**Dagster parity** in this repo means:

- **Dagster OSS user-facing semantics** for automation and operability:
  - schedules (ticks + history)
  - sensors (push/poll, durable cursor)
  - partitions + backfills (pause/resume/cancel/retry-failed)
  - partition status tracking
  - staleness reporting + reconciliation (phased)
  - selection/reruns (subset selection, re-execution semantics)
  - run identity via `run_key` idempotency

We explicitly do **not** claim Dagster+ parity (managed product operational limits). We publish Arco-specific limits/SLOs separately.

### Out of Scope / Exceptions
Anything explicitly listed as a deliberate exception is governed by:
- `docs/audits/arco-flow-dagster-exception-planning.md`

This includes (non-exhaustive): full Dagit UI parity, IO managers, multi-code-location workspace model, etc.

## Normative vs Non‑Normative (Source of Truth)
Parity claims must be backed by **repo-addressable** evidence.

### Normative (may be cited as proof)
- **Parity docs** (scope, rules, matrices): `docs/parity/**` (including this charter)
- **ADRs** (architecture + semantic decisions): `docs/adr/**`
  - Examples: `docs/adr/adr-024-schedule-sensor-automation.md`, `docs/adr/adr-025-backfill-controller.md`, `docs/adr/adr-026-partition-status-tracking.md`
- **Audits** (exceptions only): `docs/audits/**`
  - Canonical exception list: `docs/audits/arco-flow-dagster-exception-planning.md`
- **Code**: implementation in `crates/**` and `python/**`
- **Tests that run in CI**: proofs of behavior (for “Done (Implemented)”)
  - Examples: `crates/arco-flow/tests/orchestration_automation_tests.rs`, `crates/arco-flow/tests/orchestration_sensor_tests.rs`, `crates/arco-flow/tests/orchestration_schema_tests.rs`
- **CI configuration**: what is actually gated
  - `.github/workflows/ci.yml`

### Non‑normative (may not be used as proof)
- Removed legacy planning documents
- Anything under `.worktrees/**`
- External trackers/docs (Notion/Jira/etc.)
- External product docs (including Dagster docs) may define target behavior, but do not prove parity in this repo

## Source-of-Truth Checklist (Repo-Addressable)
A reader should be able to answer the following questions using only repo paths:

- **What is “Dagster parity” here?** → `docs/parity/dagster-parity-01-charter.md`
- **What is explicitly out of scope?** → `docs/audits/arco-flow-dagster-exception-planning.md`
- **What is CI-gated evidence?** → `crates/arco-flow/tests/` + `.github/workflows/ci.yml`
- **What runs the evidence in CI?** → `.github/workflows/ci.yml` `test` job (runs `cargo test -p arco-flow --features test-utils`)

## Status Taxonomy
Use these labels in parity matrices and plans:

- **Implemented**: behavior exists in code and is proven by CI-gated tests.
- **Partial**: some semantics are implemented and proven, but the full user-facing workflow is incomplete.
- **Designed**: ADR/doc exists but no CI-gated proof of behavior.
- **Planned**: intent exists (issue/plan) but no locked design.
- **Exception**: explicitly out of scope per exception planning doc.

## Definition of “Done”
A parity item is **Done** only if it meets one of the following:

### Done (Implemented)
All are required:
- The behavior is implemented in code (primary code path(s) are repo-addressable).
- There is at least one **CI-gated** test proving the behavior.
- The parity matrix row links to:
  - primary code path(s)
  - the proving test(s)
  - the CI job/command that executes the test(s)
    - Required reference: `.github/workflows/ci.yml` job `test` runs `cargo test -p arco-flow --features test-utils`

### Done (Exception)
- The item is explicitly declared as an exception in `docs/audits/arco-flow-dagster-exception-planning.md`.

## Evidence Linking Rules
In parity matrices and plans:
- Prefer linking to concrete **file paths and tests** over prose.
- Use `path:line` references where possible.
- Do not use removed legacy planning documents as evidence.

## Historical / Superseded Documents
The following documents may contain useful background but are not valid proof for parity claims:

- `docs/audits/arco-flow-dagster-parity-audit.md` is explicitly superseded.
- Removed legacy planning documents are non-normative and may not be used as proof.

## Change Policy
This charter is intended to be stable. If scope or “done” criteria must change:
- Update this document in the same PR as the first change that relies on the new definition.
