# dagster-parity-02 — Repo-Addressable Parity Audit

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`
- Exceptions: `docs/audits/arco-flow-dagster-exception-planning.md`
- Existing CI: `.github/workflows/ci.yml`

## Goal
Make parity progress **provable from this repository** via code + CI-gated tests + file-path evidence links.

This item is about **epistemology**: if we can’t prove it from the repo, we don’t claim it.

## Non-Goals
- Implement missing parity features (that is parity-06+).
- Re-scope parity beyond Dagster OSS semantics.
- Replace all prose documentation with tests.

## Deliverables
1. A tracked, stable location for parity artifacts (this directory): `docs/parity/`.
2. A policy that parity evidence must be:
   - code paths
   - tests
   - CI jobs/commands
3. A “migration map” for any still-relevant content currently living in ignored `docs/plans/**`.

## Repo Inventory (Current)
### What is normative today
- Scope + “Done” definition: `docs/parity/dagster-parity-01-charter.md`
- Exception boundaries: `docs/audits/arco-flow-dagster-exception-planning.md`
  - Note: this doc contains `docs/plans/**` citations inside “Evidence” fields. Per the charter, those citations are **historical context only** and must not be treated as proof.
- CI execution reference: `.github/workflows/ci.yml`
  - CI-gated `arco-flow` evidence harness: `cargo test -p arco-flow --features test-utils`
- Evidence-grade tests to cite for Dagster parity claims:
  - `crates/arco-flow/tests/orchestration_automation_tests.rs`
  - `crates/arco-flow/tests/orchestration_sensor_tests.rs`
  - `crates/arco-flow/tests/orchestration_schema_tests.rs`

### Non-normative parity artifacts currently present (git-ignored)
The following documents can be read for background but **must not** be used as proof.

Enforcement note: `docs/plans/**` is git-ignored by `.gitignore`, so these files are not present in CI checkouts. Parity work must not (and cannot) rely on them.

Concrete inventory (parity-relevant, git-ignored):
- `docs/plans/2026-01-13-layer2-dagster-oss-parity-serverless-ship-plan.md`
- `docs/plans/2025-12-22-layer2-automation-execution-plan.md`
- `docs/plans/2026-01-01-orchestration-read-api-hardening.md`
- `docs/plans/2025-12-19-servo-arco-vs-dagster-parity-audit.md`
- `docs/plans/2025-12-19-servo-event-driven-orchestration-execution-plan.md`
- `docs/plans/2025-12-19-servo-orchestration-superpowers-prompt.md`
- `docs/plans/2025-12-19-servo-orchestration-execution-prompt.md`
- `docs/plans/2025-12-22-layer2-execution-prompt.md`
- `docs/plans/2025-12-16-part4-integration-testing.md`
- `docs/plans/2025-12-17-python-sdk-alpha.md`
- `docs/plans/2025-12-17-python-sdk-alpha-execution-prompt.md`
- `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md`
- `docs/plans/2025-12-18-gate5-hardening.md`
- `docs/plans/2025-12-19-gate5-hardening-execution-prompt.md`
- `docs/plans/2025-12-14-part3-orchestration-mvp.md`
- `docs/plans/2025-01-12-arco-orchestration-design.md`
- `docs/plans/2025-01-12-arco-orchestration-design-part2.md`
- `docs/plans/2025-01-12-arco-unified-platform-design.md`
- `docs/plans/2025-01-13-phase1-implementation.md`
- `docs/plans/ARCO_PROJECT_PLAN.md`
- `docs/plans/ARCO_TECHNICAL_VISION.md`
- `docs/plans/ARCO_ARCHITECTURE_PART1_CORE.md`

## Migration Map (No `docs/plans/**` Proof)
The table below enumerates parity-relevant information that currently lives in ignored `docs/plans/**` (or is referenced by normative docs), and where the canonical tracked replacement must live.

| Ignored doc (`docs/plans/**`) | Claim it makes (background only) | Canonical tracked replacement |
|---|---|---|
| `docs/plans/2026-01-13-layer2-dagster-oss-parity-serverless-ship-plan.md` | Dagster OSS parity scope + serverless constraints + “what’s left” | `docs/parity/**` (scope/rules), `docs/parity/dagster-parity-03-parity-matrix.md` (status), ADRs for semantics (`docs/adr/adr-024-schedule-sensor-automation.md`, `docs/adr/adr-025-backfill-controller.md`, `docs/adr/adr-026-partition-status-tracking.md`) |
| `docs/plans/2025-12-22-layer2-automation-execution-plan.md` | Proposed schedules/sensors/backfills plan and controller patterns | `docs/parity/dagster-parity-02-12-execution-plan.md` (planning), ADRs for any adopted semantics (`docs/adr/**`), CI-gated proving tests (`crates/arco-flow/tests/**`) |
| `docs/plans/2026-01-01-orchestration-read-api-hardening.md` | Read API invariants for schedules/sensors/backfills/partitions (pagination, correctness) | CI-gated proving tests (`crates/arco-flow/tests/**`) + CI step `.github/workflows/ci.yml` job `test` (`cargo test -p arco-flow --features test-utils`) |
| `docs/plans/2025-12-19-servo-arco-vs-dagster-parity-audit.md` | Historical parity audit (including selection/reruns gaps) | `docs/parity/dagster-parity-03-parity-matrix.md` using charter taxonomy; exceptions remain in `docs/audits/arco-flow-dagster-exception-planning.md` |
| `docs/plans/2025-12-19-servo-event-driven-orchestration-execution-plan.md` | Foundational constraints for event-sourced orchestration and automation semantics | ADRs for semantics (`docs/adr/**`, especially `docs/adr/adr-020-orchestration-domain.md`, `docs/adr/adr-023-worker-contract.md`, `docs/adr/adr-024-schedule-sensor-automation.md`) + CI-gated tests (`crates/arco-flow/tests/**`) |
| `docs/plans/2025-12-19-servo-orchestration-superpowers-prompt.md` | “Uncheatable” parity invariants and suggested proving tests | Canonical parity gates doc `docs/parity/dagster-parity-04-parity-gates.md` + CI-gated parity tests (`crates/arco-flow/tests/orchestration_parity_gates_m*.rs`) |
| `docs/plans/2025-12-19-servo-orchestration-execution-prompt.md` | M1 parity expectations, including `run_key` idempotency framing | Semantics locked in ADRs (`docs/adr/adr-024-schedule-sensor-automation.md`, `docs/adr/adr-025-backfill-controller.md`) + CI-gated tests (`crates/arco-flow/tests/**`) |
| `docs/plans/2025-12-22-layer2-execution-prompt.md` | Layer2 parity invariants for schedules/sensors/backfills | Canonical parity gates doc `docs/parity/dagster-parity-04-parity-gates.md` + CI-gated parity tests (`crates/arco-flow/tests/orchestration_parity_gates_m*.rs`) |
| `docs/plans/2025-12-16-part4-integration-testing.md` | E2E/integration proving approach for orchestration semantics | CI-gated tests (`crates/arco-flow/tests/**`) that run in `.github/workflows/ci.yml` |
| `docs/plans/2025-12-17-python-sdk-alpha.md` | SDK contract rules that require canonical JSON + deterministic identity | ADRs: `docs/adr/adr-010-canonical-json.md`, `docs/adr/adr-011-partition-identity.md`, `docs/adr/adr-013-id-wire-formats.md` |
| `docs/plans/2025-12-17-python-sdk-alpha-execution-prompt.md` | Partition key encoding constraints (no floats; type-tagged canonical form) | ADR: `docs/adr/adr-011-partition-identity.md` (plus `docs/adr/adr-010-canonical-json.md`) |
| `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` | Determinism + canonical identity requirements for production-grade orchestration | ADRs: `docs/adr/adr-010-canonical-json.md`, `docs/adr/adr-011-partition-identity.md`, `docs/adr/adr-013-id-wire-formats.md` |
| `docs/plans/2025-12-18-gate5-hardening.md` | Hardening invariants that impact correctness (partition identity, single-writer assumptions) | ADRs for the invariant (`docs/adr/**`) + CI-gated tests for behavior (`crates/**/tests/**`) |
| `docs/plans/2025-12-19-gate5-hardening-execution-prompt.md` | Verification steps and suggested tests for hardening | CI-gated tests and CI job references (`.github/workflows/ci.yml`) |
| `docs/plans/2025-12-14-part3-orchestration-mvp.md` | MVP scheduler/execution tracking design assumptions | ADRs for locked behavior (`docs/adr/**`) + parity status in `docs/parity/dagster-parity-03-parity-matrix.md` |
| `docs/plans/2025-01-12-arco-orchestration-design.md` | Early design for schedules/sensors/backfills and orchestration semantics | If still relevant: capture the decision in ADRs (`docs/adr/**`) and prove with CI-gated tests (`crates/arco-flow/tests/**`) |
| `docs/plans/2025-01-12-arco-orchestration-design-part2.md` | Scheduler deployment + backfill operational details | If still relevant: capture the decision in ADRs (`docs/adr/**`, e.g. `docs/adr/adr-014-leader-election.md`) and prove behavior with CI-gated tests |
| `docs/plans/2025-01-12-arco-unified-platform-design.md` | Platform-wide invariants (canonical identity; deterministic planning) | ADRs: `docs/adr/adr-010-canonical-json.md`, `docs/adr/adr-011-partition-identity.md`, `docs/adr/adr-013-id-wire-formats.md` |
| `docs/plans/2025-01-13-phase1-implementation.md` | Early implementation plan for partitions/schedules/sensors trigger types | If still relevant: capture the decision in ADRs (`docs/adr/**`) and prove with CI-gated tests |
| `docs/plans/ARCO_PROJECT_PLAN.md` | High-level success metrics (“local-to-prod parity”) | Parity status and definition of done live in `docs/parity/dagster-parity-01-charter.md` + `docs/parity/dagster-parity-03-parity-matrix.md` |
| `docs/plans/ARCO_TECHNICAL_VISION.md` | Product vision referencing orchestration automation capabilities | If still relevant to semantics: capture in ADRs (`docs/adr/**`); parity status remains in `docs/parity/**` |
| `docs/plans/ARCO_ARCHITECTURE_PART1_CORE.md` | Bounded staleness / tiered consistency expectations | ADRs defining staleness/consistency (`docs/adr/**`), plus CI-gated tests where applicable |

Additional doc hygiene targets (tracked docs that cite ignored plans):
- `docs/audits/arco-flow-dagster-exception-planning.md` (normative for exceptions) currently contains `docs/plans/**` citations in “Evidence” fields. Per the charter, those citations are historical context only and must not be treated as proof. If we want “Evidence” fields, re-anchor them to tracked ADRs (`docs/adr/*`) and/or code/tests.
- `docs/audits/arco-flow-dagster-parity-audit.md` (tracked, superseded) cites `docs/plans/**` heavily; treat as historical and do not cite for current parity claims.

## Proof Rules (Hard)
- No parity proof may rely on `docs/plans/**` (git-ignored by `.gitignore`).
- “Implemented” requires **all** of: code path(s) + CI-gated proving test(s) + CI command/job reference.
- “Designed/Planned” may cite ADRs and parity docs, but must not be presented as shipped parity.

## Work Breakdown
### Step 0 — Inventory
- Search for references to `docs/plans/` in:
  - docs under `docs/audits/`, `docs/adr/`, `docs/parity/`
  - code comments that claim parity
- Produce a short list (table) of:
  - ignored doc path
  - what claim it makes
  - where the claim should live going forward (ADR vs test vs docs/parity)

### Step 1 — Migrate “authoritative” claims out of ignored docs
For each still-relevant claim currently living only under `docs/plans/**`:
- If it is a **semantic** decision: create/extend an ADR in `docs/adr/`.
- If it is a **behavior** claim: add or extend CI-gated tests.
- If it is a **status/progress** artifact: add it to the parity matrix (parity-03).

### Step 2 — Close the loop with CI
- Ensure any tests used as “evidence” are actually executed by CI.
- If tests are `#[ignore]` or require infra, treat them as:
  - main-only CI jobs
  - or locally reproducible scripts with explicit environment requirements

### Step 3 — Assert the rule (optional hardening)
Add a lightweight guardrail so regressions are obvious:
- A script or `cargo xtask` that fails if:
  - `docs/parity/dagster-parity-matrix.md` contains any `docs/plans/` links
  - any matrix row marked Implemented lacks a Test/CI evidence entry

## Acceptance Criteria
- A new engineer can determine current parity status by reading only:
  - `docs/parity/dagster-parity-01-charter.md`
  - `docs/parity/dagster-parity-matrix.md` (to be created in parity-03)
  - and clicking file-path evidence links.
- No “Implemented” claim depends on external docs or ignored `docs/plans/**`.

## Evidence Targets (Existing)
- CI already runs `arco-flow` tests (proof harness): `.github/workflows/ci.yml`
- Existing “evidence-grade” integration tests:
  - `crates/arco-flow/tests/orchestration_automation_tests.rs`
  - `crates/arco-flow/tests/orchestration_sensor_tests.rs`
  - `crates/arco-flow/tests/orchestration_schema_tests.rs`
  - `crates/arco-flow/tests/orchestration_correctness_tests.rs`

## Open Questions (TBD)
- Do we want the parity matrix to be purely Markdown, or also a CSV for automation?
- Do we want an automated check that blocks PRs that change parity docs without updating evidence links?
