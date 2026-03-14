# dagster-parity-11 — Production Hardening

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`
- CI gates: `.github/workflows/ci.yml`

## Goal
Make parity features production-grade:

- HA / leader election
- DR / rebuild procedures
- load tests
- runbooks and observability

This item is explicitly about **operational correctness under failure**.

## Non-Goals
- Matching Dagster’s infra choices.
- UI parity.

## Deliverables
1. HA / leader election:
   - a single active evaluator for schedules/sensors/backfills where required
   - safe failover
2. DR:
   - documented rebuild of projections from ledger
   - documented recovery if compaction falls behind
3. Load tests:
   - controller throughput
   - event duplication / reordering
   - backfill scale scenarios
4. Runbooks/observability:
   - key metrics, logs, traces
   - alert conditions

## Work Breakdown
### Step 0 — Identify critical services and failure modes
- schedule evaluator
- sensor evaluator
- backfill controller
- compactor

Enumerate failure modes:
- duplicate delivery
- out-of-order delivery
- compactor lag
- partial writes
- leader change mid-evaluation

### Step 1 — HA strategy
- Document and implement leader election policy.
- Ensure controllers are safe under leader churn.

### Step 2 — DR procedures
- Define and test a “rebuild from ledger” procedure.
- Ensure no bucket listing requirements are violated (manifest-driven paths).

### Step 3 — Load testing
- Add a load test harness (could be Rust integration tests or standalone harness).
- Define pass/fail criteria.

### Step 4 — Runbooks + observability
- Produce runbooks for:
  - schedule drift
  - sensor stuck
  - backfill stalled
  - compactor lag

## Acceptance Criteria
- A leader failover does not create duplicate runs/ticks/evals.
- DR is documented and repeatable.
- Load tests exist with explicit thresholds.

## Evidence Targets
- Existing invariants tests: `crates/arco-flow/tests/orchestration_correctness_tests.rs`
- Delta coordinator DR crash-window tests: `crates/arco-delta/tests/idempotency_replay.rs`
- Orchestration rebuild DR tests: `crates/arco-flow/tests/orchestration_rebuild_dr.rs`
- DR runbook: `docs/runbooks/disaster-recovery-coordinator-orchestration.md`
- CI wiring: `.github/workflows/ci.yml`

## Q4 DR Hardening Evidence (2026-02-21)
- Coordinator crash-window coverage:
  - crash after reservation, before delta-log write
  - crash after delta-log write, before idempotency marker
  - crash after idempotency marker, before finalize
  - repeated replay returns stable `(version, delta_log_path)`
- Orchestration rebuild coverage:
  - deterministic rebuild from explicit ledger manifest reproduces equivalent projection state
  - stale watermark rebuild correctness without ledger listing
- Proof-gate CI commands:
  - `cargo test -p arco-delta --all-features --test idempotency_replay`
  - `cargo test -p arco-flow --features test-utils --test orchestration_rebuild_dr`

## Risks / Edge Cases
- HA without strict external coordination can create duplication; must be proven by tests.
- Load tests can be flaky; keep deterministic simulation gates where possible.
