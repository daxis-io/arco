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
- CI wiring: `.github/workflows/ci.yml`

## Risks / Edge Cases
- HA without strict external coordination can create duplication; must be proven by tests.
- Load tests can be flaky; keep deterministic simulation gates where possible.
