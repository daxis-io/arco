# dagster-parity-10 — Staleness Report + Reconciliation (EX-09 Phased)

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`
- Exceptions: `docs/audits/arco-flow-dagster-exception-planning.md`
- ADR (partition status): `docs/adr/adr-026-partition-status-tracking.md`
- Partition status controller: `crates/arco-flow/src/orchestration/controllers/partition_status.rs`

## Goal
Deliver a phased EX-09 deliverable:

1) **Staleness report** (read-only, explainable)
2) **Reconciliation** (optionally triggers work; gated)

Dagster parity here is about the operator-facing ability to answer:
- “What is stale?”
- “Why is it stale?”
- “What should run to reconcile it?”

## Non-Goals
- Fully automated auto-materialize parity on day 1.
- Dagit UI.

## Deliverables
### Phase A — Staleness report
- A queryable, deterministic staleness signal per asset partition:
  - stale_since
  - stale_reason_code
  - upstream references / causal explanation

### Phase B — Reconciliation plan
- A deterministic “what should run” computation:
  - target selection
  - partition selection
  - run_key strategy

### Phase C — Controlled execution (optional)
- A controller that can emit triggers based on reconciliation output.
- Explicit safety levers:
  - rate limits
  - dry-run
  - per-workspace enable

## Work Breakdown
### Step 0 — Lock semantics
- Define what “stale” means:
  - upstream materialization newer than downstream
  - code version change
  - freshness/SLA violation
- Define what is in scope for the first report.

### Step 1 — Ensure partition status has the needed fields
- Verify `PartitionStatusRow` includes:
  - last materialization info
  - last attempt outcome
  - stale_since / stale_reason fields

Primary files:
- `crates/arco-flow/src/orchestration/compactor/fold.rs`
- `crates/arco-flow/src/orchestration/controllers/partition_status.rs`

### Step 2 — Implement staleness computation
- Implement deterministic staleness rules.
- Ensure causality/explanation is captured (even if coarse initially).

### Step 3 — Expose report API
- Provide an API endpoint to list stale partitions (and why).
- Ensure pagination and stable ordering.

### Step 4 — Proof (tests + CI)
Add tests proving:
- staleness transitions are deterministic
- stale_since monotonicity under duplicates
- explanation fields are stable

Suggested tests:
- `crates/arco-flow/tests/orchestration_staleness_tests.rs` (new)

## Acceptance Criteria
- Operators can query a staleness report and understand it without external state.
- The report is reproducible from ledger/projections.

## Risks / Edge Cases
- Defining “why stale” without full lineage can be tricky; start minimal.
- Avoid accidental auto-triggering (keep reconciliation execution gated).
