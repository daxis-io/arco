# dagster-parity-05 — Land Reliability Foundation PRs

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`

## Goal
Merge the reliability foundation work (timer callback + orchestration invariants) before higher-level parity work depends on it.

This item is explicitly about **sequencing and stability**.

## Non-Goals
- Adding new parity features.
- Broad refactors.

## Deliverables
- The currently-open PRs for:
  - timer callback ingestion
  - orchestration invariants
  are merged (or reworked and merged) into main.

## Work Breakdown
### Step 0 — Identify the PRs
- Candidate PRs found via `gh pr list` + keyword searches (`timer callback`, `callback ingestion`, `heartbeat`, `timer`, `orchestration invariants`, `idempotency`, `out-of-order`, `compactor invariants`):
  - https://github.com/daxis-io/arco/pull/25 — `docs(parity): add Dagster OSS parity matrix + CI gates`.
    - Summary: Adds CI-gated parity gate suites (M1/M2/M3) + `cargo xtask parity-matrix-check`; hardens orchestration invariants (out-of-order timer event handling + duplicate TaskFinished coverage); fixes backfill transition idempotency collisions.
    - Touches: `.github/workflows/ci.yml`, `tools/xtask/src/main.rs`, `crates/arco-flow/src/orchestration/events/mod.rs`, `crates/arco-flow/src/orchestration/controllers/backfill.rs`, `crates/arco-flow/tests/orchestration_{correctness,automation,parity_gates_m{1,2,3}}*.rs`, `crates/arco-api/tests/orchestration_parity_gates_m1.rs`, `docs/parity/**`.
    - CI: `gh pr checks 25` currently green (all required jobs passing; IAM smoke skipped on PRs per `.github/workflows/ci.yml`).
- No other open PRs in `daxis-io/arco` matched the above keywords at the time of this sweep.

### Step 1 — Confirm prerequisites and invariants
- https://github.com/daxis-io/arco/pull/25
  - Modules/files changed:
    - `.github/workflows/ci.yml`
    - `tools/xtask/src/main.rs`
    - `crates/arco-flow/src/orchestration/events/mod.rs`
    - `crates/arco-flow/src/orchestration/controllers/backfill.rs`
    - `crates/arco-flow/tests/orchestration_automation_tests.rs`
    - `crates/arco-flow/tests/orchestration_parity_gates_m1.rs`
    - `crates/arco-flow/tests/orchestration_parity_gates_m2.rs`
    - `crates/arco-flow/tests/orchestration_parity_gates_m3.rs`
    - `crates/arco-api/tests/orchestration_parity_gates_m1.rs`
    - `docs/parity/**`, `docs/adr/adr-025-backfill-controller.md`
  - Tests added/updated (CI-gated):
    - `crates/arco-flow/tests/orchestration_parity_gates_m1.rs` (new)
    - `crates/arco-flow/tests/orchestration_parity_gates_m2.rs` (new)
    - `crates/arco-flow/tests/orchestration_parity_gates_m3.rs` (new)
    - `crates/arco-api/tests/orchestration_parity_gates_m1.rs` (new)
    - `crates/arco-flow/tests/orchestration_automation_tests.rs` (updated: backfill state-change idempotency key)
  - Prerequisite for parity work items 06–11 (per `docs/parity/dagster-parity-02-12-execution-plan.md:71`).
  - CI status: green on PR (see `gh pr checks 25`).

### Step 2 — Review for semantic risk
- Event idempotency behavior
  - Global projection dedupe is by `idempotency_key` during compaction (no duplicate projection effects under at-least-once delivery).
  - CI-gated proofs:
    - `crates/arco-flow/tests/orchestration_parity_gates_m1.rs` (`parity_m1_schedule_tick_projection_is_idempotent_under_duplicate_delivery`, `parity_m1_duplicate_task_finished_event_is_noop`)
    - `crates/arco-flow/tests/orchestration_parity_gates_m3.rs` (`parity_m3_global_idempotency_gate_drops_duplicate_idempotency_keys`)
- Timer dedupe behavior
  - Timer event idempotency keys are derived from `timer_id` (`TimerRequested`/`TimerEnqueued`/`TimerFired` are deduped by key).
  - Out-of-order timer event safety is covered by existing compactor fold regression tests (TimerEnqueued-before-TimerRequested, canonical `fire_at` handling) and is CI-gated via `cargo test -p arco-flow --features test-utils --lib`.
  - Note: this repo’s “timer callback ingestion” appears to be an external HTTP target (emitted by `crates/arco-flow/src/bin/arco_flow_dispatcher.rs` via `ARCO_FLOW_TIMER_TARGET_URL`); no corresponding timer ingestion route was found in `crates/arco-api/src/routes/**` during this sweep.
- `run_key` reservation/dedupe behavior
  - Strong idempotency is enforced via reservation blobs; duplicate `run_key` returns existing run, and fingerprint mismatches are detected as conflicts.
  - CI-gated proofs:
    - `crates/arco-flow/tests/orchestration_parity_gates_m1.rs` (`parity_m1_run_key_reservation_*`)
    - `crates/arco-api/tests/orchestration_parity_gates_m1.rs` (`parity_m1_run_key_idempotency_is_order_insensitive_for_selection`, `parity_m1_run_key_conflicts_on_payload_mismatch`)
- Out-of-order delivery safety
  - Dispatch ack events can arrive before dispatch requested; compactor must retain both `attempt_id` and `cloud_task_id`.
  - Late/stale `TaskFinished` must not regress a newer attempt after retry has started.
  - CI-gated proofs:
    - `crates/arco-flow/tests/orchestration_parity_gates_m1.rs` (`parity_m1_compactor_persists_out_of_order_dispatch_fields`, `parity_m1_compactor_rejects_stale_task_finished_after_retry_started`)

### Step 3 — Merge sequencing
- Preferred order: invariants → timers → higher-level parity.
- This sweep found a single open PR containing the reliability foundation work (`https://github.com/daxis-io/arco/pull/25`), so the merge order is simply: merge PR 25.
- If conflicts appear, keep the fix minimal and preserve CI-gated proofs (no doc-only merges).

### Step 4 — Post-merge stabilization
- Run full CI.
- Update parity matrix rows that now become “Implemented” with evidence links.

## Acceptance Criteria
- CI green on main.
- Parity items 06–11 can rely on stable timer callback and invariant behavior.

## Evidence Targets
- CI: `.github/workflows/ci.yml`
- Correctness invariants tests: `crates/arco-flow/tests/orchestration_correctness_tests.rs`
