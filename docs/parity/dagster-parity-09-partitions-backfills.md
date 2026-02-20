# dagster-parity-09 — Partitions + Backfills Parity

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`
- ADR: `docs/adr/adr-025-backfill-controller.md`
- Backfill controller: `crates/arco-flow/src/orchestration/controllers/backfill.rs`
- Backfill events: `crates/arco-flow/src/orchestration/events/backfill_events.rs`
- API routes: `crates/arco-api/src/routes/orchestration.rs`
- Existing event tests: `crates/arco-flow/tests/orchestration_automation_tests.rs`

## Goal
Deliver Dagster OSS semantics for partitions and backfills:

- creating backfills over partition selections
- chunk planning
- pause/resume/cancel
- retry-failed partitions
- mapping semantics as required

## Non-Goals
- Full UI.
- Multi-code-location asset graphs.

## Deliverables
1. Backfill lifecycle state machine:
   - Created → Running → (Paused | Cancelled | Completed)
   - state_version monotonic behavior
2. Chunk planning:
   - large selectors are represented compactly (PartitionSelector)
   - chunks are durable and queryable
3. Execution semantics:
   - max concurrency
   - retry-failed affects only failed partitions
4. History APIs:
   - list backfills
   - list chunks
   - per-chunk run linkage

## Work Breakdown
### Step 0 — Lock partition identity semantics
- Ensure partition keys are canonical and stable.
- Ensure partition selection representations are compact and deterministic.

### Step 1 — Backfill create
- Emit `BackfillCreated` with PartitionSelector (not full list) per ADR.
- Derive deterministic run_keys for chunks.

Primary files:
- `crates/arco-flow/src/orchestration/controllers/backfill.rs`

### Step 2 — Pause/resume/cancel
- Ensure state transitions are:
  - idempotent
  - monotonic via state_version
  - safe under duplicate deliveries

### Step 3 — Retry-failed partitions
- Identify failed chunks/partitions.
- Create new chunks or re-run logic that only targets failures.
- Ensure auditability (history shows what was retried).

### Step 4 — Proof (tests + CI)
Add/extend tests proving:
- idempotent `BackfillCreated`
- monotonic state transitions
- deterministic chunk run_key generation
- retry-failed does not expand scope

Suggested locations:
- `crates/arco-flow/tests/orchestration_backfill_e2e_tests.rs` (new)
- Existing: `crates/arco-flow/tests/orchestration_automation_tests.rs`

CI hook:
- `.github/workflows/ci.yml` already runs `cargo test -p arco-flow --features test-utils`

## Acceptance Criteria
- Backfill operations are durable, idempotent, and auditable.
- Retry-failed behaves correctly.

## Q3 parity evidence update
- Backfill lifecycle semantics (pause/resume/cancel) and retry-failed targeting are parity-gated in:
  - `crates/arco-flow/tests/orchestration_parity_gates_m2.rs`
  - `parity_m2_backfill_pause_resume_cancel_transitions_are_monotonic`
  - `parity_m2_retry_failed_only_targets_failed_partitions_deterministically`
- Cross-flow consistency (schedule/sensor/backfill/manual reexecution run-key behavior) is covered by:
  - `parity_m2_schedule_sensor_backfill_and_manual_reexecution_share_run_key_consistency`
- CI evidence:
  - `.github/workflows/ci.yml` (job `test`: `cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m2`)

## Risks / Edge Cases
- Very large partition ranges (chunking must remain bounded and efficient).
- Duplicate chunk planning and replay safety.
- Interaction with schedule catch-up.
