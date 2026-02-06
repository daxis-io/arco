# dagster-parity-06 — Schedules E2E

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`
- ADR: `docs/adr/adr-024-schedule-sensor-automation.md`
- Schedule controller: `crates/arco-flow/src/orchestration/controllers/schedule.rs`
- Fold + row types: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- API surface: `crates/arco-api/src/routes/orchestration.rs`
- Existing tests (event semantics): `crates/arco-flow/tests/orchestration_automation_tests.rs`

## Goal
Deliver schedule semantics end-to-end:

persisted schedule definitions → tick evaluation → run creation (via `RunRequested`) → durable tick history APIs.

“E2E” here means the **full semantic loop**, not necessarily a deployed cloud environment.

## Non-Goals
- Dagit UI.
- Dagster+ advanced ops features.
- Complex cron libraries beyond what the ADR requires.

## Deliverables
1. Persisted schedule definitions:
   - CRUD semantics (create/update/enable/disable)
   - definition versioning (to correlate ticks to a definition)
2. Tick evaluation semantics:
   - tick identity
   - idempotency
   - catch-up window and max catch-up ticks
3. Run triggering:
   - tick can produce `RunRequested` with `run_key`
   - run creation is idempotent by `run_key`
4. History APIs:
   - list schedule state
   - list tick history
   - drill into a tick and see resulting run_key/run_id

## Work Breakdown
### Step 0 — Lock semantic contracts
- Confirm `tick_id` construction is stable and deterministic.
- Confirm the meaning of schedule enable/disable and how it affects tick history.
- Confirm catch-up semantics (bounded) per ADR.

### Step 1 — Ensure persisted definitions + schema
- Verify schedule definition rows are persisted and read through compactor/manifest.
- Ensure definition version changes are visible in ticks.

Primary files:
- `crates/arco-flow/src/orchestration/compactor/fold.rs` (ScheduleDefinitionRow/StateRow/TickRow)
- `crates/arco-flow/src/orchestration/compactor/parquet_util.rs` (read/write)

### Step 2 — Tick evaluation controller
- Ensure schedule controller produces:
  - `ScheduleTicked` events with correct status
  - `RunRequested` events when triggered
  - no run_key for skipped/failed ticks

Primary file:
- `crates/arco-flow/src/orchestration/controllers/schedule.rs`

### Step 3 — Run creation + correlation
- Ensure `RunRequested` events:
  - create or reuse `run_key_index`
  - support conflict detection via fingerprint

Primary files:
- `crates/arco-flow/src/orchestration/compactor/fold.rs` (run_key_index/conflicts)
- `crates/arco-flow/src/orchestration/run_key.rs` (strong reservation, if used in API path)
- `crates/arco-api/src/routes/orchestration.rs` (TriggerRun behavior)

### Step 4 — History APIs
- Ensure API exposes:
  - schedule list/state
  - tick list
  - tick → run linkage

Primary file:
- `crates/arco-api/src/routes/orchestration.rs`

### Step 5 — Proof (tests + CI)
Add/extend tests that prove:
- Tick identity and idempotency.
- Catch-up bounded behavior.
- Skipped/failed ticks do not create runs.
- Triggered ticks create exactly one run for a given run_key.

Suggested test locations:
- `crates/arco-flow/tests/orchestration_schedule_e2e_tests.rs` (new)
- Existing: `crates/arco-flow/tests/orchestration_automation_tests.rs`

CI hook:
- `.github/workflows/ci.yml` already runs `cargo test -p arco-flow --features test-utils`

## Acceptance Criteria
- A schedule can be defined, evaluated into ticks, and those ticks deterministically map to `RunRequested`.
- Duplicate evaluation (same tick) does not produce duplicates.
- Operators can query tick history and see outcomes.

## Risks / Edge Cases
- Timezone correctness and DST boundaries (define policy explicitly).
- Catch-up algorithm determinism under concurrent evaluations.
- Backpressure: max catch-up ticks must prevent runaway.
