# dagster-parity-07 — Sensors E2E

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`
- ADR: `docs/adr/adr-024-schedule-sensor-automation.md`
- Sensor controller: `crates/arco-flow/src/orchestration/controllers/sensor.rs`
- Sensor tests: `crates/arco-flow/tests/orchestration_sensor_tests.rs`
- Automation event tests: `crates/arco-flow/tests/orchestration_automation_tests.rs`

## Goal
Deliver sensor semantics end-to-end for:

- **push sensors**: duplicate delivery safe
- **poll sensors**: durable cursor, CAS semantics, backoff

with run triggering via `RunRequested` and `run_key` idempotency.

## Non-Goals
- Full connector ecosystem (S3/GCS/etc.) beyond proving the semantics.
- Complex UI.

## Deliverables
1. Sensor definition persistence (if required by ADR):
   - id
   - type (push/poll)
   - evaluation config
2. Sensor evaluation events:
   - `SensorEvaluated` with correct idempotency keys
   - cursor_before/cursor_after semantics
   - expected_state_version for CAS (poll)
3. Run triggering:
   - `RunRequested` events produced from sensor run requests
   - run_key idempotency guarantees
4. History APIs:
   - sensor evaluation history
   - sensor state (cursor/status/version)

## Work Breakdown
### Step 0 — Lock semantics
- Define push sensor idempotency key strategy (message_id based).
- Define poll sensor idempotency strategy (cursor_before + poll epoch).
- Define CAS policy:
  - accept update only if expected_state_version matches.

### Step 1 — Persist sensor state
- Ensure sensor state rows exist and are folded correctly:
  - cursor
  - status
  - state_version

Primary files:
- `crates/arco-flow/src/orchestration/compactor/fold.rs` (SensorStateRow/SensorEvalRow)
- `crates/arco-flow/src/orchestration/compactor/parquet_util.rs`

### Step 2 — Implement evaluation controller behavior
- Push: ingest should emit `SensorEvaluated` with message_id-based idempotency.
- Poll: evaluation should include expected_state_version and advance cursor.
- Backoff semantics: on failures, reschedule deterministically.

Primary file:
- `crates/arco-flow/src/orchestration/controllers/sensor.rs`

### Step 3 — Run triggering semantics
- For each emitted run request:
  - `RunRequested` must be deterministic
  - `run_key` must dedupe correctly

Primary files:
- `crates/arco-flow/src/orchestration/compactor/fold.rs`
- `crates/arco-flow/src/orchestration/run_key.rs`

### Step 4 — Proof (tests + CI)
Extend existing tests to cover:
- push duplicate message dedupe (already covered): `crates/arco-flow/tests/orchestration_sensor_tests.rs`
- poll CAS semantics:
  - correct version increments
  - reject/no-op when expected_state_version mismatches
- cursor durability:
  - state survives multiple folds

Suggested new test file:
- `crates/arco-flow/tests/orchestration_sensor_e2e_tests.rs`

CI hook:
- `.github/workflows/ci.yml` `cargo test -p arco-flow --features test-utils`

## Acceptance Criteria
- Push sensor: duplicate delivery produces at most one durable evaluation effect.
- Poll sensor: CAS prevents cursor stomping.
- Sensor evaluations can trigger runs deterministically.

## Risks / Edge Cases
- Sensor evaluation race conditions and ordering.
- Cursor encoding stability (avoid opaque/un-versioned formats).
- Backoff jitter determinism vs operational randomness (define policy).
