# dagster-parity-04 — Automated Parity Gates (M1/M2/M3)

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`
- CI: `.github/workflows/ci.yml`
- Existing invariants/tests:
  - `crates/arco-flow/tests/orchestration_correctness_tests.rs`
  - `crates/arco-flow/tests/orchestration_automation_tests.rs`
  - `crates/arco-flow/tests/orchestration_sensor_tests.rs`

## Goal
Turn milestone “uncheatable tests” into automated, reproducible gates.

A “gate” is a test suite or script that:
- is runnable locally
- is wired into CI (PR or main-only)
- fails on semantic regressions

## Non-Goals
- Full production smoke tests requiring deployed infra (those can be main-only).
- Adding new features without tests.

## Deliverables
1. A named set of parity gate suites:
   - Gate M1: execution invariants + run identity + selection semantics
   - Gate M2: schedules/sensors/backfills workflows
   - Gate M3: staleness/reconciliation + advanced UX
2. CI wiring that makes it hard to “cheat” by changing docs only.

## Gate Commands (Copy/Paste)

These are the intended **single-command** entry points for each gate suite.

- Gate M1 (PR-gated, hermetic):
  - `cargo test -p arco-api --all-features --test orchestration_parity_gates_m1 && cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m1`
- Gate M2 (PR-gated, hermetic):
  - `cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m2`
- Gate M3 (PR-gated, hermetic):
  - `cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m3`

Related operator-surface proof (CLI-first UX):
- `cd python/arco && python -m pytest tests/integration/test_cli_api.py -v`

## Recommended Gate Structure
### Gate class A — Hermetic (PR-gated)
- Deterministic, local tests.
- Prefer `cargo test` + `pytest`.

Examples of what to include:
- event idempotency keys
- fold state machine invariants
- run_key conflict detection
- schema primary key invariants

### Gate class B — Simulation (PR-gated)
- Still local/hermetic, but uses multi-step simulation.
- Example: build schedule definition rows + trigger ticks + fold events → assert state.

### Gate class C — Deployed smoke (main-only)
- Requires cloud credentials and deployed services.
- Example: “persist schedule → wait for tick → run appears in history”.

## Work Breakdown
### Step 0 — Enumerate milestone gates
For each milestone (M1/M2/M3):
- write down the minimum set of semantic invariants
- map each invariant to:
  - an existing test (if present)
  - or a new test to add

### Step 1 — Implement or consolidate tests
Preferred layout:
- `crates/arco-flow/tests/orchestration_parity_gates_m1.rs`
- `crates/arco-flow/tests/orchestration_parity_gates_m2.rs`
- `crates/arco-flow/tests/orchestration_parity_gates_m3.rs`

Alternative: keep tests in existing files, but tag them clearly.

### Step 2 — Wire into CI
- Ensure gates are exercised by `.github/workflows/ci.yml`.
- If gates are slow, split into a separate job (still PR-gated) with caching.

### Step 3 — Add “matrix honesty” validation (optional)
Add a script/xtask check that:
- fails if a matrix row is marked Implemented without CI proof links

## Acceptance Criteria
- A PR that breaks schedule tick idempotency, sensor dedupe, backfill state machine, or run_key semantics fails CI.
- A dev can run the same gates locally.

## Candidate Existing Evidence Hooks
- run_key strong idempotency: `crates/arco-flow/src/orchestration/run_key.rs`
- deterministic run_id: `crates/arco-flow/src/orchestration/ids.rs`
- schedule controller: `crates/arco-flow/src/orchestration/controllers/schedule.rs`
- sensor controller: `crates/arco-flow/src/orchestration/controllers/sensor.rs`
- backfill controller: `crates/arco-flow/src/orchestration/controllers/backfill.rs`
