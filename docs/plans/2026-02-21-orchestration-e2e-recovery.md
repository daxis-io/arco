# Orchestration End-to-End Recovery (M1-M3)

Date: 2026-02-21
Branch: `codex/orchestration-e2e-recovery`

## Goal
Restore orchestration end-to-end reliability by repairing callback-path schema drift and hardening callback lineage regression coverage across:
- `arco-integration-tests` external worker callback E2E
- `arco-flow` callback contracts and handlers
- `arco-api` callback route mapping and run-read surfaces

## Executed Tasks
- [x] Task 1: Reproduced failing seam and confirmed surrounding orchestration suites healthy.
- [x] Task 2: Fixed `TaskOutput` initializer break in external worker E2E and asserted delta lineage propagation.
- [x] Task 3: Hardened callback type/handler tests for delta lineage serialization and `TaskFinished` output propagation.
- [x] Task 4: Hardened API callback request parsing, output mapping, and HTTP integration assertions for delta lineage and execution lineage ref.
- [x] Task 5: Ran full acceptance matrix for M1/M2/M3 and API orchestration suites.

## Code Changes
- `crates/arco-integration-tests/tests/orchestration_external_worker_e2e.rs`
  - Added `delta_table`, `delta_version`, `delta_partition` in `TaskOutput`.
  - Added compacted final-task assertions for those fields.
- `crates/arco-flow/src/orchestration/callbacks/types.rs`
  - Extended success callback type test to serialize + deserialize delta lineage fields.
- `crates/arco-flow/src/orchestration/callbacks/handlers.rs`
  - Extended success handler test to assert `TaskFinished.output` contains delta lineage fields.
- `crates/arco-api/src/routes/tasks.rs`
  - Extended completed request deserialization test for `deltaTable`, `deltaVersion`, `deltaPartition`.
  - Added `TaskOutput -> FlowTaskOutput` mapping test asserting delta field passthrough.
- `crates/arco-api/tests/api_integration.rs`
  - Extended callback completion payload with delta lineage output.
  - Added run-read assertions for `delta_table`, `delta_version`, `delta_partition`, and non-empty `execution_lineage_ref`.

## Verification
Final acceptance matrix (all passed):

1. `cargo test -p arco-integration-tests --test orchestration_external_worker_e2e`
2. `cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m1`
3. `cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m2`
4. `cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m3`
5. `cargo test -p arco-flow --features test-utils --test orchestration_schedule_e2e_tests`
6. `cargo test -p arco-flow --features test-utils --test orchestration_sensor_e2e_tests`
7. `cargo test -p arco-api --all-features --test api_integration`
8. `cargo test -p arco-api --all-features --test orchestration_schedule_e2e_tests`
9. `cargo test -p arco-api --all-features --test orchestration_api_tests`

## Notes
- During verification, a `No space left on device` failure occurred while building `libduckdb-sys` in the worktree-local `target/`.
- Recovery was done with `cargo clean` in the worktree and by reusing `CARGO_TARGET_DIR=/Users/ethanurbanski/arco/target` for the acceptance runs.
