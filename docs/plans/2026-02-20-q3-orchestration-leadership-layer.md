# Q3 Orchestration Leadership Layer (Lineage + Diagnostics + Parity Consistency)

## Goal
Complete Q3 by making orchestration outcomes cross-verifiable from run execution to catalog projections, and by exposing deterministic operator diagnostics for reruns, retries/skips, and run-key conflicts.

## Delivered Scope

### 1. Schedule/Sensor/Backfill/Reexecution consistency against parity gates
- Expanded parity gate coverage in `crates/arco-flow/tests/orchestration_parity_gates_m2.rs` to assert consistent `run_key` behavior across schedule, sensor, backfill, and manual reexecution paths.
- Expanded parity gate coverage in `crates/arco-api/tests/orchestration_parity_gates_m1.rs` for deterministic rerun diagnostics and run-key conflict introspection.
- Expanded parity gate coverage in `crates/arco-flow/tests/orchestration_parity_gates_m3.rs` for execution lineage linkage into projection state.

### 2. Execution-to-catalog lineage linkage by Delta version + partition
- Added deterministic execution metadata extraction from task success output in:
  - `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Added explicit projection fields in task and partition status rows:
  - `execution_lineage_ref`
  - `delta_table`
  - `delta_version`
  - `delta_partition`
- Persisted/loaded these fields through parquet schemas in:
  - `crates/arco-flow/src/orchestration/compactor/parquet_util.rs`
- Propagated lineage/delta metadata into API read surfaces (`/runs/{id}`, `/partitions`) in:
  - `crates/arco-api/src/routes/orchestration.rs`

### 3. Operator diagnostics
- Deterministic rerun reasons returned on rerun responses and run reads.
- Retry attribution and skip attribution exposed per task summary in run reads.
- Run-key conflict introspection payloads standardized via API error `details`:
  - `conflictType`
  - `runKey`
  - `existingFingerprint`
  - `requestedFingerprint`
  - `existingRunId`/`existingPlanId`/`existingCreatedAt`

### 4. Parity evidence strictness
- Updated parity matrix entries to ensure all Implemented claims include:
  - concrete code references
  - CI-gated test references
  - explicit CI command anchors in `.github/workflows/ci.yml`
- Validation continues to be enforced by `cargo xtask parity-matrix-check`.

## Determinism + Idempotency Guarantees
- Execution lineage refs are deterministic for a successful materialization event and include stable identifiers required for cross-verification.
- Existing idempotency behavior is preserved by retaining global event idempotency gates and run-key reservation semantics.
- Tenant/workspace/user scoping remains unchanged in route handlers and storage resolution.

## Verification Commands
- `cargo test -p arco-api --all-features --test orchestration_parity_gates_m1`
- `cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m1`
- `cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m2`
- `cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m3`
- `cargo test -p arco-api --all-features --test orchestration_schedule_e2e_tests`
- `cargo test -p arco-api --all-features --test orchestration_api_tests`
- `cargo xtask parity-matrix-check`

## Q3 Exit Criteria Mapping
- Core orchestration flows are cross-verifiable from run events to catalog partition status and task projections.
- Lineage linkage includes Delta version + partition.
- Diagnostics are API-visible and CI-tested.
- Required verification command suite is run and passing.

## Closeout Status (2026-02-21)
- [x] Schedule/sensor/backfill/reexecution parity behavior remains CI-gated and green.
- [x] Execution-to-catalog lineage linkage by Delta version + partition is present on run and partition read surfaces.
- [x] Operator diagnostics include deterministic rerun reason, retry attribution, skip attribution, and run_key conflict introspection payloads.
- [x] Parity evidence policy remains enforced by `cargo xtask parity-matrix-check`.
- [x] Q3 verification suite evidence captured in `2026-02-21-q3-verification-evidence.md`.

No open Q3 code gaps remain under the Q3 scope defined in this roadmap.
