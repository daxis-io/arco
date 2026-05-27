# Worker Protocol Contract Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Publish a canonical, versioned worker protocol contract for embedded deployments without breaking existing snake_case dispatch readers.

**Architecture:** Add a small `arco-worker-contract` crate for serde/OpenAPI-facing protocol types and helper IDs, backed by protobuf definitions under `arco.orchestration.v1`. `arco-flow`, `arco-api`, and the Python worker should consume that surface instead of redefining worker callback and dispatch shapes.

**Tech Stack:** Rust 2024, serde, utoipa, prost/tonic generated protobufs, axum, Python worker client tests.

---

### Task 1: Contract Crate, Proto, and Golden Fixtures

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/README.md`
- Modify: `crates/arco-proto/build.rs`
- Modify: `crates/arco-proto/src/lib.rs`
- Modify: `proto/arco/orchestration/v1/orchestration.proto`
- Create: `crates/arco-worker-contract/Cargo.toml`
- Create: `crates/arco-worker-contract/src/lib.rs`
- Create: `crates/arco-worker-contract/tests/contract_roundtrip.rs`
- Create: `crates/arco-worker-contract/tests/fixtures/worker_protocol/*.json`
- Create: `crates/arco-proto/tests/worker_protocol_contract_tests.rs`
- Create: `crates/arco-proto/tests/worker_protocol_golden_json.rs`
- Create: `crates/arco-proto/fixtures/worker_protocol/*.json`

**Step 1: Write failing contract tests**

Add tests that require:
- `WorkerDispatchEnvelope` serializes canonical camelCase fields including `taskId`.
- The same type deserializes legacy snake_case dispatch JSON, including optional `execution_location_id`.
- `callback_task_id(run_id, task_key)` round-trips through `parse_callback_task_id`.
- Heartbeat progress rejects values greater than 100.
- Golden JSON fixtures match runtime serde output.
- Generated proto worker messages round-trip through the same fixtures.

Run: `cargo test -p arco-worker-contract`
Expected: FAIL because the crate does not exist.

Run: `cargo test -p arco-proto --test worker_protocol_contract_tests --test worker_protocol_golden_json`
Expected: FAIL because worker protocol proto types do not exist.

**Step 2: Implement the minimal contract surface**

Create `arco-worker-contract` with documented public types:
- `WorkerDispatchEnvelope`
- `TaskStartedRequest` / `TaskStartedResponse`
- `HeartbeatRequest` / `HeartbeatResponse`
- `TaskCompletedRequest` / `TaskCompletedResponse`
- `WorkerOutcome`, `TaskOutputVisibilityState`, `TaskOutput`, `TaskError`, `ErrorCategory`, `TaskMetrics`, `CallbackErrorResponse`
- `callback_task_id`, `parse_callback_task_id`, `deterministic_attempt_id`

Use canonical camelCase serialization for new writes while accepting legacy snake_case aliases for dispatch-envelope reads. Keep `execution_location_id` as an optional compatibility field.

**Step 3: Add proto worker messages**

Extend `proto/arco/orchestration/v1/orchestration.proto` additively with worker protocol messages. Configure serde derives for generated worker messages in `crates/arco-proto/build.rs`.

**Step 4: Verify the contract layer**

Run: `cargo test -p arco-worker-contract`
Expected: PASS.

Run: `cargo test -p arco-proto --test worker_protocol_contract_tests --test worker_protocol_golden_json`
Expected: PASS.

### Task 2: Flow Dispatch Uses the Published Contract

**Files:**
- Modify: `crates/arco-flow/Cargo.toml`
- Modify: `crates/arco-flow/src/orchestration/worker_contract.rs`
- Modify: `crates/arco-flow/src/orchestration/callbacks/types.rs`
- Modify: `crates/arco-flow/src/orchestration/ids.rs`
- Modify: `crates/arco-flow/src/bin/arco_flow_dispatcher.rs`
- Modify: `crates/arco-flow/src/bin/arco_flow_sweeper.rs`
- Modify: `crates/arco-flow/tests/worker_dispatch_envelope_tests.rs`

**Step 1: Write failing flow tests**

Update worker dispatch tests to prove:
- Flow re-exports the contract crate envelope.
- Current legacy snake_case fixture still deserializes.
- Canonical dispatch JSON contains `taskId` and `taskKey`.
- `task_token` is minted for the opaque callback task id.

Run: `cargo test -p arco-flow --test worker_dispatch_envelope_tests`
Expected: FAIL because flow still owns the old envelope and dispatch token identity.

**Step 2: Wire flow to the contract crate**

Replace local callback type definitions with re-exports from `arco-worker-contract`. Keep `arco_flow::orchestration::worker_contract::WorkerDispatchEnvelope` as a compatibility re-export. Re-export deterministic ID helpers from the contract crate or delegate to one implementation to prevent drift.

Update dispatcher and sweeper to:
- calculate `callback_task_id(run_id, task_key)`;
- mint task tokens against that callback id;
- include `task_id` in the dispatch envelope;
- preserve `task_key` and optional `execution_location_id`.

**Step 3: Verify flow**

Run: `cargo test -p arco-flow --test worker_dispatch_envelope_tests`
Expected: PASS.

### Task 3: API Callback Routes Use the Published Contract and Opaque IDs

**Files:**
- Modify: `crates/arco-api/Cargo.toml`
- Modify: `crates/arco-api/src/routes/tasks.rs`
- Modify: `crates/arco-api/src/openapi.rs`
- Modify: `crates/arco-api/tests/task_token_contract_tests.rs`
- Modify: `crates/arco-api/tests/openapi_orchestration_routes.rs`

**Step 1: Write failing API tests**

Add tests that require:
- OpenAPI callback schemas come from the worker contract types.
- `ParquetTaskStateLookup` resolves canonical opaque callback task ids to `(run_id, task_key)`.
- Legacy task-key callback paths still work when unambiguous.
- Legacy task-key callback paths fail with an ambiguity error when multiple runs share the same task key.
- Task token validation succeeds when the token claim uses the opaque callback task id.

Run: `cargo test -p arco-api --test task_token_contract_tests --test openapi_orchestration_routes`
Expected: FAIL until route DTOs and lookup are updated.

**Step 2: Replace duplicated API DTOs**

Import the contract crate callback types directly and remove the API-to-flow conversion layer where the types now match. Keep route names and response bodies stable.

**Step 3: Add callback task id resolution**

Parse canonical callback ids first. If parsing succeeds, find the exact `(run_id, task_key)` row. If parsing fails, treat the path segment as a legacy `task_key` and only accept it when exactly one matching row exists.

**Step 4: Verify API**

Run: `cargo test -p arco-api --test task_token_contract_tests --test openapi_orchestration_routes`
Expected: PASS.

### Task 4: Python Worker Reads Canonical Dispatch and Emits Canonical Callback Paths

**Files:**
- Modify: `python/arco/src/arco_flow/client.py`
- Modify: `python/arco/src/arco_flow/worker/server.py`
- Modify: `python/arco/tests/unit/test_worker_dispatch_envelope.py`
- Modify: `python/arco/tests/unit/test_worker_callback_token.py`
- Create: `python/arco/tests/unit/test_client_worker_callbacks.py`
- Create: `python/arco/tests/unit/test_worker_heartbeat_contract.py`

**Step 1: Write failing Python tests**

Add tests that require:
- Worker dispatch parsing accepts canonical camelCase with `taskId`.
- Legacy snake_case dispatch still parses.
- Worker callbacks use `task_id` for callback paths and retain `task_key` for asset execution.
- Heartbeat client treats an empty successful response body as implicit continue.
- Completed callbacks omit `output` when no output exists and still accept legacy downstream payload expectations.

Run: `PYTHONPATH=python/arco/src uv run pytest python/arco/tests/unit/test_worker_dispatch_envelope.py python/arco/tests/unit/test_worker_callback_token.py python/arco/tests/unit/test_client_worker_callbacks.py python/arco/tests/unit/test_worker_heartbeat_contract.py -q`
Expected: FAIL until client and worker are updated.

**Step 2: Update Python client and worker**

Add `task_id` to `WorkerDispatchEnvelope`. Use it for callback URLs, falling back to `task_key` only for legacy dispatches. Add heartbeat callback support with canonical JSON response handling and empty-body compatibility.

**Step 3: Verify Python**

Run: `PYTHONPATH=python/arco/src uv run pytest python/arco/tests/unit/test_worker_dispatch_envelope.py python/arco/tests/unit/test_worker_callback_token.py python/arco/tests/unit/test_client_worker_callbacks.py python/arco/tests/unit/test_worker_heartbeat_contract.py -q`
Expected: PASS.

### Task 5: Documentation and Final Verification

**Files:**
- Modify: `docs/adr/adr-023-worker-contract.md`
- Modify: `docs/runbooks/split-services-cutover.md`
- Modify: `docs/runbooks/split-services-topology.md`
- Modify: `crates/arco-api/openapi.json` if regenerated by the repo workflow

**Step 1: Update docs**

Document:
- `arco-worker-contract` as the canonical Rust/OpenAPI protocol surface.
- `arco.orchestration.v1` worker proto messages as the language-neutral schema.
- Canonical camelCase writes plus legacy snake_case dispatch read compatibility.
- Opaque `taskId` callback identity and legacy task-key fallback.
- Required task callback auth header.
- Heartbeat empty-body compatibility, if client-side only.

**Step 2: Run focused verification**

Run:
- `cargo test -p arco-worker-contract`
- `cargo test -p arco-proto --test worker_protocol_contract_tests --test worker_protocol_golden_json`
- `cargo test -p arco-flow --test worker_dispatch_envelope_tests`
- `cargo test -p arco-api --test task_token_contract_tests --test openapi_orchestration_routes`
- `PYTHONPATH=python/arco/src uv run pytest python/arco/tests/unit/test_worker_dispatch_envelope.py python/arco/tests/unit/test_worker_callback_token.py python/arco/tests/unit/test_client_worker_callbacks.py python/arco/tests/unit/test_worker_heartbeat_contract.py -q`
- `cargo fmt --all --check`
- `git diff --check`

Expected: all pass.
