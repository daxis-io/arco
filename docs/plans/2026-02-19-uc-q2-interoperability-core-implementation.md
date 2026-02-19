# UC Q2 Interoperability Core Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Deliver UC Q2 interoperability core (tenant-scoped object CRUD, coordinator-backed delta commits, cross-engine smoke coverage, and standardized UC error/request-id behavior).

**Architecture:** Keep the UC facade mounted behind `unity_catalog.enabled` and preserve scoped storage boundaries by tenant/workspace. Replace preview object-level 501 handlers with real read/delete implementations over scoped UC preview state, and route UC delta commits through `arco-delta` coordinator semantics with deterministic idempotent staging. Keep GCS-first credential behavior unchanged and add smoke suites that can run locally (ignored/gated) and be explicitly invoked in CI/docs.

**Tech Stack:** Rust (`axum`, `tokio`, `serde`, `arco-delta`), workspace integration tests, optional Python/PySpark smoke scripts.

---

### Task 1: Add failing UC object CRUD tests (GET/DELETE + scoping)

**Files:**
- Modify: `crates/arco-uc/tests/preview_crud.rs`
- Modify: `crates/arco-api/src/server.rs`

**Step 1: Write the failing test**

Add tests for:
- `GET /catalogs/{name}`, `GET /schemas/{full_name}`, `GET /tables/{full_name}` success/not-found.
- `DELETE /catalogs/{name}`, `DELETE /schemas/{full_name}`, `DELETE /tables/{full_name}` success/not-found.
- cross-tenant/workspace non-leakage for get/delete.
- deny-by-default (missing tenant/workspace headers).

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-uc --test preview_crud`
Expected: fail on 501/not-implemented paths and/or missing handlers.

**Step 3: Write minimal implementation**

No production code in this task.

**Step 4: Run test to verify it passes**

Deferred until Tasks 2-4.

**Step 5: Commit**

Deferred to final single commit required by request.

### Task 2: Implement catalog GET/DELETE subset

**Files:**
- Modify: `crates/arco-uc/src/routes/catalogs.rs`
- Modify: `crates/arco-uc/src/routes/preview.rs`

**Step 1: Write the failing test**

Use Task 1 tests targeting catalog get/delete behavior.

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-uc --test preview_crud create_and_get_catalog` (or exact new test name)
Expected: fail due 501/unimplemented.

**Step 3: Write minimal implementation**

Implement:
- `GET /catalogs/:name` by reading scoped object.
- `DELETE /catalogs/:name` with not-found and non-empty conflict behavior (support `force` delete path for children cleanup).

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-uc --test preview_crud`
Expected: new catalog tests pass.

**Step 5: Commit**

Deferred to final single commit.

### Task 3: Implement schema GET/DELETE subset

**Files:**
- Modify: `crates/arco-uc/src/routes/schemas.rs`
- Modify: `crates/arco-uc/src/routes/preview.rs`

**Step 1: Write the failing test**

Use Task 1 tests targeting schema get/delete behavior and isolation.

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-uc --test preview_crud`
Expected: failing schema object tests.

**Step 3: Write minimal implementation**

Implement:
- `GET /schemas/:full_name` from scoped preview state.
- `DELETE /schemas/:full_name` with not-found and child-table conflict/force cleanup behavior.

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-uc --test preview_crud`
Expected: schema tests pass.

**Step 5: Commit**

Deferred to final single commit.

### Task 4: Implement table GET/DELETE subset

**Files:**
- Modify: `crates/arco-uc/src/routes/tables.rs`
- Modify: `crates/arco-uc/src/routes/preview.rs`

**Step 1: Write the failing test**

Use Task 1 tests targeting table get/delete behavior and isolation.

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-uc --test preview_crud`
Expected: failing table delete and/or inconsistent get behavior.

**Step 3: Write minimal implementation**

Implement:
- `GET /tables/:full_name` via scoped preview table object.
- `DELETE /tables/:full_name` with not-found semantics.

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-uc --test preview_crud`
Expected: table object tests pass.

**Step 5: Commit**

Deferred to final single commit.

### Task 5: Add failing UC delta commit coordinator-semantics tests

**Files:**
- Add: `crates/arco-uc/tests/delta_commit_coordinator_semantics.rs`

**Step 1: Write the failing test**

Add tests for UC `GET|POST /delta/preview/commits`:
- idempotency replay correctness.
- deterministic stale-read/conflict rejection.
- inflight recovery behavior (expired inflight from coordinator state + conflict on unrelated key).

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-uc --test delta_commit_coordinator_semantics`
Expected: failures against preview state implementation.

**Step 3: Write minimal implementation**

Deferred to Task 6.

**Step 4: Run test to verify it passes**

Deferred until Task 6.

**Step 5: Commit**

Deferred to final single commit.

### Task 6: Map UC delta commit routes to `arco-delta` coordinator semantics

**Files:**
- Modify: `crates/arco-uc/src/routes/delta_commits.rs`
- Modify: `crates/arco-uc/src/error.rs` (if needed for standardized shape)
- Modify: `crates/arco-uc/tests/discovery_endpoints.rs` (align expectations if required)

**Step 1: Write the failing test**

Use Task 5 tests plus request-id/error-shape assertions.

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-uc --test delta_commit_coordinator_semantics`
Expected: failures until coordinator-backed semantics are live.

**Step 3: Write minimal implementation**

Implement:
- deterministic staging for UC commits keyed by `Idempotency-Key`.
- commit via `arco_delta::DeltaCommitCoordinator` (`read_version = commit_info.version - 1`).
- conflict handling consistent with coordinator stale-read/idempotency behavior.
- GET commit listing from committed delta logs with start/end and backfill watermark filtering.
- standardized UC error mapping and ensure `x-request-id` echo remains on all responses.

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-uc --test delta_commit_coordinator_semantics`
Expected: all coordinator semantics tests pass.

**Step 5: Commit**

Deferred to final single commit.

### Task 7: Add Spark + delta-rs + PySpark smoke suites and CI invocation hooks

**Files:**
- Add: `crates/arco-integration-tests/tests/delta_engine_smoke.rs`
- Add: `tools/smoke/uc_arco_engine_smoke.py`
- Add: `tools/smoke/README.md`
- Modify: `.github/workflows/ci.yml`

**Step 1: Write the failing test**

Create ignored/gated smoke tests that verify discover/read/commit flow for:
- UC facade endpoint family.
- Arco-native Delta endpoints.

**Step 2: Run test to verify it fails**

Run explicit smoke commands (documented in runbook) before implementation.

**Step 3: Write minimal implementation**

Provide:
- rust integration smoke harness for core flow,
- Python entrypoint capable of driving delta-rs and PySpark smoke path when env/tooling is present,
- explicit CI command blocks (ignored/gated) so invocation is visible and deterministic.

**Step 4: Run test to verify it passes**

Run documented smoke commands locally and capture pass/fail evidence.

**Step 5: Commit**

Deferred to final single commit.

### Task 8: Update inventory/support docs and complete full verification gate

**Files:**
- Modify: `docs/plans/2026-02-04-unity-catalog-openapi-inventory.md`
- Modify: `docs/plans/2026-02-19-unity-catalog-support-matrix.md`
- Add/Modify: smoke runbook in `tools/smoke/README.md` or `docs/runbooks/...`

**Step 1: Write the failing test**

N/A (docs + generated inventory checks).

**Step 2: Run test to verify it fails**

Run:
- `cargo xtask uc-openapi-inventory`
- `cargo xtask parity-matrix-check`

Expected: fails if drift exists.

**Step 3: Write minimal implementation**

Update docs to reflect Q2 status/evidence and regenerate inventory.

**Step 4: Run test to verify it passes**

Run required verification set:
- `cargo test -p arco-uc`
- `cargo test -p arco-uc --test openapi_compliance`
- `cargo test -p arco-api test_unity_catalog_`
- `cargo test -p arco-delta`
- smoke-suite commands (Spark, delta-rs, PySpark)
- `cargo xtask uc-openapi-inventory`
- `cargo xtask parity-matrix-check`

**Step 5: Commit**

Create one commit:
`uc: deliver q2 interoperability core (crud + delta commits + engine smokes)`
