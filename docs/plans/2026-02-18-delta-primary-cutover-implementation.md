# Delta-Primary Lake Format Cutover Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ship a single release-train cutover that makes Delta the default table format while preserving Iceberg compatibility and legacy parquet fallback behavior.

**Architecture:** Add a canonical table-format contract in `arco-core`, enforce canonical format writes and defaulting in catalog/API write paths, and harden Delta commit routing to verify table metadata and derive `_delta_log` from table location. Keep UC facade parity checks pinned to upstream OpenAPI and update policy/docs/audit evidence for Delta-primary operation.

**Tech Stack:** Rust (axum + workspace crates), Python SDK dataclasses/pytest, OpenAPI fixture tooling (`xtask`).

---

### Task 1: Core table-format contract + Delta path helpers

**Files:**
- Create: `crates/arco-core/src/table_format.rs`
- Create: `crates/arco-core/src/flow_paths.rs`
- Create: `crates/arco-core/tests/table_format_contracts.rs`
- Create: `crates/arco-core/tests/flow_paths_contracts.rs`
- Modify: `crates/arco-core/src/lib.rs`

**Step 1: Write failing contract tests**
- Add tests for case-insensitive parse, lowercase persistence, `None => parquet` effective fallback.
- Add tests for `DeltaPaths` staging/coordinator/idempotency and `_delta_log/{version:020}.json` derivation from table root.

**Step 2: Run targeted failing tests**
- Run: `cargo test -p arco-core --test table_format_contracts`
- Run: `cargo test -p arco-core --test flow_paths_contracts`
- Expected: compile/runtime failures before implementation.

**Step 3: Implement minimal core contract**
- Implement `TableFormat` enum + parse/normalize/effective helpers.
- Implement `DeltaPaths` typed path builder API.
- Export modules from `lib.rs`.

**Step 4: Re-run tests to green**
- Run both targeted tests; expect PASS.

### Task 2: Catalog + API default/validation wiring

**Files:**
- Modify: `crates/arco-api/src/routes/tables.rs`
- Modify: `crates/arco-api/src/routes/catalogs.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Modify: `crates/arco-catalog/src/parquet_util.rs` (comments only if needed)
- Modify: `crates/arco-api/tests/api_integration.rs`

**Step 1: Add failing tests**
- API test: table creation without `format` returns/persists `"delta"`.
- API test: schema table route canonicalizes case and rejects unknown formats with 400.
- Catalog writer tests: new writes persist canonical lowercase and reject invalid format.

**Step 2: Run failing tests**
- Run targeted tests by name in `arco-api` and `arco-catalog`.

**Step 3: Implement validation/defaults**
- Namespace route request includes optional `format`; omit defaults to Delta.
- Schema route defaults omitted format to Delta, canonicalizes via `TableFormat`, rejects unknown.
- Catalog writer canonicalizes and stores explicit format for new writes.

**Step 4: Re-run package tests**
- `cargo test -p arco-catalog`
- `cargo test -p arco-api`

### Task 3: Delta commit gating and location-aware `_delta_log`

**Files:**
- Modify: `crates/arco-api/src/routes/delta.rs`
- Modify: `crates/arco-catalog/src/reader.rs` (table lookup by id)
- Modify: `crates/arco-delta/src/coordinator.rs`
- Modify: `crates/arco-delta/src/types.rs` (if needed)
- Modify: `crates/arco-integration-tests/tests/delta_commit_coordinator.rs`

**Step 1: Add failing tests**
- Integration: commit rejected when table missing.
- Integration: commit rejected when table effective format != delta.
- Integration: delta log path resolves from table location root, not `tables/{table_id}`.

**Step 2: Run failing integration test**
- `cargo test -p arco-integration-tests --test delta_commit_coordinator`

**Step 3: Implement API + coordinator updates**
- Route loads table by ID, validates existence and effective `delta` format.
- Resolve table root from location (fallback deterministic path when location missing).
- Coordinator uses `DeltaPaths` instead of hardcoded literals.

**Step 4: Verify delta crate + integration**
- `cargo test -p arco-delta`
- `cargo test -p arco-integration-tests --test delta_commit_coordinator`

### Task 4: UC parity fixture + compliance hardening

**Files:**
- Modify: `crates/arco-uc/tests/fixtures/unitycatalog-openapi.yaml`
- Modify: `crates/arco-uc/tests/openapi_compliance.rs`
- Modify: `docs/plans/2026-02-04-unity-catalog-openapi-inventory.md`
- Modify: `tools/xtask/src/main.rs` (only if fixture parsing/inventory needs fixes)

**Step 1: Add/adjust failing test expectations**
- Unignore UC compliance test.
- Ensure fixture-pinned guard validates commit hash header.

**Step 2: Replace placeholder fixture**
- Vendor pinned upstream `api/all.yaml` with commit hash in header.

**Step 3: Generate inventory**
- `cargo xtask uc-openapi-inventory`

**Step 4: Validate UC tests**
- `cargo test -p arco-uc --tests`
- `cargo test -p arco-uc --test openapi_compliance -- --ignored`

### Task 5: Python SDK default update

**Files:**
- Modify: `python/arco/src/arco_flow/types/asset.py`
- Modify: related Python unit tests under `python/arco/tests/unit`

**Step 1: Write failing test**
- Assert default `IoConfig().format == "delta"` and decorator default manifests as delta.

**Step 2: Run failing test**
- `pytest python/arco/tests/unit/test_decorator.py -k io`

**Step 3: Implement default switch**
- Change `IoConfig.format` default from parquet to delta.

**Step 4: Re-run targeted Python tests**
- `pytest python/arco/tests/unit/test_asset_types.py python/arco/tests/unit/test_decorator.py python/arco/tests/unit/test_manifest_model.py`

### Task 6: Policy/docs/audit evidence updates

**Files:**
- Create: `docs/runbooks/lake-format-policy.md`
- Modify: `docs/adr/adr-030-delta-uc-metastore.md`
- Modify: `docs/adr/adr-031-unity-catalog-api-facade.md`
- Modify: `docs/runbooks/pen-test-scope.md`
- Modify: `docs/audits/2026-02-12-prod-readiness/signal-ledger.md`
- Modify: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json`
- Modify: `docs/audits/2026-02-12-prod-readiness/summary.md`
- Create/modify under: `release_evidence/`

**Step 1: Document policy and acceptance status**
- Delta default-primary, Iceberg secondary-supported, no net-new Iceberg feature scope.

**Step 2: Wire audit gates and evidence placeholders**
- Add Delta-primary signal set + required artifact references.

**Step 3: Validate docs formatting/build as available**
- Run relevant doc checks from gate tracker.

### Task 7: End-to-end verification matrix

**Step 1: Run targeted matrix**
1. `cargo test -p arco-core --test table_format_contracts`
2. `cargo test -p arco-core --test flow_paths_contracts`
3. `cargo test -p arco-catalog`
4. `cargo test -p arco-api`
5. `cargo test -p arco-delta`
6. `cargo test -p arco-integration-tests --test delta_commit_coordinator`
7. `cargo test -p arco-uc --tests`
8. `cargo xtask uc-openapi-inventory`
9. `cargo test -p arco-uc --test openapi_compliance -- --ignored`

**Step 2: Run broader regression commands from gate tracker as time allows**
- fmt, clippy, workspace tests, docs, deny, buf.

**Step 3: Collect evidence**
- Record exact command outputs and link artifacts in audit/release evidence files.
