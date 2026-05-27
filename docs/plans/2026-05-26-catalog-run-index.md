# Catalog Run Index Implementation Plan

**Goal:** Add an Arco-owned catalog run projection so org-scoped catalog readers can consume authoritative run metadata without scanning canonical run objects.

**Architecture:** Keep the orchestration event ledger and canonical run/task state as the source of truth. Derive a versioned `catalog_run_index` projection inside the orchestration fold and publish it through the existing immutable Parquet + manifest pointer boundary. The first slice uses tenant id as the Arco org scope, writes org-scoped manifest artifacts, and records enough asset/output metadata for catalog and lineage readers, while leaving downstream JSON-prefix migration as a documented backfill integration point.

**Tech Stack:** Rust, `arco-flow` orchestration compactor, Arrow/Parquet schemas, manifest table artifacts, existing compactor tests and golden schema tests.

---

### Task 1: Add Projection Row and Fold Derivation

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Test: `crates/arco-flow/src/orchestration/compactor/fold.rs`

**Step 1: Write failing lifecycle tests**

Add tests that fold `RunTriggered`, `PlanCreated`, `DispatchRequested`, `TaskStarted`, `TaskHeartbeat`, `TaskFinished`, `TaskOutputVisibilityChanged`, and `RunCancelRequested`, then assert `FoldState.catalog_run_index` contains rows keyed by `(org_id, workspace_id, run_id, task_key)`.

Expected assertions:
- `org_id` equals event `tenant_id`.
- `workspace_id`, `run_id`, `plan_id`, `run_status`, `task_status`, `run_key`, labels, timestamps, `asset_key`, `target_namespace`, `target_table`, `source_type`, and output metadata are populated from canonical fold state.
- Stale attempt completions and stale output visibility changes do not regress index rows.
- Retry/heartbeat/cancel/failure transitions update `row_version` and status consistently with existing task/run rows.

**Step 2: Run tests to verify RED**

Run: `cargo test -p arco-flow catalog_run_index --lib -- --nocapture`

Expected: FAIL because `catalog_run_index` does not exist.

**Step 3: Implement projection row and update hooks**

Add:
- `CatalogRunIndexRow` struct with schema-versioned catalog fields.
- `FoldState.catalog_run_index: HashMap<(String, String, String, String), CatalogRunIndexRow>`.
- A small `refresh_catalog_run_index_for_run(run_id, event_id)` helper called after run/task lifecycle updates.
- Asset parsing helper for `target_namespace` / `target_table` from canonical `asset_key`.

Do not add a new source of truth. Every row must be derived from existing `runs` and `tasks`.

**Step 4: Run tests to verify GREEN**

Run: `cargo test -p arco-flow catalog_run_index --lib -- --nocapture`

Expected: PASS.

### Task 2: Add Parquet Schema, Roundtrip, and Golden Contract

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/parquet_util.rs`
- Modify: `crates/arco-flow/tests/orchestration_schema_contracts.rs`
- Create: `crates/arco-flow/tests/golden_schemas/orchestration/catalog_run_index.schema.json`

**Step 1: Write failing schema and roundtrip tests**

Add a `catalog_run_index` schema contract entry and a parquet roundtrip test that includes nullable output fields and non-null org/run/task identifiers.

**Step 2: Run tests to verify RED**

Run: `cargo test -p arco-flow orchestration_schema_contracts catalog_run_index -- --nocapture`

Expected: FAIL because the schema accessor, writer, reader, and golden file do not exist.

**Step 3: Implement parquet read/write**

Add:
- `catalog_run_index_schema()`.
- `catalog_run_index_parquet_schema()`.
- `write_catalog_run_index(&[CatalogRunIndexRow])`.
- `read_catalog_run_index(&Bytes)`.

Keep new columns nullable unless required for the org-scoped identity contract.

**Step 4: Run tests to verify GREEN**

Run: `cargo test -p arco-flow orchestration_schema_contracts catalog_run_index -- --nocapture`

Expected: PASS.

### Task 3: Publish Projection Through Manifested State

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/manifest.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/service.rs`
- Test: `crates/arco-flow/src/orchestration/compactor/service.rs`

**Step 1: Write failing compactor publication tests**

Add tests proving:
- Delta compaction writes org-scoped `catalog_run_index` artifacts when run/task metadata changes.
- Base snapshot merge preserves the projection.
- Loading current state restores `catalog_run_index`.
- Row counts include the projection.

**Step 2: Run tests to verify RED**

Run: `cargo test -p arco-flow compact_creates_l0_delta_with_state --lib -- --nocapture`

Expected: FAIL because manifest/service do not publish or load the projection table.

**Step 3: Wire manifest and service**

Add:
- `TablePaths.catalog_run_index_by_org`.
- `RowCounts.catalog_run_index`.
- `TablePaths::all()` support.
- Snapshot/delta read/write support in `MicroCompactor`.
- `merge_states`, `delta_from_states`, and `delta_state_is_empty` support.

**Step 4: Run tests to verify GREEN**

Run:
- `cargo test -p arco-flow compact_creates_l0_delta_with_state --lib -- --nocapture`
- `cargo test -p arco-flow fold_state_survives_roundtrip_through_parquet --lib -- --nocapture`
- `cargo test -p arco-flow compact_writes_delta_rows_only --lib -- --nocapture`

Expected: PASS.

### Task 4: Document Contract and Migration Boundary

**Files:**
- Modify: `docs/guide/src/reference/orchestration-product-contract.md`
- Modify or create: `docs/guide/src/reference/catalog-run-index.md`
- Modify: `docs/guide/src/SUMMARY.md`

**Step 1: Write documentation update**

Document:
- The projection is derived, not canonical.
- Org scope maps to Arco tenant id in this first contract.
- Catalog readers should consume the manifest-published projection instead of listing canonical run objects.
- Existing downstream `authoritative-runs/runs/` or `shadow-runs/runs/` objects need a one-time backfill/replay into the Arco ledger/projection boundary.
- Schema versioning and nullable-column compatibility rules.

**Step 2: Verify docs**

Run: `mdbook build docs/guide`

Expected: PASS.

### Task 5: Final Verification and Review

**Files:**
- All modified files.

**Step 1: Run focused verification**

Run:
- `cargo fmt --all --check`
- `git diff --check`
- `cargo test -p arco-flow catalog_run_index --lib -- --nocapture`
- `cargo test -p arco-flow --test orchestration_schema_contracts -- --nocapture`
- `cargo test -p arco-flow compact_creates_l0_delta_with_state --lib -- --nocapture`
- `cargo test -p arco-flow fold_state_survives_roundtrip_through_parquet --lib -- --nocapture`
- `cargo test -p arco-flow compact_writes_delta_rows_only --lib -- --nocapture`
- `mdbook build docs/guide`

**Step 2: Request final code review**

Review the complete diff against issue #132 acceptance criteria:
- No canonical source replacement.
- Projection remains derived from fold state.
- Org-scoped identity is explicit.
- Lifecycle tests cover create/start/heartbeat/completion/retry/cancel/failure/output visibility.
- Schema/versioning and migration docs exist.

**Step 3: Address review findings**

Fix any critical or important findings, rerun focused verification, and report exact evidence.
