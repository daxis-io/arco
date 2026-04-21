# System Catalog Tables Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a tenant-visible, read-only `system` catalog for Arco that exposes the control-plane, lineage, and the Daxis-relevant runtime state users actually need for debugging, governance, scheduling, backfills, and run operations through SQL, then add a first-class visible publication history table as `system.catalog.commits`.

**Architecture:** Reuse the existing pointer-first snapshot model and the existing `/api/v1/query` DataFusion path instead of inventing a new serving stack. Introduce an API-local system-table registry that maps logical names like `system.catalog.namespaces`, `system.orchestration.partition_status`, and `system.orchestration.backfills` to manifest-selected Parquet artifacts, then extend the catalog snapshot writer to emit `commits.parquet` for visible Tier-1 head publications. Keep system tables derived, asynchronous, and read-only; they must never become the synchronous authz or correctness path.

**Tech Stack:** Rust, Axum, DataFusion, Arrow/Parquet, `arco-api`, `arco-catalog`, `arco-flow`, mdBook, `cargo test`.

---

## Scope

- Expose existing manifest-selected current-state tables as:
  - `system.catalog.catalogs`
  - `system.catalog.namespaces`
  - `system.catalog.tables`
  - `system.catalog.columns`
  - `system.lineage.edges`
  - `system.orchestration.runs`
  - `system.orchestration.tasks`
  - `system.orchestration.dep_satisfaction`
  - `system.orchestration.timers`
  - `system.orchestration.dispatch_outbox`
  - `system.orchestration.sensor_state`
  - `system.orchestration.sensor_evals`
  - `system.orchestration.partition_status`
  - `system.orchestration.schedule_definitions`
  - `system.orchestration.schedule_state`
  - `system.orchestration.schedule_ticks`
  - `system.orchestration.backfills`
  - `system.orchestration.backfill_chunks`
  - `system.orchestration.run_key_conflicts`
- Add a new catalog snapshot projection at `snapshots/catalog/vN/commits.parquet`.
- Expose that projection as `system.catalog.commits`.
- Document the architectural boundary: immutable ledger and pointer-published manifests remain truth; system tables are read-only operational projections.

## Prioritized Daxis-Relevant Follow-Ons

- Add `system.access.audit` for auth allow/deny and signed-URL mint decision history once audit events are projected into Parquet rather than tracing-only sinks.
- Add `system.access.auth_denies` and `system.access.url_mint_events` as curated views over the same audit projection so users can answer “why was I denied?” and “who minted data-path access?” without scanning raw event payloads.
- Add `system.query.history` once `/api/v1/query` and `/api/v1/query-data` executions emit a durable history projection.
- Add `system.storage.retention_policies`, `system.storage.gc_runs`, `system.storage.snapshot_inventory`, and `system.storage.reconciliation_issues` after GC/reconciler outputs are materialized as queryable state.
- Keep these tables in the plan because they are relevant to Daxis operators and tenants, but do not block the first execution tranche on inventing new telemetry domains.

## Non-Goals

- Do not add billing or generic compute tables in this plan. Those domains do not exist in the repo today and are lower priority for Arco’s execution-first/serverless posture.
- Do not add policy-evaluation, masking, classification, or grant-management system tables until those governance domains become authoritative runtime state in the repo.
- Do not use system tables in the synchronous enforcement path.
- Do not expose raw ledger files, manifest JSON, or `search.token_postings` as tenant-visible system tables by default.
- Do not add a new always-on metadata database.

## Implementation Notes

- Use `@test-driven-development` on every code task.
- Use `@verification-before-completion` before claiming the feature is done.
- Prefer small vertical slices over a giant “system catalog” branch.
- Keep system-table schemas additive-only per ADR-006.

## Verification Matrix

- `cargo test -p arco-api --test system_tables_api -- --nocapture`
- `cargo test -p arco-api --test api_integration test_query_returns_json_format -- --nocapture`
- `cargo test -p arco-catalog --test schema_contracts -- --nocapture`
- `cargo test -p arco-catalog --test protocol_invariants -- --nocapture`
- `cargo test -p arco-flow --test orchestration_schema_contracts -- --nocapture`
- `cargo test -p arco-flow --test orchestration_system_table_paths -- --nocapture`
- `cargo fmt --all --check`
- `cargo clippy -p arco-api -p arco-catalog -p arco-flow --tests -- -D warnings`
- `cargo xtask adr-check`
- `cd docs/guide && mdbook build`

## Acceptance Criteria

- `POST /api/v1/query` can resolve the planned `system.catalog.*`, `system.lineage.*`, and Daxis-relevant `system.orchestration.*` table names without scanning storage or reading the ledger.
- `system.catalog.commits` is populated from visible catalog publications, not from legacy `commits/catalog/{id}.json` receipts.
- The system-table surface is explicitly allowlisted and workspace-scoped.
- Raw search postings and raw internal object paths are not exposed as default tenant-visible system tables, and internal orchestration dedupe tables stay off the default user-facing surface unless explicitly promoted later.
- Docs explain the boundary between truth, serving indexes, and system tables.

### Task 1: Lock The Contract In Docs

**Files:**
- Create: `docs/adr/adr-035-system-catalog-tables.md`
- Modify: `docs/adr/README.md`
- Modify: `docs/guide/src/concepts/catalog.md`
- Modify: `docs/guide/src/concepts/architecture.md`
- Modify: `docs/guide/src/introduction.md`

**Step 1: Write the ADR**

Add an ADR with these sections:

```markdown
# ADR-035: System Catalog Tables

## Decision

Arco exposes a tenant-visible logical `system` catalog that contains read-only
operational tables derived from pointer-published snapshots and runtime
projections.

## Initial Tables

- `system.catalog.{catalogs,namespaces,tables,columns,commits}`
- `system.lineage.edges`
- `system.orchestration.{runs,tasks,dep_satisfaction,timers,dispatch_outbox,sensor_state,sensor_evals,partition_status,schedule_definitions,schedule_state,schedule_ticks,backfills,backfill_chunks,run_key_conflicts}`

## Boundaries

- Truth: immutable ledger + pointer-published manifests
- Serving path: manifest-selected Parquet artifacts
- Operations surface: read-only system tables
- Not exposed by default: raw ledger, raw manifests, raw search postings
```

**Step 2: Update guide docs**

Add one short paragraph to each guide page so the docs say:

- catalog reads stay pointer-first
- `/api/v1/query` is the initial SQL surface for system tables
- system tables are queryable projections, not the commit point

**Step 3: Run doc verification**

Run: `cargo xtask adr-check`
Expected: `SUCCESS`

Run: `cd docs/guide && mdbook build`
Expected: build completes without broken links

**Step 4: Commit**

```bash
git add docs/adr/adr-035-system-catalog-tables.md docs/adr/README.md docs/guide/src/concepts/catalog.md docs/guide/src/concepts/architecture.md docs/guide/src/introduction.md
git commit -m "docs: define system catalog tables contract"
```

### Task 2: Expose `system.catalog` And `system.lineage` On The Query Path

**Files:**
- Create: `crates/arco-api/src/system_tables.rs`
- Modify: `crates/arco-api/src/lib.rs`
- Modify: `crates/arco-api/src/routes/query.rs`
- Create: `crates/arco-api/tests/system_tables_api.rs`

**Step 1: Write the failing API tests**

Create `crates/arco-api/tests/system_tables_api.rs` with tests like:

```rust
#[tokio::test]
async fn query_can_select_from_system_catalog_namespaces() -> Result<()> {
    let router = seed_catalog(test_router()).await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT name FROM system.catalog.namespaces ORDER BY name"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn query_can_select_from_system_lineage_edges() -> Result<()> {
    let router = seed_catalog(test_router()).await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT count(*) AS edge_count FROM system.lineage.edges"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}
```

**Step 2: Run the tests to verify they fail**

Run: `cargo test -p arco-api --test system_tables_api query_can_select_from_system_catalog_namespaces -- --nocapture`
Expected: FAIL with a table-resolution error for `system.catalog.namespaces`

**Step 3: Add a system-table registry module**

In `crates/arco-api/src/system_tables.rs`, add a small registry instead of hard-coding more logic in `routes/query.rs`.

Start with shapes like:

```rust
pub(crate) struct SystemTableSpec {
    pub schema: &'static str,
    pub table: &'static str,
    pub path: &'static str,
}

pub(crate) async fn register_catalog_and_lineage_system_tables(
    session: &SessionContext,
    reader: &CatalogReader,
    storage: &ScopedStorage,
) -> Result<usize, ApiError> { /* ... */ }
```

Implementation rules:

- register a DataFusion catalog named `system`
- register schemas `catalog` and `lineage` using `MemoryCatalogProvider` and `MemorySchemaProvider`
- resolve current snapshot paths from `CatalogReader::get_mintable_paths(...)`
- only register an explicit allowlist:
  - `catalogs.parquet` -> `system.catalog.catalogs`
  - `namespaces.parquet` -> `system.catalog.namespaces`
  - `tables.parquet` -> `system.catalog.tables`
  - `columns.parquet` -> `system.catalog.columns`
  - `lineage_edges.parquet` -> `system.lineage.edges`
- do not auto-register every file returned by the manifest

**Step 4: Wire the registry into `/api/v1/query`**

Update `crates/arco-api/src/routes/query.rs` so query setup does both:

- existing domain registration (`catalog.*`, `lineage.*`, `search.*`)
- new logical system registration (`system.catalog.*`, `system.lineage.*`)

Keep the route read-only and keep the existing response formatting unchanged.

**Step 5: Run the focused tests**

Run: `cargo test -p arco-api --test system_tables_api -- --nocapture`
Expected: PASS

Run: `cargo test -p arco-api --test api_integration test_query_returns_json_format -- --nocapture`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/arco-api/src/system_tables.rs crates/arco-api/src/lib.rs crates/arco-api/src/routes/query.rs crates/arco-api/tests/system_tables_api.rs
git commit -m "feat: expose catalog and lineage system tables"
```

### Task 3: Expose `system.orchestration` From The Pointer-Selected Base Snapshot

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/service.rs`
- Create: `crates/arco-flow/tests/orchestration_system_table_paths.rs`
- Modify: `crates/arco-api/src/system_tables.rs`
- Modify: `crates/arco-api/tests/system_tables_api.rs`

**Step 1: Write the failing tests**

Add a flow-level helper test:

```rust
#[tokio::test]
async fn current_base_table_paths_returns_pointer_selected_tables() -> Result<()> {
    let compactor = seeded_compactor().await?;
    let paths = compactor.current_base_table_paths().await?;
    assert!(paths.runs.is_some());
    assert!(paths.tasks.is_some());
    Ok(())
}
```

Add an API test:

```rust
#[tokio::test]
async fn query_can_select_from_system_orchestration_runs() -> Result<()> {
    let router = seed_orchestration_router().await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT run_id FROM system.orchestration.runs ORDER BY run_id"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn query_can_select_from_system_orchestration_partition_status() -> Result<()> {
    let router = seed_orchestration_router().await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT asset_key, stale_reason_code FROM system.orchestration.partition_status ORDER BY asset_key"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}
```

**Step 2: Run the tests to verify they fail**

Run: `cargo test -p arco-flow --test orchestration_system_table_paths -- --nocapture`
Expected: FAIL because `MicroCompactor` has no public helper yet

**Step 3: Add a read helper on `MicroCompactor`**

In `crates/arco-flow/src/orchestration/compactor/service.rs`, add a public read helper that returns only the current visible base snapshot artifacts, not the ledger and not merged synthetic state.

Use a signature like:

```rust
pub async fn current_base_table_paths(&self) -> Result<TablePaths> {
    let manifest = self.load_current_manifest_only().await?;
    Ok(manifest.base_snapshot.tables.clone())
}
```

Do not implement this by calling `load_state()` and then discarding reconstructed rows. Add a manifest-only helper if needed, but keep the same contract:

- pointer-first
- no storage listing for correctness
- return only visible base snapshot artifacts

**Step 4: Register orchestration system tables**

Extend `crates/arco-api/src/system_tables.rs` to register:

- `system.orchestration.runs`
- `system.orchestration.tasks`
- `system.orchestration.dep_satisfaction`
- `system.orchestration.timers`
- `system.orchestration.dispatch_outbox`
- `system.orchestration.sensor_state`
- `system.orchestration.sensor_evals`
- `system.orchestration.partition_status`
- `system.orchestration.schedule_definitions`
- `system.orchestration.schedule_state`
- `system.orchestration.schedule_ticks`
- `system.orchestration.backfills`
- `system.orchestration.backfill_chunks`
- `system.orchestration.run_key_conflicts`

Use the `TablePaths` artifact paths from the current visible base snapshot. Do not register L0 files directly in DataFusion for v1.

**Step 5: Run focused tests**

Run: `cargo test -p arco-flow --test orchestration_system_table_paths -- --nocapture`
Expected: PASS

Run: `cargo test -p arco-api --test system_tables_api query_can_select_from_system_orchestration_runs -- --nocapture`
Expected: PASS

Run: `cargo test -p arco-api --test system_tables_api query_can_select_from_system_orchestration_partition_status -- --nocapture`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/arco-flow/src/orchestration/compactor/service.rs crates/arco-flow/tests/orchestration_system_table_paths.rs crates/arco-api/src/system_tables.rs crates/arco-api/tests/system_tables_api.rs
git commit -m "feat: expose orchestration system tables"
```

### Task 4: Materialize Visible Catalog Publications As `system.catalog.commits`

**Files:**
- Modify: `crates/arco-catalog/src/parquet_util.rs`
- Modify: `crates/arco-catalog/src/state.rs`
- Modify: `crates/arco-catalog/src/tier1_state.rs`
- Modify: `crates/arco-catalog/src/tier1_snapshot.rs`
- Modify: `crates/arco-catalog/src/tier1_compactor.rs`
- Modify: `crates/arco-catalog/tests/schema_contracts.rs`
- Modify: `crates/arco-catalog/tests/protocol_invariants.rs`
- Create: `crates/arco-catalog/tests/golden_schemas/commits.schema.json`
- Modify: `crates/arco-api/src/system_tables.rs`
- Modify: `crates/arco-api/tests/system_tables_api.rs`

**Step 1: Write the failing tests**

Add a schema contract test in `crates/arco-catalog/tests/schema_contracts.rs`:

```rust
#[test]
fn contract_commits_parquet_schema_backward_compatible() {
    let golden = load_golden_schema("commits");
    let current = schema_to_golden("commits", &arco_catalog::parquet_util::commit_schema());
    if let Err(msg) = is_backward_compatible(&golden, &current) {
        panic!("Commits schema is NOT backward compatible: {msg}");
    }
}
```

Add an API test:

```rust
#[tokio::test]
async fn query_can_select_from_system_catalog_commits() -> Result<()> {
    let router = seed_catalog(test_router()).await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT commit_ulid, snapshot_version FROM system.catalog.commits ORDER BY published_at DESC"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}
```

**Step 2: Run the tests to verify they fail**

Run: `cargo test -p arco-api --test system_tables_api query_can_select_from_system_catalog_commits -- --nocapture`
Expected: FAIL because `system.catalog.commits` is not registered

**Step 3: Add the commit projection schema**

In `crates/arco-catalog/src/parquet_util.rs`, add a `CatalogCommitRecord` and schema accessor:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogCommitRecord {
    pub commit_ulid: String,
    pub manifest_id: String,
    pub snapshot_version: i64,
    pub published_at: i64,
    pub fencing_token: i64,
    pub manifest_path: String,
    pub previous_manifest_path: Option<String>,
    pub watermark_event_id: Option<String>,
    pub operation: Option<String>,
    pub object_type: Option<String>,
    pub object_id: Option<String>,
    pub object_name: Option<String>,
}
```

Keep the initial schema intentionally small. `actor_id`, `request_id`, and policy/authz fields can be added later if the event envelope reliably carries them.

**Step 4: Thread commits through catalog snapshot state**

Update these files so the catalog snapshot can load and write `commits.parquet`:

- `crates/arco-catalog/src/state.rs`: add `pub commits: Vec<CatalogCommitRecord>`
- `crates/arco-catalog/src/tier1_state.rs`: read `snapshots/catalog/vN/commits.parquet` when present
- `crates/arco-catalog/src/tier1_snapshot.rs`: write `commits.parquet` and add it to `SnapshotInfo.files`

Use the file name `commits.parquet` under `snapshots/catalog/v{version}/`.

**Step 5: Populate commit rows in the Tier-1 compactor**

Update `crates/arco-catalog/src/tier1_compactor.rs` so a commit row is appended only after a visible catalog publish is constructed from:

- the new immutable manifest metadata
- the pointer-selected snapshot version
- the watermark event ID
- the input catalog DDL event metadata when available

Important rule:

- derive `system.catalog.commits` from visible publish state
- do not depend on legacy `commits/catalog/{commit_id}.json`
- do not make commit-row write success part of a second commit point

The record should be part of the new snapshot written before pointer CAS, so pointer publication atomically makes the new `commits.parquet` visible together with the rest of the catalog snapshot.

**Step 6: Register the new table on the API side**

Extend `crates/arco-api/src/system_tables.rs` so `commits.parquet` maps to `system.catalog.commits`.

**Step 7: Run verification**

Run: `cargo test -p arco-catalog --test schema_contracts -- --nocapture`
Expected: PASS

Run: `cargo test -p arco-catalog --test protocol_invariants -- --nocapture`
Expected: PASS

Run: `cargo test -p arco-api --test system_tables_api query_can_select_from_system_catalog_commits -- --nocapture`
Expected: PASS

**Step 8: Commit**

```bash
git add crates/arco-catalog/src/parquet_util.rs crates/arco-catalog/src/state.rs crates/arco-catalog/src/tier1_state.rs crates/arco-catalog/src/tier1_snapshot.rs crates/arco-catalog/src/tier1_compactor.rs crates/arco-catalog/tests/schema_contracts.rs crates/arco-catalog/tests/protocol_invariants.rs crates/arco-catalog/tests/golden_schemas/commits.schema.json crates/arco-api/src/system_tables.rs crates/arco-api/tests/system_tables_api.rs
git commit -m "feat: add system catalog commits projection"
```

### Task 5: Harden The Default Surface For Privacy And Scope

**Files:**
- Modify: `crates/arco-api/src/system_tables.rs`
- Modify: `crates/arco-api/tests/system_tables_api.rs`
- Modify: `docs/adr/adr-035-system-catalog-tables.md`

**Step 1: Write the failing privacy tests**

Add tests like:

```rust
#[tokio::test]
async fn query_does_not_expose_system_search_postings() -> Result<()> {
    let router = seed_catalog(test_router()).await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT * FROM system.search.token_postings"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}
```

Also add a workspace isolation test that seeds two workspaces and verifies the current request only sees the current workspace’s `system.catalog.tables`.

Also add a deny-by-default test for internal orchestration tables:

```rust
#[tokio::test]
async fn query_does_not_expose_internal_orchestration_idempotency_keys() -> Result<()> {
    let router = seed_orchestration_router().await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT * FROM system.orchestration.idempotency_keys"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}
```

**Step 2: Run the tests to verify they fail**

Run: `cargo test -p arco-api --test system_tables_api query_does_not_expose_system_search_postings -- --nocapture`
Expected: FAIL if the registry is too permissive

**Step 3: Tighten the registry**

In `crates/arco-api/src/system_tables.rs`:

- keep a hard-coded allowlist
- do not expose `search.token_postings`
- do not expose raw manifest JSON or raw ledger paths
- do not expose `run_key_index` or `idempotency_keys` in the default tenant-facing `system.orchestration` surface
- reject future “register everything in manifest” refactors in comments and tests

Update the ADR so the default exposure policy is explicit.

**Step 4: Run focused verification**

Run: `cargo test -p arco-api --test system_tables_api -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-api/src/system_tables.rs crates/arco-api/tests/system_tables_api.rs docs/adr/adr-035-system-catalog-tables.md
git commit -m "feat: harden system table exposure policy"
```

### Task 6: Publish Operator-Facing Reference Docs

**Files:**
- Create: `docs/guide/src/reference/system-catalog.md`
- Modify: `docs/guide/src/SUMMARY.md`
- Modify: `README.md`

**Step 1: Write the reference page**

Create a concise reference page with:

- what `system` is
- table list for v1
- example SQL
- explicit freshness note
- explicit “not for synchronous authorization” warning

Use example SQL like:

```sql
select name
from system.catalog.tables
order by name;

select commit_ulid, snapshot_version
from system.catalog.commits
order by published_at desc;

select run_id, state
from system.orchestration.runs
order by created_at desc;

select asset_key, stale_reason_code
from system.orchestration.partition_status
order by asset_key;

select backfill_id, state
from system.orchestration.backfills
order by created_at desc;
```

**Step 2: Update navigation and README**

- add the reference page to `docs/guide/src/SUMMARY.md`
- add one short README note that Arco now exposes a SQL-native `system` catalog over operational metadata

**Step 3: Run doc verification**

Run: `cd docs/guide && mdbook build`
Expected: PASS

**Step 4: Commit**

```bash
git add docs/guide/src/reference/system-catalog.md docs/guide/src/SUMMARY.md README.md
git commit -m "docs: add system catalog reference"
```

## Final Verification And Merge Checklist

1. Run the full verification matrix from this plan.
2. Manually exercise:
   - `SELECT name FROM system.catalog.namespaces`
   - `SELECT commit_ulid FROM system.catalog.commits ORDER BY published_at DESC`
   - `SELECT run_id FROM system.orchestration.runs`
3. Confirm no query path can resolve `system.search.token_postings`.
4. Confirm no code path reads the ledger for system-table correctness.
5. Confirm `commits.parquet` appears in catalog `SnapshotInfo.files` and is therefore pointer-published with the rest of the snapshot.

## Deferred Follow-Ons

- `system.catalog.object_history` after the commit projection is stable and event metadata is rich enough to make history rows useful.
- `system.access.audit`, `system.access.auth_denies`, and `system.access.url_mint_events` after audit events are projected into Parquet state rather than tracing-only sinks.
- `system.query.history` after `/api/v1/query` and `/api/v1/query-data` emit durable history records.
- `system.storage.retention_policies`, `system.storage.gc_runs`, `system.storage.snapshot_inventory`, and `system.storage.reconciliation_issues` after GC/reconciler outputs are materialized as first-class Arco projections.
- `system.billing.*`, `system.compute.*`, `system.classification.*`, and policy/grant tables only after those domains become authoritative runtime state in Arco.
- Browser-specific logical `system` catalog affordances if the UI needs direct DuckDB-WASM access to the same logical names instead of going through `/api/v1/query`.
