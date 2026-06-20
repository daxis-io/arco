# Catalog Read Performance Implementation Plan

**Goal:** Make ordinary catalog, authorization, and storage-governance reads use domain-scoped manifests, version-pinned in-memory read models, and indexed enforcement views without weakening Arco's pointer-published consistency model.

**Architecture:** Keep immutable snapshots and projection pointers as the visibility boundary. Replace repeated request-time manifest fanout and Parquet decoding with per-snapshot read models cached behind version checks. Authorization and storage-governance enforcement remain fail-closed on stale or missing watermarks, but hot decisions use indexed in-memory views.

**Tech Stack:** Rust 1.85, Tokio, Arrow/Parquet, object-store CAS, `arco-core::ScopedStorage`, `arco-catalog`, `arco-uc`, Criterion, existing spy storage tests.

---

## Current State

This plan was written against the live checkout on 2026-05-22.

The working tree had existing user/WIP changes:

```text
 M crates/arco-catalog/src/reader.rs
?? crates/arco-api/tests/catalog_inventory_api.rs
```

Implementation should start in a fresh worktree or after those changes are reconciled. Do not overwrite them.

Relevant current seams:

- `crates/arco-catalog/src/reader.rs:145-149` stores only a `table_lookup_cache`.
- `crates/arco-catalog/src/reader.rs:243-297` reads root plus catalog, lineage, executions, and search manifests for many catalog-only operations.
- `crates/arco-catalog/src/reader.rs:451-628` repeatedly loads and decodes snapshot Parquet files for list/get operations.
- `crates/arco-catalog/src/reader.rs:638-832` routes catalog metadata methods through full `read_manifest()`.
- `crates/arco-catalog/src/authz/decision.rs:101-108` scans compiled permission rows for every decision.
- `crates/arco-catalog/src/storage_governance/mod.rs:398-419` rebuilds path authority candidates on each path decision.
- `crates/arco-catalog/src/metastore/publish.rs:240-303` reloads projection pointer, manifest, latest ledger watermark, Parquet rows, and storage-governance state for each enforcement read.
- Existing invariant coverage in `crates/arco-catalog/tests/protocol_invariants.rs:310-377` proves ordinary reads do not list storage, but does not yet prove domain-scoped manifest reads.

## Non-Negotiable Invariants

- Readers never use raw ledger replay or object-store listing for request-time correctness.
- Pointer movement remains the visibility cut. Readers may see the old complete snapshot or the new complete snapshot, never a mixed state.
- Read models are immutable once built and keyed by the pointer-selected snapshot identity.
- Root-token reads remain pinned to the immutable root transaction manifest and must not poison the moving-head cache.
- Authorization and credential vending deny closed when compiled permissions or storage-governance projections are missing, corrupt, unsupported, or stale.
- No secret material, provider tokens, raw policy payloads, or hidden objects are exposed through read models.
- Do not add new dependencies in the first implementation pass. Use `Arc`, `RwLock`, and `tokio::sync::Mutex`; consider `arc-swap` only after benchmarks prove lock contention matters.

## Preflight

**Files:**
- Read: `crates/arco-catalog/src/reader.rs`
- Read: `crates/arco-catalog/src/authz/compiler.rs`
- Read: `crates/arco-catalog/src/authz/decision.rs`
- Read: `crates/arco-catalog/src/metastore/publish.rs`
- Read: `crates/arco-catalog/src/storage_governance/mod.rs`
- Read: `crates/arco-uc/src/state.rs`
- Read: `crates/arco-uc/src/routes/credentials.rs`
- Read: `crates/arco-catalog/tests/protocol_invariants.rs`
- Read: `crates/arco-catalog/tests/metastore_replay_publication.rs`
- Read: `crates/arco-catalog/benches/catalog_lookup.rs`

**Step 1: Confirm the working state**

Run:

```bash
git status --short
git branch --show-current
git rev-parse --show-toplevel
```

Expected: exact branch and dirty files are known before editing.

If `crates/arco-catalog/src/reader.rs` is dirty from unrelated work, create a new worktree:

```bash
git worktree add .worktrees/catalog-read-performance -b catalog-read-performance
cd .worktrees/catalog-read-performance
```

Expected: implementation happens away from user-owned edits.

**Step 2: Capture baseline tests**

Run:

```bash
cargo test -p arco-catalog --test protocol_invariants ordinary_catalog_reads_never_list_storage -- --nocapture
cargo test -p arco-catalog --test metastore_replay_publication -- --nocapture
cargo test -p arco-catalog --test authz_decisions -- --nocapture
cargo test -p arco-uc --test credentials_authoritative -- --nocapture
cargo bench -p arco-catalog --bench catalog_lookup
```

Expected: passing tests. Benchmark output is evidence only; do not commit generated benchmark artifacts.

If baseline fails, stop and report the pre-existing failure before implementing.

---

## Task 1: Domain-Scoped Manifest Reads

**Files:**
- Modify: `crates/arco-catalog/src/reader.rs:239-386`
- Modify: `crates/arco-catalog/src/reader.rs:638-900`
- Test: `crates/arco-catalog/tests/protocol_invariants.rs:310-377`
- Test: `crates/arco-catalog/benches/catalog_lookup.rs:272-280`

**Step 1: Write failing domain-scope tests**

Add a test next to `ordinary_catalog_reads_never_list_storage`:

```rust
#[tokio::test]
async fn catalog_metadata_reads_only_catalog_domain_manifest() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let spy = Arc::new(SpyBackend::new(inner));
    let storage = scoped_storage(spy.clone());
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");
    writer
        .create_namespace("analytics", None, WriteOptions::default())
        .await
        .expect("create namespace");

    let reader = CatalogReader::new(storage);
    spy.clear_ops();

    reader.list_namespaces().await.expect("list namespaces");
    reader.get_namespace("analytics").await.expect("get namespace");
    reader.list_tables("analytics").await.expect("list tables");
    reader
        .get_freshness(CatalogDomain::Catalog)
        .await
        .expect("catalog freshness");

    let forbidden = ["lineage", "executions", "search"];
    let forbidden_ops = spy
        .ops()
        .into_iter()
        .filter(|op| match op {
            SpyOp::Get { path } | SpyOp::Head { path } => {
                forbidden.iter().any(|needle| path.contains(needle))
            }
            _ => false,
        })
        .collect::<Vec<_>>();
    assert!(
        forbidden_ops.is_empty(),
        "catalog metadata reads must not fetch non-catalog manifests: {forbidden_ops:?}"
    );
}
```

Add a separate lineage test:

```rust
#[tokio::test]
async fn lineage_reads_only_lineage_domain_manifest() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let spy = Arc::new(SpyBackend::new(inner));
    let storage = scoped_storage(spy.clone());
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");

    let reader = CatalogReader::new(storage);
    spy.clear_ops();
    reader.get_lineage("missing").await.expect("lineage read");

    let forbidden = ["catalog.pointer", "executions", "search"];
    let forbidden_ops = spy
        .ops()
        .into_iter()
        .filter(|op| match op {
            SpyOp::Get { path } | SpyOp::Head { path } => {
                forbidden.iter().any(|needle| path.contains(needle))
            }
            _ => false,
        })
        .collect::<Vec<_>>();
    assert!(
        forbidden_ops.is_empty(),
        "lineage reads must not fetch unrelated domain manifests: {forbidden_ops:?}"
    );
}
```

**Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p arco-catalog --test protocol_invariants catalog_metadata_reads_only_catalog_domain_manifest -- --nocapture
cargo test -p arco-catalog --test protocol_invariants lineage_reads_only_lineage_domain_manifest -- --nocapture
```

Expected: FAIL because current catalog and lineage reads call full `read_manifest()`.

**Step 3: Add domain-specific manifest helpers**

In `crates/arco-catalog/src/reader.rs`, keep `read_manifest()` for legacy callers that need all domains, but add helpers:

```rust
async fn read_catalog_domain_manifest(&self) -> Result<CatalogDomainManifest> {
    let bytes = self
        .read_domain_manifest_bytes(CatalogDomain::Catalog, None)
        .await?;
    serde_json::from_slice(&bytes).map_err(|e| CatalogError::Serialization {
        message: format!("failed to parse catalog manifest: {e}"),
    })
}

async fn read_lineage_domain_manifest(&self) -> Result<LineageManifest> {
    let bytes = self
        .read_domain_manifest_bytes(CatalogDomain::Lineage, None)
        .await?;
    serde_json::from_slice(&bytes).map_err(|e| CatalogError::Serialization {
        message: format!("failed to parse lineage manifest: {e}"),
    })
}

async fn read_search_domain_manifest(&self) -> Result<SearchManifest> {
    let bytes = self
        .read_domain_manifest_bytes(CatalogDomain::Search, None)
        .await?;
    serde_json::from_slice(&bytes).map_err(|e| CatalogError::Serialization {
        message: format!("failed to parse search manifest: {e}"),
    })
}
```

Use these helpers in catalog-only methods:

- `list_catalogs`
- `get_catalog`
- `list_schemas`
- `list_tables_in_schema`
- `get_table_in_schema`
- `list_namespaces`
- `get_namespace`
- `list_tables`
- `get_table`
- `get_table_by_id`
- `get_columns`
- `get_freshness(CatalogDomain::Catalog)`

Use `read_lineage_domain_manifest()` in:

- `get_lineage`
- `get_freshness(CatalogDomain::Lineage)`

Leave executions and whole-manifest APIs on the existing path until they get their own slice.

**Step 4: Run tests**

Run:

```bash
cargo test -p arco-catalog --test protocol_invariants catalog_metadata_reads_only_catalog_domain_manifest -- --nocapture
cargo test -p arco-catalog --test protocol_invariants lineage_reads_only_lineage_domain_manifest -- --nocapture
cargo test -p arco-catalog --test protocol_invariants ordinary_catalog_reads_never_list_storage -- --nocapture
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-catalog/src/reader.rs crates/arco-catalog/tests/protocol_invariants.rs
git commit -m "perf: scope catalog manifest reads by domain"
```

---

## Task 2: Version-Pinned Catalog Read Model

**Files:**
- Create: `crates/arco-catalog/src/read_model.rs`
- Modify: `crates/arco-catalog/src/lib.rs:78-110`
- Modify: `crates/arco-catalog/src/reader.rs:145-167`
- Modify: `crates/arco-catalog/src/reader.rs:451-628`
- Test: `crates/arco-catalog/tests/protocol_invariants.rs`
- Test: `crates/arco-catalog/benches/catalog_lookup.rs`

**Step 1: Write failing cache reuse test**

Add a test proving repeated hot catalog lookups do not repeatedly fetch Parquet files for the same snapshot:

```rust
#[tokio::test]
async fn catalog_read_model_reuses_snapshot_bytes_for_hot_reads() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let spy = Arc::new(SpyBackend::new(inner));
    let storage = scoped_storage(spy.clone());
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");
    writer
        .create_namespace("analytics", None, WriteOptions::default())
        .await
        .expect("create namespace");
    writer
        .register_table(
            RegisterTableRequest {
                namespace: "analytics".to_string(),
                name: "events".to_string(),
                description: None,
                location: Some("gs://bucket/events".to_string()),
                format: Some("delta".to_string()),
                columns: vec![test_column("event_id")],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register table");

    let reader = CatalogReader::new(storage);

    reader.list_tables("analytics").await.expect("warm list");
    spy.clear_ops();

    reader.list_namespaces().await.expect("hot namespaces");
    reader.get_namespace("analytics").await.expect("hot namespace");
    reader.list_tables("analytics").await.expect("hot list tables");
    reader.get_table("analytics", "events").await.expect("hot table");

    let parquet_gets = spy
        .ops()
        .into_iter()
        .filter(|op| matches!(op, SpyOp::Get { path } if path.ends_with(".parquet")))
        .collect::<Vec<_>>();
    assert!(
        parquet_gets.is_empty(),
        "hot catalog reads should use the version-pinned read model: {parquet_gets:?}"
    );
}
```

**Step 2: Run test to verify failure**

Run:

```bash
cargo test -p arco-catalog --test protocol_invariants catalog_read_model_reuses_snapshot_bytes_for_hot_reads -- --nocapture
```

Expected: FAIL because current methods reload Parquet files.

**Step 3: Add `read_model` module**

Create `crates/arco-catalog/src/read_model.rs`:

```rust
//! Immutable read models for pointer-published catalog snapshots.

use std::collections::HashMap;
use std::sync::Arc;

use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};

use crate::error::{CatalogError, Result};
use crate::manifest::CatalogDomainManifest;
use crate::parquet_util;
use crate::writer::{Catalog, Column, Schema, Table};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogSnapshotIdentity {
    pub(crate) snapshot_version: u64,
    pub(crate) manifest_id: String,
    pub(crate) last_commit_id: Option<String>,
}

impl CatalogSnapshotIdentity {
    pub(crate) fn from_manifest(manifest: &CatalogDomainManifest) -> Self {
        Self {
            snapshot_version: manifest.snapshot_version,
            manifest_id: manifest.manifest_id.clone(),
            last_commit_id: manifest.last_commit_id.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct CatalogReadModel {
    pub(crate) identity: CatalogSnapshotIdentity,
    catalogs: Arc<Vec<Catalog>>,
    namespaces: Arc<Vec<Schema>>,
    tables: Arc<Vec<Table>>,
    namespace_by_name: HashMap<String, Schema>,
    catalog_by_name: HashMap<String, Catalog>,
    tables_by_id: HashMap<String, Table>,
    tables_by_namespace_id: HashMap<String, Vec<Table>>,
    table_by_namespace_id_and_name: HashMap<(String, String), Table>,
    columns_by_table_id: HashMap<String, Vec<Column>>,
}

impl CatalogReadModel {
    pub(crate) async fn load(
        storage: &ScopedStorage,
        manifest: &CatalogDomainManifest,
    ) -> Result<Self> {
        if manifest.snapshot_version == 0 {
            return Ok(Self::empty(manifest));
        }

        let version = manifest.snapshot_version;
        let catalogs_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "catalogs.parquet");
        let namespaces_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "namespaces.parquet");
        let tables_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "tables.parquet");
        let columns_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "columns.parquet");

        let (catalogs_bytes, namespaces_bytes, tables_bytes, columns_bytes) = tokio::join!(
            storage.get_raw(&catalogs_path),
            storage.get_raw(&namespaces_path),
            storage.get_raw(&tables_path),
            storage.get_raw(&columns_path),
        );

        let catalogs = match catalogs_bytes {
            Ok(bytes) => parquet_util::read_catalogs(&bytes)?
                .into_iter()
                .map(Catalog::try_from)
                .collect::<Result<Vec<_>>>()?,
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                Vec::new()
            }
            Err(err) => return Err(err.into()),
        };
        let namespaces = parquet_util::read_namespaces(&namespaces_bytes?)?
            .into_iter()
            .map(Schema::try_from)
            .collect::<Result<Vec<_>>>()?;
        let tables = parquet_util::read_tables(&tables_bytes?)?
            .into_iter()
            .map(Table::try_from)
            .collect::<Result<Vec<_>>>()?;
        let columns = parquet_util::read_columns(&columns_bytes?)?
            .into_iter()
            .map(Column::from)
            .collect::<Vec<_>>();

        Ok(Self::from_parts(manifest, catalogs, namespaces, tables, columns))
    }

    fn empty(manifest: &CatalogDomainManifest) -> Self {
        Self::from_parts(manifest, Vec::new(), Vec::new(), Vec::new(), Vec::new())
    }
}
```

Complete `from_parts` as an infallible map-building helper with sorted column vectors. Do not expose mutable collections.

**Step 4: Export the module internally**

In `crates/arco-catalog/src/lib.rs`, add:

```rust
mod read_model;
```

Keep it private unless another crate truly needs direct access.

**Step 5: Replace `TableLookupCache` with read-model cache**

In `CatalogReader`, replace:

```rust
table_lookup_cache: RwLock<Option<TableLookupCache>>,
```

with:

```rust
catalog_read_model: RwLock<Option<Arc<CatalogReadModel>>>,
catalog_read_model_refresh: tokio::sync::Mutex<()>,
```

Add helper:

```rust
async fn catalog_read_model(
    &self,
    manifest: &CatalogDomainManifest,
) -> Result<Arc<CatalogReadModel>> {
    let identity = CatalogSnapshotIdentity::from_manifest(manifest);
    if let Some(model) = self.cached_catalog_read_model(&identity)? {
        return Ok(model);
    }

    let _guard = self.catalog_read_model_refresh.lock().await;
    if let Some(model) = self.cached_catalog_read_model(&identity)? {
        return Ok(model);
    }

    let model = Arc::new(CatalogReadModel::load(&self.storage, manifest).await?);
    let mut cache = self.catalog_read_model.write().map_err(|_| {
        CatalogError::InvariantViolation {
            message: "catalog read model cache lock poisoned".to_string(),
        }
    })?;
    *cache = Some(Arc::clone(&model));
    Ok(model)
}
```

Add a small `cached_catalog_read_model` helper so lock handling stays out of the public read methods.

**Step 6: Run tests**

Run:

```bash
cargo test -p arco-catalog --test protocol_invariants catalog_read_model_reuses_snapshot_bytes_for_hot_reads -- --nocapture
cargo test -p arco-catalog --test protocol_invariants ordinary_catalog_reads_never_list_storage -- --nocapture
```

Expected: PASS.

**Step 7: Commit**

```bash
git add crates/arco-catalog/src/lib.rs crates/arco-catalog/src/read_model.rs crates/arco-catalog/src/reader.rs crates/arco-catalog/tests/protocol_invariants.rs
git commit -m "perf: cache catalog snapshots as read models"
```

---

## Task 3: Adopt Read Model Across Catalog APIs And Root Tokens

**Files:**
- Modify: `crates/arco-catalog/src/read_model.rs`
- Modify: `crates/arco-catalog/src/reader.rs:451-832`
- Test: `crates/arco-catalog/tests/protocol_invariants.rs:379-409`
- Test: `crates/arco-catalog/benches/catalog_lookup.rs:195-284`

**Step 1: Write behavioral tests for cache invalidation and root-token isolation**

Add:

```rust
#[tokio::test]
async fn catalog_read_model_refreshes_after_pointer_moves() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = scoped_storage(backend.clone());
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");
    writer
        .create_namespace("analytics", None, WriteOptions::default())
        .await
        .expect("create analytics");

    let reader = CatalogReader::new(storage.clone());
    assert!(
        reader
            .get_namespace("analytics")
            .await
            .expect("lookup analytics")
            .is_some()
    );

    writer
        .create_namespace("finance", None, WriteOptions::default())
        .await
        .expect("create finance");

    assert!(
        reader
            .get_namespace("finance")
            .await
            .expect("lookup finance after pointer move")
            .is_some()
    );
}
```

Add:

```rust
#[tokio::test]
async fn root_token_read_model_does_not_replace_moving_head_cache() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let spy = Arc::new(SpyBackend::new(inner));
    let storage = scoped_storage(spy.clone());
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");
    writer
        .create_namespace("before_root", None, WriteOptions::default())
        .await
        .expect("create before_root");

    let (pointer, manifest, _) = current_catalog_pointer(&storage).await;
    seed_root_token_for_catalog(&storage, "01JROOTCATTOKEN000000000001", &pointer, &manifest).await;

    let reader = CatalogReader::new(storage.clone());
    reader.list_namespaces().await.expect("warm moving head");

    writer
        .create_namespace("after_root", None, WriteOptions::default())
        .await
        .expect("create after_root");

    let pinned = reader
        .list_namespaces_for_root_token("root:01JROOTCATTOKEN000000000001")
        .await
        .expect("pinned namespaces");
    assert!(pinned.iter().any(|ns| ns.name == "before_root"));
    assert!(!pinned.iter().any(|ns| ns.name == "after_root"));

    let moving = reader.list_namespaces().await.expect("moving namespaces");
    assert!(moving.iter().any(|ns| ns.name == "after_root"));
}
```

**Step 2: Run tests to verify current gaps**

Run:

```bash
cargo test -p arco-catalog --test protocol_invariants catalog_read_model_refreshes_after_pointer_moves -- --nocapture
cargo test -p arco-catalog --test protocol_invariants root_token_read_model_does_not_replace_moving_head_cache -- --nocapture
```

Expected: pointer refresh should pass only after Task 2 cache identity is correct. Root-token isolation should fail if pinned reads reuse the moving cache incorrectly.

**Step 3: Add read-model lookup methods**

In `CatalogReadModel`, add methods:

```rust
pub(crate) fn list_catalogs(&self) -> Vec<Catalog>;
pub(crate) fn get_catalog(&self, name: &str) -> Option<Catalog>;
pub(crate) fn list_namespaces(&self) -> Vec<Schema>;
pub(crate) fn get_namespace(&self, name: &str) -> Option<Schema>;
pub(crate) fn list_schemas(&self, catalog: &str) -> Result<Vec<Schema>>;
pub(crate) fn list_tables(&self, namespace: &str) -> Result<Vec<Table>>;
pub(crate) fn list_tables_in_schema(&self, catalog: &str, schema: &str) -> Result<Vec<Table>>;
pub(crate) fn get_table(&self, namespace: &str, table: &str) -> Result<Option<Table>>;
pub(crate) fn get_table_in_schema(
    &self,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<Option<Table>>;
pub(crate) fn get_table_by_id(&self, table_id: &str) -> Option<Table>;
pub(crate) fn get_columns(&self, table_id: &str) -> Vec<Column>;
```

Match existing legacy/default catalog behavior from `list_schemas_from_catalog_manifest`.

**Step 4: Route catalog APIs through read model**

Update methods in `CatalogReader`:

```rust
pub async fn list_namespaces(&self) -> Result<Vec<Schema>> {
    let manifest = self.read_catalog_domain_manifest().await?;
    let model = self.catalog_read_model(&manifest).await?;
    Ok(model.list_namespaces())
}
```

Apply the same pattern to all catalog metadata APIs listed in Task 1.

For root-token reads, build an uncached read model:

```rust
let manifest = self.read_catalog_manifest_for_root_token(read_token).await?;
let model = CatalogReadModel::load(&self.storage, &manifest).await?;
Ok(model.list_namespaces())
```

Do not store root-token models in the moving-head cache.

**Step 5: Update benchmark labels**

In `crates/arco-catalog/benches/catalog_lookup.rs`, add separate warm and hot measurements:

```rust
group.bench_function("hot_get_table_single", |b| {
    let backend = Arc::new(MemoryBackend::new());
    let storage = rt.block_on(setup_catalog(backend.clone(), 10, 10));
    let reader = CatalogReader::new(storage);
    rt.block_on(reader.get_table("namespace_5", "table_5"))
        .expect("warm read");

    b.iter(|| {
        let result = rt.block_on(reader.get_table("namespace_5", "table_5"));
        black_box(result)
    });
});
```

Add hot variants for `list_namespaces`, `list_tables`, `get_table_by_id`, and `get_columns`.

**Step 6: Run tests and benchmark**

Run:

```bash
cargo test -p arco-catalog --test protocol_invariants -- --nocapture
cargo bench -p arco-catalog --bench catalog_lookup
```

Expected: tests pass. Hot benchmark variants should show no repeated Parquet decode cost.

**Step 7: Commit**

```bash
git add crates/arco-catalog/src/read_model.rs crates/arco-catalog/src/reader.rs crates/arco-catalog/tests/protocol_invariants.rs crates/arco-catalog/benches/catalog_lookup.rs
git commit -m "perf: serve catalog APIs from versioned read models"
```

---

## Task 4: Indexed Authorization Decisions

**Files:**
- Modify: `crates/arco-catalog/src/authz/compiler.rs:86-118`
- Modify: `crates/arco-catalog/src/authz/decision.rs:82-143`
- Test: `crates/arco-catalog/tests/authz_decisions.rs`
- Test: `crates/arco-uc/tests/permissions_authoritative.rs`
- Test: `crates/arco-uc/tests/credentials_authoritative.rs`

**Step 1: Write failing index behavior test**

Add a test in `crates/arco-catalog/tests/authz_decisions.rs`:

```rust
#[test]
fn authorization_index_matches_scan_semantics_with_many_irrelevant_rows() {
    let mut rows = Vec::new();
    for i in 0..1_000 {
        rows.push(CompiledPermissionRow {
            principal_id: format!("principal_{i}"),
            object_id: format!("object_{i}"),
            object_type: "TABLE".to_string(),
            privilege: Privilege::Select,
            source: "grant".to_string(),
            source_grant_id: Some(format!("grant_{i}")),
            source_principal_id: format!("principal_{i}"),
            source_object_id: format!("object_{i}"),
            inheritance_path: format!("object_{i}"),
            grant_option: false,
            group_snapshot_version: "groups_1".to_string(),
        });
    }
    rows.push(CompiledPermissionRow {
        principal_id: "alice".to_string(),
        object_id: "table_1".to_string(),
        object_type: "TABLE".to_string(),
        privilege: Privilege::Select,
        source: "grant".to_string(),
        source_grant_id: Some("grant_allow".to_string()),
        source_principal_id: "alice".to_string(),
        source_object_id: "table_1".to_string(),
        inheritance_path: "table_1".to_string(),
        grant_option: false,
        group_snapshot_version: "groups_1".to_string(),
    });

    let compiled = CompiledPermissionSet::new("event_1", "groups_1", true, rows);
    let request = AuthzRequest::new("alice", "table_1", "TABLE", Privilege::Select);
    let decision = AuthzDecision::evaluate(&request, &compiled);

    assert_eq!(decision.outcome, DecisionOutcome::Allow);
    assert_eq!(decision.evidence.grant_ids, vec!["grant_allow"]);
}
```

This test may already pass. Keep it as a semantic guard before changing internals.

**Step 2: Add private index to compiled permissions**

In `CompiledPermissionSet`, add a private index:

```rust
type PermissionIndexKey = (String, String, String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledPermissionSet {
    pub ledger_watermark: String,
    pub group_snapshot_version: String,
    pub fresh: bool,
    pub rows: Vec<CompiledPermissionRow>,
    by_principal_object: BTreeMap<PermissionIndexKey, Vec<usize>>,
}
```

Build the index inside `CompiledPermissionSet::new`.

Use uppercase object type in the key:

```rust
let key = (
    row.principal_id.clone(),
    row.object_id.clone(),
    row.object_type.to_ascii_uppercase(),
);
```

Do not require callers to provide the index. Existing tests and route setup should still use `CompiledPermissionSet::new`.

**Step 3: Make row lookup index-backed**

Replace the current scan in `rows_for_principal_object_privilege` with:

```rust
let key = (
    principal_id.to_string(),
    object_id.to_string(),
    object_type.to_ascii_uppercase(),
);
self.by_principal_object
    .get(&key)
    .into_iter()
    .flatten()
    .filter_map(|idx| self.rows.get(*idx))
    .filter(move |row| row.privilege.implies(privilege))
```

If returning `impl Iterator` becomes awkward because of lifetimes, return `Vec<&CompiledPermissionRow>` and adapt `AuthzDecision::evaluate`.

**Step 4: Run tests**

Run:

```bash
cargo test -p arco-catalog --test authz_decisions -- --nocapture
cargo test -p arco-uc --test permissions_authoritative -- --nocapture
cargo test -p arco-uc --test credentials_authoritative -- --nocapture
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-catalog/src/authz/compiler.rs crates/arco-catalog/src/authz/decision.rs crates/arco-catalog/tests/authz_decisions.rs
git commit -m "perf: index compiled authorization decisions"
```

---

## Task 5: Cached And Indexed Storage-Governance Views

**Files:**
- Modify: `crates/arco-catalog/src/storage_governance/mod.rs:62-72`
- Modify: `crates/arco-catalog/src/storage_governance/mod.rs:230-319`
- Modify: `crates/arco-catalog/src/storage_governance/mod.rs:398-481`
- Modify: `crates/arco-catalog/src/metastore/publish.rs:119-126`
- Modify: `crates/arco-catalog/src/metastore/publish.rs:240-303`
- Modify: `crates/arco-uc/src/state.rs:20-31`
- Modify: `crates/arco-uc/src/routes/credentials.rs:147-185`
- Modify: `crates/arco-uc/src/routes/credentials.rs:267-330`
- Test: `crates/arco-catalog/tests/metastore_replay_publication.rs`
- Test: `crates/arco-uc/tests/credentials_authoritative.rs`
- Test: `crates/arco-uc/tests/storage_governance_authoritative.rs`

**Step 1: Write failing hot-cache test for projection loading**

In `crates/arco-catalog/tests/metastore_replay_publication.rs`, add a test that publishes a storage-governance projection, loads it once through a cache, clears spy ops, then loads it again and asserts no Parquet file is fetched.

Target API to introduce:

```rust
let cache = PublishedStorageGovernanceCache::default();
let first = cache.load(&storage).await?;
spy.clear_ops();
let second = cache.load(&storage).await?;
assert_eq!(first.ledger_watermark, second.ledger_watermark);
```

Expected hot behavior:

```rust
let parquet_gets = spy
    .ops()
    .into_iter()
    .filter(|op| matches!(op, SpyOp::Get { path } if path.ends_with("storage_governance.parquet")))
    .collect::<Vec<_>>();
assert!(parquet_gets.is_empty(), "hot load should reuse cached projection state");
```

**Step 2: Run test to verify failure**

Run:

```bash
cargo test -p arco-catalog --test metastore_replay_publication storage_governance_projection_cache_reuses_hot_state -- --nocapture
```

Expected: FAIL because no cache exists.

**Step 3: Add `PublishedStorageGovernanceCache`**

In `crates/arco-catalog/src/metastore/publish.rs`, add:

```rust
#[derive(Debug, Default)]
pub struct PublishedStorageGovernanceCache {
    current: std::sync::RwLock<Option<Arc<PublishedStorageGovernance>>>,
    refresh: tokio::sync::Mutex<()>,
}

impl PublishedStorageGovernanceCache {
    pub async fn load(&self, storage: &ScopedStorage) -> Result<Arc<PublishedStorageGovernance>> {
        let manifest = load_projection_manifest(storage).await?;
        let latest = MetastoreLedger::new(storage.clone()).latest_watermark().await?;
        validate_storage_governance_manifest_freshness(&manifest, latest.as_ref())?;

        if let Some(current) = self.current.read().map_err(|_| {
            CatalogError::InvariantViolation {
                message: "storage governance cache lock poisoned".to_string(),
            }
        })?.as_ref() {
            if current.ledger_watermark == manifest.ledger_watermark {
                return Ok(Arc::clone(current));
            }
        }

        let _guard = self.refresh.lock().await;
        if let Some(current) = self.current.read().map_err(|_| {
            CatalogError::InvariantViolation {
                message: "storage governance cache lock poisoned".to_string(),
            }
        })?.as_ref() {
            if current.ledger_watermark == manifest.ledger_watermark {
                return Ok(Arc::clone(current));
            }
        }

        let loaded = Arc::new(load_published_storage_governance_from_manifest(storage, manifest).await?);
        *self.current.write().map_err(|_| CatalogError::InvariantViolation {
            message: "storage governance cache lock poisoned".to_string(),
        })? = Some(Arc::clone(&loaded));
        Ok(loaded)
    }
}
```

Refactor the existing `load_published_storage_governance` into smaller helpers:

- `load_projection_manifest`
- `validate_storage_governance_manifest_freshness`
- `load_published_storage_governance_from_manifest`

Keep the existing free function as a no-cache wrapper for tests and callers that do not own server state.

**Step 4: Index path authority checks**

In `StorageGovernanceState`, add derived indexes built in `from_projection_rows` and mutation methods:

```rust
authorities_by_workspace: BTreeMap<String, Vec<PathAuthority>>,
global_authorities: Vec<PathAuthority>,
bindings_by_workspace: BTreeMap<String, BTreeSet<(String, String)>>,
```

Keep the existing maps as source state. Use indexes only for hot decisions.

Update `authority_for_path` so it checks:

- authorities directly scoped to `workspace_id`
- globally governed external locations bound to that workspace
- test-only unsafe authorities under `#[cfg(test)]`

Sort candidates by path length descending and reject ambiguity as before.

**Step 5: Thread cache through UC state**

In `crates/arco-uc/src/state.rs`, add:

```rust
use arco_catalog::metastore::publish::PublishedStorageGovernanceCache;

pub struct UnityCatalogState {
    pub storage: Arc<dyn StorageBackend>,
    pub config: UnityCatalogConfig,
    pub compiled_permissions: Option<Arc<RwLock<CompiledPermissionSet>>>,
    pub storage_governance_cache: Arc<PublishedStorageGovernanceCache>,
    pub audit_emitter: Option<AuditEmitter>,
}
```

Initialize the cache in `new` and `with_config`.

In credential routes, replace:

```rust
let published = load_published_storage_governance(&storage).await?;
```

with:

```rust
let published = state
    .storage_governance_cache
    .load(&storage)
    .await
    .map_err(credential_projection_error)?;
```

Use `Arc<PublishedStorageGovernance>` by reference. Avoid cloning the full storage-governance state on every request.

**Step 6: Run tests**

Run:

```bash
cargo test -p arco-catalog --test metastore_replay_publication -- --nocapture
cargo test -p arco-uc --test credentials_authoritative -- --nocapture
cargo test -p arco-uc --test storage_governance_authoritative -- --nocapture
```

Expected: PASS.

**Step 7: Commit**

```bash
git add crates/arco-catalog/src/metastore/publish.rs crates/arco-catalog/src/storage_governance/mod.rs crates/arco-uc/src/state.rs crates/arco-uc/src/routes/credentials.rs crates/arco-catalog/tests/metastore_replay_publication.rs
git commit -m "perf: cache storage governance enforcement views"
```

---

## Task 6: Performance Gates And Observability

**Files:**
- Modify: `crates/arco-catalog/benches/catalog_lookup.rs`
- Modify: `docs/runbooks/perf-baseline.md`
- Modify: `crates/arco-catalog/src/metrics.rs`
- Optional Modify: `crates/arco-api/src/metrics.rs`
- Test: `crates/arco-catalog/tests/protocol_invariants.rs`

**Step 1: Add benchmark coverage for cold vs hot paths**

Extend `catalog_lookup` with:

- `cold_list_namespaces`
- `hot_list_namespaces`
- `cold_get_table_single`
- `hot_get_table_single`
- `hot_get_columns`
- `hot_get_table_by_id`
- `large_hot_get_table_single` with 100 namespaces and 100 tables each

Use one warm call before hot benchmarks.

**Step 2: Add storage-op regression tests**

Keep benchmark expectations out of unit tests. Unit tests should assert storage shape only:

- first read may fetch pointer, manifest, and Parquet snapshot files
- second hot read for the same snapshot must not fetch Parquet files
- after pointer movement, hot read may fetch new snapshot files once
- non-catalog domains are not fetched by catalog-only reads

**Step 3: Add metrics**

Add counters/histograms where the project already records catalog metrics:

- `arco_catalog_read_model_cache_hit_total`
- `arco_catalog_read_model_cache_miss_total`
- `arco_catalog_read_model_refresh_seconds`
- `arco_storage_governance_cache_hit_total`
- `arco_storage_governance_cache_miss_total`
- `arco_storage_governance_refresh_seconds`
- `arco_authz_index_candidate_rows`

Do not include tenant IDs, user IDs, object IDs, paths, or tokens as metric labels.

**Step 4: Update runbook**

In `docs/runbooks/perf-baseline.md`, add this verification set:

```bash
cargo bench -p arco-catalog --bench catalog_lookup
cargo test -p arco-catalog --test protocol_invariants -- --nocapture
cargo test -p arco-catalog --test authz_decisions -- --nocapture
cargo test -p arco-catalog --test metastore_replay_publication -- --nocapture
cargo test -p arco-uc --test credentials_authoritative -- --nocapture
```

Document that hot-read benchmarks are the release signal for this slice, while staging load tests remain the production signal.

**Step 5: Run verification**

Run:

```bash
cargo fmt --all
cargo test -p arco-catalog --test protocol_invariants -- --nocapture
cargo test -p arco-catalog --test authz_decisions -- --nocapture
cargo test -p arco-catalog --test metastore_replay_publication -- --nocapture
cargo test -p arco-uc --test credentials_authoritative -- --nocapture
cargo test -p arco-uc --test storage_governance_authoritative -- --nocapture
cargo bench -p arco-catalog --bench catalog_lookup
```

Expected: tests pass and benchmark output includes cold/hot split.

**Step 6: Commit**

```bash
git add crates/arco-catalog/benches/catalog_lookup.rs docs/runbooks/perf-baseline.md crates/arco-catalog/src/metrics.rs crates/arco-api/src/metrics.rs
git commit -m "test: add catalog read performance gates"
```

If `crates/arco-api/src/metrics.rs` is not needed, omit it from the commit.

---

## Task 7: Final Compatibility And Release Audit

**Files:**
- Read: `git diff main...HEAD`
- Read: `docs/adr/adr-037-arco-catalog-product-surface.md`
- Read: `docs/adr/adr-039-catalog-consistency-model.md`
- Read: `docs/guide/src/reference/credential-vending-security.md`
- Read: `docs/runbooks/perf-baseline.md`

**Step 1: Run full focused verification**

Run:

```bash
cargo fmt --all -- --check
cargo test -p arco-catalog --test protocol_invariants -- --nocapture
cargo test -p arco-catalog --test metastore_replay_publication -- --nocapture
cargo test -p arco-catalog --test authz_decisions -- --nocapture
cargo test -p arco-uc --test credentials_authoritative -- --nocapture
cargo test -p arco-uc --test permissions_authoritative -- --nocapture
cargo test -p arco-uc --test storage_governance_authoritative -- --nocapture
cargo bench -p arco-catalog --bench catalog_lookup
```

Expected: all tests pass. Benchmarks complete. Record benchmark highlights in the final handoff.

**Step 2: Check invariants manually**

Inspect:

```bash
git diff -- crates/arco-catalog/src/reader.rs
git diff -- crates/arco-catalog/src/read_model.rs
git diff -- crates/arco-catalog/src/authz/compiler.rs
git diff -- crates/arco-catalog/src/metastore/publish.rs
git diff -- crates/arco-catalog/src/storage_governance/mod.rs
git diff -- crates/arco-uc/src/routes/credentials.rs
```

Confirm:

- no read path uses ledger replay for ordinary catalog metadata
- no request-time object-store listing was added
- stale authz/storage-governance projections still deny closed
- root-token reads are isolated from the moving-head cache
- metric labels do not include sensitive or high-cardinality values
- no new dependency was added without evidence

**Step 3: Run broader cheap gates if time allows**

Run:

```bash
cargo xtask ci-parity
cargo xtask proto-breaking-check
cargo xtask adr-check
cd docs/guide && mdbook build
```

Expected: pass, or report pre-existing unrelated failures.

**Step 4: Final commit or squash**

If the branch is clean and the user wants a single change:

```bash
git log --oneline --decorate -8
git status --short
```

Then either keep task commits for reviewability or squash with DCO if requested.

---

## Acceptance Criteria

- Catalog-only reads no longer fetch non-catalog domain manifests.
- Hot catalog metadata reads for the same published snapshot do not fetch or decode Parquet again.
- Pointer movement refreshes the moving-head read model exactly once for the new snapshot.
- Root-token reads stay pinned to their root transaction manifest and do not replace the moving-head cache.
- Authorization decisions use indexed compiled permissions while preserving existing allow/deny evidence.
- Storage-governance credential paths use cached published projection state and indexed path authority checks.
- Enforcement still fails closed on stale, missing, corrupt, or unsupported projections.
- Existing no-listing invariant tests continue to pass.
- `cargo bench -p arco-catalog --bench catalog_lookup` includes cold/hot coverage for the optimized paths.

## Deferred Work

- `arc-swap` or lock-free cache publication. Defer until benchmark evidence shows `RwLock` read contention matters.
- New Parquet lookup projection files such as `tables_by_namespace.parquet` or `columns_by_table.parquet`. Defer until large-catalog benchmarks show snapshot load time dominates.
- Cross-node distributed cache invalidation. Defer until deployment topology requires it; pointer version checks are sufficient for this slice.
- Browser-side signed URL/read-model optimization. This plan focuses server-side catalog and enforcement reads.
