# Part 5: Catalog MVP Productization

**Date:** 2025-12-17
**Updated:** 2025-12-18
**Status:** Complete (All milestones finished)
**Audit Reference:** [2025-12-17-part2-catalog-mvp-audit.md](../audits/2025-12-17-part2-catalog-mvp-audit.md)

## Executive Summary

This plan closes the gap between "production-quality catalog core" and "GO for Catalog MVP launch." It incorporates all P0/P1 findings from the production-readiness audit plus best-in-class engineering practices.

**Key Principle:** A broken gate destroys trust in the entire engineering system. Quality gates are prerequisites, not nice-to-haves.

---

## Milestone Overview

| Milestone | Name | Priority | Status |
|-----------|------|----------|--------|
| M0.0 | CI Gate Health | P0 (Prerequisite) | Complete |
| M0.1 | Contract Alignment (ADR Pack) | P0 | Complete |
| M1 | Storage Layout Canonicalization | P0 | Complete |
| M2 | Event Envelope + Ordering + Invariants | P0 | Complete |
| M3 | CatalogReader/CatalogWriter Facades | P0 | Complete |
| M4 | REST API Layer (arco-api) | P0 | Complete |
| M5 | Browser Read Path + Signed URLs | P0 | Complete |
| M6 | Security Hardening | P0 | Complete |
| M7 | Deployment + Rollback + Anti-Entropy | P0 | Complete |
| M8 | Observability | P1 | Complete |
| M9 | Runbooks + Operational Docs | P1 | Complete |
| M10 | GC + Retention | **P0** | Complete |
| M11 | Engineering System Hardening | P1 | Complete |

---

## Milestone 0.0: CI Gate Health (Prerequisite)

**Rationale:** All required checks must be green on main, locally reproducible, and tool versions pinned before any feature work proceeds. A broken gate is a structural defect.

### Tasks

#### Task 0.0.1: Fix cargo-deny Configuration
```bash
# Regenerate compatible deny.toml
cargo deny init
# Reapply policies (license allowlist, sources, bans)
# Verify green
cargo deny check
```

**Acceptance:**
- [ ] `cargo deny check` passes locally
- [ ] CI "Cargo Deny" job passes on main
- [ ] Tool version pinned in CI workflow

#### Task 0.0.2: Fix Buf Configuration
```bash
# Convert proto/buf.yaml and proto/buf.gen.yaml to Buf v1 format
# OR pin CI to Buf v2 if that's the intended version
buf lint proto/
buf breaking proto/ --against 'origin/main'
```

**Acceptance:**
- [ ] `buf lint proto/` passes locally
- [ ] CI "Proto Compatibility" job passes on main
- [ ] Buf version pinned in CI workflow and matches local

#### Task 0.0.3: Add xtask doctor Command
```rust
// tools/xtask/src/doctor.rs
// Validates toolchain + configs are consistent
// - Check cargo-deny version matches CI
// - Check buf version matches CI
// - Check rustc version matches rust-toolchain.toml
// - Validate deny.toml schema
// - Validate buf.yaml schema
```

**Acceptance:**
- [ ] `cargo xtask doctor` validates all tool versions
- [ ] `cargo xtask doctor` fails fast with actionable errors
- [ ] CI runs `cargo xtask doctor` before other checks

### Exit Criteria (M0.0)
- `cargo xtask ci` is green (including cargo-deny)
- `buf lint proto/` passes
- Tool versions pinned and aligned between CI and local
- `cargo xtask doctor` validates environment consistency

---

## Milestone 0.1: Contract Alignment (ADR Pack)

**Rationale:** Clients, schemas, and ordering assumptions ossify quickly. Lock decisions now before they become implicit contracts.

### Tasks

#### Task 0.1.1: ADR-002 ID Strategy by Entity Type

**Decision to Lock:**
| Entity Type | ID Format | Rationale |
|-------------|-----------|-----------|
| Namespace, Table, Asset | UUID v7 | Stability across renames (per Technical Vision "Stable UUIDs") |
| Event, Materialization | ULID | Sortability for compaction/ordering |
| Commit, Version | ULID | Time-ordered for audit chain |

**Acceptance:**
- [ ] ADR-002 written to `docs/adr/adr-002-id-strategy.md`
- [ ] Proto types updated if needed
- [ ] Code aligns with ADR (no UUID/ULID conflicts)
- [ ] CI contract test validates ID formats

#### Task 0.1.2: ADR-003 Manifest Domain Names + Contention Strategy

**Decision to Lock:**
Align implementation with Technical Vision. Use **four domain manifests** to reduce write contention.

| Domain | Manifest Filename | Lock File | Update Rate | Purpose |
|--------|-------------------|-----------|-------------|---------|
| catalog | `catalog.manifest.json` | `catalog.lock.json` | Low (DDL) | Namespaces, tables, columns |
| lineage | `lineage.manifest.json` | `lineage.lock.json` | Medium (per-execution) | Lineage edges and graph |
| executions | `executions.manifest.json` | `executions.lock.json` | High (Tier-2) | Run/task execution state |
| search | `search.manifest.json` | `search.lock.json` | Low (rebuild) | Token postings index |

**Key Insight:** Lineage writes happen "as tasks execute" (medium frequency). Storing lineage under the same lock as catalog DDL creates a hotspot. **Separate locks per domain** ensures lineage writes don't block catalog DDL.

**Acceptance:**
- [ ] ADR-003 written with domain split + contention rationale
- [ ] Each domain has its own manifest + lock file
- [ ] CatalogWriter internally uses separate Tier1Writer per domain
- [ ] Technical Vision updated if deviating from naming
- [ ] Migration story documented if renaming

#### Task 0.1.3: ADR-004 Event Envelope Format + Evolution

**Decision to Lock:**
- Canonical `CatalogEvent` envelope (proto-backed or shared Rust struct)
- Required fields: `event_type`, `event_version`, `idempotency_key`, `timestamp`, `source`
- Evolution strategy: additive-only with version gates

**Acceptance:**
- [ ] ADR-004 written
- [ ] Proto `CatalogEvent` matches ADR
- [ ] EventWriter uses envelope for all ledger writes
- [ ] Compatibility tests for envelope evolution

#### Task 0.1.4: ADR-005 Storage Layout Canonical Spec

**Decision to Lock:**

```text
tenant={tenant}/workspace={workspace}/
├── manifests/
│   ├── root.manifest.json           # Points to domain manifests
│   ├── catalog.manifest.json        # Tier-1: namespaces/tables/columns
│   ├── lineage.manifest.json        # Tier-1: lineage edges
│   ├── executions.manifest.json     # Tier-2: run/task state
│   └── search.manifest.json         # Tier-1: token postings
├── locks/
│   ├── catalog.lock.json            # Separate lock per domain
│   ├── lineage.lock.json
│   ├── executions.lock.json
│   └── search.lock.json
├── commits/
│   └── {domain}/
│       └── {commit_id}.json         # Per-domain commit chain
├── snapshots/
│   ├── catalog/
│   │   └── v{version}/
│   │       ├── namespaces.parquet
│   │       ├── tables.parquet
│   │       └── columns.parquet
│   └── lineage/
│       └── v{version}/
│           └── lineage_edges.parquet
├── ledger/
│   └── {domain}/
│       └── {event_id}.json          # Tier-2 append-only events
└── state/
    └── {domain}/
        └── snapshot_{version}.parquet  # Tier-2 compacted state
```

**Acceptance:**

- [ ] ADR-005 written with canonical layout
- [ ] Single module defines all canonical paths
- [ ] Unit tests assert all writers use path module
- [ ] No divergence between helpers and actual writes
- [ ] Each domain has separate lock + commit chain

#### Task 0.1.5: Wire ADR Conformance to CI

**Rationale:** ADRs are useless if code can drift from decisions. CI must enforce conformance.

```rust
// tools/xtask/src/adr_check.rs

pub fn check_adr_conformance() -> Result<()> {
    // ADR-002: ID Strategy
    check_id_strategy()?;

    // ADR-003: Manifest Domain Names
    check_manifest_names()?;

    // ADR-005: Storage Layout
    check_storage_layout()?;

    Ok(())
}

fn check_id_strategy() -> Result<()> {
    // Verify ID formats in proto match ADR-002
    let proto_content = std::fs::read_to_string("proto/arco/catalog/v1/catalog.proto")?;

    // Namespace, Table, Asset should be UUID
    assert!(proto_content.contains("string namespace_id"), "namespace_id must be string (UUID)");

    // Event, Materialization should be ULID
    assert!(proto_content.contains("string event_id"), "event_id must be string (ULID)");

    Ok(())
}

fn check_manifest_names() -> Result<()> {
    // Verify code uses correct domain manifest names from ADR-003
    let paths_content = std::fs::read_to_string("crates/arco-core/src/paths.rs")?;

    for domain in ["catalog", "lineage", "executions", "search"] {
        assert!(
            paths_content.contains(&format!("{domain}.manifest.json")),
            "CatalogPaths must define {domain}.manifest.json"
        );
    }

    Ok(())
}

fn check_storage_layout() -> Result<()> {
    // Verify storage layout matches ADR-005
    // Check that all writers use CatalogPaths, not hardcoded strings
    let writer_files = glob::glob("crates/arco-catalog/src/**/*.rs")?;

    for file in writer_files {
        let content = std::fs::read_to_string(file?)?;
        // Flag any hardcoded manifest/lock/snapshot paths
        let forbidden_patterns = [
            r#""manifests/"#,
            r#""locks/"#,
            r#""snapshots/"#,
            r#""ledger/"#,
        ];

        for pattern in forbidden_patterns {
            if content.contains(pattern) && !content.contains("CatalogPaths") {
                bail!("Hardcoded path found: {pattern}. Use CatalogPaths instead.");
            }
        }
    }

    Ok(())
}
```

**CI Integration:**
```yaml
# .github/workflows/ci.yml
- name: ADR Conformance Check
  run: cargo xtask adr-check
```

**Acceptance:**

- [ ] `cargo xtask adr-check` validates all ADR decisions
- [ ] CI fails if code diverges from ADRs
- [ ] Check covers: ID formats, manifest names, storage layout
- [ ] False positives minimized (allowlist for legitimate uses)

### Exit Criteria (M0.1)
- 4 ADRs locked and checked in
- Docs + proto + code agree on ID formats
- Manifest names locked (implementation matches decision)
- Storage layout spec enforced by tests
- **ADR conformance wired to CI (cargo xtask adr-check)**

---

## Milestone 1: Storage Layout Canonicalization

**Rationale:** When helpers say one thing and writers do another, every future feature multiplies complexity.

### Tasks

#### Task 1.1: Create Canonical Path Module
```rust
// crates/arco-core/src/paths.rs
pub struct CatalogPaths;

impl CatalogPaths {
    pub fn manifest_root() -> &'static str;
    pub fn manifest(domain: &str) -> String;
    pub fn lock(domain: &str) -> String;
    pub fn commit(commit_id: &str) -> String;
    pub fn snapshot_dir(domain: &str, version: u64) -> String;
    pub fn ledger_event(domain: &str, event_id: &str) -> String;
    pub fn state_snapshot(domain: &str, version: u64) -> String;
}
```

**Acceptance:**
- [ ] `CatalogPaths` is the single source of truth for all paths
- [ ] All writers import and use `CatalogPaths`
- [ ] No hardcoded path strings outside this module

#### Task 1.2: Align ScopedStorage Helpers with Canonical Paths
- Remove or deprecate `ledger_path(domain, date)` (date partitioning not used)
- Remove or deprecate `state_path(domain, table)` (doesn't match compactor)
- Add new helpers that delegate to `CatalogPaths`

**Acceptance:**
- [ ] `ScopedStorage` helpers match actual writer behavior
- [ ] No dead code paths
- [ ] Tests validate helper output matches writer paths

#### Task 1.3: Add Path Contract Tests
```rust
#[test]
fn contract_event_writer_uses_canonical_paths() {
    // TracingMemoryBackend captures all paths
    // Assert all writes go through CatalogPaths
}

#[test]
fn contract_compactor_uses_canonical_paths() {
    // Same pattern
}

#[test]
fn contract_tier1_writer_uses_canonical_paths() {
    // Same pattern
}
```

**Acceptance:**
- [ ] Contract tests for all writers
- [ ] Tests fail if hardcoded paths introduced
- [ ] CI runs these tests

### Exit Criteria (M1)
- Single `CatalogPaths` module for all path generation
- All writers use canonical paths (enforced by tests)
- No divergence between helpers and implementation

---

## Milestone 2: Event Envelope + Ordering

**Rationale:** Without versioned envelopes and deterministic ordering, Tier-2 cannot safely evolve or handle disorder.

### Tasks

#### Task 2.1: Adopt CatalogEvent Envelope for Tier-2 Writes
```rust
// crates/arco-catalog/src/event.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEvent<T> {
    pub event_id: String,           // ULID
    pub event_type: String,         // e.g., "materialization.completed"
    pub event_version: u32,         // Schema version
    pub idempotency_key: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub source: EventSource,
    pub data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSource {
    pub tenant: String,
    pub workspace: String,
    pub run_id: Option<String>,
    pub task_id: Option<String>,
    pub sequence: Option<u64>,      // Per-execution ordering
}
```

**Acceptance:**
- [ ] `CatalogEvent` envelope defined
- [ ] EventWriter wraps all payloads in envelope
- [ ] Existing tests updated to use envelope
- [ ] Proto `CatalogEvent` matches Rust struct

#### Task 2.2: Implement Deterministic Ordering in Compactor

Ordering rule (per architecture):
1. `envelope.timestamp` (primary sort)
2. `envelope.event_id` (deterministic tie-breaker)
3. `envelope.source.sequence` (optional per-execution ordering)

```rust
impl Compactor {
    fn order_events(&self, events: Vec<CatalogEvent<T>>) -> Vec<CatalogEvent<T>> {
        events.sort_by(|a, b| {
            a.timestamp.cmp(&b.timestamp)
                .then_with(|| a.event_id.cmp(&b.event_id))
        })
    }
}
```

**Acceptance:**
- [ ] Compactor orders by timestamp + event_id
- [ ] Ordering is deterministic (same input → same output)
- [ ] Tests with out-of-order events pass

#### Task 2.3: Per-Domain Watermarks

Current: Single watermark in `ExecutionManifest` (breaks multi-domain)
Target: Each domain manifest has its own watermark

```rust
// Each domain manifest includes:
pub struct DomainManifest {
    pub snapshot_path: Option<String>,
    pub snapshot_version: u64,
    pub watermark: Watermark,
    pub last_compaction: Option<DateTime<Utc>>,
}

pub struct Watermark {
    pub last_event_id: String,      // Highest processed event_id
    pub last_timestamp: DateTime<Utc>,
    pub events_processed: u64,
}
```

**Acceptance:**
- [ ] Each domain has independent watermark
- [ ] Watermarks based on (timestamp, event_id), not filename
- [ ] Compactor updates only affected domain's watermark

#### Task 2.4: Compactor CAS Retry with Backoff

Current: Compactor returns `CasFailed` without retry (operationally fragile)
Target: Retry with exponential backoff

```rust
impl Compactor {
    pub async fn compact_domain(&self, domain: &str) -> Result<CompactionResult> {
        let mut attempt = 0;
        loop {
            match self.try_compact(domain).await {
                Ok(result) => return Ok(result),
                Err(CatalogError::CasFailed { .. }) if attempt < MAX_RETRIES => {
                    attempt += 1;
                    let backoff = Duration::from_millis(100 * 2u64.pow(attempt));
                    tokio::time::sleep(backoff).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
```

**Acceptance:**
- [ ] Compactor retries on CasFailed (max 3-5 attempts)
- [ ] Exponential backoff between retries
- [ ] Metrics/logs for retry attempts
- [ ] `concurrent_compactor_race` test passes reliably

#### Task 2.5: Late Event + Duplicate + Out-of-Order Tests

```rust
#[tokio::test]
async fn compactor_handles_out_of_order_events() {
    // Write events with timestamps: t3, t1, t2
    // Compact
    // Assert output ordered: t1, t2, t3
}

#[tokio::test]
async fn compactor_handles_late_events() {
    // Write events t1, t2, compact (watermark at t2)
    // Write event t1.5 (late arrival)
    // Compact again
    // Assert t1.5 is included (not skipped)
}

#[tokio::test]
async fn compactor_dedupes_by_idempotency_key() {
    // Write same event with same idempotency_key 3x
    // Compact
    // Assert only 1 row in output
}
```

**Acceptance:**
- [ ] Out-of-order events produce deterministic output
- [ ] Late events are not permanently skipped
- [ ] Idempotency keys dedupe correctly
- [ ] All tests pass with 100% reliability

#### Task 2.6: Invariant Test Suite (First-Class Deliverable)

The six architectural invariants must be **executable CI gates**, not just documentation.

| Invariant | Test | Description |
|-----------|------|-------------|
| 1. Append-only ingest | `invariant_ledger_append_only` | Ledger writes use DoesNotExist precondition; overwrite attempts fail |
| 2. Compactor sole Parquet writer | `invariant_compactor_sole_writer` | Only compactor writes to `state/` directory |
| 3. Idempotent compaction | `invariant_compaction_idempotent` | Same input → same output; no double-counting |
| 4. Atomic publish | `invariant_snapshot_atomic_publish` | Snapshot not visible until manifest CAS succeeds |
| 5. Readers never need ledger | `invariant_readers_parquet_only` | CatalogReader uses Parquet snapshots, never ledger |
| 6. No bucket listing for correctness | `invariant_no_listing_dependency` | Normal read path doesn't require bucket listing |

```rust
// crates/arco-catalog/tests/invariants.rs

#[tokio::test]
async fn invariant_ledger_append_only() {
    let storage = TestStorage::new();
    let writer = EventWriter::new(storage.clone(), test_source());

    // First write succeeds
    let event = test_event("event-1");
    writer.write(&event).await.unwrap();

    // Second write with same ID fails (DoesNotExist precondition)
    let result = writer.write(&event).await;
    assert!(matches!(result, Err(CatalogError::AlreadyExists { .. })));
}

#[tokio::test]
async fn invariant_snapshot_atomic_publish() {
    // Use FailingBackend that fails on manifest CAS
    let backend = FailingBackend::fail_on("manifests/catalog.manifest.json");
    let storage = ScopedStorage::new(backend, "acme", "prod")?;

    let writer = CatalogWriter::new(storage.clone());
    writer.initialize().await?;

    // Write that will fail at CAS
    let result = writer.create_namespace("test", WriteOptions::default()).await;
    assert!(result.is_err());

    // Snapshot files may exist, but manifest doesn't point to them
    let manifest = read_manifest(&storage).await?;
    let visible_version = manifest.catalog.snapshot.map(|s| s.version).unwrap_or(0);

    // Any orphaned snapshot directories are not visible
    let orphans = storage.list("snapshots/catalog/").await?;
    for orphan in orphans {
        let orphan_version: u64 = extract_version(&orphan);
        assert!(orphan_version <= visible_version,
            "Orphan v{} must not be newer than visible v{}", orphan_version, visible_version);
    }
}

#[tokio::test]
async fn invariant_compaction_idempotent() {
    let storage = TestStorage::new();
    let compactor = Compactor::new(storage.clone());

    // Write events
    write_test_events(&storage, 10).await;

    // First compaction
    let result1 = compactor.compact("executions").await?;

    // Second compaction with same events (no new events)
    let result2 = compactor.compact("executions").await?;

    // Output must be identical
    assert_eq!(result1.snapshot_version, result2.snapshot_version);
    assert_eq!(result1.rows_written, result2.rows_written);
}

#[tokio::test]
async fn invariant_readers_parquet_only() {
    // Use TracingBackend that records all path accesses
    let backend = TracingBackend::new();
    let storage = ScopedStorage::new(backend.clone(), "acme", "prod")?;

    // Initialize with data
    let writer = CatalogWriter::new(storage.clone());
    writer.initialize().await?;
    writer.create_namespace("test", WriteOptions::default()).await?;

    // Read operations
    let reader = CatalogReader::new(storage.clone());
    let _ = reader.list_namespaces().await?;
    let _ = reader.get_namespace("test").await?;

    // Verify: NO ledger paths accessed
    let accessed = backend.accessed_paths();
    for path in accessed {
        assert!(!path.contains("ledger/"),
            "Reader accessed ledger path: {}", path);
    }
}

#[tokio::test]
async fn invariant_no_listing_dependency() {
    // Use NoListBackend that fails on list() calls
    let backend = NoListBackend::new();
    let storage = ScopedStorage::new(backend, "acme", "prod")?;

    // Write and read operations should work without listing
    let writer = CatalogWriter::new(storage.clone());
    writer.initialize().await?;
    writer.create_namespace("test", WriteOptions::default()).await?;

    let reader = CatalogReader::new(storage.clone());
    let namespaces = reader.list_namespaces().await?;
    assert_eq!(namespaces.len(), 1);
    // If we get here, no list() was required for correctness
}
```

**Acceptance:**

- [ ] All 6 invariant tests implemented
- [ ] Tests use failure injection (FailingBackend, TracingBackend, NoListBackend)
- [ ] CI runs invariant tests on every PR
- [ ] Tests are in dedicated `tests/invariants.rs` module
- [ ] Invariant violations cause test failures, not warnings

### Exit Criteria (M2)
- All Tier-2 ledger writes use `CatalogEvent` envelope
- Compactor orders by timestamp + event_id
- Per-domain watermarks implemented
- CAS retry with backoff implemented
- Correctness tests: duplicates + out-of-order + late events
- **Invariant test suite implemented and passing in CI**

---

## Milestone 3: CatalogReader/CatalogWriter Facades

**Rationale:** Remove stubs, expose real behavior through thin facades that preserve two-tier model.

### Tasks

#### Task 3.0: Define WriteOptions Struct (First-Class Write Context)

Standardize idempotency, actor, request context, and optimistic locking in one struct.

```rust
/// Strongly-typed idempotency key
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(String);

/// Version for optimistic locking (if-match guard)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotVersion(u64);

/// Write options for all mutating operations
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Idempotency key for safe retries
    pub idempotency_key: Option<IdempotencyKey>,
    /// Optimistic lock: fail if current version doesn't match
    pub if_match: Option<SnapshotVersion>,
    /// Actor performing the write (service/user)
    pub actor: Option<String>,
    /// Request ID for tracing/correlation
    pub request_id: Option<String>,
}

impl WriteOptions {
    pub fn with_idempotency(key: impl Into<String>) -> Self {
        Self {
            idempotency_key: Some(IdempotencyKey(key.into())),
            ..Default::default()
        }
    }

    pub fn with_if_match(version: u64) -> Self {
        Self {
            if_match: Some(SnapshotVersion(version)),
            ..Default::default()
        }
    }
}
```

**Acceptance:**

- [ ] `WriteOptions` used on all mutating methods
- [ ] No loose `Option<&str>` idempotency params
- [ ] `if_match` returns 409/412 on version mismatch
- [ ] Actor + request_id propagated to logs/traces

#### Task 3.1: Implement CatalogWriter (Domain-Split Architecture)

**Key Design Decision:** CatalogWriter internally uses **separate Tier1Writers per domain** to avoid lock contention between catalog DDL and lineage writes.

```rust
pub struct CatalogWriter {
    storage: ScopedStorage,
    /// Tier-1 writer for catalog domain (namespaces/tables/columns)
    catalog_tier1: Tier1Writer,
    /// Tier-1 writer for lineage domain (edges)
    lineage_tier1: Tier1Writer,
}

impl CatalogWriter {
    pub fn new(storage: ScopedStorage) -> Self {
        Self {
            catalog_tier1: Tier1Writer::new(storage.clone(), "catalog"),
            lineage_tier1: Tier1Writer::new(storage.clone(), "lineage"),
            storage,
        }
    }

    /// Initialize catalog with empty Parquet state tables + checksums.
    /// Idempotent: safe to call multiple times.
    pub async fn initialize(&self) -> Result<()>;

    // === Namespaces (Tier 1 - catalog domain) ===

    pub async fn create_namespace(
        &self,
        name: &str,
        opts: WriteOptions,
    ) -> Result<Namespace>;

    pub async fn delete_namespace(
        &self,
        name: &str,
        opts: WriteOptions,
    ) -> Result<()>;

    // === Tables (Tier 1 - catalog domain) ===

    pub async fn register_table(
        &self,
        req: RegisterTableRequest,
        opts: WriteOptions,
    ) -> Result<Table>;

    /// Update table with optimistic locking.
    /// If `opts.if_match` is set and doesn't match current version,
    /// returns `Err(CatalogError::PreconditionFailed)`.
    pub async fn update_table(
        &self,
        namespace: &str,
        name: &str,
        patch: TablePatch,
        opts: WriteOptions,
    ) -> Result<Table>;

    pub async fn drop_table(
        &self,
        namespace: &str,
        name: &str,
        opts: WriteOptions,
    ) -> Result<()>;

    // === Lineage (Tier 1 - lineage domain, separate lock) ===

    pub async fn add_lineage_edge(
        &self,
        edge: LineageEdge,
        opts: WriteOptions,
    ) -> Result<LineageEdge>;

    pub async fn add_lineage_edges(
        &self,
        edges: Vec<LineageEdge>,
        opts: WriteOptions,
    ) -> Result<Vec<LineageEdge>>;

    // === Tier 2 (separate object, not a reference) ===

    /// Create an EventWriter for Tier-2 event ingestion.
    /// Returns a new writer (not a reference) to maintain tier separation.
    pub fn event_writer(&self, source: EventSource) -> EventWriter {
        EventWriter::new(self.storage.clone(), source)
    }
}
```

**Acceptance:**

- [ ] Separate Tier1Writers for catalog vs lineage domains
- [ ] Lineage writes don't block catalog DDL
- [ ] All operations take `WriteOptions`
- [ ] `if_match` guard returns 409/412 on mismatch
- [ ] `event_writer()` returns owned EventWriter (not `&EventWriter`)
- [ ] Contract tests for each operation

#### Task 3.2: Implement CatalogReader with Freshness Metadata

```rust
/// Freshness metadata for snapshot reads
#[derive(Debug, Clone)]
pub struct SnapshotFreshness {
    /// Snapshot version ID
    pub version: SnapshotVersion,
    /// When this snapshot was published
    pub published_at: DateTime<Utc>,
    /// Optional: tail commit range for incremental refresh
    pub tail: Option<TailRange>,
}

pub struct CatalogReader {
    storage: ScopedStorage,
}

impl CatalogReader {
    pub fn new(storage: ScopedStorage) -> Self;

    // === Fast reads (snapshot-based) ===

    pub async fn list_namespaces(&self) -> Result<Vec<Namespace>>;
    pub async fn get_namespace(&self, name: &str) -> Result<Option<Namespace>>;
    pub async fn list_tables(&self, namespace: &str) -> Result<Vec<Table>>;
    pub async fn get_table(&self, namespace: &str, name: &str) -> Result<Option<Table>>;
    pub async fn get_lineage(&self, table_id: &str) -> Result<LineageGraph>;

    /// Get freshness metadata for a domain snapshot
    pub async fn get_freshness(&self, domain: &str) -> Result<SnapshotFreshness>;

    // === Signed URL minting (manifest-driven allowlist) ===

    /// Mint signed URLs for browser-direct reads.
    /// ONLY allows paths enumerated in the manifest's SnapshotInfo.
    pub async fn mint_signed_urls(
        &self,
        paths: Vec<String>,
        ttl: Duration,
    ) -> Result<Vec<SignedUrl>>;

    /// Get all mintable paths from current manifest.
    /// Browser clients call this, then request URLs for specific files.
    pub async fn get_mintable_paths(&self, domain: &str) -> Result<Vec<String>>;
}
```

**Acceptance:**

- [ ] CatalogReader reads from Parquet snapshots
- [ ] Reader never accesses ledger (Invariant 5)
- [ ] Freshness metadata exposed (version, published_at, tail)
- [ ] Signed URL minting uses manifest-driven allowlist
- [ ] Contract tests for each operation

#### Task 3.3: Enhanced SnapshotInfo with Per-File Metadata

The manifest must include per-file metadata for:
1. **Manifest-driven URL allowlist**: Only mint URLs for files in SnapshotInfo
2. **Integrity verification**: Checksums detect corruption/tampering
3. **Query optimization**: Row counts for cost estimation

```rust
/// Metadata for a single Parquet file in a snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotFile {
    /// Relative path within snapshot directory
    pub path: String,
    /// SHA-256 checksum of file contents
    pub checksum_sha256: String,
    /// File size in bytes
    pub byte_size: u64,
    /// Number of rows in the Parquet file
    pub row_count: u64,
    /// Optional: min/max position for L0 delta files
    pub position_range: Option<(u64, u64)>,
}

/// Complete snapshot metadata stored in domain manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// Snapshot version (monotonically increasing)
    pub version: u64,
    /// Snapshot directory path
    pub path: String,
    /// List of files with checksums and metadata
    pub files: Vec<SnapshotFile>,
    /// When this snapshot was published
    pub published_at: DateTime<Utc>,
    /// Total row count across all files
    pub total_rows: u64,
    /// Total size in bytes
    pub total_bytes: u64,
    /// Optional: tail commit range for incremental refresh
    pub tail: Option<TailRange>,
}

/// Domain manifest with enhanced SnapshotInfo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainManifest {
    /// Current published snapshot (None if not yet initialized)
    pub snapshot: Option<SnapshotInfo>,
    /// Watermark for Tier-2 compaction
    pub watermark: Option<Watermark>,
    /// Last commit ID for this domain
    pub last_commit: Option<String>,
}
```

**Acceptance:**

- [ ] SnapshotInfo includes per-file checksums
- [ ] SnapshotInfo includes row counts and byte sizes
- [ ] Signed URL minting validates paths against SnapshotInfo.files
- [ ] Integrity verifier can validate checksums

#### Task 3.4: Tier-1 Parquet Snapshot Writer

Current: Tier-1 only updates JSON manifest fields
Target: Write Parquet state tables with checksums, then CAS-update manifest

```rust
impl Tier1Writer {
    /// Write a new snapshot with all state tables.
    /// Computes checksums and row counts for each file.
    pub async fn write_snapshot(&self, state: &CatalogState) -> Result<SnapshotInfo> {
        let version = self.next_version();
        let dir = CatalogPaths::snapshot_dir(self.domain, version);

        let mut files = Vec::new();

        // Write each Parquet file and collect metadata
        for (name, data) in [
            ("namespaces.parquet", &state.namespaces),
            ("tables.parquet", &state.tables),
            ("columns.parquet", &state.columns),
        ] {
            let path = format!("{dir}/{name}");
            let (bytes, row_count) = self.serialize_parquet(data)?;
            let checksum = sha256_hex(&bytes);

            self.storage.put(&path, bytes.clone()).await?;

            files.push(SnapshotFile {
                path: path.clone(),
                checksum_sha256: checksum,
                byte_size: bytes.len() as u64,
                row_count,
                position_range: None,
            });
        }

        let snapshot_info = SnapshotInfo {
            version,
            path: dir,
            files: files.clone(),
            published_at: Utc::now(),
            total_rows: files.iter().map(|f| f.row_count).sum(),
            total_bytes: files.iter().map(|f| f.byte_size).sum(),
            tail: None,
        };

        // Atomic visibility via manifest CAS
        self.update(|manifest| {
            manifest.snapshot = Some(snapshot_info.clone());
            Ok(())
        }).await?;

        Ok(snapshot_info)
    }
}
```

**Acceptance:**

- [ ] Parquet snapshots written with checksums computed
- [ ] Row counts and byte sizes recorded per file
- [ ] Snapshot visible only after manifest CAS succeeds
- [ ] Failure between write and CAS leaves snapshot orphaned (not visible)
- [ ] Crash recovery test passes

#### Task 3.5: Initialize Writes Empty Parquet + Checksums

`initialize()` must create valid empty state so readers and URL minting work immediately.

```rust
impl CatalogWriter {
    /// Initialize catalog with empty Parquet state tables.
    ///
    /// Creates:
    /// - Empty namespaces.parquet, tables.parquet, columns.parquet
    /// - Empty lineage_edges.parquet (in lineage domain)
    /// - SnapshotInfo with checksums for all files
    /// - Published manifest pointers
    ///
    /// Idempotent: safe to call multiple times.
    pub async fn initialize(&self) -> Result<()> {
        // Initialize catalog domain (empty state)
        let empty_catalog = CatalogState::empty();
        self.catalog_tier1.write_snapshot(&empty_catalog).await?;

        // Initialize lineage domain (empty edges)
        let empty_lineage = LineageState::empty();
        self.lineage_tier1.write_snapshot(&empty_lineage).await?;

        Ok(())
    }
}
```

**Acceptance:**

- [ ] `initialize()` creates empty Parquet files with valid schemas
- [ ] Empty files have checksums in SnapshotInfo
- [ ] Readers work immediately after initialization
- [ ] `get_mintable_paths()` returns valid paths after initialization
- [ ] Idempotent: second call is a no-op

#### Task 3.6: Parquet Schema Contract Tests

```rust
#[test]
fn contract_namespaces_parquet_schema() {
    // Write namespace, read Parquet
    // Assert schema matches golden definition
    // Assert required fields present
}

#[test]
fn contract_tables_parquet_schema() {
    // Same pattern
}

#[test]
fn contract_lineage_edges_parquet_schema() {
    // Same pattern
}
```

**Acceptance:**
- [ ] Golden Parquet schema files checked in
- [ ] Schema compatibility tests prevent breaking changes
- [ ] CI runs schema contract tests

#### Task 3.7: Tier-1 Atomic Publish Failure Injection Test

```rust
#[tokio::test]
async fn tier1_crash_between_snapshot_and_cas() {
    // Use FailingBackend that fails on manifest CAS
    let backend = FailingBackend::fail_on("manifests/catalog.manifest.json");
    let storage = ScopedStorage::new(backend, "acme", "prod")?;

    let writer = CatalogWriter::new(storage.clone());
    writer.initialize().await?;

    // Attempt write that will fail at CAS
    let result = writer.create_namespace("test", WriteOptions::default()).await;
    assert!(result.is_err());

    // Verify: snapshot files exist but are NOT visible
    let manifest = read_manifest(&storage).await?;
    assert!(manifest.catalog.snapshot.is_none() ||
            manifest.catalog.snapshot.as_ref().unwrap().version == 0,
            "Snapshot must not be visible after failed CAS");

    // Orphaned files exist (GC will clean later)
    let orphans = storage.list("snapshots/catalog/v1/").await?;
    assert!(!orphans.is_empty(), "Orphaned files should exist");
}
```

**Acceptance:**

- [ ] Failure injection test for crash between write and CAS
- [ ] Snapshot not visible after failed CAS
- [ ] Orphaned files left for GC (not reader-visible)

### Exit Criteria (M3)

- [ ] CatalogWriter/CatalogReader replace stubs with real implementations
- [ ] WriteOptions struct used on all mutating operations
- [ ] Separate Tier1Writers for catalog vs lineage domains
- [ ] `if_match` guard prevents lost updates (409/412 on mismatch)
- [ ] SnapshotInfo includes per-file checksums, row counts, byte sizes
- [ ] `initialize()` creates empty Parquet files with valid schemas
- [ ] Tier-1 writes Parquet snapshots with atomic publish via CAS
- [ ] Failure injection test validates crash recovery
- [ ] Schema contract tests for all Parquet tables
- [ ] `event_writer()` returns owned EventWriter (not reference)

---

## Milestone 4: REST API Layer (arco-api)

### Tasks

#### Task 4.1: Axum Server Skeleton
```rust
// crates/arco-api/src/main.rs
#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .nest("/api/v1", api_routes())
        .layer(TraceLayer::new_for_http())
        .layer(auth_middleware());

    axum::serve(listener, app).await.unwrap();
}
```

#### Task 4.2: Request Context + Auth Middleware
```rust
pub struct RequestContext {
    pub tenant: TenantId,
    pub workspace: WorkspaceId,
    pub request_id: String,
    pub idempotency_key: Option<String>,
}

// Middleware extracts tenant/workspace from JWT claims
// Constructs ScopedStorage from auth context
```

**Acceptance:**
- [x] Tenant/workspace extracted from verified JWT
- [x] ScopedStorage constructed from auth context
- [x] Request ID propagated for tracing
- [x] Idempotency key extracted from header

#### Task 4.3: API Routes
```
POST   /api/v1/namespaces
DELETE /api/v1/namespaces/{name}
GET    /api/v1/namespaces
GET    /api/v1/namespaces/{name}

POST   /api/v1/namespaces/{namespace}/tables
PUT    /api/v1/namespaces/{namespace}/tables/{name}
DELETE /api/v1/namespaces/{namespace}/tables/{name}
GET    /api/v1/namespaces/{namespace}/tables
GET    /api/v1/namespaces/{namespace}/tables/{name}

POST   /api/v1/lineage/edges
GET    /api/v1/lineage/{table_id}

POST   /api/v1/browser/urls
```

**Acceptance:**
- [x] All routes implemented and tested
- [x] Error responses follow consistent format
- [x] OpenAPI spec generated and validated

#### Task 4.4: OpenAPI Contract Tests
```rust
#[test]
fn contract_openapi_matches_implementation() {
    // Parse OpenAPI spec
    // For each route, verify handler exists
    // For each request/response type, verify matches spec
}
```

**Acceptance:**
- [x] OpenAPI 3.1 spec checked in
- [x] Contract tests validate spec matches implementation
- [x] Breaking changes detected in CI

### Exit Criteria (M4)
- API routes respond correctly
- Request context from auth
- OpenAPI spec checked in and validated
- Error model consistent

---

## Milestone 5: Browser Read Path + Signed URLs

**Rationale:** This is the core differentiator. Browser reads via DuckDB-WASM, API only mints URLs.

### Tasks

#### Task 5.1: Signed URL Endpoint (Manifest-Driven Allowlist)

**Key Security Design:** Only mint URLs for paths enumerated in the current manifest's `SnapshotInfo.files`. This prevents:
- Minting URLs for ledger files (Tier-2 internal)
- Minting URLs for manifest files (metadata leak)
- Minting URLs for arbitrary paths (path traversal)

```rust
// POST /api/v1/browser/urls
pub async fn mint_browser_urls(
    ctx: RequestContext,
    reader: CatalogReader,
    Json(req): Json<MintUrlsRequest>,
) -> Result<Json<MintUrlsResponse>> {
    // 1. Get mintable paths from manifest
    let mintable = reader.get_mintable_paths(&req.domain).await?;
    let mintable_set: HashSet<_> = mintable.iter().collect();

    // 2. Validate ALL requested paths are in allowlist
    for path in &req.paths {
        if !mintable_set.contains(path) {
            return Err(ApiError::Forbidden(format!(
                "Path not in manifest allowlist: {path}"
            )));
        }
    }

    // 3. Mint signed URLs with bounded TTL
    let ttl = req.ttl_seconds
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(900)) // 15 min default
        .min(Duration::from_secs(3600));     // 1 hour max

    let urls = reader.mint_signed_urls(req.paths, ttl).await?;

    // 4. Return URLs only (never proxy data)
    Ok(Json(MintUrlsResponse { urls }))
}
```

**Acceptance:**

- [x] Only paths in SnapshotInfo.files are mintable
- [x] Arbitrary paths rejected with 403 Forbidden
- [x] TTL bounded (max 1 hour, default 15 min)
- [x] Tenant/workspace scoping from auth context
- [x] No data proxying (URL-only response)
- [x] Query params NOT logged (signed URL secrets)

#### Task 5.2: E2E Browser Read Acceptance Test

**This is the MVP acceptance test:**

```rust
#[tokio::test]
async fn e2e_api_write_to_browser_query() {
    // 1. API write: create namespace + table
    let client = TestClient::new();
    client.create_namespace("analytics").await;
    client.register_table("analytics", "events").await;

    // 2. Tier-1 snapshot published
    let manifest = read_manifest().await;
    assert!(manifest.catalog.snapshot_path.is_some());

    // 3. Browser URL endpoint mints signed URLs
    let urls = client.mint_browser_urls(vec![
        manifest.catalog.snapshot_path.unwrap()
    ]).await;
    assert!(!urls.is_empty());

    // 4. DuckDB can query the signed URLs
    let conn = duckdb::Connection::open_in_memory()?;
    let rows: Vec<Row> = conn.query(&format!(
        "SELECT * FROM read_parquet('{}')", urls[0]
    ))?;
    assert!(rows.iter().any(|r| r.get::<String>("name") == "events"));
}
```

**Acceptance:**
- [x] E2E test passes in CI
- [x] Test uses real signed URLs (not mocked)
- [x] DuckDB query returns expected data
- [x] Test validates entire read path

#### Task 5.3: CORS Configuration
```rust
// Configure CORS for browser access
.layer(CorsLayer::new()
    .allow_origin(AllowOrigin::list(allowed_origins))
    .allow_methods([Method::GET, Method::POST])
    .allow_headers([CONTENT_TYPE, AUTHORIZATION]))
```

**Acceptance:**
- [x] CORS configured for allowed origins
- [x] Preflight requests handled
- [x] No overly permissive wildcards in production

#### Task 5.4: Signed URL Security Tests

**Critical:** Signed URLs behave like bearer tokens. Comprehensive security tests prevent abuse.

```rust
// crates/arco-api/tests/signed_url_security.rs

#[tokio::test]
async fn security_cannot_mint_urls_for_ledger() {
    let client = authenticated_client("tenant-a", "workspace-1");

    // Attempt to mint URL for ledger path (should be forbidden)
    let result = client.mint_browser_urls(MintUrlsRequest {
        domain: "executions".to_string(),
        paths: vec!["ledger/executions/event-123.json".to_string()],
        ttl_seconds: None,
    }).await;

    assert_eq!(result.status(), 403);
    assert!(result.body().contains("not in manifest allowlist"));
}

#[tokio::test]
async fn security_cannot_mint_urls_for_manifest() {
    let client = authenticated_client("tenant-a", "workspace-1");

    // Attempt to mint URL for manifest (should be forbidden)
    let result = client.mint_browser_urls(MintUrlsRequest {
        domain: "catalog".to_string(),
        paths: vec!["manifests/catalog.manifest.json".to_string()],
        ttl_seconds: None,
    }).await;

    assert_eq!(result.status(), 403);
}

#[tokio::test]
async fn security_cannot_mint_cross_tenant_urls() {
    // Authenticated as tenant-a
    let client = authenticated_client("tenant-a", "workspace-1");

    // Attempt to mint URL for tenant-b's data (should fail)
    let result = client.mint_browser_urls(MintUrlsRequest {
        domain: "catalog".to_string(),
        paths: vec!["tenant=tenant-b/workspace=other/snapshots/catalog/v1/tables.parquet".to_string()],
        ttl_seconds: None,
    }).await;

    assert_eq!(result.status(), 403);
}

#[tokio::test]
async fn security_ttl_bounded() {
    let client = authenticated_client("tenant-a", "workspace-1");

    // Request 24-hour TTL (should be capped to 1 hour)
    let result = client.mint_browser_urls(MintUrlsRequest {
        domain: "catalog".to_string(),
        paths: vec!["snapshots/catalog/v1/tables.parquet".to_string()],
        ttl_seconds: Some(86400), // 24 hours
    }).await;

    // Should succeed but with capped TTL
    assert!(result.status().is_success());
    // URL expiration should be <= 1 hour from now
    let url = &result.body().urls[0];
    let expiry = extract_expiry_from_signed_url(url);
    assert!(expiry <= Utc::now() + Duration::hours(1));
}

#[tokio::test]
async fn security_path_traversal_rejected() {
    let client = authenticated_client("tenant-a", "workspace-1");

    // Attempt path traversal
    let result = client.mint_browser_urls(MintUrlsRequest {
        domain: "catalog".to_string(),
        paths: vec!["../../../etc/passwd".to_string()],
        ttl_seconds: None,
    }).await;

    assert_eq!(result.status(), 403);
}
```

**Acceptance:**

- [x] Security tests for ledger path rejection
- [x] Security tests for manifest path rejection
- [x] Security tests for cross-tenant rejection
- [x] Security tests for TTL bounds enforcement
- [x] Security tests for path traversal rejection
- [x] All tests run in CI

#### Task 5.5: Log Redaction Policy (URLs are Secrets)

**Critical:** Signed URLs contain secrets (signature + expiry). Logging them creates a credential leak vector.

```rust
// crates/arco-api/src/logging.rs

/// Middleware that redacts sensitive query parameters from logs
pub struct LogRedactionLayer;

impl LogRedactionLayer {
    /// Patterns to redact from logs
    const REDACT_PATTERNS: &'static [&'static str] = &[
        "X-Goog-Signature",
        "X-Amz-Signature",
        "sig=",
        "Signature=",
        "token=",
    ];

    pub fn redact_url(url: &str) -> String {
        let mut redacted = url.to_string();
        for pattern in Self::REDACT_PATTERNS {
            if let Some(start) = redacted.find(pattern) {
                // Redact from pattern to next & or end of string
                let end = redacted[start..].find('&')
                    .map(|i| start + i)
                    .unwrap_or(redacted.len());
                redacted.replace_range(start..end, &format!("{}=REDACTED", pattern.trim_end_matches('=')));
            }
        }
        redacted
    }
}

// In request handler
tracing::info!(
    tenant = %ctx.tenant,
    workspace = %ctx.workspace,
    paths_count = req.paths.len(),
    // DO NOT log: req.paths (may contain sensitive data)
    // DO NOT log: response.urls (contains signed URLs)
    "Minting browser URLs"
);
```

**Acceptance:**

- [x] Signed URLs never logged in full
- [x] Query parameters redacted in all log outputs
- [x] Log redaction tested with unit tests
- [x] No secrets in structured log fields

### Exit Criteria (M5)
- Signed URL endpoint works
- E2E test: API write → Parquet publish → signed URLs → DuckDB query
- CORS configured
- No data proxying (URLs only)
- **Signed URL security tests pass (ledger, manifest, cross-tenant, TTL, traversal)**
- **Log redaction enforced (no signed URLs in logs)**

---

## Milestone 6: Security Hardening

### Tasks

#### Task 6.1: Signed URL Security
- Tenant/workspace scoping from auth context
- Strict allowlist of readable paths (snapshots only)
- TTL enforced (15 min default, configurable)
- Do NOT log full signed URL query strings

**Acceptance:**
- [ ] Cross-tenant URL minting rejected
- [ ] Ledger/manifest paths rejected
- [ ] TTL < 1 hour enforced
- [ ] Logs redact query params

#### Task 6.2: Rate Limiting + Quotas
```rust
// Denial-of-wallet prevention
.layer(RateLimitLayer::new(
    per_tenant_limit: 1000/min,
    per_endpoint_limit: {
        "/api/v1/browser/urls": 100/min,  // URL minting is expensive
        "default": 500/min,
    }
))
```

**Acceptance:**
- [ ] Per-tenant rate limits
- [ ] URL minting has lower limit (expensive operation)
- [ ] 429 responses with Retry-After header
- [ ] Metrics for rate limit hits

#### Task 6.3: IAM Least Privilege

| Service Account | Permissions | Rationale |
|-----------------|-------------|-----------|
| arco-api | storage.objects.get, storage.objects.create | Read snapshots, write manifests |
| arco-compactor | storage.objects.* | Full access for compaction |
| browser (via signed URL) | storage.objects.get | Read-only, time-limited |

**Acceptance:**
- [ ] Separate service accounts for API and compactor
- [ ] No broad objectAdmin (or justified in ADR)
- [ ] Signed URLs use read-only scope
- [ ] Terraform enforces IAM config

### Exit Criteria (M6)
- Signed URL security hardened
- Rate limits + quotas deployed
- IAM least privilege enforced
- No secrets in logs

---

## Milestone 7: Deployment + Rollback

### Tasks

#### Task 7.1: Terraform Modules
```hcl
# infra/terraform/modules/arco-api/
# - Cloud Run service
# - Service account
# - IAM bindings
# - Secret manager references

# infra/terraform/modules/arco-compactor/
# - Cloud Functions / Cloud Run job
# - Scheduler trigger
# - Service account
# - IAM bindings
```

**Acceptance:**
- [ ] API deployable to Cloud Run
- [ ] Compactor deployable as scheduled job
- [ ] Secrets from Secret Manager (not env vars)
- [ ] All resources tagged for cost tracking

#### Task 7.2: Deployment Choreography (Compactor-First Hard Gate)

**Critical Design Principle:** Compactor must be healthy before API serves traffic. This prevents:
- Writes succeeding but reads returning stale data
- Ledger events accumulating without compaction
- Browser clients seeing empty/stale Parquet snapshots

```bash
#!/bin/bash
# scripts/deploy.sh
# HARD GATE: Compactor must be healthy before API deploy proceeds

set -euo pipefail

COMPACTOR_HEALTHY=false
MAX_WAIT_SECONDS=300
WAIT_INTERVAL=10

echo "=== Phase 1: Deploy Compactor ==="
gcloud run deploy arco-compactor \
    --image="${COMPACTOR_IMAGE}" \
    --region="${REGION}" \
    --service-account="${COMPACTOR_SA}"

echo "=== Phase 2: Wait for Compactor Health (HARD GATE) ==="
# Compactor must complete at least one successful compaction
START_TIME=$(date +%s)
while [ "$COMPACTOR_HEALTHY" = false ]; do
    ELAPSED=$(($(date +%s) - START_TIME))
    if [ $ELAPSED -gt $MAX_WAIT_SECONDS ]; then
        echo "ERROR: Compactor health check timed out after ${MAX_WAIT_SECONDS}s"
        echo "ABORTING: API deployment blocked until compactor is healthy"
        exit 1
    fi

    # Check compactor health endpoint
    HEALTH=$(curl -s "${COMPACTOR_URL}/health" | jq -r '.status')
    LAST_COMPACTION=$(curl -s "${COMPACTOR_URL}/health" | jq -r '.last_successful_compaction')

    if [ "$HEALTH" = "healthy" ] && [ "$LAST_COMPACTION" != "null" ]; then
        echo "Compactor healthy with last compaction at: ${LAST_COMPACTION}"
        COMPACTOR_HEALTHY=true
    else
        echo "Waiting for compactor... (${ELAPSED}s elapsed)"
        sleep $WAIT_INTERVAL
    fi
done

echo "=== Phase 3: Deploy API (only after compactor healthy) ==="
gcloud run deploy arco-api \
    --image="${API_IMAGE}" \
    --region="${REGION}" \
    --service-account="${API_SA}"

echo "=== Phase 4: Smoke Tests ==="
./scripts/smoke-test.sh

echo "=== Phase 5: Verify Invariants ==="
./scripts/verify-invariants.sh

echo "=== Deployment Complete ==="
```

**Health Endpoint Contract:**
```rust
// Compactor health endpoint must report:
#[derive(Serialize)]
struct CompactorHealth {
    status: String,                          // "healthy" | "degraded" | "unhealthy"
    last_successful_compaction: Option<DateTime<Utc>>,
    domains_compacted: Vec<String>,
    pending_events: u64,                     // Events waiting for compaction
}
```

**Acceptance:**

- [ ] Deployment script enforces compactor-first ordering
- [ ] API deploy blocked until compactor reports healthy
- [ ] Compactor health requires at least one successful compaction
- [ ] Timeout with clear error message if compactor unhealthy
- [ ] Script exits non-zero on any failure
- [ ] CI/CD pipeline enforces same ordering

#### Task 7.3: Rollback Script + Drill
```bash
# scripts/rollback.sh
# 1. Roll back API to N-1
gcloud run services update-traffic arco-api --to-revisions=REVISION=100
# 2. Verify API health
./scripts/health-check.sh arco-api
# 3. Roll back compactor if needed
gcloud run services update-traffic arco-compactor --to-revisions=REVISION=100
# 4. Verify invariants
./scripts/verify-invariants.sh
```

**Rollback Drill (DoD Requirement):**
1. Deploy version N to staging
2. Verify health
3. Roll back to N-1
4. Verify health + invariants
5. Document any issues

**Acceptance:**
- [ ] Rollback script tested in staging
- [ ] Rollback drill completed and documented
- [ ] Rollback time < 5 minutes
- [ ] No data corruption from rollback

#### Task 7.4: Anti-Entropy Reconciler Job

**Rationale:** Event-driven compaction (Pub/Sub triggers) is the normal path, but missed notifications can cause silent data lag. The anti-entropy reconciler is a **periodic safety net** for gap detection, not the primary consumer loop.

**Important Design Principle:** The normal read path does NOT depend on bucket listing for correctness (Invariant 6). Anti-entropy uses listing only for **gap detection and repair**, not regular operation.

```rust
// crates/arco-compactor/src/anti_entropy.rs

pub struct AntiEntropyReconciler {
    storage: ScopedStorage,
    compactor: Compactor,
}

impl AntiEntropyReconciler {
    /// Run anti-entropy check for a domain.
    /// Called by scheduled job (e.g., Cloud Scheduler every 5-10 min).
    pub async fn reconcile(&self, domain: &str) -> Result<ReconcileResult> {
        // 1. Read current watermark from manifest
        let manifest = self.read_manifest(domain).await?;
        let watermark = manifest.watermark.unwrap_or_default();

        // 2. List ledger events in bounded window (watermark - buffer to now)
        let window_start = watermark.last_timestamp - Duration::hours(1);
        let events = self.storage.list(&format!(
            "ledger/{domain}/"
        )).await?;

        // 3. Detect gaps: events newer than watermark that haven't been compacted
        let gaps: Vec<_> = events
            .filter(|e| e.timestamp > watermark.last_timestamp)
            .collect();

        if gaps.is_empty() {
            return Ok(ReconcileResult::NoGaps);
        }

        // 4. Trigger compaction if gaps detected
        tracing::warn!(
            domain = domain,
            gap_count = gaps.len(),
            oldest_gap = ?gaps.first(),
            "Anti-entropy detected uncompacted events, triggering compaction"
        );

        self.compactor.compact(domain).await?;

        // 5. Emit metrics for observability
        metrics::counter!("anti_entropy_gaps_detected", gaps.len() as u64, "domain" => domain);

        Ok(ReconcileResult::CompactionTriggered { gap_count: gaps.len() })
    }
}

#[derive(Debug)]
pub enum ReconcileResult {
    NoGaps,
    CompactionTriggered { gap_count: usize },
}
```

**Cloud Scheduler Configuration:**
```yaml
# infra/terraform/modules/arco-compactor/anti_entropy.tf
resource "google_cloud_scheduler_job" "anti_entropy" {
  name     = "arco-anti-entropy"
  schedule = "*/5 * * * *"  # Every 5 minutes
  time_zone = "UTC"

  http_target {
    uri         = "${google_cloud_run_v2_service.compactor.uri}/anti-entropy"
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.compactor.email
    }
  }
}
```

**Acceptance:**

- [ ] Anti-entropy reconciler implemented
- [ ] Scheduled job runs every 5-10 minutes
- [ ] Gap detection uses bounded window (not full history)
- [ ] Compaction triggered automatically on gap detection
- [ ] Metrics emitted for gaps detected
- [ ] Does NOT affect normal read path (gap detection only)

### Exit Criteria (M7)
- Deployment to staging succeeds
- Rollback drill passes
- Deployment choreography enforced
- Rollback time bounded
- **Anti-entropy reconciler deployed and running**

---

## Milestone 8: Observability (P1)

### Tasks

#### Task 8.1: Metrics (OpenTelemetry)
```rust
// Key metrics:
// - api_request_duration_seconds{endpoint, status}
// - api_request_total{endpoint, status}
// - compaction_duration_seconds{domain}
// - compaction_events_processed{domain}
// - compaction_lag_seconds{domain}  // time since oldest uncompacted event
// - cas_retry_total{operation}
// - signed_url_minted_total{tenant}
```

#### Task 8.2: Dashboards
- API: Request rate, latency P50/P95/P99, error rate
- Compactor: Run frequency, events processed, lag
- Storage: Object count, size by prefix

#### Task 8.3: Alerts
- API error rate > 1% for 5 min
- Compaction lag > 10 min
- CAS retry rate > 10/min
- Rate limit hits > 100/min per tenant

#### Task 8.4: Correlation IDs
```rust
// Propagate request_id through all operations
// Log format: {"request_id": "...", "tenant": "...", ...}
// Trace parent propagation for distributed tracing
```

### Exit Criteria (M8)
- Metrics exported to Cloud Monitoring
- Dashboard deployed
- Alerts configured
- Correlation IDs in all logs

---

## Milestone 9: Runbooks + Operational Docs (P1)

### Runbooks to Create

1. **Compactor Stuck/Lag**
   - Symptoms: compaction_lag_seconds increasing
   - Diagnosis: Check logs, verify storage access, check for CAS storms
   - Resolution: Restart compactor, increase resources, manual compaction

2. **Lock Contention**
   - Symptoms: High CAS retry rate, slow Tier-1 writes
   - Diagnosis: Check concurrent writers, verify lock TTL
   - Resolution: Reduce writer concurrency, extend lock TTL

3. **Manifest Corruption**
   - Symptoms: JSON parse errors, missing fields
   - Diagnosis: Run integrity verifier, check commit chain
   - Resolution: Restore from last known good commit

4. **CAS Conflict Storm**
   - Symptoms: Many concurrent writers failing
   - Diagnosis: Check for hot partitions, verify backoff
   - Resolution: Reduce concurrency, increase backoff

5. **Signed URL Abuse**
   - Symptoms: High URL minting rate, cost alerts
   - Diagnosis: Check per-tenant metrics
   - Resolution: Reduce rate limits, contact tenant

6. **Signed URL + CORS Issues (Browser Read Path)**
   - Symptoms: DuckDB-WASM fails to read Parquet, CORS errors in browser console
   - Diagnosis:
     - Check CORS headers on storage bucket
     - Verify signed URL TTL hasn't expired
     - Check range-request support (DuckDB needs byte ranges)
     - Verify allowlist paths match manifest SnapshotInfo.files
   - Resolution:
     - Fix CORS config on bucket (allow Origin, Range headers)
     - Increase TTL if needed (but stay under 1 hour)
     - Ensure no CDN caching of signed URLs
     - Check browser network tab for specific error codes

7. **Integrity Verification Failure**
   - Symptoms: integrity_verification_failed alert, checksum mismatch
   - Diagnosis:
     - Run integrity verifier: `ARCO_STORAGE_BUCKET=<bucket> cargo xtask verify-integrity --tenant=<tenant> --workspace=<ws>`
     - Use `--lock-strict` during maintenance windows to fail on any active lock
     - CI runs repo-only checks; storage integrity requires manual execution with credentials
     - Check commit chain continuity
     - Compare SnapshotInfo checksums vs actual file checksums
   - Resolution:
     - If single file corrupted: restore from previous snapshot version
     - If commit chain broken: identify last good commit, restore manifest
     - If widespread: quarantine workspace, investigate root cause
     - For Tier-2: rebuild from ledger via forced compaction

### Exit Criteria (M9)

- [ ] 7 runbooks written and reviewed
- [ ] Runbooks linked from alert definitions
- [ ] On-call rotation documented
- [ ] Escalation paths defined
- [ ] Browser read path troubleshooting documented

---

## Milestone 10: GC + Retention (P0 - MVP Critical)

**Rationale:** Without retention/GC, storage grows unbounded and costs explode. This is not a "nice to have" - it's an **operational necessity** for the "$0 at rest" promise.

**Why P0:** Every write creates artifacts that must eventually be cleaned up:
- Tier-1 snapshots: Old versions accumulate after each DDL change
- Tier-2 ledger: Processed events remain after compaction
- Orphaned files: Failed CAS operations leave unreferenced snapshots

Without GC, a workspace that writes 100 events/day accumulates ~36,500 orphaned files/year.

### Tasks

#### Task 10.1: Retention Policy Definition

```rust
// crates/arco-catalog/src/gc/policy.rs

/// Retention policy for catalog artifacts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Keep last N snapshots per domain (e.g., 10)
    /// Allows rollback to recent versions
    pub keep_snapshots: u32,

    /// Minimum age before deletion (e.g., 24 hours)
    /// Prevents racing with active readers
    pub delay_hours: u32,

    /// Ledger retention after compaction (e.g., 48 hours)
    /// Allows replay for debugging/audit
    pub ledger_retention_hours: u32,

    /// Maximum age for any artifact (e.g., 90 days)
    /// Hard limit for compliance/cost control
    pub max_age_days: u32,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            keep_snapshots: 10,
            delay_hours: 24,
            ledger_retention_hours: 48,
            max_age_days: 90,
        }
    }
}
```

**Acceptance:**

- [ ] Retention policy struct defined with sensible defaults
- [ ] Policy configurable per workspace (override defaults)
- [ ] Policy documented in ADR

#### Task 10.2: GC Job Implementation

```rust
// crates/arco-catalog/src/gc/collector.rs

pub struct GarbageCollector {
    storage: ScopedStorage,
    policy: RetentionPolicy,
}

impl GarbageCollector {
    /// Run GC for a workspace. Called by scheduled job (daily).
    pub async fn collect(&self) -> Result<GcResult> {
        let mut result = GcResult::default();

        // 1. Collect orphaned snapshots
        result.merge(self.gc_orphaned_snapshots().await?);

        // 2. Collect old ledger events
        result.merge(self.gc_compacted_ledger().await?);

        // 3. Collect old snapshot versions
        result.merge(self.gc_old_snapshots().await?);

        // 4. Emit metrics
        metrics::counter!("gc_objects_deleted", result.objects_deleted);
        metrics::counter!("gc_bytes_reclaimed", result.bytes_reclaimed);

        Ok(result)
    }

    /// Delete snapshot directories not referenced by any manifest
    async fn gc_orphaned_snapshots(&self) -> Result<GcResult> {
        let mut result = GcResult::default();

        // Read all domain manifests to get referenced snapshots
        let referenced: HashSet<String> = self.get_referenced_snapshots().await?;

        // List all snapshot directories
        let all_snapshots = self.storage.list("snapshots/").await?;

        for snapshot_dir in all_snapshots {
            if !referenced.contains(&snapshot_dir) {
                // Check age before deletion (delay window)
                let metadata = self.storage.head(&snapshot_dir).await?;
                if metadata.last_modified < Utc::now() - Duration::hours(self.policy.delay_hours as i64) {
                    tracing::info!(path = %snapshot_dir, "Deleting orphaned snapshot");
                    self.storage.delete_prefix(&snapshot_dir).await?;
                    result.objects_deleted += 1;
                    result.bytes_reclaimed += metadata.size;
                }
            }
        }

        Ok(result)
    }

    /// Delete ledger events older than watermark minus retention window
    async fn gc_compacted_ledger(&self) -> Result<GcResult> {
        let mut result = GcResult::default();

        for domain in ["catalog", "lineage", "executions", "search"] {
            let manifest = self.read_manifest(domain).await?;
            let watermark = manifest.watermark.unwrap_or_default();

            // Cutoff: watermark timestamp minus retention window
            let cutoff = watermark.last_timestamp
                - Duration::hours(self.policy.ledger_retention_hours as i64);

            // List and delete old events
            let events = self.storage.list(&format!("ledger/{domain}/")).await?;
            for event_path in events {
                let event_time = extract_timestamp_from_event_id(&event_path)?;
                if event_time < cutoff {
                    self.storage.delete(&event_path).await?;
                    result.objects_deleted += 1;
                }
            }
        }

        Ok(result)
    }

    /// Delete snapshot versions beyond retention count
    async fn gc_old_snapshots(&self) -> Result<GcResult> {
        let mut result = GcResult::default();

        for domain in ["catalog", "lineage", "executions", "search"] {
            let snapshots = self.list_snapshots(domain).await?;

            // Sort by version descending
            let mut snapshots: Vec<_> = snapshots.into_iter().collect();
            snapshots.sort_by(|a, b| b.version.cmp(&a.version));

            // Keep first N, delete rest
            for snapshot in snapshots.into_iter().skip(self.policy.keep_snapshots as usize) {
                if snapshot.age > Duration::hours(self.policy.delay_hours as i64) {
                    tracing::info!(domain = domain, version = snapshot.version, "Deleting old snapshot");
                    self.storage.delete_prefix(&snapshot.path).await?;
                    result.objects_deleted += 1;
                    result.bytes_reclaimed += snapshot.total_bytes;
                }
            }
        }

        Ok(result)
    }
}

#[derive(Debug, Default)]
pub struct GcResult {
    pub objects_deleted: u64,
    pub bytes_reclaimed: u64,
    pub errors: Vec<String>,
}
```

**Cloud Scheduler Configuration:**
```yaml
# infra/terraform/modules/arco-gc/main.tf
resource "google_cloud_scheduler_job" "gc" {
  name     = "arco-gc"
  schedule = "0 2 * * *"  # Daily at 2 AM UTC
  time_zone = "UTC"

  http_target {
    uri         = "${google_cloud_run_v2_service.gc.uri}/gc"
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.gc.email
    }
  }
}
```

**Acceptance:**

- [ ] GC job implemented with all three collection phases
- [ ] Delay window prevents racing with active readers
- [ ] Metrics emitted for objects deleted and bytes reclaimed
- [ ] Dry-run mode for testing without deletion
- [ ] Errors logged but don't abort entire GC run

#### Task 10.3: GC Observability

```rust
// Key metrics for GC health
// - gc_run_duration_seconds
// - gc_objects_deleted{type="snapshot"|"ledger"|"orphan"}
// - gc_bytes_reclaimed
// - gc_errors_total
// - gc_last_successful_run_timestamp
```

**Alert Thresholds:**
- `gc_last_successful_run_timestamp` > 48 hours → Critical
- `gc_errors_total` > 10/day → Warning
- Storage growth > 10% week-over-week despite GC → Warning

**Acceptance:**

- [ ] GC metrics exported to Cloud Monitoring
- [ ] Alerts configured for GC failures
- [ ] Dashboard shows GC health and trends

#### Task 10.4: GC Runbook

Add to M9 runbooks:

**8. GC Failure / Storage Growth**
- Symptoms: `gc_last_successful_run` alert, unexpected storage costs
- Diagnosis:
  - Check GC job logs for errors
  - Verify IAM permissions for deletion
  - Check if retention policy is too aggressive (delay too long)
  - Look for workspaces with unusually high write rates
- Resolution:
  - Fix IAM if permission errors
  - Adjust retention policy if needed
  - Manual GC run: `cargo xtask gc --workspace=<ws> --dry-run`
  - If orphans accumulating: check for failed CAS operations

**Acceptance:**

- [ ] GC runbook written
- [ ] Linked from GC alerts

### Exit Criteria (M10)

- [ ] Retention policy defined and configurable
- [ ] GC job deployed and running daily
- [ ] Orphaned snapshots cleaned up
- [ ] Old ledger events cleaned up
- [ ] Old snapshot versions cleaned up
- [ ] Metrics and alerts configured
- [ ] Runbook written
- [ ] Storage growth bounded (verified in staging)

---

## Milestone 11: Engineering System Hardening (P1)

**Rationale:** Prevent regression of architectural decisions through automated enforcement. Make it hard to accidentally break invariants or compatibility.

### Tasks

#### Task 11.1: PR Checklist for Invariants

Add to `.github/pull_request_template.md`:

```markdown
## Invariant Checklist

Before merging, verify:

### Architecture Invariants
- [ ] Two-tier consistency model preserved (Tier-1 strong via lock+CAS, Tier-2 eventual via append+compact)
- [ ] Tenant/workspace scoping enforced at API boundary (no cross-tenant access)
- [ ] Manifest atomic publish semantics maintained (snapshot not visible until CAS succeeds)
- [ ] Readers never need ledger (all reads from Parquet snapshots)
- [ ] No bucket listing required for correctness (anti-entropy uses listing for repair only)

### Code Quality
- [ ] Schema evolution rules followed (additive-only, version gates for breaking changes)
- [ ] No new hardcoded paths (use `CatalogPaths` module)
- [ ] Idempotency keys supported where applicable
- [ ] No secrets in logs (signed URLs, tokens redacted)

### Testing
- [ ] Invariant tests pass (`cargo test --test invariants`)
- [ ] Schema compatibility tests pass
- [ ] ADR conformance check passes (`cargo xtask adr-check`)
```

**Acceptance:**

- [ ] PR template checked in
- [ ] Template includes all architectural invariants
- [ ] Template includes schema evolution rules

#### Task 11.2: Schema Evolution Guardrails (Golden Files + Compatibility)

**Why This Matters:** Schema changes are the #1 source of "works on my machine" production failures. Once clients depend on Parquet schemas, breaking changes cause silent data corruption or query failures.

```rust
// crates/arco-catalog/tests/schema_compatibility.rs

use arrow::datatypes::Schema;
use std::fs;

/// Load golden schema from checked-in JSON file
fn load_golden_schema(name: &str) -> Schema {
    let path = format!("tests/golden_schemas/{name}.schema.json");
    let json = fs::read_to_string(&path)
        .expect(&format!("Golden schema not found: {path}"));
    serde_json::from_str(&json).expect("Invalid golden schema JSON")
}

/// Get current schema from code
fn current_namespaces_schema() -> Schema {
    // Build schema from current struct definitions
    NamespacesTable::arrow_schema()
}

/// Check if new schema is backward compatible with golden
fn is_backward_compatible(golden: &Schema, current: &Schema) -> bool {
    // Rule 1: All golden fields must exist in current
    for golden_field in golden.fields() {
        match current.field_with_name(golden_field.name()) {
            Ok(current_field) => {
                // Rule 2: Field types must be compatible
                if !types_compatible(golden_field.data_type(), current_field.data_type()) {
                    return false;
                }
                // Rule 3: Nullable -> non-nullable is breaking
                if golden_field.is_nullable() && !current_field.is_nullable() {
                    return false;
                }
            }
            Err(_) => return false, // Field removed = breaking
        }
    }
    // Rule 4: New fields must be nullable or have defaults
    for current_field in current.fields() {
        if golden.field_with_name(current_field.name()).is_err() {
            if !current_field.is_nullable() {
                return false; // New non-nullable field = breaking
            }
        }
    }
    true
}

#[test]
fn schema_namespaces_backward_compatible() {
    let golden = load_golden_schema("namespaces");
    let current = current_namespaces_schema();
    assert!(
        is_backward_compatible(&golden, &current),
        "Namespaces schema is not backward compatible with golden. \
         See docs/adr/adr-006-schema-evolution.md for migration guidance."
    );
}

#[test]
fn schema_tables_backward_compatible() {
    let golden = load_golden_schema("tables");
    let current = current_tables_schema();
    assert!(is_backward_compatible(&golden, &current));
}

#[test]
fn schema_lineage_edges_backward_compatible() {
    let golden = load_golden_schema("lineage_edges");
    let current = current_lineage_edges_schema();
    assert!(is_backward_compatible(&golden, &current));
}

// Test that golden files are valid and up to date
#[test]
fn golden_schemas_are_valid_parquet() {
    for name in ["namespaces", "tables", "columns", "lineage_edges"] {
        let schema = load_golden_schema(name);
        // Verify schema can be used to write/read Parquet
        let batch = RecordBatch::new_empty(Arc::new(schema.clone()));
        assert!(batch.num_rows() == 0);
    }
}
```

**Golden Schema Files:**
```
tests/golden_schemas/
├── namespaces.schema.json
├── tables.schema.json
├── columns.schema.json
└── lineage_edges.schema.json
```

**Schema Evolution ADR (ADR-006):**
```markdown
# ADR-006: Schema Evolution Policy

## Decision
All Parquet schema changes must be backward compatible unless a migration is provided.

## Rules
1. **Additive-only by default**: New fields must be nullable
2. **No field removal**: Deprecated fields marked but not removed
3. **No type changes**: Field types are immutable
4. **Version gates**: Breaking changes require version bump + migration

## Migration Process
1. Add new field (nullable) alongside old field
2. Write to both fields during transition
3. After all readers updated, stop writing old field
4. Old field remains in schema (never removed)
```

**Acceptance:**

- [ ] Golden schema files checked in for all Parquet tables
- [ ] Backward compatibility test for each schema
- [ ] ADR-006 written with evolution rules
- [ ] CI fails on incompatible schema changes
- [ ] Migration guidance documented

#### Task 11.3: Integrity Verifier Tooling

```rust
// tools/xtask/src/verify.rs

use anyhow::{bail, Result};

pub struct IntegrityVerifier {
    storage: ScopedStorage,
}

impl IntegrityVerifier {
    /// Verify complete integrity of a workspace.
    /// Returns Ok if all checks pass, Err with details if any fail.
    pub async fn verify(&self) -> Result<IntegrityReport> {
        let mut report = IntegrityReport::default();

        // 1. Verify root manifest exists and is valid JSON
        report.add(self.verify_root_manifest().await);

        // 2. Verify all domain manifests exist
        for domain in ["catalog", "lineage", "executions", "search"] {
            report.add(self.verify_domain_manifest(domain).await);
        }

        // 3. Verify commit chain integrity (hash links)
        report.add(self.verify_commit_chain().await);

        // 4. Verify snapshot files exist at manifest pointers
        report.add(self.verify_snapshot_files().await);

        // 5. Verify Parquet files are readable and match checksums
        report.add(self.verify_parquet_checksums().await);

        // 6. Verify lock files are not stale
        report.add(self.verify_lock_freshness().await);

        if report.has_errors() {
            bail!("Integrity verification failed:\n{}", report);
        }

        Ok(report)
    }

    async fn verify_parquet_checksums(&self) -> CheckResult {
        let mut result = CheckResult::new("parquet_checksums");

        for domain in ["catalog", "lineage"] {
            let manifest = self.read_manifest(domain).await?;
            if let Some(snapshot) = manifest.snapshot {
                for file in snapshot.files {
                    let bytes = self.storage.get(&file.path).await?;
                    let actual_checksum = sha256_hex(&bytes);

                    if actual_checksum != file.checksum_sha256 {
                        result.add_error(format!(
                            "Checksum mismatch for {}: expected {}, got {}",
                            file.path, file.checksum_sha256, actual_checksum
                        ));
                    }
                }
            }
        }

        result
    }
}

#[derive(Debug, Default)]
pub struct IntegrityReport {
    checks: Vec<CheckResult>,
}

impl IntegrityReport {
    pub fn has_errors(&self) -> bool {
        self.checks.iter().any(|c| !c.errors.is_empty())
    }
}
```

**CLI Integration:**
```bash
# Verify a specific workspace
ARCO_STORAGE_BUCKET=arco-prod cargo xtask verify-integrity --tenant=acme --workspace=prod

# Verify with detailed output
ARCO_STORAGE_BUCKET=arco-prod cargo xtask verify-integrity --tenant=acme --workspace=prod --verbose

# Dry-run: report issues without failing
ARCO_STORAGE_BUCKET=arco-prod cargo xtask verify-integrity --tenant=acme --workspace=prod --dry-run

# Strict lock mode (maintenance windows only)
ARCO_STORAGE_BUCKET=arco-prod cargo xtask verify-integrity --tenant=acme --workspace=prod --lock-strict
```

**Acceptance:**

- [ ] Integrity verifier implemented with all checks
- [ ] CLI command available (`cargo xtask verify-integrity`)
- [ ] Verbose mode shows detailed check results
- [ ] Can be run as scheduled job for proactive detection
- [ ] Integrates with alerting (metrics emitted)

### Exit Criteria (M11)

- [ ] PR checklist template checked in and enforced
- [ ] Golden schema files for all Parquet tables
- [ ] Schema backward compatibility tests in CI
- [ ] ADR-006 written with evolution policy
- [ ] Integrity verifier CLI available
- [ ] All automated checks integrated into CI

---

## Definition of Done (Full MVP)

### P0 Requirements (Must Have)

**CI & Tooling:**
- [ ] CI gates green (`cargo xtask ci`, `buf lint`, `cargo xtask adr-check`)
- [ ] Tool versions pinned and aligned between CI and local
- [ ] `cargo xtask doctor` validates environment consistency

**Architecture Contracts:**
- [ ] ADRs locked: IDs (ADR-002), manifests (ADR-003), envelope (ADR-004), layout (ADR-005)
- [ ] ADR conformance wired to CI (cargo xtask adr-check)
- [ ] Storage layout canonicalized via `CatalogPaths` module
- [ ] All writers use canonical paths (enforced by tests)

**Event System:**
- [ ] Event envelope (`CatalogEvent`) with deterministic ordering
- [ ] Per-domain watermarks implemented
- [ ] Compactor CAS retry with exponential backoff
- [ ] Late event + duplicate + out-of-order tests passing

**Invariant Test Suite:**
- [ ] All 6 architectural invariants have executable tests
- [ ] Invariant tests run in CI on every PR
- [ ] Failure injection tests (FailingBackend, TracingBackend, NoListBackend)

**Catalog Facades:**
- [ ] CatalogWriter with domain-split architecture (separate Tier1Writers)
- [ ] CatalogReader reads from Parquet snapshots only
- [ ] WriteOptions struct on all mutating operations
- [ ] `if_match` guard for optimistic locking (409/412 on mismatch)
- [ ] SnapshotInfo with per-file checksums, row counts, byte sizes
- [ ] `initialize()` creates empty Parquet files with valid schemas

**API Layer:**
- [x] REST API with auth context (tenant/workspace from JWT)
- [x] OpenAPI spec checked in and validated
- [x] Request ID propagation for tracing

**Browser Read Path:**
- [x] Signed URL endpoint with manifest-driven allowlist
- [x] E2E test: API write → Parquet → signed URL → DuckDB query
- [x] Signed URL security tests (ledger, manifest, cross-tenant, TTL, traversal)
- [x] Log redaction enforced (no signed URLs in logs)
- [x] CORS configured for allowed origins

**Security:**
- [ ] Rate limits per tenant and per endpoint
- [ ] IAM least privilege (separate service accounts)
- [x] TTL bounds on signed URLs (max 1 hour)

**Deployment & Operations:**
- [ ] Deployment to staging succeeds
- [ ] Compactor-first deployment ordering enforced
- [ ] Rollback drill passes (< 5 min rollback time)
- [ ] Anti-entropy reconciler deployed and running

**GC & Retention (MVP Critical):**
- [ ] Retention policy defined and configurable
- [ ] GC job deployed and running daily
- [ ] Orphaned snapshots, old ledger events, old snapshot versions cleaned up
- [ ] Storage growth bounded (verified in staging)

### P1 Requirements (Should Have)

- [ ] Observability: metrics, dashboards, alerts
- [ ] Runbooks for all 8 failure modes
- [ ] PR checklist template for invariants
- [ ] Golden schema files + backward compatibility tests
- [ ] ADR-006 schema evolution policy
- [ ] Integrity verifier tooling (`cargo xtask verify-integrity`)

### Validation Commands

```bash
# === CI Green ===
cargo xtask ci
buf lint proto/
cargo xtask adr-check
cargo xtask doctor

# === Invariant Tests ===
cargo test --test invariants

# === Schema Compatibility ===
cargo test schema_backward_compatible

# === Tier-1 E2E ===
# REST write → Tier-1 snapshot → server query → browser signed URLs → DuckDB query
cargo test e2e_api_write_to_browser_query

# === Tier-2 Resilience ===
# Duplicate events + out-of-order + compactor crash → no double-count + watermark correct
cargo test compactor_handles_out_of_order_events
cargo test compactor_handles_late_events
cargo test compactor_dedupes_by_idempotency_key

# === Security ===
cargo test --test signed_url_security

# === Deployment Verification ===
./scripts/deploy.sh --env=staging
./scripts/rollback.sh --env=staging
./scripts/verify-invariants.sh

# === Integrity Check ===
ARCO_STORAGE_BUCKET=arco-staging cargo xtask verify-integrity --tenant=acme --workspace=staging
```

### Launch Readiness Checklist

Before declaring MVP ready for production:

1. **All P0 checkboxes green**
2. **Staging deployment successful for 48+ hours**
3. **Rollback drill documented with timing**
4. **GC job has run at least 3 times successfully**
5. **Anti-entropy reconciler has detected and repaired at least one gap (or verified none exist)**
6. **On-call rotation documented**
7. **Runbooks reviewed by at least 2 engineers**

---

## Appendix: Quick Reference

### Priority Legend
- **P0**: Must-fix before any MVP launch
- **P1**: Next hardening (can ship without, but address soon)
- **P2**: Post-MVP

### Key Architecture Invariants

1. **Append-only ingest**: Ledger writes use DoesNotExist precondition
2. **Compactor sole Parquet writer**: Only compactor writes to `state/`
3. **Idempotent compaction**: Dedupe by primary key
4. **Atomic publish**: Snapshot visible only after manifest CAS
5. **Readers never need ledger**: All reads from snapshots
6. **Ordering**: timestamp first, event_id tie-breaker

### File Locations

| Artifact | Location |
|----------|----------|
| ADRs | `docs/adr/` |
| Plans | `docs/plans/` |
| Audits | `docs/audits/` |
| Runbooks | `docs/runbooks/` |
| Terraform | `infra/terraform/` |
| API crate | `crates/arco-api/` |
| Catalog crate | `crates/arco-catalog/` |
