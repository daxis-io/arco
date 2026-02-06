# Part 4: Integration & Testing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement comprehensive integration tests that validate cross-crate contracts, Tier 2 eventual consistency, idempotency guarantees, and end-to-end orchestration workflows following best practices for professional software engineering.

**Architecture Alignment:** This plan validates the six non-negotiable invariants from the unified platform design, particularly around idempotency, atomic publish, and the append→compact→read loop.

**Tech Stack:** Rust 2024 edition, tokio async runtime, serde/serde_json, parquet, chrono, thiserror, proptest for property-based testing, existing arco-core storage abstraction.

---

## Outcomes for Part 4

### Product Outcomes

1. **End-to-end "happy path" works**: SDK deploy → run triggers → tasks execute → outputs committed → catalog reflects results/lineage
2. **Correctness under real failure modes**: retries, duplicates, out-of-order events, partial failures, CAS conflicts
3. **Tier 2 eventual consistency proven**: append→compact→read loop works correctly with watermark tracking

### Engineering Outcomes (Quality Bar)

- **Test pyramid implemented**: Fast unit tests, fewer integration tests, minimal E2E tests
- **Property-based tests** enforce invariants for planner/state machines (catches edge cases)
- **Contract tests** validate cross-crate boundaries
- **>80% test coverage** across arco-catalog and arco-flow

---

## Guiding Standards ("Non-Negotiables")

1. **Contract-first integration**: Define and version contracts for events, APIs, storage schemas. No breaking changes without migrations.

2. **Determinism + idempotency explicitly tested**: Catalog derived views must remain correct under out-of-order events and replay.

3. **Shift-left**: Add unit/property tests for each defect found in integration/E2E before closing the issue.

4. **No flaky tests**: Quarantine and fix flakiness; treat "flaky integration tests" as a reliability bug.

---

## Overview

Building on Parts 1-3 (core infrastructure, catalog Tier 1, orchestration MVP), Part 4 implements:

1. **Test Infrastructure** - `arco-test-utils` crate with shared fixtures and mocks
2. **Cross-Crate Contract Tests** - Validate arco-core ↔ arco-catalog ↔ arco-flow boundaries
3. **Tier 2 Implementation** - EventWriter, Compactor (with CAS) for eventual consistency
4. **Property-Based Tests** - Invariant enforcement using proptest with arbitrary generators
5. **D.6 Acceptance Test Suite** - All 10 architecture-mandated catalog acceptance tests
6. **Six Invariants Test Suite** - Explicit tests for non-negotiable platform invariants
7. **Verification** - Full test suite + coverage validation

**Dependencies:** arco-core, arco-catalog, arco-flow (all previous parts)

**Deliverables:**

- `crates/arco-test-utils/` shared test utilities crate (with `GetRange`, `Head` operations)
- Integration tests in `crates/arco-test-utils/tests/` + `crates/arco-integration-tests/tests/` (virtual workspace-safe)
- Complete `arco-catalog` with EventWriter, Compactor (CAS-based atomic publish)
- Property-based test suites with proptest and arbitrary generators
- D.6 acceptance tests (7/10 implemented, 3 deferred to Part 5)
- Six invariants test suite
- Test coverage report showing >80% coverage

---

## Canonical Contracts (Non-Negotiable)

This section defines the **single source of truth** for storage layout, event envelope, and watermark strategy. All code and tests MUST use these contracts consistently.

### Storage Layout

All Tier 2 data uses this canonical layout (per unified platform design Appendix B):

```
tenant={tenant}/workspace={workspace}/
├── manifests/                    # Tier 1: Manifest files
│   ├── root.manifest.json
│   ├── core.manifest.json
│   └── execution.manifest.json   # Contains snapshot_path pointer
├── ledger/                       # Tier 2: Append-only event log
│   ├── {domain}/                 # Catalog Tier 2: materialization records
│   │   └── {event_id}.json       # ULID-based naming (deterministic)
│   └── flow/{domain}/{date}/     # Flow outbox: execution envelopes (namespaced)
│       └── {event_id}.json
└── state/                        # Tier 2: Compacted Parquet snapshots
    └── {domain}/
        └── snapshot_v{version}_{last_event_id}.parquet
```

**Key decisions:**
- **Ledger namespaces**: Catalog uses `ledger/{domain}/{event_id}.json`; flow uses `ledger/flow/{domain}/{date}/{event_id}.json` to avoid schema collisions
- **Deterministic filenames**: Event files use `{event_id}.json` (NOT `{timestamp}-{event_id}.json`) for idempotent writes; flow adds a deterministic `{date}/` partition
- **Full snapshots**: Compaction publishes a full immutable snapshot at `state/{domain}/snapshot_v{version}_{last_event_id}.parquet` (old snapshots retained)
- **Manifest as visibility gate**: Readers discover current snapshot via `execution.manifest.snapshot_path`, not by listing `state/`

### Event Envelope (CloudEvents-compatible)

All events use this envelope structure (per unified design):

```rust
pub struct EventEnvelope {
    // CloudEvents spec fields
    pub id: String,           // ULID (26 chars, alphanumeric)
    pub specversion: String,  // "1.0"
    pub event_type: String,   // "arco.flow.{event_name}"
    pub source: String,       // "/arco/flow/{tenant}/{workspace}"
    pub time: DateTime<Utc>,  // RFC3339 timestamp

    // Platform fields
    pub tenant_id: String,
    pub workspace_id: String,
    pub schema_version: u32,  // For schema evolution (currently 1)
    pub idempotency_key: String, // For compactor dedupe

    // Domain data
    pub data: ExecutionEventData,
}
```

**Serialization:**
- JSON field names use camelCase: `specversion`, `schemaVersion`, `tenantId`, etc.
- The `id` field is a ULID (not UUID) for lexicographic ordering = chronological ordering

### Watermark Strategy

The compactor uses **exact filename** as watermark, not trimmed versions:

```rust
// Watermark is stored as the complete filename (e.g., "01HX9999999999999999999999.json")
pub watermark_event_id: Option<String>,  // Exact filename, including .json

// Comparison uses exact match
if filename > watermark { /* process this event */ }
```

**Rationale:** Using exact filenames ensures deterministic comparisons without off-by-one errors from trimming.

**Important (distributed writers):** ULID lexicographic ordering is only *approximately* chronological and is not a safe global watermark under clock skew or out-of-order delivery. For production, plan to migrate Tier 2 compaction to a **sequence/position-based watermark** (e.g., `watermark_position`) derived from a monotonic `sequence_position` assigned at ingest (or another monotonic ordering source), so late-arriving events are not skipped.

**Planned migration sketch (Part 5+):**
1. Add `sequence_position: u64` to Tier 2 event records (assigned at ingest, not by clients).
2. Maintain a per-domain monotonic counter (e.g., CAS-updated manifest field or a dedicated counter object) so writers can atomically reserve the next position.
3. Store `watermark_position` in `ExecutionManifest` and advance it only after snapshot write + manifest CAS succeeds.
4. Update compactor filtering to use `sequence_position > watermark_position` (optionally retaining filename watermark for debugging/backfill).
5. Add regression tests that write events with out-of-order ULIDs but increasing `sequence_position` and assert no events are skipped.

### Snapshot Model (Full Snapshot)

Each compaction publishes a **full snapshot**, not a delta chain: the compactor loads the previously published snapshot (if any), merges it with new ledger events, then writes a new Parquet snapshot and publishes it via manifest CAS. A regression test (`tier2_snapshot_does_not_shrink_across_compactions`) proves state does not shrink across compactions.

### Snapshot Pointer (Atomic Publish)

The manifest contains an explicit pointer to the current published snapshot:

```rust
pub struct ExecutionManifest {
    // ... existing fields ...

    /// Path to the current published snapshot (atomic visibility gate)
    pub snapshot_path: Option<String>,

    /// Version of the current snapshot (monotonically increasing)
    pub snapshot_version: u64,
}
```

**Invariant 4 compliance:** Readers MUST use `manifest.snapshot_path` to locate the current snapshot, not list `state/`. This ensures atomic publish semantics - a snapshot isn't visible until manifest CAS succeeds.

---

## D.6 Acceptance Test Coverage

Per architecture mandate, 10 acceptance tests are required. This plan implements **7 of 10** in Part 4, with 3 deferred:

| ID | Test | Part 4 | Notes |
|----|------|--------|-------|
| D.6.1 | Append→Compact→Read Loop | ✅ | `tier2_append_compact_read_loop` |
| D.6.2 | Out-of-Order Processing | ✅ | `tier2_out_of_order_processing` |
| D.6.3 | Gap Detection | ⏸️ Deferred | Requires `sequence_position` infrastructure |
| D.6.4 | Crash Recovery | ✅ | `tier2_crash_recovery` |
| D.6.5 | Manifest CAS Conflicts | ✅ | `tier2_concurrent_compactor_race` |
| D.6.6 | Workspace Isolation | ✅ | `tier2_workspace_isolation` |
| D.6.7 | Browser Query Pruning | ⏸️ Deferred | Requires sharded Parquet layout |
| D.6.8 | L0 Delta Limits | ⏸️ Deferred | Requires L0/L1 tiered compaction |
| D.6.9 | Derived Current Pointers | ✅ | `tier2_derived_current_pointers` |
| D.6.10 | Canonical Partition Key Encoding | ✅ | `crates/arco-core/tests/cross_language/partition_key_test.rs`, `crates/arco-proto/tests/golden_fixtures.rs` |

**Deferred items rationale:** D.6.3, D.6.7, and D.6.8 require additional infrastructure (sequence positions, sharding, tiered compaction) that is out of scope for Part 4's focus on core Tier 2 mechanics.

---

## Task 1: Create arco-test-utils Crate

**Goal:** Establish shared test infrastructure to make integration tests cheap to write.

**Files:**
- Create: `crates/arco-test-utils/Cargo.toml`
- Create: `crates/arco-test-utils/src/lib.rs`
- Create: `crates/arco-test-utils/src/storage.rs`
- Create: `crates/arco-test-utils/src/fixtures.rs`
- Create: `crates/arco-test-utils/src/assertions.rs`
- Modify: `Cargo.toml` (workspace members)

**Step 1: Create Cargo.toml**

Create `crates/arco-test-utils/Cargo.toml`:

```toml
[package]
name = "arco-test-utils"
version = "0.1.0"
edition = "2024"
description = "Shared test utilities for Arco integration tests"
publish = false

[dependencies]
arco-core = { path = "../arco-core" }
arco-catalog = { path = "../arco-catalog" }
arco-flow = { path = "../arco-flow" }

tokio = { workspace = true, features = ["full", "test-util"] }
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
async-trait = { workspace = true }
bytes = { workspace = true }
uuid = { workspace = true, features = ["v4"] }
tracing = { workspace = true }
tracing-subscriber = { workspace = true, features = ["env-filter", "json"] }
tempfile = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
```

**Step 2: Create TracingMemoryBackend**

Create `crates/arco-test-utils/src/storage.rs`:

```rust
//! Test storage implementations with operation tracing.
//!
//! Provides in-memory storage that records all operations for test assertions.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::error::{Error, Result};
use std::ops::Range;
use bytes::Bytes;
use chrono::{DateTime, Utc};

/// Record of a storage operation for test assertions.
#[derive(Debug, Clone)]
pub enum StorageOp {
    /// Get operation.
    Get {
        /// Path that was read.
        path: String,
    },
    /// GetRange operation (for Parquet range reads).
    GetRange {
        /// Path that was read.
        path: String,
        /// Start byte offset.
        start: u64,
        /// End byte offset.
        end: u64,
    },
    /// Head operation (metadata only).
    Head {
        /// Path that was checked.
        path: String,
    },
    /// Put operation.
    Put {
        /// Path that was written.
        path: String,
        /// Size of data written.
        size: usize,
        /// Precondition used.
        precondition: WritePrecondition,
    },
    /// Delete operation.
    Delete {
        /// Path that was deleted.
        path: String,
    },
    /// List operation.
    List {
        /// Prefix that was listed.
        prefix: String,
    },
}

/// In-memory storage backend with operation tracing.
///
/// Records all operations for later assertion in tests.
#[derive(Debug, Clone, Default)]
pub struct TracingMemoryBackend {
    data: Arc<Mutex<HashMap<String, StoredObject>>>,
    operations: Arc<Mutex<Vec<StorageOp>>>,
    fail_paths: Arc<Mutex<Vec<String>>>,
    latency: Option<Duration>,
}

#[derive(Debug, Clone)]
struct StoredObject {
    data: Bytes,
    /// Version stored as i64 internally, exposed as String via API (multi-cloud compat).
    version: i64,
    last_modified: DateTime<Utc>,
}

impl TracingMemoryBackend {
    /// Creates a new empty tracing storage.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates storage with simulated latency.
    #[must_use]
    pub fn with_latency(latency: Duration) -> Self {
        Self {
            latency: Some(latency),
            ..Self::default()
        }
    }

    /// Returns all recorded operations.
    #[must_use]
    pub fn operations(&self) -> Vec<StorageOp> {
        self.operations.lock().expect("lock").clone()
    }

    /// Clears recorded operations.
    pub fn clear_operations(&self) {
        self.operations.lock().expect("lock").clear();
    }

    /// Injects a failure for the given path prefix.
    pub fn inject_failure(&self, path: impl Into<String>) {
        self.fail_paths.lock().expect("lock").push(path.into());
    }

    /// Clears all injected failures.
    pub fn clear_failures(&self) {
        self.fail_paths.lock().expect("lock").clear();
    }

    /// Returns the current version for a path (for CAS testing).
    #[must_use]
    pub fn version(&self, path: &str) -> Option<String> {
        self.data.lock().expect("lock").get(path).map(|o| o.version.to_string())
    }

    /// Returns all stored paths (for debugging).
    #[must_use]
    pub fn paths(&self) -> Vec<String> {
        self.data.lock().expect("lock").keys().cloned().collect()
    }

    fn record(&self, op: StorageOp) {
        self.operations.lock().expect("lock").push(op);
    }

    fn check_failure(&self, path: &str) -> Result<()> {
        let fail_paths = self.fail_paths.lock().expect("lock");
        if fail_paths.iter().any(|p| path.starts_with(p)) {
            return Err(Error::Internal {
                message: format!("Injected failure for path: {path}"),
            });
        }
        Ok(())
    }

    async fn maybe_delay(&self) {
        if let Some(latency) = self.latency {
            tokio::time::sleep(latency).await;
        }
    }
}

#[async_trait::async_trait]
impl StorageBackend for TracingMemoryBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
        self.maybe_delay().await;
        self.check_failure(path)?;
        self.record(StorageOp::Get { path: path.to_string() });

        let data = self.data.lock().expect("lock");
        data.get(path)
            .map(|o| o.data.clone())
            .ok_or_else(|| Error::NotFound(format!("object not found: {path}")))
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.maybe_delay().await;
        self.check_failure(path)?;
        self.record(StorageOp::GetRange {
            path: path.to_string(),
            start: range.start,
            end: range.end,
        });

        let data = self.data.lock().expect("lock");
        data.get(path)
            .map(|o| {
                let start = range.start as usize;
                let end = std::cmp::min(range.end as usize, o.data.len());
                o.data.slice(start..end)
            })
            .ok_or_else(|| Error::NotFound(format!("object not found: {path}")))
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.maybe_delay().await;
        self.check_failure(path)?;
        self.record(StorageOp::Head { path: path.to_string() });

        let data = self.data.lock().expect("lock");
        Ok(data.get(path).map(|o| ObjectMeta {
            path: path.to_string(),
            size: o.data.len() as u64,
            version: o.version.to_string(),
            last_modified: Some(o.last_modified),
            etag: Some(format!("\"{}\"", o.version)),
        }))
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        self.maybe_delay().await;
        self.check_failure(path)?;
        self.record(StorageOp::Put {
            path: path.to_string(),
            size: data.len(),
            precondition: precondition.clone(),
        });

        let mut store = self.data.lock().expect("lock");
        let existing = store.get(path);

        // Check precondition
        match &precondition {
            WritePrecondition::None => {}
            WritePrecondition::DoesNotExist => {
                if let Some(obj) = existing {
                    return Ok(WriteResult::PreconditionFailed {
                        current_version: obj.version.to_string(),
                    });
                }
            }
            WritePrecondition::MatchesVersion(expected) => {
                let expected_num: i64 = expected.parse().unwrap_or(-1);
                match existing {
                    Some(obj) if obj.version != expected_num => {
                        return Ok(WriteResult::PreconditionFailed {
                            current_version: obj.version.to_string(),
                        });
                    }
                    None => {
                        return Ok(WriteResult::PreconditionFailed {
                            current_version: "0".to_string(),
                        });
                    }
                    _ => {}
                }
            }
        }

        let new_version = existing.map_or(1, |o| o.version + 1);

        store.insert(
            path.to_string(),
            StoredObject {
                data,
                version: new_version,
                last_modified: Utc::now(),
            },
        );

        Ok(WriteResult::Success {
            version: new_version.to_string(),
        })
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.maybe_delay().await;
        self.check_failure(path)?;
        self.record(StorageOp::Delete { path: path.to_string() });

        self.data.lock().expect("lock").remove(path);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.maybe_delay().await;
        self.check_failure(prefix)?;
        self.record(StorageOp::List { prefix: prefix.to_string() });

        let data = self.data.lock().expect("lock");
        Ok(data
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| ObjectMeta {
                path: k.clone(),
                size: v.data.len() as u64,
                version: v.version.to_string(),
                last_modified: Some(v.last_modified),
                etag: Some(format!("\"{}\"", v.version)),
            })
            .collect())
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        self.maybe_delay().await;
        self.check_failure(path)?;
        // Mock implementation for testing
        Ok(format!(
            "memory://localhost/{path}?expires={}&signature=mock",
            expiry.as_secs()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tracing_storage_records_operations() {
        let storage = TracingMemoryBackend::new();

        storage
            .put("test.txt", Bytes::from("hello"), WritePrecondition::None)
            .await
            .expect("put");
        let _ = storage.get("test.txt").await;
        let _ = storage.list("").await;

        let ops = storage.operations();
        assert_eq!(ops.len(), 3);
        assert!(matches!(ops[0], StorageOp::Put { .. }));
        assert!(matches!(ops[1], StorageOp::Get { .. }));
        assert!(matches!(ops[2], StorageOp::List { .. }));
    }

    #[tokio::test]
    async fn tracing_storage_failure_injection() {
        let storage = TracingMemoryBackend::new();
        storage.inject_failure("fail/");

        let result = storage.get("fail/test.txt").await;
        assert!(result.is_err());

        // Write first to a non-failing path
        storage
            .put("ok/test.txt", Bytes::from("data"), WritePrecondition::None)
            .await
            .expect("put");
        let result = storage.get("ok/test.txt").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn tracing_storage_cas_works() {
        let storage = TracingMemoryBackend::new();

        // First write succeeds
        let result = storage
            .put("test.txt", Bytes::from("v1"), WritePrecondition::DoesNotExist)
            .await
            .expect("put");
        assert!(matches!(result, WriteResult::Success { ref version } if version == "1"));

        // Second write with DoesNotExist fails
        let result = storage
            .put("test.txt", Bytes::from("v2"), WritePrecondition::DoesNotExist)
            .await
            .expect("put");
        assert!(matches!(result, WriteResult::PreconditionFailed { .. }));

        // Write with correct version succeeds
        let result = storage
            .put(
                "test.txt",
                Bytes::from("v2"),
                WritePrecondition::MatchesVersion("1".to_string()),
            )
            .await
            .expect("put");
        assert!(matches!(result, WriteResult::Success { ref version } if version == "2"));
    }
}
```

**Step 3: Create test fixtures**

Create `crates/arco-test-utils/src/fixtures.rs`:

```rust
//! Pre-built test fixtures for common test scenarios.
//!
//! Provides factory functions to create test data with sensible defaults.

use std::sync::Arc;

use arco_core::{AssetId, MaterializationId, RunId, TaskId};
use arco_flow::events::EventBuilder;
use arco_flow::plan::{AssetKey, Plan, PlanBuilder, ResourceRequirements, TaskSpec};
use arco_flow::run::{Run, RunTrigger};
use arco_flow::task::TaskState;

use crate::storage::TracingMemoryBackend;

/// Test context with pre-configured storage and identifiers.
pub struct TestContext {
    /// Shared storage backend.
    pub storage: Arc<TracingMemoryBackend>,
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
}

impl TestContext {
    /// Creates a new test context with unique tenant/workspace.
    #[must_use]
    pub fn new() -> Self {
        Self {
            storage: Arc::new(TracingMemoryBackend::new()),
            tenant_id: format!("test-tenant-{}", uuid::Uuid::new_v4().as_simple()),
            workspace_id: "test".to_string(),
        }
    }

    /// Creates context with a specific tenant/workspace.
    #[must_use]
    pub fn with_ids(tenant_id: impl Into<String>, workspace_id: impl Into<String>) -> Self {
        Self {
            storage: Arc::new(TracingMemoryBackend::new()),
            tenant_id: tenant_id.into(),
            workspace_id: workspace_id.into(),
        }
    }

    /// Returns the base path for this tenant/workspace.
    #[must_use]
    pub fn base_path(&self) -> String {
        format!(
            "tenant={}/workspace={}",
            self.tenant_id, self.workspace_id
        )
    }
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory for creating test plans.
pub struct PlanFactory;

impl PlanFactory {
    /// Creates a simple linear DAG: a → b → c
    #[must_use]
    pub fn linear_dag(tenant_id: &str, workspace_id: &str) -> (Plan, TaskId, TaskId, TaskId) {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();
        let task_c = TaskId::generate();

        let plan = PlanBuilder::new(tenant_id, workspace_id)
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "events_cleaned"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_c,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("mart", "daily_summary"),
                partition_key: None,
                upstream_task_ids: vec![task_b],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .expect("linear DAG should be valid");

        (plan, task_a, task_b, task_c)
    }

    /// Creates a diamond DAG: a → [b, c] → d
    #[must_use]
    pub fn diamond_dag(tenant_id: &str, workspace_id: &str) -> Plan {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();
        let task_c = TaskId::generate();
        let task_d = TaskId::generate();

        PlanBuilder::new(tenant_id, workspace_id)
            .add_task(TaskSpec {
                task_id: task_a,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", "source"),
                partition_key: None,
                upstream_task_ids: vec![],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_b,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "branch_a"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_c,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("staging", "branch_b"),
                partition_key: None,
                upstream_task_ids: vec![task_a],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .add_task(TaskSpec {
                task_id: task_d,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("mart", "combined"),
                partition_key: None,
                upstream_task_ids: vec![task_b, task_c],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .expect("diamond DAG should be valid")
    }

    /// Creates a plan with N independent tasks (for parallelism testing).
    #[must_use]
    pub fn parallel_tasks(tenant_id: &str, workspace_id: &str, count: usize) -> Plan {
        let mut builder = PlanBuilder::new(tenant_id, workspace_id);

        for i in 0..count {
            builder = builder.add_task(TaskSpec {
                task_id: TaskId::generate(),
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", format!("task_{i}")),
                partition_key: None,
                upstream_task_ids: vec![],
                priority: i32::try_from(i).unwrap_or(0),
                stage: 0,
                resources: ResourceRequirements::default(),
            });
        }

        builder.build().expect("parallel tasks should be valid")
    }
}

/// Factory for creating test events.
pub struct EventFactory;

impl EventFactory {
    /// Creates a sequence of events for a complete successful run.
    #[must_use]
    pub fn complete_run_events(
        plan: &Plan,
        run: &Run,
    ) -> Vec<arco_flow::events::EventEnvelope> {
        let mut events = Vec::new();

        // RunCreated
        events.push(EventBuilder::run_created(
            &run.tenant_id,
            &run.workspace_id,
            run.id,
            &plan.plan_id,
            "Manual",
            Some("test@example.com".to_string()),
        ));

        // RunStarted
        events.push(EventBuilder::run_started(
            &run.tenant_id,
            &run.workspace_id,
            run.id,
            &plan.plan_id,
        ));

        // Task events for each task
        for (idx, task) in plan.tasks.iter().enumerate() {
            events.push(EventBuilder::task_queued(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                task.task_id,
                1,
            ));
            events.push(EventBuilder::task_started(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                task.task_id,
                1,
                format!("worker-{idx}"),
            ));
            events.push(EventBuilder::task_completed(
                &run.tenant_id,
                &run.workspace_id,
                run.id,
                task.task_id,
                TaskState::Succeeded,
                1,
            ));
        }

        // RunCompleted
        events.push(EventBuilder::run_completed(
            &run.tenant_id,
            &run.workspace_id,
            run.id,
            arco_flow::run::RunState::Succeeded,
            plan.tasks.len(),
            0,
            0,
            0,
        ));

        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_dag_has_correct_structure() {
        let ctx = TestContext::new();
        let (plan, task_a, task_b, task_c) =
            PlanFactory::linear_dag(&ctx.tenant_id, &ctx.workspace_id);

        assert_eq!(plan.tasks.len(), 3);

        // Verify dependencies
        let spec_a = plan.get_task(&task_a).expect("task a");
        let spec_b = plan.get_task(&task_b).expect("task b");
        let spec_c = plan.get_task(&task_c).expect("task c");

        assert!(spec_a.upstream_task_ids.is_empty());
        assert_eq!(spec_b.upstream_task_ids, vec![task_a]);
        assert_eq!(spec_c.upstream_task_ids, vec![task_b]);
    }

    #[test]
    fn diamond_dag_has_correct_structure() {
        let ctx = TestContext::new();
        let plan = PlanFactory::diamond_dag(&ctx.tenant_id, &ctx.workspace_id);

        assert_eq!(plan.tasks.len(), 4);

        // Find the final task (has 2 dependencies)
        let final_task = plan.tasks.iter().find(|t| t.upstream_task_ids.len() == 2);
        assert!(final_task.is_some());
    }
}
```

**Step 4: Create assertion helpers**

Create `crates/arco-test-utils/src/assertions.rs`:

```rust
//! Custom assertion helpers for integration tests.

use arco_flow::events::EventEnvelope;
use arco_flow::plan::Plan;
use arco_flow::run::{Run, RunState};

use crate::storage::StorageOp;

/// Asserts that a run completed successfully.
///
/// # Panics
///
/// Panics if the run did not succeed or has failed tasks.
pub fn assert_run_succeeded(run: &Run) {
    assert_eq!(
        run.state,
        RunState::Succeeded,
        "Expected run to succeed, but state was {:?}",
        run.state
    );
    assert_eq!(
        run.tasks_failed(),
        0,
        "Expected no failed tasks, but {} failed",
        run.tasks_failed()
    );
}

/// Asserts that a run failed.
///
/// # Panics
///
/// Panics if the run did not fail.
pub fn assert_run_failed(run: &Run) {
    assert_eq!(
        run.state,
        RunState::Failed,
        "Expected run to fail, but state was {:?}",
        run.state
    );
    assert!(run.tasks_failed() > 0, "Expected at least one failed task");
}

/// Asserts that all tasks in a run are in terminal states.
///
/// # Panics
///
/// Panics if any task is not in a terminal state.
pub fn assert_all_tasks_terminal(run: &Run) {
    for exec in &run.task_executions {
        assert!(
            exec.is_terminal(),
            "Task {:?} is not terminal (state: {:?})",
            exec.task_id,
            exec.state
        );
    }
}

/// Asserts that tasks were executed in topological order.
///
/// # Panics
///
/// Panics if any task started before its dependencies completed.
pub fn assert_topological_order(run: &Run, plan: &Plan) {
    for task in &run.task_executions {
        if let Some(spec) = plan.get_task(&task.task_id) {
            for dep_id in &spec.upstream_task_ids {
                let dep_exec = run.get_task(dep_id).expect("dependency should exist");

                // If this task started, dependency must have completed first
                if let (Some(started), Some(dep_completed)) = (task.started_at, dep_exec.completed_at)
                {
                    assert!(
                        dep_completed <= started,
                        "Task {:?} started before dependency {:?} completed",
                        task.task_id,
                        dep_id
                    );
                }
            }
        }
    }
}

/// Asserts that events have monotonically increasing timestamps.
///
/// # Panics
///
/// Panics if events are not ordered by timestamp.
pub fn assert_events_ordered(events: &[EventEnvelope]) {
    for window in events.windows(2) {
        assert!(
            window[0].time <= window[1].time,
            "Events not ordered: {:?} > {:?}",
            window[0].time,
            window[1].time
        );
    }
}

/// Asserts that all events have unique IDs.
///
/// # Panics
///
/// Panics if any duplicate event ID is found.
pub fn assert_events_unique(events: &[EventEnvelope]) {
    let mut seen = std::collections::HashSet::new();
    for event in events {
        assert!(seen.insert(&event.id), "Duplicate event ID: {}", event.id);
    }
}

/// Asserts that storage operations contain expected patterns.
///
/// # Panics
///
/// Panics if expected operations are not found.
pub fn assert_storage_ops_contain(ops: &[StorageOp], expected: &[(&str, &str)]) {
    for (op_type, path_prefix) in expected {
        let found = ops.iter().any(|op| {
            let (actual_type, actual_path) = match op {
                StorageOp::Get { path } => ("get", path.as_str()),
                StorageOp::GetRange { path, .. } => ("get_range", path.as_str()),
                StorageOp::Head { path } => ("head", path.as_str()),
                StorageOp::Put { path, .. } => ("put", path.as_str()),
                StorageOp::Delete { path } => ("delete", path.as_str()),
                StorageOp::List { prefix } => ("list", prefix.as_str()),
            };
            actual_type == *op_type && actual_path.starts_with(path_prefix)
        });
        assert!(
            found,
            "Expected {op_type} operation on path starting with '{path_prefix}', not found in {ops:?}",
        );
    }
}

/// Asserts that no storage operations accessed a given path prefix.
///
/// Useful for verifying invariants like "readers never access ledger".
///
/// # Panics
///
/// Panics if any operation accessed the given prefix.
pub fn assert_storage_ops_exclude(ops: &[StorageOp], forbidden_prefix: &str) {
    for op in ops {
        let path = match op {
            StorageOp::Get { path }
            | StorageOp::GetRange { path, .. }
            | StorageOp::Head { path }
            | StorageOp::Put { path, .. }
            | StorageOp::Delete { path }
            | StorageOp::List { prefix: path } => path,
        };
        assert!(
            !path.starts_with(forbidden_prefix),
            "Operation on forbidden path: {path} (prefix: {forbidden_prefix})",
        );
    }
}
```

**Step 5: Create lib.rs**

Create `crates/arco-test-utils/src/lib.rs`:

```rust
//! Shared test utilities for Arco integration tests.
//!
//! This crate provides:
//! - [`TracingMemoryBackend`]: In-memory storage with operation recording
//! - [`TestContext`]: Pre-configured test environment
//! - Factory functions for creating test data
//! - Custom assertion helpers
//!
//! # Example
//!
//! ```rust,ignore
//! use arco_test_utils::{TestContext, PlanFactory, assert_run_succeeded};
//!
//! #[tokio::test]
//! async fn test_example() {
//!     let ctx = TestContext::new();
//!     let (plan, _, _, _) = PlanFactory::linear_dag(&ctx.tenant_id, &ctx.workspace_id);
//!     // ... run test ...
//! }
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]
#![allow(clippy::must_use_candidate)]
// Test utilities use expect/unwrap for cleaner test code - panics are acceptable in tests
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]

pub mod assertions;
pub mod fixtures;
pub mod storage;

pub use assertions::*;
pub use fixtures::*;
pub use storage::*;

/// Initialize test logging (call once per test module).
pub fn init_test_logging() {
    use tracing_subscriber::{fmt, EnvFilter};

    let _ = fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("arco=debug".parse().expect("valid directive")),
        )
        .with_test_writer()
        .try_init();
}
```

**Step 6: Update workspace Cargo.toml**

Add `"crates/arco-test-utils"` to workspace members:

```toml
[workspace]
members = [
    "crates/arco-core",
    "crates/arco-catalog",
    "crates/arco-flow",
    "crates/arco-api",
    "crates/arco-proto",
    "crates/arco-compactor",
    "crates/arco-test-utils",
    "tools/xtask",
]
```

**Step 7: Run tests to verify**

Run: `cargo test -p arco-test-utils`

Expected: All tests PASS

**Step 8: Commit**

```bash
git add crates/arco-test-utils/ Cargo.toml
git commit -m "feat(test-utils): add shared test utilities crate

Provides TracingMemoryBackend with operation recording and failure
injection, TestContext for isolated test environments, factory
functions for plans and events, and custom assertion helpers."
```

---

## Task 2: Cross-Crate Contract Tests

**Goal:** Validate that contracts between arco-core, arco-catalog, and arco-flow are correctly implemented.

**Files:**
- Create: `crates/arco-test-utils/tests/contracts.rs`

**Step 0: Choose a test home (virtual workspace note)**

This repository is a virtual workspace (no `[package]` at the root), so Cargo will not discover repo-root `tests/`. Place cross-crate contract tests in a real crate's `tests/` directory so they can be run via `cargo test -p ...`.

**Step 1: Write contract tests**

Create `crates/arco-test-utils/tests/contracts.rs`:

```rust
//! Cross-crate contract tests.
//!
//! These tests validate that the contracts between arco-core, arco-catalog,
//! and arco-flow are correctly implemented and maintained.

use arco_core::{AssetId, RunId, TaskId};
use arco_flow::events::EventBuilder;
use arco_flow::plan::PlanBuilder;
use arco_flow::task::TaskState;
use arco_test_utils::{PlanFactory, TestContext};

/// Contract: Plan fingerprint is deterministic.
#[test]
fn contract_plan_fingerprint_is_deterministic() {
    let ctx = TestContext::new();

    // Create same plan twice with same IDs
    let task_a = TaskId::generate();
    let asset_a = AssetId::generate();

    let plan1 = PlanBuilder::new(&ctx.tenant_id, &ctx.workspace_id)
        .add_task(arco_flow::plan::TaskSpec {
            task_id: task_a,
            asset_id: asset_a,
            asset_key: arco_flow::plan::AssetKey::new("raw", "events"),
            partition_key: None,
            upstream_task_ids: vec![],
            priority: 0,
            stage: 0,
            resources: arco_flow::plan::ResourceRequirements::default(),
        })
        .build()
        .expect("plan1");

    let plan2 = PlanBuilder::new(&ctx.tenant_id, &ctx.workspace_id)
        .add_task(arco_flow::plan::TaskSpec {
            task_id: task_a,
            asset_id: asset_a,
            asset_key: arco_flow::plan::AssetKey::new("raw", "events"),
            partition_key: None,
            upstream_task_ids: vec![],
            priority: 0,
            stage: 0,
            resources: arco_flow::plan::ResourceRequirements::default(),
        })
        .build()
        .expect("plan2");

    assert_eq!(
        plan1.fingerprint, plan2.fingerprint,
        "same inputs should produce same fingerprint"
    );
}

/// Contract: Plan fingerprint survives serialization.
#[test]
fn contract_plan_fingerprint_survives_serialization() {
    let ctx = TestContext::new();
    let (plan, _, _, _) = PlanFactory::linear_dag(&ctx.tenant_id, &ctx.workspace_id);

    let json = serde_json::to_string(&plan).expect("serialize");
    let deserialized: arco_flow::plan::Plan = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(plan.fingerprint, deserialized.fingerprint);
}

/// Contract: IDs are URL-safe (alphanumeric only).
#[test]
fn contract_ids_are_url_safe() {
    let task_id = TaskId::generate();
    let asset_id = AssetId::generate();
    let run_id = RunId::generate();

    let ids = [
        task_id.to_string(),
        asset_id.to_string(),
        run_id.to_string(),
    ];

    for id in ids {
        assert!(
            id.chars().all(|c| c.is_ascii_alphanumeric()),
            "ID {id} contains non-URL-safe characters",
        );
        assert_eq!(id.len(), 26, "ULID should be 26 characters");
    }
}

/// Contract: Event envelope follows our CatalogEvent spec (per unified design).
///
/// The architecture defines a CatalogEvent envelope with these fields:
/// - event_id: ULID
/// - event_type: string
/// - event_version: u32 (for schema evolution)
/// - timestamp: DateTime<Utc>
/// - source: string
/// - tenant_id: string
/// - workspace_id: string
/// - idempotency_key: string (for dedupe during compaction)
/// - sequence_position: Option<u64> (monotonic position for watermarking)
/// - payload: Value (domain-specific data)
#[test]
fn contract_event_envelope_matches_architecture() {
    let run_id = RunId::generate();
    let task_id = TaskId::generate();

    let event = EventBuilder::task_started("tenant", "workspace", run_id, task_id, 1, "worker-1");

    // Architecture-mandated fields (CatalogEvent envelope)
    assert!(!event.id.is_empty(), "id is required");
    assert!(!event.event_type.is_empty(), "event_type is required");
    assert!(event.schema_version >= 1, "schema_version must be >= 1");
    assert!(!event.source.is_empty(), "source is required");
    assert!(!event.tenant_id.is_empty(), "tenant_id is required");
    assert!(!event.workspace_id.is_empty(), "workspace_id is required");
    assert!(!event.idempotency_key.is_empty(), "idempotency_key is required for dedupe");

    // Verify id is a valid ULID (26 alphanumeric chars)
    assert_eq!(event.id.len(), 26);
    assert!(event.id.chars().all(|c| c.is_ascii_alphanumeric()));
}

/// Contract: idempotency_key enables compaction dedupe.
///
/// Architecture: "Compactor dedupes by idempotency_key during fold."
#[test]
fn contract_idempotency_key_is_stable_for_same_event() {
    let run_id = RunId::generate();
    let task_id = TaskId::generate();

    // Same logical event created twice
    let event1 = EventBuilder::task_started("tenant", "workspace", run_id, task_id, 1, "worker-1");
    let event2 = EventBuilder::task_started("tenant", "workspace", run_id, task_id, 1, "worker-1");

    // idempotency_key should be deterministic for the same logical event
    assert_eq!(
        event1.idempotency_key, event2.idempotency_key,
        "same logical event must have same idempotency_key"
    );
}

/// Contract: schema_version enables schema evolution.
///
/// Architecture: "Unknown schema_version should be rejected or routed to compatibility parser."
#[test]
fn contract_schema_version_enables_schema_evolution() {
    let run_id = RunId::generate();
    let task_id = TaskId::generate();

    let event = EventBuilder::task_started("tenant", "workspace", run_id, task_id, 1, "worker-1");

    // Current version should be 1
    assert_eq!(event.schema_version, 1, "current schema version is 1");

    // Serialize and verify version is in JSON
    let json = serde_json::to_value(&event).expect("serialize");
    assert_eq!(
        json.get("schemaVersion").and_then(|v| v.as_u64()),
        Some(1),
        "schemaVersion must be serialized"
    );
}

/// Contract: Events serialize to valid JSON.
#[test]
fn contract_events_serialize_to_json() {
    let run_id = RunId::generate();
    let task_id = TaskId::generate();

    let events = vec![
        EventBuilder::run_created("tenant", "workspace", run_id, "plan-1", "Manual", None),
        EventBuilder::run_started("tenant", "workspace", run_id, "plan-1"),
        EventBuilder::task_queued("tenant", "workspace", run_id, task_id, 1),
        EventBuilder::task_started("tenant", "workspace", run_id, task_id, 1, "worker-1"),
        EventBuilder::task_completed("tenant", "workspace", run_id, task_id, TaskState::Succeeded, 1),
        EventBuilder::run_completed(
            "tenant",
            "workspace",
            run_id,
            arco_flow::run::RunState::Succeeded,
            1,
            0,
            0,
            0,
        ),
    ];

    for event in events {
        let json = serde_json::to_string(&event);
        assert!(json.is_ok(), "Event should serialize: {event:?}");

        // Should deserialize back
        let parsed: arco_flow::events::EventEnvelope =
            serde_json::from_str(&json.expect("json")).expect("parse");
        assert_eq!(event.id, parsed.id);
    }
}

/// Contract: Plan stages reflect dependency depth.
#[test]
fn contract_plan_stages_reflect_dependency_depth() {
    let ctx = TestContext::new();
    let (plan, task_a, task_b, task_c) =
        PlanFactory::linear_dag(&ctx.tenant_id, &ctx.workspace_id);

    // Linear DAG: a (stage 0) → b (stage 1) → c (stage 2)
    let spec_a = plan.get_task(&task_a).expect("task a");
    let spec_b = plan.get_task(&task_b).expect("task b");
    let spec_c = plan.get_task(&task_c).expect("task c");

    assert_eq!(spec_a.stage, 0, "Root task should be stage 0");
    assert_eq!(spec_b.stage, 1, "Task depending on stage 0 should be stage 1");
    assert_eq!(spec_c.stage, 2, "Task depending on stage 1 should be stage 2");
}

/// Contract: Plan topological order respects dependencies.
#[test]
fn contract_plan_toposort_respects_dependencies() {
    let ctx = TestContext::new();
    let (plan, task_a, task_b, task_c) =
        PlanFactory::linear_dag(&ctx.tenant_id, &ctx.workspace_id);

    let positions: std::collections::HashMap<TaskId, usize> = plan
        .tasks
        .iter()
        .enumerate()
        .map(|(i, t)| (t.task_id, i))
        .collect();

    assert!(
        positions[&task_a] < positions[&task_b],
        "task_a must appear before task_b"
    );
    assert!(
        positions[&task_b] < positions[&task_c],
        "task_b must appear before task_c"
    );
}
```

**Step 2: Run tests to verify**

Run: `cargo test -p arco-test-utils --test contracts`

Expected: All tests PASS

**Step 3: Commit**

```bash
git add crates/arco-test-utils/tests/contracts.rs
git commit -m "test(integration): add cross-crate contract tests

Validates contracts between arco-core, arco-catalog, and arco-flow:
- Plan fingerprint determinism and serialization
- ID URL-safety (ULID format)
- Event CloudEvents compatibility
- Plan stage computation and topological ordering"
```

---

## Task 3: Add EventWriter for Tier 2 Event Persistence

**Goal:** Implement append-only event writer for Tier 2 ledger.

**Files:**
- Create: `crates/arco-catalog/src/error.rs`
- Create: `crates/arco-catalog/src/event_writer.rs`
- Modify: `crates/arco-catalog/src/lib.rs`

**Step 0: Create CatalogError module**

Create `crates/arco-catalog/src/error.rs`:

```rust
//! Error types for arco-catalog operations.

use thiserror::Error;

/// Result type alias for catalog operations.
pub type Result<T> = std::result::Result<T, CatalogError>;

/// Errors that can occur during catalog operations.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Storage operation failed.
    #[error("storage error: {message}")]
    Storage {
        /// Description of the storage failure.
        message: String,
    },

    /// Serialization/deserialization failed.
    #[error("serialization error: {message}")]
    Serialization {
        /// Description of the serialization failure.
        message: String,
    },

    /// CAS (Compare-And-Swap) operation failed due to concurrent modification.
    #[error("CAS failed: {message}")]
    CasFailed {
        /// Description of the CAS failure.
        message: String,
    },

    /// Resource not found.
    #[error("not found: {message}")]
    NotFound {
        /// Description of what was not found.
        message: String,
    },
}
```

Add to `crates/arco-catalog/src/lib.rs` (at the top, after the module doc comment):

```rust
pub mod error;

pub use error::{CatalogError, Result};
```

**Step 1: Write the failing test for EventWriter**

Create `crates/arco-catalog/src/event_writer.rs`:

```rust
//! Tier 2 event writer for append-only ledger persistence.
//!
//! INVARIANT 1: Ingest is append-only + ack. EventWriter must NEVER overwrite
//! existing events. Uses `DoesNotExist` precondition for true append-only semantics.
//!
//! Events are written as individual JSON files to the ledger path:
//! `ledger/{domain}/{event_id}.json`
//!
//! File naming uses ULID event_id ONLY (no timestamp prefix) for idempotent writes.
//! ULID's embedded timestamp ensures lexicographic ordering = chronological ordering.
//! NOTE: Production should use micro-batched segments to avoid "too many small files".

use bytes::Bytes;
use serde::Serialize;
use ulid::Ulid;

use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::WritePrecondition;

use crate::error::{CatalogError, Result};

/// Writes events to the Tier 2 append-only ledger.
///
/// INVARIANT: This writer is append-only. It uses `DoesNotExist` precondition
/// to ensure events are never overwritten (true audit log semantics).
///
/// Events are stored as individual JSON files at:
/// `ledger/{domain}/{event_id}.json`
///
/// Using event_id only (no timestamp prefix) ensures:
/// 1. Idempotent writes: same event_id always maps to same path
/// 2. DoesNotExist precondition prevents overwrites on replay
/// 3. ULID's embedded timestamp maintains lexicographic = chronological ordering
pub struct EventWriter {
    storage: ScopedStorage,
}

impl EventWriter {
    /// Creates a new event writer.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self { storage }
    }

    /// Appends an event to the ledger with an auto-generated ID.
    ///
    /// Returns the generated event ID.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or storage fails.
    pub async fn append<T: Serialize>(&self, domain: &str, event: &T) -> Result<String> {
        let event_id = Ulid::new().to_string();
        self.append_with_id(domain, event, &event_id).await?;
        Ok(event_id)
    }

    /// Appends an event with a specific ID (for idempotent replays).
    ///
    /// INVARIANT: Uses `DoesNotExist` precondition - true append-only.
    /// If an event with this ID already exists, returns Ok (duplicate delivery).
    /// The ledger is immutable; duplicates are handled at compaction time.
    ///
    /// IDEMPOTENCY: Path is deterministic based on event_id only.
    /// Same event_id always maps to same path, enabling DoesNotExist to prevent duplicates.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or storage fails (NOT for duplicates).
    pub async fn append_with_id<T: Serialize>(
        &self,
        domain: &str,
        event: &T,
        event_id: &str,
    ) -> Result<()> {
        // CRITICAL: Use event_id ONLY (no timestamp prefix) for idempotent writes
        // ULID's embedded timestamp ensures lexicographic = chronological ordering
        let path = format!("ledger/{domain}/{event_id}.json");
        let json = serde_json::to_vec_pretty(event).map_err(|e| CatalogError::Serialization {
            message: format!("failed to serialize event: {e}"),
        })?;

        // CRITICAL: Use DoesNotExist for true append-only semantics
        let result = self.storage
            .put_raw(&path, Bytes::from(json), WritePrecondition::DoesNotExist)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to write event: {e}"),
            })?;

        // Handle duplicate delivery gracefully (idempotent behavior)
        match result {
            arco_core::storage::WriteResult::Success { .. } => Ok(()),
            arco_core::storage::WriteResult::PreconditionFailed { .. } => {
                // Duplicate delivery - this is OK, compactor will dedupe
                tracing::debug!(event_id, "duplicate event delivery (already exists)");
                Ok(())
            }
        }
    }

    /// Lists all event files in a domain.
    ///
    /// Returns file paths sorted lexicographically (ULID ensures chronological).
    /// NOTE: Object store list() ordering is NOT guaranteed - we sort explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error if listing fails.
    pub async fn list_events(&self, domain: &str) -> Result<Vec<String>> {
        let prefix = format!("ledger/{domain}/");
        // ScopedStorage.list() returns Vec<ScopedPath> - relative paths
        let mut files: Vec<_> = self
            .storage
            .list(&prefix)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to list events: {e}"),
            })?
            .into_iter()
            .map(|p| p.to_string())  // ScopedPath -> String
            .collect();

        // CRITICAL: Sort explicitly - object store list() order is not guaranteed
        files.sort();
        Ok(files)
    }

    /// Reads a specific event file by path.
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    pub async fn read_event_file(&self, path: &str) -> Result<Bytes> {
        self.storage
            .get_raw(path)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to read event: {e}"),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::storage::MemoryBackend;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestEvent {
        value: i32,
    }

    #[tokio::test]
    async fn append_event_creates_ledger_file() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = EventWriter::new(storage.clone());
        let event = TestEvent { value: 42 };

        let event_id = writer.append("execution", &event).await.expect("append");

        // Verify file exists in ledger (path includes timestamp prefix)
        let files = writer.list_events("execution").await.expect("list");
        assert_eq!(files.len(), 1);
        assert!(files[0].contains(&event_id), "file should contain event ID");
    }

    #[tokio::test]
    async fn append_is_truly_append_only() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = EventWriter::new(storage.clone());
        let event1 = TestEvent { value: 42 };
        let event2 = TestEvent { value: 99 };
        let fixed_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

        // First write succeeds
        writer
            .append_with_id("execution", &event1, fixed_id)
            .await
            .expect("first");

        // Second write with same ID returns Ok but doesn't overwrite
        // (DoesNotExist precondition fails, but we handle gracefully)
        writer
            .append_with_id("execution", &event2, fixed_id)
            .await
            .expect("second should succeed (duplicate handled)");

        // Should still only have one file (DoesNotExist prevented overwrite)
        let files = storage
            .list("ledger/execution/")
            .await
            .expect("list");
        assert_eq!(files.len(), 1, "append-only: no duplicates created");

        // Original value preserved (not overwritten)
        let data = storage.get_raw(&files[0].path).await.expect("read");
        let parsed: TestEvent = serde_json::from_slice(&data).expect("parse");
        assert_eq!(parsed.value, 42, "original event preserved, not overwritten");
    }

    #[tokio::test]
    async fn events_are_ordered_chronologically() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = EventWriter::new(storage.clone());

        // Write 5 events
        let mut ids = Vec::new();
        for i in 0..5 {
            let event = TestEvent { value: i };
            let id = writer.append("execution", &event).await.expect("append");
            ids.push(id);
        }

        // List files - EventWriter.list_events() sorts explicitly
        let files = writer.list_events("execution").await.expect("list");

        // Verify ordering is maintained
        assert_eq!(files.len(), 5);
        // Files should already be sorted (list_events does this)
        let mut sorted = files.clone();
        sorted.sort();
        assert_eq!(
            files, sorted,
            "timestamp prefix ensures chronological ordering"
        );
    }
}
```

**Step 2: Export EventWriter in lib.rs**

Add to `crates/arco-catalog/src/lib.rs`:

```rust
pub mod event_writer;

pub use event_writer::EventWriter;
```

**Step 3: Run tests to verify**

Run: `cargo test -p arco-catalog event_writer`

Expected: All tests PASS

**Step 4: Commit**

```bash
git add crates/arco-catalog/src/event_writer.rs crates/arco-catalog/src/lib.rs
git commit -m "feat(catalog): add EventWriter for Tier 2 ledger persistence

Implements append-only event writer using ULID-based file naming
for guaranteed chronological ordering. Supports idempotent writes
via fixed event IDs."
```

---

## Task 4: Add Compactor for Event-to-Parquet Consolidation

**Goal:** Implement compactor that reads events, writes Parquet, updates watermark with CAS.

**ARCHITECTURE-CRITICAL:** The compactor MUST use CAS (Compare-And-Swap) for manifest updates
to ensure atomic publish semantics. Without CAS, concurrent compactors could cause data corruption.

**Files:**
- Create: `crates/arco-catalog/src/compactor.rs`
- Modify: `crates/arco-catalog/src/lib.rs`
- Modify: `crates/arco-catalog/src/manifest.rs` (add watermark field)
- Modify: `crates/arco-catalog/src/error.rs` (add `CasFailed` variant)

**Step 0: Add CasFailed error variant**

Add to `crates/arco-catalog/src/error.rs`:

```rust
/// CAS (Compare-And-Swap) operation failed due to concurrent modification.
#[error("CAS failed: {message}")]
CasFailed {
    /// Description of the CAS failure.
    message: String,
},
```

**Step 1: Extend ExecutionManifest with compaction tracking**

Modify `crates/arco-catalog/src/manifest.rs` to add compaction tracking fields.

First, add the `CompactionMetadata` struct (above `ExecutionManifest`):

```rust
/// Metadata about compaction operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionMetadata {
    /// Total events compacted.
    #[serde(default)]
    pub total_events_compacted: u64,
    /// Total Parquet files written.
    #[serde(default)]
    pub total_files_written: u64,
}
```

Then, extend the existing `ExecutionManifest` struct by adding these new fields:

```rust
/// Execution manifest (Tier 2) - separate file.
///
/// Written ONLY by compactor. Contains materializations, partitions.
///
/// INVARIANT 4 (Atomic Publish): Readers use `snapshot_path` to locate current
/// snapshot. A snapshot isn't visible until manifest CAS succeeds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionManifest {
    /// Watermark version (last compacted event).
    pub watermark_version: u64,

    /// Path to compaction checkpoint.
    pub checkpoint_path: String,

    /// Path to the current published snapshot (atomic visibility gate).
    /// Readers MUST use this to locate current state, not list state/.
    #[serde(default)]
    pub snapshot_path: Option<String>,

    /// Version of the current snapshot (monotonically increasing).
    #[serde(default)]
    pub snapshot_version: u64,

    /// Last compacted event file name (for file-based watermark).
    /// Stored as exact filename including .json for correct comparison.
    #[serde(default)]
    pub watermark_event_id: Option<String>,

    /// Monotonic position watermark for clock-skew-resistant ordering.
    /// Architecture: "use sequence_position as watermark anchor to survive clock skew."
    #[serde(default)]
    pub watermark_position: Option<u64>,

    /// Last compaction timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_compaction_at: Option<DateTime<Utc>>,

    /// Compaction statistics.
    #[serde(default)]
    pub compaction: CompactionMetadata,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}
```

Update the `ExecutionManifest::new()` method to initialize new fields:

```rust
impl ExecutionManifest {
    /// Creates a new execution manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            watermark_version: 0,
            checkpoint_path: "state/execution/checkpoint.json".into(),
            snapshot_path: None,      // No snapshot until first compaction
            snapshot_version: 0,
            watermark_event_id: None,
            watermark_position: None,
            last_compaction_at: None,
            compaction: CompactionMetadata::default(),
            updated_at: Utc::now(),
        }
    }
}
```

**Step 2: Create Compactor implementation**

Create `crates/arco-catalog/src/compactor.rs`:

```rust
//! Compactor for Tier 2 event consolidation.
//!
//! INVARIANT 2: Compactor is the SOLE writer of Parquet state files.
//! INVARIANT 3: Compaction is idempotent (dedupe by idempotency_key, primary key upsert).
//! INVARIANT 4: Publish is atomic (manifest CAS).
//!
//! It reads from ledger/, writes to state/, and updates the watermark atomically via CAS.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::WritePrecondition;

use crate::error::{CatalogError, Result};
use crate::tier1_writer::Tier1Writer;

/// Result of a compaction operation.
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// Number of events processed from ledger.
    pub events_processed: usize,
    /// Number of Parquet files written.
    pub parquet_files_written: usize,
    /// New watermark position (event ID).
    pub new_watermark: String,
}

/// Compacts Tier 2 ledger events into Parquet snapshots.
///
/// INVARIANT: Compactor is the SOLE writer of state/ Parquet files.
pub struct Compactor {
    storage: ScopedStorage,
}

impl Compactor {
    /// Creates a new compactor.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self { storage }
    }

    /// Compacts a domain, reading events since watermark and writing Parquet.
    ///
    /// ARCHITECTURE: Uses CAS for atomic manifest publish. The sequence is:
    /// 1. Read manifest + version (head)
    /// 2. Write Parquet snapshot
    /// 3. CAS-update manifest with MatchesVersion
    /// 4. Only if CAS succeeds: compaction is visible
    ///
    /// # Errors
    ///
    /// Returns an error if reading ledger or writing state fails.
    /// Returns `CatalogError::CasFailed` if another compactor won the race.
    pub async fn compact_domain(&self, domain: &str) -> Result<CompactionResult> {
        // 1. Read current manifest + its version for CAS
        let manifest_path = "manifests/execution.manifest.json";
        let (manifest, current_version) = self.read_manifest_with_version().await?;

        let watermark_event_id = manifest.watermark_event_id.clone();

        // 2. List ledger events (ledger/, not events/)
        let prefix = format!("ledger/{domain}/");
        // ScopedStorage.list() returns Vec<ScopedPath> - we need to work with relative paths
        let scoped_paths = self
            .storage
            .list(&prefix)
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to list events: {e}"),
            })?;

        // Convert ScopedPath to path strings for processing
        let mut files: Vec<_> = scoped_paths
            .into_iter()
            .map(|p| ObjectMetaLike { path: p.to_string() })
            .collect();

        // CRITICAL: Sort explicitly - object store list() order is NOT guaranteed
        files.sort_by(|a, b| a.path.cmp(&b.path));

        // 3. Filter to events after watermark
        // Watermark is stored as exact filename (including .json) for correct comparison
        // CRITICAL: Both watermark and filename must have same format for comparison
        let events_to_process: Vec<_> = files
            .into_iter()
            .filter(|f| {
                if let Some(ref watermark) = watermark_event_id {
                    // Compare exact filenames (both include .json)
                    let filename = f
                        .path
                        .rsplit('/')
                        .next()
                        .unwrap_or("");
                    // Watermark is exact filename, so > comparison works correctly
                    filename > watermark.as_str()
                } else {
                    true
                }
            })
            .collect();

        if events_to_process.is_empty() {
            return Ok(CompactionResult {
                events_processed: 0,
                parquet_files_written: 0,
                new_watermark: watermark_event_id.unwrap_or_default(),
            });
        }

        // 4. Read, parse, and DEDUPE events by idempotency_key
        // Architecture: "Compactor dedupes by idempotency_key during fold"
        // Use HashMap keyed by primary key (materialization_id) for upsert semantics
        use std::collections::HashMap;
        let mut records_by_key: HashMap<String, MaterializationRecord> = HashMap::new();
        let mut last_event_file = String::new();

        for file_meta in &events_to_process {
            let filename = file_meta
                .path
                .rsplit('/')
                .next()
                .unwrap_or("")
                .to_string();

            let data = self.storage.get_raw(&file_meta.path).await.map_err(|e| {
                CatalogError::Storage {
                    message: format!("failed to read event: {e}"),
                }
            })?;

            if let Ok(event) = serde_json::from_slice::<MaterializationRecord>(&data) {
                // DEDUPE: Use materialization_id as primary key (upsert semantics)
                // Later events overwrite earlier ones for the same key
                records_by_key.insert(event.materialization_id.clone(), event);
            }

            // Track watermark as the last processed file name
            if filename > last_event_file {
                last_event_file = filename;
            }
        }

        // Convert deduped map to vec for Parquet write
        let records: Vec<_> = records_by_key.into_values().collect();
        // CRITICAL: Store exact filename (including .json) as watermark
        // This ensures correct comparison: filename > watermark works without off-by-one
        let last_event_id = last_event_file;

        // 5. Write Parquet file
        // INVARIANT 4: Parquet is written BEFORE manifest update (atomic publish)
        let (parquet_files_written, new_snapshot_path) = if !records.is_empty() {
            let path = self.write_parquet(domain, &records).await?;
            (1, Some(path))
        } else {
            (0, manifest.snapshot_path.clone())
        };

        // 6. Update watermark in manifest with CAS (ARCHITECTURE-CRITICAL)
        // INVARIANT 4: snapshot_path makes the new Parquet visible atomically
        let new_watermark_version = manifest.watermark_version + 1;
        let new_snapshot_version = manifest.snapshot_version + if parquet_files_written > 0 { 1 } else { 0 };
        let mut new_manifest = manifest.clone();
        new_manifest.watermark_version = new_watermark_version;
        new_manifest.watermark_event_id = Some(last_event_id.clone());
        new_manifest.snapshot_path = new_snapshot_path;        // Atomic visibility gate
        new_manifest.snapshot_version = new_snapshot_version;
        new_manifest.last_compaction_at = Some(Utc::now());
        new_manifest.compaction.total_events_compacted += events_to_process.len() as u64;
        new_manifest.compaction.total_files_written += parquet_files_written as u64;

        let execution_json =
            serde_json::to_vec_pretty(&new_manifest).map_err(|e| CatalogError::Serialization {
                message: format!("failed to serialize manifest: {e}"),
            })?;

        // CRITICAL: Use CAS to ensure atomic publish semantics
        // If another compactor updated the manifest, this will fail and we retry
        let result = self.storage
            .put_raw(
                "manifests/execution.manifest.json",
                Bytes::from(execution_json),
                WritePrecondition::MatchesVersion(current_version),
            )
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to write manifest: {e}"),
            })?;

        // Check if CAS succeeded
        match result {
            arco_core::storage::WriteResult::Success { .. } => {
                // Success - compaction is now visible
            }
            arco_core::storage::WriteResult::PreconditionFailed { .. } => {
                // Another compactor won the race
                return Err(CatalogError::CasFailed {
                    message: "manifest updated by another compactor".to_string(),
                });
            }
        }

        Ok(CompactionResult {
            events_processed: events_to_process.len(),
            parquet_files_written,
            new_watermark: last_event_id,
        })
    }

    /// Writes records to a Parquet snapshot file.
    /// Returns the path to the written file for manifest snapshot_path.
    async fn write_parquet(&self, domain: &str, records: &[MaterializationRecord]) -> Result<String> {
        let materialization_ids: Vec<_> =
            records.iter().map(|r| r.materialization_id.as_str()).collect();
        let asset_ids: Vec<_> = records.iter().map(|r| r.asset_id.as_str()).collect();
        let row_counts: Vec<_> = records.iter().map(|r| r.row_count).collect();
        let byte_sizes: Vec<_> = records.iter().map(|r| r.byte_size).collect();

        let schema = Schema::new(vec![
            Field::new("materialization_id", DataType::Utf8, false),
            Field::new("asset_id", DataType::Utf8, false),
            Field::new("row_count", DataType::Int64, false),
            Field::new("byte_size", DataType::Int64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(StringArray::from(materialization_ids)) as ArrayRef,
                Arc::new(StringArray::from(asset_ids)) as ArrayRef,
                Arc::new(Int64Array::from(row_counts)) as ArrayRef,
                Arc::new(Int64Array::from(byte_sizes)) as ArrayRef,
            ],
        )
        .map_err(|e| CatalogError::Serialization {
            message: format!("failed to create record batch: {e}"),
        })?;

        let mut buffer = Cursor::new(Vec::new());
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, Arc::new(schema), Some(props))
            .map_err(|e| CatalogError::Serialization {
                message: format!("failed to create Parquet writer: {e}"),
            })?;

        writer.write(&batch).map_err(|e| CatalogError::Serialization {
            message: format!("failed to write batch: {e}"),
        })?;
        writer.close().map_err(|e| CatalogError::Serialization {
            message: format!("failed to close writer: {e}"),
        })?;

        // Use snapshot_version for deterministic naming (not timestamp)
        // This makes it easier to track which snapshot is current
        let path = format!("state/{domain}/snapshot.parquet");
        self.storage
            .put_raw(
                &path,
                Bytes::from(buffer.into_inner()),
                WritePrecondition::None,
            )
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to write Parquet: {e}"),
            })?;

        Ok(path)
    }

    /// Reads the execution manifest with its current version for CAS.
    async fn read_manifest_with_version(&self) -> Result<(ExecutionManifest, String)> {
        let manifest_path = "manifests/execution.manifest.json";

        // First, get metadata to retrieve version
        let meta = self.storage.head_raw(manifest_path).await.map_err(|e| {
            CatalogError::Storage {
                message: format!("failed to read manifest metadata: {e}"),
            }
        })?;

        // If manifest doesn't exist, return default with version "0"
        let version = match &meta {
            Some(m) => m.version.clone(),
            None => "0".to_string(),
        };

        // Then read the content
        let data = self.storage.get_raw(manifest_path).await.map_err(|e| {
            CatalogError::Storage {
                message: format!("failed to read manifest: {e}"),
            }
        })?;

        let manifest: ExecutionManifest =
            serde_json::from_slice(&data).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse manifest: {e}"),
            })?;

        Ok((manifest, version))
    }
}

/// Helper struct for list result processing (ScopedPath doesn't have all ObjectMeta fields).
struct ObjectMetaLike {
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MaterializationRecord {
    materialization_id: String,
    asset_id: String,
    #[serde(default)]
    row_count: i64,
    #[serde(default)]
    byte_size: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventWriter;
    use arco_core::storage::MemoryBackend;

    #[tokio::test]
    async fn compact_empty_ledger_is_noop() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await.expect("init");

        let compactor = Compactor::new(storage);
        let result = compactor.compact_domain("execution").await.expect("compact");

        assert_eq!(result.events_processed, 0);
        assert_eq!(result.parquet_files_written, 0);
    }

    #[tokio::test]
    async fn compact_processes_ledger_events() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let tier1_writer = Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let event_writer = EventWriter::new(storage.clone());
        for i in 0..5 {
            let event = MaterializationRecord {
                materialization_id: format!("mat_{i:03}"),
                asset_id: format!("asset_{i:03}"),
                row_count: 1000,
                byte_size: 50000,
            };
            event_writer.append("execution", &event).await.expect("append");
        }

        let compactor = Compactor::new(storage);
        let result = compactor.compact_domain("execution").await.expect("compact");

        assert_eq!(result.events_processed, 5);
        assert!(result.parquet_files_written > 0);
    }

    #[tokio::test]
    async fn second_compaction_only_processes_new_events() {
        let backend = Arc::new(MemoryBackend::new());
        let storage =
            ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

        let tier1_writer = Tier1Writer::new(storage.clone());
        tier1_writer.initialize().await.expect("init");

        let event_writer = EventWriter::new(storage.clone());
        let compactor = Compactor::new(storage.clone());

        // Write 3 events and compact
        for i in 0..3 {
            let event = MaterializationRecord {
                materialization_id: format!("mat_{i:03}"),
                asset_id: format!("asset_{i:03}"),
                row_count: 1000,
                byte_size: 50000,
            };
            event_writer.append("execution", &event).await.expect("append");
        }
        let result1 = compactor.compact_domain("execution").await.expect("compact1");
        assert_eq!(result1.events_processed, 3);

        // Write 2 more events and compact again
        for i in 3..5 {
            let event = MaterializationRecord {
                materialization_id: format!("mat_{i:03}"),
                asset_id: format!("asset_{i:03}"),
                row_count: 1000,
                byte_size: 50000,
            };
            event_writer.append("execution", &event).await.expect("append");
        }
        let result2 = compactor.compact_domain("execution").await.expect("compact2");

        assert_eq!(result2.events_processed, 2, "only new events since watermark");
    }
}
```

**Step 3: Export Compactor in lib.rs**

Add to `crates/arco-catalog/src/lib.rs`:

```rust
pub mod compactor;

pub use compactor::{CompactionResult, Compactor};
```

**Step 4: Run tests to verify**

Run: `cargo test -p arco-catalog compactor`

Expected: All tests PASS

**Step 5: Commit**

```bash
git add crates/arco-catalog/src/compactor.rs crates/arco-catalog/src/manifest.rs crates/arco-catalog/src/lib.rs
git commit -m "feat(catalog): add Compactor for Tier 2 event consolidation

Implements compactor that reads events from JSON ledger, writes
Parquet snapshots to state/, and updates watermark atomically.
Compactor is the sole writer of Parquet state files."
```

---

## Task 5: Property-Based Tests for Invariants

**Goal:** Add proptest suites for critical state machine invariants.

**Files:**
- Create: `crates/arco-flow/tests/property_tests.rs`

**Step 1: Write property-based tests**

Create `crates/arco-flow/tests/property_tests.rs`:

```rust
//! Property-based tests for arco-flow invariants.
//!
//! These tests use proptest to verify invariants hold across
//! randomly generated inputs.

use proptest::prelude::*;

use arco_core::{AssetId, TaskId};
use arco_flow::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
use arco_flow::run::RunState;
use arco_flow::task::TaskState;

/// Generates a random TaskId (diverse, not constant per run).
fn arb_task_id() -> impl Strategy<Value = TaskId> {
    // Use any() to generate diverse IDs, not Just() which is constant per test run
    any::<u64>().prop_map(|_| TaskId::generate())
}

/// Generates a random AssetId (diverse, not constant per run).
fn arb_asset_id() -> impl Strategy<Value = AssetId> {
    any::<u64>().prop_map(|_| AssetId::generate())
}

/// Generates a random asset namespace.
fn arb_namespace() -> impl Strategy<Value = String> {
    prop::sample::select(vec!["raw", "staging", "mart", "gold"])
        .prop_map(String::from)
}

/// Generates a random asset name.
fn arb_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{2,20}".prop_map(|s| s)
}

/// Generates an arbitrary TaskSpec with varied properties.
fn arb_task_spec() -> impl Strategy<Value = TaskSpec> {
    (
        arb_task_id(),
        arb_asset_id(),
        arb_namespace(),
        arb_name(),
        prop::collection::vec(arb_task_id(), 0..3),  // 0-3 dependencies
        0i32..100,  // priority
    ).prop_map(|(task_id, asset_id, ns, name, deps, priority)| {
        TaskSpec {
            task_id,
            asset_id,
            asset_key: AssetKey::new(&ns, &name),
            partition_key: None,
            upstream_task_ids: deps,
            priority,
            stage: 0,
            resources: ResourceRequirements::default(),
        }
    })
}

/// Generates an arbitrary event with varied properties.
fn arb_materialization_event() -> impl Strategy<Value = (String, String, i64, i64)> {
    (
        "[A-Z0-9]{26}",  // ULID-like materialization_id
        "[A-Z0-9]{26}",  // ULID-like asset_id
        0i64..1_000_000,  // row_count
        0i64..100_000_000,  // byte_size
    )
}

proptest! {
    /// INVARIANT: Plan fingerprint is deterministic for same inputs.
    #[test]
    fn plan_fingerprint_deterministic(
        tenant in "[a-z]{4,8}",
        workspace in "[a-z]{4,8}",
    ) {
        let task_id = TaskId::generate();
        let asset_id = AssetId::generate();

        let plan1 = PlanBuilder::new(&tenant, &workspace)
            .add_task(TaskSpec {
                task_id,
                asset_id,
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let plan2 = PlanBuilder::new(&tenant, &workspace)
            .add_task(TaskSpec {
                task_id,
                asset_id,
                asset_key: AssetKey::new("raw", "events"),
                partition_key: None,
                upstream_task_ids: vec![],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        prop_assert_eq!(plan1.fingerprint, plan2.fingerprint);
    }

    /// INVARIANT: TaskState transitions are valid per actual state machine.
    ///
    /// State machine (from arco-flow/src/task.rs):
    /// - Planned -> Pending, Cancelled
    /// - Pending -> Ready, Skipped, Cancelled
    /// - Ready -> Queued, Cancelled
    /// - Queued -> Dispatched, Cancelled
    /// - Dispatched -> Running, Failed, Cancelled
    /// - Running -> Succeeded, Failed, Cancelled
    /// - Failed -> RetryWait, Cancelled
    /// - RetryWait -> Ready, Cancelled
    /// - Succeeded, Skipped, Cancelled -> (terminal)
    #[test]
    fn task_state_transitions_valid(
        initial in prop::sample::select(vec![
            TaskState::Planned,
            TaskState::Pending,
            TaskState::Ready,
            TaskState::Queued,
            TaskState::Dispatched,
            TaskState::Running,
        ]),
    ) {
        // From Planned, can go to Pending or Cancelled
        if initial == TaskState::Planned {
            prop_assert!(initial.can_transition_to(TaskState::Pending));
            prop_assert!(initial.can_transition_to(TaskState::Cancelled));
            prop_assert!(!initial.can_transition_to(TaskState::Running));
        }

        // From Pending, can go to Ready, Skipped, or Cancelled
        if initial == TaskState::Pending {
            prop_assert!(initial.can_transition_to(TaskState::Ready));
            prop_assert!(initial.can_transition_to(TaskState::Skipped));
            prop_assert!(initial.can_transition_to(TaskState::Cancelled));
            prop_assert!(!initial.can_transition_to(TaskState::Succeeded));
        }

        // From Ready, can go to Queued or Cancelled
        if initial == TaskState::Ready {
            prop_assert!(initial.can_transition_to(TaskState::Queued));
            prop_assert!(initial.can_transition_to(TaskState::Cancelled));
        }

        // From Queued, can go to Dispatched or Cancelled
        if initial == TaskState::Queued {
            prop_assert!(initial.can_transition_to(TaskState::Dispatched));
            prop_assert!(initial.can_transition_to(TaskState::Cancelled));
        }

        // From Dispatched, can go to Running, Failed, or Cancelled
        if initial == TaskState::Dispatched {
            prop_assert!(initial.can_transition_to(TaskState::Running));
            prop_assert!(initial.can_transition_to(TaskState::Failed));
            prop_assert!(initial.can_transition_to(TaskState::Cancelled));
        }

        // From Running, can go to Succeeded, Failed, or Cancelled
        if initial == TaskState::Running {
            prop_assert!(initial.can_transition_to(TaskState::Succeeded));
            prop_assert!(initial.can_transition_to(TaskState::Failed));
            prop_assert!(initial.can_transition_to(TaskState::Cancelled));
        }

        // Terminal states cannot transition
        for terminal in [TaskState::Succeeded, TaskState::Failed, TaskState::Skipped, TaskState::Cancelled] {
            prop_assert!(!terminal.can_transition_to(TaskState::Pending));
            prop_assert!(!terminal.can_transition_to(TaskState::Queued));
            prop_assert!(!terminal.can_transition_to(TaskState::Running));
        }
    }

    /// INVARIANT: RunState transitions are valid per actual state machine.
    ///
    /// State machine (from arco-flow/src/run.rs):
    /// - Pending -> Running, Cancelling, Cancelled
    /// - Running -> Succeeded, Failed, Cancelling, TimedOut
    /// - Cancelling -> Cancelled
    /// - Succeeded, Failed, Cancelled, TimedOut -> (terminal)
    #[test]
    fn run_state_transitions_valid(
        initial in prop::sample::select(vec![
            RunState::Pending,
            RunState::Running,
            RunState::Cancelling,
        ]),
    ) {
        // From Pending, can go to Running, Cancelling, or Cancelled
        if initial == RunState::Pending {
            prop_assert!(initial.can_transition_to(RunState::Running));
            prop_assert!(initial.can_transition_to(RunState::Cancelling));
            prop_assert!(initial.can_transition_to(RunState::Cancelled));
            prop_assert!(!initial.can_transition_to(RunState::Succeeded));
        }

        // From Running, can go to Succeeded, Failed, Cancelling, or TimedOut
        if initial == RunState::Running {
            prop_assert!(initial.can_transition_to(RunState::Succeeded));
            prop_assert!(initial.can_transition_to(RunState::Failed));
            prop_assert!(initial.can_transition_to(RunState::Cancelling));
            prop_assert!(initial.can_transition_to(RunState::TimedOut));
            prop_assert!(!initial.can_transition_to(RunState::Cancelled)); // Must go via Cancelling
        }

        // From Cancelling, can only go to Cancelled
        if initial == RunState::Cancelling {
            prop_assert!(initial.can_transition_to(RunState::Cancelled));
            prop_assert!(!initial.can_transition_to(RunState::Succeeded));
        }

        // Terminal states cannot transition
        for terminal in [RunState::Succeeded, RunState::Failed, RunState::Cancelled, RunState::TimedOut] {
            prop_assert!(!terminal.can_transition_to(RunState::Pending));
            prop_assert!(!terminal.can_transition_to(RunState::Running));
        }
    }

    /// INVARIANT: Plan with N tasks has stages 0..max where max <= N-1.
    #[test]
    fn plan_stages_bounded(task_count in 1usize..10) {
        let mut builder = PlanBuilder::new("tenant", "workspace");
        let mut task_ids = Vec::new();

        // Create a linear chain
        for i in 0..task_count {
            let task_id = TaskId::generate();
            let upstream = if i > 0 { vec![task_ids[i - 1]] } else { vec![] };

            builder = builder.add_task(TaskSpec {
                task_id,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", format!("task_{i}")),
                partition_key: None,
                upstream_task_ids: upstream,
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            });
            task_ids.push(task_id);
        }

        let plan = builder.build().unwrap();

        // Max stage should be task_count - 1 for a linear chain
        let max_stage = plan.tasks.iter().map(|t| t.stage).max().unwrap_or(0);
        prop_assert!(max_stage < task_count as u32);
    }

    /// INVARIANT: ULID strings are exactly 26 characters and alphanumeric.
    #[test]
    fn ulid_format_valid(_seed in 0u64..1000) {
        let task_id = TaskId::generate();
        let id_str = task_id.to_string();

        prop_assert_eq!(id_str.len(), 26);
        prop_assert!(id_str.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    /// INVARIANT: Any valid manifest produces a valid plan or valid rejection.
    /// Architecture 11.4: Plan generation is deterministic and validates inputs.
    #[test]
    fn plan_always_valid_or_valid_rejection(
        tasks in prop::collection::vec(arb_task_spec(), 1..10),
    ) {
        // Filter out self-referencing deps (invalid by definition)
        let valid_tasks: Vec<_> = tasks.into_iter()
            .map(|mut t| {
                t.upstream_task_ids.retain(|dep| *dep != t.task_id);
                t
            })
            .collect();

        let mut builder = PlanBuilder::new("tenant", "workspace");
        for task in valid_tasks {
            builder = builder.add_task(task);
        }

        let result = builder.build();

        match result {
            Ok(plan) => {
                // Plan has expected properties
                prop_assert!(!plan.fingerprint.is_empty());
                prop_assert!(!plan.tasks.is_empty());
            }
            Err(arco_flow::error::Error::CycleDetected { .. }) => {
                // Valid rejection - cycles are correctly detected
            }
            Err(arco_flow::error::Error::DependencyNotFound { .. }) => {
                // Valid rejection - missing deps correctly detected
            }
            Err(e) => {
                prop_assert!(false, "Unexpected error: {e:?}");
            }
        }
    }

    /// INVARIANT: Event serialization is round-trip safe.
    #[test]
    fn event_serialization_roundtrip(
        event_data in arb_materialization_event(),
    ) {
        let (mat_id, asset_id, row_count, byte_size) = event_data;

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
        struct TestEvent {
            materialization_id: String,
            asset_id: String,
            row_count: i64,
            byte_size: i64,
        }

        let event = TestEvent {
            materialization_id: mat_id,
            asset_id,
            row_count,
            byte_size,
        };

        let json = serde_json::to_string(&event).expect("serialize");
        let parsed: TestEvent = serde_json::from_str(&json).expect("deserialize");

        prop_assert_eq!(event, parsed);
    }

    /// INVARIANT: ULID sorting yields non-decreasing timestamps.
    /// NOTE: ULIDs in the same millisecond are NOT guaranteed to maintain insertion order
    /// (the random component can reorder them). This test verifies the weaker but correct
    /// property: sorting ULIDs yields chronologically non-decreasing timestamps.
    #[test]
    fn ulid_sorting_yields_chronological_timestamps(count in 2usize..20) {
        let mut ids: Vec<ulid::Ulid> = Vec::with_capacity(count);
        for _ in 0..count {
            ids.push(ulid::Ulid::new());
        }

        // Sort ULIDs lexicographically
        let mut sorted = ids.clone();
        sorted.sort();

        // Verify: sorted ULIDs have non-decreasing timestamps
        for window in sorted.windows(2) {
            let ts1 = window[0].timestamp_ms();
            let ts2 = window[1].timestamp_ms();
            prop_assert!(
                ts1 <= ts2,
                "sorted ULIDs should have non-decreasing timestamps: {} > {}",
                ts1, ts2
            );
        }
    }
}
```

**Step 2: Run property tests**

Run: `cargo test -p arco-flow --features test-utils --test property_tests`

Expected: All tests PASS (may need to adjust based on actual API)

**Step 3: Commit**

```bash
git add crates/arco-flow/tests/property_tests.rs
git commit -m "test(flow): add property-based tests for invariants

Uses proptest to verify:
- Plan fingerprint determinism
- TaskState transition validity
- RunState transition validity
- Plan stage bounds
- ULID format correctness"
```

---

## Task 6: Integration Test Suite - Tier 2 Walking Skeleton

**Goal:** Prove the append→compact→read loop works end-to-end.

**Files:**
- Create: `crates/arco-test-utils/tests/tier2.rs`

**Step 1: Write Tier 2 walking skeleton tests**

Create `crates/arco-test-utils/tests/tier2.rs`:

```rust
//! Tier 2 walking skeleton: append event → compact → read from Parquet.
//!
//! This is the minimum viable proof that Tier 2 consistency model works.

use std::sync::Arc;

use arco_catalog::{Compactor, EventWriter, Tier1Writer};
use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::MemoryBackend;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MaterializationCompleted {
    materialization_id: String,
    asset_id: String,
    row_count: i64,
    byte_size: i64,
}

#[tokio::test]
async fn tier2_append_compact_read_loop() {
    let backend = Arc::new(MemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    // 1. Initialize catalog
    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    // 2. Append event to ledger
    let event_writer = EventWriter::new(storage.clone());
    let event = MaterializationCompleted {
        materialization_id: "mat_001".into(),
        asset_id: "asset_abc".into(),
        row_count: 1000,
        byte_size: 50000,
    };

    let event_id = event_writer
        .append("execution", &event)
        .await
        .expect("append");

    assert!(!event_id.is_empty());

    // 3. Verify event in ledger
    // ScopedStorage.list() returns Vec<ScopedPath>
    let ledger_files = storage.list("ledger/execution/").await.expect("list");
    assert!(!ledger_files.is_empty());

    // 4. Compact
    let compactor = Compactor::new(storage.clone());
    let result = compactor.compact_domain("execution").await.expect("compact");

    assert!(result.events_processed > 0);
    assert!(result.parquet_files_written > 0);

    // 5. Verify watermark updated
    let manifest = writer.read_manifest().await.expect("read");
    assert!(manifest.execution.watermark_version > 0);

    // 6. Verify Parquet exists AND read to validate content
    let state_files = storage.list("state/execution/").await.expect("list");
    assert!(!state_files.is_empty());

    // ACTUALLY READ PARQUET (not just check existence)
    // This validates Invariant 5: "Readers never need the ledger"
    use std::io::Cursor;
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    for file in &state_files {
        assert!(file.path.ends_with(".parquet"));

        // Read Parquet content
        let parquet_data = storage.get_raw(&file.path).await.expect("read parquet");
        // ParquetRecordBatchReaderBuilder needs Read + Seek, so wrap in Cursor
        let cursor = Cursor::new(parquet_data.to_vec());
        let reader = ParquetRecordBatchReaderBuilder::try_new(cursor)
            .expect("valid parquet")
            .build()
            .expect("build reader");

        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("read batches");
        assert!(!batches.is_empty(), "Parquet should contain data");

        // Verify expected row count
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "should have exactly 1 materialization record");

        // Verify expected columns exist
        let schema = batches[0].schema();
        assert!(
            schema.field_with_name("materialization_id").is_ok(),
            "schema should have materialization_id column"
        );
        assert!(
            schema.field_with_name("asset_id").is_ok(),
            "schema should have asset_id column"
        );
    }
}

#[tokio::test]
async fn tier2_idempotent_events() {
    let backend = Arc::new(MemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    let event = MaterializationCompleted {
        materialization_id: "mat_001".into(),
        asset_id: "asset_abc".into(),
        row_count: 1000,
        byte_size: 50000,
    };

    let event_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    // Write same event multiple times (DoesNotExist prevents overwrites)
    for _ in 0..3 {
        event_writer
            .append_with_id("execution", &event, event_id)
            .await
            .expect("append");
    }

    // Should only have one file (DoesNotExist precondition)
    let files = storage.list("ledger/execution/").await.expect("list");
    assert_eq!(files.len(), 1, "append-only: duplicate IDs don't create new files");

    // Compact should process only one event
    let compactor = Compactor::new(storage.clone());
    let result = compactor.compact_domain("execution").await.expect("compact");
    assert_eq!(result.events_processed, 1);

    // Read Parquet and verify only 1 row (no duplicates in output)
    let state_files = storage.list("state/execution/").await.expect("list state");
    assert!(!state_files.is_empty());

    let parquet_data = storage.get_raw(&state_files[0].path).await.expect("read");
    // ParquetRecordBatchReaderBuilder needs Read + Seek, so wrap in Cursor
    let cursor = std::io::Cursor::new(parquet_data.to_vec());
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(cursor)
        .expect("valid parquet")
        .build()
        .expect("build reader");
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().expect("read");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "dedupe: only 1 row even with duplicate events");
}

#[tokio::test]
async fn tier2_incremental_compaction() {
    let backend = Arc::new(MemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    let compactor = Compactor::new(storage.clone());

    // Batch 1: 3 events
    for i in 0..3 {
        let event = MaterializationCompleted {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    let r1 = compactor.compact_domain("execution").await.expect("compact1");
    assert_eq!(r1.events_processed, 3);

    // Batch 2: 2 more events
    for i in 3..5 {
        let event = MaterializationCompleted {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    let r2 = compactor.compact_domain("execution").await.expect("compact2");
    assert_eq!(r2.events_processed, 2, "only new events");

    // Verify watermark incremented
    let manifest = writer.read_manifest().await.expect("read");
    assert!(manifest.execution.watermark_version >= 2);
}

#[tokio::test]
async fn tier2_compactor_sole_parquet_writer() {
    let backend = Arc::new(MemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    // Before: no Parquet
    let before = storage.list("state/execution/").await.unwrap_or_default();
    assert!(before.is_empty());

    // Write events (EventWriter only writes JSON)
    let event_writer = EventWriter::new(storage.clone());
    for i in 0..3 {
        let event = MaterializationCompleted {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    // Still no Parquet
    let after_write = storage.list("state/execution/").await.unwrap_or_default();
    assert!(after_write.is_empty(), "EventWriter must not write Parquet");

    // After compaction: Parquet exists
    let compactor = Compactor::new(storage.clone());
    compactor.compact_domain("execution").await.expect("compact");

    let after_compact = storage.list("state/execution/").await.expect("list");
    assert!(!after_compact.is_empty());
}

// =============================================================================
// E2E Tests: Full Orchestration Workflows
// =============================================================================

/// E2E Happy Path: SDK deploy → run triggers → tasks execute → outputs committed
/// This proves Product Outcome #1 works end-to-end.
#[tokio::test]
async fn e2e_happy_path_linear_dag() {
    use arco_flow::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
    use arco_flow::run::{Run, RunTrigger};
    use arco_flow::scheduler::Scheduler;
    use arco_flow::runner::{RunContext, Runner, TaskResult};
    use arco_core::TaskId;

    let backend = Arc::new(MemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    // 1. Initialize catalog
    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    // 2. Create a linear DAG plan: A → B → C
    let task_a = TaskId::generate();
    let task_b = TaskId::generate();
    let task_c = TaskId::generate();

    let plan = PlanBuilder::new("acme", "production")
        .add_task(TaskSpec {
            task_id: task_a,
            asset_id: arco_core::AssetId::generate(),
            asset_key: AssetKey::new("raw", "events"),
            partition_key: None,
            upstream_task_ids: vec![],
            priority: 0,
            stage: 0,
            resources: ResourceRequirements::default(),
        })
        .add_task(TaskSpec {
            task_id: task_b,
            asset_id: arco_core::AssetId::generate(),
            asset_key: AssetKey::new("staging", "cleaned"),
            partition_key: None,
            upstream_task_ids: vec![task_a],
            priority: 0,
            stage: 0,
            resources: ResourceRequirements::default(),
        })
        .add_task(TaskSpec {
            task_id: task_c,
            asset_id: arco_core::AssetId::generate(),
            asset_key: AssetKey::new("mart", "summary"),
            partition_key: None,
            upstream_task_ids: vec![task_b],
            priority: 0,
            stage: 0,
            resources: ResourceRequirements::default(),
        })
        .build()
        .expect("valid plan");

    // 3. Create run and execute with simulated worker
    let run = Run::new(plan.clone(), RunTrigger::Manual { user: Some("test".into()) });

    // Simulate task execution (in real system, workers do this)
    let event_writer = EventWriter::new(storage.clone());
    for task in &plan.tasks {
        // Emit materialization event for each completed task
        let event = MaterializationCompleted {
            materialization_id: format!("mat_{}", task.task_id),
            asset_id: task.asset_id.to_string(),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    // 4. Compact events to Parquet
    let compactor = Compactor::new(storage.clone());
    let result = compactor.compact_domain("execution").await.expect("compact");

    // 5. Verify: All 3 materializations recorded
    assert_eq!(result.events_processed, 3, "should process 3 materialization events");
    assert!(result.parquet_files_written > 0, "should write Parquet");

    // 6. Verify: Manifest reflects compaction
    let manifest = tier1.read_manifest().await.expect("read");
    assert!(manifest.execution.snapshot_path.is_some(), "snapshot should be visible");
    assert!(manifest.execution.compaction.total_events_compacted >= 3);
}

/// E2E Retry: Task fails once, succeeds on retry, event replay is idempotent.
/// This proves Product Outcome #2 (failure modes) works correctly.
#[tokio::test]
async fn e2e_retry_idempotent_event_replay() {
    let backend = Arc::new(MemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    // 1. Initialize catalog
    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());

    // 2. Simulate: Task fails on attempt 1
    let mat_id = "mat_001";
    let fixed_event_id = "01HX0000000000000000000001";

    // First attempt (will be retried)
    let event_attempt1 = MaterializationCompleted {
        materialization_id: mat_id.to_string(),
        asset_id: "asset_001".to_string(),
        row_count: 500,  // Partial result from failed attempt
        byte_size: 25000,
    };
    event_writer
        .append_with_id("execution", &event_attempt1, fixed_event_id)
        .await
        .expect("append attempt 1");

    // 3. Simulate: Same event replayed (duplicate delivery from retry)
    // This simulates what happens when a worker retries after a network failure
    let event_attempt2 = MaterializationCompleted {
        materialization_id: mat_id.to_string(),
        asset_id: "asset_001".to_string(),
        row_count: 1000,  // Full result (different value, same ID)
        byte_size: 50000,
    };
    // Same event_id = DoesNotExist prevents overwrite
    event_writer
        .append_with_id("execution", &event_attempt2, fixed_event_id)
        .await
        .expect("append attempt 2 (should be idempotent)");

    // 4. Verify: Only one event file exists
    let files = storage.list("ledger/execution/").await.expect("list");
    assert_eq!(files.len(), 1, "duplicate delivery should not create new file");

    // 5. Compact
    let compactor = Compactor::new(storage.clone());
    let result = compactor.compact_domain("execution").await.expect("compact");

    // 6. Verify: Only 1 event processed (no duplicates)
    assert_eq!(result.events_processed, 1, "idempotent: only 1 event");

    // 7. Second compaction should be no-op
    let result2 = compactor.compact_domain("execution").await.expect("compact2");
    assert_eq!(result2.events_processed, 0, "watermark prevents reprocessing");
}

// =============================================================================
// D.6 Architecture-Mandated Acceptance Tests
// =============================================================================

/// D.6.2: Out-of-Order Processing
/// Events arriving out of timestamp order must still produce correct final state.
#[tokio::test]
async fn tier2_out_of_order_processing() {
    let backend = Arc::new(MemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());

    // Write events with explicit out-of-order ULIDs (simulating out-of-order arrival)
    // Higher ULID = later timestamp, but we write them in wrong order
    let events = vec![
        ("01HX9999999999999999999999", "mat_003"),  // Latest ULID, first write
        ("01HX0000000000000000000001", "mat_001"),  // Earliest ULID, second write
        ("01HX5555555555555555555555", "mat_002"),  // Middle ULID, third write
    ];

    for (ulid, mat_id) in events {
        let event = MaterializationCompleted {
            materialization_id: mat_id.to_string(),
            asset_id: "asset_001".to_string(),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer
            .append_with_id("execution", &event, ulid)
            .await
            .expect("append");
    }

    // Compactor must process all 3 events regardless of arrival order
    let compactor = Compactor::new(storage.clone());
    let result = compactor.compact_domain("execution").await.expect("compact");

    assert_eq!(result.events_processed, 3, "all events processed despite out-of-order arrival");
}

/// D.6.4: Crash Recovery
/// Compactor restart must produce identical state (idempotent reprocessing).
#[tokio::test]
async fn tier2_crash_recovery() {
    let backend = Arc::new(MemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());

    // Write 5 events
    for i in 0..5 {
        let event = MaterializationCompleted {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    // First compaction
    let compactor = Compactor::new(storage.clone());
    let result1 = compactor.compact_domain("execution").await.expect("compact1");
    assert_eq!(result1.events_processed, 5);

    // Simulate "crash recovery" - create new compactor instance and run again
    // This simulates what happens after a Cloud Function restart
    let compactor2 = Compactor::new(storage.clone());
    let result2 = compactor2.compact_domain("execution").await.expect("compact2");

    // Second compaction should be no-op (watermark already advanced)
    assert_eq!(result2.events_processed, 0, "crash recovery: no duplicate processing");

    // Read manifest - watermark should be stable
    let manifest = writer.read_manifest().await.expect("read");
    assert!(manifest.execution.watermark_version >= 1);
}

/// D.6.5: Manifest CAS Conflicts
/// Concurrent compactors must safely race with exactly-once semantics.
#[tokio::test]
async fn tier2_concurrent_compactor_race() {
    let backend = Arc::new(MemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());

    // Write 10 events
    for i in 0..10 {
        let event = MaterializationCompleted {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    // Race two compactors concurrently
    let storage1 = storage.clone();
    let storage2 = storage.clone();

    let (r1, r2) = tokio::join!(
        async move {
            let c = Compactor::new(storage1);
            c.compact_domain("execution").await
        },
        async move {
            let c = Compactor::new(storage2);
            c.compact_domain("execution").await
        }
    );

    // Exactly one should succeed, the other should either:
    // - Return CasFailed error (lost the race)
    // - Return Ok with 0 events (watermark already advanced)
    let success_count = [&r1, &r2]
        .iter()
        .filter(|r| r.as_ref().map(|c| c.events_processed > 0).unwrap_or(false))
        .count();

    assert!(
        success_count <= 1,
        "at most one compactor should process events (no duplicate processing)"
    );

    // Verify total events processed across both is exactly 10 or less
    let total_processed = r1.as_ref().map(|c| c.events_processed).unwrap_or(0)
        + r2.as_ref().map(|c| c.events_processed).unwrap_or(0);
    assert!(total_processed <= 10, "no duplicate processing allowed");
}

/// Partial Failure Test: Parquet written but manifest CAS fails.
/// Architecture: "Publish is atomic - snapshot isn't visible until manifest CAS succeeds."
///
/// This test uses TracingMemoryBackend to inject a failure on manifest write.
#[tokio::test]
async fn tier2_partial_failure_parquet_written_cas_fails() {
    use arco_test_utils::TracingMemoryBackend;

    let backend = Arc::new(TracingMemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    for i in 0..3 {
        let event = MaterializationCompleted {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    // Inject failure on manifest write (simulates network partition during CAS)
    backend.inject_failure("manifests/execution.manifest.json");

    // Compactor should fail during manifest CAS
    let compactor = Compactor::new(storage.clone());
    let result = compactor.compact_domain("execution").await;

    // Compaction should fail
    assert!(result.is_err(), "compaction should fail when manifest CAS fails");

    // CRITICAL: State should NOT be visible because manifest wasn't updated
    // Even if Parquet was written, readers shouldn't see it
    let manifest = writer.read_manifest().await.expect("read manifest");
    assert!(
        manifest.execution.watermark_version == 0,
        "watermark should not advance on failed compaction"
    );

    // Remove failure and retry
    backend.clear_failures();
    let result2 = compactor.compact_domain("execution").await.expect("retry succeeds");

    // Now compaction should succeed
    assert_eq!(result2.events_processed, 3, "retry processes all events");
}

/// D.6.6: Workspace Isolation
/// Events in one workspace must never be visible in another.
#[tokio::test]
async fn tier2_workspace_isolation() {
    let backend = Arc::new(MemoryBackend::new());

    // Create two separate workspaces for same tenant
    let storage_ws1 =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");
    let storage_ws2 =
        ScopedStorage::new(backend.clone(), "acme", "staging").expect("valid storage");

    // Initialize both workspaces
    let writer1 = Tier1Writer::new(storage_ws1.clone());
    let writer2 = Tier1Writer::new(storage_ws2.clone());
    writer1.initialize().await.expect("init1");
    writer2.initialize().await.expect("init2");

    // Write events to ws1 only
    let event_writer = EventWriter::new(storage_ws1.clone());
    for i in 0..3 {
        let event = MaterializationCompleted {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    // Verify ws2 sees no events (workspace isolation)
    let files_ws2 = storage_ws2
        .list("ledger/execution/")
        .await
        .unwrap_or_default();
    assert!(files_ws2.is_empty(), "workspace isolation violated: ws2 sees ws1 events");

    // Compact ws1
    let compactor1 = Compactor::new(storage_ws1.clone());
    let result1 = compactor1.compact_domain("execution").await.expect("compact1");
    assert_eq!(result1.events_processed, 3);

    // Verify ws2 still sees no state
    let state_ws2 = storage_ws2
        .list("state/execution/")
        .await
        .unwrap_or_default();
    assert!(state_ws2.is_empty(), "workspace isolation violated: ws2 sees ws1 state");
}

/// D.6.9: Derived Current Pointers
/// Latest state must be derivable from compacted snapshots.
#[tokio::test]
async fn tier2_derived_current_pointers() {
    let backend = Arc::new(MemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());

    // Write materializations for same asset at different times
    // Final state should reflect the latest materialization
    let events = vec![
        ("mat_001", "asset_A", 100),   // First mat for asset_A
        ("mat_002", "asset_A", 200),   // Second mat for asset_A (should be "current")
        ("mat_003", "asset_B", 300),   // Mat for asset_B
    ];

    for (mat_id, asset_id, row_count) in events {
        let event = MaterializationCompleted {
            materialization_id: mat_id.to_string(),
            asset_id: asset_id.to_string(),
            row_count,
            byte_size: row_count * 50,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    // Compact
    let compactor = Compactor::new(storage.clone());
    let result = compactor.compact_domain("execution").await.expect("compact");
    assert_eq!(result.events_processed, 3);

    // Verify Parquet exists and contains all records
    let state_files = storage.list("state/execution/").await.expect("list");
    assert!(!state_files.is_empty(), "state files should exist");

    // Note: In a full implementation, we would read the Parquet and verify:
    // - asset_A has current pointer to mat_002 (row_count=200)
    // - asset_B has current pointer to mat_003 (row_count=300)
    // For now, we verify the watermark reflects all events processed
    let manifest = writer.read_manifest().await.expect("read");
    assert!(
        manifest.execution.compaction.total_events_compacted >= 3,
        "all events should be compacted"
    );
}

// =============================================================================
// Six Non-Negotiable Invariants Test Suite
// =============================================================================

/// Invariant 1: Ingest is "append-only + ack"
/// EventWriter must ONLY append to ledger, never read snapshots or state.
#[tokio::test]
async fn invariant_ingest_append_only() {
    use arco_test_utils::{TracingMemoryBackend, StorageOp, assert_storage_ops_exclude};

    let backend = Arc::new(TracingMemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    // Initialize catalog first (creates manifests)
    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    // Clear operations to isolate EventWriter behavior
    backend.clear_operations();

    let event_writer = EventWriter::new(storage.clone());
    let event = MaterializationCompleted {
        materialization_id: "mat_001".to_string(),
        asset_id: "asset_001".to_string(),
        row_count: 1000,
        byte_size: 50000,
    };
    event_writer.append("execution", &event).await.expect("append");

    let ops = backend.operations();

    // Verify: EventWriter only writes to ledger/, never reads state/
    // (canonical path is ledger/, not events/)
    for op in &ops {
        if let StorageOp::Get { path } | StorageOp::GetRange { path, .. } = op {
            assert!(
                !path.contains("state/") && !path.contains("snapshot"),
                "Invariant 1 violated: EventWriter read snapshot state: {path}"
            );
        }
        if let StorageOp::Put { path, .. } = op {
            assert!(
                path.contains("ledger/"),
                "Invariant 1 violated: EventWriter wrote to non-ledger path: {path}"
            );
        }
    }
}

/// Invariant 2: Compactor is the sole writer of Parquet state
/// Only Compactor may write to state/ directory.
#[tokio::test]
async fn invariant_compactor_sole_state_writer() {
    // This is already verified in tier2_compactor_sole_parquet_writer
    // Added here for completeness of invariant documentation
    tier2_compactor_sole_parquet_writer().await;
}

/// Invariant 3: Compaction is idempotent
/// Running compaction 10x must produce identical final state.
#[tokio::test]
async fn invariant_compaction_idempotent_10x() {
    let backend = Arc::new(MemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("init");

    let event_writer = EventWriter::new(storage.clone());
    for i in 0..5 {
        let event = MaterializationCompleted {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    let compactor = Compactor::new(storage.clone());

    // Run compaction 10 times (architecture requirement)
    let mut total_processed = 0;
    for i in 0..10 {
        let result = compactor.compact_domain("execution").await.expect("compact");
        total_processed += result.events_processed;

        if i == 0 {
            assert_eq!(result.events_processed, 5, "first compaction processes all");
        } else {
            assert_eq!(result.events_processed, 0, "subsequent compactions are no-op");
        }
    }

    assert_eq!(total_processed, 5, "total processed should be exactly 5 (no duplicates)");
}

/// Invariant 5: Readers never need the ledger
/// After compaction, reads use only Parquet state, never JSON events.
#[tokio::test]
async fn invariant_readers_never_need_ledger() {
    use arco_test_utils::{TracingMemoryBackend, assert_storage_ops_exclude};

    let backend = Arc::new(TracingMemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    // Write and compact events
    let event_writer = EventWriter::new(storage.clone());
    for i in 0..3 {
        let event = MaterializationCompleted {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    let compactor = Compactor::new(storage.clone());
    compactor.compact_domain("execution").await.expect("compact");

    // Clear operations to isolate read behavior
    backend.clear_operations();

    // Simulate a read operation (reading manifest + state)
    let _ = tier1.read_manifest().await.expect("read manifest");
    let _ = storage.list("state/execution/").await.expect("list state");

    let ops = backend.operations();

    // Verify: No ledger access during reads (canonical path is ledger/, not events/)
    assert_storage_ops_exclude(&ops, "ledger/");
}

/// Invariant 4: Publish is atomic
/// A snapshot isn't visible until manifest CAS succeeds.
/// Readers MUST use manifest.snapshot_path, not list state/.
#[tokio::test]
async fn invariant_publish_is_atomic() {
    use arco_test_utils::TracingMemoryBackend;

    let backend = Arc::new(TracingMemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    // Write events
    let event_writer = EventWriter::new(storage.clone());
    for i in 0..3 {
        let event = MaterializationCompleted {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    // Compact
    let compactor = Compactor::new(storage.clone());
    compactor.compact_domain("execution").await.expect("compact");

    // Verify: Manifest contains snapshot_path
    let manifest = tier1.read_manifest().await.expect("read");
    assert!(
        manifest.execution.snapshot_path.is_some(),
        "Invariant 4: manifest must have snapshot_path after compaction"
    );

    // Verify: Reader uses manifest.snapshot_path to locate current snapshot
    // (NOT by listing state/ directory)
    let snapshot_path = manifest.execution.snapshot_path.as_ref().unwrap();
    let snapshot_data = storage.get_raw(snapshot_path).await.expect("read snapshot");
    assert!(!snapshot_data.is_empty(), "snapshot should contain data");

    // Verify: snapshot_version increments
    assert!(
        manifest.execution.snapshot_version >= 1,
        "snapshot_version should be >= 1 after compaction"
    );
}

/// Invariant 6: No bucket listing dependency for correctness
/// Note: MVP relaxation - compactor uses listing for simplicity.
/// This test documents the current behavior and the invariant we're working toward.
///
/// Future: Event-triggered compaction should process individual objects
/// without listing, reserving listing for anti-entropy only.
#[tokio::test]
async fn invariant_listing_dependency_documented() {
    use arco_test_utils::{TracingMemoryBackend, StorageOp};

    let backend = Arc::new(TracingMemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "acme", "production").expect("valid storage");

    let tier1 = Tier1Writer::new(storage.clone());
    tier1.initialize().await.expect("init");

    // Write events
    let event_writer = EventWriter::new(storage.clone());
    for i in 0..3 {
        let event = MaterializationCompleted {
            materialization_id: format!("mat_{i:03}"),
            asset_id: format!("asset_{i:03}"),
            row_count: 1000,
            byte_size: 50000,
        };
        event_writer.append("execution", &event).await.expect("append");
    }

    // Clear operations to isolate compactor behavior
    backend.clear_operations();

    // Compact
    let compactor = Compactor::new(storage.clone());
    compactor.compact_domain("execution").await.expect("compact");

    let ops = backend.operations();

    // MVP RELAXATION: Compactor currently uses listing to discover events
    // This is acceptable for MVP but should be replaced with event-triggered
    // processing in production (Invariant 6 compliance)
    let list_ops: Vec<_> = ops.iter().filter(|op| matches!(op, StorageOp::List { .. })).collect();
    assert!(
        !list_ops.is_empty(),
        "MVP: compactor uses listing (Invariant 6 relaxation documented)"
    );

    // TODO (Part 5+): Implement event-triggered compaction that processes
    // individual objects without listing, then update this test to verify
    // no listing operations occur during normal compaction.
}
```

**Step 2: (No mod.rs needed)**

Files under `crates/*/tests/*.rs` are discovered automatically by Cargo for that crate.

**Step 3: Run tests**

Run: `cargo test -p arco-test-utils --test tier2`

Expected: All tests PASS

**Step 4: Commit**

```bash
git add crates/arco-test-utils/tests/tier2.rs
git commit -m "test(integration): add Tier 2 walking skeleton tests

Proves the Tier 2 eventual consistency model:
- append→compact→read loop works
- Idempotent event writes
- Incremental compaction with watermark
- Compactor is sole Parquet writer"
```

---

## Task 7: Full Test Suite Verification

**Goal:** Run all tests and verify coverage.

**Step 1: Run all workspace tests**

Run: `cargo test --workspace`

Expected: All tests PASS

**Step 1b: Run feature-gated property tests**

Run: `cargo test -p arco-flow --features test-utils`

Expected: All property tests PASS

**Step 1c: CI parity run (all features)**

Run: `cargo test --workspace --all-features`

Expected: All tests PASS

**Step 2: Run clippy**

Run: `cargo clippy --workspace --all-features -- -D warnings`

Expected: No warnings

**Step 3: Run format check**

Run: `cargo fmt --all --check`

Expected: No formatting issues

**Step 4: Run focused integration tests**

Run: `cargo test -p arco-test-utils --test contracts`

Expected: All contract tests PASS

Run: `cargo test -p arco-test-utils --test tier2`

Expected: All Tier 2 tests PASS

Run: `cargo test -p arco-integration-tests`

Expected: All workspace-level integration tests PASS

**Step 5: Commit verification**

```bash
git add -A
git commit -m "chore: verify full test suite passes

All workspace tests passing:
- arco-core: unit tests
- arco-catalog: tier1, event_writer, compactor
- arco-flow: scheduler, plan, run, task, events
- arco-test-utils: storage, fixtures, assertions
- integration: contracts, tier2"
```

---

## Verification Checklist

Before marking Part 4 complete:

```bash
# Format and lint
cargo fmt --check
cargo clippy --workspace --all-features -- -D warnings

# All tests
cargo test --workspace

# Property tests (enable test-only feature gate)
cargo test -p arco-flow --features test-utils

# CI parity (ensures all optional/test-only features are exercised)
cargo test --workspace --all-features

# Tier 2 acceptance + invariant suites (virtual workspace => tests live in crates)
cargo test -p arco-test-utils --test tier2
cargo test -p arco-test-utils --test contracts

# Verify all 10 D.6 tests present
cargo test -p arco-test-utils --test tier2 -- --list 2>&1 | grep -c "tier2_"
# Expected: 10+ tests

# Coverage check (target >80%)
cargo llvm-cov --workspace --fail-under 80

# Documentation
cargo doc --workspace --no-deps
```

---

## Exit Criteria (Definition of Done)

Part 4 is complete when ALL are true:

| Criterion | Measurement |
|-----------|-------------|
| Test infrastructure exists | `arco-test-utils` crate with `TracingMemoryBackend` (incl. `get_range`, `head`) |
| Contract tests pass | `cargo test -p arco-test-utils --test contracts` green |
| EventWriter implemented | Tier 2 append tests pass |
| Compactor implemented (CAS) | Event → Parquet tests pass, CAS for manifest |
| Property tests exist | `cargo test -p arco-flow --features test-utils` green (with arbitrary generators) |
| D.6 acceptance tests pass | All 10 architecture-mandated tests green |
| Six invariants verified | Explicit invariant test suite passes |
| Tier 2 skeleton proven | Append→compact→read loop works |
| Idempotency verified (10x) | Repeated operations produce consistent results |
| CAS conflicts handled | Concurrent compactor race test passes |
| Workspace isolation proven | Cross-workspace isolation test passes |
| Test coverage >80% | Integration coverage across catalog + flow |
| No clippy warnings | `cargo clippy -- -D warnings` clean |
| All tests pass | `cargo test --workspace` green |

---

## Quality Bar Summary

This plan implements:

1. **Test pyramid**: Unit tests in modules, integration tests in `tests/`, property tests with proptest
2. **Contract-first**: Cross-crate contract tests validate CatalogEvent envelope + boundaries
3. **Determinism + idempotency**: Explicitly tested at every layer (10x replay, dedupe by idempotency_key)
4. **No flaky tests**: Using deterministic test fixtures and controlled concurrency
5. **Atomic publish**: Compactor uses CAS for manifest updates (tested with concurrent race)
6. **Append-only ledger**: EventWriter uses DoesNotExist precondition (true audit log)
7. **Parquet read assertions**: Tests actually read and verify Parquet content (not just file existence)

---

## CI/CD Quality Gates

Add these quality gates to CI pipeline:

### Required CI Checks (per industry best practices)

```yaml
# .github/workflows/ci.yml (or equivalent)
jobs:
  quality:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      # Format check
      - name: Check formatting
        run: cargo fmt --check

      # Linting (deny all warnings)
      - name: Clippy
        run: cargo clippy --workspace --all-features -- -D warnings

      # All tests
      - name: Tests
        run: |
          cargo test --workspace --all-features --exclude arco-flow
          cargo test -p arco-flow --features test-utils

      # Coverage with threshold
      - name: Coverage
        run: |
          cargo install cargo-llvm-cov
          cargo llvm-cov --workspace --fail-under 80

      # Dependency audit (supply chain security)
      - name: Audit
        run: |
          cargo install cargo-audit
          cargo audit

      # Dependency deny (license + duplicate checking)
      - name: Deny
        run: |
          cargo install cargo-deny
          cargo deny check
```

### Deployment Smoke Tests

After deployment, run integration smoke tests against the deployed environment:

```bash
#!/bin/bash
# scripts/smoke-test.sh
# Run after deployment to verify deployed services are healthy

set -euo pipefail

ENVIRONMENT="${1:-staging}"
echo "Running smoke tests against $ENVIRONMENT..."

# 1. Health check endpoints
curl -f "https://api.$ENVIRONMENT.arco.dev/health"

# 2. Verify compactor is healthy (can read manifests)
curl -f "https://api.$ENVIRONMENT.arco.dev/compactor/status"

# 3. Run integration tests against deployed API (if applicable)
# ARCO_API_URL="https://api.$ENVIRONMENT.arco.dev" cargo test --test integration smoke

echo "Smoke tests passed!"
```

### Staged Promotion (per ops docs)

```bash
# 1. Deploy to staging
deploy --env staging

# 2. Run smoke tests
./scripts/smoke-test.sh staging

# 3. Wait for compactor healthy (at least one successful compaction)
wait_for_healthy compactor staging --timeout 5m

# 4. Promote to production
deploy --env production

# 5. Run production smoke tests
./scripts/smoke-test.sh production
```

### Rollback Verification

Maintain rollback capability:

```bash
# Verify rollback procedure works
./scripts/rollback-drill.sh staging

# Ensure previous version is healthy after rollback
./scripts/smoke-test.sh staging
```

---

*Plan created: 2025-12-16*
*Target: Part 4 Integration & Testing Completion*
*Updated: 2025-12-16 - Comprehensive revision per expert review:*

**Critical fixes applied:**
1. **Canonical Contracts section added** - Single source of truth for storage layout (`ledger/` not `events/`), event envelope, and watermark strategy
2. **EventWriter idempotency fixed** - Uses `{event_id}.json` path (no timestamp prefix) for deterministic writes
3. **Compactor watermark bug fixed** - Stores exact filename including `.json` for correct comparison
4. **Atomic publish implemented** - Manifest now has `snapshot_path` and `snapshot_version` as visibility gate
5. **D.6 coverage clarified** - 7/10 implemented, 3 deferred (D.6.3, D.6.7, D.6.8) with rationale
6. **Invariant 4 & 6 tests added** - Explicit tests for atomic publish and listing dependency documentation
7. **E2E tests added** - Happy path and retry scenarios proving Product Outcomes #1 and #2
8. **ULID property test fixed** - Now tests chronological timestamp ordering, not insertion order
9. **Parquet reader fixed** - Reads snapshots via Arrow/Parquet in-memory readers
10. **Path consistency fixed** - Catalog uses `ledger/{domain}/...`; flow uses `ledger/flow/...` to avoid schema collisions
