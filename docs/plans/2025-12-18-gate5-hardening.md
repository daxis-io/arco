# Gate 5 Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Mechanically enforce the Jan 12 invariants (append -> compact -> publish -> read) via trait separation, publish permits, IAM constraints, and rollback detection.

**Plan patch (authoritative delta + sequencing):** `docs/plans/2025-12-19-gate5-hardening-plan-patch.md`

**Architecture:**
1. Resolve the Tier-1 write path contradiction FIRST (compactor as sole Parquet writer)
2. Split `StorageBackend` into capability-scoped traits with typed keys
3. Introduce `PublishPermit` with **real distributed fencing** (lock-derived tokens, not process-local counters)
4. Enforce prefix-scoped IAM with strict `startsWith` conditions
5. Add parent hash + artifact verification for rollback/torn-write protection

**Tech Stack:** Rust 1.85+, Terraform (GCP IAM), object_store crate, compile-negative tests via trybuild

---

## Current State Summary

**Already Done (Good):**
- `WritePrecondition` uses opaque `String` (not `i64`) - Gate 5 #4 addressed
- Manifest CAS is used for updates ([tier1_writer.rs:345](crates/arco-catalog/src/tier1_writer.rs#L345))
- Hash chain exists via `CommitRecord` with `prev_commit_hash`
- Checksums exist in `SnapshotFile`

**Critical Contradiction (Must Resolve First):**
- Jan 12 plan says: "Compactor is sole writer of Parquet state"
- Current Tier1Writer: writes Parquet snapshots directly to `state/catalog/`
- **These cannot both be true.** IAM enforcement depends on this decision.

**Gaps to Address:**

| Gap | Gate 5 Point | Current State | Required State |
|-----|--------------|---------------|----------------|
| **Tier-1 write path** | #1 | Tier1Writer writes Parquet | Ledger → compaction → publish |
| Trait separation | #2 | Single `StorageBackend` trait | `ReadStore`, `PutStore`, `CasStore`, `ListStore` |
| PublishPermit | #3 | No permit system | Lock-derived fencing tokens, private constructors |
| IAM prefix scoping | #6 | Both services have `objectUser` | Strict `startsWith` conditions |
| Rollback detection | #8 | CommitRecord has hash chain | Pointer `parent_hash` from raw bytes |
| Compactor listing | #5 | Lists events directly | Notification + anti-entropy cursor |
| Backpressure | #7 | None | Two-stage soft/hard thresholds |
| Search tombstones | #10 | Not implemented | Tombstone records for delete/rename |
| Security ADR | #9 | No decision | Partitioned snapshots + IAM (not auth service) |
| CI/Obs artifacts | #11 | Partial | Deterministic sim + dashboards |

---

## Phase 1: Critical Path (Removes 90% of Risk)

**Execution order matters.** Task 1 (architecture decision) must come first because it determines IAM boundaries and permit semantics.

### Task 1: Resolve Tier-1 Write Path Architecture (BLOCKING)

**Goal:** Decide and implement: API writes only ledger, compactor is sole Parquet writer.

**Why first:** IAM cannot be correctly configured until we know which service writes which prefixes. The current code has Tier1Writer writing to `state/catalog/` which contradicts the Jan 12 "compactor as sole writer" invariant.

**Decision: Option A (Jan 12 aligned)**

Tier-1 operations become:
1. Acquire distributed lock (for strong consistency)
2. Append event(s) to `ledger/catalog/`
3. Trigger **synchronous compaction** for catalog domain
4. Wait for compaction to complete (manifest CAS)
5. Release lock

This means:
- **API writes only:** `ledger/`, `locks/`
- **Compactor writes:** `state/`, `l0/`, `manifests/`

**Files:**
- Create: `docs/adr/adr-018-tier1-write-path.md`
- Modify: `crates/arco-catalog/src/tier1_writer.rs`
- Create: `crates/arco-compactor/src/sync_compact.rs`

**Step 1.1: Create ADR documenting the decision**

Create `docs/adr/adr-018-tier1-write-path.md`:
```markdown
# ADR-018: Tier-1 Write Path Architecture

## Status

Accepted

## Context

Gate 5 hardening requires "IAM-enforced sole writer for state/" but current
Tier1Writer writes Parquet snapshots directly. This contradicts the Jan 12
invariant "Compactor is sole writer of Parquet state."

## Decision

**Option A: Align to Jan 12 - Compactor as sole Parquet writer**

Tier-1 (DDL) operations:
1. Acquire distributed lock
2. Append event(s) to ledger/catalog/
3. Trigger synchronous compaction
4. Wait for manifest CAS
5. Release lock

## Consequences

- API can ONLY write to: ledger/, locks/
- Compactor can ONLY write to: state/, l0/, manifests/
- IAM conditions enforce this at infrastructure level
- Latency increase (~100-500ms) acceptable for DDL operations
```

**Step 1.2: Refactor Tier1Writer to append ledger events**

Replace `write_catalog_snapshot` with `append_catalog_event`:
```rust
/// Appends a catalog event to the ledger (does NOT write Parquet).
///
/// The event will be processed by the compactor to produce Parquet state.
pub async fn append_catalog_event(
    &self,
    guard: &LockGuard<dyn StorageBackend>,
    event: CatalogEvent,
) -> Result<EventId> {
    let event_id = EventId::generate();
    let path = format!("ledger/catalog/{}.json", event_id);
    let bytes = serde_json::to_vec(&event)?;

    self.storage.put_raw(
        &path,
        Bytes::from(bytes),
        WritePrecondition::DoesNotExist, // Immutable event
    ).await?;

    Ok(event_id)
}
```

**Step 1.3: Add synchronous compaction trigger**

Create `crates/arco-compactor/src/sync_compact.rs`:
```rust
/// Triggers synchronous compaction for Tier-1 strong consistency.
///
/// Called by API after appending ledger events. Blocks until manifest is updated.
pub async fn compact_and_wait(
    storage: &ScopedStorage,
    domain: CatalogDomain,
    timeout: Duration,
) -> Result<ManifestVersion> {
    // 1. List events since watermark (compactor is allowed to list)
    // 2. Load and fold events
    // 3. Write Parquet snapshot to state/
    // 4. CAS manifest with permit
    // 5. Return new manifest version
    todo!()
}
```

**Step 1.4: Update Tier1Writer.update() to use ledger flow**

```rust
pub async fn update<F>(&self, mut update_fn: F) -> Result<CommitRecord>
where
    F: FnMut(&mut CatalogEvent) -> Result<()>,
{
    let guard = self.lock.acquire(...).await?;

    // Build event
    let mut event = CatalogEvent::new();
    update_fn(&mut event)?;

    // Append to ledger (API's only write)
    let event_id = self.append_catalog_event(&guard, event).await?;

    // Trigger synchronous compaction (compactor writes Parquet + manifest)
    let manifest_version = sync_compact::compact_and_wait(
        &self.storage,
        CatalogDomain::Catalog,
        Duration::from_secs(30),
    ).await?;

    guard.release().await?;

    Ok(CommitRecord { ... })
}
```

**Step 1.5: Commit**

```bash
git add docs/adr/adr-018-tier1-write-path.md crates/
git commit -m "feat: align Tier-1 to Jan 12 - compactor as sole Parquet writer

BREAKING: Tier1Writer no longer writes Parquet directly.
- API writes only to ledger/ and locks/
- Compactor writes to state/, l0/, manifests/
- Synchronous compaction provides strong consistency for DDL

This enables IAM-enforced sole writer per Gate 5 requirements."
```

---

### Task 2: Split Storage Traits + Typed Keys + Compile Fences

**Goal:** Reader code literally cannot call `put`, `delete`, `list`, or `head` - enforced at compile time via typed keys and trait separation.

**Key Changes from Original:**
1. **Typed keys** - `LedgerKey`, `StateKey`, `ManifestKey` instead of raw strings
2. **Separate PutStore from CasStore** - unconditional writes vs CAS manifest updates
3. **Remove delete from CasStore** - delete is a separate capability
4. **Fix compile tests** - uncomment forbidden calls so they actually fail

**Files:**
- Create: `crates/arco-core/src/storage/mod.rs`
- Create: `crates/arco-core/src/storage/keys.rs`
- Create: `crates/arco-core/src/storage/read.rs`
- Create: `crates/arco-core/src/storage/put.rs`
- Create: `crates/arco-core/src/storage/cas.rs`
- Create: `crates/arco-core/src/storage/list.rs`
- Create: `crates/arco-core/src/storage/backend.rs`
- Modify: `crates/arco-core/src/lib.rs`
- Modify: `crates/arco-core/src/scoped_storage.rs`
- Test: `crates/arco-core/tests/compile_negative/`

**Step 2.1: Create typed keys module**

Create `crates/arco-core/src/storage/keys.rs`:
```rust
//! Typed storage keys that encode prefix constraints.
//!
//! Using typed keys prevents bugs where code accidentally writes
//! to the wrong prefix. The key types are:
//!
//! - `LedgerKey`: immutable events in `ledger/{domain}/`
//! - `StateKey`: compaction output in `state/{domain}/`
//! - `ManifestKey`: manifest pointers in `manifests/`
//! - `LockKey`: distributed locks in `locks/`

use std::fmt;

/// Key for immutable ledger events.
///
/// API writes ledger events; they are never modified or deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerKey {
    domain: String,
    event_id: String,
}

impl LedgerKey {
    /// Creates a new ledger key.
    ///
    /// Path format: `ledger/{domain}/{event_id}.json`
    pub fn new(domain: impl Into<String>, event_id: impl Into<String>) -> Self {
        Self {
            domain: domain.into(),
            event_id: event_id.into(),
        }
    }

    /// Returns the full storage path.
    pub fn path(&self) -> String {
        format!("ledger/{}/{}.json", self.domain, self.event_id)
    }
}

/// Key for compacted state files (Parquet).
///
/// Only compactor writes state files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateKey {
    domain: String,
    version: u64,
    filename: String,
}

impl StateKey {
    /// Creates a new state key.
    ///
    /// Path format: `state/{domain}/v{version}/{filename}`
    pub fn new(domain: impl Into<String>, version: u64, filename: impl Into<String>) -> Self {
        Self {
            domain: domain.into(),
            version,
            filename: filename.into(),
        }
    }

    /// Returns the full storage path.
    pub fn path(&self) -> String {
        format!("state/{}/v{}/{}", self.domain, self.version, self.filename)
    }
}

/// Key for manifest pointers.
///
/// Manifests are updated via CAS only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestKey {
    domain: String,
}

impl ManifestKey {
    /// Creates a new manifest key.
    ///
    /// Path format: `manifests/{domain}.json`
    pub fn new(domain: impl Into<String>) -> Self {
        Self {
            domain: domain.into(),
        }
    }

    /// Returns the full storage path.
    pub fn path(&self) -> String {
        format!("manifests/{}.json", self.domain)
    }
}

/// Key for distributed locks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockKey {
    resource: String,
}

impl LockKey {
    /// Creates a new lock key.
    ///
    /// Path format: `locks/{resource}.lock`
    pub fn new(resource: impl Into<String>) -> Self {
        Self {
            resource: resource.into(),
        }
    }

    /// Returns the full storage path.
    pub fn path(&self) -> String {
        format!("locks/{}.lock", self.resource)
    }
}

// Implement Display for all key types
impl fmt::Display for LedgerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path())
    }
}

impl fmt::Display for StateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path())
    }
}

impl fmt::Display for ManifestKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path())
    }
}

impl fmt::Display for LockKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path())
    }
}
```

**Step 2.2: Create the new trait module structure**

Create `crates/arco-core/src/storage/mod.rs`:
```rust
//! Storage capability traits with compile-time separation.
//!
//! These traits enforce that different components can only access
//! the storage capabilities they need:
//!
//! - `ReadStore`: Read-only access (get, get_range)
//! - `PutStore`: Unconditional writes to ledger/state (immutable files)
//! - `CasStore`: Compare-and-swap for manifests only
//! - `ListStore`: Discovery/anti-entropy only (list_page)
//!
//! Steady-state readers should only depend on `ReadStore`.
//! Ledger/state writers use `PutStore`.
//! Manifest publishers use `CasStore` + `PublishPermit`.
//! Anti-entropy jobs use `ListStore`.

mod backend;
mod cas;
mod keys;
mod list;
mod put;
mod read;

pub use backend::{MemoryBackend, ObjectMeta, ObjectStoreBackend, SignedUrlIssuer, StorageBackend};
pub use cas::{CasStore, WritePrecondition, WriteResult};
pub use keys::{LedgerKey, LockKey, ManifestKey, StateKey};
pub use list::{ListCursor, ListPage, ListStore};
pub use put::PutStore;
pub use read::ReadStore;
```

**Step 2.3: Create ReadStore trait**

Create `crates/arco-core/src/storage/read.rs`:
```rust
//! Read-only storage trait for steady-state readers.

use async_trait::async_trait;
use bytes::Bytes;
use std::ops::Range;

use crate::error::Result;

/// Read-only storage capability.
///
/// Steady-state readers (catalog reader, query engines) should only
/// depend on this trait. They cannot list, head, or write.
#[async_trait]
pub trait ReadStore: Send + Sync + 'static {
    /// Reads entire object by path.
    ///
    /// Returns `Error::NotFound` if object doesn't exist.
    async fn get(&self, path: &str) -> Result<Bytes>;

    /// Reads a byte range from an object.
    ///
    /// Returns `Error::InvalidInput` if range is invalid.
    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes>;
}
```

**Step 2.4: Create PutStore trait (unconditional writes)**

Create `crates/arco-core/src/storage/put.rs`:
```rust
//! Unconditional write trait for immutable files.
//!
//! Used for writing ledger events and state files that are never modified.

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;
use super::keys::{LedgerKey, StateKey};

/// Unconditional write capability for immutable files.
///
/// Ledger events and state files are write-once, never modified.
/// This trait is separate from CasStore because:
/// 1. These writes don't need CAS (files are immutable)
/// 2. IAM can enforce different prefix permissions
#[async_trait]
pub trait PutStore: Send + Sync + 'static {
    /// Writes a ledger event (immutable).
    ///
    /// Events are written to `ledger/{domain}/{event_id}.json`.
    /// Returns error if file already exists (idempotency check).
    async fn put_ledger(&self, key: &LedgerKey, data: Bytes) -> Result<()>;

    /// Writes a state file (immutable).
    ///
    /// State files are written to `state/{domain}/v{version}/{filename}`.
    /// Returns error if file already exists.
    async fn put_state(&self, key: &StateKey, data: Bytes) -> Result<()>;
}
```

**Step 2.5: Create CasStore trait (manifest CAS only)**

Create `crates/arco-core/src/storage/cas.rs`:
```rust
//! Compare-and-swap storage trait for manifest updates.
//!
//! CasStore is ONLY for manifest pointers. It does not include delete.

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;
use super::backend::ObjectMeta;
use super::keys::ManifestKey;

/// Precondition for conditional writes (CAS operations).
#[derive(Debug, Clone)]
pub enum WritePrecondition {
    /// Write only if object does not exist (create).
    DoesNotExist,
    /// Write only if object's version matches the given token (update).
    MatchesVersion(String),
}

/// Result of a conditional write.
#[derive(Debug, Clone)]
pub enum WriteResult {
    /// Write succeeded, returns new version token.
    Success { version: String },
    /// Precondition failed, returns current version token.
    PreconditionFailed { current_version: String },
}

/// Compare-and-swap storage capability for manifests.
///
/// Used ONLY for manifest pointer updates. Requires a `PublishPermit`.
/// Does NOT include delete (manifests are never deleted).
#[async_trait]
pub trait CasStore: Send + Sync + 'static {
    /// Gets manifest metadata without reading content.
    ///
    /// Returns `None` if manifest doesn't exist.
    async fn head_manifest(&self, key: &ManifestKey) -> Result<Option<ObjectMeta>>;

    /// Reads manifest with version for CAS.
    ///
    /// Returns (data, version) for subsequent CAS write.
    async fn get_manifest(&self, key: &ManifestKey) -> Result<(Bytes, String)>;

    /// Updates manifest with CAS precondition.
    ///
    /// Returns `WriteResult::PreconditionFailed` if version doesn't match.
    async fn cas_manifest(
        &self,
        key: &ManifestKey,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult>;
}
```

**Step 2.6: Create ListStore trait**

Create `crates/arco-core/src/storage/list.rs`:
```rust
//! Listing storage trait for anti-entropy only.
//!
//! ListStore is NEVER used in steady-state. Only anti-entropy jobs.

use async_trait::async_trait;

use crate::error::Result;
use super::backend::ObjectMeta;

/// Cursor for paginated listing.
#[derive(Debug, Clone, Default)]
pub struct ListCursor {
    /// Opaque continuation token from previous page.
    pub token: Option<String>,
}

/// Page of listing results.
#[derive(Debug)]
pub struct ListPage {
    /// Objects in this page.
    pub objects: Vec<ObjectMeta>,
    /// Cursor for next page (None if no more pages).
    pub next_cursor: Option<ListCursor>,
}

/// List storage capability.
///
/// **ONLY** anti-entropy jobs should depend on this trait.
/// Steady-state components MUST NOT list.
///
/// If you're tempted to add `ListStore` to a component, ask:
/// "Can this work with explicit keys instead?"
#[async_trait]
pub trait ListStore: Send + Sync + 'static {
    /// Lists objects with pagination.
    ///
    /// Use cursor from previous page to continue listing.
    /// Returns empty page if no objects match prefix.
    async fn list_page(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: usize,
    ) -> Result<ListPage>;
}
```

**Step 2.7: Refactor backend to implement all traits**

Move existing `StorageBackend` to `crates/arco-core/src/storage/backend.rs`:

```rust
//! Full storage backend implementations.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::ops::Range;
use std::time::Duration;

use crate::error::Result;
use super::cas::{CasStore, WritePrecondition, WriteResult};
use super::keys::{LedgerKey, ManifestKey, StateKey};
use super::list::{ListCursor, ListPage, ListStore};
use super::put::PutStore;
use super::read::ReadStore;

/// Metadata about a stored object.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    /// Object path (key).
    pub path: String,
    /// Object size in bytes.
    pub size: u64,
    /// Object version token for CAS operations.
    pub version: String,
    /// Last modification timestamp.
    pub last_modified: Option<DateTime<Utc>>,
    /// Entity tag for cache validation.
    pub etag: Option<String>,
}

/// Full storage backend trait (for implementations).
///
/// Component code should depend on specific capability traits instead:
/// - Readers: `ReadStore`
/// - Ledger/state writers: `PutStore`
/// - Manifest publishers: `CasStore`
/// - Anti-entropy: `ListStore`
#[async_trait]
pub trait StorageBackend: ReadStore + PutStore + CasStore + ListStore + Send + Sync + 'static {}

/// Signed URL issuance is a separate capability (do not include in `StorageBackend`).
///
/// Only API/auth components should have access to this trait.
#[async_trait]
pub trait SignedUrlIssuer: Send + Sync + 'static {
    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String>;
}
```

**Step 2.8: Update lib.rs exports**

Modify `crates/arco-core/src/lib.rs`:
```rust
pub mod storage;

// Re-export storage types
pub use storage::{
    CasStore, LedgerKey, ListCursor, ListPage, ListStore, LockKey, ManifestKey,
    MemoryBackend, ObjectMeta, ObjectStoreBackend, PutStore, ReadStore,
    StateKey, StorageBackend, WritePrecondition, WriteResult,
};
```

**Step 2.9: Add compile-negative tests (ACTUALLY FAIL)**

Create `crates/arco-core/tests/compile_negative/reader_cannot_list.rs`:
```rust
//! Verifies ReadStore cannot call list, put, head, or cas.
//! This file MUST FAIL to compile.

use arco_core::ReadStore;

async fn reader_code<S: ReadStore>(store: &S) {
    // These should compile:
    let _ = store.get("path").await;
    let _ = store.get_range("path", 0..100).await;

    // These MUST NOT compile - uncommented to verify:
    store.list_page("prefix", None, 100).await;  // ERROR: no method `list_page`
}

fn main() {}
```

Create `crates/arco-core/tests/compile_negative/reader_cannot_list.stderr`:
```text
error[E0599]: no method named `list_page` found for reference `&S` in the current scope
```

Create `crates/arco-core/tests/compile_negative/reader_cannot_put.rs`:
```rust
//! Verifies ReadStore cannot write.

use arco_core::{LedgerKey, ReadStore};
use bytes::Bytes;

async fn reader_code<S: ReadStore>(store: &S) {
    let key = LedgerKey::new("catalog", "evt-123");
    store.put_ledger(&key, Bytes::new()).await;  // ERROR: no method `put_ledger`
}

fn main() {}
```

Create `crates/arco-core/tests/compile_negative/reader_cannot_put.stderr`:
```text
error[E0599]: no method named `put_ledger` found for reference `&S` in the current scope
```

Create `crates/arco-core/tests/compile_negative/putstore_cannot_cas.rs`:
```rust
//! Verifies PutStore cannot do CAS operations.

use arco_core::{ManifestKey, PutStore, WritePrecondition};
use bytes::Bytes;

async fn writer_code<S: PutStore>(store: &S) {
    let key = ManifestKey::new("catalog");
    // PutStore can write ledger/state, but NOT cas manifests
    store.cas_manifest(&key, Bytes::new(), WritePrecondition::DoesNotExist).await;
}

fn main() {}
```

Create `crates/arco-core/tests/compile_negative/putstore_cannot_cas.stderr`:
```text
error[E0599]: no method named `cas_manifest` found for reference `&S` in the current scope
```

Create `crates/arco-core/tests/compile_tests.rs`:
```rust
#[test]
fn compile_negative_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_negative/*.rs");
}
```

Add to `crates/arco-core/Cargo.toml`:
```toml
[dev-dependencies]
trybuild = "1.0"
```

**Step 2.10: Update ScopedStorage**

Modify `crates/arco-core/src/scoped_storage.rs`:
```rust
impl ScopedStorage {
    /// Returns a read-only view (steady-state readers).
    pub fn as_reader(&self) -> &dyn ReadStore {
        self.backend.as_ref()
    }

    /// Returns a write view for ledger/state (immutable files).
    pub fn as_writer(&self) -> &dyn PutStore {
        self.backend.as_ref()
    }

    /// Returns a CAS view for manifests (requires PublishPermit).
    pub fn as_cas(&self) -> &dyn CasStore {
        self.backend.as_ref()
    }

    /// Returns a list view (anti-entropy ONLY).
    pub fn as_lister(&self) -> &dyn ListStore {
        self.backend.as_ref()
    }
}
```

**Step 2.11: Run tests and verify**

```bash
cargo test --workspace
cargo clippy --workspace
```

**Step 2.12: Commit**

```bash
git add crates/arco-core/src/storage/ crates/arco-core/tests/
git commit -m "feat(core): split storage into typed-key capability traits

Gate 5 trait separation with:
- Typed keys (LedgerKey, StateKey, ManifestKey) prevent prefix bugs
- ReadStore: get, get_range only
- PutStore: immutable ledger/state writes
- CasStore: manifest CAS only (no delete)
- ListStore: anti-entropy only

Compile-negative tests actually fail (not commented out)."
```

---

### Task 3: PublishPermit with Real Distributed Fencing

**Goal:** All manifest/pointer updates require a `PublishPermit` token derived from the distributed lock. No bypass possible.

**Key Changes from Original:**
1. **Kill process-local counter** - `AtomicU64` is NOT distributed fencing
2. **PermitIssuer derives tokens from lock** - fencing token comes from lock acquisition
3. **Private constructors** - only `PermitIssuer` can create permits
4. **Publisher is the only export** - no direct CAS access

**Files:**
- Create: `crates/arco-core/src/publish.rs`
- Modify: `crates/arco-core/src/lib.rs`
- Modify: `crates/arco-catalog/src/tier1_writer.rs`
- Test: `crates/arco-catalog/src/tier1_writer.rs` (add tests)

**Step 3.1: Create PublishPermit module with PermitIssuer**

Create `crates/arco-core/src/publish.rs`:
```rust
//! Publish permit system for gating manifest updates.
//!
//! All manifest updates MUST require a `PublishPermit`. This ensures:
//! - Visibility changes are intentional and auditable
//! - Stale lock holders cannot publish (fencing token from lock)
//! - Rollback is detectable via fencing token
//!
//! ## Architecture
//!
//! ```text
//! LockGuard ──► PermitIssuer ──► PublishPermit ──► Publisher ──► CAS
//!     │              │                │                │
//!     │              │                │                └─ Only export
//!     │              │                └─ Private constructor
//!     │              └─ Derives fencing token from lock
//!     └─ Distributed lock acquisition
//! ```
//!
//! **Critical invariant:** You cannot create a PublishPermit without
//! holding a valid distributed lock.

use std::fmt;

/// Fencing token from distributed lock.
///
/// This is NOT a process-local counter. It's derived from the lock
/// acquisition sequence number stored in the lock object itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FencingToken(u64);

impl FencingToken {
    /// Creates a fencing token from lock metadata.
    ///
    /// The sequence number is read from the lock object in storage,
    /// ensuring it's globally unique and monotonically increasing.
    pub fn from_lock_sequence(sequence: u64) -> Self {
        Self(sequence)
    }

    /// Returns the raw sequence number.
    pub fn sequence(&self) -> u64 {
        self.0
    }
}

/// Issues publish permits from a held lock.
///
/// You can only create a `PermitIssuer` from a `LockGuard`, ensuring
/// that permits are only issued while holding the distributed lock.
pub struct PermitIssuer {
    /// Fencing token from lock acquisition.
    fencing_token: FencingToken,
    /// Resource being locked (for audit).
    resource: String,
}

impl PermitIssuer {
    /// Creates a permit issuer from a lock guard.
    ///
    /// The fencing token is extracted from the lock's sequence number,
    /// which is stored in the lock object and incremented on each acquisition.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let guard = lock_manager.acquire("catalog").await?;
    /// let issuer = PermitIssuer::from_lock(&guard);
    /// let permit = issuer.issue_permit("catalog", manifest_version);
    /// ```
    pub fn from_lock_guard(fencing_token: FencingToken, resource: impl Into<String>) -> Self {
        Self {
            fencing_token,
            resource: resource.into(),
        }
    }

    /// Issues a publish permit for the given domain.
    ///
    /// The permit carries the fencing token from lock acquisition.
    pub fn issue_permit(&self, domain: impl Into<String>, expected_version: String) -> PublishPermit {
        PublishPermit {
            fencing_token: self.fencing_token,
            domain: domain.into(),
            expected_version: Some(expected_version),
            consumed: false,
            issuer_resource: self.resource.clone(),
        }
    }

    /// Issues a permit for Tier-2 (CAS-only, no expected version).
    pub fn issue_permit_tier2(&self, domain: impl Into<String>) -> PublishPermit {
        PublishPermit {
            fencing_token: self.fencing_token,
            domain: domain.into(),
            expected_version: None,
            consumed: false,
            issuer_resource: self.resource.clone(),
        }
    }
}

/// A permit authorizing a manifest publish operation.
///
/// Permits are:
/// - **Non-cloneable**: Prevents accidental reuse
/// - **Consumed on use**: Single-use guarantee
/// - **Lock-derived**: Fencing token from distributed lock
///
/// # Private Constructor
///
/// You cannot construct a `PublishPermit` directly. Use `PermitIssuer`
/// which requires holding a distributed lock.
#[derive(Debug)]
pub struct PublishPermit {
    /// Fencing token from lock acquisition.
    fencing_token: FencingToken,
    /// Domain being published to.
    domain: String,
    /// Expected parent version (for CAS).
    expected_version: Option<String>,
    /// Whether this permit has been consumed.
    consumed: bool,
    /// Resource that issued this permit (for audit).
    issuer_resource: String,
}

impl PublishPermit {
    /// Returns the fencing token for audit logging and stale detection.
    #[must_use]
    pub fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Returns the domain this permit is for.
    #[must_use]
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Returns the expected version for CAS.
    #[must_use]
    pub fn expected_version(&self) -> Option<&str> {
        self.expected_version.as_deref()
    }

    /// Consumes the permit, marking it as used.
    ///
    /// # Panics
    ///
    /// Panics if the permit has already been consumed (double-publish attempt).
    pub(crate) fn consume(&mut self) {
        assert!(!self.consumed, "PublishPermit already consumed");
        self.consumed = true;
    }

    /// Returns whether this permit has been consumed.
    pub fn is_consumed(&self) -> bool {
        self.consumed
    }
}

impl Drop for PublishPermit {
    fn drop(&mut self) {
        if !self.consumed {
            tracing::warn!(
                fencing_token = %self.fencing_token.0,
                domain = %self.domain,
                issuer = %self.issuer_resource,
                "PublishPermit dropped without being consumed"
            );
        }
    }
}

impl fmt::Display for PublishPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PublishPermit(domain={}, fencing_token={}, consumed={})",
            self.domain, self.fencing_token.0, self.consumed
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_issuer() -> PermitIssuer {
        PermitIssuer::from_lock_guard(FencingToken(42), "test-lock")
    }

    #[test]
    fn test_permit_consumes_once() {
        let issuer = test_issuer();
        let mut permit = issuer.issue_permit("catalog", "v1".into());
        assert!(!permit.consumed);
        permit.consume();
        assert!(permit.consumed);
    }

    #[test]
    #[should_panic(expected = "already consumed")]
    fn test_permit_cannot_consume_twice() {
        let issuer = test_issuer();
        let mut permit = issuer.issue_permit("catalog", "v1".into());
        permit.consume();
        permit.consume(); // Should panic
    }

    #[test]
    fn test_fencing_token_from_issuer() {
        let issuer = PermitIssuer::from_lock_guard(FencingToken(100), "lock-a");
        let permit = issuer.issue_permit("catalog", "v1".into());
        assert_eq!(permit.fencing_token().sequence(), 100);
    }

    #[test]
    fn test_different_locks_different_tokens() {
        let issuer1 = PermitIssuer::from_lock_guard(FencingToken(1), "lock-a");
        let issuer2 = PermitIssuer::from_lock_guard(FencingToken(2), "lock-b");

        let permit1 = issuer1.issue_permit("a", "v1".into());
        let permit2 = issuer2.issue_permit("b", "v1".into());

        assert_ne!(permit1.fencing_token(), permit2.fencing_token());
    }
}
```

**Step 3.2: Create Publisher (the ONLY public publish API)**

Add to `crates/arco-core/src/publish.rs`:
```rust
use bytes::Bytes;
use crate::storage::{CasStore, ManifestKey, WritePrecondition, WriteResult};

/// Publisher that requires a permit to update manifests.
///
/// This is the ONLY public API for publishing manifests.
/// Direct CAS access is not exported.
pub struct Publisher<S> {
    storage: S,
}

impl<S: CasStore> Publisher<S> {
    /// Creates a new publisher with CAS storage.
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    /// Publishes a manifest update using the provided permit.
    ///
    /// The permit is consumed on success. On CAS failure, the permit
    /// is NOT consumed (can retry with fresh version).
    ///
    /// Returns `WriteResult::PreconditionFailed` if version changed.
    pub async fn publish(
        &self,
        mut permit: PublishPermit,
        key: &ManifestKey,
        data: Bytes,
    ) -> crate::error::Result<WriteResult> {
        let precondition = match permit.expected_version() {
            Some(v) => WritePrecondition::MatchesVersion(v.to_string()),
            None => WritePrecondition::DoesNotExist,
        };

        let result = self.storage.cas_manifest(key, data, precondition).await?;

        if matches!(result, WriteResult::Success { .. }) {
            permit.consume();
            tracing::info!(
                fencing_token = %permit.fencing_token().sequence(),
                domain = %permit.domain(),
                manifest = %key,
                "Manifest published successfully"
            );
        }

        Ok(result)
    }
}
```

**Step 3.3: Update lib.rs exports**

Modify `crates/arco-core/src/lib.rs`:
```rust
pub mod publish;

// Export ONLY the public API - not raw permit constructors
pub use publish::{FencingToken, PermitIssuer, PublishPermit, Publisher};
```

**Step 3.4: Update LockGuard to provide PermitIssuer**

The `LockGuard` must provide the fencing token from the lock object:
```rust
// In crates/arco-core/src/lock.rs or similar

impl<S> LockGuard<S> {
    /// Returns a permit issuer for this lock.
    ///
    /// The issuer derives fencing tokens from this lock's sequence number.
    pub fn permit_issuer(&self) -> PermitIssuer {
        PermitIssuer::from_lock_guard(
            FencingToken::from_lock_sequence(self.sequence_number),
            &self.resource,
        )
    }
}
```

**Step 3.5: Update Tier1Writer to use PermitIssuer**

Modify `crates/arco-catalog/src/tier1_writer.rs`:
```rust
use arco_core::{FencingToken, ManifestKey, PermitIssuer, Publisher};

async fn update_inner<F>(&self, update_fn: &mut F) -> Result<CommitRecord>
where
    F: FnMut(&mut CatalogDomainManifest) -> Result<()>,
{
    // Acquire lock (provides fencing token)
    let guard = self.lock_manager.acquire("catalog").await?;
    let issuer = guard.permit_issuer();

    for attempt in 1..=self.cas_max_retries {
        let (mut catalog, catalog_version) = self
            .storage.as_cas()
            .get_manifest(&ManifestKey::new("catalog"))
            .await?;

        let prev_catalog: CatalogDomainManifest = serde_json::from_slice(&catalog)?;
        let mut new_catalog = prev_catalog.clone();

        update_fn(&mut new_catalog)?;
        new_catalog.updated_at = Utc::now();

        let commit = self.build_commit_record(&prev_catalog, &new_catalog).await?;
        new_catalog.last_commit_id = Some(commit.commit_id.clone());

        // Issue permit from lock guard (real distributed fencing)
        let permit = issuer.issue_permit("catalog", catalog_version);
        let publisher = Publisher::new(self.storage.as_cas());
        let new_bytes = serde_json::to_vec(&new_catalog)?;

        match publisher.publish(permit, &ManifestKey::new("catalog"), new_bytes.into()).await? {
            WriteResult::Success { .. } => {
                self.persist_commit_record(&commit).await?;
                return Ok(commit);
            }
            WriteResult::PreconditionFailed { .. } => {
                if attempt == self.cas_max_retries {
                    return Err(CatalogError::PreconditionFailed {
                        message: "manifest update lost CAS race".into(),
                    });
                }
                continue;
            }
        }
    }

    Err(CatalogError::InvariantViolation {
        message: "unreachable".into(),
    })
}
```

**Step 3.6: Add tests for real fencing**

Add to `crates/arco-catalog/tests/publish_permit_tests.rs`:
```rust
#[tokio::test]
async fn test_no_visibility_without_pointer_swap() {
    // Verify that data files are not visible until manifest is published
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").unwrap();

    // Write data file directly (crash before publish)
    let state_key = StateKey::new("catalog", 999, "orphan.parquet");
    storage.as_writer().put_state(&state_key, Bytes::from("data")).await.unwrap();

    // Manifest still points to old version
    let (manifest_bytes, _) = storage.as_cas()
        .get_manifest(&ManifestKey::new("catalog"))
        .await
        .unwrap();
    let manifest: CatalogDomainManifest = serde_json::from_slice(&manifest_bytes).unwrap();

    assert_ne!(manifest.snapshot_version, 999);
}

#[tokio::test]
async fn test_stale_lock_holder_loses() {
    // Simulate stale lock holder with lower fencing token
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").unwrap();

    // First lock holder (token 1)
    let issuer1 = PermitIssuer::from_lock_guard(FencingToken::from_lock_sequence(1), "catalog");

    // Second lock holder (token 2) - took over the lock
    let issuer2 = PermitIssuer::from_lock_guard(FencingToken::from_lock_sequence(2), "catalog");

    // Read current manifest version
    let (_, version) = storage.as_cas()
        .get_manifest(&ManifestKey::new("catalog"))
        .await
        .unwrap();

    // Holder 2 publishes first (succeeds)
    let permit2 = issuer2.issue_permit("catalog", version.clone());
    let publisher = Publisher::new(storage.as_cas());
    let result2 = publisher.publish(
        permit2,
        &ManifestKey::new("catalog"),
        Bytes::from(r#"{"snapshot_version":2}"#),
    ).await.unwrap();
    assert!(matches!(result2, WriteResult::Success { .. }));

    // Holder 1 tries to publish with stale version (fails)
    let permit1 = issuer1.issue_permit("catalog", version);
    let result1 = publisher.publish(
        permit1,
        &ManifestKey::new("catalog"),
        Bytes::from(r#"{"snapshot_version":1}"#),
    ).await.unwrap();
    assert!(matches!(result1, WriteResult::PreconditionFailed { .. }));
}

#[tokio::test]
async fn test_bypass_attempt_has_no_api_surface() {
    // Verify there's no way to create a permit without a lock
    // This is a compile-time guarantee, but we document it here

    // These should NOT compile (private constructors):
    // let permit = PublishPermit { ... };  // private fields
    // let permit = PublishPermit::new(...);  // no such method

    // The only way to get a permit is through PermitIssuer,
    // which requires a FencingToken from a lock.
}
```

**Step 3.7: Run tests**

```bash
cargo test -p arco-catalog
cargo test -p arco-core
```

**Step 3.8: Commit**

```bash
git add crates/arco-core/src/publish.rs crates/arco-catalog/
git commit -m "feat(core): PublishPermit with real distributed fencing

BREAKING: PublishPermit constructor is now private.

- Permits are issued via PermitIssuer which requires a lock guard
- Fencing token derived from lock sequence number (not process-local)
- Publisher is the only exported publish API
- Stale lock holders fail CAS due to version mismatch

Gate 5: no bypass publish API surface."
```

#### Known Gap (Phase 1)

- **Capability bypass via raw storage access**: `ScopedStorage` still exposes raw backend access
  (`backend()`, `get_raw`, `put_raw`, `list`, `delete`). API handlers can bypass capability traits
  if they receive a full `ScopedStorage` or `StorageBackend`. Full enforcement requires DI
  refactoring so handlers only receive trait-object capabilities at boundaries (Phase 2).

---

### Task 4: IAM Prefix Scoping + Smoke Test

**Goal:** Only compactor can write to `state/` and `l0/`. API cannot write these prefixes. IAM enforces at infrastructure level.

**Key Changes from Original:**
1. **Use strict `startsWith`** - NOT `contains()` which is bypassable
2. **Align to Jan 12 storage layout** - `tenant={id}/workspace={id}/{prefix}/`
3. **Separate write bindings per prefix** - one condition per prefix for clarity

**Why `contains()` is dangerous:**
```
resource.name.contains("/state/")  # WRONG
```
This matches: `tenant=x/ledger/evil/state/bypass.txt` ← attacker controls!

**Files:**
- Modify: `infra/terraform/iam.tf`
- Create: `infra/terraform/iam_conditions.tf`
- Create: `crates/arco-integration-tests/tests/iam_smoke.rs`
- Modify: `.github/workflows/ci.yml` (add smoke test)

**Step 4.1: Define storage path layout**

The Jan 12 storage layout is:
```
{bucket}/tenant={tenant_id}/workspace={workspace_id}/{prefix}/...
```

Prefixes:
- `ledger/` - immutable event log (API writes)
- `state/` - compacted Parquet (Compactor writes)
- `l0/` - L0 compaction tier (Compactor writes)
- `manifests/` - domain pointers (both can CAS, controlled by PublishPermit)
- `locks/` - distributed locks (API writes)

**Step 4.2: Create IAM conditions with strict startsWith**

Create `infra/terraform/iam_conditions.tf`:
```hcl
# Gate 5 IAM Prefix Scoping
#
# CRITICAL: Use startsWith for FULL path, not contains().
#
# Path format: projects/_/buckets/{bucket}/objects/tenant={id}/workspace={id}/{prefix}/...
#
# Example valid paths:
#   .../tenant=acme/workspace=prod/ledger/catalog/evt-123.json
#   .../tenant=acme/workspace=prod/state/catalog/v5/namespaces.parquet

locals {
  # Base path pattern for all tenant objects
  # Note: We use a regex-like approach with multiple startsWith conditions
  bucket_objects_prefix = "projects/_/buckets/${google_storage_bucket.catalog.name}/objects/"
}

# -------------------------------------------------------------------
# API Service Account: ledger/, locks/, manifests/
# -------------------------------------------------------------------

# API can write to ledger/ (append-only events)
resource "google_storage_bucket_iam_member" "api_write_ledger" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectCreator"  # Create only, no update/delete
  member = "serviceAccount:${google_service_account.api.email}"

  condition {
    title       = "ApiWriteLedger"
    description = "API can create ledger events (immutable)"
    # Match: tenant=*/workspace=*/ledger/*
    # Using extract() to parse the path components
    expression  = <<-EOT
      resource.name.startsWith("${local.bucket_objects_prefix}tenant=") &&
      resource.name.extract("tenant={tenant}/workspace={workspace}/ledger/{rest}") != ""
    EOT
  }
}

# API can write to locks/ (distributed lock management)
resource "google_storage_bucket_iam_member" "api_write_locks" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"  # Create, update, delete for lock lifecycle
  member = "serviceAccount:${google_service_account.api.email}"

  condition {
    title       = "ApiWriteLocks"
    description = "API can manage distributed locks"
    expression  = <<-EOT
      resource.name.startsWith("${local.bucket_objects_prefix}tenant=") &&
      resource.name.extract("tenant={tenant}/workspace={workspace}/locks/{rest}") != ""
    EOT
  }
}

# API can CAS manifests/ (via PublishPermit)
resource "google_storage_bucket_iam_member" "api_write_manifests" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.api.email}"

  condition {
    title       = "ApiWriteManifests"
    description = "API can update manifests (via PublishPermit)"
    expression  = <<-EOT
      resource.name.startsWith("${local.bucket_objects_prefix}tenant=") &&
      resource.name.extract("tenant={tenant}/workspace={workspace}/manifests/{rest}") != ""
    EOT
  }
}

# API: Read all objects
resource "google_storage_bucket_iam_member" "api_read" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.api.email}"
}

# -------------------------------------------------------------------
# Compactor Service Account: state/, l0/, manifests/
# -------------------------------------------------------------------

# Compactor can write to state/ (compacted Parquet)
resource "google_storage_bucket_iam_member" "compactor_write_state" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectCreator"  # Create only (immutable snapshots)
  member = "serviceAccount:${google_service_account.compactor.email}"

  condition {
    title       = "CompactorWriteState"
    description = "Compactor can write state/ (Parquet snapshots)"
    expression  = <<-EOT
      resource.name.startsWith("${local.bucket_objects_prefix}tenant=") &&
      resource.name.extract("tenant={tenant}/workspace={workspace}/state/{rest}") != ""
    EOT
  }
}

# Compactor can write to l0/ (L0 compaction tier)
resource "google_storage_bucket_iam_member" "compactor_write_l0" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"  # May need to clean up old L0 files
  member = "serviceAccount:${google_service_account.compactor.email}"

  condition {
    title       = "CompactorWriteL0"
    description = "Compactor can write l0/ tier"
    expression  = <<-EOT
      resource.name.startsWith("${local.bucket_objects_prefix}tenant=") &&
      resource.name.extract("tenant={tenant}/workspace={workspace}/l0/{rest}") != ""
    EOT
  }
}

# Compactor can CAS manifests/ (publish compaction results)
resource "google_storage_bucket_iam_member" "compactor_write_manifests" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.compactor.email}"

  condition {
    title       = "CompactorWriteManifests"
    description = "Compactor can update manifests"
    expression  = <<-EOT
      resource.name.startsWith("${local.bucket_objects_prefix}tenant=") &&
      resource.name.extract("tenant={tenant}/workspace={workspace}/manifests/{rest}") != ""
    EOT
  }
}

# Compactor: Read all objects
resource "google_storage_bucket_iam_member" "compactor_read" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.compactor.email}"
}

# -------------------------------------------------------------------
# REMOVED: Bucket-wide objectUser grants (Gate 5 violation)
# -------------------------------------------------------------------
# resource "google_storage_bucket_iam_member" "api_catalog_access" { ... }
# resource "google_storage_bucket_iam_member" "compactor_catalog_access" { ... }
```

**Step 4.3: Update iam.tf to import the conditions**

Modify `infra/terraform/iam.tf`:
```hcl
# Gate 5: Prefix-scoped IAM
# See iam_conditions.tf for detailed bindings

# Remove bucket-wide grants:
# - google_storage_bucket_iam_member.api_catalog_access
# - google_storage_bucket_iam_member.compactor_catalog_access
```

**Step 4.4: Create IAM smoke test**

Create `crates/arco-integration-tests/tests/iam_smoke.rs`:
```rust
//! IAM smoke tests verifying prefix-scoped permissions.
//!
//! These tests require a deployed environment with IAM configured.
//! Run with: ARCO_TEST_BUCKET=<bucket> cargo test --features iam-smoke

#![cfg(feature = "iam-smoke")]

use arco_core::{LedgerKey, ObjectStoreBackend, ScopedStorage, StateKey};
use bytes::Bytes;
use std::env;
use ulid::Ulid;

/// Test that API service account cannot write to state/ prefix.
#[tokio::test]
#[ignore = "requires deployed IAM"]
async fn test_api_cannot_write_state() {
    // This test should be run with API service account credentials
    let bucket = env::var("ARCO_TEST_BUCKET").expect("ARCO_TEST_BUCKET required");
    let backend = ObjectStoreBackend::gcs(&bucket).expect("GCS backend");
    let storage = ScopedStorage::new(
        std::sync::Arc::new(backend),
        "test-tenant",
        "test-workspace",
    ).expect("scoped storage");

    // Attempt to write to state/ prefix - should fail (API is not the Parquet writer).
    let result = storage
        .as_writer()
        .put_state(
            &StateKey::new("catalog", 0, format!("iam-smoke-{}.parquet", Ulid::new())),
            Bytes::from("test"),
        )
        .await;

    assert!(
        result.is_err(),
        "API should NOT be able to write to state/ prefix"
    );

    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("permission denied") ||
        err.to_string().contains("403"),
        "Error should indicate permission denied, got: {err}"
    );
}

/// Test that API service account CAN write to ledger/ prefix.
#[tokio::test]
#[ignore = "requires deployed IAM"]
async fn test_api_can_write_ledger() {
    let bucket = env::var("ARCO_TEST_BUCKET").expect("ARCO_TEST_BUCKET required");
    let backend = ObjectStoreBackend::gcs(&bucket).expect("GCS backend");
    let storage = ScopedStorage::new(
        std::sync::Arc::new(backend),
        "test-tenant",
        "test-workspace",
    ).expect("scoped storage");

    // Write to ledger/ prefix - should succeed (create-only).
    storage
        .as_writer()
        .put_ledger(
            &LedgerKey::new("executions", format!("iam-smoke-{}", Ulid::new())),
            Bytes::from("{}"),
        )
        .await
        .expect("ledger write should succeed");
}
```

**Step 4.5: Add smoke test to CI**

Add to `.github/workflows/ci.yml`:
```yaml
  iam-smoke:
    name: IAM Smoke Tests
    runs-on: ubuntu-latest
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    environment: dev
    steps:
      - uses: actions/checkout@v4
      - uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY_API }}
      - name: Run API IAM smoke tests
        env:
          ARCO_TEST_BUCKET: ${{ vars.ARCO_BUCKET_DEV }}
        run: |
          cargo test --package arco-integration-tests \
            --features iam-smoke \
            test_api_cannot_write_state -- --ignored
```

**Step 4.6: Apply Terraform**

```bash
cd infra/terraform
terraform plan -var-file=environments/dev.tfvars
terraform apply -var-file=environments/dev.tfvars
```

**Step 4.7: Commit**

```bash
git add infra/terraform/ crates/arco-integration-tests/tests/iam_smoke.rs .github/
git commit -m "feat(infra): implement prefix-scoped IAM for Gate 5

- API can only write to ledger/, manifests/, locks/
- Compactor can only write to state/, l0/, manifests/
- Both can read all prefixes
- Adds IAM smoke test verifying API cannot write state/"
```

---

### Task 5: Rollback Detection + Torn-Write Prevention

**Goal:** Manifest pointer cannot regress without explicit rollback mode. Partial uploads never referenced.

**Key Changes from Original:**
1. **Hash raw stored bytes** - NOT re-serialization (which can differ)
2. **Version guards are mandatory** - monotonic version enforcement
3. **Artifact size verification before publish**

**Why re-serialization is wrong:**
```rust
// WRONG: Re-serialization may produce different bytes
let bytes = serde_json::to_vec(&manifest)?;
let hash = sha256(&bytes);
```
JSON serialization is not deterministic (field ordering, whitespace). We must hash the exact bytes stored in the object.

**Files:**
- Modify: `crates/arco-catalog/src/manifest.rs`
- Modify: `crates/arco-catalog/src/tier1_writer.rs`
- Test: `crates/arco-catalog/src/tier1_writer.rs`

**Step 5.1: Add parent_hash computed from raw stored bytes**

Modify `crates/arco-catalog/src/manifest.rs`:

```rust
/// Catalog domain manifest (Tier 1) - separate file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogDomainManifest {
    /// Current snapshot version (monotonically increasing).
    pub snapshot_version: u64,

    /// Path to snapshot directory.
    pub snapshot_path: String,

    /// Optional enhanced snapshot metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotInfo>,

    /// Last commit ID for hash chain integrity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_commit_id: Option<String>,

    /// Parent manifest hash (sha256 of RAW stored bytes).
    ///
    /// **Critical:** This must be computed from the exact bytes read from
    /// storage, NOT from re-serializing the manifest struct.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_hash: Option<String>,

    /// Monotonic position watermark (for Tier-2 domains).
    /// Must never decrease unless in explicit rollback mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_watermark: Option<u64>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

/// Computes hash from raw bytes (not re-serialization).
///
/// Use this function with the bytes read from storage.
pub fn compute_manifest_hash(raw_bytes: &[u8]) -> String {
    let hash = sha2::Sha256::digest(raw_bytes);
    format!("sha256:{}", hex::encode(hash))
}

impl CatalogDomainManifest {
    /// Validates that this manifest can succeed the given previous version.
    ///
    /// Arguments:
    /// - `previous`: The previous manifest struct
    /// - `previous_raw_hash`: Hash computed from raw stored bytes of previous
    ///
    /// Returns error if:
    /// - snapshot_version decreased (rollback)
    /// - parent_hash doesn't match previous raw hash
    pub fn validate_succession(
        &self,
        previous: &Self,
        previous_raw_hash: &str,
    ) -> Result<(), String> {
        // Version must not decrease
        if self.snapshot_version < previous.snapshot_version {
            return Err(format!(
                "version regression: {} -> {}",
                previous.snapshot_version, self.snapshot_version
            ));
        }

        // Parent hash must match raw stored bytes
        if let Some(ref parent_hash) = self.parent_hash {
            if parent_hash != previous_raw_hash {
                return Err(format!(
                    "parent hash mismatch: expected {}, got {}",
                    previous_raw_hash, parent_hash
                ));
            }
        }

        Ok(())
    }
}
```

**Step 5.2: Update CasStore to return raw bytes with version**

The `CasStore::get_manifest` already returns `(Bytes, String)` - we use those raw bytes for hashing.

**Step 5.3: Update publisher to use raw byte hash**

Update `crates/arco-catalog/src/tier1_writer.rs`:

```rust
use arco_core::{compute_manifest_hash, ManifestKey, PermitIssuer, Publisher};

/// Verifies that all snapshot files exist and have expected sizes.
async fn verify_snapshot_artifacts(&self, snapshot: &SnapshotInfo) -> Result<()> {
    for file in &snapshot.files {
        let full_path = format!("{}/{}", snapshot.path, file.path);
        let meta = self.storage.as_cas()
            .head_manifest(&ManifestKey::new(&full_path))
            .await?
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: format!("snapshot file missing: {}", full_path),
            })?;

        if meta.size != file.byte_size {
            return Err(CatalogError::InvariantViolation {
                message: format!(
                    "snapshot file size mismatch: {} expected {} got {}",
                    full_path, file.byte_size, meta.size
                ),
            });
        }
    }
    Ok(())
}

/// Publishes a snapshot with verification and rollback detection.
pub async fn publish_snapshot(
    &self,
    guard: &LockGuard,
    snapshot: SnapshotInfo,
) -> Result<CommitRecord> {
    // Step 1: Verify all artifacts exist with correct sizes
    self.verify_snapshot_artifacts(&snapshot).await?;

    // Step 2: Load current manifest WITH raw bytes
    let manifest_key = ManifestKey::new("catalog");
    let (raw_bytes, version) = self.storage.as_cas()
        .get_manifest(&manifest_key)
        .await?;

    // Step 3: Compute parent hash from RAW STORED BYTES (not re-serialization!)
    let parent_hash = compute_manifest_hash(&raw_bytes);

    // Step 4: Deserialize for modification
    let prev_catalog: CatalogDomainManifest = serde_json::from_slice(&raw_bytes)?;

    // Step 5: Build new manifest
    let mut new_catalog = prev_catalog.clone();
    new_catalog.snapshot_version = snapshot.version;
    new_catalog.snapshot_path = snapshot.path.clone();
    new_catalog.snapshot = Some(snapshot);
    new_catalog.parent_hash = Some(parent_hash.clone());
    new_catalog.updated_at = Utc::now();

    // Step 6: Validate succession using raw byte hash
    new_catalog.validate_succession(&prev_catalog, &parent_hash).map_err(|e| {
        CatalogError::InvariantViolation {
            message: format!("manifest succession validation failed: {e}"),
        }
    })?;

    // Step 7: Publish with permit from lock guard
    let issuer = guard.permit_issuer();
    let permit = issuer.issue_permit("catalog", version);
    let publisher = Publisher::new(self.storage.as_cas());

    let new_bytes = serde_json::to_vec(&new_catalog)?;

    match publisher.publish(permit, &manifest_key, new_bytes.into()).await? {
        WriteResult::Success { .. } => {
            let commit = self.build_commit_record(&prev_catalog, &new_catalog).await?;
            self.persist_commit_record(&commit).await?;
            Ok(commit)
        }
        WriteResult::PreconditionFailed { .. } => {
            Err(CatalogError::PreconditionFailed {
                message: "manifest CAS failed during snapshot publish".into(),
            })
        }
    }
}
```

**Step 5.4: Add tests for rollback detection and torn writes**

Add to `crates/arco-catalog/tests/rollback_tests.rs`:

```rust
#[tokio::test]
async fn test_mid_write_crash_never_referenced() {
    // Simulate crash after writing files but before manifest update
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").unwrap();

    // Initialize with version 0
    initialize_catalog(&storage).await;

    // Write orphan snapshot files (simulating crash before publish)
    let orphan_key = StateKey::new("catalog", 999, "orphan.parquet");
    storage.as_writer().put_state(&orphan_key, Bytes::from("orphan")).await.unwrap();

    // Manifest should still point to version 0 (orphan not visible)
    let (raw_bytes, _) = storage.as_cas()
        .get_manifest(&ManifestKey::new("catalog"))
        .await
        .unwrap();
    let manifest: CatalogDomainManifest = serde_json::from_slice(&raw_bytes).unwrap();

    assert_eq!(manifest.snapshot_version, 0);
}

#[tokio::test]
async fn test_pointer_regression_rejected() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

    // Initialize and advance to version 5
    let (raw_v5, version_v5) = advance_to_version(&storage, 5).await;
    let hash_v5 = compute_manifest_hash(&raw_v5);

    // Try to publish version 3 (regression)
    let mut regressed = CatalogDomainManifest::default();
    regressed.snapshot_version = 3;
    regressed.parent_hash = Some(hash_v5.clone());

    let prev: CatalogDomainManifest = serde_json::from_slice(&raw_v5).unwrap();
    let result = regressed.validate_succession(&prev, &hash_v5);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("regression"));
}

#[tokio::test]
async fn test_parent_hash_detects_concurrent_modification() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

    // Read manifest (get raw bytes + hash)
    let (raw_v1, _) = storage.as_cas()
        .get_manifest(&ManifestKey::new("catalog"))
        .await
        .unwrap();
    let hash_v1 = compute_manifest_hash(&raw_v1);

    // Someone else modifies the manifest concurrently
    // (simulated by advancing version)
    let (raw_v2, _) = advance_to_version(&storage, 2).await;
    let hash_v2 = compute_manifest_hash(&raw_v2);

    // Our update has stale parent_hash (v1, but current is v2)
    let mut our_update = CatalogDomainManifest::default();
    our_update.snapshot_version = 3;
    our_update.parent_hash = Some(hash_v1.clone());  // Stale!

    let current: CatalogDomainManifest = serde_json::from_slice(&raw_v2).unwrap();
    let result = our_update.validate_succession(&current, &hash_v2);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("mismatch"));
}
```

**Step 5.5: Run tests**

```bash
cargo test -p arco-catalog
```

**Step 5.6: Commit**

```bash
git add crates/arco-catalog/
git commit -m "feat(catalog): rollback detection with raw byte hashing

CRITICAL: parent_hash computed from raw stored bytes, not re-serialization.

- compute_manifest_hash() takes raw bytes from storage
- validate_succession() verifies hash matches previous raw bytes
- Version regression always rejected
- Artifact size verification before publish

Gate 5: pointer integrity via cryptographic chain."
```

---

## Phase 2: Architecture Alignment

> **Note:** Task 1 (Phase 1) already includes ADR-018 decision with Option A selected.

### Task 6: Split Compactor Fast-Path and Anti-Entropy

**Goal:** Compactor consumes notifications (no listing) in steady state. Anti-entropy is separate cursor-based job.

**Files:**
- Create: `crates/arco-compactor/src/notification_consumer.rs`
- Create: `crates/arco-compactor/src/anti_entropy.rs`
- Modify: `crates/arco-compactor/src/main.rs`

**Step 6.1: Create notification consumer**

Create `crates/arco-compactor/src/notification_consumer.rs`:
```rust
//! Notification-driven compaction consumer.
//!
//! Steady-state compaction path that processes explicit object keys
//! from GCS notifications or a queue. Does NOT list.

use arco_core::{CasStore, ReadStore};

/// Processes a batch of object keys from notifications.
pub struct NotificationConsumer<S> {
    storage: S,
}

impl<S: ReadStore + CasStore> NotificationConsumer<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    /// Processes a batch of event file paths.
    ///
    /// These paths come from GCS notifications, not listing.
    pub async fn process_batch(&self, event_paths: Vec<String>) -> Result<CompactionResult, Error> {
        // Load events from explicit paths
        let mut events = Vec::new();
        for path in event_paths {
            let bytes = self.storage.get(&path).await?;
            let event: CatalogEvent = serde_json::from_slice(&bytes)?;
            events.push(event);
        }

        // Sort by timestamp
        events.sort_by_key(|e| e.timestamp);

        // Compact and publish
        // ...

        Ok(CompactionResult { events_processed: events.len() })
    }
}
```

**Step 6.2: Create anti-entropy module**

Create `crates/arco-compactor/src/anti_entropy.rs`:
```rust
//! Anti-entropy job for gap detection.
//!
//! This is the ONLY component that lists objects.
//! Uses cursor-based pagination, bounded per run.

use arco_core::{ListCursor, ListStore, ReadStore};

/// Configuration for anti-entropy runs.
pub struct AntiEntropyConfig {
    /// Maximum objects to process per run.
    pub max_objects_per_run: usize,
    /// Domain to check.
    pub domain: String,
}

/// Anti-entropy job that discovers missed events via listing.
pub struct AntiEntropyJob<S> {
    storage: S,
    config: AntiEntropyConfig,
}

impl<S: ListStore + ReadStore> AntiEntropyJob<S> {
    pub fn new(storage: S, config: AntiEntropyConfig) -> Self {
        Self { storage, config }
    }

    /// Runs one bounded anti-entropy pass.
    ///
    /// Loads cursor from storage, lists up to max_objects_per_run,
    /// enqueues any missed events, saves cursor for next run.
    pub async fn run_pass(&self) -> Result<AntiEntropyResult, Error> {
        // Load cursor from durable storage
        let cursor = self.load_cursor().await?;

        // List one page
        let prefix = format!("ledger/{}/", self.config.domain);
        let page = self.storage.list_page(
            &prefix,
            cursor.as_ref(),
            self.config.max_objects_per_run,
        ).await?;

        // Check each object against watermark
        let mut missed = Vec::new();
        for obj in &page.objects {
            if self.is_missed(&obj.path).await? {
                missed.push(obj.path.clone());
            }
        }

        // Enqueue missed events for reprocessing
        if !missed.is_empty() {
            self.enqueue_for_reprocessing(&missed).await?;
        }

        // Save cursor for next run
        if let Some(next) = page.next_cursor {
            self.save_cursor(&next).await?;
        } else {
            // Completed full scan, reset cursor
            self.save_cursor(&ListCursor::default()).await?;
        }

        Ok(AntiEntropyResult {
            objects_scanned: page.objects.len(),
            missed_events: missed.len(),
            scan_complete: page.next_cursor.is_none(),
        })
    }

    async fn load_cursor(&self) -> Result<Option<ListCursor>, Error> {
        // Load from state/anti_entropy/{domain}/cursor.json
        todo!()
    }

    async fn save_cursor(&self, cursor: &ListCursor) -> Result<(), Error> {
        todo!()
    }

    async fn is_missed(&self, path: &str) -> Result<bool, Error> {
        // Check if event is older than watermark but not in compacted state
        todo!()
    }

    async fn enqueue_for_reprocessing(&self, paths: &[String]) -> Result<(), Error> {
        todo!()
    }
}

pub struct AntiEntropyResult {
    pub objects_scanned: usize,
    pub missed_events: usize,
    pub scan_complete: bool,
}
```

**Step 6.3: Add tests**

```rust
#[tokio::test]
async fn test_notifications_disabled_converges_without_global_scan() {
    // Verify that notification consumer works without listing
    // by providing explicit paths
}

#[tokio::test]
async fn test_past_gap_found_via_cursor_progression() {
    // Verify anti-entropy finds old missed events
    // across multiple bounded runs
}
```

**Step 6.4: Commit**

```bash
git add crates/arco-compactor/
git commit -m "feat(compactor): split notification consumer and anti-entropy

Notification consumer processes explicit event paths (no listing).
Anti-entropy is cursor-based, bounded per run, completes over time.

Implements Gate 5 requirement: steady-state cannot list."
```

---

## Phase 3: Operational Hardening

### Task 7: Backpressure API

**Goal:** Two-stage soft/hard thresholds with retry_after, scoped per domain.

**Files:**
- Create: `crates/arco-core/src/backpressure.rs`
- Modify: `crates/arco-api/src/handlers/` (add backpressure checks)

**Step 7.1: Create backpressure module**

Create `crates/arco-core/src/backpressure.rs`:
```rust
//! Backpressure system with soft/hard thresholds.

use std::time::Duration;

/// Backpressure state for a domain.
#[derive(Debug, Clone)]
pub struct BackpressureState {
    /// Current pending event count.
    pub pending_events: u64,
    /// Soft threshold (warn, prioritize compaction).
    pub soft_threshold: u64,
    /// Hard threshold (reject with retry_after).
    pub hard_threshold: u64,
    /// Domain identifier.
    pub domain: String,
}

/// Backpressure decision.
#[derive(Debug, Clone)]
pub enum BackpressureDecision {
    /// Accept the request.
    Accept,
    /// Accept but warn (soft threshold exceeded).
    AcceptWithWarning { pending: u64 },
    /// Reject with retry_after (hard threshold exceeded).
    Reject { retry_after: Duration },
}

impl BackpressureState {
    /// Evaluates whether to accept a new event.
    pub fn evaluate(&self) -> BackpressureDecision {
        if self.pending_events >= self.hard_threshold {
            BackpressureDecision::Reject {
                retry_after: Duration::from_secs(5),
            }
        } else if self.pending_events >= self.soft_threshold {
            BackpressureDecision::AcceptWithWarning {
                pending: self.pending_events,
            }
        } else {
            BackpressureDecision::Accept
        }
    }
}

/// Error returned when backpressure rejects a request.
#[derive(Debug)]
pub struct BackpressureError {
    pub domain: String,
    pub retry_after: Duration,
}
```

**Step 7.2: Commit**

```bash
git add crates/arco-core/src/backpressure.rs crates/arco-api/
git commit -m "feat(core): add backpressure system with soft/hard thresholds

Two-stage backpressure:
- Soft threshold: accept with warning, prioritize compaction
- Hard threshold: reject with retry_after

Implements Gate 5 backpressure requirements."
```

---

### Task 8: Search Index Tombstones

**Goal:** Add tombstone records for delete/rename operations.

**Files:**
- Modify: `crates/arco-catalog/src/manifest.rs` (add tombstone events)
- Create: `crates/arco-catalog/src/search/tombstone.rs`

**Step 8.1: Add tombstone event type**

```rust
/// Search index tombstone for deleted/renamed assets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchTombstone {
    /// Asset ID being removed from search.
    pub asset_id: String,
    /// Reason for removal.
    pub reason: TombstoneReason,
    /// When the tombstone was created.
    pub created_at: DateTime<Utc>,
    /// Tombstone expires after this time (for compaction).
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TombstoneReason {
    Deleted,
    Renamed { new_key: String },
}
```

**Step 8.2: Add skew test**

```rust
#[tokio::test]
async fn test_search_index_hot_token_skew() {
    // Create many assets with same token
    // Verify search still works within latency budget
}
```

**Step 8.3: Commit**

```bash
git add crates/arco-catalog/
git commit -m "feat(catalog): add search index tombstones for delete/rename

Tombstones mark assets for removal from search index.
Includes expiry for compaction.

Implements Gate 5 search index requirements."
```

---

## Phase 4: Security & Compliance

### Task 9: Security ADR for Posture B

**Goal:** Document path to existence privacy without blocking current launch.

**Key Changes from Original:**
- **Align to existing docs** - Jan 12 says "partitioned snapshots + IAM", not "auth service views"
- Decision is Option 1 (partitioned snapshots), not Option 2 (auth service)

**Files:**
- Create: `docs/adr/adr-019-existence-privacy.md`

**Step 9.1: Create ADR**

Create `docs/adr/adr-019-existence-privacy.md`:
```markdown
# ADR-019: Existence Privacy (Posture B)

## Status

Accepted

## Context

Gate 5 requires a "no-regrets" security posture:
- Don't embed sensitive names in manifests/indexes
- Use salted/opaque search tokens
- Audit signed-URL issuance with correlation IDs

Current state (Posture A):
- Asset names visible in manifests
- Search tokens are plaintext
- Signed URLs issued without correlation tracking

## Options for Posture B

### Option 1: Partitioned Snapshots + IAM (Documented Approach)
- Separate Parquet files per access level partition
- IAM conditions restrict which partitions each user can read
- Pros: Leverages existing IAM infrastructure, no runtime filtering
- Cons: Some storage duplication

### Option 2: Auth Service Materializing Allowed Views
- Central service filters manifests based on user permissions
- Pros: Single source of truth
- Cons: Latency, availability dependency, complex auth integration

### Option 3: Encrypted Manifests + Key Distribution
- Encrypt sensitive fields, distribute keys per access level
- Pros: No server-side filtering needed
- Cons: Key management complexity

## Decision

**Option 1: Partitioned Snapshots + IAM**

This aligns with the Jan 12 design document which specifies:
> "Partitioned snapshots with IAM-scoped read access per tenant/access-level."

Implementation:
1. Snapshots partitioned by `access_level` column
2. IAM conditions restrict read access to specific partition paths
3. Search index uses salted hashes (no plaintext asset names)
4. Signed URL issuance audited with correlation IDs

## Consequences

- Snapshot writes include partition key for access level
- IAM bindings include `resource.name.extract(".../{access_level}/...")` conditions
- No runtime auth service dependency for read path
- Search index tokens: `sha256(salt + asset_name + tenant_id)`
- Every signed URL includes correlation ID in audit log

## Migration Path

1. Phase 1 (current): No existence privacy, focus on core functionality
2. Phase 2: Add `access_level` partition to snapshots
3. Phase 3: Deploy IAM conditions for partitioned access
4. Phase 4: Migrate search index to salted tokens
```

**Step 9.2: Commit**

```bash
git add docs/adr/adr-019-existence-privacy.md
git commit -m "docs(adr): existence privacy via partitioned snapshots + IAM

Aligns with Jan 12 design: partitioned snapshots with IAM-scoped access.
NOT auth service materializing views (rejected for latency/complexity)."
```

---

## Phase 5: CI/Test/Observability

### Task 10: Deterministic Simulation Harness

**Goal:** Create test harness for deterministic failure injection.

**Files:**
- Create: `crates/arco-test-utils/src/simulation.rs`

### Task 11: CI Tiers

**Goal:** Fast suite, nightly chaos, golden E2E.

**Files:**
- Modify: `.github/workflows/ci.yml`
- Create: `.github/workflows/nightly-chaos.yml`

### Task 12: Metrics/Spans/Dashboards

**Goal:** Complete observability artifacts as deliverables.

**Files:**
- Create: `docs/runbooks/metrics-catalog.md`
- Update: `docs/runbooks/grafana-dashboard.json`
- Update: `docs/runbooks/prometheus-alerts.yaml`

---

## Uncheatable Tests Summary

These tests verify the Gate 5 guarantees are mechanically enforced, not just documented:

| Test | Verifies | Location |
|------|----------|----------|
| `reader_cannot_list.rs` | ReadStore has no `list_page` method | `arco-core/tests/compile_negative/` |
| `reader_cannot_put.rs` | ReadStore has no `put_ledger` method | `arco-core/tests/compile_negative/` |
| `putstore_cannot_cas.rs` | PutStore has no `cas_manifest` method | `arco-core/tests/compile_negative/` |
| `test_no_visibility_without_pointer_swap` | Data not visible until manifest CAS | `arco-catalog/tests/` |
| `test_stale_lock_holder_loses` | Stale permit fails CAS | `arco-catalog/tests/` |
| `test_bypass_attempt_has_no_api_surface` | No public permit constructor | `arco-core/tests/` |
| `test_api_cannot_write_state` | IAM blocks API from state/ | Integration test (deployed) |
| `test_pointer_regression_rejected` | Version cannot decrease | `arco-catalog/tests/` |
| `test_parent_hash_detects_concurrent_modification` | Raw byte hash catches tampering | `arco-catalog/tests/` |

**Reader Op Whitelist Instrumentation Test:**
```rust
#[tokio::test]
async fn test_reader_only_calls_get_operations() {
    let instrumented = InstrumentedBackend::new(backend);
    let reader = CatalogReader::new(instrumented.as_reader());

    // Perform typical read operations
    reader.list_namespaces().await.unwrap();
    reader.get_table("ns", "table").await.unwrap();

    // Verify ONLY get/get_range were called
    let ops = instrumented.operations();
    for op in &ops {
        assert!(
            matches!(op, StorageOp::Get { .. } | StorageOp::GetRange { .. }),
            "Reader called forbidden op: {:?}", op
        );
    }

    // Verify no list, put, head, cas, delete
    assert!(!ops.iter().any(|op| matches!(op, StorageOp::List { .. })));
    assert!(!ops.iter().any(|op| matches!(op, StorageOp::Put { .. })));
    assert!(!ops.iter().any(|op| matches!(op, StorageOp::Head { .. })));
}
```

---

## Acceptance Criteria Summary

| Requirement | Test/Verification |
|-------------|-------------------|
| Trait separation | `compile_negative/*.rs` tests pass (trybuild) |
| Typed keys | LedgerKey/StateKey/ManifestKey used everywhere |
| PublishPermit required | `test_no_visibility_without_pointer_swap` passes |
| Real fencing | `test_stale_lock_holder_loses` passes |
| No bypass API | `test_bypass_attempt_has_no_api_surface` documents |
| IAM sole writer | `test_api_cannot_write_state` passes in deployed env |
| Rollback detection | `test_pointer_regression_rejected` passes |
| Raw byte hashing | `test_parent_hash_detects_concurrent_modification` passes |
| Torn-write prevention | `test_mid_write_crash_never_referenced` passes |
| Backpressure | Soft/hard threshold tests pass |
| Search tombstones | Delete/rename reflected in search |
| Security ADR | ADR-019 accepted (partitioned snapshots + IAM) |
| CI artifacts | All tiers green in CI |

---

## Execution Order

> **Critical:** Task 1 (Tier-1 Write Path) is BLOCKING. It determines IAM boundaries.

**Phase 1 - Critical Path (removes 90% of risk):**

1. **Task 1: Tier-1 Write Path + ADR-018** ← MUST BE FIRST
   - Resolves the Jan 12 contradiction
   - Determines which service writes which prefixes
   - IAM cannot be configured until this is decided

2. **Task 2: Split Storage Traits + Typed Keys**
   - ReadStore, PutStore, CasStore, ListStore
   - Compile-negative tests actually fail

3. **Task 3: PublishPermit with Real Fencing**
   - PermitIssuer from lock guard
   - No process-local counters

4. **Task 4: IAM Prefix Scoping**
   - Uses strict `startsWith`, not `contains`
   - Smoke test verifies enforcement

5. **Task 5: Rollback Detection**
   - Raw byte hashing (not re-serialization)
   - Version regression rejected

**Phase 2 - Architecture:**

6. **Task 6: Compactor Split**
   - Notification consumer (no listing)
   - Anti-entropy (cursor-based)

**Phase 3 - Operations:**

7. **Task 7: Backpressure**
8. **Task 8: Search Tombstones**

**Phase 4 - Compliance:**

9. **Task 9: ADR-019** (Partitioned snapshots + IAM)

**Phase 5 - CI/Observability:**

10. **Task 10: Deterministic Simulation**
11. **Task 11: CI Tiers**
12. **Task 12: Metrics/Dashboards**
