# Gate 5 Hardening Plan Patch (Jan 12 → Mechanically Enforced)

> **For Claude:** This is the AUTHORITATIVE delta to `docs/plans/2025-12-18-gate5-hardening.md`. Apply these changes in order.

**Goal:** Convert the Jan 12 invariants (append → compact → publish → read) from "true by convention" to **true by construction** via capability boundaries, publish fencing, IAM prefix scoping, and rollback/torn-write detection.

**Referenced Documents:**
- Unified Platform Design (Jan 12, 2025): `docs/plans/2025-01-12-arco-unified-platform-design.md`
- Gate 5 base plan: `docs/plans/2025-12-18-gate5-hardening.md`

---

## Critical Contradictions to Resolve

### Contradiction 1: "Who publishes manifests?"

**Current plan says:**
- Task 1: API writes only `ledger/` + `locks/`; Compactor writes `state/`, `l0/`, `manifests/`
- Task 4 IAM: Grants API `objectUser` on `manifests/` (write permission)

**This is inconsistent.** If compactor is the sole publisher, API should NOT have manifest write.

**Resolution:** Remove API write to `manifests/`. Compactor is the **only** principal that updates manifests. This makes IAM enforcement "hard" (deploy-time) not "soft" (code discipline).

### Contradiction 2: Sync compaction uses listing

**Current Task 1 says:**
- `compact_and_wait()` step 1: "List events since watermark"

**This violates:** Jan 12 says listing is anti-entropy only; normal path is notification-driven.

**Resolution:** Synchronous compaction takes **explicit event keys** (no list). API passes event paths to compactor RPC.

---

## Patch Delta Checklist (Apply in Order)

### Patch 1: Remove API manifest write (Task 4)

**Change:** Delete `api_write_manifests` IAM binding entirely.

**Rationale:** If compactor is the sole publisher (Jan 12 invariant), API cannot write manifests. This converts "sole writer" from code discipline to IAM-enforced.

**Updated IAM grants:**
| Principal | Prefixes (write) |
|-----------|------------------|
| API SA | `ledger/`, `locks/` |
| Compactor SA | `state/`, `l0/`, `manifests/` |

**If Tier-1 DDL needs API to publish:** Then compactor cannot publish the same manifest, and "sole writer" becomes "API for core, compactor for domains." This requires explicit ADR decision. **Recommend against** - keep compactor as sole publisher and use RPC for sync compaction.

---

### Patch 2: Split PutStore → LedgerPutStore + StatePutStore (Task 2)

**Current:** `PutStore` has both `put_ledger()` and `put_state()`.

**Problem:** Any code with `&dyn PutStore` can compile a `put_state()` call. Relies on IAM to block at runtime.

**Change:** Split into prefix-scoped capabilities:

```rust
/// API gets this (only)
#[async_trait]
pub trait LedgerPutStore: Send + Sync + 'static {
    async fn put_ledger(&self, key: &LedgerKey, data: Bytes) -> Result<()>;
}

/// Compactor gets this (only)
#[async_trait]
pub trait StatePutStore: Send + Sync + 'static {
    async fn put_state(&self, key: &StateKey, data: Bytes) -> Result<()>;
}
```

**Add compile-negative test:**
```rust
// crates/arco-core/tests/compile_negative/ledger_cannot_state.rs
use arco_core::{LedgerPutStore, StateKey};
use bytes::Bytes;

async fn api_code<S: LedgerPutStore>(store: &S) {
    let key = StateKey::new("catalog", 1, "foo.parquet");
    store.put_state(&key, Bytes::new()).await;  // ERROR: no method
}

fn main() {}
```

**Rationale:** Gate 5 "uncheatable" philosophy - make "oops, API wrote state/" impossible at compile time, not just runtime IAM.

---

### Patch 3: Update manifest key naming (Task 2)

**Current:** `ManifestKey { domain }` → `manifests/{domain}.json`

**Jan 12 actual naming:**
- `manifests/root.manifest.json`
- `manifests/catalog_core.manifest.json`
- `manifests/executions.manifest.json`

**Change:** Introduce two typed keys:

```rust
/// Root manifest pointer (single source of truth)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootManifestKey;

impl RootManifestKey {
    pub fn path(&self) -> &'static str {
        "manifests/root.manifest.json"
    }
}

/// Domain-specific manifest pointer
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainManifestKey {
    domain: String,
}

impl DomainManifestKey {
    pub fn new(domain: impl Into<String>) -> Self {
        Self { domain: domain.into() }
    }

    pub fn path(&self) -> String {
        format!("manifests/{}.manifest.json", self.domain)
    }
}
```

**Tests must exercise the exact pointer chain:** root → domain → parquet refs.

---

### Patch 4: Make FencingToken unforgeable (Task 3)

**Current problem:** Any code can do:
```rust
FencingToken::from_lock_sequence(999999)
PermitIssuer::from_lock_guard(token, resource)
```

This violates "permit is lock-derived, no bypass."

**Change 1:** Make `FencingToken` constructor `pub(crate)`:

```rust
// In crates/arco-core/src/publish.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FencingToken(u64);

impl FencingToken {
    /// Creates a fencing token from lock metadata.
    /// PRIVATE: Only the lock module can create tokens.
    pub(crate) fn from_lock_sequence(sequence: u64) -> Self {
        Self(sequence)
    }

    pub fn sequence(&self) -> u64 {
        self.0
    }
}
```

**Change 2:** `PermitIssuer` takes `&LockGuard`, not token:

```rust
impl PermitIssuer {
    /// Creates a permit issuer from a lock guard.
    /// The LockGuard is the ONLY way to get a valid fencing token.
    pub fn from_lock(guard: &LockGuard) -> Self {
        Self {
            fencing_token: guard.fencing_token(),  // LockGuard exposes token
            resource: guard.resource().to_string(),
        }
    }
}
```

**Change 3:** Add permit TTL fields:

```rust
pub struct PublishPermit {
    fencing_token: FencingToken,
    domain: String,
    expected_version: Option<String>,
    consumed: bool,
    issuer_resource: String,
    issued_at: DateTime<Utc>,      // NEW
    expires_at: DateTime<Utc>,     // NEW
}
```

---

### Patch 5: Real fencing enforcement via token in manifest (Task 3 + 5)

**Current problem:** Tests show stale holders losing via CAS version mismatch. But:
- Stale holder loses lock
- Later reads **current** manifest version
- Builds update, CAS succeeds (version matches)

CAS alone does NOT prevent split-brain unless fencing is bound to lock epochs.

**Change:** Add monotonic `fencing_token` to manifest payload:

```rust
pub struct CatalogDomainManifest {
    // ... existing fields ...

    /// Fencing token from the lock that authorized this publish.
    /// MUST be >= previous fencing_token.
    pub fencing_token: u64,

    /// Monotonic commit identifier (ULID).
    pub commit_ulid: String,

    /// Parent manifest hash (sha256 of raw stored bytes).
    pub parent_hash: Option<String>,
}
```

**Enforce at publish time:**

```rust
impl Publisher {
    pub async fn publish(&self, mut permit: PublishPermit, ...) -> Result<WriteResult> {
        // ... existing CAS logic ...

        // CRITICAL: Enforce fencing token monotonicity
        let prev: CatalogDomainManifest = serde_json::from_slice(&prev_bytes)?;
        if new_manifest.fencing_token < prev.fencing_token {
            return Err(CatalogError::StaleFencingToken {
                prev: prev.fencing_token,
                attempted: new_manifest.fencing_token,
            });
        }

        // ... proceed with CAS ...
    }
}
```

**Required test - REAL stale holder:**

```rust
#[tokio::test]
async fn test_stale_holder_cannot_publish_even_with_fresh_version() {
    // Holder A gets token=1, loses lock
    let issuer_a = PermitIssuer::from_lock(&guard_a);  // token=1

    // Holder B gets token=2, publishes successfully
    let guard_b = lock_manager.acquire("catalog").await?;
    let issuer_b = PermitIssuer::from_lock(&guard_b);  // token=2
    // ... publish with token=2 ...

    // Holder A reads FRESH manifest version (after B's publish)
    let (raw_bytes, fresh_version) = storage.as_cas()
        .get_manifest(&DomainManifestKey::new("catalog"))
        .await?;

    // Holder A tries to publish with stale token but fresh version
    let permit_a = issuer_a.issue_permit("catalog", fresh_version);
    // This must FAIL because token=1 < prev.token=2
    let result = publisher.publish(permit_a, &key, new_bytes).await;

    assert!(matches!(result, Err(CatalogError::StaleFencingToken { .. })));
}
```

**This test catches "we thought CAS was enough."**

---

### Patch 6: Add commit_ulid to pointer payload (Task 5)

**Gate 5 requires these pointer payload fields:**
- `commit_ulid` (monotonic identity)
- `parent_hash` (raw bytes)
- artifact sizes/checksums

**Change:** Add `commit_ulid` and enforce monotonicity:

```rust
pub struct CatalogDomainManifest {
    /// Monotonic commit ULID (lexicographically increasing).
    /// This is the "identity" of this manifest version.
    pub commit_ulid: String,

    // ... other fields ...
}

impl CatalogDomainManifest {
    pub fn validate_succession(&self, previous: &Self, previous_raw_hash: &str) -> Result<(), String> {
        // Existing version regression check
        if self.snapshot_version < previous.snapshot_version {
            return Err(format!("version regression: {} -> {}",
                previous.snapshot_version, self.snapshot_version));
        }

        // NEW: commit_ulid must be > previous
        if self.commit_ulid <= previous.commit_ulid {
            return Err(format!("commit_ulid regression: {} -> {}",
                previous.commit_ulid, self.commit_ulid));
        }

        // NEW: fencing_token must be >= previous
        if self.fencing_token < previous.fencing_token {
            return Err(format!("fencing_token regression: {} -> {}",
                previous.fencing_token, self.fencing_token));
        }

        // Parent hash check
        if let Some(ref parent_hash) = self.parent_hash {
            if parent_hash != previous_raw_hash {
                return Err(format!("parent hash mismatch: expected {}, got {}",
                    previous_raw_hash, parent_hash));
            }
        }

        Ok(())
    }
}
```

---

### Patch 7: Fix artifact verification trait (Task 5)

**Current problem:** `verify_snapshot_artifacts()` calls:
```rust
self.storage.as_cas().head_manifest(&ManifestKey::new(&full_path))
```

But:
- `CasStore::head_manifest` is manifest-specific
- Snapshot files are NOT manifests

**Change:** Add `MetaStore` capability for explicit object metadata:

```rust
/// Metadata capability for explicit objects (NOT listing).
#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// Gets object metadata by explicit path.
    /// Returns None if object doesn't exist.
    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>>;

    /// Typed variant for state files.
    async fn head_state(&self, key: &StateKey) -> Result<Option<ObjectMeta>> {
        self.head(&key.path()).await
    }
}
```

**Keep `MetaStore` out of `ReadStore`** - readers should only have `{get, get_range}` per Gate 5 instrumentation test.

**Fix artifact verification:**

```rust
async fn verify_snapshot_artifacts(&self, snapshot: &SnapshotInfo) -> Result<()> {
    for file in &snapshot.files {
        let key = StateKey::new(&snapshot.domain, snapshot.version, &file.path);
        let meta = self.storage.as_meta()  // NEW: MetaStore capability
            .head_state(&key)
            .await?
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: format!("snapshot file missing: {}", key),
            })?;

        if meta.size != file.byte_size {
            return Err(CatalogError::InvariantViolation {
                message: format!("size mismatch: {} expected {} got {}",
                    key, file.byte_size, meta.size),
            });
        }
    }
    Ok(())
}
```

---

### Patch 8: Clarify sync compaction mechanism (Task 1)

**Current problem:** `compact_and_wait()` lists events (step 1).

**Jan 12 says:** Normal path is notification-driven; listing is anti-entropy only.

**Change:** Synchronous compaction is an RPC to compactor with explicit event keys:

**API flow (Tier-1 DDL):**
```
1. Acquire distributed lock
2. Write ledger segment: ledger/catalog/{ulid}.json
3. Call compactor RPC: POST /compact?domain=catalog_core
   Body: { event_paths: ["ledger/catalog/{ulid}.json"], fencing_token: N }
4. Wait for response: { manifest_version, commit_ulid }
5. Release lock
```

**Compactor RPC handler:**
```rust
/// Synchronous compaction for Tier-1 strong consistency.
/// Takes explicit event paths (NO listing).
pub async fn handle_sync_compact(
    storage: &ScopedStorage,
    request: SyncCompactRequest,
) -> Result<SyncCompactResponse> {
    // Validate fencing token matches current lock holder
    let expected_token = storage.lock_manager.current_holder_token("catalog").await?;
    if request.fencing_token != expected_token {
        return Err(CompactorError::StaleFencingToken);
    }

    // Load events from EXPLICIT paths (no list)
    let mut events = Vec::new();
    for path in &request.event_paths {
        let bytes = storage.as_reader().get(path).await?;
        let event: CatalogEvent = serde_json::from_slice(&bytes)?;
        events.push(event);
    }

    // Read current manifest
    let manifest_key = DomainManifestKey::new(&request.domain);
    let (raw_bytes, version) = storage.as_cas().get_manifest(&manifest_key).await?;
    let prev_manifest: CatalogDomainManifest = serde_json::from_slice(&raw_bytes)?;

    // Fold events into new snapshot
    let new_snapshot = self.fold_events(&prev_manifest, &events).await?;

    // Write Parquet to state/ (compactor is sole writer)
    self.write_snapshot_parquet(&new_snapshot).await?;

    // Publish manifest via permit
    let permit = self.permit_issuer.issue_permit(&request.domain, version);
    let new_manifest = self.build_manifest(&prev_manifest, &new_snapshot)?;
    let result = self.publisher.publish(permit, &manifest_key, new_manifest).await?;

    Ok(SyncCompactResponse {
        manifest_version: result.version,
        commit_ulid: new_manifest.commit_ulid,
    })
}
```

**This maintains:**
- "append-only ingest" (API writes ledger)
- "sole Parquet writer" (compactor writes state)
- "no listing dependency for correctness" (explicit keys)

---

### Patch 9: Split compactor IAM (Task 6 + IAM hardening)

**Extra hardening:** Use two service accounts for compactor:

| SA | Permissions | Purpose |
|----|-------------|---------|
| `compactor-fastpath-sa` | `state/**`, `l0/**`, `manifests/**` write; **NO list** | Notification consumer |
| `compactor-antientry-sa` | `ledger/**` list; `state/**` read | Anti-entropy job |

**Rationale:** Makes "oops, compactor started listing in hot path" not just a compile-time issue but also a deploy-time IAM failure.

**Update IAM:**

```hcl
# Compactor fast-path: NO list permission
resource "google_storage_bucket_iam_member" "compactor_fastpath_write_state" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.compactor_fastpath.email}"
  # ... condition for state/ prefix ...
}

# Anti-entropy: list permission on ledger/
resource "google_storage_bucket_iam_member" "antientry_list_ledger" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectViewer"  # includes list
  member = "serviceAccount:${google_service_account.compactor_antientry.email}"
  # ... condition for ledger/ prefix ...
}
```

---

### Patch 10: Backpressure from watermark positions (Task 7)

**Current problem:** `pending_events` source is undefined (implies listing).

**Jan 12 recommends:** Use Servo's monotonic global position as watermark anchor.

**Change:** Define pending as position lag:

```rust
/// Backpressure state computed from watermark positions.
#[derive(Debug, Clone)]
pub struct BackpressureState {
    /// Last written position (from Servo/ingestion).
    pub last_written_position: u64,
    /// Last compacted position (from manifest watermark).
    pub last_compacted_position: u64,
    /// Soft threshold (position lag).
    pub soft_threshold: u64,
    /// Hard threshold (position lag).
    pub hard_threshold: u64,
    /// Domain identifier.
    pub domain: String,
}

impl BackpressureState {
    /// Position lag (no listing required).
    pub fn pending_lag(&self) -> u64 {
        self.last_written_position.saturating_sub(self.last_compacted_position)
    }

    pub fn evaluate(&self) -> BackpressureDecision {
        let lag = self.pending_lag();
        if lag >= self.hard_threshold {
            BackpressureDecision::Reject {
                retry_after: Duration::from_secs(5),
            }
        } else if lag >= self.soft_threshold {
            BackpressureDecision::AcceptWithWarning { pending_lag: lag }
        } else {
            BackpressureDecision::Accept
        }
    }
}
```

**Positions come from:**
- `last_written_position`: incremented on each ledger append (in-memory or from Servo metadata)
- `last_compacted_position`: read from manifest `position_watermark` field

**No listing required for backpressure computation.**

---

### Patch 11: Supersede older path conventions (ADR-018 addition)

**Problem:** Some docs show `tenant=acme/_catalog/...` while Jan 12 uses `{tenant}/{workspace}/ledger|state|manifests/...`

**Change:** Add explicit callout in ADR-018:

```markdown
## Superseded Conventions

The following path conventions from older documents are superseded by this ADR:

- `gs://.../tenant={id}/_catalog/...` → now `{tenant}/{workspace}/state/catalog/...`
- `gs://.../tenant={id}/_events/...` → now `{tenant}/{workspace}/ledger/...`

The canonical storage layout is:
```
{bucket}/{tenant}/{workspace}/
├── ledger/     # immutable events (API writes)
├── state/      # compacted Parquet (Compactor writes)
├── l0/         # L0 compaction tier (Compactor writes)
├── manifests/  # domain pointers (Compactor writes)
└── locks/      # distributed locks (API writes)
```
```

---

## Updated Acceptance Criteria

| Requirement | Test/Verification |
|-------------|-------------------|
| **Trait separation (prefix-scoped)** | `LedgerPutStore` cannot `put_state` (compile-fail) |
| **Unforgeable FencingToken** | `FencingToken::from_lock_sequence` is `pub(crate)` |
| **PermitIssuer from LockGuard only** | `PermitIssuer::from_lock(&guard)` requires guard ref |
| **Real fencing in manifest** | `fencing_token` field enforced >= previous |
| **commit_ulid monotonic** | `validate_succession` checks ULID ordering |
| **Stale holder blocked (real test)** | Stale token + fresh version → rejected |
| **IAM: API cannot write manifests** | No `api_write_manifests` binding |
| **IAM: compactor fast-path no list** | Separate SA without list permission |
| **MetaStore for artifacts** | `head_state(&StateKey)` not `head_manifest` |
| **Sync compaction no list** | Explicit event paths in RPC |
| **Backpressure from positions** | `pending_lag = written - compacted` |

---

## Execution Sequencing

Apply patches in this order (dependencies noted):

1. **Patch 1** (IAM: remove API manifest write) - can apply immediately
2. **Patch 3** (manifest key naming) - can apply immediately
3. **Patch 2** (LedgerPutStore/StatePutStore) - depends on Patch 3
4. **Patch 7** (MetaStore) - can apply with Patch 2
5. **Patch 4** (unforgeable FencingToken) - can apply immediately
6. **Patch 5** (fencing in manifest + commit_ulid) - depends on Patch 4
7. **Patch 6** (commit_ulid) - incorporated into Patch 5
8. **Patch 8** (sync compaction RPC) - depends on Patch 5
9. **Patch 9** (split compactor IAM) - apply after Patch 1
10. **Patch 10** (backpressure from positions) - can apply immediately
11. **Patch 11** (path conventions ADR) - apply with ADR-018

---

## Uncheatable Test Matrix

| Test Name | What It Proves | Cheat It Catches |
|-----------|----------------|------------------|
| `ledger_cannot_state.rs` (compile-fail) | `LedgerPutStore` has no `put_state` | "Just this once" state write from API |
| `test_stale_holder_cannot_publish_even_with_fresh_version` | Fencing token in manifest enforced | "CAS version match is enough" |
| `test_fencing_token_unforgeable` | Can't construct token without lock | Bypass via fake token |
| `test_api_cannot_write_manifests` (IAM smoke) | No API manifest write permission | "API publishes directly" |
| `test_compactor_fastpath_cannot_list` (IAM smoke) | Fast-path SA has no list | "Just list to find events" |
| `test_commit_ulid_monotonic` | ULID must increase | Replay old manifest |
| `test_artifact_verification_uses_metastore` | `head_state` not `head_manifest` | Wrong trait for artifact check |
