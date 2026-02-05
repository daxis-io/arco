# Iceberg REST Catalog Integration Design

**Status:** Draft (refined from RFC-002)
**Date:** 2025-12-22
**Based on:** RFC-002 + architecture review feedback

---

## Executive Summary

This design refines RFC-002 to ensure Arco's Iceberg REST integration is:
1. **Spec-compliant** with Iceberg REST's idempotency and atomicity contracts
2. **Architecturally aligned** with Arco's existing Tier-1/Tier-2 patterns
3. **Portable** via StorageBackend abstraction (not GCS-specific)

Key changes from RFC-002:
- Durable idempotency markers replace ring buffer
- Immutable pending/committed receipts replace single `pending_event` slot
- Iceberg metadata becomes authoritative for schema (Arco schema is projection)
- Pointer contract simplified (no embedded history)

---

## 1. Authority Model

### 1.1 What Each Layer Owns

| Authority | Owns | Consistency | Mechanism |
|-----------|------|-------------|-----------|
| **Tier 1** | Identity, governance, storage location, format tag | Strong | Tenant lock + manifest CAS |
| **Iceberg Pointer** | Current Iceberg state (metadata location, refs, sequence) | Strong | Table-scoped CAS |
| **Iceberg Metadata File** | Schema, partition spec, snapshots, properties | Strong | Immutable file + pointer reference |
| **Tier 2** | History, lineage, UI timeline | Eventual | Append-only events + compaction |

### 1.2 Schema Authority for Iceberg Tables

**Decision:** Iceberg metadata is authoritative for Iceberg tables.

For tables with `format = ICEBERG`:
- The committed Iceberg metadata file is the source of truth for schema
- Arco's schema registry (`columns.parquet`) is a **projection with bounded staleness**
- Projection updated asynchronously by compactor reading committed Iceberg metadata

This avoids the atomicity violation where "Tier-1 schema commit then pointer CAS" creates intermediate visible states on crash.

**Tier 1 remains authoritative for:**
- `(namespace, name) -> table_uuid` mapping
- Table existence / deletion state
- Storage location (Arco-managed)
- Format tag (`ICEBERG` | `DELTA` | `ARCO_NATIVE`)
- Governance metadata (owner, description, tags)

### 1.3 Governance Guardrails

To prevent "dual authority" drift, Arco enforces guardrails on Iceberg updates:

| Update Type | Arco Behavior |
|-------------|---------------|
| `SetLocationUpdate` | **Reject with 400** - Arco owns storage location |
| `SetPropertiesUpdate` with `arco.*` keys (case-insensitive) | **Reject with 400** - Reserved namespace |
| `RemovePropertiesUpdate` with `arco.*` keys (case-insensitive) | **Reject with 400** - Reserved namespace |
| All other updates | Allow (Iceberg metadata authoritative) |

The `arco.*` property namespace is reserved for Arco-managed metadata (lineage refs, quality status, etc.) and is enforced case-insensitively.

---

## 2. Pointer Contract (Simplified)

### 2.1 Pointer File Format

```rust
/// gs://bucket/tenant/_catalog/iceberg_pointers/{table_uuid}.json
pub struct IcebergTablePointer {
    /// Schema version for forward compatibility
    pub version: u32,  // Currently 1

    /// The table this pointer belongs to
    pub table_uuid: Uuid,

    /// Current Iceberg metadata file location
    pub current_metadata_location: String,

    /// Current snapshot ID (denormalized for fast checks)
    pub current_snapshot_id: Option<i64>,

    /// Snapshot refs (main, branches, tags)
    pub refs: HashMap<String, SnapshotRef>,

    /// Last sequence number assigned
    pub last_sequence_number: i64,

    /// Previous metadata location (for history building and validation)
    pub previous_metadata_location: Option<String>,

    /// Last update metadata
    pub updated_at: DateTime<Utc>,
    pub updated_by: UpdateSource,
}
```

### 2.2 Pointer Lifecycle

| Operation | Pointer Action |
| --------- | -------------- |
| `createTable` | Create pointer with `DoesNotExist` precondition |
| `dropTable` | Delete pointer with `MatchesVersion` precondition |
| `commit_table` | CAS update with `MatchesVersion` precondition |
| Table re-create after drop | New pointer (new table_uuid) |

**Deletion semantics:** Table deletion must remove the pointer using a version-match precondition (or write a tombstone) so in-flight commits with stale versions fail. Commit flow treats a missing pointer as `404 NoSuchTableException`.

**Removed from RFC-002:**
- `recent_commits: VecDeque<CommitRecord>` - replaced by durable idempotency markers
- `pending_event: Option<PendingEvent>` - replaced by immutable event receipts

### 2.3 CAS Semantics

Pointer updates use `StorageBackend` with version-match preconditions:

```rust
/// Opaque version token - portable across GCS/S3/ADLS
/// GCS: generation number as string
/// S3: ETag string
/// ADLS: ETag string
pub struct ObjectVersion(pub String);

pub async fn compare_and_swap(
    &self,
    table_uuid: &Uuid,
    expected_version: &ObjectVersion,
    new_pointer: &IcebergTablePointer,
) -> Result<CasResult> {
    let result = self.storage
        .put(
            &pointer_path(table_uuid),
            Bytes::from(serde_json::to_vec(new_pointer)?),
            WritePrecondition::MatchesVersion(expected_version.clone()),
        )
        .await?;

    match result {
        WriteResult::Success { version } => Ok(CasResult::Success { new_version: version }),
        WriteResult::PreconditionFailed { current_version } => {
            Ok(CasResult::Conflict { current_version })
        }
    }
}
```

**Backend mapping:**
- GCS: `ifGenerationMatch` header with generation number
- S3: `If-Match` header with ETag (conditional writes supported since 2024)
- Azure Blob: `If-Match` header with ETag

### 2.4 StorageBackend Requirements

To safely implement CAS on object storage, `StorageBackend` must provide:
- Strongly consistent conditional writes with a monotonic generation/version.
- Conditional deletes (MatchesVersion) for pointer removal on table delete.
- Read-after-write for pointer + idempotency marker paths.

If a backend cannot satisfy these guarantees, `commit_table` must be disabled for that backend.

---

## 3. Idempotency (Durable Markers)

### 3.1 Why Ring Buffer is Insufficient

Iceberg REST defines `Idempotency-Key` header semantics with server-advertised `idempotency-key-lifetime` in `/v1/config`. A 16-slot ring buffer violates this contract on busy tables where 16 commits may occur within the retry window.

### 3.2 Durable Two-Phase Markers

**Path:** `_catalog/iceberg_idempotency/{table_uuid}/{hash_prefix}/{idempotency_key_hash}.json`

Hash prefix (first 2 chars of key hash) provides fanout to avoid prefix hotspots.
`idempotency_key_hash = SHA256(raw Idempotency-Key)`, and the raw key is stored in the marker for diagnostics.
Validate Idempotency-Key: must be UUIDv7 (RFC 9562) in canonical string form; invalid keys return 400.
Canonical request hashing must follow RFC 8785 JCS; use a vetted implementation (e.g., `serde_jcs`) to avoid spec drift.

**Marker States:**

```rust
pub struct IdempotencyMarker {
    pub status: IdempotencyStatus,
    pub idempotency_key: String,
    pub table_uuid: Uuid,
    pub request_hash: String,  // SHA256 of canonical request body (RFC 8785 JCS)
    pub started_at: DateTime<Utc>,
    pub committed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub error_http_status: Option<u16>,
    pub error_payload: Option<IcebergErrorResponse>,

    // Allocated at claim time (stable for crash recovery)
    pub event_id: Ulid,
    pub base_metadata_location: String, // Pointer current metadata at claim time
    pub metadata_location: String,  // Deterministic: .../metadata/{seq}-{key_hash}.metadata.json

    // Set after pointer CAS succeeds
    pub response_payload: Option<CommitTableResponse>,
}

pub enum IdempotencyStatus {
    InProgress,
    Committed,
    Failed,
}
```

**Key design choice:** `event_id`, `metadata_location`, and `base_metadata_location` are allocated at marker claim time (not after CAS). This enables crash recovery: if we find an IN_PROGRESS marker, we can check whether its `metadata_location` matches the current pointer to determine if the commit actually succeeded.

If `response_payload` is large (e.g., >1MB), store only `metadata_location` and rehydrate the response by reading the metadata file on replay.

### 3.3 Marker Flow

**Step 1: Claim marker (before work)**
```rust
// Validate Idempotency-Key is UUIDv7 before proceeding
// Allocate IDs at claim time (metadata_location is deterministic)
let event_id = Ulid::new();
let metadata_location = format!(
    "{}/metadata/{}-{}.metadata.json",
    table_location, next_sequence, idempotency_key_hash
);
let base_metadata_location = pointer.current_metadata_location.clone();

let marker = IdempotencyMarker {
    status: IdempotencyStatus::InProgress,
    request_hash: sha256_jcs(&canonical_request),
    started_at: Utc::now(),
    event_id,
    base_metadata_location,
    metadata_location: metadata_location.clone(),
    ..
};

match storage.put(&marker_path, marker_bytes, WritePrecondition::DoesNotExist).await? {
    WriteResult::Success { version } => {
        // Proceed with commit
        marker_version = version;
    }
    WriteResult::PreconditionFailed { .. } => {
        // Check existing marker
        let (existing, existing_version) = storage.get_with_version(&marker_path).await?;

        match existing.status {
            IdempotencyStatus::Committed if existing.request_hash == our_hash => {
                // Return cached response (200)
                return Ok(existing.response_payload);
            }

            IdempotencyStatus::Failed if existing.request_hash == our_hash => {
                // Return cached error with original status
                return Err(existing.error_payload, existing.error_http_status);
            }

            IdempotencyStatus::InProgress if existing.request_hash == our_hash => {
                // CRITICAL: Attempt crash recovery before returning 503
                let pointer = storage.get(&pointer_path).await?;

                if pointer.current_metadata_location == existing.metadata_location {
                    // Commit DID succeed - crashed before marker finalization
                    // Complete the flow: write committed receipt, finalize marker
                    self.emit_committed_receipt(&existing).await.ok();
                    let finalized = IdempotencyMarker {
                        status: IdempotencyStatus::Committed,
                        committed_at: Some(Utc::now()),
                        response_payload: Some(self.build_response(&pointer)),
                        ..existing
                    };
                    storage.put(&marker_path, finalized,
                        WritePrecondition::MatchesVersion(existing_version)).await.ok();
                    return Ok(finalized.response_payload);
                }

                // Commit did not succeed - check if stale enough for takeover
                if existing.started_at + in_progress_timeout < Utc::now() {
                    // Stale in-progress marker, attempt takeover via CAS
                    let takeover = IdempotencyMarker {
                        started_at: Utc::now(),
                        event_id: Ulid::new(),  // New event_id for new attempt
                        metadata_location: new_metadata_location,
                        ..existing
                    };
                    match storage.put(&marker_path, takeover,
                        WritePrecondition::MatchesVersion(existing_version)).await? {
                        WriteResult::Success { version } => {
                            marker_version = version;
                            // Proceed with commit using takeover's event_id/metadata_location
                        }
                        WriteResult::PreconditionFailed { .. } => {
                            // Lost race, retry from start
                            return Err(RetryFromStart);
                        }
                    }
                } else {
                    // Another request legitimately in flight
                    return Err(ServiceUnavailable { retry_after: in_progress_timeout });
                }
            }

            _ => {
                // Different payload with same key - deterministic 409
                return Err(IcebergErrorResponse {
                    error_type: "CommitFailedException",
                    message: "Idempotency-Key reused with different request payload",
                    code: 409,
                });
            }
        }
    }
}
```

**Step 2: After pointer CAS succeeds - finalize marker**
```rust
let finalized = IdempotencyMarker {
    status: IdempotencyStatus::Committed,
    committed_at: Some(Utc::now()),
    response_payload: Some(response),
    ..marker
};

// CAS update using marker version
storage.put(&marker_path, finalized_bytes,
    WritePrecondition::MatchesVersion(marker_version)).await?;
```

### 3.4 Failure Finalization + Stale Takeover

- Finalize markers on **success (200/201/204)** and **deterministic terminal 4xx** (e.g., 404/409/422) with cached payload + status.
- **Do not finalize on 5xx** (commit state unknown). Return the error and leave the marker `InProgress`; retries must resolve via crash recovery or stale takeover.
- If marker finalization fails, return the error; the next retry will either see a cached result or trigger stale takeover.
- `in_progress_timeout` must be <= `idempotency-key-lifetime` (default `PT10M`), after which a new request with the same hash may take over the marker via CAS.

### 3.5 Canonical Request Hashing

- Use RFC 8785 JSON Canonicalization Scheme (JCS) on the request body.
- Preserve array order for `requirements` and `updates`.
- Include all request fields (e.g., `identifier`, `staged`, `table-uuid`) in the hash.
- Reject bodies that cannot be canonicalized or exceed size limits (400).

### 3.6 /v1/config Advertisement

```json
{
  "defaults": {},
  "overrides": {
    "prefix": "arco",
    "namespace-separator": "%1F"
  },
  "idempotency-key-lifetime": "PT1H",
  "endpoints": [
    "GET /v1/config",
    "GET /v1/{prefix}/namespaces",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "POST /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/tables/rename"
  ]
}
```

**Notes:**

- HEAD endpoints for existence checks (many clients use these)
- Explicitly listing endpoints tells clients we don't support scan planning, views, or transactions
- `namespace-separator` must be honored and the server must also accept `%1F` (default unit separator) for compatibility
- `namespace-separator` is URL-encoded in config and must be decoded before use
- `stage-create=true` in createTable returns 400 NotSupported (Phase B)

### 3.7 Idempotency Coverage (All Mutations)

Apply Idempotency-Key semantics to all mutation endpoints we expose:
- `createNamespace`, `createTable`, `dropNamespace`, `dropTable`, `renameTable`, `commit_table`
- Finalization rules match Section 3.4 (2xx + deterministic 4xx finalized; 5xx not finalized)

For non-commit endpoints, use a simplified marker payload (status + request_hash + cached response/error). The extended fields shown in Section 3.2 are specific to `commit_table`.

---

## 4. Event Delivery (Immutable Receipts)

### 4.1 Why Single Pending Slot Fails

RFC-002's `pending_event: Option<PendingEvent>` has a drop-on-overwrite bug:
1. Commit A succeeds, event emission fails, `pending_event = A`
2. Commit B succeeds, overwrites `pending_event = B`
3. Event A is permanently lost

### 4.2 Append-Only Receipt Pattern

Two immutable files per commit attempt:

```
events/YYYY-MM-DD/iceberg/pending/{commit_key}.json   # Written before pointer CAS
events/YYYY-MM-DD/iceberg/committed/{commit_key}.json # Written after pointer CAS
```

`commit_key = SHA256(metadata_location)` (hex). Decision: no prefix sharding in v1; add optional `{commit_prefix}/` (first 2 chars) if we observe hot-prefix errors or sustained >100 writes/sec per table. Guard with a feature flag and a one-time backfill to the sharded path.

Both written with `WritePrecondition::DoesNotExist` (idempotent).
Pending receipts are best-effort for observability; committed receipts are deterministically backfillable from the metadata log.

**Alternative isolation path** (if existing compactor is strict about event shape):
```
_catalog/iceberg/outbox/pending/{commit_key}.json
_catalog/iceberg/outbox/committed/{commit_key}.json
```

### 4.3 Event Schema

Extend `CatalogEvent` enum for compactor compatibility:

```rust
pub enum CatalogEvent {
    // Existing variants...

    /// Iceberg commit was attempted (pending)
    IcebergCommitPending {
        table_uuid: Uuid,
        commit_key: String, // SHA256(metadata_location)
        event_id: Ulid,
        metadata_location: String,
        base_metadata_location: String,
        snapshot_id: Option<i64>,
        source: UpdateSource,
    },

    /// Iceberg commit was confirmed (pointer CAS succeeded)
    IcebergCommitCommitted {
        table_uuid: Uuid,
        commit_key: String, // SHA256(metadata_location)
        event_id: Ulid,
        metadata_location: String,
        snapshot_id: Option<i64>,
        previous_metadata_location: Option<String>,
        source: UpdateSource,
    },
}
```

### 4.4 Compactor Rules

- Only process `IcebergCommitCommitted` events
- Ignore `IcebergCommitPending` (or don't write them to main event stream)
- Update Arco schema projection from Iceberg metadata
- Update lineage/history materializations

### 4.5 Anti-Entropy Reconciler (Metadata-Log Driven)

**Critical design choice:** Reconcile from the **Iceberg metadata log**, not from bucket listings or the latest pointer. This allows deterministic backfill of all missing committed receipts without listing.

```rust
pub async fn reconcile_iceberg_events(&self, tenant_id: &str) -> ReconcileResult {
    for (table_uuid, pointer) in self.pointer_store.list_all(tenant_id).await? {
        let metadata = self.load_metadata(&pointer.current_metadata_location).await?;
        let history = self.metadata_log_chain(&metadata); // includes current + previous entries

        for entry in history {
            let commit_key = sha256(&entry.metadata_location);
            let committed_path = committed_receipt_path(&entry.timestamp, &commit_key);

            let receipt = IcebergCommitCommitted {
                table_uuid,
                commit_key,
                event_id: Ulid::new(),
                metadata_location: entry.metadata_location.clone(),
                snapshot_id: entry.snapshot_id,
                previous_metadata_location: entry.previous_metadata_location.clone(),
                source: UpdateSource::IcebergRest { client_info: None, principal: None },
            };

            // Backfill if missing; precondition failure indicates already present
            let _ = self.storage.put(&committed_path, receipt_bytes,
                WritePrecondition::DoesNotExist).await;
        }
    }
}
```

**Why metadata-log driven is correct:**
- Backfills all committed metadata entries, not just the latest pointer
- Does not require bucket listing for correctness
- Aligns with Iceberg metadata retention rules (log entries define survivorship)

Implementation notes:
- Walk metadata-log entries within retention; capture active refs/branches/tags in each entry for downstream GC/pointer derivation.
- Resolve metadata locations from absolute URIs to storage-relative paths (strip scheme/bucket and tenant/workspace prefix) while preserving the original location for commit-key derivation.
- Return entries in deterministic newest-to-oldest order (by metadata `last-updated-ms`, tie-break by location).
- Skip unreadable/corrupt metadata files but record counts for observability.
- Cap traversal depth to avoid unbounded reads on very old tables.

---

## 5. Commit Flow (Complete)

```
CommitTable(request, idempotency_key):

1. TIER 1 READ (no lock)
   - Resolve (namespace, table_name) -> table_uuid
   - Verify table exists, format = ICEBERG
   - Get storage location

2. LOAD POINTER + VERSION
   - GET iceberg_pointers/{table_uuid}.json (body + version)
   - If missing, return 404 TableNotFound
   - Note object version for CAS

3. LOAD BASE METADATA FILE
   - Read pointer.current_metadata_location
   - Parse schema, partition spec, refs, properties, metadata-log
   - Compute `next_sequence = last_sequence_number + 1`

4. CLAIM IDEMPOTENCY MARKER
   - PUT _catalog/iceberg_idempotency/{table_uuid}/{hash_prefix}/{idempotency_key_hash}.json
   - Precondition: DoesNotExist
   - Body: { status: "IN_PROGRESS", request_hash, started_at, event_id, metadata_location, base_metadata_location }
   - On conflict: check existing marker status, return cached success/error, or attempt stale takeover

5. VALIDATE ICEBERG REQUIREMENTS
   - assert-ref-snapshot-id
   - assert-table-uuid
   - assert-current-schema-id (if present)
   - Return 409 on failure

6. COMPUTE NEW STATE
   - Apply updates (AddSnapshot, SetSnapshotRef, etc.)
   - Assign new sequence number = `next_sequence`
   - Derive pointer fields (snapshot id, refs, last_sequence_number) from the new metadata file
   - Compute `commit_key = SHA256(metadata_location)`

7. WRITE NEW METADATA FILE
   - Path: deterministic from idempotency_key to prevent orphan spray
   - E.g., .../metadata/{sequence}-{idempotency_key_hash}.metadata.json
   - PUT with DoesNotExist; if exists, verify contents hash matches or fail

8. WRITE PENDING RECEIPT (best effort)
   - PUT events/.../iceberg/pending/{commit_key}.json
   - Precondition: DoesNotExist
   - Body: { table_uuid, commit_key, event_id, metadata_location, base_metadata_location, source }
   - If write fails, continue; committed receipts are backfillable from metadata log

9. POINTER CAS
   - PUT iceberg_pointers/{table_uuid}.json
   - Precondition: MatchesVersion(expected_version)
   - On conflict:
     - Reload pointer
     - If pointer missing: return 404 TableNotFound
     - If pointer.current_metadata_location == our_new_location:
       -> Our commit won (race with ourselves), continue to step 10
     - Else: genuine conflict, return 409 and finalize marker as Failed

10. WRITE COMMITTED RECEIPT (best effort)
   - PUT events/.../iceberg/committed/{commit_key}.json
   - Precondition: DoesNotExist
   - Body: full event with metadata_location, snapshot_id, source
   - If write fails, continue and rely on reconciler to backfill

11. FINALIZE IDEMPOTENCY MARKER
    - PUT marker with MatchesVersion(marker_version)
    - Body: { status: "COMMITTED", response_payload, event_id }

12. RETURN RESPONSE
    - metadata_location, metadata (inline JSON)

Failure handling (all steps after marker claim):
- On any terminal deterministic failure (4xx), update marker to `Failed` with cached error payload/status.
- On 5xx, do not finalize marker; retries will resolve via crash recovery or stale takeover.
```

---

## 6. Storage Layout

```
gs://bucket/tenant/_catalog/
├── core/                              # Tier 1 (tenant-locked)
│   ├── manifest.json
│   └── snapshots/
│       ├── tables.parquet             # Includes format=ICEBERG tag
│       └── columns.parquet            # Projection from Iceberg metadata
│
├── iceberg_pointers/                  # Table-scoped CAS
│   ├── {table_uuid_1}.json
│   └── {table_uuid_2}.json
│
├── iceberg_idempotency/               # Durable idempotency markers
│   └── {table_uuid}/
│       └── {hash_prefix}/
│           └── {idempotency_key_hash}.json
│
├── events/                            # Tier 2 (append-only)
│   └── YYYY-MM-DD/
│       └── iceberg/
│           ├── pending/               # Commit attempts
│           │   └── {commit_key}.json
│           └── committed/             # Confirmed commits
│               └── {commit_key}.json
│
└── operational/                       # Tier 2 projections
    └── materializations/
```

If commit sharding is enabled, insert `{commit_prefix}/` between `committed/` (or `pending/`) and `{commit_key}.json`.

---

## 7. Error Mapping

| Condition | HTTP Status | Iceberg Exception |
|-----------|-------------|-------------------|
| Requirements not met (ref mismatch, etc.) | 409 | CommitFailedException |
| Pointer CAS conflict | 409 | CommitFailedException |
| Idempotency key reused with different payload | 409 | CommitFailedException |
| Invalid Idempotency-Key (not UUIDv7) | 400 | BadRequestException |
| Commit in progress (concurrent duplicate) | 503 + Retry-After | - |
| Unknown update type | 400 | - |
| Table not found / pointer missing | 404 | NoSuchTableException |
| Crash during commit (uncertain state) | 500/502/504 | CommitStateUnknownException |

For CommitStateUnknownException: client should retry with same idempotency key; marker will resolve to cached response if commit succeeded.

---

## 8. Lineage for External Writes

### 8.1 Source Classification

| Source | Lineage Confidence | Quality Status |
|--------|-------------------|----------------|
| Servo | Verified (execution-observed) | Checked |
| Iceberg REST | DeclaredUnverified | NotChecked |
| Admin API | None | NotChecked |

### 8.2 Event Source Tracking

```rust
pub enum UpdateSource {
    Servo { run_id: Ulid, task_id: Ulid },
    IcebergRest {
        client_info: Option<String>,  // User-Agent
        principal: Option<String>,     // Auth subject
    },
    AdminApi { user_id: String },
}
```

### 8.3 UI Presentation

External writes are visually distinguished in Arco UI:

```
v5 (current) - 2025-01-15 14:30
    Source: Servo Run #1234
    Lineage: raw.orders[2025-01-15] -> daily_orders
    Quality: Passed

v4 - 2025-01-15 10:15
    Source: External (Iceberg REST API)
    Lineage: Unknown (external write)
    Quality: Not checked
```

---

## 9. Decisions / Remaining Gaps

### 9.1 REST Spec Compliance (Decisions)

- `stage-create=true`: not supported in Phase B; return 400 with a NotSupported error message and revisit in Phase C.
- ETag: return pointer version as ETag for `loadTable`; honor `If-None-Match` with 304.
- Unknown updates/requirements: return 400 with a list of unknown types.
- Idempotency-Key: require UUIDv7; finalize markers for 2xx + deterministic 4xx; do not finalize 5xx.
- Namespace separator: advertise `namespace-separator` and accept both the configured separator and `%1F`.
- Endpoints list: include `/credentials` if supported; if `endpoints` is returned it is authoritative.
- Delegated access: honor `X-Iceberg-Access-Delegation`; return `storage-credentials` in `loadTable` when requested.

### 9.2 Operational Defaults

- Reconciler: for each table, read current metadata and walk the metadata-log chain; compute `commit_key` for each entry and `PUT DoesNotExist` missing committed receipts; run every 15m (no listing required).
- Idempotency marker GC: retain `Committed`/`Failed` for `idempotency-key-lifetime + PT24H` grace; treat `InProgress` older than `in_progress_timeout + PT24H` as stale and either delete or mark `Failed` after verifying pointer does not match its metadata_location.
- Metadata GC: follow Iceberg metadata log retention (`metadata.previous-versions-max`, `metadata.delete-after-commit`); never delete files referenced by current pointer or active refs/branches/tags; orphan metadata from failed CAS is safe to delete after a conservative window.
- Event receipts GC: pending receipts older than PT24H are deletable; committed receipts retained 90 days or until compaction.

### 9.3 Credential Vending

Arco honors `X-Iceberg-Access-Delegation` with supported values:
- `vended-credentials` (return short-lived storage credentials)
- `remote-signing` (future; not supported in Phase A/B)

When `vended-credentials` is requested, return `storage-credentials` in the **LoadTableResponse** (not only the `/credentials` endpoint). Optionally include `config.client.refresh-credentials-endpoint` pointing to `/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials`.
Clients must set `X-Iceberg-Access-Delegation: vended-credentials` to receive storage credentials.

Arco vends short-lived credentials via `loadTable` response's `storage-credentials` field. Clients select credentials by **longest prefix match** and must prefer `storage-credentials` over `config` for auth.

**S3 (spec-defined keys):**
```json
{
  "storage-credentials": [{
    "prefix": "s3://bucket/warehouse/",
    "config": {
      "s3.access-key-id": "AKIA...",
      "s3.secret-access-key": "...",
      "s3.session-token": "...",
      "client.region": "us-east-1"
    }
  }]
}
```

**GCS:**
```json
{
  "storage-credentials": [{
    "prefix": "gs://bucket/warehouse/",
    "config": {
      "gcs.oauth2.token": "ya29...",
      "gcs.oauth2.token-expires-at": "2025-01-15T15:00:00Z"
    }
  }]
}
```

**ADLS:**
```json
{
  "storage-credentials": [{
    "prefix": "abfss://container@account.dfs.core.windows.net/",
    "config": {
      "adls.sas-token": "...",
      "adls.sas-token-expires-at": "2025-01-15T15:00:00Z"
    }
  }]
}
```

### 9.4 Engine Configuration Recipes

**Target engine versions (compatibility matrix):**
- DataFusion 40.x (primary; via iceberg-rust REST catalog)
- Spark 3.5.x (Iceberg runtime 1.6.x)
- PyIceberg 0.7.x
- Trino 450.x (validate vended credentials behavior)
- Flink 1.18.x

**DataFusion (Rust / iceberg-rust):**
- Use iceberg-rust REST catalog client; configure catalog URI and auth token.

**Spark:**
```
spark.sql.catalog.arco=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.arco.catalog-impl=org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.arco.uri=https://api.arco.example.com/iceberg
spark.sql.catalog.arco.credential=<oauth-token>
```

**Trino:**
```
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://api.arco.example.com/iceberg
iceberg.rest-catalog.vended-credentials-enabled=true
```
Compatibility note: validate vended-credential behavior against target Trino versions; if unsupported, require baseline storage access or plan for remote-signing.

**Flink:**
```yaml
catalog-type: rest
uri: https://api.arco.example.com/iceberg
credential: <oauth-token>
```

### 9.5 Compatibility Checklist

1. `/v1/config` returns `idempotency-key-lifetime`, `endpoints`, and `namespace-separator`
2. Namespace decoding supports configured separator and `%1F`
3. Idempotency: UUIDv7 validation, finalize deterministic 4xx, do not finalize 5xx
4. Delegated access: honor `X-Iceberg-Access-Delegation`; return `storage-credentials` in `loadTable`
5. Engine validation matrix: DataFusion 40.x (primary), Spark 3.5.x, PyIceberg 0.7.x, Trino 450.x, Flink 1.18.x

### 9.6 Lakekeeper Parity Scope

**Phase A/B parity target:** Iceberg REST spec compliance plus delegated access (`X-Iceberg-Access-Delegation`), vended credentials, and change events (Arco receipts + compaction).

**Explicitly out of scope for this plan:** Lakekeeper's project/warehouse management UI, OpenFGA-based fine-grained auth, change approval hooks, and external event backends. These can be addressed in follow-on ADRs if required.

### 9.7 Future Considerations

- Multi-table transactions (v2 - requires coordination service)
- Server-side scan planning endpoints
- Views support

---

## 10. Implementation Phases

### Phase A: Read-Only + Config
- Implement `/v1/config` with explicit `endpoints` list
- Advertise and honor `namespace-separator`
- Implement read endpoints: list_namespaces, list_tables, load_table
- Add ETag/If-None-Match support for load_table
- Implement credential vending (including `X-Iceberg-Access-Delegation`)
- Deploy as `/iceberg` route

### Phase B: Writes with Correctness
- Implement durable idempotency markers (two-phase)
- Add Failed markers + stale takeover handling
- Apply idempotency to create/drop/rename mutations
- Implement pointer CAS
- Enforce conditional metadata writes (DoesNotExist + hash check)
- Implement immutable event receipts with deterministic `commit_key`
- Implement `commit_table` endpoint
- Add `IcebergCommitCommitted` to CatalogEvent

### Phase C: Projections + Reconciliation
- Schema projection from Iceberg metadata
- Metadata-log-driven reconciler for committed receipts (no listing)
- Metadata GC integration
- Idempotency marker GC

### Phase D: Servo Integration
- Servo uses same commit path for Iceberg tables
- Unified lineage tracking

---

## References

1. [Apache Iceberg REST Catalog OpenAPI Spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
2. [LakeKeeper - Rust Iceberg REST Catalog](https://github.com/lakekeeper/lakekeeper)
3. [RFC-001: Arco Serverless Catalog](./RFC_001_ARCO_SERVERLESS_CATALOG.md)
4. [ADR-018: Tier-1 Write Path Architecture](../adr/adr-018-tier1-write-path.md)
