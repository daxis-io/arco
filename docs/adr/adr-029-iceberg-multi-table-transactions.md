# ADR-029: Iceberg Multi-Table Transactions (ICE-7)

## Status

Accepted

## Context

The Iceberg REST Catalog specification includes `POST /v1/{prefix}/transactions/commit`
(ICE-7) for atomic multi-table commits. Arco currently rejects requests with more than
one table change with HTTP 406, limiting clients that rely on transactional semantics
(e.g., Spark branching workflows, coordinated schema changes across related tables).

The core challenge is that Arco uses object storage (GCS/S3) which provides no native
transactions. Each Iceberg table has its own pointer file with compare-and-swap (CAS)
semantics, but there is no way to atomically update multiple pointers.

### Requirements

1. **Atomic visibility**: All tables in a transaction become visible together or none do
2. **All-or-nothing under crash/retry**: Partial commits must never be visible
3. **Idempotency**: Same `Idempotency-Key` (UUIDv7) returns cached response
4. **Overlapping transaction handling**: Deterministic behavior when two transactions touch the same table
5. **Limit**: `MAX_TABLES_PER_TRANSACTION = 10` (configurable), designed for 100

### Existing Infrastructure

- `IcebergTablePointer` (v1): Per-table pointer with `current_metadata_location`, updated via CAS
- `IdempotencyMarker`: InProgress/Committed/Failed states for exactly-once single-table commits
- `CommitService`: 12-step single-table commit flow with crash recovery

## Decision

**Introduce a TransactionRecord as the single atomic commit point for multi-table transactions.**

The protocol follows a prepare-commit-finalize pattern where:
- **Prepare phase** writes metadata and marks pointers with `pending` state
- **Commit phase** writes a single TransactionRecord as the atomic visibility gate
- **Finalize phase** (best-effort) clears pending state from pointers

### New Types

#### TransactionRecord

```rust
pub struct TransactionRecord {
    pub tx_id: String,           // = Idempotency-Key (UUIDv7)
    pub request_hash: String,    // SHA256 of canonical JSON request
    pub status: TransactionStatus,
    pub tables: Vec<TableEntry>,
    pub started_at: DateTime<Utc>,
    pub committed_at: Option<DateTime<Utc>>,
    pub aborted_at: Option<DateTime<Utc>>,
}

pub enum TransactionStatus {
    Preparing,
    Committed,
    Aborted,
}

pub struct TableEntry {
    pub table_uuid: Uuid,
    pub namespace: String,
    pub table_name: String,
    pub base_pointer_version: String,
    pub new_metadata_location: String,
    pub new_snapshot_id: Option<i64>,
    pub new_last_sequence_number: i64,
}
```

Storage path: `_catalog/iceberg_transactions/{tx_id}.json`

#### PendingPointerUpdate (added to IcebergTablePointer v2)

```rust
pub struct PendingPointerUpdate {
    pub tx_id: String,
    pub metadata_location: String,
    pub snapshot_id: Option<i64>,
    pub last_sequence_number: i64,
    pub prepared_at: DateTime<Utc>,
}
```

### Protocol Flow

```
1. VALIDATE
   - Parse request, validate Idempotency-Key is UUIDv7
   - Enforce MAX_TABLES_PER_TRANSACTION limit (400 if exceeded)
   - Compute request_hash from table-changes array (order-sensitive)
   - Note: Clients must submit tables in consistent order for idempotent retries

2. IDEMPOTENCY CHECK
   - Load TransactionRecord by tx_id
   - If exists with matching request_hash:
     - Preparing: return 503 RetryAfter
     - Committed: return 204 (cached success)
     - Aborted: return cached error
   - If exists with different request_hash: return 409 Conflict

3. CREATE TRANSACTION RECORD (Preparing)
   - Write TransactionRecord with status=Preparing (DoesNotExist)
   - This anchors idempotency and enables safe abort

4. RESOLVE TABLES
   - For each table: resolve identifier -> UUID, validate table exists
   - Sort by table_uuid to ensure deterministic lock ordering

5. PREPARE PHASE (per table, sorted order)
   For each table:
   a. Load pointer + version
   b. If pointer.pending exists for another tx_id:
      - If that tx is Committed: finalize it first, then rebase
      - If that tx is Preparing and not stale: abort with 503 RetryAfter
      - If that tx is Preparing and stale: CAS tx to Aborted, clear pending
      - If that tx is Aborted: clear pending
   c. Load base metadata, validate requirements
   d. Compute new metadata (apply updates)
   e. Write new metadata file (DoesNotExist precondition)
   f. CAS pointer to add pending={tx_id, new_metadata_location, ...}

6. COMMIT BARRIER (critical)
   - Re-read each pointer
   - Confirm each still has pending.tx_id == our tx_id
   - Confirm each pending.metadata_location matches expected
   - If any mismatch: ABORT (something disturbed prepared state)

7. COMMIT
   - CAS TransactionRecord: Preparing -> Committed
   - This is THE atomic visibility gate

8. FINALIZE (best-effort)
   For each table:
   - CAS pointer: apply pending to current fields, clear pending
   - If CAS fails (concurrent writer), skip (effective resolution handles it)
   - Bounded retries (1-2), then rely on background finalizer
```

### Effective Pointer Resolution

All code paths that read pointers must use "effective" resolution:

```rust
pub async fn effective_metadata_location<S: StorageBackend>(
    pointer: &IcebergTablePointer,
    tx_store: &TransactionStore<S>,
) -> IcebergResult<String> {
    if let Some(pending) = &pointer.pending {
        match tx_store.load(&pending.tx_id).await {
            Ok(Some(tx_record)) if tx_record.status == TransactionStatus::Committed => {
                // Transaction committed - pending IS the current state
                return Ok(pending.metadata_location.clone());
            }
            Ok(Some(_)) | Ok(None) => {
                // Preparing/Aborted/Missing - ignore pending
            }
            Err(e) if is_transient_error(&e) => {
                // Transient error - MUST NOT fall back to current
                return Err(e);
            }
            Err(_) => {
                // Non-transient (e.g., parse error) - ignore pending
            }
        }
    }
    Ok(pointer.current_metadata_location.clone())
}
```

**Critical**: Only 404/not-found means "not committed". Transient errors (timeout, 5xx)
must NOT fall back to `current_metadata_location` as this could violate atomicity.

### Pointer Schema Versioning

Pointer version strategy:

- `CURRENT_VERSION = 1`: Default version for new/updated pointers
- `MAX_READABLE_VERSION = 2`: Maximum version this binary can read
- v2 adds: `pending: Option<PendingPointerUpdate>` with `#[serde(default)]`

Version behavior:

- Old binaries (MAX_READABLE=1) REJECT v2 pointers (prevents silent data loss)
- New binaries (MAX_READABLE=2) can read v1 and v2 pointers
- Single-table commits preserve existing pointer version
- Multi-table commits upgrade to v2 via `with_pending()`

Safe rollout:

1. Phase 1: Deploy new binaries (reads v1+v2, writes v1). Multi-table tx disabled.
2. Phase 2: Enable `allow_multi_table_transactions`. v2 pointers written only when tx uses pending.

### Single-Table Commit Integration

When `CommitService::commit_table` encounters a pointer with `pending`:

1. Load the referenced TransactionRecord
2. If Committed: finalize pointer first (apply pending -> current), then proceed
3. If Preparing and not stale: return 503 RetryAfter
4. If Preparing and stale (> timeout): attempt to CAS tx to Aborted, then proceed
5. If Aborted: clear pending, then proceed

This ensures single-table and multi-table commits coexist correctly.

### Failure Matrix

| Failure Point | Tables State | Visibility | Recovery |
|---------------|--------------|------------|----------|
| Crash before any prepare | No pending, no tx record | None visible | Clean |
| Crash after N prepares, before commit | N pointers have pending, tx=Preparing | None visible | Stale pending cleared by timeout |
| Crash after commit, before finalize | All pending, tx=Committed | All visible (via effective resolution) | Background finalizer clears pending |
| Finalize CAS conflict | Mixed pending/current | All visible (via effective resolution) | Background finalizer converges |
| Abort after partial prepare | Some pending, tx=Aborted | None visible | Best-effort cleanup + timeout |

### HTTP Semantics

| Scenario | Response | Retryable |
|----------|----------|-----------|
| Success | 204 No Content | N/A |
| In-progress (same key) | 503 + Retry-After | Yes |
| Conflict (same key, different body) | 409 Conflict | No |
| Overlapping table (other tx active) | 503 + Retry-After | Yes |
| Overlapping table (other tx stale) | Takeover proceeds | N/A |
| Requirement failed | 409 Conflict | No (client must reload) |
| Max tables exceeded | 400 Bad Request | No |

## Consequences

### Positive

- True multi-table atomicity without database transactions
- Builds on existing idempotency and CAS infrastructure
- Readers (load_table) see consistent multi-table state
- Backward compatible with single-table commits
- Crash-safe with well-defined recovery semantics

### Negative

- Added complexity in pointer resolution (all readers must use effective resolution)
- Pointer v2 requires careful deployment (can't mix old/new writers)
- Transaction records add storage objects (GC consideration)
- Prepare phase is O(N) serial per table (sorted order for deadlock avoidance)

### Acceptable Trade-offs

Multi-table transactions are:
- Relatively rare (most commits are single-table)
- Expected to have higher latency (preparing N tables sequentially)
- Limited to 10 tables (with design headroom for 100)

The added complexity is acceptable for the atomicity guarantee.

## Implementation Notes

### Phase 1: True Atomicity (Not Advertised)

1. Add `transactions.rs` with TransactionRecord types and storage
2. Extend `pointer.rs` with PendingPointerUpdate and v2 schema
3. Add effective pointer resolution to all read paths
4. Implement MultiTableTransactionCoordinator
5. Update single-table commit to be transaction-aware
6. Comprehensive tests for atomicity invariants

**Do NOT advertise** `/transactions/commit` in `/v1/config` until Phase 2.

### Phase 2: Feature Flag Advertisement

1. Add `IcebergConfig.allow_multi_table_transactions: bool` (default false)
2. Advertise endpoint in `/v1/config` only when enabled
3. Integration tests for conditional advertisement

### Phase 3: Default-On After Interop

1. Run Spark/Trino/PyIceberg interop tests exercising transactions
2. Flip default to enabled after validation
3. Document in release notes

### Code Paths Requiring Updates

| File | Change |
|------|--------|
| `pointer.rs` | Add PendingPointerUpdate, bump to v2 |
| `transactions.rs` | New module with TransactionRecord, TransactionStore |
| `paths.rs` | Add `iceberg_transaction_record_path()` |
| `routes/tables.rs` | Use effective pointer resolution in load_table |
| `commit.rs` | Transaction-aware single-table commit |
| `reconciler.rs` | Use effective pointer resolution |
| `gc.rs` | Use effective pointer resolution, consider tx record retention |
| `routes/catalog.rs` | Route multi-table requests to coordinator |

### Transaction Record Retention

- Minimum retention: 30 days (significantly longer than pending TTL)
- GC should check no pointers reference the tx_id before deletion
- UUIDv7 enables efficient date-based cleanup (consider date prefix in path)
