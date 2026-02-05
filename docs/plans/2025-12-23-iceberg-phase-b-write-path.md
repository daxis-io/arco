# Iceberg REST Catalog Phase B: Write Path Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the Iceberg REST Catalog write path with CAS-based commits, durable idempotency markers, and event receipts per design doc Section 5.

**Architecture:**
- `IdempotencyMarker` provides exactly-once semantics via two-phase durable markers
- `commit_table` endpoint follows the 12-step flow from design doc
- Governance guardrails prevent authority drift (reject `SetLocationUpdate`, `arco.*` properties)
- Event receipts (pending/committed) enable reconciliation and lineage tracking

**Tech Stack:** Rust 1.85+, axum, serde, sha2, ulid, chrono, arco-core (StorageBackend, WritePrecondition)

---

## Task 1: ObjectVersion Newtype

**Files:**
- Modify: `crates/arco-iceberg/src/pointer.rs`
- Test: Unit tests in same file

**Step 1: Write the failing test**

Add at the bottom of the `#[cfg(test)] mod tests` block in `pointer.rs`:

```rust
#[test]
fn test_object_version_newtype() {
    let version = ObjectVersion::new("12345");
    assert_eq!(version.as_str(), "12345");

    // Test From<String>
    let version: ObjectVersion = "67890".to_string().into();
    assert_eq!(version.as_str(), "67890");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-iceberg test_object_version_newtype`
Expected: FAIL with "cannot find type `ObjectVersion`"

**Step 3: Write minimal implementation**

Add above `IcebergTablePointer` struct in `pointer.rs`:

```rust
/// Opaque version token for CAS operations - portable across GCS/S3/ADLS.
///
/// - GCS: generation number as string
/// - S3: ETag string
/// - ADLS: ETag string
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectVersion(String);

impl ObjectVersion {
    /// Creates a new object version from a string.
    #[must_use]
    pub fn new(version: impl Into<String>) -> Self {
        Self(version.into())
    }

    /// Returns the version as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for ObjectVersion {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl AsRef<str> for ObjectVersion {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-iceberg test_object_version_newtype`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-iceberg/src/pointer.rs
git commit -m "feat(iceberg): add ObjectVersion newtype for portable CAS"
```

---

## Task 2: CommitKey Newtype

**Files:**
- Create: `crates/arco-iceberg/src/types/commit.rs`
- Modify: `crates/arco-iceberg/src/types/mod.rs`

**Step 1: Write the failing test**

Create `crates/arco-iceberg/src/types/commit.rs` with:

```rust
//! Commit-related types for the Iceberg REST write path.

use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};

/// Deterministic commit key derived from metadata location.
///
/// `commit_key = SHA256(metadata_location)` (hex encoded).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommitKey(String);

impl CommitKey {
    /// Creates a commit key from a metadata location.
    #[must_use]
    pub fn from_metadata_location(location: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(location.as_bytes());
        let hash = hasher.finalize();
        Self(hex::encode(hash))
    }

    /// Returns the commit key as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for CommitKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_key_deterministic() {
        let location = "gs://bucket/table/metadata/00001-abc.metadata.json";
        let key1 = CommitKey::from_metadata_location(location);
        let key2 = CommitKey::from_metadata_location(location);
        assert_eq!(key1, key2);
        assert_eq!(key1.as_str().len(), 64); // SHA256 hex is 64 chars
    }

    #[test]
    fn test_commit_key_different_locations() {
        let key1 = CommitKey::from_metadata_location("location1");
        let key2 = CommitKey::from_metadata_location("location2");
        assert_ne!(key1, key2);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-iceberg test_commit_key`
Expected: FAIL with "cannot find module `commit`" or hex dependency missing

**Step 3: Add hex dependency to Cargo.toml**

In `crates/arco-iceberg/Cargo.toml`, add under `[dependencies]`:

```toml
hex = "0.4"
```

**Step 4: Export the module**

In `crates/arco-iceberg/src/types/mod.rs`, add:

```rust
mod commit;

pub use commit::*;
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p arco-iceberg test_commit_key`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/arco-iceberg/Cargo.toml crates/arco-iceberg/src/types/commit.rs crates/arco-iceberg/src/types/mod.rs
git commit -m "feat(iceberg): add CommitKey newtype for deterministic commit identification"
```

---

## Task 3: IdempotencyMarker Core Type

**Files:**
- Create: `crates/arco-iceberg/src/idempotency.rs`
- Modify: `crates/arco-iceberg/src/lib.rs`

**Step 1: Write the type definition and tests**

Create `crates/arco-iceberg/src/idempotency.rs`:

```rust
//! Durable idempotency markers for exactly-once commit semantics.
//!
//! See design doc Section 3 for the two-phase marker protocol.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;
use uuid::Uuid;

use crate::error::IcebergErrorResponse;

/// Status of an idempotency marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IdempotencyStatus {
    /// Commit is in progress - marker claimed, not yet finalized.
    InProgress,
    /// Commit succeeded - response cached for replay.
    Committed,
    /// Commit failed with a terminal error - error cached for replay.
    Failed,
}

/// Durable idempotency marker for commit deduplication.
///
/// Path: `_catalog/iceberg_idempotency/{table_uuid}/{hash_prefix}/{idempotency_key_hash}.json`
///
/// The marker goes through these states:
/// 1. `InProgress` - Claimed before starting commit work
/// 2. `Committed` - Finalized after successful pointer CAS
/// 3. `Failed` - Finalized after terminal 4xx error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyMarker {
    /// Current status of this marker.
    pub status: IdempotencyStatus,

    /// The raw idempotency key from the request header.
    pub idempotency_key: String,

    /// SHA256 hash of the idempotency key (used in path).
    pub idempotency_key_hash: String,

    /// The table this commit targets.
    pub table_uuid: Uuid,

    /// SHA256 hash of the canonical request body (RFC 8785 JCS).
    pub request_hash: String,

    /// When this marker was created/claimed.
    pub started_at: DateTime<Utc>,

    /// When the commit was finalized (success or failure).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_at: Option<DateTime<Utc>>,

    /// When the commit failed (only set for Failed status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_at: Option<DateTime<Utc>>,

    /// HTTP status code of the failure (only set for Failed status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_http_status: Option<u16>,

    /// Cached error response (only set for Failed status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_payload: Option<IcebergErrorResponse>,

    /// Event ID allocated at claim time (deterministic for crash recovery).
    pub event_id: Ulid,

    /// Pointer's metadata location when marker was claimed.
    pub base_metadata_location: String,

    /// Deterministic new metadata location for this commit.
    pub metadata_location: String,

    /// Cached success response (only set for Committed status).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_metadata_location: Option<String>,
}

impl IdempotencyMarker {
    /// Computes the SHA256 hash of an idempotency key.
    #[must_use]
    pub fn hash_key(key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Returns the storage path for this marker.
    ///
    /// Path: `_catalog/iceberg_idempotency/{table_uuid}/{hash_prefix}/{key_hash}.json`
    #[must_use]
    pub fn storage_path(table_uuid: &Uuid, idempotency_key_hash: &str) -> String {
        let prefix = &idempotency_key_hash[..2.min(idempotency_key_hash.len())];
        format!(
            "_catalog/iceberg_idempotency/{table_uuid}/{prefix}/{idempotency_key_hash}.json"
        )
    }

    /// Creates a new in-progress marker.
    #[must_use]
    pub fn new_in_progress(
        idempotency_key: String,
        table_uuid: Uuid,
        request_hash: String,
        base_metadata_location: String,
        metadata_location: String,
    ) -> Self {
        let idempotency_key_hash = Self::hash_key(&idempotency_key);
        Self {
            status: IdempotencyStatus::InProgress,
            idempotency_key,
            idempotency_key_hash,
            table_uuid,
            request_hash,
            started_at: Utc::now(),
            committed_at: None,
            failed_at: None,
            error_http_status: None,
            error_payload: None,
            event_id: Ulid::new(),
            base_metadata_location,
            metadata_location,
            response_metadata_location: None,
        }
    }

    /// Finalizes the marker as committed.
    #[must_use]
    pub fn finalize_committed(mut self, response_metadata_location: String) -> Self {
        self.status = IdempotencyStatus::Committed;
        self.committed_at = Some(Utc::now());
        self.response_metadata_location = Some(response_metadata_location);
        self
    }

    /// Finalizes the marker as failed.
    #[must_use]
    pub fn finalize_failed(mut self, http_status: u16, error: IcebergErrorResponse) -> Self {
        self.status = IdempotencyStatus::Failed;
        self.failed_at = Some(Utc::now());
        self.error_http_status = Some(http_status);
        self.error_payload = Some(error);
        self
    }

    /// Returns whether this marker can be taken over (stale in-progress).
    #[must_use]
    pub fn is_stale(&self, timeout: chrono::Duration) -> bool {
        self.status == IdempotencyStatus::InProgress
            && self.started_at + timeout < Utc::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_key_deterministic() {
        let key = "01924a7c-8d9f-7000-8000-000000000001";
        let hash1 = IdempotencyMarker::hash_key(key);
        let hash2 = IdempotencyMarker::hash_key(key);
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 64);
    }

    #[test]
    fn test_storage_path_format() {
        let table_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let key_hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let path = IdempotencyMarker::storage_path(&table_uuid, key_hash);
        assert_eq!(
            path,
            "_catalog/iceberg_idempotency/550e8400-e29b-41d4-a716-446655440000/ab/abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890.json"
        );
    }

    #[test]
    fn test_marker_state_transitions() {
        let marker = IdempotencyMarker::new_in_progress(
            "01924a7c-8d9f-7000-8000-000000000001".to_string(),
            Uuid::new_v4(),
            "request_hash".to_string(),
            "base/metadata.json".to_string(),
            "new/metadata.json".to_string(),
        );
        assert_eq!(marker.status, IdempotencyStatus::InProgress);
        assert!(marker.committed_at.is_none());

        let committed = marker.finalize_committed("new/metadata.json".to_string());
        assert_eq!(committed.status, IdempotencyStatus::Committed);
        assert!(committed.committed_at.is_some());
    }

    #[test]
    fn test_marker_serialization_roundtrip() {
        let marker = IdempotencyMarker::new_in_progress(
            "key".to_string(),
            Uuid::new_v4(),
            "hash".to_string(),
            "base.json".to_string(),
            "new.json".to_string(),
        );
        let json = serde_json::to_string(&marker).expect("serialize");
        let parsed: IdempotencyMarker = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(marker.idempotency_key, parsed.idempotency_key);
        assert_eq!(marker.status, parsed.status);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-iceberg idempotency`
Expected: FAIL with "cannot find module `idempotency`"

**Step 3: Export the module**

In `crates/arco-iceberg/src/lib.rs`, add:

```rust
pub mod idempotency;
```

And add to prelude:

```rust
pub use crate::idempotency::{IdempotencyMarker, IdempotencyStatus};
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-iceberg idempotency`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-iceberg/src/idempotency.rs crates/arco-iceberg/src/lib.rs
git commit -m "feat(iceberg): add IdempotencyMarker with state machine"
```

---

## Task 4: UUIDv7 Validation for Idempotency Keys

**Files:**
- Modify: `crates/arco-iceberg/src/idempotency.rs`

**Step 1: Write the failing test**

Add to `idempotency.rs` tests:

```rust
#[test]
fn test_validate_uuidv7_valid() {
    // Valid UUIDv7 (version nibble = 7, variant = 10xx)
    let key = "01924a7c-8d9f-7000-8000-000000000001";
    assert!(IdempotencyMarker::validate_uuidv7(key).is_ok());
}

#[test]
fn test_validate_uuidv7_invalid_version() {
    // UUIDv4 (version nibble = 4)
    let key = "550e8400-e29b-41d4-a716-446655440000";
    assert!(IdempotencyMarker::validate_uuidv7(key).is_err());
}

#[test]
fn test_validate_uuidv7_invalid_format() {
    let key = "not-a-uuid";
    assert!(IdempotencyMarker::validate_uuidv7(key).is_err());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-iceberg test_validate_uuidv7`
Expected: FAIL with "no method named `validate_uuidv7`"

**Step 3: Write minimal implementation**

Add to `IdempotencyMarker` impl:

```rust
/// Validates that an idempotency key is a valid UUIDv7.
///
/// Per design doc: Idempotency-Key must be UUIDv7 (RFC 9562) in canonical string form.
///
/// # Errors
///
/// Returns an error if the key is not a valid UUIDv7.
pub fn validate_uuidv7(key: &str) -> Result<Uuid, IdempotencyKeyError> {
    let uuid = Uuid::parse_str(key).map_err(|_| IdempotencyKeyError::InvalidFormat)?;

    // UUIDv7 has version nibble = 7 (bits 48-51)
    let version = uuid.get_version_num();
    if version != 7 {
        return Err(IdempotencyKeyError::NotUuidV7 { found_version: version });
    }

    Ok(uuid)
}
```

Add error type above the impl:

```rust
/// Error validating an idempotency key.
#[derive(Debug, thiserror::Error)]
pub enum IdempotencyKeyError {
    /// Key is not a valid UUID.
    #[error("Idempotency-Key must be a valid UUID")]
    InvalidFormat,

    /// Key is not UUIDv7.
    #[error("Idempotency-Key must be UUIDv7 (RFC 9562), found version {found_version}")]
    NotUuidV7 {
        /// The version number found.
        found_version: usize,
    },
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-iceberg test_validate_uuidv7`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-iceberg/src/idempotency.rs
git commit -m "feat(iceberg): add UUIDv7 validation for idempotency keys"
```

---

## Task 5: Canonical Request Hashing (RFC 8785 JCS)

**Files:**
- Modify: `crates/arco-iceberg/src/idempotency.rs`
- Modify: `crates/arco-iceberg/Cargo.toml`

**Step 1: Write the failing test**

Add to `idempotency.rs` tests:

```rust
#[test]
fn test_canonical_request_hash_deterministic() {
    let json1 = r#"{"b": 2, "a": 1}"#;
    let json2 = r#"{"a": 1, "b": 2}"#;

    let hash1 = IdempotencyMarker::hash_request(json1).expect("hash1");
    let hash2 = IdempotencyMarker::hash_request(json2).expect("hash2");

    // Same data in different key order should produce same hash (JCS)
    assert_eq!(hash1, hash2);
}

#[test]
fn test_canonical_request_hash_different_values() {
    let json1 = r#"{"a": 1}"#;
    let json2 = r#"{"a": 2}"#;

    let hash1 = IdempotencyMarker::hash_request(json1).expect("hash1");
    let hash2 = IdempotencyMarker::hash_request(json2).expect("hash2");

    assert_ne!(hash1, hash2);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-iceberg test_canonical_request_hash`
Expected: FAIL with "no method named `hash_request`"

**Step 3: Add json-canon dependency**

In `crates/arco-iceberg/Cargo.toml`, add:

```toml
json-canon = "0.1"
```

**Step 4: Write minimal implementation**

Add to `IdempotencyMarker` impl:

```rust
/// Computes the SHA256 hash of a request body using RFC 8785 JCS canonicalization.
///
/// # Errors
///
/// Returns an error if the JSON cannot be parsed or canonicalized.
pub fn hash_request(json_body: &str) -> Result<String, RequestHashError> {
    // Parse the JSON
    let value: serde_json::Value = serde_json::from_str(json_body)
        .map_err(|e| RequestHashError::InvalidJson(e.to_string()))?;

    // Canonicalize using RFC 8785 JCS
    let canonical = json_canon::to_string(&value)
        .map_err(|e| RequestHashError::CanonicalizeError(e.to_string()))?;

    // Hash the canonical form
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}
```

Add error type:

```rust
/// Error computing request hash.
#[derive(Debug, thiserror::Error)]
pub enum RequestHashError {
    /// JSON parsing failed.
    #[error("Invalid JSON: {0}")]
    InvalidJson(String),

    /// Canonicalization failed.
    #[error("Failed to canonicalize JSON: {0}")]
    CanonicalizeError(String),
}
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p arco-iceberg test_canonical_request_hash`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/arco-iceberg/Cargo.toml crates/arco-iceberg/src/idempotency.rs
git commit -m "feat(iceberg): add RFC 8785 JCS canonical request hashing"
```

---

## Task 6: Event Receipt Types

**Files:**
- Create: `crates/arco-iceberg/src/events.rs`
- Modify: `crates/arco-iceberg/src/lib.rs`

**Step 1: Write the failing test**

Create `crates/arco-iceberg/src/events.rs`:

```rust
//! Iceberg commit event receipts for reconciliation and lineage.
//!
//! Two immutable files per commit attempt:
//! - `events/YYYY-MM-DD/iceberg/pending/{commit_key}.json` - before pointer CAS
//! - `events/YYYY-MM-DD/iceberg/committed/{commit_key}.json` - after pointer CAS

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use uuid::Uuid;

use crate::pointer::UpdateSource;
use crate::types::CommitKey;

/// Pending commit receipt - written before pointer CAS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingReceipt {
    /// Table this commit targets.
    pub table_uuid: Uuid,
    /// Deterministic commit key.
    pub commit_key: CommitKey,
    /// Event ID for correlation.
    pub event_id: Ulid,
    /// New metadata file location.
    pub metadata_location: String,
    /// Base metadata location when commit started.
    pub base_metadata_location: String,
    /// Expected snapshot ID after commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    /// Source of this commit.
    pub source: UpdateSource,
    /// When the commit was started.
    pub started_at: DateTime<Utc>,
}

impl PendingReceipt {
    /// Returns the storage path for this receipt.
    ///
    /// Path: `events/YYYY-MM-DD/iceberg/pending/{commit_key}.json`
    #[must_use]
    pub fn storage_path(date: NaiveDate, commit_key: &CommitKey) -> String {
        format!("events/{}/iceberg/pending/{}.json", date.format("%Y-%m-%d"), commit_key)
    }
}

/// Committed commit receipt - written after pointer CAS succeeds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedReceipt {
    /// Table this commit targeted.
    pub table_uuid: Uuid,
    /// Deterministic commit key.
    pub commit_key: CommitKey,
    /// Event ID for correlation.
    pub event_id: Ulid,
    /// New metadata file location.
    pub metadata_location: String,
    /// Snapshot ID after commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    /// Previous metadata location (for history).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_metadata_location: Option<String>,
    /// Source of this commit.
    pub source: UpdateSource,
    /// When the commit was confirmed.
    pub committed_at: DateTime<Utc>,
}

impl CommittedReceipt {
    /// Returns the storage path for this receipt.
    ///
    /// Path: `events/YYYY-MM-DD/iceberg/committed/{commit_key}.json`
    #[must_use]
    pub fn storage_path(date: NaiveDate, commit_key: &CommitKey) -> String {
        format!("events/{}/iceberg/committed/{}.json", date.format("%Y-%m-%d"), commit_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_receipt_path() {
        let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        let commit_key = CommitKey::from_metadata_location("test/metadata.json");
        let path = PendingReceipt::storage_path(date, &commit_key);
        assert!(path.starts_with("events/2025-01-15/iceberg/pending/"));
        assert!(path.ends_with(".json"));
    }

    #[test]
    fn test_committed_receipt_path() {
        let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        let commit_key = CommitKey::from_metadata_location("test/metadata.json");
        let path = CommittedReceipt::storage_path(date, &commit_key);
        assert!(path.starts_with("events/2025-01-15/iceberg/committed/"));
        assert!(path.ends_with(".json"));
    }

    #[test]
    fn test_pending_receipt_serialization() {
        let receipt = PendingReceipt {
            table_uuid: Uuid::new_v4(),
            commit_key: CommitKey::from_metadata_location("test.json"),
            event_id: Ulid::new(),
            metadata_location: "new.json".to_string(),
            base_metadata_location: "base.json".to_string(),
            snapshot_id: Some(123),
            source: UpdateSource::IcebergRest {
                client_info: Some("spark/3.5".to_string()),
                principal: None,
            },
            started_at: Utc::now(),
        };

        let json = serde_json::to_string(&receipt).expect("serialize");
        let parsed: PendingReceipt = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(receipt.table_uuid, parsed.table_uuid);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-iceberg events`
Expected: FAIL with "cannot find module `events`"

**Step 3: Export the module**

In `crates/arco-iceberg/src/lib.rs`, add:

```rust
pub mod events;
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-iceberg events`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-iceberg/src/events.rs crates/arco-iceberg/src/lib.rs
git commit -m "feat(iceberg): add event receipt types for reconciliation"
```

---

## Task 7: Iceberg Update Requirement Types

**Files:**
- Modify: `crates/arco-iceberg/src/types/commit.rs`

**Step 1: Write the failing test**

Add to `commit.rs` tests:

```rust
#[test]
fn test_update_requirement_deserialization() {
    let json = r#"{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 123}"#;
    let req: UpdateRequirement = serde_json::from_str(json).expect("deserialize");
    assert!(matches!(req, UpdateRequirement::AssertRefSnapshotId { .. }));
}

#[test]
fn test_assert_table_uuid() {
    let json = r#"{"type": "assert-table-uuid", "uuid": "550e8400-e29b-41d4-a716-446655440000"}"#;
    let req: UpdateRequirement = serde_json::from_str(json).expect("deserialize");
    if let UpdateRequirement::AssertTableUuid { uuid } = req {
        assert_eq!(uuid.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    } else {
        panic!("wrong variant");
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-iceberg test_update_requirement`
Expected: FAIL with "cannot find type `UpdateRequirement`"

**Step 3: Write minimal implementation**

Add to `commit.rs`:

```rust
use uuid::Uuid;

/// Iceberg table update requirement for optimistic concurrency.
///
/// Requirements are checked before applying updates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum UpdateRequirement {
    /// Assert that the table UUID matches.
    AssertTableUuid {
        /// Expected table UUID.
        uuid: Uuid,
    },

    /// Assert that a ref points to a specific snapshot.
    AssertRefSnapshotId {
        /// Reference name (e.g., "main").
        #[serde(rename = "ref")]
        ref_name: String,
        /// Expected snapshot ID (null means ref should not exist).
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },

    /// Assert the last assigned column ID.
    AssertLastAssignedFieldId {
        /// Expected last assigned field ID.
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i32,
    },

    /// Assert the current schema ID.
    AssertCurrentSchemaId {
        /// Expected current schema ID.
        #[serde(rename = "current-schema-id")]
        current_schema_id: i32,
    },

    /// Assert the last assigned partition ID.
    AssertLastAssignedPartitionId {
        /// Expected last assigned partition ID.
        #[serde(rename = "last-assigned-partition-id")]
        last_assigned_partition_id: i32,
    },

    /// Assert the default spec ID.
    AssertDefaultSpecId {
        /// Expected default spec ID.
        #[serde(rename = "default-spec-id")]
        default_spec_id: i32,
    },

    /// Assert the default sort order ID.
    AssertDefaultSortOrderId {
        /// Expected default sort order ID.
        #[serde(rename = "default-sort-order-id")]
        default_sort_order_id: i32,
    },
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-iceberg test_update_requirement`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-iceberg/src/types/commit.rs
git commit -m "feat(iceberg): add UpdateRequirement types for optimistic concurrency"
```

---

## Task 8: Iceberg Table Update Types

**Files:**
- Modify: `crates/arco-iceberg/src/types/commit.rs`

**Step 1: Write the failing test**

Add to `commit.rs` tests:

```rust
#[test]
fn test_table_update_add_snapshot() {
    let json = r#"{
        "action": "add-snapshot",
        "snapshot": {
            "snapshot-id": 12345,
            "timestamp-ms": 1234567890000,
            "manifest-list": "s3://bucket/manifests/snap-12345.avro",
            "summary": {"operation": "append"}
        }
    }"#;
    let update: TableUpdate = serde_json::from_str(json).expect("deserialize");
    assert!(matches!(update, TableUpdate::AddSnapshot { .. }));
}

#[test]
fn test_table_update_set_snapshot_ref() {
    let json = r#"{
        "action": "set-snapshot-ref",
        "ref-name": "main",
        "type": "branch",
        "snapshot-id": 12345
    }"#;
    let update: TableUpdate = serde_json::from_str(json).expect("deserialize");
    if let TableUpdate::SetSnapshotRef { ref_name, ref_type, snapshot_id, .. } = update {
        assert_eq!(ref_name, "main");
        assert_eq!(ref_type, "branch");
        assert_eq!(snapshot_id, 12345);
    } else {
        panic!("wrong variant");
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-iceberg test_table_update`
Expected: FAIL with "cannot find type `TableUpdate`"

**Step 3: Write minimal implementation**

Add to `commit.rs`:

```rust
use crate::types::table::{Schema, Snapshot, PartitionSpec, SortOrder};
use std::collections::HashMap;

/// Iceberg table update action.
///
/// Updates modify table metadata atomically.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    /// Assign a new UUID to the table.
    AssignUuid {
        /// New UUID for the table.
        uuid: Uuid,
    },

    /// Upgrade the format version.
    UpgradeFormatVersion {
        /// Target format version.
        #[serde(rename = "format-version")]
        format_version: i32,
    },

    /// Add a new schema.
    AddSchema {
        /// The schema to add.
        schema: Schema,
        /// ID to assign (optional).
        #[serde(rename = "last-column-id", skip_serializing_if = "Option::is_none")]
        last_column_id: Option<i32>,
    },

    /// Set the current schema.
    SetCurrentSchema {
        /// Schema ID to make current.
        #[serde(rename = "schema-id")]
        schema_id: i32,
    },

    /// Add a new partition spec.
    AddPartitionSpec {
        /// The partition spec to add.
        spec: PartitionSpec,
    },

    /// Set the default partition spec.
    SetDefaultSpec {
        /// Spec ID to make default.
        #[serde(rename = "spec-id")]
        spec_id: i32,
    },

    /// Add a new sort order.
    AddSortOrder {
        /// The sort order to add.
        #[serde(rename = "sort-order")]
        sort_order: SortOrder,
    },

    /// Set the default sort order.
    SetDefaultSortOrder {
        /// Sort order ID to make default.
        #[serde(rename = "sort-order-id")]
        sort_order_id: i32,
    },

    /// Add a new snapshot.
    AddSnapshot {
        /// The snapshot to add.
        snapshot: Snapshot,
    },

    /// Set a snapshot reference (branch or tag).
    SetSnapshotRef {
        /// Reference name.
        #[serde(rename = "ref-name")]
        ref_name: String,
        /// Reference type ("branch" or "tag").
        #[serde(rename = "type")]
        ref_type: String,
        /// Snapshot ID for the ref.
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        /// Max ref age in ms (branches only).
        #[serde(rename = "max-ref-age-ms", skip_serializing_if = "Option::is_none")]
        max_ref_age_ms: Option<i64>,
        /// Max snapshot age in ms (branches only).
        #[serde(rename = "max-snapshot-age-ms", skip_serializing_if = "Option::is_none")]
        max_snapshot_age_ms: Option<i64>,
        /// Min snapshots to keep (branches only).
        #[serde(rename = "min-snapshots-to-keep", skip_serializing_if = "Option::is_none")]
        min_snapshots_to_keep: Option<i32>,
    },

    /// Remove a snapshot reference.
    RemoveSnapshotRef {
        /// Reference name to remove.
        #[serde(rename = "ref-name")]
        ref_name: String,
    },

    /// Remove snapshots by IDs.
    RemoveSnapshots {
        /// Snapshot IDs to remove.
        #[serde(rename = "snapshot-ids")]
        snapshot_ids: Vec<i64>,
    },

    /// Set table location.
    ///
    /// **Note:** Arco rejects this update (400 BadRequest) - Arco owns storage location.
    SetLocation {
        /// New location.
        location: String,
    },

    /// Set table properties.
    SetProperties {
        /// Properties to set.
        updates: HashMap<String, String>,
    },

    /// Remove table properties.
    RemoveProperties {
        /// Property keys to remove.
        removals: Vec<String>,
    },
}

impl TableUpdate {
    /// Returns true if this update is rejected by Arco governance guardrails.
    ///
    /// Per design doc Section 1.3:
    /// - `SetLocation` is always rejected (Arco owns storage location)
    /// - `SetProperties` with `arco.*` keys is rejected (reserved namespace)
    /// - `RemoveProperties` with `arco.*` keys is rejected (reserved namespace)
    #[must_use]
    pub fn is_rejected_by_guardrails(&self) -> Option<&'static str> {
        match self {
            Self::SetLocation { .. } => {
                Some("SetLocationUpdate is rejected: Arco owns storage location")
            }
            Self::SetProperties { updates } => {
                if updates.keys().any(|k| k.starts_with("arco.")) {
                    Some("SetPropertiesUpdate with 'arco.*' keys is rejected: reserved namespace")
                } else {
                    None
                }
            }
            Self::RemoveProperties { removals } => {
                if removals.iter().any(|k| k.starts_with("arco.")) {
                    Some("RemovePropertiesUpdate with 'arco.*' keys is rejected: reserved namespace")
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-iceberg test_table_update`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-iceberg/src/types/commit.rs
git commit -m "feat(iceberg): add TableUpdate types with governance guardrails"
```

---

## Task 9: Governance Guardrail Tests

**Files:**
- Modify: `crates/arco-iceberg/src/types/commit.rs`

**Step 1: Write the failing test**

Add to `commit.rs` tests:

```rust
#[test]
fn test_guardrail_rejects_set_location() {
    let update = TableUpdate::SetLocation {
        location: "s3://new-bucket/table".to_string(),
    };
    assert!(update.is_rejected_by_guardrails().is_some());
}

#[test]
fn test_guardrail_rejects_arco_properties() {
    let update = TableUpdate::SetProperties {
        updates: [("arco.lineage.source".to_string(), "value".to_string())]
            .into_iter()
            .collect(),
    };
    assert!(update.is_rejected_by_guardrails().is_some());
}

#[test]
fn test_guardrail_allows_normal_properties() {
    let update = TableUpdate::SetProperties {
        updates: [("write.format.default".to_string(), "parquet".to_string())]
            .into_iter()
            .collect(),
    };
    assert!(update.is_rejected_by_guardrails().is_none());
}

#[test]
fn test_guardrail_rejects_remove_arco_properties() {
    let update = TableUpdate::RemoveProperties {
        removals: vec!["arco.governance.owner".to_string()],
    };
    assert!(update.is_rejected_by_guardrails().is_some());
}

#[test]
fn test_guardrail_allows_add_snapshot() {
    let update = TableUpdate::AddSnapshot {
        snapshot: Snapshot {
            snapshot_id: 1,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 1234567890000,
            manifest_list: "s3://bucket/manifests/snap.avro".to_string(),
            summary: HashMap::new(),
            schema_id: Some(0),
        },
    };
    assert!(update.is_rejected_by_guardrails().is_none());
}
```

**Step 2: Run test to verify it passes**

Run: `cargo test -p arco-iceberg test_guardrail`
Expected: PASS (tests should pass with existing implementation)

**Step 3: Commit**

```bash
git add crates/arco-iceberg/src/types/commit.rs
git commit -m "test(iceberg): add governance guardrail tests"
```

---

## Task 10: CommitTableRequest and CommitTableResponse Types

**Files:**
- Modify: `crates/arco-iceberg/src/types/commit.rs`

**Step 1: Write the failing test**

Add to `commit.rs` tests:

```rust
#[test]
fn test_commit_table_request_deserialization() {
    let json = r#"{
        "identifier": {"namespace": ["sales"], "name": "orders"},
        "requirements": [
            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 100}
        ],
        "updates": [
            {"action": "add-snapshot", "snapshot": {"snapshot-id": 101, "timestamp-ms": 1234567890000, "manifest-list": "s3://bucket/snap.avro"}}
        ]
    }"#;
    let req: CommitTableRequest = serde_json::from_str(json).expect("deserialize");
    assert_eq!(req.identifier.name, "orders");
    assert_eq!(req.requirements.len(), 1);
    assert_eq!(req.updates.len(), 1);
}

#[test]
fn test_commit_table_response_serialization() {
    let response = CommitTableResponse {
        metadata_location: "s3://bucket/metadata/00001.json".to_string(),
        metadata: serde_json::json!({"format-version": 2}),
    };
    let json = serde_json::to_string(&response).expect("serialize");
    assert!(json.contains("metadata-location"));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-iceberg test_commit_table_request`
Expected: FAIL with "cannot find type `CommitTableRequest`"

**Step 3: Write minimal implementation**

Add to `commit.rs`:

```rust
use crate::types::TableIdent;

/// Request body for `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitTableRequest {
    /// Table identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdent>,

    /// Requirements that must be met before applying updates.
    #[serde(default)]
    pub requirements: Vec<UpdateRequirement>,

    /// Updates to apply atomically.
    #[serde(default)]
    pub updates: Vec<TableUpdate>,
}

impl CommitTableRequest {
    /// Checks all updates against governance guardrails.
    ///
    /// Returns the first rejection reason if any update is rejected.
    #[must_use]
    pub fn check_guardrails(&self) -> Option<&'static str> {
        self.updates.iter().find_map(TableUpdate::is_rejected_by_guardrails)
    }
}

/// Response from `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitTableResponse {
    /// Location of the new metadata file.
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,

    /// Full table metadata (inline JSON).
    pub metadata: serde_json::Value,
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-iceberg test_commit_table`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-iceberg/src/types/commit.rs
git commit -m "feat(iceberg): add CommitTableRequest and CommitTableResponse types"
```

---

## Task 11: PointerStore Trait for CAS Operations

**Files:**
- Modify: `crates/arco-iceberg/src/pointer.rs`

**Step 1: Write the failing test**

Add to `pointer.rs` tests:

```rust
use crate::pointer::{CasResult, PointerStore};
use arco_core::storage::MemoryBackend;
use std::sync::Arc;

#[tokio::test]
async fn test_pointer_store_create() {
    let storage = Arc::new(MemoryBackend::new());
    let store = PointerStoreImpl::new(storage);

    let table_uuid = Uuid::new_v4();
    let pointer = IcebergTablePointer::new(
        table_uuid,
        "gs://bucket/metadata/00000.json".to_string(),
    );

    let result = store.create(&table_uuid, &pointer).await.expect("create");
    assert!(matches!(result, CasResult::Success { .. }));
}

#[tokio::test]
async fn test_pointer_store_cas_conflict() {
    let storage = Arc::new(MemoryBackend::new());
    let store = PointerStoreImpl::new(storage);

    let table_uuid = Uuid::new_v4();
    let pointer = IcebergTablePointer::new(
        table_uuid,
        "gs://bucket/metadata/00000.json".to_string(),
    );

    // Create initial pointer
    let result = store.create(&table_uuid, &pointer).await.expect("create");
    let CasResult::Success { new_version } = result else {
        panic!("expected success");
    };

    // Update with correct version succeeds
    let mut updated = pointer.clone();
    updated.current_metadata_location = "gs://bucket/metadata/00001.json".to_string();
    let result = store.compare_and_swap(&table_uuid, &new_version, &updated).await.expect("cas");
    assert!(matches!(result, CasResult::Success { .. }));

    // Update with stale version fails
    let result = store.compare_and_swap(&table_uuid, &new_version, &updated).await.expect("cas");
    assert!(matches!(result, CasResult::Conflict { .. }));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-iceberg test_pointer_store`
Expected: FAIL with "cannot find type `PointerStore`"

**Step 3: Write minimal implementation**

Add to `pointer.rs`:

```rust
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};

use crate::error::{IcebergError, IcebergResult};

/// Result of a CAS operation on a pointer.
#[derive(Debug, Clone)]
pub enum CasResult {
    /// CAS succeeded.
    Success {
        /// New version after the write.
        new_version: ObjectVersion,
    },
    /// CAS failed due to version mismatch.
    Conflict {
        /// Current version that caused the conflict.
        current_version: ObjectVersion,
    },
}

/// Trait for pointer storage operations.
#[async_trait]
pub trait PointerStore: Send + Sync {
    /// Loads a pointer and its version.
    async fn load(&self, table_uuid: &Uuid) -> IcebergResult<Option<(IcebergTablePointer, ObjectVersion)>>;

    /// Creates a new pointer (fails if already exists).
    async fn create(&self, table_uuid: &Uuid, pointer: &IcebergTablePointer) -> IcebergResult<CasResult>;

    /// Updates a pointer with CAS.
    async fn compare_and_swap(
        &self,
        table_uuid: &Uuid,
        expected_version: &ObjectVersion,
        new_pointer: &IcebergTablePointer,
    ) -> IcebergResult<CasResult>;

    /// Deletes a pointer with version check.
    async fn delete(&self, table_uuid: &Uuid, expected_version: &ObjectVersion) -> IcebergResult<CasResult>;
}

/// Implementation of `PointerStore` using `StorageBackend`.
pub struct PointerStoreImpl {
    storage: Arc<dyn StorageBackend>,
}

impl PointerStoreImpl {
    /// Creates a new pointer store.
    #[must_use]
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl PointerStore for PointerStoreImpl {
    async fn load(&self, table_uuid: &Uuid) -> IcebergResult<Option<(IcebergTablePointer, ObjectVersion)>> {
        let path = IcebergTablePointer::storage_path(table_uuid);

        let meta = self.storage.head(&path).await.map_err(|e| IcebergError::Internal {
            message: format!("Failed to check pointer existence: {e}"),
        })?;

        let Some(meta) = meta else {
            return Ok(None);
        };

        let bytes = self.storage.get(&path).await.map_err(|e| IcebergError::Internal {
            message: format!("Failed to read pointer: {e}"),
        })?;

        let pointer: IcebergTablePointer = serde_json::from_slice(&bytes)
            .map_err(|e| IcebergError::Internal {
                message: format!("Failed to parse pointer: {e}"),
            })?;

        Ok(Some((pointer, ObjectVersion::new(meta.version))))
    }

    async fn create(&self, table_uuid: &Uuid, pointer: &IcebergTablePointer) -> IcebergResult<CasResult> {
        let path = IcebergTablePointer::storage_path(table_uuid);
        let bytes = serde_json::to_vec(pointer).map_err(|e| IcebergError::Internal {
            message: format!("Failed to serialize pointer: {e}"),
        })?;

        match self.storage.put(&path, Bytes::from(bytes), WritePrecondition::DoesNotExist).await {
            Ok(WriteResult::Success { version }) => {
                Ok(CasResult::Success { new_version: ObjectVersion::new(version) })
            }
            Ok(WriteResult::PreconditionFailed { current_version }) => {
                Ok(CasResult::Conflict { current_version: ObjectVersion::new(current_version) })
            }
            Err(e) => Err(IcebergError::Internal {
                message: format!("Failed to create pointer: {e}"),
            }),
        }
    }

    async fn compare_and_swap(
        &self,
        table_uuid: &Uuid,
        expected_version: &ObjectVersion,
        new_pointer: &IcebergTablePointer,
    ) -> IcebergResult<CasResult> {
        let path = IcebergTablePointer::storage_path(table_uuid);
        let bytes = serde_json::to_vec(new_pointer).map_err(|e| IcebergError::Internal {
            message: format!("Failed to serialize pointer: {e}"),
        })?;

        let precondition = WritePrecondition::MatchesVersion(expected_version.0.clone());

        match self.storage.put(&path, Bytes::from(bytes), precondition).await {
            Ok(WriteResult::Success { version }) => {
                Ok(CasResult::Success { new_version: ObjectVersion::new(version) })
            }
            Ok(WriteResult::PreconditionFailed { current_version }) => {
                Ok(CasResult::Conflict { current_version: ObjectVersion::new(current_version) })
            }
            Err(e) => Err(IcebergError::Internal {
                message: format!("Failed to CAS pointer: {e}"),
            }),
        }
    }

    async fn delete(&self, table_uuid: &Uuid, expected_version: &ObjectVersion) -> IcebergResult<CasResult> {
        // Note: Most object stores don't support conditional delete.
        // For now, we read-check-delete which has a race window.
        // A proper implementation would use versioned delete if available.
        let path = IcebergTablePointer::storage_path(table_uuid);

        let meta = self.storage.head(&path).await.map_err(|e| IcebergError::Internal {
            message: format!("Failed to check pointer: {e}"),
        })?;

        let Some(meta) = meta else {
            return Ok(CasResult::Conflict {
                current_version: ObjectVersion::new("0".to_string())
            });
        };

        if meta.version != expected_version.0 {
            return Ok(CasResult::Conflict {
                current_version: ObjectVersion::new(meta.version),
            });
        }

        self.storage.delete(&path).await.map_err(|e| IcebergError::Internal {
            message: format!("Failed to delete pointer: {e}"),
        })?;

        Ok(CasResult::Success { new_version: ObjectVersion::new("0".to_string()) })
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-iceberg test_pointer_store`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-iceberg/src/pointer.rs
git commit -m "feat(iceberg): add PointerStore trait and implementation"
```

---

## Task 12: IdempotencyStore Trait

**Files:**
- Modify: `crates/arco-iceberg/src/idempotency.rs`

**Step 1: Write the failing test**

Add to `idempotency.rs` tests:

```rust
use arco_core::storage::MemoryBackend;
use std::sync::Arc;

#[tokio::test]
async fn test_idempotency_store_claim() {
    let storage = Arc::new(MemoryBackend::new());
    let store = IdempotencyStoreImpl::new(storage);

    let table_uuid = Uuid::new_v4();
    let marker = IdempotencyMarker::new_in_progress(
        "01924a7c-8d9f-7000-8000-000000000001".to_string(),
        table_uuid,
        "request_hash".to_string(),
        "base.json".to_string(),
        "new.json".to_string(),
    );

    // First claim succeeds
    let result = store.claim(&marker).await.expect("claim");
    assert!(matches!(result, ClaimResult::Success { .. }));

    // Second claim finds existing
    let result = store.claim(&marker).await.expect("claim");
    assert!(matches!(result, ClaimResult::Exists { .. }));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-iceberg test_idempotency_store`
Expected: FAIL with "cannot find type `IdempotencyStore`"

**Step 3: Write minimal implementation**

Add to `idempotency.rs`:

```rust
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};

use crate::error::{IcebergError, IcebergResult};
use crate::pointer::ObjectVersion;

/// Result of claiming an idempotency marker.
#[derive(Debug)]
pub enum ClaimResult {
    /// Successfully claimed new marker.
    Success {
        /// Version of the new marker.
        marker_version: ObjectVersion,
    },
    /// Marker already exists.
    Exists {
        /// The existing marker.
        existing: IdempotencyMarker,
        /// Version of the existing marker.
        marker_version: ObjectVersion,
    },
}

/// Trait for idempotency marker storage.
#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    /// Claims an idempotency marker (creates with DoesNotExist precondition).
    async fn claim(&self, marker: &IdempotencyMarker) -> IcebergResult<ClaimResult>;

    /// Loads an existing marker.
    async fn load(
        &self,
        table_uuid: &Uuid,
        idempotency_key_hash: &str,
    ) -> IcebergResult<Option<(IdempotencyMarker, ObjectVersion)>>;

    /// Finalizes a marker (CAS update).
    async fn finalize(
        &self,
        marker: &IdempotencyMarker,
        expected_version: &ObjectVersion,
    ) -> IcebergResult<bool>;
}

/// Implementation of `IdempotencyStore` using `StorageBackend`.
pub struct IdempotencyStoreImpl {
    storage: Arc<dyn StorageBackend>,
}

impl IdempotencyStoreImpl {
    /// Creates a new idempotency store.
    #[must_use]
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl IdempotencyStore for IdempotencyStoreImpl {
    async fn claim(&self, marker: &IdempotencyMarker) -> IcebergResult<ClaimResult> {
        let path = IdempotencyMarker::storage_path(&marker.table_uuid, &marker.idempotency_key_hash);
        let bytes = serde_json::to_vec(marker).map_err(|e| IcebergError::Internal {
            message: format!("Failed to serialize marker: {e}"),
        })?;

        match self.storage.put(&path, Bytes::from(bytes), WritePrecondition::DoesNotExist).await {
            Ok(WriteResult::Success { version }) => {
                Ok(ClaimResult::Success {
                    marker_version: ObjectVersion::new(version),
                })
            }
            Ok(WriteResult::PreconditionFailed { current_version }) => {
                // Load the existing marker
                let existing_bytes = self.storage.get(&path).await.map_err(|e| {
                    IcebergError::Internal {
                        message: format!("Failed to read existing marker: {e}"),
                    }
                })?;
                let existing: IdempotencyMarker = serde_json::from_slice(&existing_bytes)
                    .map_err(|e| IcebergError::Internal {
                        message: format!("Failed to parse existing marker: {e}"),
                    })?;
                Ok(ClaimResult::Exists {
                    existing,
                    marker_version: ObjectVersion::new(current_version),
                })
            }
            Err(e) => Err(IcebergError::Internal {
                message: format!("Failed to claim marker: {e}"),
            }),
        }
    }

    async fn load(
        &self,
        table_uuid: &Uuid,
        idempotency_key_hash: &str,
    ) -> IcebergResult<Option<(IdempotencyMarker, ObjectVersion)>> {
        let path = IdempotencyMarker::storage_path(table_uuid, idempotency_key_hash);

        let meta = self.storage.head(&path).await.map_err(|e| IcebergError::Internal {
            message: format!("Failed to check marker: {e}"),
        })?;

        let Some(meta) = meta else {
            return Ok(None);
        };

        let bytes = self.storage.get(&path).await.map_err(|e| IcebergError::Internal {
            message: format!("Failed to read marker: {e}"),
        })?;

        let marker: IdempotencyMarker = serde_json::from_slice(&bytes)
            .map_err(|e| IcebergError::Internal {
                message: format!("Failed to parse marker: {e}"),
            })?;

        Ok(Some((marker, ObjectVersion::new(meta.version))))
    }

    async fn finalize(
        &self,
        marker: &IdempotencyMarker,
        expected_version: &ObjectVersion,
    ) -> IcebergResult<bool> {
        let path = IdempotencyMarker::storage_path(&marker.table_uuid, &marker.idempotency_key_hash);
        let bytes = serde_json::to_vec(marker).map_err(|e| IcebergError::Internal {
            message: format!("Failed to serialize marker: {e}"),
        })?;

        let precondition = WritePrecondition::MatchesVersion(expected_version.as_str().to_string());

        match self.storage.put(&path, Bytes::from(bytes), precondition).await {
            Ok(WriteResult::Success { .. }) => Ok(true),
            Ok(WriteResult::PreconditionFailed { .. }) => Ok(false),
            Err(e) => Err(IcebergError::Internal {
                message: format!("Failed to finalize marker: {e}"),
            }),
        }
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-iceberg test_idempotency_store`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-iceberg/src/idempotency.rs
git commit -m "feat(iceberg): add IdempotencyStore trait and implementation"
```

---

## Task 13: Commit Flow - Requirements Validation

**Files:**
- Create: `crates/arco-iceberg/src/commit.rs`
- Modify: `crates/arco-iceberg/src/lib.rs`

**Step 1: Write the failing test**

Create `crates/arco-iceberg/src/commit.rs`:

```rust
//! Iceberg table commit flow implementation.
//!
//! See design doc Section 5 for the complete 12-step flow.

use uuid::Uuid;

use crate::error::{IcebergError, IcebergResult};
use crate::types::{TableMetadata, UpdateRequirement};

/// Validates update requirements against current table metadata.
///
/// # Errors
///
/// Returns `IcebergError::Conflict` if any requirement is not met.
pub fn validate_requirements(
    metadata: &TableMetadata,
    requirements: &[UpdateRequirement],
) -> IcebergResult<()> {
    for requirement in requirements {
        validate_requirement(metadata, requirement)?;
    }
    Ok(())
}

fn validate_requirement(
    metadata: &TableMetadata,
    requirement: &UpdateRequirement,
) -> IcebergResult<()> {
    match requirement {
        UpdateRequirement::AssertTableUuid { uuid } => {
            if metadata.table_uuid.as_uuid() != uuid {
                return Err(IcebergError::commit_conflict(format!(
                    "Table UUID mismatch: expected {}, found {}",
                    uuid,
                    metadata.table_uuid.as_uuid()
                )));
            }
        }
        UpdateRequirement::AssertRefSnapshotId { ref_name, snapshot_id } => {
            let current_snapshot_id = metadata
                .refs
                .get(ref_name)
                .map(|r| r.snapshot_id);

            if current_snapshot_id != *snapshot_id {
                return Err(IcebergError::commit_conflict(format!(
                    "Ref '{}' snapshot mismatch: expected {:?}, found {:?}",
                    ref_name, snapshot_id, current_snapshot_id
                )));
            }
        }
        UpdateRequirement::AssertCurrentSchemaId { current_schema_id } => {
            if metadata.current_schema_id != *current_schema_id {
                return Err(IcebergError::commit_conflict(format!(
                    "Current schema ID mismatch: expected {}, found {}",
                    current_schema_id, metadata.current_schema_id
                )));
            }
        }
        UpdateRequirement::AssertLastAssignedFieldId { last_assigned_field_id } => {
            if metadata.last_column_id != *last_assigned_field_id {
                return Err(IcebergError::commit_conflict(format!(
                    "Last assigned field ID mismatch: expected {}, found {}",
                    last_assigned_field_id, metadata.last_column_id
                )));
            }
        }
        UpdateRequirement::AssertDefaultSpecId { default_spec_id } => {
            if metadata.default_spec_id != *default_spec_id {
                return Err(IcebergError::commit_conflict(format!(
                    "Default spec ID mismatch: expected {}, found {}",
                    default_spec_id, metadata.default_spec_id
                )));
            }
        }
        UpdateRequirement::AssertDefaultSortOrderId { default_sort_order_id } => {
            if metadata.default_sort_order_id != *default_sort_order_id {
                return Err(IcebergError::commit_conflict(format!(
                    "Default sort order ID mismatch: expected {}, found {}",
                    default_sort_order_id, metadata.default_sort_order_id
                )));
            }
        }
        UpdateRequirement::AssertLastAssignedPartitionId { last_assigned_partition_id } => {
            if metadata.last_partition_id != *last_assigned_partition_id {
                return Err(IcebergError::commit_conflict(format!(
                    "Last assigned partition ID mismatch: expected {}, found {}",
                    last_assigned_partition_id, metadata.last_partition_id
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{TableUuid, SnapshotRefMetadata};
    use std::collections::HashMap;

    fn test_metadata() -> TableMetadata {
        TableMetadata {
            format_version: 2,
            table_uuid: TableUuid::new(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            location: "gs://bucket/table".to_string(),
            last_sequence_number: 5,
            last_updated_ms: 1234567890000,
            last_column_id: 10,
            current_schema_id: 0,
            schemas: vec![],
            current_snapshot_id: Some(100),
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            properties: HashMap::new(),
            default_spec_id: 0,
            partition_specs: vec![],
            last_partition_id: 1000,
            refs: [("main".to_string(), SnapshotRefMetadata {
                snapshot_id: 100,
                ref_type: "branch".to_string(),
            })].into_iter().collect(),
            default_sort_order_id: 0,
            sort_orders: vec![],
        }
    }

    #[test]
    fn test_validate_table_uuid_success() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertTableUuid {
            uuid: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        }];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }

    #[test]
    fn test_validate_table_uuid_failure() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertTableUuid {
            uuid: Uuid::new_v4(),
        }];
        assert!(validate_requirements(&metadata, &requirements).is_err());
    }

    #[test]
    fn test_validate_ref_snapshot_id_success() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertRefSnapshotId {
            ref_name: "main".to_string(),
            snapshot_id: Some(100),
        }];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }

    #[test]
    fn test_validate_ref_snapshot_id_failure() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertRefSnapshotId {
            ref_name: "main".to_string(),
            snapshot_id: Some(999),
        }];
        assert!(validate_requirements(&metadata, &requirements).is_err());
    }

    #[test]
    fn test_validate_missing_ref() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertRefSnapshotId {
            ref_name: "missing".to_string(),
            snapshot_id: None, // Expecting ref to not exist
        }];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-iceberg validate_`
Expected: FAIL with "cannot find module `commit`"

**Step 3: Export the module and add TableUuid::as_uuid**

In `crates/arco-iceberg/src/lib.rs`, add:

```rust
pub mod commit;
```

In `crates/arco-iceberg/src/types/ids.rs`, add method to `TableUuid`:

```rust
impl TableUuid {
    /// Returns the inner UUID.
    #[must_use]
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-iceberg validate_`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-iceberg/src/commit.rs crates/arco-iceberg/src/lib.rs crates/arco-iceberg/src/types/ids.rs
git commit -m "feat(iceberg): add requirements validation for commit flow"
```

---

## Task 14: Commit Table Route Handler (Skeleton)

**Files:**
- Modify: `crates/arco-iceberg/src/routes/tables.rs`
- Modify: `crates/arco-iceberg/src/routes/mod.rs`

**Step 1: Add commit route to router**

In `routes/tables.rs`, modify the `routes()` function:

```rust
/// Creates table routes.
pub fn routes() -> Router<IcebergState> {
    Router::new()
        .route("/namespaces/:namespace/tables", get(list_tables))
        .route(
            "/namespaces/:namespace/tables/:table",
            get(load_table).head(head_table).post(commit_table),
        )
        .route(
            "/namespaces/:namespace/tables/:table/credentials",
            get(get_credentials),
        )
}
```

**Step 2: Add the handler skeleton**

Add to `routes/tables.rs`:

```rust
use axum::http::header::HeaderMap;
use crate::types::{CommitTableRequest, CommitTableResponse};

/// Commit table updates.
#[utoipa::path(
    post,
    path = "/v1/{prefix}/namespaces/{namespace}/tables/{table}",
    params(
        ("prefix" = String, Path, description = "Catalog prefix"),
        ("namespace" = String, Path, description = "Namespace name"),
        ("table" = String, Path, description = "Table name")
    ),
    request_body = CommitTableRequest,
    responses(
        (status = 200, description = "Commit successful", body = CommitTableResponse),
        (status = 400, description = "Bad request", body = crate::error::IcebergErrorResponse),
        (status = 404, description = "Not found", body = crate::error::IcebergErrorResponse),
        (status = 409, description = "Conflict", body = crate::error::IcebergErrorResponse),
        (status = 503, description = "Service unavailable", body = crate::error::IcebergErrorResponse),
    ),
    tag = "Tables"
)]
#[instrument(skip_all, fields(request_id = %ctx.request_id, tenant = %ctx.tenant, workspace = %ctx.workspace, prefix = %path.prefix, namespace = %path.namespace, table = %path.table))]
async fn commit_table(
    Extension(ctx): Extension<IcebergRequestContext>,
    State(state): State<IcebergState>,
    Path(path): Path<TablePath>,
    headers: HeaderMap,
    Json(request): Json<CommitTableRequest>,
) -> IcebergResult<Json<CommitTableResponse>> {
    ensure_prefix(&path.prefix, &state.config)?;

    // Check if writes are enabled
    if !state.config.allow_write {
        return Err(IcebergError::BadRequest {
            message: "Write operations are not enabled".to_string(),
            error_type: "NotImplementedException",
        });
    }

    // Step 1: Check governance guardrails
    if let Some(reason) = request.check_guardrails() {
        return Err(IcebergError::BadRequest {
            message: reason.to_string(),
            error_type: "BadRequestException",
        });
    }

    // TODO: Implement full commit flow (steps 2-12)
    // For now, return not implemented
    Err(IcebergError::Internal {
        message: "commit_table not yet implemented".to_string(),
    })
}
```

**Step 3: Add imports**

At the top of `routes/tables.rs`, add:

```rust
use crate::types::{CommitTableRequest, CommitTableResponse};
```

**Step 4: Run build to verify compilation**

Run: `cargo build -p arco-iceberg`
Expected: SUCCESS

**Step 5: Commit**

```bash
git add crates/arco-iceberg/src/routes/tables.rs
git commit -m "feat(iceberg): add commit_table route handler skeleton"
```

---

## Task 15: Complete Commit Flow Implementation

**Files:**
- Modify: `crates/arco-iceberg/src/routes/tables.rs`
- Modify: `crates/arco-iceberg/src/commit.rs`

This is a larger task that implements the full 12-step commit flow. Due to complexity, this should be broken into sub-steps during execution.

**Step 1: Write integration test for commit flow**

Add to `routes/tables.rs` tests:

```rust
#[tokio::test]
async fn test_commit_table_guardrails_reject_set_location() {
    let state = build_state_with_write_enabled();
    let _table_id = seed_table(&state, "sales", "orders").await;

    let app = app(state);
    let request_body = serde_json::json!({
        "requirements": [],
        "updates": [{"action": "set-location", "location": "s3://new-bucket/table"}]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/arco/namespaces/sales/tables/orders")
                .header("X-Tenant-Id", "acme")
                .header("X-Workspace-Id", "analytics")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_string(&request_body).unwrap()))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

fn build_state_with_write_enabled() -> IcebergState {
    let storage = Arc::new(MemoryBackend::new());
    let config = IcebergConfig {
        allow_write: true,
        ..Default::default()
    };
    IcebergState::with_config(storage, config)
}
```

**Step 2: Implement the full commit flow**

The commit flow should follow design doc Section 5:

1. TIER 1 READ - Resolve table identity (already in handler context)
2. LOAD POINTER + VERSION
3. LOAD BASE METADATA FILE
4. CLAIM IDEMPOTENCY MARKER
5. VALIDATE ICEBERG REQUIREMENTS
6. COMPUTE NEW STATE
7. WRITE NEW METADATA FILE
8. WRITE PENDING RECEIPT (best effort)
9. POINTER CAS
10. WRITE COMMITTED RECEIPT (best effort)
11. FINALIZE IDEMPOTENCY MARKER
12. RETURN RESPONSE

This implementation requires the following imports and sub-components:

```rust
use crate::commit::validate_requirements;
use crate::events::{CommittedReceipt, PendingReceipt};
use crate::idempotency::{IdempotencyMarker, IdempotencyStoreImpl, ClaimResult};
use crate::pointer::{PointerStoreImpl, CasResult, ObjectVersion};
use crate::types::CommitKey;
```

**Full implementation to be added in commit.rs as `CommitService` struct that encapsulates the flow.**

**Step 3: Run tests**

Run: `cargo test -p arco-iceberg`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/arco-iceberg/
git commit -m "feat(iceberg): implement full commit_table flow"
```

---

## Task 16: Verify All Tests Pass

**Files:** None (verification only)

**Step 1: Run full test suite**

Run: `cargo test --workspace`
Expected: All tests PASS

**Step 2: Run clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: No warnings

**Step 3: Run fmt check**

Run: `cargo fmt --check`
Expected: No formatting issues

---

## Summary

This plan implements Phase B of the Iceberg REST Catalog integration:

1. **Tasks 1-2:** Core newtypes (`ObjectVersion`, `CommitKey`)
2. **Tasks 3-5:** Idempotency infrastructure (`IdempotencyMarker`, UUIDv7 validation, JCS hashing)
3. **Task 6:** Event receipts (`PendingReceipt`, `CommittedReceipt`)
4. **Tasks 7-10:** Iceberg types (`UpdateRequirement`, `TableUpdate`, `CommitTableRequest/Response`)
5. **Tasks 11-12:** Storage traits (`PointerStore`, `IdempotencyStore`)
6. **Tasks 13-15:** Commit flow implementation
7. **Task 16:** Verification

Total: ~16 tasks following TDD with frequent commits.
