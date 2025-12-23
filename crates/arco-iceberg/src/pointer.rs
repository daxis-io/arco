//! Iceberg table pointer for CAS-based state management.
//!
//! The pointer file tracks the current Iceberg metadata location for each table,
//! enabling atomic updates via conditional writes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

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

/// Tracks the current state of an Iceberg table.
///
/// The pointer is stored at `_catalog/iceberg_pointers/{table_uuid}.json`
/// and updated atomically using CAS (compare-and-swap) semantics.
///
/// # Storage Semantics
///
/// - Create: `WritePrecondition::DoesNotExist`
/// - Update: `WritePrecondition::MatchesVersion(expected_version)`
/// - Delete: `WritePrecondition::MatchesVersion(expected_version)`
///
/// # Example
///
/// ```rust
/// use arco_iceberg::pointer::IcebergTablePointer;
/// use uuid::Uuid;
///
/// let pointer = IcebergTablePointer::new(
///     Uuid::new_v4(),
///     "gs://bucket/warehouse/ns/table/metadata/00000-uuid.metadata.json".to_string(),
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IcebergTablePointer {
    /// Schema version for forward compatibility.
    pub version: u32,

    /// The table this pointer belongs to.
    pub table_uuid: Uuid,

    /// Current Iceberg metadata file location.
    pub current_metadata_location: String,

    /// Current snapshot ID (denormalized for fast checks).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,

    /// Snapshot refs (main, branches, tags).
    #[serde(default)]
    pub refs: HashMap<String, SnapshotRef>,

    /// Last sequence number assigned.
    pub last_sequence_number: i64,

    /// Previous metadata location (for history building and validation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_metadata_location: Option<String>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,

    /// Source of the last update.
    #[serde(default)]
    pub updated_by: UpdateSource,
}

impl IcebergTablePointer {
    /// Current pointer schema version.
    pub const CURRENT_VERSION: u32 = 1;

    /// Creates a new pointer for an Iceberg table.
    ///
    /// The pointer starts with sequence number 0 and no snapshot.
    #[must_use]
    pub fn new(table_uuid: Uuid, current_metadata_location: String) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            table_uuid,
            current_metadata_location,
            current_snapshot_id: None,
            refs: HashMap::new(),
            last_sequence_number: 0,
            previous_metadata_location: None,
            updated_at: Utc::now(),
            updated_by: UpdateSource::Unknown,
        }
    }

    /// Validates the pointer version is supported.
    ///
    /// # Errors
    ///
    /// Returns an error if the version is unsupported.
    pub fn validate_version(&self) -> Result<(), PointerVersionError> {
        if self.version > Self::CURRENT_VERSION {
            return Err(PointerVersionError::UnsupportedVersion {
                found: self.version,
                max_supported: Self::CURRENT_VERSION,
            });
        }
        Ok(())
    }

    /// Returns the path where this pointer should be stored.
    #[must_use]
    pub fn storage_path(table_uuid: &Uuid) -> String {
        format!("_catalog/iceberg_pointers/{table_uuid}.json")
    }
}

/// Reference to a snapshot (branch or tag).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotRef {
    /// The snapshot ID this ref points to.
    pub snapshot_id: i64,

    /// Type of reference.
    #[serde(rename = "type")]
    pub ref_type: SnapshotRefType,

    /// Maximum age of snapshots to retain for this ref (branches only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ref_age_ms: Option<i64>,

    /// Maximum age of snapshots to retain (branches only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_snapshot_age_ms: Option<i64>,

    /// Minimum number of snapshots to retain (branches only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_snapshots_to_keep: Option<i32>,
}

/// Type of snapshot reference.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotRefType {
    /// A mutable branch (like git branch).
    Branch,
    /// An immutable tag (like git tag).
    Tag,
}

/// Source of the last update to the pointer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UpdateSource {
    /// Update from Servo orchestration.
    Servo {
        /// The run ID.
        run_id: ulid::Ulid,
        /// The task ID.
        task_id: ulid::Ulid,
    },
    /// Update from Iceberg REST API.
    IcebergRest {
        /// Client user-agent string.
        #[serde(skip_serializing_if = "Option::is_none")]
        client_info: Option<String>,
        /// Authenticated principal.
        #[serde(skip_serializing_if = "Option::is_none")]
        principal: Option<String>,
    },
    /// Update from admin API.
    AdminApi {
        /// Admin user ID.
        user_id: String,
    },
    /// Unknown source (for deserialization compatibility).
    #[serde(other)]
    Unknown,
}

impl Default for UpdateSource {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Error when validating pointer version.
#[derive(Debug, thiserror::Error)]
pub enum PointerVersionError {
    /// Pointer version is newer than supported.
    #[error("Unsupported pointer version {found}, max supported is {max_supported}")]
    UnsupportedVersion {
        /// Version found in the pointer.
        found: u32,
        /// Maximum supported version.
        max_supported: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pointer_serialization_roundtrip() {
        let pointer = IcebergTablePointer {
            version: 1,
            table_uuid: Uuid::new_v4(),
            current_metadata_location: "gs://bucket/path/metadata.json".to_string(),
            current_snapshot_id: Some(123),
            refs: HashMap::from([(
                "main".to_string(),
                SnapshotRef {
                    snapshot_id: 123,
                    ref_type: SnapshotRefType::Branch,
                    max_ref_age_ms: None,
                    max_snapshot_age_ms: None,
                    min_snapshots_to_keep: None,
                },
            )]),
            last_sequence_number: 1,
            previous_metadata_location: Some("gs://bucket/path/old.json".to_string()),
            updated_at: Utc::now(),
            updated_by: UpdateSource::IcebergRest {
                client_info: Some("spark/3.5".to_string()),
                principal: Some("user@example.com".to_string()),
            },
        };

        let json = serde_json::to_string_pretty(&pointer).expect("serialization failed");
        let parsed: IcebergTablePointer =
            serde_json::from_str(&json).expect("deserialization failed");

        assert_eq!(pointer, parsed);
    }

    #[test]
    fn test_pointer_version_validation() {
        let mut pointer = IcebergTablePointer::new(Uuid::new_v4(), "test".to_string());

        // Current version should be valid
        assert!(pointer.validate_version().is_ok());

        // Future version should fail
        pointer.version = 999;
        assert!(pointer.validate_version().is_err());
    }

    #[test]
    fn test_storage_path() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("valid uuid");
        let path = IcebergTablePointer::storage_path(&uuid);
        assert_eq!(
            path,
            "_catalog/iceberg_pointers/550e8400-e29b-41d4-a716-446655440000.json"
        );
    }

    #[test]
    fn test_update_source_variants() {
        // Test each variant serializes correctly
        let servo = UpdateSource::Servo {
            run_id: ulid::Ulid::new(),
            task_id: ulid::Ulid::new(),
        };
        let json = serde_json::to_string(&servo).expect("serialization");
        assert!(json.contains("\"type\":\"servo\""));

        let rest = UpdateSource::IcebergRest {
            client_info: Some("test".to_string()),
            principal: None,
        };
        let json = serde_json::to_string(&rest).expect("serialization");
        assert!(json.contains("\"type\":\"iceberg_rest\""));
    }

    #[test]
    fn test_update_source_unknown_fallback() {
        let json = r#"{"type":"new_source","foo":"bar"}"#;
        let parsed: UpdateSource = serde_json::from_str(json).expect("deserialize");
        assert!(matches!(parsed, UpdateSource::Unknown));
    }

    #[test]
    fn test_object_version_newtype() {
        let version = ObjectVersion::new("12345");
        assert_eq!(version.as_str(), "12345");

        // Test From<String>
        let version: ObjectVersion = "67890".to_string().into();
        assert_eq!(version.as_str(), "67890");
    }
}
