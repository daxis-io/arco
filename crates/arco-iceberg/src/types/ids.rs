//! Domain-specific identifier types for Iceberg.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Table UUID wrapper.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(transparent)]
pub struct TableUuid(pub Uuid);

impl TableUuid {
    /// Creates a new table UUID wrapper.
    #[must_use]
    pub const fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Returns the inner UUID.
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl From<Uuid> for TableUuid {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

/// Opaque object version token.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(transparent)]
pub struct ObjectVersion(pub String);

/// Deterministic commit key derived from metadata location.
///
/// `commit_key = SHA256(metadata_location)` (hex encoded).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(transparent)]
pub struct CommitKey(pub String);

impl CommitKey {
    /// Creates a commit key from a metadata location using SHA256.
    #[must_use]
    pub fn from_metadata_location(location: &str) -> Self {
        use sha2::{Digest, Sha256};
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
    fn test_table_uuid_roundtrip() {
        let uuid = Uuid::new_v4();
        let wrapped = TableUuid::new(uuid);
        let json = serde_json::to_string(&wrapped).expect("serialize");
        let parsed: TableUuid = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.as_uuid(), &uuid);
    }

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
