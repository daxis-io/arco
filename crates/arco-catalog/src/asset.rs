//! Asset data model for the Arco catalog.
//!
//! Assets are the fundamental unit of data in Arco. Each asset represents
//! a logical data entity (table, view, file collection) with associated
//! metadata, lineage, and governance information.
//!
//! # Asset Keys
//!
//! Assets are uniquely identified by their key, which consists of:
//! - `group`: Logical grouping (e.g., "raw", "staging", "analytics")
//! - `name`: Asset name within the group
//!
//! The combination `group/name` must be unique within a workspace.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use arco_core::AssetId;

/// Format of the underlying data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum AssetFormat {
    /// Apache Parquet format.
    #[default]
    Parquet,
    /// Delta Lake format.
    Delta,
    /// Apache Iceberg format.
    Iceberg,
    /// CSV files.
    Csv,
    /// JSON files.
    Json,
    /// Avro format.
    Avro,
}

impl AssetFormat {
    /// Returns the file extension for this format.
    #[must_use]
    pub const fn extension(&self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Delta => "delta",
            Self::Iceberg => "iceberg",
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Avro => "avro",
        }
    }
}

/// Asset key: unique identifier within a workspace.
///
/// The key consists of a group and name, forming a path-like identifier:
/// `{group}/{name}` (e.g., `raw/events`, `analytics/daily_summary`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetKey {
    /// Logical group (e.g., "raw", "staging", "analytics").
    pub group: String,
    /// Asset name within the group.
    pub name: String,
}

impl AssetKey {
    /// Creates a new asset key.
    ///
    /// # Errors
    ///
    /// Returns an error if group or name is empty or contains invalid characters.
    pub fn new(group: impl Into<String>, name: impl Into<String>) -> Result<Self, AssetKeyError> {
        let group = group.into();
        let name = name.into();

        Self::validate_segment(&group, "group")?;
        Self::validate_segment(&name, "name")?;

        Ok(Self { group, name })
    }

    /// Validates a key segment (group or name).
    fn validate_segment(segment: &str, field: &str) -> Result<(), AssetKeyError> {
        if segment.is_empty() {
            return Err(AssetKeyError::Empty {
                field: field.into(),
            });
        }

        if segment.len() > 128 {
            return Err(AssetKeyError::TooLong {
                field: field.into(),
                len: segment.len(),
            });
        }

        // Must start with lowercase letter
        if !segment
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase())
        {
            return Err(AssetKeyError::InvalidStart {
                field: field.into(),
            });
        }

        // Can contain lowercase letters, digits, underscores
        if !segment
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(AssetKeyError::InvalidCharacters {
                field: field.into(),
            });
        }

        Ok(())
    }

    /// Returns the full path representation: `{group}/{name}`.
    #[must_use]
    pub fn path(&self) -> String {
        format!("{}/{}", self.group, self.name)
    }

    /// Parses a key from a path string.
    ///
    /// # Errors
    ///
    /// Returns an error if the path is not in `{group}/{name}` format.
    pub fn parse(path: &str) -> Result<Self, AssetKeyError> {
        let mut parts = path.splitn(2, '/');
        let group = parts.next().ok_or_else(|| AssetKeyError::InvalidPath {
            path: path.to_string(),
        })?;
        let name = parts.next().ok_or_else(|| AssetKeyError::InvalidPath {
            path: path.to_string(),
        })?;
        // Ensure there are no additional slashes
        if name.contains('/') {
            return Err(AssetKeyError::InvalidPath {
                path: path.to_string(),
            });
        }
        Self::new(group, name)
    }
}

impl std::fmt::Display for AssetKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.group, self.name)
    }
}

/// Errors for asset key validation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AssetKeyError {
    /// Field is empty.
    #[error("{field} cannot be empty")]
    Empty {
        /// The field that was empty.
        field: String,
    },

    /// Field is too long.
    #[error("{field} is too long ({len} > 128 characters)")]
    TooLong {
        /// The field that was too long.
        field: String,
        /// The actual length.
        len: usize,
    },

    /// Field doesn't start with a lowercase letter.
    #[error("{field} must start with a lowercase letter")]
    InvalidStart {
        /// The field with invalid start.
        field: String,
    },

    /// Field contains invalid characters.
    #[error("{field} contains invalid characters (only a-z, 0-9, _ allowed)")]
    InvalidCharacters {
        /// The field with invalid characters.
        field: String,
    },

    /// Invalid path format.
    #[error("invalid asset path format: {path} (expected group/name)")]
    InvalidPath {
        /// The invalid path.
        path: String,
    },
}

/// Asset definition with all metadata.
///
/// This is the complete asset record stored in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Asset {
    /// Unique identifier (generated).
    pub id: AssetId,

    /// Asset key (group/name).
    pub key: AssetKey,

    /// Storage location (GCS/S3 URI).
    pub location: String,

    /// Data format.
    pub format: AssetFormat,

    /// Human-readable description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Asset owners (user IDs or emails).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub owners: Vec<String>,

    /// Custom tags for discovery and governance.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,

    /// Schema hash for change detection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_hash: Option<String>,

    /// Whether this asset is partitioned.
    #[serde(default)]
    pub is_partitioned: bool,

    /// Partition columns (if partitioned).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_columns: Vec<String>,

    /// Creation timestamp.
    pub created_at: DateTime<Utc>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl Asset {
    /// Creates a new asset with the given key and location.
    #[must_use]
    pub fn new(key: AssetKey, location: impl Into<String>, format: AssetFormat) -> Self {
        let now = Utc::now();
        Self {
            id: AssetId::generate(),
            key,
            location: location.into(),
            format,
            description: None,
            owners: Vec::new(),
            tags: Vec::new(),
            schema_hash: None,
            is_partitioned: false,
            partition_columns: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Returns the asset path (group/name).
    #[must_use]
    pub fn path(&self) -> String {
        self.key.path()
    }
}

/// Request to create a new asset.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAssetRequest {
    /// Asset key.
    pub key: AssetKey,

    /// Storage location.
    pub location: String,

    /// Data format.
    pub format: AssetFormat,

    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Optional owners.
    #[serde(default)]
    pub owners: Vec<String>,

    /// Optional tags.
    #[serde(default)]
    pub tags: Vec<String>,
}

impl CreateAssetRequest {
    /// Creates a request for a simple asset.
    #[must_use]
    pub fn simple(key: AssetKey, location: impl Into<String>) -> Self {
        Self {
            key,
            location: location.into(),
            format: AssetFormat::Parquet,
            description: None,
            owners: Vec::new(),
            tags: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_asset_key_valid() {
        let key = AssetKey::new("raw", "events").expect("valid key");
        assert_eq!(key.group, "raw");
        assert_eq!(key.name, "events");
        assert_eq!(key.path(), "raw/events");
    }

    #[test]
    fn test_asset_key_with_underscores() {
        let key = AssetKey::new("staging_v2", "daily_summary").expect("valid key");
        assert_eq!(key.path(), "staging_v2/daily_summary");
    }

    #[test]
    fn test_asset_key_with_digits() {
        let key = AssetKey::new("raw123", "events456").expect("valid key");
        assert_eq!(key.path(), "raw123/events456");
    }

    #[test]
    fn test_asset_key_empty_group() {
        let result = AssetKey::new("", "events");
        assert!(matches!(result, Err(AssetKeyError::Empty { .. })));
    }

    #[test]
    fn test_asset_key_empty_name() {
        let result = AssetKey::new("raw", "");
        assert!(matches!(result, Err(AssetKeyError::Empty { .. })));
    }

    #[test]
    fn test_asset_key_invalid_start() {
        let result = AssetKey::new("123raw", "events");
        assert!(matches!(result, Err(AssetKeyError::InvalidStart { .. })));

        let result = AssetKey::new("Raw", "events");
        assert!(matches!(result, Err(AssetKeyError::InvalidStart { .. })));
    }

    #[test]
    fn test_asset_key_invalid_characters() {
        let result = AssetKey::new("raw-data", "events");
        assert!(matches!(
            result,
            Err(AssetKeyError::InvalidCharacters { .. })
        ));

        let result = AssetKey::new("raw", "events.v1");
        assert!(matches!(
            result,
            Err(AssetKeyError::InvalidCharacters { .. })
        ));

        let result = AssetKey::new("raw", "events/sub");
        assert!(matches!(
            result,
            Err(AssetKeyError::InvalidCharacters { .. })
        ));
    }

    #[test]
    fn test_asset_key_parse() {
        let key = AssetKey::parse("analytics/daily_summary").expect("valid");
        assert_eq!(key.group, "analytics");
        assert_eq!(key.name, "daily_summary");
    }

    #[test]
    fn test_asset_key_parse_invalid() {
        assert!(AssetKey::parse("no_slash").is_err());
        assert!(AssetKey::parse("too/many/slashes").is_err());
        assert!(AssetKey::parse("").is_err());
    }

    #[test]
    fn test_asset_format_extension() {
        assert_eq!(AssetFormat::Parquet.extension(), "parquet");
        assert_eq!(AssetFormat::Delta.extension(), "delta");
        assert_eq!(AssetFormat::Csv.extension(), "csv");
    }

    #[test]
    fn test_asset_creation() {
        let key = AssetKey::new("raw", "events").expect("valid");
        let asset = Asset::new(key, "gs://bucket/raw/events", AssetFormat::Parquet);

        assert_eq!(asset.path(), "raw/events");
        assert_eq!(asset.location, "gs://bucket/raw/events");
        assert_eq!(asset.format, AssetFormat::Parquet);
        assert!(!asset.is_partitioned);
    }

    #[test]
    fn test_asset_serialization() {
        let key = AssetKey::new("raw", "events").expect("valid");
        let asset = Asset::new(key, "gs://bucket/raw/events", AssetFormat::Parquet);

        let json = serde_json::to_string_pretty(&asset).expect("serialize");
        assert!(json.contains("\"group\": \"raw\""));
        assert!(json.contains("\"name\": \"events\""));
        assert!(json.contains("\"format\": \"parquet\""));

        // Roundtrip
        let parsed: Asset = serde_json::from_str(&json).expect("parse");
        assert_eq!(parsed.path(), "raw/events");
    }

    #[test]
    fn test_create_asset_request() {
        let key = AssetKey::new("analytics", "summary").expect("valid");
        let request = CreateAssetRequest::simple(key, "gs://bucket/analytics/summary");

        assert_eq!(request.key.path(), "analytics/summary");
        assert_eq!(request.format, AssetFormat::Parquet);
    }
}
