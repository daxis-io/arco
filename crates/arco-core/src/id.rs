//! Strongly-typed identifiers for Arco entities.
//!
//! All identifiers in Arco are:
//! - **Strongly typed**: Prevents mixing up different ID types at compile time
//! - **Lexicographically sortable**: ULIDs encode creation time and sort naturally
//! - **Globally unique**: No coordination required for generation
//!
//! # Example
//!
//! ```rust
//! use arco_core::id::{AssetId, RunId};
//!
//! let asset = AssetId::generate();
//! let run = RunId::generate();
//!
//! // IDs are different types - this won't compile:
//! // let wrong: AssetId = run;
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use ulid::Ulid;

use crate::error::{Error, Result};

/// A unique identifier for an asset in the catalog.
///
/// Assets are the primary unit of data organization in Arco,
/// representing tables, views, or other data artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AssetId(Ulid);

impl AssetId {
    /// Generates a new unique asset ID.
    ///
    /// Uses ULID generation which is:
    /// - Lexicographically sortable by creation time
    /// - Globally unique without coordination
    /// - URL-safe and case-insensitive
    #[must_use]
    pub fn generate() -> Self {
        Self(Ulid::new())
    }

    /// Creates an asset ID from a raw ULID.
    #[must_use]
    pub const fn from_ulid(ulid: Ulid) -> Self {
        Self(ulid)
    }

    /// Returns the underlying ULID.
    #[must_use]
    pub const fn as_ulid(&self) -> Ulid {
        self.0
    }

    /// Returns the creation timestamp encoded in the ID.
    #[must_use]
    pub fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        let ms = self.0.timestamp_ms();
        chrono::DateTime::from_timestamp_millis(ms as i64).unwrap_or_else(chrono::Utc::now)
    }
}

impl fmt::Display for AssetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for AssetId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ulid::from_string(s)
            .map(Self)
            .map_err(|e| Error::InvalidId {
                message: format!("invalid asset ID '{s}': {e}"),
            })
    }
}

/// A unique identifier for a pipeline run.
///
/// Runs represent a single execution of a pipeline or asset computation.
/// Each run captures inputs, outputs, and execution metadata for lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(Ulid);

impl RunId {
    /// Generates a new unique run ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Ulid::new())
    }

    /// Creates a run ID from a raw ULID.
    #[must_use]
    pub const fn from_ulid(ulid: Ulid) -> Self {
        Self(ulid)
    }

    /// Returns the underlying ULID.
    #[must_use]
    pub const fn as_ulid(&self) -> Ulid {
        self.0
    }

    /// Returns the creation timestamp encoded in the ID.
    #[must_use]
    pub fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        let ms = self.0.timestamp_ms();
        chrono::DateTime::from_timestamp_millis(ms as i64).unwrap_or_else(chrono::Utc::now)
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for RunId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ulid::from_string(s)
            .map(Self)
            .map_err(|e| Error::InvalidId {
                message: format!("invalid run ID '{s}': {e}"),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_id_roundtrip() {
        let id = AssetId::generate();
        let s = id.to_string();
        let parsed: AssetId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn run_id_roundtrip() {
        let id = RunId::generate();
        let s = id.to_string();
        let parsed: RunId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn ids_are_unique() {
        let id1 = AssetId::generate();
        let id2 = AssetId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn invalid_id_returns_error() {
        let result: Result<AssetId> = "not-a-valid-ulid".parse();
        assert!(result.is_err());
    }
}
