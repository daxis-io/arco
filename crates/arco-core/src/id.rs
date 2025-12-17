//! Strongly-typed identifiers for Arco entities.
//!
//! Arco uses different identifier formats based on the entity requirements:
//! - **Stable entity IDs** (e.g., assets): UUID v7 (stable identity, time-sortable)
//! - **Ordered log IDs** (e.g., runs, tasks): ULID (lexicographically sortable)
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
use uuid::Uuid;

use crate::error::{Error, Result};

/// A unique identifier for an asset in the catalog.
///
/// Assets are the primary unit of data organization in Arco,
/// representing tables, views, or other data artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AssetId(Uuid);

impl AssetId {
    /// Generates a new unique asset ID.
    ///
    /// Uses UUID v7 generation which is:
    /// - Stable across renames
    /// - Time-sortable (creation-time ordered)
    /// - Globally unique without coordination
    #[must_use]
    pub fn generate() -> Self {
        Self(Uuid::now_v7())
    }

    /// Creates an asset ID from a raw UUID.
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Returns the underlying UUID.
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Returns the creation timestamp encoded in the ID.
    #[must_use]
    pub fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        let Some(ts) = self.0.get_timestamp() else {
            return chrono::Utc::now();
        };

        let (seconds, nanos) = ts.to_unix();
        let seconds = i64::try_from(seconds).unwrap_or(i64::MAX);
        chrono::DateTime::from_timestamp(seconds, nanos).unwrap_or_else(chrono::Utc::now)
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
        Uuid::parse_str(s).map(Self).map_err(|e| Error::InvalidId {
            message: format!("invalid asset ID '{s}': {e}"),
        })
    }
}

/// A unique identifier for a pipeline run.
///
/// Runs represent a single execution of a pipeline or asset computation.
/// Each run captures inputs, outputs, and execution metadata for lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
        let ms_i64 = i64::try_from(ms).unwrap_or(i64::MAX);
        chrono::DateTime::from_timestamp_millis(ms_i64).unwrap_or_else(chrono::Utc::now)
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

/// A unique identifier for a task within a run.
///
/// Tasks are individual units of work that materialize a single asset
/// (or asset partition) within an orchestration run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(Ulid);

impl TaskId {
    /// Generates a new unique task ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Ulid::new())
    }

    /// Creates a task ID from a raw ULID.
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
        let ms_i64 = i64::try_from(ms).unwrap_or(i64::MAX);
        chrono::DateTime::from_timestamp_millis(ms_i64).unwrap_or_else(chrono::Utc::now)
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for TaskId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ulid::from_string(s)
            .map(Self)
            .map_err(|e| Error::InvalidId {
                message: format!("invalid task ID '{s}': {e}"),
            })
    }
}

/// A unique identifier for a materialization.
///
/// Materializations represent a single successful execution of an asset
/// or asset partition, capturing the output files and metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MaterializationId(Ulid);

impl MaterializationId {
    /// Generates a new unique materialization ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Ulid::new())
    }

    /// Creates a materialization ID from a raw ULID.
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
        let ms_i64 = i64::try_from(ms).unwrap_or(i64::MAX);
        chrono::DateTime::from_timestamp_millis(ms_i64).unwrap_or_else(chrono::Utc::now)
    }
}

impl fmt::Display for MaterializationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for MaterializationId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ulid::from_string(s)
            .map(Self)
            .map_err(|e| Error::InvalidId {
                message: format!("invalid materialization ID '{s}': {e}"),
            })
    }
}

/// A unique identifier for a Tier 2 ledger event.
///
/// Event IDs are ULIDs to preserve chronological ordering when comparing as strings
/// (critical for lexicographic watermarking and file naming).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(Ulid);

impl EventId {
    /// Generates a new unique event ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(Ulid::new())
    }

    /// Creates an event ID from a raw ULID.
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
        let ms_i64 = i64::try_from(ms).unwrap_or(i64::MAX);
        chrono::DateTime::from_timestamp_millis(ms_i64).unwrap_or_else(chrono::Utc::now)
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for EventId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ulid::from_string(s)
            .map(Self)
            .map_err(|e| Error::InvalidId {
                message: format!("invalid event ID '{s}': {e}"),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_id_roundtrip() -> Result<()> {
        let id = AssetId::generate();
        let s = id.to_string();
        let parsed: AssetId = s.parse()?;
        assert_eq!(id, parsed);
        Ok(())
    }

    #[test]
    fn run_id_roundtrip() -> Result<()> {
        let id = RunId::generate();
        let s = id.to_string();
        let parsed: RunId = s.parse()?;
        assert_eq!(id, parsed);
        Ok(())
    }

    #[test]
    fn ids_are_unique() {
        let id1 = AssetId::generate();
        let id2 = AssetId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn invalid_id_returns_error() {
        let result: Result<AssetId> = "not-a-valid-uuid".parse();
        assert!(result.is_err());
    }

    #[test]
    fn task_id_roundtrip() -> Result<()> {
        let id = TaskId::generate();
        let s = id.to_string();
        let parsed: TaskId = s.parse()?;
        assert_eq!(id, parsed);
        Ok(())
    }

    #[test]
    fn materialization_id_roundtrip() -> Result<()> {
        let id = MaterializationId::generate();
        let s = id.to_string();
        let parsed: MaterializationId = s.parse()?;
        assert_eq!(id, parsed);
        Ok(())
    }

    #[test]
    fn task_ids_are_unique() {
        let id1 = TaskId::generate();
        let id2 = TaskId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn materialization_ids_are_unique() {
        let id1 = MaterializationId::generate();
        let id2 = MaterializationId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn event_id_roundtrip() -> Result<()> {
        let id = EventId::generate();
        let s = id.to_string();
        let parsed: EventId = s.parse()?;
        assert_eq!(id, parsed);
        Ok(())
    }
}
