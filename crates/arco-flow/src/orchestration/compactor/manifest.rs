//! Orchestration manifest schema for tracking compaction state.
//!
//! The manifest tracks:
//! - Watermarks: Which events have been processed
//! - Base snapshot: Current merged Parquet files
//! - L0 deltas: Recent micro-compaction outputs (pending merge)
//!
//! The manifest is updated atomically using CAS (Compare-And-Swap) semantics
//! to prevent concurrent compactor conflicts.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Orchestration manifest tracking compaction state.
///
/// This follows the unified platform pattern from ADR-020:
/// - Controllers read base + L0 deltas (never ledger)
/// - Compactor is sole writer (IAM-enforced)
/// - CAS publish for atomic updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationManifest {
    /// Schema version for forward compatibility.
    pub schema_version: u32,

    /// Unique revision identifier (ULID) for CAS operations.
    pub revision_ulid: String,

    /// When this manifest revision was published.
    pub published_at: DateTime<Utc>,

    /// Event processing watermarks.
    pub watermarks: Watermarks,

    /// Current base snapshot (periodically merged).
    pub base_snapshot: BaseSnapshot,

    /// L0 delta files pending merge into base.
    pub l0_deltas: Vec<L0Delta>,

    /// Current L0 count (for compaction triggers).
    pub l0_count: u32,

    /// L0 limits for triggering merge.
    pub l0_limits: L0Limits,
}

impl OrchestrationManifest {
    /// Creates a new empty manifest.
    #[must_use]
    pub fn new(revision_ulid: impl Into<String>) -> Self {
        Self {
            schema_version: 1,
            revision_ulid: revision_ulid.into(),
            published_at: Utc::now(),
            watermarks: Watermarks::default(),
            base_snapshot: BaseSnapshot::default(),
            l0_deltas: Vec::new(),
            l0_count: 0,
            l0_limits: L0Limits::default(),
        }
    }

    /// Checks if L0 compaction should be triggered.
    #[must_use]
    pub fn should_compact_l0(&self) -> bool {
        self.l0_count >= self.l0_limits.max_count
    }

    /// Returns the watermark lag (time since last processed event).
    #[must_use]
    pub fn watermark_lag(&self) -> chrono::Duration {
        Utc::now() - self.watermarks.last_processed_at
    }
}

/// Event processing watermarks.
///
/// Tracks which events have been processed so controllers can trust
/// that Parquet projections are reasonably fresh.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Watermarks {
    /// ULID of last processed event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_processed_through: Option<String>,

    /// Filename of last processed ledger file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_processed_file: Option<String>,

    /// When the last event was processed.
    pub last_processed_at: DateTime<Utc>,
}

impl Watermarks {
    /// Checks if compaction is fresh enough for timer actions.
    ///
    /// Timer actions (retry, heartbeat) should not proceed if compaction
    /// is significantly behind, as they might make decisions based on stale state.
    #[must_use]
    pub fn is_fresh(&self, max_lag: chrono::Duration) -> bool {
        Utc::now() - self.last_processed_at <= max_lag
    }
}

/// Base snapshot metadata.
///
/// The base snapshot contains fully merged Parquet files.
/// Controllers read base + L0 deltas for current state.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BaseSnapshot {
    /// Unique snapshot identifier (ULID).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,

    /// When the base snapshot was created.
    pub published_at: DateTime<Utc>,

    /// Table file paths within the snapshot.
    pub tables: TablePaths,
}

/// Paths to Parquet table files within a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TablePaths {
    /// Path to runs.parquet (relative to state/).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runs: Option<String>,

    /// Path to tasks.parquet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tasks: Option<String>,

    /// Path to dep_satisfaction.parquet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dep_satisfaction: Option<String>,

    /// Path to timers.parquet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timers: Option<String>,

    /// Path to dispatch_outbox.parquet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dispatch_outbox: Option<String>,
}

impl TablePaths {
    /// Returns all non-empty table paths.
    #[must_use]
    pub fn all(&self) -> HashMap<&'static str, &str> {
        let mut paths = HashMap::new();
        if let Some(ref p) = self.runs {
            paths.insert("runs", p.as_str());
        }
        if let Some(ref p) = self.tasks {
            paths.insert("tasks", p.as_str());
        }
        if let Some(ref p) = self.dep_satisfaction {
            paths.insert("dep_satisfaction", p.as_str());
        }
        if let Some(ref p) = self.timers {
            paths.insert("timers", p.as_str());
        }
        if let Some(ref p) = self.dispatch_outbox {
            paths.insert("dispatch_outbox", p.as_str());
        }
        paths
    }
}

/// L0 delta file metadata.
///
/// L0 deltas are small Parquet files produced by micro-compaction.
/// They are merged into the base snapshot when limits are reached.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L0Delta {
    /// Unique delta identifier (ULID).
    pub delta_id: String,

    /// When this delta was created.
    pub created_at: DateTime<Utc>,

    /// Event range covered by this delta.
    pub event_range: EventRange,

    /// Table file paths within this delta.
    pub tables: TablePaths,

    /// Number of rows in this delta (for merge optimization).
    pub row_counts: RowCounts,
}

/// Event range covered by a delta or snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRange {
    /// First event ULID included.
    pub from_event: String,

    /// Last event ULID included.
    pub to_event: String,

    /// Number of events in range.
    pub event_count: u32,
}

/// Row counts for merge optimization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RowCounts {
    /// Rows in runs table.
    pub runs: u32,

    /// Rows in tasks table.
    pub tasks: u32,

    /// Rows in dep_satisfaction table.
    pub dep_satisfaction: u32,

    /// Rows in timers table.
    pub timers: u32,

    /// Rows in dispatch_outbox table.
    pub dispatch_outbox: u32,
}

impl RowCounts {
    /// Total rows across all tables.
    #[must_use]
    pub fn total(&self) -> u32 {
        self.runs + self.tasks + self.dep_satisfaction + self.timers + self.dispatch_outbox
    }
}

/// L0 compaction limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L0Limits {
    /// Maximum L0 files before merge is triggered.
    pub max_count: u32,

    /// Maximum L0 total rows before merge is triggered.
    pub max_rows: u32,

    /// Maximum L0 age before merge is triggered.
    pub max_age_seconds: u32,
}

impl Default for L0Limits {
    fn default() -> Self {
        Self {
            max_count: 10,
            max_rows: 100_000,
            max_age_seconds: 300, // 5 minutes
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_manifest_serialization() {
        let manifest = OrchestrationManifest::new("01HQXYZ123REV");

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        assert!(json.contains("watermarks"));
        assert!(json.contains("base_snapshot"));
        assert!(json.contains("l0_deltas"));

        let parsed: OrchestrationManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.schema_version, 1);
        assert_eq!(parsed.revision_ulid, "01HQXYZ123REV");
    }

    #[test]
    fn test_watermark_freshness() {
        let mut watermarks = Watermarks::default();
        watermarks.last_processed_at = Utc::now() - Duration::seconds(10);

        // Fresh within 30s
        assert!(watermarks.is_fresh(Duration::seconds(30)));

        // Not fresh within 5s
        assert!(!watermarks.is_fresh(Duration::seconds(5)));
    }

    #[test]
    fn test_l0_compaction_trigger() {
        let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
        manifest.l0_limits.max_count = 5;

        // Not triggered initially
        assert!(!manifest.should_compact_l0());

        // Triggered at limit
        manifest.l0_count = 5;
        assert!(manifest.should_compact_l0());

        // Triggered above limit
        manifest.l0_count = 10;
        assert!(manifest.should_compact_l0());
    }

    #[test]
    fn test_table_paths_all() {
        let mut tables = TablePaths::default();
        tables.runs = Some("runs/snapshot-01.parquet".to_string());
        tables.tasks = Some("tasks/snapshot-01.parquet".to_string());

        let all = tables.all();
        assert_eq!(all.len(), 2);
        assert_eq!(all.get("runs"), Some(&"runs/snapshot-01.parquet"));
        assert_eq!(all.get("tasks"), Some(&"tasks/snapshot-01.parquet"));
    }

    #[test]
    fn test_row_counts_total() {
        let counts = RowCounts {
            runs: 10,
            tasks: 100,
            dep_satisfaction: 300,
            timers: 50,
            dispatch_outbox: 40,
        };

        assert_eq!(counts.total(), 500);
    }

    #[test]
    fn test_manifest_with_l0_deltas() {
        let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");

        let delta = L0Delta {
            delta_id: "01HQXYZ456DEL".to_string(),
            created_at: Utc::now(),
            event_range: EventRange {
                from_event: "01HQXYZ001EVT".to_string(),
                to_event: "01HQXYZ010EVT".to_string(),
                event_count: 10,
            },
            tables: TablePaths {
                tasks: Some("l0/delta-01/tasks.parquet".to_string()),
                ..Default::default()
            },
            row_counts: RowCounts {
                tasks: 25,
                ..Default::default()
            },
        };

        manifest.l0_deltas.push(delta);
        manifest.l0_count = 1;

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        assert!(json.contains("l0/delta-01/tasks.parquet"));
        assert!(json.contains("01HQXYZ001EVT"));
    }
}
