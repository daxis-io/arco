//! Anti-entropy job for gap detection and recovery.
//!
//! This is the **ONLY** compactor component that lists objects.
//! It runs periodically to detect missed events that didn't arrive via notifications.
//!
//! # Architecture (Gate 5)
//!
//! ```text
//! Anti-Entropy Job (periodic)
//!     │
//!     ├── Load cursor from durable storage
//!     │
//!     ├── List one page of ledger objects (cursor-bounded)
//!     │
//!     ├── Compare each object to compaction watermark
//!     │
//!     ├── Enqueue missed events for reprocessing
//!     │
//!     └── Save cursor for next run
//! ```
//!
//! # Critical Invariants
//!
//! - **Bounded per run**: Only lists `max_objects_per_run` objects
//! - **Cursor persistence**: Cursor saved to storage, survives restarts
//! - **Eventually complete**: Full scan completes over multiple runs
//! - **Not on hot path**: Runs periodically, not per-request
//!
//! # IAM (Patch 9)
//!
//! Anti-entropy runs with `compactor-antientropy-sa` which has:
//! - List: `ledger/**` (can enumerate events)
//! - Read: `state/**` (can read manifests to check watermark)
//! - **NO write permission** (only enqueues for reprocessing)

use serde::{Deserialize, Serialize};

const DEFAULT_RUN_INTERVAL_SECS: u64 = 300;

fn default_run_interval_secs() -> u64 {
    DEFAULT_RUN_INTERVAL_SECS
}

/// Configuration for anti-entropy runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AntiEntropyConfig {
    /// Maximum objects to list per run.
    ///
    /// This bounds the amount of work per anti-entropy pass.
    /// A full scan completes over multiple runs.
    pub max_objects_per_run: usize,

    /// Domain to check (e.g., "catalog", "lineage").
    pub domain: String,

    /// Tenant ID for scoping the scan.
    pub tenant_id: String,

    /// Workspace ID for scoping the scan.
    pub workspace_id: String,

    /// Interval between anti-entropy runs (seconds).
    #[serde(default = "default_run_interval_secs")]
    pub run_interval_secs: u64,
}

impl Default for AntiEntropyConfig {
    fn default() -> Self {
        Self {
            max_objects_per_run: 1000,
            domain: "catalog".to_string(),
            tenant_id: String::new(),
            workspace_id: String::new(),
            run_interval_secs: default_run_interval_secs(),
        }
    }
}

/// Cursor for anti-entropy scans.
///
/// Persisted to storage so scans survive restarts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AntiEntropyCursor {
    /// Opaque continuation token from last list operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,

    /// Last object path scanned (for debugging/metrics).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_path: Option<String>,

    /// Total objects scanned in current full pass.
    pub objects_scanned: u64,

    /// When this cursor was last updated.
    pub last_updated: chrono::DateTime<chrono::Utc>,

    /// Number of complete full scans.
    pub completed_scans: u64,
}

impl AntiEntropyCursor {
    /// Creates a new cursor at the beginning.
    pub fn new() -> Self {
        Self {
            token: None,
            last_path: None,
            objects_scanned: 0,
            last_updated: chrono::Utc::now(),
            completed_scans: 0,
        }
    }

    /// Returns true if this cursor is at the start of a scan.
    pub fn is_at_start(&self) -> bool {
        self.token.is_none()
    }

    /// Updates cursor after processing a page.
    pub fn advance(&mut self, next_token: Option<String>, last_path: Option<String>) {
        self.token = next_token;
        self.last_path = last_path;
        self.last_updated = chrono::Utc::now();
    }

    /// Marks a complete scan finished and resets for next pass.
    pub fn complete_scan(&mut self) {
        self.token = None;
        self.last_path = None;
        self.objects_scanned = 0;
        self.completed_scans += 1;
        self.last_updated = chrono::Utc::now();
    }
}

/// Result of an anti-entropy run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AntiEntropyResult {
    /// Number of objects scanned in this run.
    pub objects_scanned: usize,

    /// Number of missed events found.
    pub missed_events: usize,

    /// Whether the full scan is complete.
    pub scan_complete: bool,

    /// Paths of missed events (for metrics/debugging).
    pub missed_paths: Vec<String>,

    /// Duration of this run.
    pub duration_ms: u64,
}

/// Anti-entropy job that discovers missed events via listing.
///
/// # IAM Requirement
///
/// This job MUST run with a service account that has list permission.
/// The fast-path compactor should NOT have list permission.
///
/// # Example
///
/// ```rust,ignore
/// use arco_compactor::anti_entropy::{AntiEntropyJob, AntiEntropyConfig};
///
/// let config = AntiEntropyConfig {
///     domain: "catalog".to_string(),
///     tenant_id: "acme".to_string(),
///     workspace_id: "prod".to_string(),
///     max_objects_per_run: 500,
///     ..Default::default()
/// };
///
/// let job = AntiEntropyJob::new(storage, config);
///
/// // Run one bounded pass
/// let result = job.run_pass().await?;
/// println!("Scanned {} objects, found {} missed",
///     result.objects_scanned, result.missed_events);
/// ```
pub struct AntiEntropyJob<S: Send + Sync> {
    /// Storage backend for listing and reading.
    #[allow(dead_code)]
    storage: S,

    /// Configuration.
    config: AntiEntropyConfig,

    /// Current cursor state (loaded from/saved to storage).
    cursor: AntiEntropyCursor,
}

impl<S: Send + Sync> AntiEntropyJob<S> {
    /// Creates a new anti-entropy job.
    pub fn new(storage: S, config: AntiEntropyConfig) -> Self {
        Self {
            storage,
            config,
            cursor: AntiEntropyCursor::new(),
        }
    }

    /// Returns the current cursor.
    pub fn cursor(&self) -> &AntiEntropyCursor {
        &self.cursor
    }

    /// Returns the configuration.
    pub fn config(&self) -> &AntiEntropyConfig {
        &self.config
    }

    /// Returns the cursor storage path.
    fn cursor_path(&self) -> String {
        format!(
            "state/anti_entropy/{}/cursor.json",
            self.config.domain
        )
    }

    /// Loads cursor from durable storage.
    ///
    /// Returns default cursor if none exists.
    #[allow(clippy::unused_async)]
    pub async fn load_cursor(&mut self) -> Result<(), AntiEntropyError> {
        // TODO: Load cursor from storage
        // let path = self.cursor_path();
        // let bytes = self.storage.get(&path).await?;
        // self.cursor = serde_json::from_slice(&bytes)?;

        tracing::debug!(
            path = %self.cursor_path(),
            "loading anti-entropy cursor"
        );

        Ok(())
    }

    /// Saves cursor to durable storage.
    #[allow(clippy::unused_async)]
    pub async fn save_cursor(&self) -> Result<(), AntiEntropyError> {
        // TODO: Save cursor to storage
        // let path = self.cursor_path();
        // let bytes = serde_json::to_vec(&self.cursor)?;
        // self.storage.put(&path, bytes).await?;

        tracing::debug!(
            path = %self.cursor_path(),
            token = ?self.cursor.token,
            objects_scanned = self.cursor.objects_scanned,
            "saving anti-entropy cursor"
        );

        Ok(())
    }

    /// Runs one bounded anti-entropy pass.
    ///
    /// # Process
    ///
    /// 1. Load cursor from durable storage
    /// 2. List one page of ledger objects (bounded by `max_objects_per_run`)
    /// 3. Compare each object to compaction watermark
    /// 4. Enqueue missed events for reprocessing
    /// 5. Save cursor for next run
    ///
    /// # Returns
    ///
    /// - `objects_scanned`: How many objects were checked
    /// - `missed_events`: How many events were behind watermark
    /// - `scan_complete`: Whether the full scan finished
    #[allow(clippy::unused_async)]
    pub async fn run_pass(&mut self) -> Result<AntiEntropyResult, AntiEntropyError> {
        // Phase 2 will wire storage listing, cursor persistence, and reprocessing.
        Err(AntiEntropyError::NotImplemented {
            message: "anti-entropy not yet implemented - Phase 2".to_string(),
        })
    }

    /// Checks if an event is "missed" (should have been compacted but wasn't).
    ///
    /// An event is missed if:
    /// - Its timestamp is older than the compaction watermark
    /// - It hasn't been included in any snapshot
    ///
    /// # Arguments
    ///
    /// - `event_path`: Path to the event file
    /// - `watermark_position`: Current compaction watermark from manifest
    #[allow(clippy::unused_async)]
    async fn is_missed(
        &self,
        _event_path: &str,
        _watermark_position: u64,
    ) -> Result<bool, AntiEntropyError> {
        // TODO: Implement missed event detection
        // 1. Parse event ID from path
        // 2. Compare to watermark position
        // 3. If event is older than watermark but not in snapshot, it's missed

        Ok(false)
    }

    /// Enqueues missed events for reprocessing.
    ///
    /// Missed events are sent to the notification consumer for processing.
    /// This could be via:
    /// - Direct call to NotificationConsumer
    /// - Publishing to a reprocessing queue
    /// - Writing to a "reprocess" ledger
    #[allow(clippy::unused_async)]
    async fn enqueue_for_reprocessing(
        &self,
        paths: &[String],
    ) -> Result<(), AntiEntropyError> {
        if paths.is_empty() {
            return Ok(());
        }

        tracing::info!(
            count = paths.len(),
            domain = %self.config.domain,
            "enqueueing missed events for reprocessing"
        );

        // TODO: Implement reprocessing enqueue
        // Options:
        // 1. Call NotificationConsumer::add_event() directly
        // 2. Publish to Pub/Sub reprocessing topic
        // 3. Write to a "missed_events" ledger for later processing

        Ok(())
    }
}

/// Error type for anti-entropy operations.
#[derive(Debug)]
pub enum AntiEntropyError {
    /// Anti-entropy is not yet wired.
    NotImplemented {
        /// Error message.
        message: String,
    },

    /// Failed to load cursor.
    CursorLoadError {
        /// Error message.
        message: String,
    },

    /// Failed to save cursor.
    CursorSaveError {
        /// Error message.
        message: String,
    },

    /// Failed to list objects.
    ListError {
        /// Prefix being listed.
        prefix: String,
        /// Error message.
        message: String,
    },

    /// Failed to read manifest/watermark.
    WatermarkError {
        /// Error message.
        message: String,
    },

    /// Failed to enqueue events.
    EnqueueError {
        /// Error message.
        message: String,
    },
}

impl std::fmt::Display for AntiEntropyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotImplemented { message } => write!(f, "{message}"),
            Self::CursorLoadError { message } => {
                write!(f, "failed to load cursor: {message}")
            }
            Self::CursorSaveError { message } => {
                write!(f, "failed to save cursor: {message}")
            }
            Self::ListError { prefix, message } => {
                write!(f, "failed to list '{prefix}': {message}")
            }
            Self::WatermarkError { message } => {
                write!(f, "failed to read watermark: {message}")
            }
            Self::EnqueueError { message } => {
                write!(f, "failed to enqueue events: {message}")
            }
        }
    }
}

impl std::error::Error for AntiEntropyError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_lifecycle() {
        let mut cursor = AntiEntropyCursor::new();
        assert!(cursor.is_at_start());
        assert_eq!(cursor.completed_scans, 0);

        // Advance through pages
        cursor.advance(Some("page2".to_string()), Some("ledger/catalog/evt1.json".to_string()));
        assert!(!cursor.is_at_start());
        assert_eq!(cursor.token, Some("page2".to_string()));

        cursor.advance(Some("page3".to_string()), Some("ledger/catalog/evt50.json".to_string()));
        assert_eq!(cursor.token, Some("page3".to_string()));

        // Complete scan
        cursor.complete_scan();
        assert!(cursor.is_at_start());
        assert_eq!(cursor.completed_scans, 1);
    }

    #[test]
    fn test_cursor_serialization() {
        let cursor = AntiEntropyCursor {
            token: Some("abc123".to_string()),
            last_path: Some("ledger/catalog/evt.json".to_string()),
            objects_scanned: 500,
            last_updated: chrono::Utc::now(),
            completed_scans: 3,
        };

        let json = serde_json::to_string(&cursor).expect("serialize");
        let parsed: AntiEntropyCursor = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(parsed.token, cursor.token);
        assert_eq!(parsed.objects_scanned, cursor.objects_scanned);
        assert_eq!(parsed.completed_scans, cursor.completed_scans);
    }

    #[tokio::test]
    async fn test_anti_entropy_pass_placeholder() {
        let config = AntiEntropyConfig {
            domain: "catalog".to_string(),
            tenant_id: "acme".to_string(),
            workspace_id: "prod".to_string(),
            max_objects_per_run: 100,
            ..Default::default()
        };

        let mut job = AntiEntropyJob::new((), config);

        let err = job.run_pass().await.expect_err("not implemented");
        assert!(matches!(err, AntiEntropyError::NotImplemented { .. }));
    }

    #[test]
    fn test_cursor_path_format() {
        let config = AntiEntropyConfig {
            domain: "lineage".to_string(),
            ..Default::default()
        };

        let job = AntiEntropyJob::new((), config);
        assert_eq!(job.cursor_path(), "state/anti_entropy/lineage/cursor.json");
    }

    #[test]
    fn test_anti_entropy_is_only_lister() {
        // This test documents the critical invariant:
        // AntiEntropyJob is the ONLY component that lists storage.
        //
        // The fast-path NotificationConsumer does NOT list.
        // IAM enforces this:
        // - compactor-fastpath-sa: NO list permission
        // - compactor-antientropy-sa: list permission on ledger/
        //
        // This separation ensures the hot path is never blocked by list latency.

        let config = AntiEntropyConfig::default();
        let job = AntiEntropyJob::new((), config);

        // The job has list capability (when storage trait is wired)
        // NotificationConsumer does NOT have list capability
        let _ = job;
    }

    #[test]
    fn test_bounded_per_run() {
        // Anti-entropy is bounded to prevent runaway scans
        let config = AntiEntropyConfig {
            max_objects_per_run: 500,
            ..Default::default()
        };

        // The job will list at most 500 objects per run
        assert_eq!(config.max_objects_per_run, 500);

        // A full scan completes over multiple runs
        // With 10,000 events and 500 per run, it takes 20 runs
        let total_events = 10_000;
        let runs_needed = (total_events + config.max_objects_per_run - 1) / config.max_objects_per_run;
        assert_eq!(runs_needed, 20);
    }
}
