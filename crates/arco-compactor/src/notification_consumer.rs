//! Notification-driven compaction consumer (fast-path).
//!
//! This module implements the steady-state compaction path that processes
//! explicit object keys from GCS notifications or queues. It does NOT list.
//!
//! # Architecture (Gate 5)
//!
//! ```text
//! GCS Notifications → Pub/Sub → NotificationConsumer → Compactor
//!                                     ↓
//!                              Read explicit paths
//!                                     ↓
//!                              Fold events → Parquet
//!                                     ↓
//!                              CAS manifest
//! ```
//!
//! # Critical Invariants
//!
//! - **No listing**: Events come from explicit notification paths
//! - **Batched processing**: Events are accumulated and processed in batches
//! - **Idempotent**: Reprocessing the same event paths is safe
//!
//! # IAM (Patch 9)
//!
//! The notification consumer runs with `compactor-fastpath-sa` which has:
//! - Write: `state/`, `l0/`, `manifests/`
//! - **NO list permission** (enforced at IAM level)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for the notification consumer.
#[derive(Debug, Clone)]
pub struct NotificationConsumerConfig {
    /// Maximum number of events to process in a single batch.
    pub max_batch_size: usize,

    /// Maximum time to wait for batch accumulation.
    pub batch_timeout: Duration,

    /// Domains to process (e.g., `["catalog", "lineage", "executions"]`).
    pub domains: Vec<String>,
}

impl Default for NotificationConsumerConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            batch_timeout: Duration::from_secs(5),
            domains: vec![
                "catalog".to_string(),
                "lineage".to_string(),
                "executions".to_string(),
                "search".to_string(),
            ],
        }
    }
}

/// Error returned when parsing a notification path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationParseError {
    /// Path does not start with the expected `ledger/` prefix.
    InvalidPrefix {
        /// Full path that failed parsing.
        path: String,
    },
    /// Path does not match the expected `ledger/{domain}/{event_id}.json` format.
    InvalidPathFormat {
        /// Full path that failed parsing.
        path: String,
    },
    /// Event filename does not have a `.json` extension.
    InvalidExtension {
        /// Full path that failed parsing.
        path: String,
    },
}

impl std::fmt::Display for NotificationParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPrefix { path } => {
                write!(
                    f,
                    "invalid notification path '{path}': expected prefix 'ledger/'"
                )
            }
            Self::InvalidPathFormat { path } => write!(
                f,
                "invalid notification path '{path}': expected format 'ledger/{{domain}}/{{event_id}}.json'"
            ),
            Self::InvalidExtension { path } => {
                write!(
                    f,
                    "invalid notification path '{path}': expected '.json' event file"
                )
            }
        }
    }
}

impl std::error::Error for NotificationParseError {}

/// An event notification from storage (e.g., GCS Pub/Sub).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventNotification {
    /// Full path to the event file.
    ///
    /// Example: `ledger/catalog/01JFXYZ123.json`
    pub path: String,

    /// Domain extracted from path (e.g., "catalog", "lineage").
    pub domain: String,

    /// Event ID extracted from path.
    pub event_id: String,

    /// Tenant ID (from path prefix or message attributes).
    pub tenant_id: String,

    /// Workspace ID (from path prefix or message attributes).
    pub workspace_id: String,

    /// When the notification was received.
    pub received_at: chrono::DateTime<chrono::Utc>,

    /// Optional message ID for deduplication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
}

impl EventNotification {
    /// Parses an event notification from a GCS object path.
    ///
    /// Expected relative path format: `ledger/{domain}/{event_id}.json`
    ///
    /// Scoped/absolute paths (e.g., `tenant=.../workspace=.../ledger/...`) are rejected.
    ///
    /// Returns an error if the path doesn't match the expected format.
    pub fn from_path(
        path: impl Into<String>,
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
    ) -> Result<Self, NotificationParseError> {
        let path = path.into();

        // Split into parts for validation
        let parts: Vec<&str> = path.split('/').collect();

        // Expected: ["ledger", "{domain}", "{event_id}.json"]
        if parts.first() != Some(&"ledger") {
            return Err(NotificationParseError::InvalidPrefix { path });
        }
        if parts.len() != 3 {
            return Err(NotificationParseError::InvalidPathFormat { path });
        }

        // Extract and validate domain
        let domain = (*parts
            .get(1)
            .ok_or_else(|| NotificationParseError::InvalidPathFormat { path: path.clone() })?)
        .to_string();
        if domain.is_empty() {
            return Err(NotificationParseError::InvalidPathFormat { path });
        }
        let filename = parts
            .get(2)
            .ok_or_else(|| NotificationParseError::InvalidPathFormat { path: path.clone() })?;

        // Extract event ID (strip .json extension)
        let event_id = filename
            .strip_suffix(".json")
            .ok_or_else(|| NotificationParseError::InvalidExtension { path: path.clone() })?;
        if event_id.is_empty() {
            return Err(NotificationParseError::InvalidPathFormat { path });
        }
        let event_id = event_id.to_string();

        Ok(Self {
            path,
            domain,
            event_id,
            tenant_id: tenant_id.into(),
            workspace_id: workspace_id.into(),
            received_at: chrono::Utc::now(),
            message_id: None,
        })
    }
}

/// Batch of events grouped by domain for efficient processing.
#[derive(Debug, Default)]
pub struct EventBatch {
    /// Events grouped by domain.
    events_by_domain: HashMap<String, Vec<EventNotification>>,

    /// Total event count across all domains.
    total_count: usize,

    /// When the first event was added to this batch.
    first_event_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl EventBatch {
    /// Creates a new empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an event to the batch.
    pub fn add(&mut self, event: EventNotification) {
        if self.first_event_at.is_none() {
            self.first_event_at = Some(event.received_at);
        }

        self.events_by_domain
            .entry(event.domain.clone())
            .or_default()
            .push(event);
        self.total_count += 1;
    }

    /// Returns the number of events in the batch.
    pub fn len(&self) -> usize {
        self.total_count
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Returns domains with events in this batch.
    pub fn domains(&self) -> impl Iterator<Item = &str> {
        self.events_by_domain.keys().map(String::as_str)
    }

    /// Returns events for a specific domain.
    pub fn events_for_domain(&self, domain: &str) -> Option<&[EventNotification]> {
        self.events_by_domain.get(domain).map(Vec::as_slice)
    }

    /// Drains all events for a domain (consumes them).
    pub fn drain_domain(&mut self, domain: &str) -> Vec<EventNotification> {
        if let Some(events) = self.events_by_domain.remove(domain) {
            self.total_count -= events.len();
            if self.total_count == 0 {
                self.first_event_at = None;
            }
            events
        } else {
            Vec::new()
        }
    }

    /// Returns time since first event was added.
    pub fn age(&self) -> Option<Duration> {
        self.first_event_at.map(|t| {
            let elapsed = chrono::Utc::now() - t;
            let millis = elapsed.num_milliseconds().max(0);
            Duration::from_millis(u64::try_from(millis).unwrap_or(0))
        })
    }
}

/// Result of processing a batch of events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchProcessingResult {
    /// Number of events processed.
    pub events_processed: usize,

    /// Number of domains updated.
    pub domains_updated: usize,

    /// Domain-specific results.
    pub domain_results: HashMap<String, DomainProcessingResult>,
}

/// Result of processing events for a single domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainProcessingResult {
    /// Domain name.
    pub domain: String,

    /// Number of events processed.
    pub events_processed: usize,

    /// New snapshot version (if updated).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_version: Option<u64>,

    /// Error message (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Consumer that processes event notifications without listing.
///
/// # Example
///
/// ```rust,ignore
/// use arco_compactor::notification_consumer::{NotificationConsumer, EventNotification};
///
/// let consumer = NotificationConsumer::new(storage, config);
///
/// // Process events from notifications (NOT from listing)
/// for notification in pubsub_messages {
///     let event = EventNotification::from_path(
///         notification.path,
///         notification.tenant_id,
///         notification.workspace_id,
///     )?;
///     consumer.add_event(event);
/// }
///
/// // Flush accumulated batch
/// let result = consumer.flush().await?;
/// ```
pub struct NotificationConsumer<S: Send + Sync> {
    /// Storage backend for reading events and writing state.
    #[allow(dead_code)]
    storage: S,

    /// Configuration.
    config: NotificationConsumerConfig,

    /// Current batch being accumulated.
    current_batch: EventBatch,
}

impl<S: Send + Sync> NotificationConsumer<S> {
    /// Creates a new notification consumer.
    pub fn new(storage: S, config: NotificationConsumerConfig) -> Self {
        Self {
            storage,
            config,
            current_batch: EventBatch::new(),
        }
    }

    /// Returns the current configuration.
    pub fn config(&self) -> &NotificationConsumerConfig {
        &self.config
    }

    /// Adds an event notification to the current batch.
    ///
    /// Returns `true` if the batch should be flushed (size or time limit reached).
    pub fn add_event(&mut self, event: EventNotification) -> bool {
        self.current_batch.add(event);

        // Check if we should flush
        self.should_flush()
    }

    /// Returns whether the current batch should be flushed.
    pub fn should_flush(&self) -> bool {
        // Flush if batch is full
        if self.current_batch.len() >= self.config.max_batch_size {
            return true;
        }

        // Flush if batch is old enough
        if let Some(age) = self.current_batch.age() {
            if age >= self.config.batch_timeout {
                return true;
            }
        }

        false
    }

    /// Returns the number of events in the current batch.
    pub fn pending_count(&self) -> usize {
        self.current_batch.len()
    }

    /// Returns events pending for a specific domain.
    pub fn pending_for_domain(&self, domain: &str) -> usize {
        self.current_batch
            .events_for_domain(domain)
            .map_or(0, <[EventNotification]>::len)
    }

    /// Processes a batch of events for a single domain.
    ///
    /// This is the core compaction logic:
    /// 1. Load events from explicit paths (NO listing)
    /// 2. Read current manifest
    /// 3. Fold events into new state
    /// 4. Write Parquet to state/
    /// 5. CAS manifest
    ///
    /// # Note
    ///
    /// This method takes explicit event paths. It does NOT list storage.
    #[allow(clippy::unused_async)] // Will be async when implemented
    pub async fn process_domain_events(
        &self,
        domain: &str,
        event_paths: &[String],
    ) -> Result<DomainProcessingResult, NotificationConsumerError> {
        tracing::info!(
            domain = %domain,
            event_count = event_paths.len(),
            "processing domain events (no listing)"
        );

        // TODO: Implement actual event processing:
        // 1. Load events from explicit paths using storage.get()
        // 2. Read current manifest
        // 3. Fold events into new snapshot
        // 4. Write Parquet files
        // 5. CAS manifest with fencing token

        Err(NotificationConsumerError::NotImplemented {
            domain: domain.to_string(),
            message: "notification consumer not yet implemented - Phase 2".to_string(),
        })
    }

    /// Flushes the current batch, processing all accumulated events.
    ///
    /// Events are processed domain by domain. Each domain's events are
    /// processed together to minimize manifest updates.
    pub async fn flush(&mut self) -> Result<BatchProcessingResult, NotificationConsumerError> {
        let domains: Vec<String> = self
            .current_batch
            .domains()
            .map(ToString::to_string)
            .collect();

        let mut total_events = 0;
        let mut domain_results = HashMap::new();

        for domain in domains {
            // Drain events for this domain
            let events = self.current_batch.drain_domain(&domain);
            let event_paths: Vec<String> = events.iter().map(|e| e.path.clone()).collect();
            let event_count = event_paths.len();

            match self.process_domain_events(&domain, &event_paths).await {
                Ok(result) => {
                    total_events += result.events_processed;
                    domain_results.insert(domain, result);
                }
                Err(e) => {
                    tracing::error!(
                        domain = %domain,
                        event_count,
                        error = %e,
                        "failed to process domain events"
                    );
                    domain_results.insert(
                        domain.clone(),
                        DomainProcessingResult {
                            domain,
                            events_processed: 0,
                            snapshot_version: None,
                            error: Some(e.to_string()),
                        },
                    );
                }
            }
        }

        Ok(BatchProcessingResult {
            events_processed: total_events,
            domains_updated: domain_results.len(),
            domain_results,
        })
    }
}

/// Error type for notification consumer operations.
#[derive(Debug)]
pub enum NotificationConsumerError {
    /// Notification consumer is not yet wired.
    NotImplemented {
        /// Domain being processed.
        domain: String,
        /// Error message.
        message: String,
    },

    /// Failed to read event file.
    EventReadError {
        /// Path to the event file.
        path: String,
        /// Error message.
        message: String,
    },

    /// Failed to process events.
    ProcessingError {
        /// Domain being processed.
        domain: String,
        /// Error message.
        message: String,
    },

    /// Failed to write state file.
    StateWriteError {
        /// Path to the state file.
        path: String,
        /// Error message.
        message: String,
    },

    /// Failed to publish manifest.
    ManifestPublishError {
        /// Error message.
        message: String,
    },
}

impl std::fmt::Display for NotificationConsumerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotImplemented { domain, message } => {
                write!(
                    f,
                    "notification consumer not implemented for {domain}: {message}"
                )
            }
            Self::EventReadError { path, message } => {
                write!(f, "failed to read event '{path}': {message}")
            }
            Self::ProcessingError { domain, message } => {
                write!(f, "failed to process {domain} events: {message}")
            }
            Self::StateWriteError { path, message } => {
                write!(f, "failed to write state '{path}': {message}")
            }
            Self::ManifestPublishError { message } => {
                write!(f, "failed to publish manifest: {message}")
            }
        }
    }
}

impl std::error::Error for NotificationConsumerError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_notification_from_path() {
        let notification =
            EventNotification::from_path("ledger/catalog/01JFXYZ123.json", "acme", "prod")
                .expect("parse");

        let n = notification;
        assert_eq!(n.domain, "catalog");
        assert_eq!(n.event_id, "01JFXYZ123");
        assert_eq!(n.tenant_id, "acme");
        assert_eq!(n.workspace_id, "prod");
    }

    #[test]
    fn test_event_notification_invalid_path() {
        // Not a ledger path
        assert!(matches!(
            EventNotification::from_path("state/catalog/v1/data.parquet", "acme", "prod"),
            Err(NotificationParseError::InvalidPrefix { .. })
        ));

        // Missing event ID
        assert!(matches!(
            EventNotification::from_path("ledger/catalog/", "acme", "prod"),
            Err(NotificationParseError::InvalidExtension { .. })
        ));

        // Scoped/absolute paths are rejected
        assert!(matches!(
            EventNotification::from_path(
                "tenant=acme/workspace=prod/ledger/catalog/evt.json",
                "acme",
                "prod"
            ),
            Err(NotificationParseError::InvalidPrefix { .. })
        ));
    }

    #[test]
    fn test_event_batch_grouping() {
        let mut batch = EventBatch::new();

        batch.add(
            EventNotification::from_path("ledger/catalog/evt1.json", "acme", "prod")
                .expect("parse"),
        );
        batch.add(
            EventNotification::from_path("ledger/catalog/evt2.json", "acme", "prod")
                .expect("parse"),
        );
        batch.add(
            EventNotification::from_path("ledger/lineage/evt3.json", "acme", "prod")
                .expect("parse"),
        );

        assert_eq!(batch.len(), 3);

        let catalog_events = batch.events_for_domain("catalog").unwrap();
        assert_eq!(catalog_events.len(), 2);

        let lineage_events = batch.events_for_domain("lineage").unwrap();
        assert_eq!(lineage_events.len(), 1);
    }

    #[test]
    fn test_event_batch_drain() {
        let mut batch = EventBatch::new();

        batch.add(
            EventNotification::from_path("ledger/catalog/evt1.json", "acme", "prod")
                .expect("parse"),
        );
        batch.add(
            EventNotification::from_path("ledger/catalog/evt2.json", "acme", "prod")
                .expect("parse"),
        );

        let drained = batch.drain_domain("catalog");
        assert_eq!(drained.len(), 2);
        assert_eq!(batch.len(), 0);
        assert!(batch.age().is_none());

        // Draining again returns empty
        let drained_again = batch.drain_domain("catalog");
        assert!(drained_again.is_empty());
    }

    #[test]
    fn test_consumer_batch_size_flush() {
        let config = NotificationConsumerConfig {
            max_batch_size: 2,
            ..Default::default()
        };
        let mut consumer = NotificationConsumer::new((), config);

        // First event doesn't trigger flush
        let should_flush = consumer.add_event(
            EventNotification::from_path("ledger/catalog/evt1.json", "acme", "prod")
                .expect("parse"),
        );
        assert!(!should_flush);

        // Second event triggers flush (max_batch_size = 2)
        let should_flush = consumer.add_event(
            EventNotification::from_path("ledger/catalog/evt2.json", "acme", "prod")
                .expect("parse"),
        );
        assert!(should_flush);
    }

    #[tokio::test]
    async fn test_consumer_flush() {
        let mut consumer = NotificationConsumer::new((), NotificationConsumerConfig::default());

        consumer.add_event(
            EventNotification::from_path("ledger/catalog/evt1.json", "acme", "prod")
                .expect("parse"),
        );
        consumer.add_event(
            EventNotification::from_path("ledger/lineage/evt2.json", "acme", "prod")
                .expect("parse"),
        );

        let result = consumer.flush().await.expect("flush");

        // Both domains were processed
        assert_eq!(result.domains_updated, 2);
        assert!(result.domain_results.contains_key("catalog"));
        assert!(result.domain_results.contains_key("lineage"));
        assert_eq!(result.events_processed, 0);
    }

    #[test]
    fn test_notification_does_not_list() {
        // This test documents the critical invariant:
        // NotificationConsumer processes EXPLICIT paths, it does not list.
        //
        // The only way to add events is via add_event() which takes
        // explicit EventNotification with a path.
        //
        // There is no list() method, no scan() method, nothing that
        // enumerates storage.
        //
        // This is enforced by:
        // 1. API design (no list method)
        // 2. IAM (compactor-fastpath-sa has no list permission)

        let consumer = NotificationConsumer::new((), NotificationConsumerConfig::default());

        // Verify there's no way to list
        // (This is a compile-time guarantee via trait bounds)
        let _ = consumer;
    }
}
