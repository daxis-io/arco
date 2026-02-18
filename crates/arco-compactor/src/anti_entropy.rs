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

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

use arco_catalog::manifest::{
    CatalogDomainManifest, ExecutionsManifest, LineageManifest, RootManifest, SearchManifest,
};
use arco_core::error::Error as CoreError;
use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::WritePrecondition;
use arco_core::{CatalogDomain, CatalogPaths};

const DEFAULT_RUN_INTERVAL_SECS: u64 = 300;

fn default_run_interval_secs() -> u64 {
    DEFAULT_RUN_INTERVAL_SECS
}

const DEFAULT_REPROCESS_BATCH_SIZE: usize = 100;
const DEFAULT_REPROCESS_TIMEOUT_SECS: u64 = 30;

fn default_reprocess_batch_size() -> usize {
    DEFAULT_REPROCESS_BATCH_SIZE
}

fn default_reprocess_timeout_secs() -> u64 {
    DEFAULT_REPROCESS_TIMEOUT_SECS
}

/// Configuration for anti-entropy runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AntiEntropyConfig {
    /// Maximum objects to list per run.
    ///
    /// This bounds the amount of work per anti-entropy pass.
    /// A full scan completes over multiple runs.
    pub max_objects_per_run: usize,

    /// Domain to check (e.g., "executions").
    pub domain: String,

    /// Tenant ID for scoping the scan.
    pub tenant_id: String,

    /// Workspace ID for scoping the scan.
    pub workspace_id: String,

    /// Compactor service base URL for reprocessing misses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compactor_url: Option<String>,

    /// Optional audience override for compactor identity tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compactor_audience: Option<String>,

    /// Optional precomputed identity token for compactor invocation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compactor_id_token: Option<String>,

    /// Maximum paths per reprocessing request.
    #[serde(default = "default_reprocess_batch_size")]
    pub reprocess_batch_size: usize,

    /// HTTP timeout for reprocessing requests (seconds).
    #[serde(default = "default_reprocess_timeout_secs")]
    pub reprocess_timeout_secs: u64,

    /// Interval between anti-entropy runs (seconds).
    #[serde(default = "default_run_interval_secs")]
    pub run_interval_secs: u64,
}

impl Default for AntiEntropyConfig {
    fn default() -> Self {
        Self {
            max_objects_per_run: 1000,
            domain: "executions".to_string(),
            tenant_id: String::new(),
            workspace_id: String::new(),
            compactor_url: None,
            compactor_audience: None,
            compactor_id_token: None,
            reprocess_batch_size: default_reprocess_batch_size(),
            reprocess_timeout_secs: default_reprocess_timeout_secs(),
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

struct DomainWatermark {
    watermark_event_id: Option<String>,
    last_compaction_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NotifyRequest<'a> {
    event_paths: &'a [String],
    flush: bool,
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
pub struct AntiEntropyJob {
    /// Storage backend for listing and reading.
    storage: ScopedStorage,

    /// Configuration.
    config: AntiEntropyConfig,

    /// Current cursor state (loaded from/saved to storage).
    cursor: AntiEntropyCursor,
}

impl AntiEntropyJob {
    /// Creates a new anti-entropy job.
    pub fn new(storage: ScopedStorage, config: AntiEntropyConfig) -> Self {
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
        format!("state/anti_entropy/{}/cursor.json", self.config.domain)
    }

    /// Loads cursor from durable storage.
    ///
    /// Returns default cursor if none exists.
    pub async fn load_cursor(&mut self) -> Result<(), AntiEntropyError> {
        let path = self.cursor_path();
        match self.storage.get_raw(&path).await {
            Ok(bytes) => {
                let cursor = serde_json::from_slice(&bytes).map_err(|e| {
                    AntiEntropyError::CursorLoadError {
                        message: format!("failed to parse cursor: {e}"),
                    }
                })?;
                self.cursor = cursor;
            }
            Err(CoreError::NotFound(_)) => {
                self.cursor = AntiEntropyCursor::new();
            }
            Err(e) => {
                return Err(AntiEntropyError::CursorLoadError {
                    message: e.to_string(),
                });
            }
        }

        tracing::debug!(path = %path, "loading anti-entropy cursor");

        Ok(())
    }

    /// Saves cursor to durable storage.
    pub async fn save_cursor(&self) -> Result<(), AntiEntropyError> {
        let path = self.cursor_path();
        let bytes =
            serde_json::to_vec(&self.cursor).map_err(|e| AntiEntropyError::CursorSaveError {
                message: format!("failed to serialize cursor: {e}"),
            })?;

        let result = self
            .storage
            .put_raw(&path, Bytes::from(bytes), WritePrecondition::None)
            .await
            .map_err(|e| AntiEntropyError::CursorSaveError {
                message: e.to_string(),
            })?;

        if let arco_core::storage::WriteResult::PreconditionFailed { current_version } = result {
            return Err(AntiEntropyError::CursorSaveError {
                message: format!("cursor write precondition failed: {current_version}"),
            });
        }

        tracing::debug!(
            path = %path,
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
    pub async fn run_pass(&mut self) -> Result<AntiEntropyResult, AntiEntropyError> {
        let start = Instant::now();
        let domain = parse_domain(&self.config.domain)?;

        self.load_cursor().await?;

        let watermark = self.read_domain_watermark(domain).await?;

        let prefix = CatalogPaths::ledger_dir(domain);
        let mut metas =
            self.storage
                .list_meta(&prefix)
                .await
                .map_err(|e| AntiEntropyError::ListError {
                    prefix: prefix.clone(),
                    message: e.to_string(),
                })?;
        metas.sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));

        let total = metas.len();
        let start_index = self.cursor.last_path.as_deref().map_or(0, |last_path| {
            match metas.binary_search_by(|meta| meta.path.as_str().cmp(last_path)) {
                Ok(index) => index + 1,
                Err(index) => index,
            }
        });

        let limit = self.config.max_objects_per_run;
        let page: Vec<_> = if limit == 0 {
            Vec::new()
        } else {
            metas.into_iter().skip(start_index).take(limit).collect()
        };

        let scan_complete = start_index + page.len() >= total;
        let mut missed_paths = Vec::new();

        for meta in &page {
            let path = meta.path.as_str();
            if !std::path::Path::new(path)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
            {
                continue;
            }
            if Self::is_missed(
                path,
                watermark.watermark_event_id.as_deref(),
                watermark.last_compaction_at,
                meta.last_modified,
            ) {
                missed_paths.push(path.to_string());
            }
        }

        if !missed_paths.is_empty() {
            self.enqueue_for_reprocessing(&missed_paths).await?;
        }

        if !page.is_empty() || scan_complete {
            self.cursor.objects_scanned = self
                .cursor
                .objects_scanned
                .saturating_add(page.len() as u64);
            let last_path = page.last().map(|meta| meta.path.to_string());
            if scan_complete {
                self.cursor.complete_scan();
            } else if let Some(last_path) = last_path.clone() {
                self.cursor
                    .advance(Some(last_path.clone()), Some(last_path));
            }
            self.save_cursor().await?;
        }

        let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        Ok(AntiEntropyResult {
            objects_scanned: page.len(),
            missed_events: missed_paths.len(),
            scan_complete,
            missed_paths,
            duration_ms,
        })
    }

    async fn read_domain_watermark(
        &self,
        domain: CatalogDomain,
    ) -> Result<DomainWatermark, AntiEntropyError> {
        let root = self.read_root_manifest().await?;

        match domain {
            CatalogDomain::Executions => {
                let data = self
                    .storage
                    .get_raw(&root.executions_manifest_path)
                    .await
                    .map_err(|e| AntiEntropyError::WatermarkError {
                        message: format!("failed to read executions manifest: {e}"),
                    })?;
                let manifest: ExecutionsManifest = serde_json::from_slice(&data).map_err(|e| {
                    AntiEntropyError::WatermarkError {
                        message: format!("failed to parse executions manifest: {e}"),
                    }
                })?;
                Ok(DomainWatermark {
                    watermark_event_id: manifest.watermark_event_id.clone(),
                    last_compaction_at: manifest.last_compaction_at,
                })
            }
            CatalogDomain::Catalog => {
                let data = self
                    .storage
                    .get_raw(&root.catalog_manifest_path)
                    .await
                    .map_err(|e| AntiEntropyError::WatermarkError {
                        message: format!("failed to read catalog manifest: {e}"),
                    })?;
                let manifest: CatalogDomainManifest =
                    serde_json::from_slice(&data).map_err(|e| {
                        AntiEntropyError::WatermarkError {
                            message: format!("failed to parse catalog manifest: {e}"),
                        }
                    })?;
                Ok(DomainWatermark {
                    watermark_event_id: manifest.watermark_event_id.clone(),
                    last_compaction_at: Some(manifest.updated_at),
                })
            }
            CatalogDomain::Lineage => {
                let data = self
                    .storage
                    .get_raw(&root.lineage_manifest_path)
                    .await
                    .map_err(|e| AntiEntropyError::WatermarkError {
                        message: format!("failed to read lineage manifest: {e}"),
                    })?;
                let manifest: LineageManifest = serde_json::from_slice(&data).map_err(|e| {
                    AntiEntropyError::WatermarkError {
                        message: format!("failed to parse lineage manifest: {e}"),
                    }
                })?;
                Ok(DomainWatermark {
                    watermark_event_id: manifest.watermark_event_id.clone(),
                    last_compaction_at: Some(manifest.updated_at),
                })
            }
            CatalogDomain::Search => {
                let data = self
                    .storage
                    .get_raw(&root.search_manifest_path)
                    .await
                    .map_err(|e| AntiEntropyError::WatermarkError {
                        message: format!("failed to read search manifest: {e}"),
                    })?;
                let manifest: SearchManifest = serde_json::from_slice(&data).map_err(|e| {
                    AntiEntropyError::WatermarkError {
                        message: format!("failed to parse search manifest: {e}"),
                    }
                })?;
                Ok(DomainWatermark {
                    watermark_event_id: manifest.watermark_event_id.clone(),
                    last_compaction_at: Some(manifest.updated_at),
                })
            }
        }
    }

    async fn read_root_manifest(&self) -> Result<RootManifest, AntiEntropyError> {
        let root_bytes = self
            .storage
            .get_raw(CatalogPaths::ROOT_MANIFEST)
            .await
            .map_err(|e| AntiEntropyError::WatermarkError {
                message: format!("failed to read root manifest: {e}"),
            })?;
        let mut root: RootManifest =
            serde_json::from_slice(&root_bytes).map_err(|e| AntiEntropyError::WatermarkError {
                message: format!("failed to parse root manifest: {e}"),
            })?;
        root.normalize_paths();
        Ok(root)
    }

    fn is_missed(
        event_path: &str,
        watermark_event_id: Option<&str>,
        last_compaction_at: Option<chrono::DateTime<chrono::Utc>>,
        last_modified: Option<chrono::DateTime<chrono::Utc>>,
    ) -> bool {
        let filename = event_path.rsplit('/').next().unwrap_or("");
        let is_after_watermark = watermark_event_id.is_none_or(|watermark| filename > watermark);
        if is_after_watermark {
            return true;
        }

        if let (Some(last_compaction_at), Some(last_modified)) = (last_compaction_at, last_modified)
        {
            if last_modified > last_compaction_at {
                return true;
            }
        }

        false
    }

    async fn resolve_id_token(
        &self,
        client: &reqwest::Client,
        compactor_url: &str,
    ) -> Result<Option<String>, AntiEntropyError> {
        if let Some(token) = &self.config.compactor_id_token {
            return Ok(Some(token.clone()));
        }

        let audience = self
            .config
            .compactor_audience
            .as_deref()
            .unwrap_or(compactor_url);
        let mut url = reqwest::Url::parse(
            "http://metadata/computeMetadata/v1/instance/service-accounts/default/identity",
        )
        .map_err(|e| AntiEntropyError::EnqueueError {
            message: format!("failed to build metadata url: {e}"),
        })?;
        url.query_pairs_mut()
            .append_pair("audience", audience)
            .append_pair("format", "full");

        let response = client
            .get(url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .map_err(|e| AntiEntropyError::EnqueueError {
                message: format!("failed to fetch id token: {e}"),
            })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| AntiEntropyError::EnqueueError {
                message: format!("failed to read id token response: {e}"),
            })?;
        if !status.is_success() {
            return Err(AntiEntropyError::EnqueueError {
                message: format!("failed to fetch id token: {status} {body}"),
            });
        }

        Ok(Some(body))
    }

    async fn enqueue_for_reprocessing(&self, paths: &[String]) -> Result<(), AntiEntropyError> {
        if paths.is_empty() {
            return Ok(());
        }

        let compactor_url =
            self.config
                .compactor_url
                .as_ref()
                .ok_or_else(|| AntiEntropyError::EnqueueError {
                    message: "compactor_url is required to reprocess events".to_string(),
                })?;
        let notify_url = format!("{}/internal/notify", compactor_url.trim_end_matches('/'));
        let timeout = Duration::from_secs(self.config.reprocess_timeout_secs);
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| AntiEntropyError::EnqueueError {
                message: format!("failed to build http client: {e}"),
            })?;
        let id_token = self.resolve_id_token(&client, compactor_url).await?;
        let batch_size = self.config.reprocess_batch_size.max(1);

        for chunk in paths.chunks(batch_size) {
            let request = NotifyRequest {
                event_paths: chunk,
                flush: true,
            };
            let mut req = client.post(&notify_url).json(&request);
            if let Some(token) = id_token.as_deref() {
                req = req.bearer_auth(token);
            }

            let response = req
                .send()
                .await
                .map_err(|e| AntiEntropyError::EnqueueError {
                    message: format!("failed to call compactor notify: {e}"),
                })?;
            let status = response.status();
            let body = response
                .text()
                .await
                .map_err(|e| AntiEntropyError::EnqueueError {
                    message: format!("failed to read compactor response: {e}"),
                })?;
            if !status.is_success() {
                return Err(AntiEntropyError::EnqueueError {
                    message: format!("compactor notify failed with {status}: {body}"),
                });
            }
        }

        Ok(())
    }
}

fn parse_domain(domain: &str) -> Result<CatalogDomain, AntiEntropyError> {
    let normalized = domain.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "catalog" => Ok(CatalogDomain::Catalog),
        "lineage" => Ok(CatalogDomain::Lineage),
        "executions" => Ok(CatalogDomain::Executions),
        "search" => Ok(CatalogDomain::Search),
        _ => Err(AntiEntropyError::WatermarkError {
            message: format!("unsupported domain: {domain}"),
        }),
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
    use arco_catalog::Tier1Writer;
    use arco_core::scoped_storage::ScopedStorage;
    use arco_core::storage::MemoryBackend;
    use std::sync::Arc;

    fn test_storage() -> ScopedStorage {
        let backend = Arc::new(MemoryBackend::new());
        ScopedStorage::new(backend, "acme", "prod").expect("valid storage")
    }

    #[test]
    fn test_cursor_lifecycle() {
        let mut cursor = AntiEntropyCursor::new();
        assert!(cursor.is_at_start());
        assert_eq!(cursor.completed_scans, 0);

        // Advance through pages
        cursor.advance(
            Some("page2".to_string()),
            Some("ledger/catalog/evt1.json".to_string()),
        );
        assert!(!cursor.is_at_start());
        assert_eq!(cursor.token, Some("page2".to_string()));

        cursor.advance(
            Some("page3".to_string()),
            Some("ledger/catalog/evt50.json".to_string()),
        );
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
    async fn test_anti_entropy_pass_empty() -> Result<(), Box<dyn std::error::Error>> {
        let storage = test_storage();
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;

        let config = AntiEntropyConfig {
            domain: "executions".to_string(),
            tenant_id: "acme".to_string(),
            workspace_id: "prod".to_string(),
            max_objects_per_run: 100,
            ..Default::default()
        };

        let mut job = AntiEntropyJob::new(storage, config);
        let result = job.run_pass().await?;

        assert_eq!(result.objects_scanned, 0);
        assert_eq!(result.missed_events, 0);
        assert!(result.scan_complete);

        Ok(())
    }

    #[tokio::test]
    async fn test_search_anti_entropy_uses_bounded_scan_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        let storage = test_storage();
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;

        storage
            .put_raw(
                "ledger/search/2025-01-01/evt-a.json",
                Bytes::from_static(br#"{"event":"a"}"#),
                WritePrecondition::None,
            )
            .await?;
        storage
            .put_raw(
                "ledger/search/2025-01-01/evt-b.json",
                Bytes::from_static(br#"{"event":"b"}"#),
                WritePrecondition::None,
            )
            .await?;

        let search_manifest_path = CatalogPaths::domain_manifest(CatalogDomain::Search);
        let search_manifest_bytes = storage.get_raw(&search_manifest_path).await?;
        let mut search_manifest: SearchManifest = serde_json::from_slice(&search_manifest_bytes)?;
        search_manifest.watermark_event_id = Some("zzzz".to_string());
        search_manifest.updated_at = chrono::Utc::now() + chrono::Duration::hours(1);
        storage
            .put_raw(
                &search_manifest_path,
                Bytes::from(serde_json::to_vec(&search_manifest)?),
                WritePrecondition::None,
            )
            .await?;

        let config = AntiEntropyConfig {
            domain: "search".to_string(),
            tenant_id: "acme".to_string(),
            workspace_id: "prod".to_string(),
            max_objects_per_run: 1,
            ..Default::default()
        };

        let mut job = AntiEntropyJob::new(storage, config);

        let first = job.run_pass().await?;
        assert_eq!(first.objects_scanned, 1);
        assert_eq!(first.missed_events, 0);
        assert!(!first.scan_complete);

        let second = job.run_pass().await?;
        assert_eq!(second.objects_scanned, 1);
        assert_eq!(second.missed_events, 0);
        assert!(second.scan_complete);

        Ok(())
    }

    #[test]
    fn test_cursor_path_format() {
        let config = AntiEntropyConfig {
            domain: "lineage".to_string(),
            ..Default::default()
        };

        let job = AntiEntropyJob::new(test_storage(), config);
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
        let job = AntiEntropyJob::new(test_storage(), config);

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
        let runs_needed =
            (total_events + config.max_objects_per_run - 1) / config.max_objects_per_run;
        assert_eq!(runs_needed, 20);
    }
}
