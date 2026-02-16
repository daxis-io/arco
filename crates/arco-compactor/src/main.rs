//! # arco-compactor
//!
//! Compaction service for the Arco serverless lakehouse infrastructure.
//!
//! The compactor merges Tier 2 events into Tier 1 snapshots, maintaining
//! query performance while preserving the append-only event history.
//!
//! ## Modes
//!
//! - **Service Mode**: Runs continuously with HTTP health endpoints
//! - **CLI Mode**: Manual compaction for debugging or recovery
//!
//! ## Health Endpoints
//!
//! - `GET /health` - Shallow liveness check (always 200)
//! - `GET /ready` - Readiness check with compaction health status
//!
//! ## Usage
//!
//! ```bash
//! # Run as service (default)
//! arco-compactor serve --port 8081
//!
//! # Manual compaction
//! arco-compactor compact --tenant acme-corp
//!
//! # Dry run
//! arco-compactor compact --tenant acme-corp --dry-run
//! ```

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

pub mod anti_entropy;
mod metrics;
pub mod notification_consumer;
pub mod sync_compact;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use arco_catalog::Compactor;
use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_core::{CatalogDomain, InternalOidcConfig, InternalOidcError, InternalOidcVerifier};

use crate::notification_consumer::{
    EventNotification, NotificationConsumer, NotificationConsumerConfig,
};

// ============================================================================
// CLI Arguments
// ============================================================================

const COMPACTION_DOMAINS: [&str; 4] = ["catalog", "lineage", "executions", "search"];
const AUTO_ANTI_ENTROPY_DOMAINS: [CatalogDomain; 3] = [
    CatalogDomain::Catalog,
    CatalogDomain::Lineage,
    CatalogDomain::Executions,
];
const COMPACTION_LAG_UPDATE_SECS: u64 = 30;

/// Arco catalog compactor.
#[derive(Debug, Parser)]
#[command(name = "arco-compactor")]
#[command(about = "Compacts Tier 2 events into Tier 1 snapshots")]
#[command(version)]
struct Args {
    /// Tenant ID for scoped compaction (single-tenant service mode).
    #[arg(long, env = "ARCO_TENANT_ID", global = true)]
    tenant_id: Option<String>,

    /// Workspace ID for scoped compaction (single-tenant service mode).
    #[arg(long, env = "ARCO_WORKSPACE_ID", global = true)]
    workspace_id: Option<String>,

    /// Object storage bucket name (e.g., `my-bucket`, `gs://my-bucket`, `s3://my-bucket`).
    #[arg(long, env = "ARCO_STORAGE_BUCKET", global = true)]
    storage_bucket: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run as a service with health endpoints.
    Serve {
        /// HTTP port for health endpoints.
        #[arg(long, env = "ARCO_COMPACTOR_PORT", default_value = "8081")]
        port: u16,

        /// Compaction interval in seconds.
        #[arg(long, env = "ARCO_COMPACTOR_INTERVAL_SECS", default_value = "60")]
        interval_secs: u64,

        /// Maximum time without successful compaction before unhealthy (seconds).
        #[arg(
            long,
            env = "ARCO_COMPACTOR_UNHEALTHY_THRESHOLD_SECS",
            default_value = "300"
        )]
        unhealthy_threshold_secs: u64,

        #[arg(
            long,
            env = "ARCO_COMPACTOR_URL",
            help = "Base URL for this compactor (used for /internal/notify)."
        )]
        compactor_url: Option<String>,

        #[arg(
            long,
            env = "ARCO_COMPACTOR_AUDIENCE",
            help = "Override audience for compactor identity tokens."
        )]
        compactor_audience: Option<String>,

        #[arg(
            long,
            env = "ARCO_COMPACTOR_ID_TOKEN",
            help = "Precomputed identity token for compactor invocation."
        )]
        compactor_id_token: Option<String>,

        #[arg(
            long,
            env = "ARCO_ANTI_ENTROPY_MAX_OBJECTS_PER_RUN",
            default_value = "1000",
            help = "Maximum objects to list per auto anti-entropy run."
        )]
        anti_entropy_max_objects_per_run: usize,

        #[arg(
            long,
            env = "ARCO_ANTI_ENTROPY_REPROCESS_BATCH_SIZE",
            default_value = "100",
            help = "Maximum paths per auto anti-entropy reprocessing request."
        )]
        anti_entropy_reprocess_batch_size: usize,

        #[arg(
            long,
            env = "ARCO_ANTI_ENTROPY_REPROCESS_TIMEOUT_SECS",
            default_value = "30",
            help = "HTTP timeout for auto anti-entropy reprocessing requests (seconds)."
        )]
        anti_entropy_reprocess_timeout_secs: u64,

        #[arg(
            long,
            env = "ARCO_METRICS_SECRET",
            help = "Optional shared secret for /metrics endpoint. Non-empty (trimmed) values enable the gate; requests must include X-Metrics-Secret or Authorization: Bearer header."
        )]
        metrics_secret: Option<String>,
    },

    /// Run a single compaction pass.
    Compact {
        /// Tenant ID to compact.
        #[arg(long, conflicts_with = "all")]
        tenant: Option<String>,

        /// Compact all tenants.
        #[arg(long, conflicts_with = "tenant")]
        all: bool,

        /// Perform a dry run without making changes.
        #[arg(long)]
        dry_run: bool,

        /// Minimum events before compaction (default: 1000).
        #[arg(long, default_value = "1000")]
        min_events: usize,
    },

    /// Run a single anti-entropy pass.
    AntiEntropy {
        /// Domain to scan (e.g., "catalog", "lineage").
        #[arg(long, default_value = "executions")]
        domain: String,

        /// Maximum objects to scan per run.
        #[arg(long, default_value = "1000")]
        max_objects_per_run: usize,

        #[arg(
            long,
            env = "ARCO_COMPACTOR_URL",
            help = "Base URL for the compactor service (used for /internal/notify)."
        )]
        compactor_url: Option<String>,

        #[arg(
            long,
            env = "ARCO_COMPACTOR_AUDIENCE",
            help = "Override audience for compactor identity tokens."
        )]
        compactor_audience: Option<String>,

        #[arg(
            long,
            env = "ARCO_COMPACTOR_ID_TOKEN",
            help = "Precomputed identity token for compactor invocation."
        )]
        compactor_id_token: Option<String>,

        #[arg(
            long,
            env = "ARCO_ANTI_ENTROPY_REPROCESS_BATCH_SIZE",
            help = "Maximum paths per reprocessing request."
        )]
        reprocess_batch_size: Option<usize>,

        #[arg(
            long,
            env = "ARCO_ANTI_ENTROPY_REPROCESS_TIMEOUT_SECS",
            help = "HTTP timeout for reprocessing requests in seconds."
        )]
        reprocess_timeout_secs: Option<u64>,
    },
}

// ============================================================================
// Scoped Configuration (Single-Tenant Mode)
// ============================================================================

/// Scoped configuration for single-tenant compactor instances.
struct ScopedConfig {
    tenant_id: String,
    workspace_id: String,
    storage_bucket: String,
}

impl ScopedConfig {
    fn from_args(args: &Args) -> Result<Self> {
        let tenant_id = args
            .tenant_id
            .clone()
            .ok_or_else(|| anyhow!("missing ARCO_TENANT_ID (required for service mode)"))?;
        let workspace_id = args
            .workspace_id
            .clone()
            .ok_or_else(|| anyhow!("missing ARCO_WORKSPACE_ID (required for service mode)"))?;
        let storage_bucket = args
            .storage_bucket
            .clone()
            .ok_or_else(|| anyhow!("missing ARCO_STORAGE_BUCKET (required for service mode)"))?;

        Ok(Self {
            tenant_id,
            workspace_id,
            storage_bucket,
        })
    }

    fn scoped_storage(&self) -> Result<ScopedStorage> {
        let backend = ObjectStoreBackend::from_bucket(&self.storage_bucket)?;
        let backend: Arc<dyn StorageBackend> = Arc::new(backend);
        ScopedStorage::new(backend, &self.tenant_id, &self.workspace_id)
            .map_err(anyhow::Error::from)
    }
}

// ============================================================================
// Health State
// ============================================================================

/// Shared state for tracking compaction health.
#[derive(Debug)]
struct CompactorState {
    /// Whether the service is ready to accept work.
    ready: AtomicBool,
    /// Unix timestamp of last successful compaction.
    last_successful_compaction_ts: AtomicU64,
    /// Total successful compaction cycles.
    successful_compactions: AtomicU64,
    /// Total failed compaction cycles.
    failed_compactions: AtomicU64,
    /// Whether a compaction cycle is currently running.
    compaction_in_progress: AtomicBool,
    /// Serializes compaction cycles to avoid concurrent runs.
    compaction_lock: Mutex<()>,
    /// Threshold (seconds) before marking unhealthy.
    unhealthy_threshold_secs: u64,
}

impl CompactorState {
    fn new(unhealthy_threshold_secs: u64) -> Self {
        Self {
            ready: AtomicBool::new(false),
            last_successful_compaction_ts: AtomicU64::new(0),
            successful_compactions: AtomicU64::new(0),
            failed_compactions: AtomicU64::new(0),
            compaction_in_progress: AtomicBool::new(false),
            compaction_lock: Mutex::new(()),
            unhealthy_threshold_secs,
        }
    }

    fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    fn record_success(&self) {
        let now: u64 = Utc::now().timestamp().try_into().unwrap_or_default();
        self.last_successful_compaction_ts
            .store(now, Ordering::Release);
        self.successful_compactions.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.failed_compactions.fetch_add(1, Ordering::Relaxed);
    }

    fn is_healthy(&self) -> bool {
        if !self.ready.load(Ordering::Acquire) {
            return false;
        }

        if self.successful_compactions.load(Ordering::Acquire) == 0 {
            // HARD GATE: we are not healthy until we've completed at least one
            // successful compaction cycle. This prevents serving traffic against a
            // potentially stale/uninitialized snapshot state.
            return false;
        }

        let last = self.last_successful_compaction_ts.load(Ordering::Acquire);
        if last == 0 {
            return false;
        }

        let now: u64 = Utc::now().timestamp().try_into().unwrap_or_default();
        let elapsed = now.saturating_sub(last);
        elapsed < self.unhealthy_threshold_secs
    }

    fn last_successful_compaction(&self) -> Option<DateTime<Utc>> {
        let ts = self.last_successful_compaction_ts.load(Ordering::Acquire);
        if ts == 0 {
            None
        } else {
            let ts = i64::try_from(ts).ok()?;
            DateTime::from_timestamp(ts, 0)
        }
    }
}

/// Shared state for HTTP handlers (health + sync compaction).
#[derive(Clone)]
struct ServiceState {
    compactor: Arc<CompactorState>,
    storage: ScopedStorage,
    tenant_id: String,
    workspace_id: String,
    notification_consumer: Arc<Mutex<NotificationConsumer>>,
    auto_anti_entropy: AutoAntiEntropyConfig,
}

#[derive(Clone)]
struct AutoAntiEntropyConfig {
    compactor_url: String,
    compactor_audience: Option<String>,
    compactor_id_token: Option<String>,
    max_objects_per_run: usize,
    reprocess_batch_size: usize,
    reprocess_timeout_secs: u64,
}

#[derive(Clone)]
struct InternalAuthState {
    verifier: Arc<InternalOidcVerifier>,
    enforce: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NotifyRequest {
    event_paths: Vec<String>,
    #[serde(default)]
    flush: bool,
}

// ============================================================================
// Health Endpoints
// ============================================================================

/// Health check response.
#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
}

/// Readiness check response.
#[derive(Debug, Serialize)]
struct ReadyResponse {
    ready: bool,
    healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_successful_compaction: Option<String>,
    successful_compactions: u64,
    failed_compactions: u64,
    compaction_in_progress: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

/// GET /health - Shallow liveness check.
async fn health() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

/// GET /ready - Readiness check with compaction health.
async fn ready(State(state): State<Arc<ServiceState>>) -> impl IntoResponse {
    let ready = state.compactor.ready.load(Ordering::Acquire);
    let healthy = state.compactor.is_healthy();
    let last_successful = state.compactor.last_successful_compaction();
    let successful_compactions = state
        .compactor
        .successful_compactions
        .load(Ordering::Relaxed);
    let failed_compactions = state.compactor.failed_compactions.load(Ordering::Relaxed);
    let compaction_in_progress = state
        .compactor
        .compaction_in_progress
        .load(Ordering::Acquire);

    let message = if !ready {
        Some("Service starting up".to_string())
    } else if successful_compactions == 0 {
        Some("Waiting for first successful compaction".to_string())
    } else if !healthy {
        Some(format!(
            "No successful compaction in {} seconds",
            state.compactor.unhealthy_threshold_secs
        ))
    } else {
        None
    };

    let status = if ready && healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        Json(ReadyResponse {
            ready,
            healthy,
            last_successful_compaction: last_successful.map(|dt| dt.to_rfc3339()),
            successful_compactions,
            failed_compactions,
            compaction_in_progress,
            message,
        }),
    )
}

/// POST /internal/anti-entropy - Trigger an anti-entropy pass (Gate 5).
///
/// This endpoint runs a bounded anti-entropy scan to detect missed events.
/// It lists ledger objects (the ONLY component that lists!) and compares
/// to the compaction watermark.
///
/// Returns:
/// - `200 OK` with scan results
/// - `409 Conflict` if anti-entropy is already running
/// - `501 Not Implemented` if anti-entropy is not yet wired
/// - `500 Internal Server Error` on failure
async fn anti_entropy_handler(
    State(state): State<Arc<ServiceState>>,
    Json(request): Json<anti_entropy::AntiEntropyConfig>,
) -> impl IntoResponse {
    // Check if anti-entropy is already running
    if state
        .compactor
        .compaction_in_progress
        .load(Ordering::Acquire)
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "busy",
                "message": "Compaction or anti-entropy is already in progress"
            })),
        );
    }

    let Ok(compaction_guard) = state.compactor.compaction_lock.try_lock() else {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "busy",
                "message": "Compaction or anti-entropy is already in progress"
            })),
        );
    };

    if state
        .compactor
        .compaction_in_progress
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "busy",
                "message": "Compaction or anti-entropy is already in progress"
            })),
        );
    }

    let mut job = anti_entropy::AntiEntropyJob::new(state.storage.clone(), request);
    let response = match job.run_pass().await {
        Ok(result) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "completed",
                "objects_scanned": result.objects_scanned,
                "missed_events": result.missed_events,
                "scan_complete": result.scan_complete,
                "duration_ms": result.duration_ms
            })),
        ),
        Err(anti_entropy::AntiEntropyError::NotImplemented { message }) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({
                "error": "not_implemented",
                "message": message
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "anti_entropy_failed",
                "message": e.to_string()
            })),
        ),
    };

    state
        .compactor
        .compaction_in_progress
        .store(false, Ordering::Release);
    drop(compaction_guard);

    response
}

async fn notify_handler(
    State(state): State<Arc<ServiceState>>,
    Json(request): Json<NotifyRequest>,
) -> impl IntoResponse {
    if request.event_paths.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid_request",
                "message": "event_paths must not be empty"
            })),
        );
    }

    let mut consumer = state.notification_consumer.lock().await;
    let mut should_flush = request.flush;
    let mut notifications = Vec::with_capacity(request.event_paths.len());

    for path in request.event_paths {
        let notification = match EventNotification::from_path(
            path,
            state.tenant_id.clone(),
            state.workspace_id.clone(),
        ) {
            Ok(notification) => notification,
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "invalid_path",
                        "message": err.to_string()
                    })),
                );
            }
        };

        if !consumer.accepts_domain(&notification.domain) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "unsupported_domain",
                    "message": format!("domain '{}' is not configured", notification.domain)
                })),
            );
        }

        notifications.push(notification);
    }

    for notification in notifications {
        if consumer.add_event(notification) {
            should_flush = true;
        }
    }

    if should_flush {
        match consumer.flush().await {
            Ok(result) => {
                let has_errors = result
                    .domain_results
                    .values()
                    .any(|domain_result| domain_result.error.is_some());
                if has_errors {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({
                            "error": "processing_error",
                            "result": result
                        })),
                    );
                }
                (StatusCode::OK, Json(serde_json::json!(result)))
            }
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "processing_error",
                    "message": err.to_string()
                })),
            ),
        }
    } else {
        (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "status": "queued",
                "pending": consumer.pending_count()
            })),
        )
    }
}

/// POST /internal/sync-compact - Synchronous compaction for Tier-1 DDL (ADR-018).
///
/// This endpoint is called by API while holding a distributed lock.
/// It processes explicit event paths (no listing) and publishes the manifest.
///
/// Returns:
/// - `200 OK` with manifest version on success
/// - `400 Bad Request` if request is invalid
/// - `409 Conflict` if fencing token is stale
/// - `501 Not Implemented` if sync compaction is not yet wired
/// - `500 Internal Server Error` on processing failure
async fn sync_compact_handler(
    State(state): State<Arc<ServiceState>>,
    Json(request): Json<sync_compact::SyncCompactRequest>,
) -> impl IntoResponse {
    let handler = sync_compact::SyncCompactHandler::new(state.storage.clone());

    match handler.handle(request).await {
        Ok(response) => (StatusCode::OK, Json(serde_json::json!(response))),
        Err(sync_compact::SyncCompactError::StaleFencingToken { .. }) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "stale_fencing_token",
                "message": "Fencing token does not match current lock holder"
            })),
        ),
        Err(sync_compact::SyncCompactError::UnsupportedDomain { domain }) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "unsupported_domain",
                "message": format!("Domain '{}' is not supported for sync compaction", domain)
            })),
        ),
        Err(sync_compact::SyncCompactError::NotImplemented { domain, message }) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({
                "error": "not_implemented",
                "domain": domain,
                "message": message
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "processing_error",
                "message": e.to_string()
            })),
        ),
    }
}

/// POST /compact - Trigger a compaction cycle on-demand.
///
/// Returns:
/// - `202 Accepted` if a new compaction cycle was started
/// - `409 Conflict` if a compaction cycle is already in progress
async fn compact(State(state): State<Arc<ServiceState>>) -> impl IntoResponse {
    if state
        .compactor
        .compaction_in_progress
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "status": "already_running",
                "message": "Compaction is already in progress"
            })),
        );
    }

    let state_clone = Arc::clone(&state);
    tokio::spawn(async move {
        run_compaction_cycle_guarded(&state_clone).await;
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "status": "started",
            "message": "Compaction triggered"
        })),
    )
}

// Compaction Loop
// ============================================================================

/// Runs the compaction loop in service mode.
async fn run_compaction_loop(state: Arc<ServiceState>, interval: Duration) {
    let mut interval_timer = tokio::time::interval(interval);

    // Mark as ready after first tick (startup complete).
    //
    // Note: the first `tick()` completes immediately to align the interval.
    interval_timer.tick().await;
    state.compactor.mark_ready();
    tracing::info!("Compactor ready, starting compaction loop");

    // Run a compaction cycle immediately on startup so readiness can become healthy
    // without waiting a full interval.
    run_compaction_cycle_guarded(&state).await;

    loop {
        interval_timer.tick().await;

        tracing::info!("Starting compaction cycle");

        run_compaction_cycle_guarded(&state).await;
    }
}

async fn run_compaction_cycle_guarded(state: &Arc<ServiceState>) {
    let _guard = state.compactor.compaction_lock.lock().await;

    // If this cycle was started by the periodic loop, `compaction_in_progress` may be false.
    // If it was started by `/compact`, it is already true. Either way, ensure it's true while
    // work is running and reset it at the end.
    state
        .compactor
        .compaction_in_progress
        .store(true, Ordering::Release);

    match run_compaction_cycle(state).await {
        Ok(()) => {
            state.compactor.record_success();
            tracing::info!("Compaction cycle completed successfully");
        }
        Err(e) => {
            state.compactor.record_failure();
            metrics::record_compaction_error("all");
            tracing::error!(error = %e, "Compaction cycle failed");
        }
    }

    state
        .compactor
        .compaction_in_progress
        .store(false, Ordering::Release);
}

/// Runs a single compaction cycle.
async fn run_compaction_cycle(state: &Arc<ServiceState>) -> Result<()> {
    let mut consumer = state.notification_consumer.lock().await;
    if consumer.pending_count() == 0 {
        drop(consumer);
        run_auto_anti_entropy(state).await?;
        return Ok(());
    }
    if !consumer.should_flush() {
        return Ok(());
    }

    let result = consumer.flush().await?;
    drop(consumer);
    let mut has_errors = false;

    for (domain, domain_result) in &result.domain_results {
        if domain_result.error.is_some() {
            metrics::record_compaction_error(domain);
            has_errors = true;
        }
    }

    if has_errors {
        return Err(anyhow!("compaction cycle completed with errors"));
    }

    Ok(())
}

async fn run_auto_anti_entropy(state: &Arc<ServiceState>) -> Result<()> {
    if state.auto_anti_entropy.max_objects_per_run == 0 {
        return Ok(());
    }

    for domain in AUTO_ANTI_ENTROPY_DOMAINS {
        let default_config = anti_entropy::AntiEntropyConfig::default();
        let config = anti_entropy::AntiEntropyConfig {
            domain: domain.as_str().to_string(),
            tenant_id: state.tenant_id.clone(),
            workspace_id: state.workspace_id.clone(),
            max_objects_per_run: state.auto_anti_entropy.max_objects_per_run,
            compactor_url: Some(state.auto_anti_entropy.compactor_url.clone()),
            compactor_audience: state.auto_anti_entropy.compactor_audience.clone(),
            compactor_id_token: state.auto_anti_entropy.compactor_id_token.clone(),
            reprocess_batch_size: state.auto_anti_entropy.reprocess_batch_size,
            reprocess_timeout_secs: state.auto_anti_entropy.reprocess_timeout_secs,
            ..default_config
        };

        let mut job = anti_entropy::AntiEntropyJob::new(state.storage.clone(), config);
        let result = job
            .run_pass()
            .await
            .map_err(|err| anyhow!("auto anti-entropy failed for {}: {err}", domain.as_str()))?;

        tracing::info!(
            domain = domain.as_str(),
            objects_scanned = result.objects_scanned,
            missed_events = result.missed_events,
            scan_complete = result.scan_complete,
            "auto anti-entropy pass completed"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_core::storage::MemoryBackend;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::util::ServiceExt;

    fn test_state() -> Arc<ServiceState> {
        let storage = Arc::new(MemoryBackend::new());
        let scoped_storage =
            ScopedStorage::new(storage, "acme", "analytics").expect("scoped storage");
        let compactor_state = Arc::new(CompactorState::new(60));
        let notification_consumer = NotificationConsumer::new(
            scoped_storage.clone(),
            NotificationConsumerConfig::default(),
        );
        let auto_anti_entropy = AutoAntiEntropyConfig {
            compactor_url: "http://127.0.0.1:8081".to_string(),
            compactor_audience: None,
            compactor_id_token: None,
            max_objects_per_run: 0,
            reprocess_batch_size: 0,
            reprocess_timeout_secs: 30,
        };
        Arc::new(ServiceState {
            compactor: compactor_state,
            storage: scoped_storage,
            tenant_id: "acme".to_string(),
            workspace_id: "analytics".to_string(),
            notification_consumer: Arc::new(Mutex::new(notification_consumer)),
            auto_anti_entropy,
        })
    }

    #[test]
    fn test_normalize_metrics_secret_trims_empty() {
        assert!(normalize_metrics_secret(Some("  ".to_string())).is_none());
        assert_eq!(
            normalize_metrics_secret(Some("  secret  ".to_string())),
            Some("secret".to_string())
        );
    }

    #[tokio::test]
    async fn test_metrics_gate_disabled_when_secret_empty() {
        metrics::init_metrics();
        let state = test_state();
        let router = build_router(
            state,
            normalize_metrics_secret(Some("  ".to_string())),
            None,
        );
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metrics_gate_accepts_x_metrics_secret() {
        metrics::init_metrics();
        let state = test_state();
        let router = build_router(state, Some("topsecret".to_string()), None);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header("X-Metrics-Secret", "topsecret")
                    .body(Body::empty())
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metrics_gate_accepts_bearer_secret() {
        metrics::init_metrics();
        let state = test_state();
        let router = build_router(state, Some("topsecret".to_string()), None);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header("Authorization", "Bearer topsecret")
                    .body(Body::empty())
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metrics_gate_rejects_missing_or_wrong_secret() {
        let state = test_state();
        let router = build_router(Arc::clone(&state), Some("topsecret".to_string()), None);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let router = build_router(state, Some("topsecret".to_string()), None);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header("X-Metrics-Secret", "wrong")
                    .body(Body::empty())
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_metrics_gate_accepts_when_both_headers_present() {
        metrics::init_metrics();
        let state = test_state();
        let router = build_router(state, Some("topsecret".to_string()), None);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header("X-Metrics-Secret", "wrong")
                    .header("Authorization", "Bearer topsecret")
                    .body(Body::empty())
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::OK);
    }
}

fn normalize_metrics_secret(secret: Option<String>) -> Option<String> {
    secret.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for i in 0..max_len {
        let left_byte = *left.get(i).unwrap_or(&0);
        let right_byte = *right.get(i).unwrap_or(&0);
        diff |= (left_byte ^ right_byte) as usize;
    }
    diff == 0
}

fn build_internal_auth() -> Result<Option<Arc<InternalAuthState>>> {
    let config =
        InternalOidcConfig::from_env().map_err(|e| anyhow!("invalid internal auth config: {e}"))?;
    let Some(config) = config else {
        return Ok(None);
    };

    let enforce = config.enforce;
    let verifier = InternalOidcVerifier::new(config)
        .map_err(|e| anyhow!("invalid internal auth config: {e}"))?;

    Ok(Some(Arc::new(InternalAuthState {
        verifier: Arc::new(verifier),
        enforce,
    })))
}

async fn internal_auth_middleware(
    State(state): State<Arc<InternalAuthState>>,
    request: axum::http::Request<Body>,
    next: Next,
) -> impl IntoResponse {
    match state.verifier.verify_headers(request.headers()).await {
        Ok(_) => next.run(request).await.into_response(),
        Err(err) => {
            if state.enforce {
                let message = match err {
                    InternalOidcError::MissingBearerToken => "missing bearer token".to_string(),
                    InternalOidcError::InvalidToken(reason) => format!("invalid token: {reason}"),
                    InternalOidcError::PrincipalNotAllowlisted => {
                        "principal not allowlisted".to_string()
                    }
                    InternalOidcError::JwksRefresh(reason) => {
                        format!("jwks refresh failed: {reason}")
                    }
                };
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({
                        "error": "unauthorized",
                        "message": message,
                    })),
                )
                    .into_response();
            }

            tracing::warn!(error = %err, "internal auth check failed in report-only mode");
            next.run(request).await.into_response()
        }
    }
}

fn build_router(
    state: Arc<ServiceState>,
    metrics_secret: Option<String>,
    internal_auth: Option<Arc<InternalAuthState>>,
) -> Router {
    // Build HTTP router
    // Note: /internal/anti-entropy is separate from /internal/sync-compact
    // because they have different IAM requirements:
    // - sync-compact: compactor-fastpath-sa (NO list)
    // - anti-entropy: compactor-antientropy-sa (WITH list)
    let compact_route = if let Some(auth) = internal_auth.clone() {
        post(compact).route_layer(middleware::from_fn_with_state(
            auth,
            internal_auth_middleware,
        ))
    } else {
        post(compact)
    };
    let notify_route = if let Some(auth) = internal_auth.clone() {
        post(notify_handler).route_layer(middleware::from_fn_with_state(
            auth,
            internal_auth_middleware,
        ))
    } else {
        post(notify_handler)
    };
    let sync_compact_route = if let Some(auth) = internal_auth.clone() {
        post(sync_compact_handler).route_layer(middleware::from_fn_with_state(
            auth,
            internal_auth_middleware,
        ))
    } else {
        post(sync_compact_handler)
    };
    let anti_entropy_route = if let Some(auth) = internal_auth {
        post(anti_entropy_handler).route_layer(middleware::from_fn_with_state(
            auth,
            internal_auth_middleware,
        ))
    } else {
        post(anti_entropy_handler)
    };

    let base_router = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/compact", compact_route)
        .route("/internal/notify", notify_route)
        .route("/internal/sync-compact", sync_compact_route)
        .route("/internal/anti-entropy", anti_entropy_route);

    let router = if let Some(secret) = metrics_secret {
        tracing::info!(
            "Metrics endpoint protected (accepts X-Metrics-Secret or Authorization: Bearer)"
        );
        let secret = Arc::<str>::from(secret);
        base_router.route(
            "/metrics",
            get(move |headers: axum::http::HeaderMap| {
                let secret = Arc::clone(&secret);
                async move {
                    let from_custom = headers
                        .get("X-Metrics-Secret")
                        .and_then(|v| v.to_str().ok());
                    let from_bearer = headers
                        .get("Authorization")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.strip_prefix("Bearer "));
                    let secret_bytes = secret.as_bytes();
                    let matches_custom = from_custom
                        .is_some_and(|value| constant_time_eq(value.as_bytes(), secret_bytes));
                    let matches_bearer = from_bearer
                        .is_some_and(|value| constant_time_eq(value.as_bytes(), secret_bytes));
                    if matches_custom || matches_bearer {
                        metrics::serve_metrics().await.into_response()
                    } else {
                        StatusCode::FORBIDDEN.into_response()
                    }
                }
            }),
        )
    } else {
        base_router.route("/metrics", get(metrics::serve_metrics))
    };

    router.with_state(state)
}

// ============================================================================
// Main Entry Point
// ============================================================================

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .json()
        .init();

    let args = Args::parse();
    let scoped = ScopedConfig::from_args(&args)?;

    match args.command {
        Commands::Serve {
            port,
            interval_secs,
            unhealthy_threshold_secs,
            compactor_url,
            compactor_audience,
            compactor_id_token,
            anti_entropy_max_objects_per_run,
            anti_entropy_reprocess_batch_size,
            anti_entropy_reprocess_timeout_secs,
            metrics_secret,
        } => {
            let scoped_storage = scoped.scoped_storage()?;
            let metrics_secret = normalize_metrics_secret(metrics_secret);
            let internal_auth = build_internal_auth()?;

            // Initialize metrics before starting
            metrics::init_metrics();
            arco_catalog::metrics::register_metrics();

            tracing::info!(
                port = port,
                interval_secs = interval_secs,
                unhealthy_threshold_secs = unhealthy_threshold_secs,
                tenant_id = %scoped.tenant_id,
                workspace_id = %scoped.workspace_id,
                "Starting compactor service"
            );

            let compactor_state = Arc::new(CompactorState::new(unhealthy_threshold_secs));
            let mut notification_config = NotificationConsumerConfig::default();
            for domain in ["catalog", "lineage", "executions"] {
                if !notification_config
                    .domains
                    .iter()
                    .any(|value| value == domain)
                {
                    notification_config.domains.push(domain.to_string());
                }
            }
            let notification_consumer =
                NotificationConsumer::new(scoped_storage.clone(), notification_config);
            let auto_anti_entropy = AutoAntiEntropyConfig {
                compactor_url: compactor_url.unwrap_or_else(|| format!("http://127.0.0.1:{port}")),
                compactor_audience,
                compactor_id_token,
                max_objects_per_run: anti_entropy_max_objects_per_run,
                reprocess_batch_size: anti_entropy_reprocess_batch_size,
                reprocess_timeout_secs: anti_entropy_reprocess_timeout_secs,
            };
            let state = Arc::new(ServiceState {
                compactor: Arc::clone(&compactor_state),
                storage: scoped_storage,
                tenant_id: scoped.tenant_id.clone(),
                workspace_id: scoped.workspace_id.clone(),
                notification_consumer: Arc::new(Mutex::new(notification_consumer)),
                auto_anti_entropy,
            });

            // Update compaction lag gauge periodically using last successful compaction as proxy.
            let lag_state = Arc::clone(&compactor_state);
            tokio::spawn(async move {
                let start = Utc::now();
                loop {
                    let now = Utc::now();
                    let lag_seconds = lag_state.last_successful_compaction().map_or_else(
                        || (now - start).num_seconds(),
                        |ts| (now - ts).num_seconds(),
                    );
                    let lag_seconds = u64::try_from(lag_seconds.max(0)).unwrap_or(u64::MAX);
                    #[allow(clippy::cast_precision_loss)]
                    let lag_seconds = lag_seconds as f64;

                    for domain in COMPACTION_DOMAINS {
                        metrics::set_compaction_lag(domain, lag_seconds);
                    }

                    tokio::time::sleep(Duration::from_secs(COMPACTION_LAG_UPDATE_SECS)).await;
                }
            });

            let router = build_router(Arc::clone(&state), metrics_secret, internal_auth);

            // Spawn compaction loop
            let state_clone = Arc::clone(&state);
            let interval = Duration::from_secs(interval_secs);
            tokio::spawn(async move {
                run_compaction_loop(state_clone, interval).await;
            });

            // Start HTTP server
            let addr = SocketAddr::from(([0, 0, 0, 0], port));
            tracing::info!(address = %addr, "Starting health server");

            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, router).await?;
        }

        Commands::Compact {
            ref tenant,
            all,
            dry_run,
            min_events,
        } => {
            tracing::info!(
                tenant = ?tenant,
                all = all,
                dry_run = dry_run,
                min_events = min_events,
                "Starting manual compaction"
            );

            let scoped_storage = scoped.scoped_storage()?;

            if dry_run {
                tracing::info!("Dry run mode - no changes will be made");
                return Ok(());
            }

            let compactor = Compactor::new(scoped_storage);
            let result = compactor.compact_domain(CatalogDomain::Executions).await?;

            tracing::info!(
                events_processed = result.events_processed,
                parquet_files_written = result.parquet_files_written,
                new_watermark = result.new_watermark,
                "Compaction complete"
            );
        }

        Commands::AntiEntropy {
            ref domain,
            max_objects_per_run,
            ref compactor_url,
            ref compactor_audience,
            ref compactor_id_token,
            reprocess_batch_size,
            reprocess_timeout_secs,
        } => {
            let scoped_storage = scoped.scoped_storage()?;

            let default_config = anti_entropy::AntiEntropyConfig::default();
            let config = anti_entropy::AntiEntropyConfig {
                domain: domain.clone(),
                tenant_id: scoped.tenant_id.clone(),
                workspace_id: scoped.workspace_id.clone(),
                max_objects_per_run,
                compactor_url: compactor_url.clone(),
                compactor_audience: compactor_audience.clone(),
                compactor_id_token: compactor_id_token.clone(),
                reprocess_batch_size: reprocess_batch_size
                    .unwrap_or(default_config.reprocess_batch_size),
                reprocess_timeout_secs: reprocess_timeout_secs
                    .unwrap_or(default_config.reprocess_timeout_secs),
                ..default_config
            };

            let mut job = anti_entropy::AntiEntropyJob::new(scoped_storage, config);

            let result = job
                .run_pass()
                .await
                .map_err(|err| anyhow!("anti-entropy run failed: {err}"))?;

            tracing::info!(
                objects_scanned = result.objects_scanned,
                missed_events = result.missed_events,
                scan_complete = result.scan_complete,
                "anti-entropy run completed"
            );
        }
    }

    Ok(())
}
