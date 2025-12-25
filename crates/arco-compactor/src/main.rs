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
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use serde::Serialize;
use tokio::sync::Mutex;

use arco_core::scoped_storage::ScopedStorage;
use arco_core::storage::{ObjectStoreBackend, StorageBackend};

// ============================================================================
// CLI Arguments
// ============================================================================

const COMPACTION_DOMAINS: [&str; 4] = ["catalog", "lineage", "executions", "search"];
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
        #[arg(long, default_value = "catalog")]
        domain: String,

        /// Maximum objects to scan per run.
        #[arg(long, default_value = "1000")]
        max_objects_per_run: usize,
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

    // Run anti-entropy pass
    let mut job = anti_entropy::AntiEntropyJob::new((), request);
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

    let state_clone = Arc::clone(&state.compactor);
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

// ============================================================================
// Compaction Loop
// ============================================================================

/// Runs the compaction loop in service mode.
async fn run_compaction_loop(state: Arc<CompactorState>, interval: Duration) {
    let mut interval_timer = tokio::time::interval(interval);

    // Mark as ready after first tick (startup complete).
    //
    // Note: the first `tick()` completes immediately to align the interval.
    interval_timer.tick().await;
    state.mark_ready();
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

async fn run_compaction_cycle_guarded(state: &Arc<CompactorState>) {
    let _guard = state.compaction_lock.lock().await;

    // If this cycle was started by the periodic loop, `compaction_in_progress` may be false.
    // If it was started by `/compact`, it is already true. Either way, ensure it's true while
    // work is running and reset it at the end.
    state.compaction_in_progress.store(true, Ordering::Release);

    match run_compaction_cycle().await {
        Ok(()) => {
            state.record_success();
            tracing::info!("Compaction cycle completed successfully");
        }
        Err(e) => {
            state.record_failure();
            metrics::record_compaction_error("all");
            tracing::error!(error = %e, "Compaction cycle failed");
        }
    }

    state.compaction_in_progress.store(false, Ordering::Release);
}

/// Runs a single compaction cycle.
async fn run_compaction_cycle() -> Result<()> {
    // TODO: Implement actual compaction logic
    // This will call arco_catalog::Compactor::compact_domain() for each domain
    //
    // When implemented, use metrics::CompactionTimer for each domain:
    //   let timer = metrics::CompactionTimer::start("catalog");
    //   let result = compact_domain("catalog").await;
    //   timer.finish(events_processed);

    // For now, record a simulated compaction for all domains
    for domain in COMPACTION_DOMAINS {
        let timer = metrics::CompactionTimer::start(domain);

        // Simulate some work per domain
        tokio::time::sleep(Duration::from_millis(25)).await;

        // Record simulated metrics (0 events processed for now)
        timer.finish(0);
    }

    Ok(())
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

    match args.command {
        Commands::Serve {
            port,
            interval_secs,
            unhealthy_threshold_secs,
        } => {
            let scoped = ScopedConfig::from_args(&args)?;
            let scoped_storage = scoped.scoped_storage()?;

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
            let state = Arc::new(ServiceState {
                compactor: Arc::clone(&compactor_state),
                storage: scoped_storage,
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

            // Build HTTP router
            // Note: /internal/anti-entropy is separate from /internal/sync-compact
            // because they have different IAM requirements:
            // - sync-compact: compactor-fastpath-sa (NO list)
            // - anti-entropy: compactor-antientropy-sa (WITH list)
            let router = Router::new()
                .route("/health", get(health))
                .route("/ready", get(ready))
                .route("/metrics", get(metrics::serve_metrics))
                .route("/compact", post(compact))
                .route("/internal/sync-compact", post(sync_compact_handler))
                .route("/internal/anti-entropy", post(anti_entropy_handler))
                .with_state(Arc::clone(&state));

            // Spawn compaction loop
            let state_clone = Arc::clone(&compactor_state);
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
            tenant,
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

            if dry_run {
                tracing::info!("Dry run mode - no changes will be made");
            }

            // TODO: Implement actual compaction logic
            run_compaction_cycle().await?;

            tracing::info!("Compaction complete");
        }

        Commands::AntiEntropy {
            ref domain,
            max_objects_per_run,
        } => {
            let scoped = ScopedConfig::from_args(&args)?;
            let scoped_storage = scoped.scoped_storage()?;

            let config = anti_entropy::AntiEntropyConfig {
                domain: domain.clone(),
                tenant_id: scoped.tenant_id,
                workspace_id: scoped.workspace_id,
                max_objects_per_run,
                ..anti_entropy::AntiEntropyConfig::default()
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
