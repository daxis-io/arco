//! Compactor metrics.
//!
//! Provides OpenTelemetry-compatible metrics for compaction operations:
//! - Compaction duration by domain
//! - Events processed per compaction
//! - Compaction lag (time since oldest uncompacted event)

use std::sync::OnceLock;
use std::time::Instant;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

// ============================================================================
// Metric Names (per plan Task 8.1)
// ============================================================================

/// Compaction cycle duration in seconds.
pub const COMPACTION_DURATION: &str = "compaction_duration_seconds";

/// Events processed per compaction cycle.
pub const COMPACTION_EVENTS_PROCESSED: &str = "compaction_events_processed";

/// Compaction lag (seconds since oldest uncompacted event).
pub const COMPACTION_LAG: &str = "compaction_lag_seconds";

/// Total compaction cycles.
pub const COMPACTION_CYCLES_TOTAL: &str = "compaction_cycles_total";

/// Compaction errors.
pub const COMPACTION_ERRORS_TOTAL: &str = "compaction_errors_total";

// ============================================================================
// Prometheus Recorder
// ============================================================================

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Initializes the global metrics recorder with Prometheus exporter.
///
/// Safe to call multiple times; subsequent calls are no-ops.
///
/// # Panics
///
/// Panics if the Prometheus recorder cannot be installed. This is intentional
/// as metrics are critical infrastructure and the service should not start without them.
#[allow(clippy::panic)]
pub fn init_metrics() -> PrometheusHandle {
    PROMETHEUS_HANDLE
        .get_or_init(|| {
            let builder = PrometheusBuilder::new();
            let handle = builder.install_recorder().unwrap_or_else(|e| {
                panic!("failed to install prometheus recorder: {e}")
            });

            // Register metric descriptions
            describe_histogram!(
                COMPACTION_DURATION,
                "Duration of compaction cycles in seconds"
            );
            describe_counter!(
                COMPACTION_EVENTS_PROCESSED,
                "Total events processed by compaction"
            );
            describe_gauge!(
                COMPACTION_LAG,
                "Seconds since oldest uncompacted event (compaction lag)"
            );
            describe_counter!(COMPACTION_CYCLES_TOTAL, "Total compaction cycles completed");
            describe_counter!(COMPACTION_ERRORS_TOTAL, "Total compaction errors");

            tracing::info!("Prometheus metrics recorder initialized for compactor");
            handle
        })
        .clone()
}

/// Returns the global Prometheus handle, if initialized.
#[must_use]
pub fn prometheus_handle() -> Option<PrometheusHandle> {
    PROMETHEUS_HANDLE.get().cloned()
}

// ============================================================================
// Metrics Endpoint
// ============================================================================

/// Handler for the `/metrics` endpoint.
pub async fn serve_metrics() -> impl IntoResponse {
    match prometheus_handle() {
        Some(handle) => {
            let metrics = handle.render();
            (
                StatusCode::OK,
                [("content-type", "text/plain; charset=utf-8")],
                metrics,
            )
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            [("content-type", "text/plain; charset=utf-8")],
            "Metrics not initialized".to_string(),
        ),
    }
}

// ============================================================================
// Metric Recording
// ============================================================================

/// Records a compaction cycle completion.
pub fn record_compaction_cycle(domain: &str, duration_secs: f64, events_processed: u64) {
    let labels = [("domain", domain.to_string())];

    histogram!(COMPACTION_DURATION, &labels).record(duration_secs);
    counter!(COMPACTION_EVENTS_PROCESSED, &labels).increment(events_processed);
    counter!(COMPACTION_CYCLES_TOTAL, &labels).increment(1);

    tracing::debug!(
        domain = %domain,
        duration_secs = %duration_secs,
        events_processed = %events_processed,
        "Recorded compaction metrics"
    );
}

/// Records a compaction error.
pub fn record_compaction_error(domain: &str) {
    counter!(COMPACTION_ERRORS_TOTAL, "domain" => domain.to_string()).increment(1);
}

/// Updates the compaction lag for a domain.
pub fn set_compaction_lag(domain: &str, lag_seconds: f64) {
    gauge!(COMPACTION_LAG, "domain" => domain.to_string()).set(lag_seconds);
}

/// RAII guard for measuring compaction duration.
pub struct CompactionTimer {
    domain: String,
    start: Instant,
}

impl CompactionTimer {
    /// Start timing a compaction for the given domain.
    #[must_use]
    pub fn start(domain: &str) -> Self {
        Self {
            domain: domain.to_string(),
            start: Instant::now(),
        }
    }

    /// Stop the timer and record metrics.
    pub fn finish(self, events_processed: u64) {
        let duration = self.start.elapsed().as_secs_f64();
        record_compaction_cycle(&self.domain, duration, events_processed);
    }
}
