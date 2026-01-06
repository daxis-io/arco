//! Metrics middleware and instrumentation for the Iceberg REST API.

use std::sync::OnceLock;
use std::time::Instant;

use axum::extract::MatchedPath;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use metrics::{counter, describe_counter, describe_histogram, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// Iceberg request duration histogram.
pub const ICEBERG_REQUEST_DURATION: &str = "iceberg_request_duration_seconds";

/// Iceberg request counter.
pub const ICEBERG_REQUEST_TOTAL: &str = "iceberg_request_total";

/// Iceberg request error counter (4xx/5xx responses).
pub const ICEBERG_REQUEST_ERROR_TOTAL: &str = "iceberg_request_error_total";

/// Iceberg CAS conflict counter for write operations.
pub const ICEBERG_CAS_CONFLICT_TOTAL: &str = "iceberg_cas_conflict_total";

// ============================================================================
// Reconciler Metrics
// ============================================================================

/// Reconciler tables processed counter.
pub const RECONCILER_TABLES_PROCESSED: &str = "iceberg_reconciler_tables_processed_total";

/// Reconciler receipts backfilled counter.
pub const RECONCILER_RECEIPTS_BACKFILLED: &str = "iceberg_reconciler_receipts_backfilled_total";

/// Reconciler duration histogram.
pub const RECONCILER_DURATION: &str = "iceberg_reconciler_duration_seconds";

// ============================================================================
// GC Metrics
// ============================================================================

/// GC markers deleted counter.
pub const GC_MARKERS_DELETED: &str = "iceberg_gc_markers_deleted_total";

/// GC markers skipped counter.
pub const GC_MARKERS_SKIPPED: &str = "iceberg_gc_markers_skipped_total";

/// GC markers failed counter.
pub const GC_MARKERS_FAILED: &str = "iceberg_gc_markers_failed_total";

/// GC orphans deleted counter.
pub const GC_ORPHANS_DELETED: &str = "iceberg_gc_orphans_deleted_total";

/// GC event receipts deleted counter.
pub const GC_RECEIPTS_DELETED: &str = "iceberg_gc_receipts_deleted_total";

/// GC duration histogram.
pub const GC_DURATION: &str = "iceberg_gc_duration_seconds";

// ============================================================================
// Credential Vending Metrics
// ============================================================================

/// Credential vending request counter.
pub const CREDENTIAL_VENDING_TOTAL: &str = "iceberg_credential_vending_total";

/// Credential vending duration histogram.
pub const CREDENTIAL_VENDING_DURATION: &str = "iceberg_credential_vending_duration_seconds";

const UNMATCHED_ENDPOINT: &str = "unmatched";

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static METRICS_REGISTERED: OnceLock<()> = OnceLock::new();

/// Registers Iceberg metric descriptions.
///
/// Safe to call multiple times; subsequent calls are no-ops.
pub fn register_metrics() {
    METRICS_REGISTERED.get_or_init(|| {
        // Request metrics
        describe_histogram!(
            ICEBERG_REQUEST_DURATION,
            "Duration of Iceberg REST requests in seconds"
        );
        describe_counter!(
            ICEBERG_REQUEST_TOTAL,
            "Total number of Iceberg REST requests"
        );
        describe_counter!(
            ICEBERG_REQUEST_ERROR_TOTAL,
            "Total number of Iceberg REST requests returning 4xx/5xx"
        );
        describe_counter!(
            ICEBERG_CAS_CONFLICT_TOTAL,
            "Total number of Iceberg CAS conflicts"
        );

        // Reconciler metrics
        describe_counter!(
            RECONCILER_TABLES_PROCESSED,
            "Total number of tables processed by the reconciler"
        );
        describe_counter!(
            RECONCILER_RECEIPTS_BACKFILLED,
            "Total number of receipts backfilled by the reconciler"
        );
        describe_histogram!(
            RECONCILER_DURATION,
            "Duration of reconciliation runs in seconds"
        );

        // GC metrics
        describe_counter!(
            GC_MARKERS_DELETED,
            "Total number of idempotency markers deleted by GC"
        );
        describe_counter!(
            GC_MARKERS_SKIPPED,
            "Total number of idempotency markers skipped by GC (not yet eligible)"
        );
        describe_counter!(
            GC_MARKERS_FAILED,
            "Total number of idempotency markers that failed to delete"
        );
        describe_counter!(
            GC_ORPHANS_DELETED,
            "Total number of orphan metadata files deleted by GC"
        );
        describe_counter!(
            GC_RECEIPTS_DELETED,
            "Total number of event receipts deleted by GC"
        );
        describe_histogram!(GC_DURATION, "Duration of GC runs in seconds");

        describe_counter!(
            CREDENTIAL_VENDING_TOTAL,
            "Total number of credential vending requests"
        );
        describe_histogram!(
            CREDENTIAL_VENDING_DURATION,
            "Duration of credential vending requests in seconds"
        );
    });
}

/// Initializes the global Prometheus metrics recorder.
///
/// Safe to call multiple times; subsequent calls are no-ops.
///
/// # Panics
///
/// Panics if the Prometheus recorder cannot be installed. This is a
/// critical initialization failure that should not occur in normal
/// operation.
#[allow(clippy::panic)]
pub fn init_metrics() -> PrometheusHandle {
    PROMETHEUS_HANDLE
        .get_or_init(|| {
            let builder = PrometheusBuilder::new();
            let handle = builder
                .install_recorder()
                .unwrap_or_else(|e| panic!("failed to install prometheus recorder: {e}"));

            register_metrics();

            tracing::info!("Iceberg metrics recorder initialized");
            handle
        })
        .clone()
}

/// Returns the global Prometheus handle, if initialized.
#[must_use]
pub fn prometheus_handle() -> Option<PrometheusHandle> {
    PROMETHEUS_HANDLE.get().cloned()
}

fn endpoint_label<B>(request: &Request<B>) -> String {
    request.extensions().get::<MatchedPath>().map_or_else(
        || UNMATCHED_ENDPOINT.to_string(),
        |path| path.as_str().to_string(),
    )
}

/// Middleware that records request metrics.
pub async fn metrics_middleware(request: Request, next: Next) -> Response {
    let start = Instant::now();

    let path = endpoint_label(&request);
    let method = request.method().to_string();

    let response = next.run(request).await;

    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();
    let status_class = status_class(response.status());

    let labels = [
        ("endpoint", path.clone()),
        ("method", method.clone()),
        ("status_class", status_class.to_string()),
    ];

    histogram!(ICEBERG_REQUEST_DURATION, &labels).record(duration);
    counter!(ICEBERG_REQUEST_TOTAL, &labels).increment(1);
    if should_record_error(response.status()) {
        counter!(ICEBERG_REQUEST_ERROR_TOTAL, &labels).increment(1);
    }

    if duration > 1.0 {
        tracing::warn!(
            endpoint = %path,
            method = %method,
            status = %status,
            duration_secs = %duration,
            "Slow Iceberg request detected"
        );
    }

    response
}

/// Records a CAS conflict for an Iceberg write operation.
pub fn record_cas_conflict(endpoint: &str, method: &str, operation: &str) {
    register_metrics();
    let labels = [
        ("endpoint", endpoint.to_string()),
        ("method", method.to_string()),
        ("operation", operation.to_string()),
    ];
    counter!(ICEBERG_CAS_CONFLICT_TOTAL, &labels).increment(1);
}

// ============================================================================
// Reconciler Metrics Recording
// ============================================================================

/// Records a completed table reconciliation.
pub fn record_reconciler_table() {
    register_metrics();
    counter!(RECONCILER_TABLES_PROCESSED).increment(1);
}

/// Records backfilled receipts during reconciliation.
pub fn record_reconciler_receipts(count: u64) {
    register_metrics();
    counter!(RECONCILER_RECEIPTS_BACKFILLED).increment(count);
}

/// Records reconciliation run duration.
pub fn record_reconciler_duration(duration_secs: f64) {
    register_metrics();
    histogram!(RECONCILER_DURATION).record(duration_secs);
}

// ============================================================================
// GC Metrics Recording
// ============================================================================

/// Records deleted idempotency markers.
pub fn record_gc_markers_deleted(status: &str, count: u64) {
    register_metrics();
    let labels = [("status", status.to_string())];
    counter!(GC_MARKERS_DELETED, &labels).increment(count);
}

/// Records skipped idempotency markers.
pub fn record_gc_markers_skipped(count: u64) {
    register_metrics();
    counter!(GC_MARKERS_SKIPPED).increment(count);
}

/// Records failed marker deletions.
pub fn record_gc_markers_failed(count: u64) {
    register_metrics();
    counter!(GC_MARKERS_FAILED).increment(count);
}

/// Records deleted orphan metadata files.
pub fn record_gc_orphans_deleted(count: u64) {
    register_metrics();
    counter!(GC_ORPHANS_DELETED).increment(count);
}

/// Records deleted event receipts.
pub fn record_gc_receipts_deleted(receipt_type: &str, count: u64) {
    register_metrics();
    let labels = [("type", receipt_type.to_string())];
    counter!(GC_RECEIPTS_DELETED, &labels).increment(count);
}

/// Records GC run duration.
pub fn record_gc_duration(gc_type: &str, duration_secs: f64) {
    register_metrics();
    let labels = [("type", gc_type.to_string())];
    histogram!(GC_DURATION, &labels).record(duration_secs);
}

// ============================================================================
// Credential Vending Metrics Recording
// ============================================================================

/// Records a credential vending request.
pub fn record_credential_vending(provider: &str, status: &str) {
    register_metrics();
    let labels = [
        ("provider", provider.to_string()),
        ("status", status.to_string()),
    ];
    counter!(CREDENTIAL_VENDING_TOTAL, &labels).increment(1);
}

/// Records credential vending duration.
pub fn record_credential_vending_duration(provider: &str, duration_secs: f64) {
    register_metrics();
    let labels = [("provider", provider.to_string())];
    histogram!(CREDENTIAL_VENDING_DURATION, &labels).record(duration_secs);
}

fn status_class(status: StatusCode) -> &'static str {
    match status.as_u16() {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}

fn should_record_error(status: StatusCode) -> bool {
    status.is_client_error() || status.is_server_error()
}

/// Handler for the `/metrics` endpoint.
#[allow(clippy::must_use_candidate)] // Route handler, return value used by framework
pub fn serve_metrics() -> impl IntoResponse {
    prometheus_handle().map_or_else(
        || {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [("content-type", "text/plain; charset=utf-8")],
                "Metrics not initialized".to_string(),
            )
        },
        |handle| {
            let metrics = handle.render();
            (
                StatusCode::OK,
                [("content-type", "text/plain; charset=utf-8")],
                metrics,
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::routing::get;
    use tower::Service;

    fn metric_lines<'a>(metrics: &'a str, name: &str) -> Vec<&'a str> {
        metrics
            .lines()
            .filter(|line| line.starts_with(name))
            .collect()
    }

    fn assert_metric_lines_contain(metrics: &str, name: &str, needle: &str) {
        let lines = metric_lines(metrics, name);
        assert!(!lines.is_empty());
        assert!(lines.iter().any(|line| line.contains(needle)));
    }

    fn assert_metric_lines_do_not_contain(metrics: &str, name: &str, needle: &str) {
        let lines = metric_lines(metrics, name);
        assert!(!lines.is_empty());
        assert!(lines.iter().all(|line| !line.contains(needle)));
    }

    #[test]
    fn test_status_class() {
        assert_eq!(status_class(StatusCode::OK), "2xx");
        assert_eq!(status_class(StatusCode::NOT_FOUND), "4xx");
        assert_eq!(status_class(StatusCode::INTERNAL_SERVER_ERROR), "5xx");
    }

    #[test]
    fn test_should_record_error() {
        assert!(!should_record_error(StatusCode::OK));
        assert!(!should_record_error(StatusCode::NO_CONTENT));
        assert!(should_record_error(StatusCode::BAD_REQUEST));
        assert!(should_record_error(StatusCode::INTERNAL_SERVER_ERROR));
    }

    #[test]
    fn test_endpoint_label_unmatched() {
        let request = Request::builder()
            .uri("/missing")
            .body(Body::empty())
            .unwrap();
        let label = endpoint_label(&request);
        assert_eq!(label, UNMATCHED_ENDPOINT);
    }

    #[tokio::test]
    async fn test_request_metrics_labels() {
        let handle = init_metrics();
        let app = Router::new()
            .route("/tables/:id", get(|| async { StatusCode::OK }))
            .route_layer(axum::middleware::from_fn(metrics_middleware));
        let request = Request::builder()
            .uri("/tables/123")
            .body(Body::empty())
            .unwrap();
        let mut service = app.into_service::<Body>();
        let _response = service.call(request).await.unwrap();
        let metrics = handle.render();
        assert_metric_lines_contain(&metrics, ICEBERG_REQUEST_TOTAL, "endpoint=\"/tables/:id\"");
        assert!(!metrics.contains("endpoint=\"/tables/123\""));
        assert_metric_lines_do_not_contain(&metrics, ICEBERG_REQUEST_TOTAL, "status=\"");
        assert_metric_lines_do_not_contain(&metrics, ICEBERG_REQUEST_DURATION, "status=\"");
    }
}
