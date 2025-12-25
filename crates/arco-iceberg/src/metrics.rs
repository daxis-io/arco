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

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static METRICS_REGISTERED: OnceLock<()> = OnceLock::new();

/// Registers Iceberg metric descriptions.
///
/// Safe to call multiple times; subsequent calls are no-ops.
pub fn register_metrics() {
    METRICS_REGISTERED.get_or_init(|| {
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

/// Middleware that records request metrics.
pub async fn metrics_middleware(request: Request, next: Next) -> Response {
    let start = Instant::now();

    let path = request.extensions().get::<MatchedPath>().map_or_else(
        || request.uri().path().to_string(),
        |mp| mp.as_str().to_string(),
    );
    let method = request.method().to_string();

    let response = next.run(request).await;

    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();
    let status_class = status_class(response.status());

    let labels = [
        ("endpoint", path.clone()),
        ("method", method.clone()),
        ("status", status.clone()),
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
}
