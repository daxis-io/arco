//! Metrics middleware and instrumentation for the Arco API.
//!
//! Provides OpenTelemetry-compatible metrics for:
//! - Request duration and throughput
//! - Error rates by endpoint
//! - Signed URL minting
//! - Rate limit hits

use std::sync::OnceLock;
use std::time::Instant;

use axum::extract::{MatchedPath, Request};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use metrics::{counter, describe_counter, describe_histogram, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

// ============================================================================
// Metric Names (per plan Task 8.1)
// ============================================================================

/// API request duration histogram.
pub const API_REQUEST_DURATION: &str = "api_request_duration_seconds";

/// API request counter.
pub const API_REQUEST_TOTAL: &str = "api_request_total";

/// Signed URL minting counter.
pub const SIGNED_URL_MINTED: &str = "signed_url_minted_total";

/// Rate limit hit counter.
pub const RATE_LIMIT_HITS: &str = "rate_limit_hits_total";

/// CAS retry counter (for writer operations).
pub const CAS_RETRY: &str = "cas_retry_total";

// ============================================================================
// Prometheus Recorder
// ============================================================================

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Initializes the global metrics recorder with Prometheus exporter.
///
/// Safe to call multiple times; subsequent calls are no-ops.
///
/// Returns a handle for rendering metrics. The handle is also stored globally
/// for use by the `/metrics` endpoint.
///
/// # Panics
///
/// Panics if the Prometheus recorder cannot be installed. This is intentional
/// as metrics are critical infrastructure and server should not start without them.
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
                API_REQUEST_DURATION,
                "Duration of API requests in seconds"
            );
            describe_counter!(API_REQUEST_TOTAL, "Total number of API requests");
            describe_counter!(
                SIGNED_URL_MINTED,
                "Total number of signed URLs minted for browser reads"
            );
            describe_counter!(
                RATE_LIMIT_HITS,
                "Total number of requests rejected by rate limiting"
            );
            describe_counter!(CAS_RETRY, "Total number of CAS retries in write operations");

            tracing::info!("Prometheus metrics recorder initialized");
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
// Metrics Middleware
// ============================================================================

/// Middleware that records request metrics.
///
/// Captures:
/// - `api_request_duration_seconds{endpoint, status}` - histogram of request durations
/// - `api_request_total{endpoint, status}` - counter of total requests
pub async fn metrics_middleware(request: Request, next: Next) -> Response {
    let start = Instant::now();

    // Extract the matched path (e.g., "/api/v1/namespaces/:namespace_id")
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| request.uri().path().to_string(), |mp| mp.as_str().to_string());

    let method = request.method().to_string();

    // Execute the request
    let response = next.run(request).await;

    // Record metrics
    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();
    let status_class = status_class(response.status());

    let labels = [
        ("endpoint", path.clone()),
        ("method", method.clone()),
        ("status", status.clone()),
        ("status_class", status_class.to_string()),
    ];

    histogram!(API_REQUEST_DURATION, &labels).record(duration);
    counter!(API_REQUEST_TOTAL, &labels).increment(1);

    // Log slow requests (> 1s)
    if duration > 1.0 {
        tracing::warn!(
            endpoint = %path,
            method = %method,
            status = %status,
            duration_secs = %duration,
            "Slow request detected"
        );
    }

    response
}

/// Returns the status class (2xx, 3xx, 4xx, 5xx) for a status code.
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

// ============================================================================
// Metrics Endpoint
// ============================================================================

/// Handler for the `/metrics` endpoint.
///
/// Returns Prometheus-formatted metrics text.
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
// Metric Recording Helpers
// ============================================================================

/// Records a signed URL minting event.
pub fn record_signed_url_minted(tenant: &str) {
    counter!(SIGNED_URL_MINTED, "tenant" => tenant.to_string()).increment(1);
}

/// Records a rate limit hit.
pub fn record_rate_limit_hit(tenant: &str, endpoint: &str) {
    counter!(
        RATE_LIMIT_HITS,
        "tenant" => tenant.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(1);
}

/// Records a CAS retry.
pub fn record_cas_retry(operation: &str) {
    counter!(CAS_RETRY, "operation" => operation.to_string()).increment(1);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_class() {
        assert_eq!(status_class(StatusCode::OK), "2xx");
        assert_eq!(status_class(StatusCode::CREATED), "2xx");
        assert_eq!(status_class(StatusCode::MOVED_PERMANENTLY), "3xx");
        assert_eq!(status_class(StatusCode::BAD_REQUEST), "4xx");
        assert_eq!(status_class(StatusCode::NOT_FOUND), "4xx");
        assert_eq!(status_class(StatusCode::INTERNAL_SERVER_ERROR), "5xx");
    }
}
