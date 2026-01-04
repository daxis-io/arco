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
const UNMATCHED_ENDPOINT: &str = "unmatched";

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
            let handle = builder
                .install_recorder()
                .unwrap_or_else(|e| panic!("failed to install prometheus recorder: {e}"));

            // Register metric descriptions
            describe_histogram!(API_REQUEST_DURATION, "Duration of API requests in seconds");
            describe_counter!(API_REQUEST_TOTAL, "Total number of API requests");
            describe_counter!(
                SIGNED_URL_MINTED,
                "Total number of signed URLs minted for browser reads"
            );
            describe_counter!(
                RATE_LIMIT_HITS,
                "Total number of requests rejected by rate limiting"
            );

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

pub(crate) fn endpoint_label<B>(request: &Request<B>) -> String {
    request.extensions().get::<MatchedPath>().map_or_else(
        || UNMATCHED_ENDPOINT.to_string(),
        |path| path.as_str().to_string(),
    )
}

/// Middleware that records request metrics.
///
/// Captures:
/// - `api_request_duration_seconds{endpoint, method, status_class}` - histogram of request durations
/// - `api_request_total{endpoint, method, status_class}` - counter of total requests
pub async fn metrics_middleware(request: Request, next: Next) -> Response {
    let start = Instant::now();

    let path = endpoint_label(&request);

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

// ============================================================================
// Metric Recording Helpers
// ============================================================================

/// Records a signed URL minting event.
pub fn record_signed_url_minted() {
    counter!(SIGNED_URL_MINTED).increment(1);
}

/// Records a rate limit hit.
pub fn record_rate_limit_hit(endpoint: &str) {
    counter!(RATE_LIMIT_HITS, "endpoint" => endpoint.to_string()).increment(1);
}

// ============================================================================
// Tests
// ============================================================================

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
        assert_eq!(status_class(StatusCode::CREATED), "2xx");
        assert_eq!(status_class(StatusCode::MOVED_PERMANENTLY), "3xx");
        assert_eq!(status_class(StatusCode::BAD_REQUEST), "4xx");
        assert_eq!(status_class(StatusCode::NOT_FOUND), "4xx");
        assert_eq!(status_class(StatusCode::INTERNAL_SERVER_ERROR), "5xx");
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

    #[test]
    fn test_counters_without_tenant_labels() {
        let handle = init_metrics();
        record_signed_url_minted();
        record_rate_limit_hit("/api/v1/test");
        let metrics = handle.render();
        assert_metric_lines_do_not_contain(&metrics, SIGNED_URL_MINTED, "tenant=");
        assert_metric_lines_do_not_contain(&metrics, RATE_LIMIT_HITS, "tenant=");
    }

    #[tokio::test]
    async fn test_request_metrics_labels() {
        let handle = init_metrics();
        let app = Router::new()
            .route("/items/:id", get(|| async { StatusCode::OK }))
            .route_layer(axum::middleware::from_fn(metrics_middleware));
        let request = Request::builder()
            .uri("/items/123")
            .body(Body::empty())
            .unwrap();
        let mut service = app.into_service::<Body>();
        let _response = service.call(request).await.unwrap();
        let metrics = handle.render();
        assert_metric_lines_contain(&metrics, API_REQUEST_TOTAL, "endpoint=\"/items/:id\"");
        assert!(!metrics.contains("endpoint=\"/items/123\""));
        assert_metric_lines_do_not_contain(&metrics, API_REQUEST_TOTAL, "status=\"");
        assert_metric_lines_do_not_contain(&metrics, API_REQUEST_DURATION, "status=\"");
    }
}
