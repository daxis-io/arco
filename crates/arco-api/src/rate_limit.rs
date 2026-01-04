//! Rate limiting middleware for denial-of-wallet prevention.
//!
//! Implements per-tenant rate limiting with configurable quotas per endpoint.
//! URL minting has lower limits since it's an expensive operation.
//!
//! ## Rate Limit Categories
//!
//! - **Default**: 500 req/min per tenant
//! - **URL Minting**: 100 req/min per tenant (expensive operation)
//!
//! ## Response Headers
//!
//! When rate limited, returns:
//! - `429 Too Many Requests` status
//! - `Retry-After` header with seconds to wait
//! - `X-RateLimit-Limit` with the configured limit
//! - `X-RateLimit-Remaining` with remaining quota

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderValue, Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::context::RequestContext;

// ============================================================================
// Configuration
// ============================================================================

/// Rate limiting configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Whether rate limiting is enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Default requests per minute per tenant.
    #[serde(default = "default_requests_per_minute")]
    pub default_requests_per_minute: u32,

    /// URL minting requests per minute per tenant (lower limit for expensive operation).
    #[serde(default = "default_url_minting_per_minute")]
    pub url_minting_requests_per_minute: u32,

    /// Maximum burst size (requests allowed above steady rate).
    #[serde(default = "default_burst_size")]
    pub burst_size: u32,
}

const fn default_enabled() -> bool {
    true
}

const fn default_requests_per_minute() -> u32 {
    500
}

const fn default_url_minting_per_minute() -> u32 {
    100
}

const fn default_burst_size() -> u32 {
    50
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            default_requests_per_minute: default_requests_per_minute(),
            url_minting_requests_per_minute: default_url_minting_per_minute(),
            burst_size: default_burst_size(),
        }
    }
}

// ============================================================================
// Rate Limiter State
// ============================================================================

/// Per-tenant rate limiter using in-memory state.
type TenantLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

/// Rate limiting state shared across all request handlers.
#[derive(Clone)]
pub struct RateLimitState {
    config: RateLimitConfig,
    /// Per-tenant rate limiters for default endpoints.
    default_limiters: Arc<RwLock<HashMap<String, Arc<TenantLimiter>>>>,
    /// Per-tenant rate limiters for URL minting (lower limits).
    url_minting_limiters: Arc<RwLock<HashMap<String, Arc<TenantLimiter>>>>,
}

impl std::fmt::Debug for RateLimitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitState")
            .field("config", &self.config)
            .field("default_limiters", &"<HashMap>")
            .field("url_minting_limiters", &"<HashMap>")
            .finish()
    }
}

impl RateLimitState {
    /// Creates new rate limit state with the given configuration.
    #[must_use]
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            default_limiters: Arc::new(RwLock::new(HashMap::new())),
            url_minting_limiters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Gets or creates a rate limiter for the given tenant.
    async fn get_or_create_limiter(
        limiters: &RwLock<HashMap<String, Arc<TenantLimiter>>>,
        tenant: &str,
        requests_per_minute: u32,
        burst_size: u32,
    ) -> Arc<TenantLimiter> {
        // Fast path: check if limiter exists
        {
            let read_guard = limiters.read().await;
            if let Some(limiter) = read_guard.get(tenant) {
                return Arc::clone(limiter);
            }
        }

        // Slow path: create new limiter
        let mut write_guard = limiters.write().await;

        // Double-check after acquiring write lock
        if let Some(limiter) = write_guard.get(tenant) {
            return Arc::clone(limiter);
        }

        // Create quota: X requests per minute with burst capacity
        let replenish_rate = NonZeroU32::new(requests_per_minute.max(1)).unwrap_or(NonZeroU32::MIN);
        let burst = NonZeroU32::new(burst_size.max(1)).unwrap_or(NonZeroU32::MIN);

        let quota = Quota::per_minute(replenish_rate).allow_burst(burst);
        let limiter = Arc::new(RateLimiter::direct(quota));

        write_guard.insert(tenant.to_string(), Arc::clone(&limiter));
        limiter
    }

    /// Checks rate limit for a default endpoint.
    pub async fn check_default(&self, tenant: &str) -> RateLimitResult {
        if !self.config.enabled {
            return RateLimitResult::Allowed {
                limit: 0,
                remaining: 0,
            };
        }

        let limiter = Self::get_or_create_limiter(
            &self.default_limiters,
            tenant,
            self.config.default_requests_per_minute,
            self.config.burst_size,
        )
        .await;

        Self::check_limiter(&limiter, self.config.default_requests_per_minute)
    }

    /// Checks rate limit for URL minting (lower limit).
    pub async fn check_url_minting(&self, tenant: &str) -> RateLimitResult {
        if !self.config.enabled {
            return RateLimitResult::Allowed {
                limit: 0,
                remaining: 0,
            };
        }

        let limiter = Self::get_or_create_limiter(
            &self.url_minting_limiters,
            tenant,
            self.config.url_minting_requests_per_minute,
            self.config.burst_size / 2, // Lower burst for expensive operations
        )
        .await;

        Self::check_limiter(&limiter, self.config.url_minting_requests_per_minute)
    }

    fn check_limiter(limiter: &TenantLimiter, limit: u32) -> RateLimitResult {
        match limiter.check() {
            Ok(()) => {
                // Estimate remaining (approximate, not exact)
                let remaining = limiter
                    .check()
                    .map(|()| limit.saturating_sub(1))
                    .unwrap_or(0);
                RateLimitResult::Allowed { limit, remaining }
            }
            Err(not_until) => {
                let retry_after =
                    not_until.wait_time_from(governor::clock::Clock::now(&DefaultClock::default()));
                RateLimitResult::Limited {
                    limit,
                    retry_after_secs: retry_after.as_secs(),
                }
            }
        }
    }
}

/// Result of a rate limit check.
#[derive(Debug)]
pub enum RateLimitResult {
    /// Request is allowed.
    Allowed {
        /// Configured limit.
        limit: u32,
        /// Approximate remaining requests in window.
        remaining: u32,
    },
    /// Request is rate limited.
    Limited {
        /// Configured limit.
        limit: u32,
        /// Seconds until the client can retry.
        retry_after_secs: u64,
    },
}

// ============================================================================
// Middleware
// ============================================================================

/// Rate limiting middleware for Axum.
///
/// Extracts tenant from request context and applies rate limits.
/// URL minting endpoint has a lower limit.
pub async fn rate_limit_middleware(
    State(rate_limit): State<Arc<RateLimitState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    // Extract tenant from the verified request context (injected by auth middleware).
    let (tenant, request_id) = req.extensions().get::<RequestContext>().map_or_else(
        || ("unknown".to_string(), "-".to_string()),
        |ctx| (ctx.tenant.clone(), ctx.request_id.clone()),
    );

    let path = req.uri().path();
    let endpoint = crate::metrics::endpoint_label(&req);

    // Check appropriate rate limit based on endpoint
    let result = if path.contains("/browser/urls") {
        rate_limit.check_url_minting(&tenant).await
    } else {
        rate_limit.check_default(&tenant).await
    };

    match result {
        RateLimitResult::Allowed { limit, remaining } => {
            let mut response = next.run(req).await;

            // Add rate limit headers to successful responses
            if limit > 0 {
                add_rate_limit_headers(response.headers_mut(), limit, remaining);
            }

            response
        }
        RateLimitResult::Limited {
            limit,
            retry_after_secs,
        } => {
            // Log rate limit hit
            tracing::warn!(
                tenant = %tenant,
                path = %path,
                endpoint = %endpoint,
                request_id = %request_id,
                limit = limit,
                retry_after_secs = retry_after_secs,
                "Rate limit exceeded"
            );

            // Record rate limit metric
            crate::metrics::record_rate_limit_hit(endpoint.as_str());

            rate_limit_response(limit, retry_after_secs)
        }
    }
}

fn add_rate_limit_headers(headers: &mut axum::http::HeaderMap, limit: u32, remaining: u32) {
    if let Ok(v) = HeaderValue::from_str(&limit.to_string()) {
        headers.insert(header::HeaderName::from_static("x-ratelimit-limit"), v);
    }
    if let Ok(v) = HeaderValue::from_str(&remaining.to_string()) {
        headers.insert(header::HeaderName::from_static("x-ratelimit-remaining"), v);
    }
}

fn rate_limit_response(limit: u32, retry_after_secs: u64) -> Response {
    let body = serde_json::json!({
        "code": "RATE_LIMITED",
        "message": format!(
            "Rate limit exceeded. Limit: {} requests per minute. Retry after {} seconds.",
            limit, retry_after_secs
        ),
    });

    let mut response = (StatusCode::TOO_MANY_REQUESTS, axum::Json(body)).into_response();

    let headers = response.headers_mut();

    if let Ok(v) = HeaderValue::from_str(&retry_after_secs.to_string()) {
        headers.insert(header::RETRY_AFTER, v);
    }
    if let Ok(v) = HeaderValue::from_str(&limit.to_string()) {
        headers.insert(header::HeaderName::from_static("x-ratelimit-limit"), v);
    }
    headers.insert(
        header::HeaderName::from_static("x-ratelimit-remaining"),
        HeaderValue::from_static("0"),
    );

    response
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limit_allows_within_quota() {
        let config = RateLimitConfig {
            enabled: true,
            default_requests_per_minute: 10,
            url_minting_requests_per_minute: 5,
            burst_size: 5,
        };
        let state = RateLimitState::new(config);

        // First request should be allowed
        let result = state.check_default("tenant-1").await;
        assert!(matches!(result, RateLimitResult::Allowed { .. }));
    }

    #[tokio::test]
    async fn test_rate_limit_different_tenants() {
        let config = RateLimitConfig {
            enabled: true,
            default_requests_per_minute: 2,
            url_minting_requests_per_minute: 1,
            burst_size: 1,
        };
        let state = RateLimitState::new(config);

        // Each tenant has its own quota
        let result1 = state.check_default("tenant-1").await;
        let result2 = state.check_default("tenant-2").await;

        assert!(matches!(result1, RateLimitResult::Allowed { .. }));
        assert!(matches!(result2, RateLimitResult::Allowed { .. }));
    }

    #[tokio::test]
    async fn test_rate_limit_disabled() {
        let config = RateLimitConfig {
            enabled: false,
            ..RateLimitConfig::default()
        };
        let state = RateLimitState::new(config);

        // Always allowed when disabled
        for _ in 0..100 {
            let result = state.check_default("tenant-1").await;
            assert!(matches!(result, RateLimitResult::Allowed { limit: 0, .. }));
        }
    }

    #[tokio::test]
    async fn test_url_minting_lower_limit() {
        let config = RateLimitConfig {
            enabled: true,
            default_requests_per_minute: 100,
            url_minting_requests_per_minute: 10,
            burst_size: 10,
        };
        let state = RateLimitState::new(config);

        // URL minting has its own (lower) limit
        let result = state.check_url_minting("tenant-1").await;
        assert!(matches!(result, RateLimitResult::Allowed { limit: 10, .. }));

        let result = state.check_default("tenant-1").await;
        assert!(matches!(
            result,
            RateLimitResult::Allowed { limit: 100, .. }
        ));
    }
}
