//! Browser URL minting routes.
//!
//! Provides signed URL minting for browser-based Parquet reads via DuckDB-WASM.
//!
//! ## Security Design
//!
//! This endpoint is the bridge between the API and browser-based query engines.
//! Key security invariants:
//!
//! 1. **Manifest-driven allowlist**: Only paths in `SnapshotInfo.files` are mintable
//! 2. **No ledger access**: Ledger paths are never mintable (internal)
//! 3. **No manifest access**: Manifest paths are never mintable (metadata leak)
//! 4. **Tenant scoping**: All paths are scoped to auth context tenant/workspace
//! 5. **TTL bounded**: Maximum 1 hour, default 15 minutes
//! 6. **No data proxying**: Only URLs returned, never actual data
//!
//! ## Log Redaction Policy
//!
//! **CRITICAL**: Signed URLs are secrets and MUST NOT appear in logs.
//!
//! The following data is considered sensitive and is NOT logged:
//! - `req.paths` - May contain sensitive file paths or naming patterns
//! - `response.urls` - Contains signed URLs with query string credentials
//!
//! Only safe metadata is logged: tenant, workspace, domain, path count, TTL.
//!
//! For internal handling of sensitive URL data, use `crate::redaction::RedactedUrl`.
//!
//! ## Routes
//!
//! - `POST /browser/urls` - Mint signed URLs for snapshot files

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use arco_core::CatalogDomain;

use crate::context::RequestContext;
use crate::error::ApiError;
use crate::error::ApiErrorBody;
use crate::server::AppState;

/// Maximum TTL for signed URLs (1 hour).
const MAX_TTL_SECONDS: u64 = 3600;
/// Default TTL for signed URLs (15 minutes).
const DEFAULT_TTL_SECONDS: u64 = 900;

/// Request to mint signed URLs.
#[derive(Debug, Deserialize, ToSchema)]
pub struct MintUrlsRequest {
    /// Catalog domain (e.g., "catalog", "lineage").
    pub domain: String,
    /// Paths to mint URLs for (must be in manifest allowlist).
    pub paths: Vec<String>,
    /// Optional TTL in seconds (max 3600, default 900).
    pub ttl_seconds: Option<u64>,
}

/// Response with signed URLs.
#[derive(Debug, Serialize, ToSchema)]
pub struct MintUrlsResponse {
    /// Signed URLs keyed by path.
    pub urls: Vec<SignedUrl>,
    /// TTL used for all URLs.
    pub ttl_seconds: u64,
}

/// A single signed URL.
#[derive(Debug, Serialize, ToSchema)]
pub struct SignedUrl {
    /// Original path requested.
    pub path: String,
    /// Signed URL (valid for TTL).
    pub url: String,
}

/// Creates browser routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new().route("/browser/urls", post(mint_urls))
}

/// Mint signed URLs for browser-based reads.
///
/// POST /api/v1/browser/urls
///
/// ## Security
///
/// - Only paths in the current manifest's `SnapshotInfo.files` are mintable
/// - Ledger and manifest paths are rejected
/// - Cross-tenant paths are rejected (tenant scoping)
/// - Path traversal attempts are rejected
/// - TTL is bounded to maximum 1 hour
#[utoipa::path(
    post,
    path = "/api/v1/browser/urls",
    tag = "browser",
    request_body = MintUrlsRequest,
    responses(
        (status = 200, description = "Signed URLs minted", body = MintUrlsResponse),
        (status = 400, description = "Bad request", body = ApiErrorBody),
        (status = 401, description = "Unauthorized", body = ApiErrorBody),
        (status = 403, description = "Forbidden", body = ApiErrorBody),
        (status = 404, description = "Not found", body = ApiErrorBody),
        (status = 500, description = "Internal error", body = ApiErrorBody),
    ),
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn mint_urls(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Json(req): Json<MintUrlsRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let domain = match parse_domain(&req.domain) {
        Ok(d) => d,
        Err(e) => {
            crate::audit::emit_url_mint_deny(
                state.audit(),
                &ctx,
                &req.domain,
                crate::audit::REASON_UNKNOWN_DOMAIN,
                &state.config.audit,
            );
            return Err(e);
        }
    };

    // Bound TTL
    let ttl_seconds = req
        .ttl_seconds
        .unwrap_or(DEFAULT_TTL_SECONDS)
        .min(MAX_TTL_SECONDS);
    let ttl = Duration::from_secs(ttl_seconds);

    // SECURITY: Validate path traversal FIRST (before any I/O)
    for path in &req.paths {
        if path.contains("..") {
            crate::audit::emit_url_mint_deny(
                state.audit(),
                &ctx,
                &req.domain,
                crate::audit::REASON_PATH_TRAVERSAL,
                &state.config.audit,
            );
            return Err(ApiError::forbidden(format!(
                "Path traversal not allowed: {path}"
            )));
        }
    }

    tracing::info!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        domain = %req.domain,
        paths_count = req.paths.len(),
        ttl_seconds = ttl_seconds,
        // DO NOT log: req.paths (may contain sensitive data)
        // DO NOT log: response.urls (contains signed URLs)
        "Minting browser URLs"
    );

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let reader = arco_catalog::CatalogReader::new(storage);

    // Get mintable paths from manifest
    let mintable = reader
        .get_mintable_paths(domain)
        .await
        .map_err(ApiError::from)?;
    let mintable_set: HashSet<_> = mintable.iter().collect();

    // Validate ALL requested paths are in allowlist
    for path in &req.paths {
        if !mintable_set.contains(path) {
            crate::audit::emit_url_mint_deny(
                state.audit(),
                &ctx,
                &req.domain,
                crate::audit::REASON_NOT_IN_ALLOWLIST,
                &state.config.audit,
            );
            return Err(ApiError::forbidden(format!(
                "Path not in manifest allowlist: {path}"
            )));
        }
    }

    // Mint signed URLs
    let paths_to_mint = req.paths.clone();
    let signed = reader
        .mint_signed_urls(paths_to_mint, ttl)
        .await
        .map_err(ApiError::from)?;

    // Record metrics for signed URL minting
    crate::metrics::record_signed_url_minted();
    crate::audit::emit_url_mint_allow(
        state.audit(),
        &ctx,
        &req.domain,
        req.paths.len(),
        &state.config.audit,
    );

    let urls = signed
        .into_iter()
        .map(|s| SignedUrl {
            path: s.path,
            url: s.url,
        })
        .collect();

    Ok((StatusCode::OK, Json(MintUrlsResponse { urls, ttl_seconds })))
}

/// Parse domain string to `CatalogDomain`.
fn parse_domain(domain: &str) -> Result<CatalogDomain, ApiError> {
    match domain.to_lowercase().as_str() {
        "catalog" => Ok(CatalogDomain::Catalog),
        "lineage" => Ok(CatalogDomain::Lineage),
        "executions" => Ok(CatalogDomain::Executions),
        "search" => Ok(CatalogDomain::Search),
        _ => Err(ApiError::bad_request(format!("Unknown domain: {domain}"))),
    }
}
