//! Request context extraction and authentication middleware.
//!
//! In debug mode, tenant/workspace are supplied via headers for local development.
//! In production mode, tenant/workspace are extracted from a verified JWT. A user
//! identifier claim is also required (default `sub`, configurable via
//! `ARCO_JWT_USER_CLAIM`).

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::FromRequestParts;
use axum::extract::State;
use axum::http::header::HeaderName;
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, Request};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde_json::Value;
use ulid::Ulid;

use arco_core::ScopedStorage;
use arco_core::storage::StorageBackend;

use crate::error::ApiError;
use crate::server::AppState;

/// Header name for request IDs.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Per-request context derived from authentication and headers.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Tenant identifier.
    pub tenant: String,
    /// Workspace identifier.
    pub workspace: String,
    /// Optional user identifier (from JWT or debug headers).
    pub user_id: Option<String>,
    /// Request ID for tracing/correlation.
    pub request_id: String,
    /// Optional idempotency key (safe retries).
    pub idempotency_key: Option<String>,
}

impl RequestContext {
    /// Creates tenant/workspace scoped storage for this request.
    ///
    /// # Errors
    ///
    /// Returns an error if tenant/workspace identifiers are invalid.
    pub fn scoped_storage(
        &self,
        backend: Arc<dyn StorageBackend>,
    ) -> arco_core::Result<ScopedStorage> {
        ScopedStorage::new(backend, self.tenant.as_str(), self.workspace.as_str())
    }
}

#[async_trait]
impl FromRequestParts<Arc<AppState>> for RequestContext {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        if let Some(existing) = parts.extensions.get::<Self>() {
            return Ok(existing.clone());
        }

        let headers = &parts.headers;

        let request_id =
            request_id_from_headers(headers).unwrap_or_else(|| Ulid::new().to_string());
        let idempotency_key = header_string(headers, "Idempotency-Key");

        let (tenant, workspace, user_id) = if state.config.debug {
            let tenant = header_string(headers, "X-Tenant-Id").ok_or_else(|| {
                ApiError::unauthorized("missing X-Tenant-Id header (debug mode)")
                    .with_request_id(request_id.clone())
            })?;
            let workspace = header_string(headers, "X-Workspace-Id").ok_or_else(|| {
                ApiError::unauthorized("missing X-Workspace-Id header (debug mode)")
                    .with_request_id(request_id.clone())
            })?;
            let user_id = user_id_from_headers(headers);
            (tenant, workspace, user_id)
        } else {
            extract_from_jwt(headers, state, &request_id)?
        };

        let ctx = Self {
            tenant,
            workspace,
            user_id,
            request_id,
            idempotency_key,
        };

        parts.extensions.insert(ctx.clone());
        Ok(ctx)
    }
}

fn extract_from_jwt(
    headers: &HeaderMap,
    state: &AppState,
    request_id: &str,
) -> Result<(String, String, Option<String>), ApiError> {
    let token = bearer_token(headers)
        .ok_or_else(|| ApiError::missing_auth().with_request_id(request_id.to_string()))?;

    let (decoding_key, algorithm) = jwt_decoding_key(&state.config.jwt, request_id)?;
    let mut validation = Validation::new(algorithm);
    validation.validate_nbf = true;

    if let Some(iss) = state.config.jwt.issuer.as_deref() {
        validation.set_issuer(&[iss]);
    }
    if let Some(aud) = state.config.jwt.audience.as_deref() {
        validation.set_audience(&[aud]);
    }

    let data = jsonwebtoken::decode::<Value>(&token, &decoding_key, &validation)
        .map_err(|_| ApiError::invalid_token().with_request_id(request_id.to_string()))?;

    let Some(obj) = data.claims.as_object() else {
        return Err(ApiError::invalid_token().with_request_id(request_id.to_string()));
    };

    let tenant = extract_required_claim(obj, &state.config.jwt.tenant_claim, request_id)?;
    let workspace = extract_required_claim(obj, &state.config.jwt.workspace_claim, request_id)?;
    let user_id = extract_required_claim(obj, &state.config.jwt.user_claim, request_id)?;

    Ok((tenant, workspace, Some(user_id)))
}

fn jwt_decoding_key(
    jwt: &crate::config::JwtConfig,
    request_id: &str,
) -> Result<(DecodingKey, Algorithm), ApiError> {
    match (
        jwt.hs256_secret.as_deref(),
        jwt.rs256_public_key_pem.as_deref(),
    ) {
        (Some(secret), None) => Ok((DecodingKey::from_secret(secret.as_bytes()), Algorithm::HS256)),
        (None, Some(pem)) => DecodingKey::from_rsa_pem(pem.as_bytes())
            .map(|key| (key, Algorithm::RS256))
            .map_err(|e| {
                ApiError::internal(format!("failed to parse jwt.rs256_public_key_pem: {e}"))
                    .with_request_id(request_id.to_string())
            }),
        (Some(_), Some(_)) => Err(
            ApiError::internal("jwt.hs256_secret and jwt.rs256_public_key_pem are mutually exclusive")
                .with_request_id(request_id.to_string()),
        ),
        (None, None) => Err(ApiError::internal(
            "jwt.hs256_secret or jwt.rs256_public_key_pem is required when debug=false",
        )
        .with_request_id(request_id.to_string())),
    }
}

fn request_id_from_headers(headers: &HeaderMap) -> Option<String> {
    header_string(headers, "X-Request-Id").or_else(|| header_string(headers, "X-Request-ID"))
}

fn user_id_from_headers(headers: &HeaderMap) -> Option<String> {
    header_string(headers, "X-User-Id").or_else(|| header_string(headers, "X-User-ID"))
}

fn extract_required_claim(
    obj: &serde_json::Map<String, Value>,
    claim: &str,
    request_id: &str,
) -> Result<String, ApiError> {
    obj.get(claim)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ApiError::invalid_token().with_request_id(request_id.to_string()))
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let raw = header_string(headers, "Authorization")?;
    let token = raw.strip_prefix("Bearer ")?;
    Some(token.to_string())
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    let value = headers.get(name)?;
    header_value_to_string(value)
}

fn header_value_to_string(value: &HeaderValue) -> Option<String> {
    value.to_str().ok().map(str::to_string)
}

/// Authentication middleware.
///
/// This runs before other middleware (e.g. rate limiting) and injects a verified
/// [`RequestContext`] into request extensions.
pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let (mut parts, body) = req.into_parts();

    let ctx = match RequestContext::from_request_parts(&mut parts, &state).await {
        Ok(ctx) => ctx,
        Err(err) => return err.into_response(),
    };

    let mut req = Request::from_parts(parts, body);
    let request_id = ctx.request_id.clone();
    req.extensions_mut().insert(ctx);

    let mut response = next.run(req).await;
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), value);
    }
    response
}
