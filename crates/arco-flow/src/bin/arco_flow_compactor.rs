//! Arco Flow orchestration micro-compactor service.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};

use arco_core::observability::{LogFormat, init_logging};
use arco_core::orchestration_compaction::{
    OrchestrationCompactRequest, OrchestrationCompactionResponse, OrchestrationRebuildRequest,
};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_core::{InternalOidcConfig, InternalOidcError, InternalOidcVerifier, ScopedStorage};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::compactor::{
    CompactionResult, MicroCompactor, OrchestrationReconciler, OrchestrationReconciliationReport,
    OrchestrationRepairResult,
};

const REBUILD_MANIFEST_PREFIX: &str = "state/orchestration/rebuilds/";

#[derive(Clone)]
struct AppState {
    compactor: MicroCompactor,
    storage: ScopedStorage,
}

#[derive(Clone)]
struct InternalAuthState {
    verifier: Arc<InternalOidcVerifier>,
    enforce: bool,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, Deserialize)]
struct ReconcileRequest {
    #[serde(default)]
    repair: bool,
}

#[derive(Debug, Serialize)]
struct ReconcileResponse {
    report: OrchestrationReconciliationReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    repair_result: Option<OrchestrationRepairResult>,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        let status = match &error {
            Error::StaleFencingToken { .. } | Error::FencingLockUnavailable { .. } => {
                StatusCode::CONFLICT
            }
            Error::Core(arco_core::Error::PreconditionFailed { .. }) => StatusCode::CONFLICT,
            Error::Core(
                arco_core::Error::InvalidInput(_)
                | arco_core::Error::InvalidId { .. }
                | arco_core::Error::Validation { .. },
            ) => StatusCode::BAD_REQUEST,
            Error::Core(
                arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. },
            ) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn compact_handler(
    State(state): State<AppState>,
    Json(request): Json<OrchestrationCompactRequest>,
) -> std::result::Result<Json<OrchestrationCompactionResponse>, ApiError> {
    let CompactionResult {
        events_processed,
        delta_id,
        manifest_revision,
        visibility_status,
        repair_pending,
    } = match (request.fencing_token, request.lock_path.as_deref()) {
        (Some(fencing_token), Some(lock_path)) => {
            state
                .compactor
                .compact_events_fenced(request.event_paths, fencing_token, lock_path)
                .await?
        }
        (token, _) => {
            state
                .compactor
                .compact_events_with_epoch(request.event_paths, token)
                .await?
        }
    };

    Ok(Json(OrchestrationCompactionResponse {
        events_processed,
        delta_id,
        manifest_revision,
        visibility_status: visibility_status.into(),
        repair_pending,
    }))
}

async fn rebuild_handler(
    State(state): State<AppState>,
    Json(request): Json<OrchestrationRebuildRequest>,
) -> std::result::Result<Json<OrchestrationCompactionResponse>, ApiError> {
    if !is_valid_rebuild_manifest_path(&request.rebuild_manifest_path) {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: format!(
                "invalid rebuild_manifest_path: expected '{REBUILD_MANIFEST_PREFIX}*.json'"
            ),
        });
    }

    let CompactionResult {
        events_processed,
        delta_id,
        manifest_revision,
        visibility_status,
        repair_pending,
    } = match (request.fencing_token, request.lock_path.as_deref()) {
        (Some(fencing_token), Some(lock_path)) => {
            state
                .compactor
                .rebuild_from_ledger_manifest_path_fenced(
                    &request.rebuild_manifest_path,
                    fencing_token,
                    lock_path,
                )
                .await?
        }
        (token, _) => {
            state
                .compactor
                .rebuild_from_ledger_manifest_path(&request.rebuild_manifest_path, token)
                .await?
        }
    };

    Ok(Json(OrchestrationCompactionResponse {
        events_processed,
        delta_id,
        manifest_revision,
        visibility_status: visibility_status.into(),
        repair_pending,
    }))
}

async fn reconcile_handler(
    State(state): State<AppState>,
    Json(request): Json<ReconcileRequest>,
) -> std::result::Result<Json<ReconcileResponse>, ApiError> {
    let reconciler = OrchestrationReconciler::new(state.storage.clone());
    let report = reconciler.check().await?;
    let repair_result = if request.repair {
        Some(reconciler.repair(&report).await?)
    } else {
        None
    };

    Ok(Json(ReconcileResponse {
        report,
        repair_result,
    }))
}

fn required_env(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::configuration(format!("missing {key}")))
}

fn is_valid_rebuild_manifest_path(path: &str) -> bool {
    !path.is_empty()
        && path.starts_with(REBUILD_MANIFEST_PREFIX)
        && std::path::Path::new(path)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
}

fn resolve_port() -> Result<u16> {
    if let Ok(port) = std::env::var("PORT") {
        return port
            .parse::<u16>()
            .map_err(|_| Error::configuration("invalid PORT"));
    }

    if let Ok(port) = std::env::var("ARCO_FLOW_PORT") {
        return port
            .parse::<u16>()
            .map_err(|_| Error::configuration("invalid ARCO_FLOW_PORT"));
    }

    Ok(8080)
}

fn log_format_from_env() -> LogFormat {
    match std::env::var("ARCO_LOG_FORMAT") {
        Ok(value) if value.eq_ignore_ascii_case("json") => LogFormat::Json,
        _ => LogFormat::Pretty,
    }
}

fn parse_bool_env(key: &str) -> bool {
    std::env::var(key).ok().is_some_and(|value| {
        value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("yes")
    })
}

fn load_tenant_secret() -> Result<Vec<u8>> {
    let require_secret = parse_bool_env("ARCO_REQUIRE_TENANT_SECRET");
    let raw = std::env::var("ARCO_TENANT_SECRET_B64").ok();
    let secret = raw.map_or_else(Vec::new, |value| match STANDARD.decode(value.as_bytes()) {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "invalid ARCO_TENANT_SECRET_B64; using empty secret"
            );
            Vec::new()
        }
    });

    if secret.is_empty() {
        if require_secret {
            return Err(Error::configuration(
                "ARCO_TENANT_SECRET_B64 required when ARCO_REQUIRE_TENANT_SECRET=true",
            ));
        }
        if !cfg!(test) {
            tracing::warn!("ARCO_TENANT_SECRET_B64 not set; run_id HMAC uses empty secret");
        }
    }

    Ok(secret)
}

fn build_internal_auth() -> Result<Option<Arc<InternalAuthState>>> {
    let config = InternalOidcConfig::from_env().map_err(|e| Error::configuration(e.to_string()))?;
    let Some(config) = config else {
        return Ok(None);
    };

    let enforce = config.enforce;
    let verifier =
        InternalOidcVerifier::new(config).map_err(|e| Error::configuration(e.to_string()))?;
    Ok(Some(Arc::new(InternalAuthState {
        verifier: Arc::new(verifier),
        enforce,
    })))
}

async fn internal_auth_middleware(
    State(state): State<Arc<InternalAuthState>>,
    request: axum::http::Request<Body>,
    next: Next,
) -> Response {
    match state.verifier.verify_headers(request.headers()).await {
        Ok(_) => next.run(request).await,
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
                    Json(ErrorResponse { error: message }),
                )
                    .into_response();
            }

            tracing::warn!(error = %err, "internal auth check failed in report-only mode");
            next.run(request).await
        }
    }
}

fn build_router(state: AppState, internal_auth: Option<Arc<InternalAuthState>>) -> Router {
    let compact_route = internal_auth.clone().map_or_else(
        || post(compact_handler),
        |auth| {
            post(compact_handler).route_layer(middleware::from_fn_with_state(
                auth,
                internal_auth_middleware,
            ))
        },
    );
    let rebuild_route = internal_auth.clone().map_or_else(
        || post(rebuild_handler),
        |auth| {
            post(rebuild_handler).route_layer(middleware::from_fn_with_state(
                auth,
                internal_auth_middleware,
            ))
        },
    );
    let reconcile_route = internal_auth.map_or_else(
        || post(reconcile_handler),
        |auth| {
            post(reconcile_handler).route_layer(middleware::from_fn_with_state(
                auth,
                internal_auth_middleware,
            ))
        },
    );

    Router::new()
        .route("/health", get(health_handler))
        .route("/compact", compact_route)
        .route("/rebuild", rebuild_route)
        .route("/internal/reconcile", reconcile_route)
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use arco_core::Error as CoreError;
    use arco_core::storage::MemoryBackend;
    use axum::http::Request;
    use tower::util::ServiceExt;

    fn test_state() -> AppState {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        AppState {
            compactor: MicroCompactor::new(storage.clone()),
            storage,
        }
    }

    fn test_internal_auth_state() -> Arc<InternalAuthState> {
        let config = InternalOidcConfig::hs256_for_tests(
            "https://accounts.google.com",
            "https://flow-compactor.internal",
            "test-secret",
            [String::from("svc-flow-compactor")]
                .into_iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::new(),
            true,
        );
        let verifier = InternalOidcVerifier::new(config).expect("test verifier");
        Arc::new(InternalAuthState {
            verifier: Arc::new(verifier),
            enforce: true,
        })
    }

    #[test]
    fn compact_request_allows_missing_epoch() {
        let raw = r#"{"event_paths":["ledger/orchestration/2025-01-15/evt_01.json"]}"#;
        let parsed: OrchestrationCompactRequest = serde_json::from_str(raw).expect("valid request");
        assert_eq!(parsed.fencing_token, None);
    }

    #[test]
    fn compact_request_accepts_epoch_payload() {
        let raw = r#"{
            "event_paths":["ledger/orchestration/2025-01-15/evt_01.json"],
            "epoch":7
        }"#;
        let parsed: OrchestrationCompactRequest =
            serde_json::from_str(raw).expect("valid epoch request");
        assert_eq!(parsed.fencing_token, Some(7));
    }

    #[test]
    fn rebuild_request_requires_manifest_path() {
        let raw = r#"{"rebuild_manifest_path":"state/orchestration/rebuilds/rebuild-01.json"}"#;
        let parsed: OrchestrationRebuildRequest =
            serde_json::from_str(raw).expect("valid rebuild request");
        assert_eq!(
            parsed.rebuild_manifest_path,
            "state/orchestration/rebuilds/rebuild-01.json"
        );
        assert_eq!(parsed.fencing_token, None);
    }

    #[test]
    fn rebuild_request_accepts_epoch_payload() {
        let raw = r#"{
            "rebuild_manifest_path":"state/orchestration/rebuilds/rebuild-01.json",
            "epoch":9
        }"#;
        let parsed: OrchestrationRebuildRequest =
            serde_json::from_str(raw).expect("valid epoch request");
        assert_eq!(parsed.fencing_token, Some(9));
    }

    #[test]
    fn rebuild_manifest_path_validator_accepts_expected_path() {
        assert!(is_valid_rebuild_manifest_path(
            "state/orchestration/rebuilds/rebuild-01.json"
        ));
    }

    #[test]
    fn rebuild_manifest_path_validator_rejects_invalid_paths() {
        assert!(!is_valid_rebuild_manifest_path(""));
        assert!(!is_valid_rebuild_manifest_path(
            "state/orchestration/manifest.pointer.json"
        ));
        assert!(!is_valid_rebuild_manifest_path(
            "state/orchestration/rebuilds/"
        ));
        assert!(!is_valid_rebuild_manifest_path(
            "state/orchestration/rebuilds/rebuild-01"
        ));
    }

    #[test]
    fn api_error_maps_core_precondition_to_conflict() {
        let error = Error::Core(CoreError::PreconditionFailed {
            message: "manifest publish failed - concurrent write".to_string(),
        });
        let api_error = ApiError::from(error);
        assert_eq!(api_error.status, StatusCode::CONFLICT);
    }

    #[test]
    fn api_error_maps_core_invalid_input_to_bad_request() {
        let error = Error::Core(CoreError::InvalidInput("invalid lock_path".to_string()));
        let api_error = ApiError::from(error);
        assert_eq!(api_error.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn api_error_maps_core_not_found_to_not_found() {
        let error = Error::Core(CoreError::NotFound("missing object".to_string()));
        let api_error = ApiError::from(error);
        assert_eq!(api_error.status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn reconcile_request_defaults_to_check_mode() {
        let request: ReconcileRequest = serde_json::from_str("{}").expect("default reconcile");
        assert!(!request.repair);
    }

    #[tokio::test]
    async fn reconcile_endpoint_returns_report_when_auth_disabled() {
        let router = build_router(test_state(), None);
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/reconcile")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"repair":false}"#.to_string()))
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn reconcile_endpoint_requires_auth_when_internal_auth_enforced() {
        let router = build_router(test_state(), Some(test_internal_auth_state()));
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/reconcile")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"repair":false}"#.to_string()))
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging(log_format_from_env());

    let tenant_id = required_env("ARCO_TENANT_ID")?;
    let workspace_id = required_env("ARCO_WORKSPACE_ID")?;
    let bucket = required_env("ARCO_STORAGE_BUCKET")?;
    let port = resolve_port()?;
    let tenant_secret = load_tenant_secret()?;
    let internal_auth = build_internal_auth()?;

    let backend = ObjectStoreBackend::from_bucket(&bucket)?;
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let storage = ScopedStorage::new(backend, tenant_id, workspace_id)?;

    let state = AppState {
        compactor: MicroCompactor::with_tenant_secret(storage.clone(), tenant_secret),
        storage,
    };

    let app = build_router(state, internal_auth);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::configuration(format!("failed to bind: {e}")))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| Error::configuration(format!("server error: {e}")))
}
