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
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use arco_core::observability::{LogFormat, init_logging};
use arco_core::orchestration_compaction::{
    OrchestrationCompactRequest, OrchestrationCompactionResponse, OrchestrationRebuildRequest,
};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_core::{InternalOidcConfig, InternalOidcError, InternalOidcVerifier, ScopedStorage};
use arco_flow::error::{Error, Result};
use arco_flow::metrics::{
    record_orch_compactor_legacy_epoch_request, record_orch_compactor_partial_fenced_request,
};
use arco_flow::orchestration::compactor::{
    CompactionResult, MicroCompactor, OrchestrationReconciler, OrchestrationReconciliationReport,
    OrchestrationRepairResult,
};

const REBUILD_MANIFEST_PREFIX: &str = "state/orchestration/rebuilds/";
const ARCO_FLOW_COMPACTOR_REQUEST_MODE_ENV: &str = "ARCO_FLOW_COMPACTOR_REQUEST_MODE";
const ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE_ENV: &str = "ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum CompactionRequestMode {
    #[default]
    Compatibility,
    FencedOnly,
}

impl CompactionRequestMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Compatibility => "compatibility",
            Self::FencedOnly => "fenced_only",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "compatibility" => Ok(Self::Compatibility),
            "fenced_only" => Ok(Self::FencedOnly),
            other => Err(Error::configuration(format!(
                "invalid {ARCO_FLOW_COMPACTOR_REQUEST_MODE_ENV}: expected compatibility|fenced_only, got {other}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum LegacyEpochPayloadMode {
    #[default]
    Accept,
    Reject,
}

impl LegacyEpochPayloadMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Reject => "reject",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "accept" => Ok(Self::Accept),
            "reject" => Ok(Self::Reject),
            other => Err(Error::configuration(format!(
                "invalid {ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE_ENV}: expected accept|reject, got {other}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RolloutConfig {
    request_mode: CompactionRequestMode,
    legacy_epoch_mode: LegacyEpochPayloadMode,
}

impl RolloutConfig {
    fn from_env() -> Result<Self> {
        Self::from_env_reader(|key| std::env::var(key).ok())
    }

    fn from_env_reader<F>(mut read_env: F) -> Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let request_mode = read_env(ARCO_FLOW_COMPACTOR_REQUEST_MODE_ENV)
            .as_deref()
            .map(CompactionRequestMode::parse)
            .transpose()?
            .unwrap_or_default();
        let legacy_epoch_mode = read_env(ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE_ENV)
            .as_deref()
            .map(LegacyEpochPayloadMode::parse)
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            request_mode,
            legacy_epoch_mode,
        })
    }
}

#[derive(Clone)]
struct AppState {
    compactor: MicroCompactor,
    storage: ScopedStorage,
    rollout: RolloutConfig,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactorEndpoint {
    Compact,
    Rebuild,
}

impl CompactorEndpoint {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Compact => "compact",
            Self::Rebuild => "rebuild",
        }
    }

    const fn fenced_only_message(self) -> &'static str {
        match self {
            Self::Compact => {
                "fenced-only mode requires compact requests to include fencing_token and lock_path"
            }
            Self::Rebuild => {
                "fenced-only mode requires rebuild requests to include fencing_token and lock_path"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestExecutionMode {
    CompatibilityFallback,
    Fenced,
}

#[derive(Debug)]
struct ParsedRequest<T> {
    request: T,
    used_legacy_epoch: bool,
    supplied_canonical_fencing_token: bool,
}

fn parse_request<T>(raw: serde_json::Value) -> std::result::Result<ParsedRequest<T>, ApiError>
where
    T: DeserializeOwned,
{
    let supplied_canonical_fencing_token = raw.get("fencing_token").is_some();
    let used_legacy_epoch = raw.get("epoch").is_some() && !supplied_canonical_fencing_token;
    let request = serde_json::from_value(raw).map_err(|error| ApiError {
        status: StatusCode::BAD_REQUEST,
        message: format!("invalid request body: {error}"),
    })?;

    Ok(ParsedRequest {
        request,
        used_legacy_epoch,
        supplied_canonical_fencing_token,
    })
}

fn log_compactor_request(
    endpoint: CompactorEndpoint,
    request_id: Option<&str>,
    used_legacy_epoch: bool,
    rollout: RolloutConfig,
) {
    tracing::info!(
        endpoint = endpoint.as_str(),
        request_id = request_id.unwrap_or(""),
        used_legacy_epoch,
        request_mode = rollout.request_mode.as_str(),
        legacy_epoch_mode = rollout.legacy_epoch_mode.as_str(),
        "received orchestration compactor request"
    );
}

fn resolve_request_execution(
    endpoint: CompactorEndpoint,
    rollout: RolloutConfig,
    request_id: Option<&str>,
    used_legacy_epoch: bool,
    supplied_canonical_fencing_token: bool,
    fencing_token: Option<u64>,
    lock_path: Option<&str>,
) -> std::result::Result<RequestExecutionMode, ApiError> {
    if used_legacy_epoch && rollout.legacy_epoch_mode == LegacyEpochPayloadMode::Reject {
        record_orch_compactor_legacy_epoch_request(
            endpoint.as_str(),
            "rejected",
            rollout.request_mode.as_str(),
        );
        tracing::warn!(
            endpoint = endpoint.as_str(),
            request_id = request_id.unwrap_or(""),
            request_mode = rollout.request_mode.as_str(),
            "rejecting legacy epoch payload by rollout gate"
        );
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: format!(
                "legacy epoch payloads are disabled by {ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE_ENV}=reject"
            ),
        });
    }

    let missing_field = match (
        supplied_canonical_fencing_token,
        lock_path.is_some(),
        used_legacy_epoch,
    ) {
        (true, false, _) => Some("lock_path"),
        (false, true, false) => Some("fencing_token"),
        _ => None,
    };

    if let Some(missing_field) = missing_field {
        record_orch_compactor_partial_fenced_request(
            endpoint.as_str(),
            missing_field,
            rollout.request_mode.as_str(),
        );
        tracing::warn!(
            endpoint = endpoint.as_str(),
            request_id = request_id.unwrap_or(""),
            missing_field,
            request_mode = rollout.request_mode.as_str(),
            "rejecting partial canonical fenced request"
        );
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: format!(
                "partial fenced request requires both fencing_token and lock_path; missing {missing_field}"
            ),
        });
    }

    if fencing_token.is_some() && lock_path.is_some() {
        if used_legacy_epoch {
            record_orch_compactor_legacy_epoch_request(
                endpoint.as_str(),
                "accepted",
                rollout.request_mode.as_str(),
            );
        }
        return Ok(RequestExecutionMode::Fenced);
    }

    if rollout.request_mode == CompactionRequestMode::FencedOnly {
        if used_legacy_epoch {
            record_orch_compactor_legacy_epoch_request(
                endpoint.as_str(),
                "rejected",
                rollout.request_mode.as_str(),
            );
        }
        tracing::warn!(
            endpoint = endpoint.as_str(),
            request_id = request_id.unwrap_or(""),
            has_fencing_token = fencing_token.is_some(),
            has_lock_path = lock_path.is_some(),
            "rejecting request because fenced-only mode requires the full fenced contract"
        );
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: endpoint.fenced_only_message().to_string(),
        });
    }

    if used_legacy_epoch {
        record_orch_compactor_legacy_epoch_request(
            endpoint.as_str(),
            "accepted",
            rollout.request_mode.as_str(),
        );
    }

    if fencing_token.is_some() || lock_path.is_some() {
        tracing::warn!(
            endpoint = endpoint.as_str(),
            request_id = request_id.unwrap_or(""),
            has_fencing_token = fencing_token.is_some(),
            has_lock_path = lock_path.is_some(),
            "using orchestration compactor compatibility fallback without the full fenced contract"
        );
    }

    Ok(RequestExecutionMode::CompatibilityFallback)
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn compact_handler(
    State(state): State<AppState>,
    Json(raw_request): Json<serde_json::Value>,
) -> std::result::Result<Json<OrchestrationCompactionResponse>, ApiError> {
    let ParsedRequest {
        request,
        used_legacy_epoch,
        supplied_canonical_fencing_token,
    } = parse_request::<OrchestrationCompactRequest>(raw_request)?;
    log_compactor_request(
        CompactorEndpoint::Compact,
        request.request_id.as_deref(),
        used_legacy_epoch,
        state.rollout,
    );
    let execution = resolve_request_execution(
        CompactorEndpoint::Compact,
        state.rollout,
        request.request_id.as_deref(),
        used_legacy_epoch,
        supplied_canonical_fencing_token,
        request.fencing_token,
        request.lock_path.as_deref(),
    )?;

    let CompactionResult {
        events_processed,
        delta_id,
        manifest_revision,
        visibility_status,
        repair_pending,
    } = match execution {
        RequestExecutionMode::Fenced => {
            let fencing_token = request
                .fencing_token
                .expect("fenced execution requires fencing_token");
            let lock_path = request
                .lock_path
                .as_deref()
                .expect("fenced execution requires lock_path");
            state
                .compactor
                .compact_events_fenced(request.event_paths, fencing_token, lock_path)
                .await?
        }
        RequestExecutionMode::CompatibilityFallback => {
            state
                .compactor
                .compact_events_with_epoch(request.event_paths, request.fencing_token)
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
    Json(raw_request): Json<serde_json::Value>,
) -> std::result::Result<Json<OrchestrationCompactionResponse>, ApiError> {
    let ParsedRequest {
        request,
        used_legacy_epoch,
        supplied_canonical_fencing_token,
    } = parse_request::<OrchestrationRebuildRequest>(raw_request)?;
    log_compactor_request(
        CompactorEndpoint::Rebuild,
        request.request_id.as_deref(),
        used_legacy_epoch,
        state.rollout,
    );
    if !is_valid_rebuild_manifest_path(&request.rebuild_manifest_path) {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: format!(
                "invalid rebuild_manifest_path: expected '{REBUILD_MANIFEST_PREFIX}*.json'"
            ),
        });
    }
    let execution = resolve_request_execution(
        CompactorEndpoint::Rebuild,
        state.rollout,
        request.request_id.as_deref(),
        used_legacy_epoch,
        supplied_canonical_fencing_token,
        request.fencing_token,
        request.lock_path.as_deref(),
    )?;

    let CompactionResult {
        events_processed,
        delta_id,
        manifest_revision,
        visibility_status,
        repair_pending,
    } = match execution {
        RequestExecutionMode::Fenced => {
            let fencing_token = request
                .fencing_token
                .expect("fenced execution requires fencing_token");
            let lock_path = request
                .lock_path
                .as_deref()
                .expect("fenced execution requires lock_path");
            state
                .compactor
                .rebuild_from_ledger_manifest_path_fenced(
                    &request.rebuild_manifest_path,
                    fencing_token,
                    lock_path,
                )
                .await?
        }
        RequestExecutionMode::CompatibilityFallback => {
            state
                .compactor
                .rebuild_from_ledger_manifest_path(
                    &request.rebuild_manifest_path,
                    request.fencing_token,
                )
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
    use std::io;
    use std::sync::{Mutex, OnceLock};

    use super::*;
    use arco_core::Error as CoreError;
    use arco_core::storage::MemoryBackend;
    use axum::http::Request;
    use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
    use tower::util::ServiceExt;
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone)]
    struct SharedWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedWriter {
        fn new(buffer: Arc<Mutex<Vec<u8>>>) -> Self {
            Self { buffer }
        }
    }

    struct BufferWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl io::Write for BufferWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer
                .lock()
                .expect("buffer lock")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedWriter {
        type Writer = BufferWriter;

        fn make_writer(&'a self) -> Self::Writer {
            BufferWriter {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }

    fn init_metrics() -> PrometheusHandle {
        static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
        PROMETHEUS_HANDLE
            .get_or_init(|| {
                PrometheusBuilder::new()
                    .install_recorder()
                    .expect("install prometheus recorder for compactor tests")
            })
            .clone()
    }

    fn test_state() -> AppState {
        test_state_with_rollout(RolloutConfig::default())
    }

    fn test_state_with_rollout(rollout: RolloutConfig) -> AppState {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        AppState {
            compactor: MicroCompactor::new(storage.clone()),
            storage,
            rollout,
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
    fn rollout_config_defaults_preserve_pi1_compatibility() {
        let config = RolloutConfig::default();

        assert_eq!(config.request_mode, CompactionRequestMode::Compatibility);
        assert_eq!(config.legacy_epoch_mode, LegacyEpochPayloadMode::Accept);
    }

    #[test]
    fn rollout_config_parses_fenced_only_and_legacy_epoch_reject() {
        let config = RolloutConfig::from_env_reader(|key| match key {
            "ARCO_FLOW_COMPACTOR_REQUEST_MODE" => Some("fenced_only".to_string()),
            "ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE" => Some("reject".to_string()),
            _ => None,
        })
        .expect("parse rollout config");

        assert_eq!(config.request_mode, CompactionRequestMode::FencedOnly);
        assert_eq!(config.legacy_epoch_mode, LegacyEpochPayloadMode::Reject);
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

    #[tokio::test]
    async fn fenced_only_mode_rejects_legacy_epoch_payload_and_records_metric() {
        let handle = init_metrics();
        let router = build_router(
            test_state_with_rollout(RolloutConfig {
                request_mode: CompactionRequestMode::FencedOnly,
                legacy_epoch_mode: LegacyEpochPayloadMode::Accept,
            }),
            None,
        );

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/compact")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"event_paths":[],"epoch":7,"request_id":"req-flow-legacy"}"#
                            .to_string(),
                    ))
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let rendered = handle.render();
        assert!(
            rendered.contains(
                "arco_flow_orch_compactor_legacy_epoch_requests_total{endpoint=\"compact\",status=\"rejected\",request_mode=\"fenced_only\"}"
            ),
            "expected legacy epoch metric in rendered output, got {rendered}"
        );
    }

    #[tokio::test]
    async fn compatibility_mode_rejects_lock_path_without_fencing_token_and_records_metric() {
        let handle = init_metrics();
        let router = build_router(test_state(), None);

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/compact")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"event_paths":[],"lock_path":"locks/orchestration.compaction.lock.json","request_id":"req-partial-lock"}"#
                            .to_string(),
                    ))
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let rendered = handle.render();
        assert!(
            rendered.contains(
                "arco_flow_orch_compactor_partial_fenced_requests_total{endpoint=\"compact\",missing_field=\"fencing_token\",request_mode=\"compatibility\"}"
            ),
            "expected partial fenced request metric in rendered output, got {rendered}"
        );
    }

    #[tokio::test]
    async fn compatibility_mode_rejects_fencing_token_without_lock_path_and_records_metric() {
        let handle = init_metrics();
        let router = build_router(test_state(), None);

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rebuild")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"rebuild_manifest_path":"state/orchestration/rebuilds/rebuild-01.json","fencing_token":9,"request_id":"req-partial-token"}"#
                            .to_string(),
                    ))
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let rendered = handle.render();
        assert!(
            rendered.contains(
                "arco_flow_orch_compactor_partial_fenced_requests_total{endpoint=\"rebuild\",missing_field=\"lock_path\",request_mode=\"compatibility\"}"
            ),
            "expected partial fenced request metric in rendered output, got {rendered}"
        );
    }

    #[tokio::test]
    async fn legacy_epoch_reject_mode_rejects_rebuild_payload() {
        let router = build_router(
            test_state_with_rollout(RolloutConfig {
                request_mode: CompactionRequestMode::Compatibility,
                legacy_epoch_mode: LegacyEpochPayloadMode::Reject,
            }),
            None,
        );

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rebuild")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"rebuild_manifest_path":"state/orchestration/rebuilds/rebuild-01.json","epoch":9}"#
                            .to_string(),
                    ))
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn compact_handler_logs_request_id_for_legacy_epoch_payload() {
        let router = build_router(
            test_state_with_rollout(RolloutConfig {
                request_mode: CompactionRequestMode::FencedOnly,
                legacy_epoch_mode: LegacyEpochPayloadMode::Accept,
            }),
            None,
        );
        let logs = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(SharedWriter::new(Arc::clone(&logs)))
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/compact")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"event_paths":[],"epoch":7,"request_id":"req-flow-trace"}"#.to_string(),
                    ))
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let rendered =
            String::from_utf8(logs.lock().expect("logs lock").clone()).expect("utf8 logs");
        assert!(
            rendered.contains("req-flow-trace"),
            "expected request_id in tracing output, got {rendered}"
        );
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
    let rollout = RolloutConfig::from_env()?;

    let backend = ObjectStoreBackend::from_bucket(&bucket)?;
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let storage = ScopedStorage::new(backend, tenant_id, workspace_id)?;

    tracing::info!(
        request_mode = rollout.request_mode.as_str(),
        legacy_epoch_mode = rollout.legacy_epoch_mode.as_str(),
        "loaded orchestration compactor rollout config"
    );

    let state = AppState {
        compactor: MicroCompactor::with_tenant_secret(storage.clone(), tenant_secret),
        storage,
        rollout,
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
