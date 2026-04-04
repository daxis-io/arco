//! Arco Flow orchestration micro-compactor service.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use chrono::Utc;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use arco_core::observability::{LogFormat, init_logging};
use arco_core::orchestration_compaction::{
    OrchestrationCompactRequest, OrchestrationCompactionResponse, OrchestrationRebuildRequest,
};
use arco_core::repair_backlog::RepairBacklogEntry;
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_core::{InternalOidcConfig, InternalOidcError, InternalOidcVerifier, ScopedStorage};
use arco_flow::error::{Error, Result};
use arco_flow::metrics::{
    record_orch_compactor_contract_rejection, record_orch_repair_automation_findings,
    record_orch_repair_automation_run, record_orch_repair_completion_latency,
    record_orch_repair_repeat, set_orch_repair_backlog,
};
use arco_flow::orchestration::compactor::{
    CompactionResult, MicroCompactor, OrchestrationReconciler, OrchestrationReconciliationReport,
    OrchestrationRepairResult, OrchestrationRepairScope,
};

const REBUILD_MANIFEST_PREFIX: &str = "state/orchestration/rebuilds/";
const ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_MODE_ENV: &str =
    "ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_MODE";
const ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS_ENV: &str =
    "ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS";
const ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_SCOPE_ENV: &str =
    "ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_SCOPE";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum RepairAutomationMode {
    #[default]
    Disabled,
    DryRun,
    Enforce,
}

impl RepairAutomationMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::DryRun => "dry_run",
            Self::Enforce => "enforce",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "disabled" => Ok(Self::Disabled),
            "dry_run" => Ok(Self::DryRun),
            "enforce" => Ok(Self::Enforce),
            other => Err(Error::configuration(format!(
                "invalid {ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_MODE_ENV}: expected disabled|dry_run|enforce, got {other}"
            ))),
        }
    }
}

#[derive(Clone, Debug)]
struct RepairAutomationConfig {
    mode: RepairAutomationMode,
    interval: Duration,
    scope: OrchestrationRepairScope,
}

impl Default for RepairAutomationConfig {
    fn default() -> Self {
        Self {
            mode: RepairAutomationMode::Enforce,
            interval: Duration::from_secs(300),
            scope: OrchestrationRepairScope::CurrentHeadOnly,
        }
    }
}

impl RepairAutomationConfig {
    fn from_env() -> Result<Self> {
        Self::from_env_reader(|key| std::env::var(key).ok())
    }

    fn from_env_reader<F>(mut read_env: F) -> Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let mut config = Self::default();

        if let Some(raw) = read_env(ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_MODE_ENV) {
            config.mode = RepairAutomationMode::parse(&raw)?;
        }
        if let Some(raw) = read_env(ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS_ENV) {
            let secs = raw.parse::<u64>().map_err(|_| {
                Error::configuration(format!(
                    "invalid {ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS_ENV}: expected u64 seconds"
                ))
            })?;
            config.interval = Duration::from_secs(secs.max(1));
        }
        if let Some(raw) = read_env(ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_SCOPE_ENV) {
            config.scope = match raw.trim().to_ascii_lowercase().as_str() {
                "current_head_only" => OrchestrationRepairScope::CurrentHeadOnly,
                "full" => OrchestrationRepairScope::Full,
                other => {
                    return Err(Error::configuration(format!(
                        "invalid {ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_SCOPE_ENV}: expected current_head_only|full, got {other}"
                    )));
                }
            };
        }

        Ok(config)
    }
}

type RepairBacklogState = RepairBacklogEntry;

#[derive(Clone)]
struct AppState {
    compactor: MicroCompactor,
    storage: ScopedStorage,
    repair_automation: RepairAutomationConfig,
    repair_backlog: Arc<Mutex<RepairBacklogState>>,
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

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReconcileRequest {
    #[serde(default)]
    repair: bool,
    #[serde(default, alias = "repair_scope")]
    repair_scope: Option<OrchestrationRepairScope>,
}

impl ReconcileRequest {
    fn effective_repair_scope(&self) -> OrchestrationRepairScope {
        self.repair_scope
            .unwrap_or(OrchestrationRepairScope::CurrentHeadOnly)
    }
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

fn init_metrics() -> PrometheusHandle {
    PROMETHEUS_HANDLE
        .get_or_init(|| {
            PrometheusBuilder::new()
                .install_recorder()
                .expect("install prometheus recorder for arco-flow-compactor")
        })
        .clone()
}

fn prometheus_handle() -> Option<PrometheusHandle> {
    PROMETHEUS_HANDLE.get().cloned()
}

async fn metrics_handler() -> impl IntoResponse {
    prometheus_handle().map_or_else(
        || {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [("content-type", "text/plain; charset=utf-8")],
                "Metrics not initialized".to_string(),
            )
        },
        |handle| {
            (
                StatusCode::OK,
                [("content-type", "text/plain; charset=utf-8")],
                handle.render(),
            )
        },
    )
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

    const fn fenced_contract_message(self) -> &'static str {
        match self {
            Self::Compact => "compact requests must include fencing_token and lock_path",
            Self::Rebuild => "rebuild requests must include fencing_token and lock_path",
        }
    }
}

fn parse_request<T>(
    endpoint: CompactorEndpoint,
    raw: serde_json::Value,
) -> std::result::Result<T, ApiError>
where
    T: DeserializeOwned,
{
    let request_id = request_id_from_raw(&raw).unwrap_or("");
    let has_legacy_epoch = raw.get("epoch").is_some();
    let has_fencing_token = raw.get("fencing_token").is_some();
    let has_lock_path = raw.get("lock_path").is_some();

    let rejection_reason = if has_legacy_epoch {
        Some("legacy_epoch_alias")
    } else {
        match (has_fencing_token, has_lock_path) {
            (false, false) => Some("missing_fencing_token_and_lock_path"),
            (false, true) => Some("missing_fencing_token"),
            (true, false) => Some("missing_lock_path"),
            (true, true) => None,
        }
    };

    if let Some(reason) = rejection_reason {
        record_orch_compactor_contract_rejection(endpoint.as_str(), reason);
        tracing::warn!(
            endpoint = endpoint.as_str(),
            request_id,
            reason,
            "rejecting removed orchestration compactor compatibility request shape"
        );
        let message = if reason == "legacy_epoch_alias" {
            format!(
                "{}; the legacy `epoch` alias is no longer accepted",
                endpoint.fenced_contract_message()
            )
        } else {
            endpoint.fenced_contract_message().to_string()
        };
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message,
        });
    }

    let request = serde_json::from_value(raw).map_err(|error| ApiError {
        status: StatusCode::BAD_REQUEST,
        message: format!("invalid request body: {error}"),
    })?;

    Ok(request)
}

fn request_id_from_raw(raw: &serde_json::Value) -> Option<&str> {
    raw.get("request_id").and_then(serde_json::Value::as_str)
}

fn log_compactor_request(endpoint: CompactorEndpoint, request_id: Option<&str>) {
    tracing::info!(
        endpoint = endpoint.as_str(),
        request_id = request_id.unwrap_or(""),
        "received orchestration compactor request"
    );
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn compact_handler(
    State(state): State<AppState>,
    Json(raw_request): Json<serde_json::Value>,
) -> std::result::Result<Json<OrchestrationCompactionResponse>, ApiError> {
    log_compactor_request(
        CompactorEndpoint::Compact,
        request_id_from_raw(&raw_request),
    );
    let request =
        parse_request::<OrchestrationCompactRequest>(CompactorEndpoint::Compact, raw_request)?;

    let CompactionResult {
        events_processed,
        delta_id,
        manifest_revision,
        visibility_status,
        repair_pending,
    } = state
        .compactor
        .compact_events_fenced(
            request.event_paths,
            request.fencing_token,
            &request.lock_path,
        )
        .await?;

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
    log_compactor_request(
        CompactorEndpoint::Rebuild,
        request_id_from_raw(&raw_request),
    );
    let request =
        parse_request::<OrchestrationRebuildRequest>(CompactorEndpoint::Rebuild, raw_request)?;
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
    } = state
        .compactor
        .rebuild_from_ledger_manifest_path_fenced(
            &request.rebuild_manifest_path,
            request.fencing_token,
            &request.lock_path,
        )
        .await?;

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
        Some(
            reconciler
                .repair_with_scope(&report, request.effective_repair_scope())
                .await?,
        )
    } else {
        None
    };

    Ok(Json(ReconcileResponse {
        report,
        repair_result,
    }))
}

const fn repair_scope_as_str(scope: OrchestrationRepairScope) -> &'static str {
    match scope {
        OrchestrationRepairScope::CurrentHeadOnly => "current_head_only",
        OrchestrationRepairScope::Full => "full",
    }
}

fn repairable_issue_count(
    report: &OrchestrationReconciliationReport,
    scope: OrchestrationRepairScope,
) -> u64 {
    let current_head = u64::from(report.current_head_legacy_manifest_issue.is_some());
    match scope {
        OrchestrationRepairScope::CurrentHeadOnly => current_head,
        OrchestrationRepairScope::Full => {
            current_head
                + u64::try_from(report.orphan_manifest_snapshots.len()).unwrap_or(u64::MAX)
                + u64::try_from(report.orphan_base_dirs.len()).unwrap_or(u64::MAX)
                + u64::try_from(report.orphan_l0_dirs.len()).unwrap_or(u64::MAX)
        }
    }
}

fn repair_backlog_fingerprint(
    report: &OrchestrationReconciliationReport,
    scope: OrchestrationRepairScope,
) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(issue) = report.current_head_legacy_manifest_issue {
        parts.push(format!("current_head:{issue:?}"));
    }
    if scope == OrchestrationRepairScope::Full {
        parts.extend(
            report
                .orphan_manifest_snapshots
                .iter()
                .map(|path| format!("manifest:{path}")),
        );
        parts.extend(
            report
                .orphan_base_dirs
                .iter()
                .map(|path| format!("base:{path}")),
        );
        parts.extend(
            report
                .orphan_l0_dirs
                .iter()
                .map(|path| format!("l0:{path}")),
        );
    }
    if parts.is_empty() {
        None
    } else {
        parts.sort();
        Some(parts.join("|"))
    }
}

async fn update_repair_backlog_metrics(
    state: &AppState,
    report: &OrchestrationReconciliationReport,
) {
    let findings = repairable_issue_count(report, state.repair_automation.scope);
    let fingerprint = repair_backlog_fingerprint(report, state.repair_automation.scope);
    let mode = state.repair_automation.mode.as_str();
    let scope = repair_scope_as_str(state.repair_automation.scope);

    let snapshot = {
        let mut backlog = state.repair_backlog.lock().await;
        backlog.update(findings, fingerprint, Utc::now())
    };
    if snapshot.repeated_finding {
        record_orch_repair_repeat(
            state.storage.tenant_id(),
            state.storage.workspace_id(),
            mode,
            scope,
        );
    }

    set_orch_repair_backlog(
        state.storage.tenant_id(),
        state.storage.workspace_id(),
        mode,
        scope,
        snapshot.count,
        snapshot.age_seconds,
    );
}

async fn run_repair_automation_once(state: &AppState) {
    if state.repair_automation.mode == RepairAutomationMode::Disabled {
        return;
    }

    let mode = state.repair_automation.mode.as_str();
    let scope = repair_scope_as_str(state.repair_automation.scope);
    let start = Instant::now();
    let reconciler = OrchestrationReconciler::new(state.storage.clone());

    let report = match reconciler.check().await {
        Ok(report) => report,
        Err(error) => {
            record_orch_repair_automation_run(mode, scope, "check_failed");
            tracing::error!(error = %error, "orchestration repair automation check failed");
            return;
        }
    };

    let findings = repairable_issue_count(&report, state.repair_automation.scope);
    record_orch_repair_automation_findings(mode, scope, findings);

    let status = match state.repair_automation.mode {
        RepairAutomationMode::DryRun => {
            update_repair_backlog_metrics(state, &report).await;
            if findings > 0 {
                "repair_needed"
            } else {
                "clean"
            }
        }
        RepairAutomationMode::Enforce => {
            if findings == 0 {
                update_repair_backlog_metrics(state, &report).await;
                "clean"
            } else {
                match reconciler
                    .repair_with_scope(&report, state.repair_automation.scope)
                    .await
                {
                    Ok(_) => match reconciler.check().await {
                        Ok(post_report) => {
                            let remaining =
                                repairable_issue_count(&post_report, state.repair_automation.scope);
                            update_repair_backlog_metrics(state, &post_report).await;
                            if remaining > 0 {
                                "repair_needed"
                            } else {
                                "repaired"
                            }
                        }
                        Err(error) => {
                            record_orch_repair_automation_run(mode, scope, "recheck_failed");
                            tracing::error!(
                                error = %error,
                                "orchestration repair automation recheck failed"
                            );
                            return;
                        }
                    },
                    Err(error) => {
                        update_repair_backlog_metrics(state, &report).await;
                        tracing::error!(
                            error = %error,
                            "orchestration repair automation enforcement failed"
                        );
                        "repair_failed"
                    }
                }
            }
        }
        RepairAutomationMode::Disabled => return,
    };

    record_orch_repair_completion_latency(mode, scope, start.elapsed().as_secs_f64());
    record_orch_repair_automation_run(mode, scope, status);
}

async fn run_repair_automation_loop(state: AppState) {
    if state.repair_automation.mode == RepairAutomationMode::Disabled {
        return;
    }

    tracing::info!(
        mode = state.repair_automation.mode.as_str(),
        scope = repair_scope_as_str(state.repair_automation.scope),
        interval_secs = state.repair_automation.interval.as_secs(),
        "orchestration repair automation enabled"
    );

    let mut interval_timer = tokio::time::interval(state.repair_automation.interval);
    interval_timer.tick().await;

    loop {
        interval_timer.tick().await;
        run_repair_automation_once(&state).await;
    }
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
        .route("/metrics", get(metrics_handler))
        .route("/compact", compact_route)
        .route("/rebuild", rebuild_route)
        .route("/internal/reconcile", reconcile_route)
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::io;
    use std::sync::Mutex;

    use super::*;
    use arco_core::Error as CoreError;
    use arco_core::WritePrecondition;
    use arco_core::storage::MemoryBackend;
    use arco_flow::orchestration::compactor::manifest::{
        OrchestrationManifest, OrchestrationManifestPointer,
    };
    use axum::body::to_bytes;
    use axum::http::Request;
    use bytes::Bytes;
    use chrono::{Duration, Utc};
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
        super::init_metrics()
    }

    fn test_state() -> AppState {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "prod").expect("storage");
        AppState {
            compactor: MicroCompactor::new(storage.clone()),
            storage,
            repair_automation: RepairAutomationConfig::default(),
            repair_backlog: Arc::new(tokio::sync::Mutex::new(RepairBacklogState::default())),
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

    async fn seed_orphaned_orchestration_manifest(state: &AppState) -> String {
        let current_manifest_path = "state/orchestration/manifests/00000000000000000001.json";
        let mut current_manifest = OrchestrationManifest::new("rev-current");
        current_manifest.manifest_id = "00000000000000000001".to_string();
        current_manifest.published_at = Utc::now() - Duration::minutes(30);

        state
            .storage
            .put_raw(
                current_manifest_path,
                Bytes::from(
                    serde_json::to_vec(&current_manifest).expect("serialize current manifest"),
                ),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write current manifest");
        state
            .storage
            .put_raw(
                "state/orchestration/manifest.pointer.json",
                Bytes::from(
                    serde_json::to_vec(&OrchestrationManifestPointer {
                        manifest_id: "00000000000000000001".to_string(),
                        manifest_path: current_manifest_path.to_string(),
                        epoch: 1,
                        parent_pointer_hash: None,
                        updated_at: Utc::now() - Duration::minutes(30),
                    })
                    .expect("serialize pointer"),
                ),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write pointer");
        state
            .storage
            .put_raw(
                arco_core::FlowPaths::orchestration_manifest_path(),
                Bytes::from(
                    serde_json::to_vec(&current_manifest).expect("serialize legacy manifest"),
                ),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write legacy manifest");

        let orphan_manifest_path = "state/orchestration/manifests/00000000000000000002.json";
        state
            .storage
            .put_raw(
                orphan_manifest_path,
                Bytes::from_static(b"{}"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("write orphan manifest");

        orphan_manifest_path.to_string()
    }

    #[test]
    fn compact_request_requires_fencing_token() {
        let raw = r#"{"event_paths":["ledger/orchestration/2025-01-15/evt_01.json"]}"#;
        let error =
            serde_json::from_str::<OrchestrationCompactRequest>(raw).expect_err("must reject");
        assert!(
            error.to_string().contains("fencing_token") || error.to_string().contains("lock_path"),
            "expected missing fenced field error, got {error}"
        );
    }

    #[test]
    fn compact_request_rejects_epoch_payload() {
        let raw = r#"{
            "event_paths":["ledger/orchestration/2025-01-15/evt_01.json"],
            "epoch":7
        }"#;
        let error =
            serde_json::from_str::<OrchestrationCompactRequest>(raw).expect_err("must reject");
        assert!(
            error.to_string().contains("epoch"),
            "expected legacy alias error, got {error}"
        );
    }

    #[test]
    fn rebuild_request_requires_fencing_token() {
        let raw = r#"{"rebuild_manifest_path":"state/orchestration/rebuilds/rebuild-01.json"}"#;
        let error =
            serde_json::from_str::<OrchestrationRebuildRequest>(raw).expect_err("must reject");
        assert!(
            error.to_string().contains("fencing_token") || error.to_string().contains("lock_path"),
            "expected missing fenced field error, got {error}"
        );
    }

    #[test]
    fn rebuild_request_rejects_epoch_payload() {
        let raw = r#"{
            "rebuild_manifest_path":"state/orchestration/rebuilds/rebuild-01.json",
            "epoch":9
        }"#;
        let error =
            serde_json::from_str::<OrchestrationRebuildRequest>(raw).expect_err("must reject");
        assert!(
            error.to_string().contains("epoch"),
            "expected legacy alias error, got {error}"
        );
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
    fn repair_automation_config_defaults_to_enforce_current_head_only() {
        let config =
            RepairAutomationConfig::from_env_reader(|_| None).expect("default repair config");

        assert_eq!(config.mode, RepairAutomationMode::Enforce);
        assert_eq!(config.interval, std::time::Duration::from_secs(300));
        assert_eq!(config.scope, OrchestrationRepairScope::CurrentHeadOnly);
    }

    #[test]
    fn repair_automation_config_parses_enforce_full_scope() {
        let config = RepairAutomationConfig::from_env_reader(|key| match key {
            "ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_MODE" => Some("enforce".to_string()),
            "ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS" => Some("42".to_string()),
            "ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_SCOPE" => Some("full".to_string()),
            _ => None,
        })
        .expect("parse repair automation config");

        assert_eq!(config.mode, RepairAutomationMode::Enforce);
        assert_eq!(config.interval, std::time::Duration::from_secs(42));
        assert_eq!(config.scope, OrchestrationRepairScope::Full);
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
        assert_eq!(
            request.effective_repair_scope(),
            OrchestrationRepairScope::CurrentHeadOnly
        );
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
    async fn metrics_endpoint_serves_prometheus_text() {
        init_metrics();
        arco_flow::metrics::register_metrics();
        record_orch_compactor_contract_rejection("compact", "legacy_epoch_alias");
        let router = build_router(test_state(), None);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read metrics body");
        let rendered = String::from_utf8(body.to_vec()).expect("metrics utf8");
        assert!(
            rendered.contains("arco_flow_orch_compactor_request_contract_rejections_total"),
            "expected prometheus output, got {rendered}"
        );
        assert!(
            rendered.contains(
                "arco_flow_orch_compactor_request_contract_rejections_total{endpoint=\"compact\",reason=\"legacy_epoch_alias\"}"
            ),
            "expected emitted sample in metrics output, got {rendered}"
        );
    }

    #[tokio::test]
    async fn reconcile_endpoint_defaults_to_current_head_only_scope() {
        let state = test_state();
        let orphan_manifest_path = seed_orphaned_orchestration_manifest(&state).await;
        let router = build_router(state.clone(), None);

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/reconcile")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"repair":true}"#.to_string()))
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            state
                .storage
                .head_raw(&orphan_manifest_path)
                .await
                .expect("head orphan manifest")
                .is_some(),
            "default orchestration reconcile repair scope must not delete generic cleanup candidates"
        );
    }

    #[tokio::test]
    async fn reconcile_endpoint_accepts_camel_case_full_scope() {
        let state = test_state();
        let orphan_manifest_path = seed_orphaned_orchestration_manifest(&state).await;
        let router = build_router(state.clone(), None);

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/reconcile")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"repair":true,"repairScope":"full"}"#.to_string(),
                    ))
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read reconcile body");
        let payload: serde_json::Value =
            serde_json::from_slice(&body).expect("deserialize reconcile body");
        assert_eq!(payload["repair_result"]["deferred_paths"], 1);
        assert_eq!(payload["repair_result"]["skipped_paths"], 0);
        assert!(
            state
                .storage
                .head_raw(&orphan_manifest_path)
                .await
                .expect("head orphan manifest")
                .is_some(),
            "fresh full-scope reports should still defer generic cleanup until the delete quarantine expires"
        );
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
    async fn compact_endpoint_requires_auth_when_internal_auth_enforced() {
        let router = build_router(test_state(), Some(test_internal_auth_state()));
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/compact")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"event_paths":[],"fencing_token":7,"lock_path":"locks/orchestration.compaction.lock.json"}"#
                            .to_string(),
                    ))
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rebuild_endpoint_requires_auth_when_internal_auth_enforced() {
        let router = build_router(test_state(), Some(test_internal_auth_state()));
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rebuild")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"rebuild_manifest_path":"state/orchestration/rebuilds/rebuild-01.json","fencing_token":7,"lock_path":"locks/orchestration.compaction.lock.json"}"#
                            .to_string(),
                    ))
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn default_request_contract_rejects_compact_requests_without_fencing() {
        let router = build_router(test_state(), None);

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/compact")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"event_paths":[]}"#.to_string()))
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn default_request_contract_rejects_legacy_epoch_payload_and_records_metric() {
        let handle = init_metrics();
        let router = build_router(test_state(), None);

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
            rendered.contains("reason=\"legacy_epoch_alias\""),
            "expected compatibility rejection metric in rendered output, got {rendered}"
        );
    }

    #[tokio::test]
    async fn request_contract_rejects_lock_path_without_fencing_token_and_records_metric() {
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
                "arco_flow_orch_compactor_request_contract_rejections_total{endpoint=\"compact\",reason=\"missing_fencing_token\"}"
            ),
            "expected compatibility rejection metric in rendered output, got {rendered}"
        );
    }

    #[tokio::test]
    async fn request_contract_rejects_fencing_token_without_lock_path_and_records_metric() {
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
                "arco_flow_orch_compactor_request_contract_rejections_total{endpoint=\"rebuild\",reason=\"missing_lock_path\"}"
            ),
            "expected compatibility rejection metric in rendered output, got {rendered}"
        );
    }

    #[tokio::test]
    async fn default_request_contract_rejects_rebuild_epoch_payload() {
        let router = build_router(test_state(), None);

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
    async fn log_compactor_request_includes_request_id() {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(SharedWriter::new(Arc::clone(&logs)))
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        log_compactor_request(CompactorEndpoint::Compact, Some("req-flow-trace"));

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
    let repair_automation = RepairAutomationConfig::from_env()?;
    init_metrics();
    arco_flow::metrics::register_metrics();

    let backend = ObjectStoreBackend::from_bucket(&bucket)?;
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let storage = ScopedStorage::new(backend, tenant_id, workspace_id)?;

    tracing::info!(
        request_contract = "fenced_only",
        repair_automation_mode = repair_automation.mode.as_str(),
        repair_automation_scope = repair_scope_as_str(repair_automation.scope),
        "loaded orchestration compactor production config"
    );

    let state = AppState {
        compactor: MicroCompactor::with_tenant_secret(storage.clone(), tenant_secret),
        storage,
        repair_automation: repair_automation.clone(),
        repair_backlog: Arc::new(Mutex::new(RepairBacklogState::default())),
    };

    let app = build_router(state.clone(), internal_auth);
    if repair_automation.mode != RepairAutomationMode::Disabled {
        tokio::spawn(run_repair_automation_loop(state.clone()));
    }

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::configuration(format!("failed to bind: {e}")))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| Error::configuration(format!("server error: {e}")))
}
