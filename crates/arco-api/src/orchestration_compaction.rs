//! Orchestration compaction helpers (sync or remote).

use arco_core::lock::{DEFAULT_LOCK_TTL, DistributedLock};
use arco_core::{ScopedStorage, VisibilityStatus};
use arco_flow::compaction_client::compact_orchestration_events_fenced;
use arco_flow::error::Error as FlowError;
use arco_flow::orchestration::OrchestrationLedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::events::OrchestrationEvent;
use arco_flow::orchestration::ledger::LedgerWriter;
use arco_flow::orchestration_compaction_lock_path;

use crate::config::Config;
use crate::error::ApiError;

/// Appends an orchestration event and compacts it while holding the shared lock.
///
/// # Errors
///
/// Returns an error if the event append, compaction request, or visibility check fails.
pub async fn append_event_and_compact(
    config: &Config,
    storage: ScopedStorage,
    event: OrchestrationEvent,
    request_id: Option<&str>,
) -> Result<String, ApiError> {
    let mut event_paths =
        append_events_and_compact(config, storage, vec![event], request_id).await?;
    Ok(event_paths.pop().unwrap_or_default())
}

/// Visible commit metadata for one orchestration batch publish.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct OrchestrationCommitOutcome {
    /// Event paths appended before compaction.
    pub event_paths: Vec<String>,
    /// Canonical lock path used to fence the append+compact flow.
    pub lock_path: String,
    /// Fencing token held for this publish.
    pub fencing_token: u64,
    /// Visible immutable manifest identifier.
    pub manifest_id: String,
    /// Visible manifest revision ULID.
    pub manifest_revision: String,
    /// Pointer object version returned by the visible CAS publish.
    pub pointer_version: String,
    /// Delta identifier written by this batch, if any.
    pub delta_id: Option<String>,
    /// Number of events processed by compaction.
    pub events_processed: u32,
    /// Whether post-commit side effects still require repair.
    pub repair_pending: bool,
}

/// Appends orchestration events and compacts them with the same fencing token.
///
/// This is the PI-1 API write helper for routes that must ensure the append and
/// compaction share a single lock acquisition.
///
/// # Errors
///
/// Returns an error if lock acquisition, event append, compaction, or visibility
/// validation fails.
pub async fn append_events_and_compact(
    config: &Config,
    storage: ScopedStorage,
    events: Vec<OrchestrationEvent>,
    request_id: Option<&str>,
) -> Result<Vec<String>, ApiError> {
    Ok(
        append_events_and_compact_with_result(config, storage, events, request_id)
            .await?
            .event_paths,
    )
}

/// Appends orchestration events, waits for visible publication, and returns commit metadata.
///
/// # Errors
///
/// Returns an error if lock acquisition, event append, compaction, or visibility
/// validation fails.
pub async fn append_events_and_compact_with_result(
    config: &Config,
    storage: ScopedStorage,
    events: Vec<OrchestrationEvent>,
    request_id: Option<&str>,
) -> Result<OrchestrationCommitOutcome, ApiError> {
    if events.is_empty() {
        return Err(ApiError::bad_request(
            "orchestration batch must include at least one event",
        ));
    }

    let lock_path = orchestration_compaction_lock_path();
    let lock = DistributedLock::new(storage.backend().clone(), lock_path);
    let guard = lock.acquire(DEFAULT_LOCK_TTL, 10).await.map_err(|e| {
        ApiError::conflict(format!(
            "failed to acquire orchestration compaction lock: {e}"
        ))
    })?;
    let fencing_token = guard.fencing_token().sequence();

    let outcome = async {
        let event_paths: Vec<String> = events.iter().map(LedgerWriter::event_path).collect();
        let ledger = LedgerWriter::new(storage.clone());
        ledger.append_all(events).await.map_err(|e| {
            ApiError::internal(format!("failed to append orchestration events: {e}"))
        })?;

        let publish_result = compact_orchestration_events_with_fencing(
            config,
            storage.clone(),
            event_paths.clone(),
            fencing_token,
            lock_path,
            request_id,
        )
        .await?;

        Ok::<_, ApiError>((event_paths, publish_result))
    }
    .await;

    if let Err(error) = guard.release().await {
        tracing::warn!(
            error = %error,
            "failed to release orchestration compaction lock after compaction; relying on TTL cleanup"
        );
    }

    let (event_paths, publish_result) = outcome?;

    Ok(OrchestrationCommitOutcome {
        event_paths,
        lock_path: lock_path.to_string(),
        fencing_token,
        manifest_id: publish_result.manifest_id,
        manifest_revision: publish_result.manifest_revision,
        pointer_version: publish_result.pointer_version,
        delta_id: publish_result.delta_id,
        events_processed: publish_result.events_processed,
        repair_pending: publish_result.repair_pending,
    })
}

async fn compact_orchestration_events_with_fencing(
    config: &Config,
    storage: ScopedStorage,
    event_paths: Vec<String>,
    fencing_token: u64,
    lock_path: &str,
    request_id: Option<&str>,
) -> Result<OrchestrationCompactionResult, ApiError> {
    if event_paths.is_empty() {
        return Ok(OrchestrationCompactionResult {
            events_processed: 0,
            delta_id: None,
            manifest_id: String::new(),
            manifest_revision: String::new(),
            pointer_version: String::new(),
            repair_pending: false,
            visibility_status: VisibilityStatus::Visible,
        });
    }

    if let Some(url) = config.orchestration_compactor_url.as_ref() {
        let response = compact_orchestration_events_fenced(
            url,
            event_paths,
            fencing_token,
            lock_path,
            request_id,
        )
        .await
        .map_err(|error| map_flow_compaction_error(&error))?;
        return require_visible_compaction(response);
    }

    let compactor = MicroCompactor::new(storage);
    let result = compactor
        .compact_events_fenced(event_paths, fencing_token, lock_path)
        .await
        .map_err(|error| map_flow_compaction_error(&error))?;
    require_visible_compaction(OrchestrationCompactionResult {
        events_processed: result.events_processed,
        delta_id: result.delta_id,
        manifest_id: result.manifest_id,
        manifest_revision: result.manifest_revision,
        pointer_version: result.pointer_version,
        repair_pending: result.repair_pending,
        visibility_status: result.visibility_status.into(),
    })
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct OrchestrationCompactionResult {
    events_processed: u32,
    delta_id: Option<String>,
    manifest_id: String,
    manifest_revision: String,
    pointer_version: String,
    repair_pending: bool,
    visibility_status: VisibilityStatus,
}

impl From<arco_core::orchestration_compaction::OrchestrationCompactionResponse>
    for OrchestrationCompactionResult
{
    fn from(value: arco_core::orchestration_compaction::OrchestrationCompactionResponse) -> Self {
        Self {
            events_processed: value.events_processed,
            delta_id: value.delta_id,
            manifest_id: value.manifest_id,
            manifest_revision: value.manifest_revision,
            pointer_version: value.pointer_version,
            repair_pending: value.repair_pending,
            visibility_status: value.visibility_status,
        }
    }
}

fn require_visible_compaction(
    result: impl Into<OrchestrationCompactionResult>,
) -> Result<OrchestrationCompactionResult, ApiError> {
    let result = result.into();
    if result.visibility_status == VisibilityStatus::Visible {
        return Ok(result);
    }

    Err(ApiError::internal(format!(
        "orchestration compaction did not become visible: {}",
        result.visibility_status.as_str()
    )))
}

fn map_flow_compaction_error(error: &FlowError) -> ApiError {
    match error {
        FlowError::StaleFencingToken { .. }
        | FlowError::FencingLockUnavailable { .. }
        | FlowError::Core(arco_core::Error::PreconditionFailed { .. }) => {
            ApiError::conflict(error.to_string())
        }
        FlowError::Core(
            arco_core::Error::InvalidInput(_)
            | arco_core::Error::InvalidId { .. }
            | arco_core::Error::Validation { .. },
        ) => ApiError::bad_request(error.to_string()),
        _ => ApiError::internal(format!("orchestration compaction failed: {error}")),
    }
}

/// Ledger writer that triggers orchestration compaction after each event append.
pub struct CompactingLedgerWriter {
    storage: ScopedStorage,
    config: Config,
}

impl CompactingLedgerWriter {
    /// Creates a new compacting ledger writer.
    #[must_use]
    pub fn new(storage: ScopedStorage, config: Config) -> Self {
        Self { storage, config }
    }
}

impl OrchestrationLedgerWriter for CompactingLedgerWriter {
    async fn write_event(&self, event: &OrchestrationEvent) -> Result<(), String> {
        append_event_and_compact(&self.config, self.storage.clone(), event.clone(), None)
            .await
            .map_err(|e| format!("{e:?}"))
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use arco_core::orchestration_compaction::{
        OrchestrationCompactRequest, OrchestrationCompactionResponse,
    };
    use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
    use arco_flow::orchestration::events::{
        OrchestrationEvent, OrchestrationEventData, TriggerInfo,
    };
    use async_trait::async_trait;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Json, Router};
    use bytes::Bytes;

    #[derive(Debug)]
    struct LockReleaseFailureBackend {
        inner: arco_core::MemoryBackend,
    }

    impl LockReleaseFailureBackend {
        fn new() -> Self {
            Self {
                inner: arco_core::MemoryBackend::new(),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for LockReleaseFailureBackend {
        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            if path == orchestration_compaction_lock_path()
                && matches!(precondition, WritePrecondition::MatchesVersion(_))
            {
                return Err(arco_core::Error::storage(
                    "injected lock release failure for test",
                ));
            }
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    async fn spawn_error_server(status: StatusCode, body: serde_json::Value) -> String {
        let app = Router::new().route(
            "/compact",
            post(move || {
                let status = status;
                let body = body.clone();
                async move { (status, Json(body)) }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    async fn spawn_delay_server(delay: Duration) -> String {
        let app = Router::new().route(
            "/compact",
            post(move || {
                let delay = delay;
                async move {
                    tokio::time::sleep(delay).await;
                    (
                        StatusCode::OK,
                        Json(OrchestrationCompactionResponse {
                            events_processed: 1,
                            delta_id: None,
                            manifest_id: "00000000000000000001".to_string(),
                            manifest_revision: "manifest-rev".to_string(),
                            pointer_version: "ptr-1".to_string(),
                            visibility_status: VisibilityStatus::Visible,
                            repair_pending: false,
                        }),
                    )
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    async fn spawn_bearer_server(expected_auth: &'static str) -> String {
        let app = Router::new().route(
            "/compact",
            post(
                move |headers: HeaderMap, Json(request): Json<OrchestrationCompactRequest>| {
                    async move {
                        let auth = headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok());
                        if auth != Some(expected_auth) {
                            return (
                                StatusCode::UNAUTHORIZED,
                                Json(serde_json::json!({ "error": "missing bearer auth" })),
                            );
                        }

                        let _ = request;
                        (
                            StatusCode::OK,
                            Json(serde_json::json!(OrchestrationCompactionResponse {
                                events_processed: 1,
                                delta_id: None,
                                manifest_id: "00000000000000000001".to_string(),
                                manifest_revision: "manifest-rev".to_string(),
                                pointer_version: "ptr-1".to_string(),
                                visibility_status: VisibilityStatus::Visible,
                                repair_pending: false,
                            })),
                        )
                    }
                },
            ),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    async fn spawn_capture_server(
        captured_request: Arc<Mutex<Option<OrchestrationCompactRequest>>>,
    ) -> String {
        let app = Router::new().route(
            "/compact",
            post(move |Json(request): Json<OrchestrationCompactRequest>| {
                let captured_request = Arc::clone(&captured_request);
                async move {
                    *captured_request.lock().expect("capture lock") = Some(request.clone());
                    (
                        StatusCode::OK,
                        Json(serde_json::json!(OrchestrationCompactionResponse {
                            events_processed: 1,
                            delta_id: None,
                            manifest_id: "00000000000000000001".to_string(),
                            manifest_revision: "manifest-rev".to_string(),
                            pointer_version: "ptr-1".to_string(),
                            visibility_status: VisibilityStatus::Visible,
                            repair_pending: false,
                        })),
                    )
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    fn sample_storage() -> ScopedStorage {
        let backend = Arc::new(arco_core::MemoryBackend::new());
        ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage")
    }

    fn sample_storage_with_backend<B: StorageBackend>(backend: Arc<B>) -> ScopedStorage {
        ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage")
    }

    #[tokio::test]
    async fn append_events_and_compact_noop_when_empty() {
        let result = append_events_and_compact(
            &Config::default(),
            sample_storage(),
            Vec::<OrchestrationEvent>::new(),
            None,
        )
        .await;
        assert!(result.expect("empty append").is_empty());
    }

    #[tokio::test]
    async fn append_events_and_compact_makes_inline_writes_visible_without_remote_compactor() {
        let storage = sample_storage();
        let compactor = MicroCompactor::new(storage.clone());

        append_events_and_compact(
            &Config::default(),
            storage,
            vec![OrchestrationEvent::new(
                "tenant",
                "workspace",
                OrchestrationEventData::RunTriggered {
                    run_id: "run_inline_visible".to_string(),
                    plan_id: "plan_inline_visible".to_string(),
                    trigger: TriggerInfo::Manual {
                        user_id: "tester".to_string(),
                    },
                    root_assets: vec![],
                    run_key: None,
                    labels: std::collections::HashMap::new(),
                    code_version: None,
                },
            )],
            Some("req_inline_visible"),
        )
        .await
        .expect("append and compact");

        let (_manifest, state) = compactor.load_state().await.expect("load state");
        assert!(
            state.runs.contains_key("run_inline_visible"),
            "default API append+compact must leave the emitted event visible to readers"
        );
    }

    #[tokio::test]
    async fn append_events_and_compact_maps_remote_error_message() {
        let url = spawn_error_server(
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({ "error": "compactor unavailable" }),
        )
        .await;

        let config = Config {
            debug: false,
            orchestration_compactor_url: Some(url),
            ..Config::default()
        };

        let result = append_events_and_compact(
            &config,
            sample_storage(),
            vec![OrchestrationEvent::new(
                "tenant",
                "workspace",
                OrchestrationEventData::RunTriggered {
                    run_id: "run_remote_error".to_string(),
                    plan_id: "plan_remote_error".to_string(),
                    trigger: TriggerInfo::Manual {
                        user_id: "tester".to_string(),
                    },
                    root_assets: vec![],
                    run_key: None,
                    labels: std::collections::HashMap::new(),
                    code_version: None,
                },
            )],
            Some("req_remote_error"),
        )
        .await;
        let err = result.expect_err("must fail");
        assert_eq!(err.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(err.message().contains("compactor unavailable"));
    }

    #[tokio::test]
    async fn append_events_and_compact_sends_canonical_fencing_request() {
        let captured_request = Arc::new(Mutex::new(None));
        let url = spawn_capture_server(Arc::clone(&captured_request)).await;
        let config = Config {
            debug: false,
            orchestration_compactor_url: Some(url),
            ..Config::default()
        };

        let result = compact_orchestration_events_with_fencing(
            &config,
            sample_storage(),
            vec!["ledger/orchestration/2026-03-28/01JREMOTEFENCED.json".to_string()],
            7,
            orchestration_compaction_lock_path(),
            Some("req_remote_fenced"),
        )
        .await;
        assert!(
            result.is_ok(),
            "remote fenced request must succeed: {result:?}"
        );

        let request = captured_request
            .lock()
            .expect("capture lock")
            .clone()
            .expect("captured request");
        assert_eq!(request.request_id.as_deref(), Some("req_remote_fenced"));
        assert_eq!(
            request.lock_path,
            "locks/orchestration.compaction.lock.json".to_string()
        );
        assert!(request.fencing_token > 0);
    }

    #[tokio::test]
    async fn append_events_and_compact_sends_bearer_auth_to_remote_compactor() {
        let base_url = spawn_bearer_server("Bearer test-token").await;
        let authed_url = format!(
            "http://bearer:test-token@{}",
            base_url.trim_start_matches("http://")
        );
        let config = Config {
            debug: false,
            orchestration_compactor_url: Some(authed_url),
            ..Config::default()
        };

        let result = compact_orchestration_events_with_fencing(
            &config,
            sample_storage(),
            vec!["ledger/orchestration/2026-03-28/01JREMOTEAUTH.json".to_string()],
            11,
            orchestration_compaction_lock_path(),
            Some("req_remote_auth"),
        )
        .await;

        assert!(result.is_ok(), "expected bearer-auth compaction to succeed");
    }

    #[tokio::test]
    async fn append_events_and_compact_maps_remote_conflict_to_conflict() {
        let url = spawn_error_server(
            StatusCode::CONFLICT,
            serde_json::json!({ "error": "stale_fencing_token" }),
        )
        .await;
        let config = Config {
            debug: false,
            orchestration_compactor_url: Some(url),
            ..Config::default()
        };

        let result = append_events_and_compact(
            &config,
            sample_storage(),
            vec![OrchestrationEvent::new(
                "tenant",
                "workspace",
                OrchestrationEventData::RunTriggered {
                    run_id: "run_remote_conflict".to_string(),
                    plan_id: "plan_remote_conflict".to_string(),
                    trigger: TriggerInfo::Manual {
                        user_id: "tester".to_string(),
                    },
                    root_assets: vec![],
                    run_key: None,
                    labels: std::collections::HashMap::new(),
                    code_version: None,
                },
            )],
            Some("req_remote_conflict"),
        )
        .await;

        let err = result.expect_err("remote conflict must fail");
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn append_events_and_compact_times_out_remote_compactor_requests() {
        let url = spawn_delay_server(Duration::from_secs(10)).await;
        let config = Config {
            debug: false,
            orchestration_compactor_url: Some(url),
            ..Config::default()
        };

        let result = tokio::time::timeout(
            Duration::from_secs(8),
            append_events_and_compact(
                &config,
                sample_storage(),
                vec![OrchestrationEvent::new(
                    "tenant",
                    "workspace",
                    OrchestrationEventData::RunTriggered {
                        run_id: "run_remote_timeout".to_string(),
                        plan_id: "plan_remote_timeout".to_string(),
                        trigger: TriggerInfo::Manual {
                            user_id: "tester".to_string(),
                        },
                        root_assets: vec![],
                        run_key: None,
                        labels: std::collections::HashMap::new(),
                        code_version: None,
                    },
                )],
                Some("req_remote_timeout"),
            ),
        )
        .await;

        match result {
            Ok(inner) => {
                let err = inner.expect_err("timed out compaction must fail");
                assert_eq!(err.status(), StatusCode::INTERNAL_SERVER_ERROR);
                assert!(err.message().contains("timed out"));
            }
            Err(elapsed) => panic!(
                "outer timeout elapsed; remote orchestration compaction still lacks a client timeout: {elapsed}"
            ),
        }
    }

    #[tokio::test]
    async fn append_events_and_compact_succeeds_when_lock_release_fails_after_visible_compaction() {
        let backend = Arc::new(LockReleaseFailureBackend::new());
        let storage = sample_storage_with_backend(backend);
        let compactor = MicroCompactor::new(storage.clone());

        let result = append_events_and_compact(
            &Config::default(),
            storage,
            vec![OrchestrationEvent::new(
                "tenant",
                "workspace",
                OrchestrationEventData::RunTriggered {
                    run_id: "run_release_failure".to_string(),
                    plan_id: "plan_release_failure".to_string(),
                    trigger: TriggerInfo::Manual {
                        user_id: "tester".to_string(),
                    },
                    root_assets: vec![],
                    run_key: None,
                    labels: std::collections::HashMap::new(),
                    code_version: None,
                },
            )],
            Some("req_release_failure"),
        )
        .await;

        assert!(
            result.is_ok(),
            "post-commit lock release failures should not fail the caller"
        );

        let (_manifest, state) = compactor.load_state().await.expect("load state");
        assert!(state.runs.contains_key("run_release_failure"));
    }
}
