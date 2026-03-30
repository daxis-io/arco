//! Orchestration compaction helpers (sync or remote).

use arco_core::lock::{DEFAULT_LOCK_TTL, DistributedLock};
use arco_core::orchestration_compaction::{
    OrchestrationCompactRequest, OrchestrationCompactionResponse,
};
use arco_core::{ScopedStorage, VisibilityStatus};
use arco_flow::error::Error as FlowError;
use arco_flow::orchestration::OrchestrationLedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::events::OrchestrationEvent;
use arco_flow::orchestration::ledger::LedgerWriter;
use arco_flow::orchestration_compaction_lock_path;

use crate::config::Config;
use crate::error::ApiError;

/// Compacts orchestration events using a remote compactor or inline micro-compactor.
pub async fn compact_orchestration_events(
    config: &Config,
    storage: ScopedStorage,
    event_paths: Vec<String>,
) -> Result<(), ApiError> {
    if event_paths.is_empty() {
        return Ok(());
    }

    if let Some(url) = config.orchestration_compactor_url.as_ref() {
        post_remote_compaction(
            url,
            &OrchestrationCompactRequest {
                event_paths,
                fencing_token: None,
                lock_path: None,
                request_id: None,
            },
        )
        .await?;
        return Ok(());
    }

    if config.debug {
        let compactor = MicroCompactor::new(storage);
        compactor
            .compact_events(event_paths)
            .await
            .map_err(|e| ApiError::internal(format!("orchestration compaction failed: {e}")))?;
    }

    Ok(())
}

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
    if events.is_empty() {
        return Ok(Vec::new());
    }

    let lock_path = orchestration_compaction_lock_path();
    let lock = DistributedLock::new(storage.backend().clone(), lock_path);
    let guard = lock.acquire(DEFAULT_LOCK_TTL, 10).await.map_err(|e| {
        ApiError::conflict(format!(
            "failed to acquire orchestration compaction lock: {e}"
        ))
    })?;

    let event_paths: Vec<String> = events.iter().map(LedgerWriter::event_path).collect();
    let ledger = LedgerWriter::new(storage.clone());
    ledger
        .append_all(events)
        .await
        .map_err(|e| ApiError::internal(format!("failed to append orchestration events: {e}")))?;

    compact_orchestration_events_with_fencing(
        config,
        storage,
        event_paths.clone(),
        guard.fencing_token().sequence(),
        lock_path,
        request_id,
    )
    .await?;

    guard.release().await.map_err(|e| {
        ApiError::internal(format!(
            "failed to release orchestration compaction lock: {e}"
        ))
    })?;

    Ok(event_paths)
}

async fn compact_orchestration_events_with_fencing(
    config: &Config,
    storage: ScopedStorage,
    event_paths: Vec<String>,
    fencing_token: u64,
    lock_path: &str,
    request_id: Option<&str>,
) -> Result<(), ApiError> {
    if event_paths.is_empty() {
        return Ok(());
    }

    if let Some(url) = config.orchestration_compactor_url.as_ref() {
        let response = post_remote_compaction(
            url,
            &OrchestrationCompactRequest {
                event_paths,
                fencing_token: Some(fencing_token),
                lock_path: Some(lock_path.to_string()),
                request_id: request_id.map(str::to_string),
            },
        )
        .await?;
        return require_visible_compaction(response.visibility_status);
    }

    if config.debug {
        let compactor = MicroCompactor::new(storage);
        let result = compactor
            .compact_events_fenced(event_paths, fencing_token, lock_path)
            .await
            .map_err(map_flow_compaction_error)?;
        return require_visible_compaction(result.visibility_status.into());
    }

    Ok(())
}

async fn post_remote_compaction(
    url: &str,
    request: &OrchestrationCompactRequest,
) -> Result<OrchestrationCompactionResponse, ApiError> {
    let client = reqwest::Client::new();
    let endpoint = format!("{}/compact", url.trim_end_matches('/'));
    let response = client
        .post(endpoint)
        .json(request)
        .send()
        .await
        .map_err(|e| ApiError::internal(format!("orchestration compaction request failed: {e}")))?;

    if response.status().is_success() {
        return response
            .json::<OrchestrationCompactionResponse>()
            .await
            .map_err(|e| {
                ApiError::internal(format!(
                    "failed to decode orchestration compaction response: {e}"
                ))
            });
    }

    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|e| ApiError::internal(format!("compaction error body read failed: {e}")))?;
    let message = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| String::from_utf8_lossy(&body).to_string());

    Err(ApiError::internal(format!(
        "orchestration compaction failed ({status}): {message}"
    )))
}

fn require_visible_compaction(visibility_status: VisibilityStatus) -> Result<(), ApiError> {
    if visibility_status == VisibilityStatus::Visible {
        return Ok(());
    }

    Err(ApiError::internal(format!(
        "orchestration compaction did not become visible: {}",
        visibility_status.as_str()
    )))
}

fn map_flow_compaction_error(error: FlowError) -> ApiError {
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
    use std::sync::Arc;

    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::{Json, Router};

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

    fn sample_storage() -> ScopedStorage {
        let backend = Arc::new(arco_core::MemoryBackend::new());
        ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage")
    }

    #[tokio::test]
    async fn compact_orchestration_events_noop_when_empty() {
        let result = compact_orchestration_events(
            &Config::default(),
            sample_storage(),
            Vec::<String>::new(),
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn compact_orchestration_events_maps_remote_error_message() {
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

        let result = compact_orchestration_events(
            &config,
            sample_storage(),
            vec!["ledger/executions/evt.json".to_string()],
        )
        .await;
        let err = result.expect_err("must fail");
        assert_eq!(err.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(err.message().contains("compactor unavailable"));
    }
}
