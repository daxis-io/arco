//! Orchestration compaction helpers (sync or remote).

use serde::Serialize;

use arco_core::ScopedStorage;
use arco_flow::orchestration::OrchestrationLedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::events::OrchestrationEvent;
use arco_flow::orchestration::ledger::LedgerWriter;

use crate::config::Config;
use crate::error::ApiError;

#[derive(Debug, Serialize)]
struct CompactRequest {
    event_paths: Vec<String>,
}

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
        let client = reqwest::Client::new();
        let endpoint = format!("{}/compact", url.trim_end_matches('/'));
        let response = client
            .post(endpoint)
            .json(&CompactRequest { event_paths })
            .send()
            .await
            .map_err(|e| {
                ApiError::internal(format!("orchestration compaction request failed: {e}"))
            })?;

        if response.status().is_success() {
            return Ok(());
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

        return Err(ApiError::internal(format!(
            "orchestration compaction failed ({status}): {message}"
        )));
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

/// Ledger writer that triggers orchestration compaction after each event append.
pub struct CompactingLedgerWriter {
    ledger: LedgerWriter,
    storage: ScopedStorage,
    config: Config,
}

impl CompactingLedgerWriter {
    /// Creates a new compacting ledger writer.
    #[must_use]
    pub fn new(storage: ScopedStorage, config: Config) -> Self {
        Self {
            ledger: LedgerWriter::new(storage.clone()),
            storage,
            config,
        }
    }
}

impl OrchestrationLedgerWriter for CompactingLedgerWriter {
    async fn write_event(&self, event: &OrchestrationEvent) -> Result<(), String> {
        let path = LedgerWriter::event_path(event);
        self.ledger
            .append(event.clone())
            .await
            .map_err(|e| format!("{e}"))?;
        compact_orchestration_events(&self.config, self.storage.clone(), vec![path])
            .await
            .map_err(|e| format!("{e:?}"))
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
