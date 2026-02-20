//! HTTP client for invoking the sync-compact endpoint.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;

use arco_catalog::SyncCompactor;
use arco_catalog::error::CatalogError;
use arco_core::sync_compact::{SyncCompactRequest, SyncCompactResponse};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// HTTP client for invoking the compactor service.
#[derive(Clone)]
pub struct CompactorClient {
    base_url: String,
    client: reqwest::Client,
}

impl CompactorClient {
    /// Creates a new client targeting the given base URL.
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            base_url: base_url.into(),
            client,
        }
    }

    fn sync_compact_url(&self) -> String {
        format!(
            "{}/internal/sync-compact",
            self.base_url.trim_end_matches('/')
        )
    }
}

#[async_trait]
impl SyncCompactor for CompactorClient {
    async fn sync_compact(
        &self,
        request: SyncCompactRequest,
    ) -> Result<SyncCompactResponse, CatalogError> {
        let response = self
            .client
            .post(self.sync_compact_url())
            .json(&request)
            .send()
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("sync compaction request failed: {e}"),
            })?;

        if response.status().is_success() {
            return response.json::<SyncCompactResponse>().await.map_err(|e| {
                CatalogError::Serialization {
                    message: format!("invalid sync compaction response: {e}"),
                }
            });
        }

        let status = response.status();
        let body = response.bytes().await.map_err(|e| CatalogError::Storage {
            message: format!("failed reading sync compaction error body: {e}"),
        })?;
        let message = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .get("message")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| String::from_utf8_lossy(&body).to_string());

        match status {
            StatusCode::CONFLICT => Err(CatalogError::PreconditionFailed { message }),
            StatusCode::BAD_REQUEST => Err(CatalogError::Validation { message }),
            StatusCode::NOT_IMPLEMENTED => Err(CatalogError::InvariantViolation { message }),
            _ => Err(CatalogError::Storage {
                message: format!("sync compaction failed ({status}): {message}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::routing::post;
    use serde_json::json;

    async fn spawn_status_server(status: StatusCode, body: serde_json::Value) -> String {
        let app = Router::new().route(
            "/internal/sync-compact",
            post(move || {
                let status = status;
                let body = body.clone();
                async move { (status, axum::Json(body)) }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        format!("http://{addr}")
    }

    fn sample_request() -> SyncCompactRequest {
        SyncCompactRequest {
            domain: "catalog".to_string(),
            event_paths: vec!["ledger/catalog/evt.json".to_string()],
            fencing_token: 1,
            request_id: Some("req-1".to_string()),
        }
    }

    #[tokio::test]
    async fn sync_compact_maps_conflict_to_precondition_failed() {
        let base_url =
            spawn_status_server(StatusCode::CONFLICT, json!({ "message": "stale" })).await;
        let client = CompactorClient::new(base_url);

        let result = client.sync_compact(sample_request()).await;
        assert!(matches!(
            result,
            Err(CatalogError::PreconditionFailed { .. })
        ));
    }

    #[tokio::test]
    async fn sync_compact_maps_bad_request_to_validation() {
        let base_url =
            spawn_status_server(StatusCode::BAD_REQUEST, json!({ "message": "bad" })).await;
        let client = CompactorClient::new(base_url);

        let result = client.sync_compact(sample_request()).await;
        assert!(matches!(result, Err(CatalogError::Validation { .. })));
    }

    #[tokio::test]
    async fn sync_compact_maps_not_implemented_to_invariant_violation() {
        let base_url = spawn_status_server(
            StatusCode::NOT_IMPLEMENTED,
            json!({ "message": "not implemented" }),
        )
        .await;
        let client = CompactorClient::new(base_url);

        let result = client.sync_compact(sample_request()).await;
        assert!(matches!(
            result,
            Err(CatalogError::InvariantViolation { .. })
        ));
    }
}
