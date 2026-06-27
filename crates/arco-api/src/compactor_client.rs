//! HTTP client for invoking the sync-compact endpoint.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;

use arco_catalog::SyncCompactor;
use arco_catalog::error::CatalogError;
use arco_core::sync_compact::{SyncCompactRequest, SyncCompactResponse};

use crate::config::{CompactorAuthConfig, CompactorAuthMode};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_GCP_IDENTITY_METADATA_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity";

/// HTTP client for invoking the compactor service.
#[derive(Clone)]
pub struct CompactorClient {
    base_url: String,
    client: reqwest::Client,
    auth: CompactorAuthConfig,
}

impl CompactorClient {
    /// Creates a new client targeting the given base URL.
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::new_with_auth(base_url, CompactorAuthConfig::default())
    }

    /// Creates a new client targeting the given base URL with auth configuration.
    #[must_use]
    pub fn new_with_auth(base_url: impl Into<String>, auth: CompactorAuthConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            base_url: base_url.into(),
            client,
            auth,
        }
    }

    fn sync_compact_url(&self) -> String {
        format!(
            "{}/internal/sync-compact",
            self.base_url.trim_end_matches('/')
        )
    }

    async fn authorization_header(&self) -> Result<Option<String>, CatalogError> {
        match self.auth.mode {
            CompactorAuthMode::None => Ok(None),
            CompactorAuthMode::StaticBearer => {
                let token = self
                    .auth
                    .static_bearer_token
                    .as_deref()
                    .ok_or_else(|| CatalogError::Storage {
                        message: "sync compactor static bearer token is not configured".to_string(),
                    })?
                    .trim();
                if token.is_empty() {
                    return Err(CatalogError::Storage {
                        message: "sync compactor static bearer token is empty".to_string(),
                    });
                }
                Ok(Some(format!("Bearer {token}")))
            }
            CompactorAuthMode::GcpIdToken => {
                let token = self.fetch_gcp_id_token().await?;
                Ok(Some(format!("Bearer {token}")))
            }
        }
    }

    async fn fetch_gcp_id_token(&self) -> Result<String, CatalogError> {
        let audience = self
            .auth
            .audience
            .as_deref()
            .unwrap_or_else(|| self.base_url.trim_end_matches('/'));
        let metadata_url = self
            .auth
            .metadata_url
            .as_deref()
            .unwrap_or(DEFAULT_GCP_IDENTITY_METADATA_URL);
        let response = self
            .client
            .get(metadata_url)
            .header("Metadata-Flavor", "Google")
            .query(&[("audience", audience), ("format", "full")])
            .send()
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed fetching GCP id token for sync compaction: {e}"),
            })?;
        let status = response.status();
        let body = response.text().await.map_err(|e| CatalogError::Storage {
            message: format!("failed reading GCP id token response: {e}"),
        })?;
        if !status.is_success() {
            return Err(CatalogError::Storage {
                message: format!("failed fetching GCP id token ({status}): {body}"),
            });
        }
        let token = body.trim();
        if token.is_empty() {
            return Err(CatalogError::Storage {
                message: "metadata server returned an empty GCP id token".to_string(),
            });
        }
        Ok(token.to_string())
    }
}

#[async_trait]
impl SyncCompactor for CompactorClient {
    async fn sync_compact(
        &self,
        request: SyncCompactRequest,
    ) -> Result<SyncCompactResponse, CatalogError> {
        let mut builder = self.client.post(self.sync_compact_url()).json(&request);
        if let Some(header) = self.authorization_header().await? {
            builder = builder.header("Authorization", header);
        }
        let response = builder.send().await.map_err(|e| CatalogError::Storage {
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
            StatusCode::CONFLICT => Err(CatalogError::CasFailed { message }),
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
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::extract::Query;
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use tokio::sync::oneshot;

    use crate::config::CompactorAuthConfig;
    use arco_core::sync_compact::VisibilityStatus;
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

    async fn spawn_auth_capture_server() -> (String, oneshot::Receiver<Option<String>>) {
        let (tx, rx) = oneshot::channel();
        let tx = Arc::new(Mutex::new(Some(tx)));
        let app = Router::new().route(
            "/internal/sync-compact",
            post(move |headers: HeaderMap| {
                let tx = tx.clone();
                async move {
                    let authorization = headers
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string);
                    if let Some(tx) = tx.lock().expect("capture sender").take() {
                        let _ = tx.send(authorization);
                    }
                    axum::Json(SyncCompactResponse {
                        manifest_version: "manifest-version".to_string(),
                        commit_ulid: "01TEST".to_string(),
                        manifest_id: "manifest-id".to_string(),
                        events_processed: 1,
                        snapshot_version: 1,
                        visibility_status: VisibilityStatus::Visible,
                        repair_pending: false,
                    })
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        (format!("http://{addr}"), rx)
    }

    #[derive(Debug, PartialEq, Eq)]
    struct MetadataRequest {
        flavor: Option<String>,
        audience: Option<String>,
        format: Option<String>,
    }

    async fn spawn_metadata_server(
        token: &'static str,
    ) -> (String, oneshot::Receiver<MetadataRequest>) {
        let (tx, rx) = oneshot::channel();
        let tx = Arc::new(Mutex::new(Some(tx)));
        let app = Router::new().route(
            "/identity",
            get(
                move |headers: HeaderMap, Query(params): Query<HashMap<String, String>>| {
                    let tx = tx.clone();
                    async move {
                        let request = MetadataRequest {
                            flavor: headers
                                .get("metadata-flavor")
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_string),
                            audience: params.get("audience").cloned(),
                            format: params.get("format").cloned(),
                        };
                        if let Some(tx) = tx.lock().expect("metadata sender").take() {
                            let _ = tx.send(request);
                        }
                        token
                    }
                },
            ),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        (format!("http://{addr}/identity"), rx)
    }

    fn sample_request() -> SyncCompactRequest {
        SyncCompactRequest {
            domain: "catalog".to_string(),
            event_paths: vec!["ledger/catalog/evt.json".to_string()],
            fencing_token: 1,
            lock_path: Some("locks/catalog.lock.json".to_string()),
            request_id: Some("req-1".to_string()),
        }
    }

    #[tokio::test]
    async fn sync_compact_preserves_conflict_status_for_lock_races() {
        let base_url =
            spawn_status_server(StatusCode::CONFLICT, json!({ "message": "lock busy" })).await;
        let client = CompactorClient::new(base_url);

        let result = client.sync_compact(sample_request()).await;
        assert!(matches!(result, Err(CatalogError::CasFailed { .. })));
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

    #[tokio::test]
    async fn sync_compact_attaches_static_bearer_token() {
        let (base_url, authorization_rx) = spawn_auth_capture_server().await;
        let client = CompactorClient::new_with_auth(
            base_url,
            CompactorAuthConfig::static_bearer("sync-compact-token"),
        );

        let response = client
            .sync_compact(sample_request())
            .await
            .expect("sync compact response");

        assert_eq!(response.events_processed, 1);
        assert_eq!(
            authorization_rx.await.expect("captured authorization"),
            Some("Bearer sync-compact-token".to_string())
        );
    }

    #[tokio::test]
    async fn sync_compact_fetches_gcp_id_token_and_attaches_authorization() {
        let (base_url, authorization_rx) = spawn_auth_capture_server().await;
        let (metadata_url, metadata_rx) = spawn_metadata_server("metadata-id-token").await;
        let client = CompactorClient::new_with_auth(
            base_url.clone(),
            CompactorAuthConfig::gcp_id_token(None).with_metadata_url(metadata_url),
        );

        let response = client
            .sync_compact(sample_request())
            .await
            .expect("sync compact response");

        assert_eq!(response.events_processed, 1);
        assert_eq!(
            authorization_rx.await.expect("captured authorization"),
            Some("Bearer metadata-id-token".to_string())
        );
        assert_eq!(
            metadata_rx.await.expect("metadata request"),
            MetadataRequest {
                flavor: Some("Google".to_string()),
                audience: Some(base_url),
                format: Some("full".to_string()),
            }
        );
    }
}
