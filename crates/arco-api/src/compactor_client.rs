//! HTTP client for the compactor sync-compaction endpoint.

use async_trait::async_trait;
use reqwest::StatusCode;

use arco_catalog::SyncCompactor;
use arco_catalog::error::CatalogError;
use arco_core::sync_compact::{SyncCompactRequest, SyncCompactResponse};

/// Sync-compaction HTTP client.
#[derive(Clone)]
pub struct CompactorClient {
    base_url: String,
    client: reqwest::Client,
}

impl CompactorClient {
    /// Creates a new compactor client.
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            client: reqwest::Client::new(),
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
