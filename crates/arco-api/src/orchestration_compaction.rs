//! Orchestration compaction helpers (sync or remote).

use std::time::Duration;

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

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const METADATA_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_ATTEMPTS: usize = 3;

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
        compact_remote(url, event_paths).await?;
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

async fn compact_remote(url: &str, event_paths: Vec<String>) -> Result<(), ApiError> {
    let client = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .map_err(|e| ApiError::internal(format!("failed to build HTTP client: {e}")))?;

    let (endpoint, bearer_token) = build_compactor_endpoint(url)?;
    let auth_header = build_auth_header(&client, &endpoint, bearer_token, METADATA_TIMEOUT).await?;

    let mut attempt = 0;
    loop {
        attempt += 1;

        let mut request = client
            .post(&endpoint)
            .json(&CompactRequest {
                event_paths: event_paths.clone(),
            })
            .timeout(REQUEST_TIMEOUT);

        if let Some(auth_header) = auth_header.as_deref() {
            request = request.header(reqwest::header::AUTHORIZATION, auth_header);
        }

        let response = request.send().await;
        match response {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                let status = resp.status();
                let body = resp.bytes().await.unwrap_or_default();

                let message = serde_json::from_slice::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("error")
                            .and_then(|v| v.as_str())
                            .map(str::to_string)
                    })
                    .unwrap_or_else(|| String::from_utf8_lossy(&body).to_string());

                let retryable =
                    status.as_u16() == 409 || status.as_u16() == 429 || status.is_server_error();

                if retryable && attempt < MAX_ATTEMPTS {
                    // Exponential backoff with cap; keep API path bounded.
                    let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
                    let backoff_ms = 50_u64
                        .saturating_mul(2_u64.saturating_pow(exponent))
                        .min(500);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }

                return Err(ApiError::internal(format!(
                    "orchestration compaction failed ({status}): {message}"
                )));
            }
            Err(err) => {
                // Don't retry timeouts: the API request path should fail fast.
                if err.is_timeout() {
                    return Err(ApiError::internal(format!(
                        "orchestration compaction request timed out: {err}"
                    )));
                }

                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }

                return Err(ApiError::internal(format!(
                    "orchestration compaction request failed: {err}"
                )));
            }
        }
    }
}

fn build_compactor_endpoint(url: &str) -> Result<(String, Option<String>), ApiError> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|e| ApiError::internal(format!("invalid orchestration compactor URL: {e}")))?;

    let bearer_token = bearer_token_from_url(&parsed);

    let mut sanitized = parsed;
    let _ = sanitized.set_username("");
    let _ = sanitized.set_password(None);

    let endpoint = format!("{}/compact", sanitized.as_str().trim_end_matches('/'));
    Ok((endpoint, bearer_token))
}

fn bearer_token_from_url(url: &reqwest::Url) -> Option<String> {
    let username = url.username();
    if username != "bearer" {
        return None;
    }
    url.password().map(str::to_string)
}

async fn build_auth_header(
    client: &reqwest::Client,
    endpoint: &str,
    bearer_token: Option<String>,
    metadata_timeout: Duration,
) -> Result<Option<String>, ApiError> {
    if let Some(token) = bearer_token {
        return Ok(Some(format!("Bearer {token}")));
    }

    if !is_cloud_run_endpoint(endpoint) {
        return Ok(None);
    }

    let audience = endpoint.trim_end_matches("/compact");
    let id_token = fetch_gcp_identity_token(client, audience, metadata_timeout).await?;
    Ok(Some(format!("Bearer {id_token}")))
}

fn is_cloud_run_endpoint(endpoint: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(endpoint) else {
        return false;
    };

    if url.scheme() != "https" {
        return false;
    }

    let Some(host) = url.host_str() else {
        return false;
    };

    host.ends_with(".run.app") || host.ends_with(".a.run.app")
}

async fn fetch_gcp_identity_token(
    client: &reqwest::Client,
    audience: &str,
    timeout: Duration,
) -> Result<String, ApiError> {
    let mut url = reqwest::Url::parse(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity",
    )
    .map_err(|e| ApiError::internal(format!("invalid metadata identity URL: {e}")))?;

    url.query_pairs_mut()
        .append_pair("audience", audience)
        .append_pair("format", "full");

    let response = client
        .get(url)
        .header("Metadata-Flavor", "Google")
        .timeout(timeout)
        .send()
        .await
        .map_err(|e| ApiError::internal(format!("metadata identity token request failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(ApiError::internal(format!(
            "metadata identity token request failed (status={status}): {body}"
        )));
    }

    response
        .text()
        .await
        .map_err(|e| ApiError::internal(format!("metadata identity token read failed: {e}")))
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
