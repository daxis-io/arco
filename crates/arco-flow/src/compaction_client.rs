//! Remote orchestration compaction client.
//!
//! Flow controller services (dispatcher, sweeper, automation) append orchestration events
//! to the ledger and must promptly trigger micro-compaction so Parquet projections stay
//! fresh for subsequent reconciliations.
//!
//! In cloud deployments, this is done via the `arco-flow-compactor` Cloud Run service.

#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
use arco_core::orchestration_compaction::{
    OrchestrationCompactRequest, OrchestrationCompactionResponse,
};
#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
use std::sync::OnceLock;
#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
use std::time::Duration;

use crate::error::{Error, Result};

#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
const METADATA_TIMEOUT: Duration = Duration::from_secs(2);

#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
/// Request orchestration compaction using caller-held fencing data.
///
/// This is the ADR-034 PI-1 internal write contract used by API and flow
/// services when they append orchestration events while holding the shared
/// compaction lock.
///
/// # Errors
///
/// Returns an error if the HTTP request fails, the service returns a non-success
/// status, or the response cannot be decoded.
pub async fn compact_orchestration_events_fenced(
    url: &str,
    event_paths: Vec<String>,
    fencing_token: u64,
    lock_path: &str,
    request_id: Option<&str>,
) -> Result<OrchestrationCompactionResponse> {
    if event_paths.is_empty() {
        return Ok(OrchestrationCompactionResponse {
            events_processed: 0,
            delta_id: None,
            manifest_id: String::new(),
            manifest_revision: String::new(),
            pointer_version: String::new(),
            visibility_status: arco_core::VisibilityStatus::Visible,
            repair_pending: false,
        });
    }

    compact_orchestration_events_fenced_impl(
        url,
        &event_paths,
        fencing_token,
        lock_path,
        request_id,
    )
    .await
}

#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
async fn compact_orchestration_events_fenced_impl(
    url: &str,
    event_paths: &[String],
    fencing_token: u64,
    lock_path: &str,
    request_id: Option<&str>,
) -> Result<OrchestrationCompactionResponse> {
    const MAX_ATTEMPTS: usize = 3;
    let client = shared_http_client()?;

    let (endpoint, bearer_token) = build_compactor_endpoint(url)?;
    let auth_header = build_auth_header(client, &endpoint, bearer_token, METADATA_TIMEOUT).await?;

    // Basic retry for transient transport and server failures.
    let mut attempt = 0;

    loop {
        attempt += 1;

        let mut request = client
            .post(&endpoint)
            .json(&OrchestrationCompactRequest {
                event_paths: event_paths.to_vec(),
                fencing_token,
                lock_path: lock_path.to_string(),
                request_id: request_id.map(str::to_string),
            })
            .timeout(REQUEST_TIMEOUT);

        if let Some(auth_header) = auth_header.as_deref() {
            request = request.header(reqwest::header::AUTHORIZATION, auth_header);
        }

        let response = request.send().await;

        match response {
            Ok(resp) if resp.status().is_success() => {
                return resp
                    .json::<OrchestrationCompactionResponse>()
                    .await
                    .map_err(|e| {
                        Error::dispatch(format!(
                            "failed to decode orchestration compaction response: {e}"
                        ))
                    });
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                let retryable = status.as_u16() == 429 || status.is_server_error();

                if retryable && attempt < MAX_ATTEMPTS {
                    // Exponential backoff with a small deterministic cap.
                    let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
                    let backoff_ms = 50_u64
                        .saturating_mul(2_u64.saturating_pow(exponent))
                        .min(500);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }

                return Err(map_http_error(status, &body));
            }
            Err(err) => {
                // Don't retry timeouts: failing fast avoids wedging controllers.
                if err.is_timeout() {
                    return Err(Error::dispatch(format!(
                        "orchestration compaction request timed out: {err}"
                    )));
                }

                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }

                return Err(Error::dispatch(format!(
                    "orchestration compaction request failed: {err}"
                )));
            }
        }
    }
}

#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
fn shared_http_client() -> Result<&'static reqwest::Client> {
    static HTTP_CLIENT: OnceLock<std::result::Result<reqwest::Client, String>> = OnceLock::new();

    match HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|e| format!("failed to build HTTP client: {e}"))
    }) {
        Ok(client) => Ok(client),
        Err(message) => Err(Error::dispatch(message.clone())),
    }
}

#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
fn map_http_error(status: reqwest::StatusCode, body: &str) -> Error {
    let message = extract_error_message(status, body);

    match status.as_u16() {
        400 | 422 => Error::Core(arco_core::Error::InvalidInput(message)),
        404 => Error::Core(arco_core::Error::NotFound(message)),
        409 => Error::Core(arco_core::Error::PreconditionFailed { message }),
        401 | 403 => Error::dispatch(format!(
            "orchestration compaction authorization failed (status={status}): {message}"
        )),
        _ => Error::dispatch(format!(
            "orchestration compaction failed (status={status}): {message}"
        )),
    }
}

#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
fn extract_error_message(status: reqwest::StatusCode, body: &str) -> String {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(body) {
        if let Some(message) = value.get("error").and_then(serde_json::Value::as_str) {
            if !message.trim().is_empty() {
                return message.trim().to_string();
            }
        }
    }

    let trimmed = body.trim();
    if trimmed.is_empty() {
        format!("remote service returned HTTP {status}")
    } else {
        trimmed.to_string()
    }
}

#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
fn build_compactor_endpoint(url: &str) -> Result<(String, Option<String>)> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|e| Error::configuration(format!("invalid orchestration compactor URL: {e}")))?;

    let bearer_token = bearer_token_from_url(&parsed);

    // Strip userinfo from the URL before sending the request to avoid:
    // 1) leaking credentials in logs / metrics
    // 2) implicit basic-auth headers from the HTTP client
    let mut sanitized = parsed;
    let _ = sanitized.set_username("");
    let _ = sanitized.set_password(None);

    let endpoint = format!("{}/compact", sanitized.as_str().trim_end_matches('/'));
    Ok((endpoint, bearer_token))
}

#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
fn bearer_token_from_url(url: &reqwest::Url) -> Option<String> {
    let username = url.username();
    if username != "bearer" {
        return None;
    }
    url.password().map(str::to_string)
}

#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
async fn build_auth_header(
    client: &reqwest::Client,
    endpoint: &str,
    bearer_token: Option<String>,
    metadata_timeout: Duration,
) -> Result<Option<String>> {
    if let Some(token) = bearer_token {
        return Ok(Some(format!("Bearer {token}")));
    }

    // Default to GCP ID-token auth when calling Cloud Run. This keeps internal
    // services private-by-default while allowing local/dev `http://127.0.0.1:*`
    // compactors to work without auth.
    if !is_cloud_run_endpoint(endpoint) {
        return Ok(None);
    }

    let audience = endpoint.trim_end_matches("/compact");
    let id_token = fetch_gcp_identity_token(client, audience, metadata_timeout).await?;
    Ok(Some(format!("Bearer {id_token}")))
}

#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
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

#[cfg(any(feature = "gcp", feature = "test-utils", feature = "http-client"))]
async fn fetch_gcp_identity_token(
    client: &reqwest::Client,
    audience: &str,
    timeout: Duration,
) -> Result<String> {
    // Works on Cloud Run, GCE, and GKE with metadata server enabled.
    let mut url = reqwest::Url::parse(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity",
    )
    .map_err(|e| Error::dispatch(format!("invalid metadata identity URL: {e}")))?;

    url.query_pairs_mut()
        .append_pair("audience", audience)
        .append_pair("format", "full");

    let response = client
        .get(url)
        .header("Metadata-Flavor", "Google")
        .timeout(timeout)
        .send()
        .await
        .map_err(|e| Error::dispatch(format!("metadata identity token request failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(Error::dispatch(format!(
            "metadata identity token request failed (status={status}): {body}"
        )));
    }

    response
        .text()
        .await
        .map_err(|e| Error::dispatch(format!("metadata identity token read failed: {e}")))
}

#[cfg(not(any(feature = "gcp", feature = "test-utils", feature = "http-client")))]
/// Request orchestration compaction using caller-held fencing data.
///
/// # Errors
///
/// Always returns a configuration error unless the `gcp`, `test-utils`,
/// or `http-client`
/// feature is enabled.
pub async fn compact_orchestration_events_fenced(
    _url: &str,
    _event_paths: Vec<String>,
    _fencing_token: u64,
    _lock_path: &str,
    _request_id: Option<&str>,
) -> Result<arco_core::OrchestrationCompactionResponse> {
    Err(Error::configuration(
        "orchestration compaction requires the 'gcp', 'test-utils', or 'http-client' feature",
    ))
}
