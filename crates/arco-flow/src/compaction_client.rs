//! Remote orchestration compaction client.
//!
//! Flow controller services (dispatcher, sweeper, automation) append orchestration events
//! to the ledger and must promptly trigger micro-compaction so Parquet projections stay
//! fresh for subsequent reconciliations.
//!
//! In cloud deployments, this is done via the `arco-flow-compactor` Cloud Run service:
//! `POST {ARCO_ORCH_COMPACTOR_URL}/compact` with `{ "event_paths": [...] }`.

use serde::Serialize;

use crate::error::{Error, Result};

#[derive(Debug, Serialize)]
struct CompactRequest<'a> {
    event_paths: &'a [String],
}

/// Request compaction of the provided orchestration ledger event paths.
///
/// The compactor service is expected to be idempotent: re-compacting the same paths
/// should be a no-op.
///
/// # Errors
///
/// Returns an error if the HTTP request fails or the service returns a non-success status.
pub async fn compact_orchestration_events(url: &str, event_paths: Vec<String>) -> Result<()> {
    if event_paths.is_empty() {
        return Ok(());
    }

    compact_orchestration_events_impl(url, &event_paths).await
}

#[cfg(any(feature = "gcp", feature = "test-utils"))]
async fn compact_orchestration_events_impl(url: &str, event_paths: &[String]) -> Result<()> {
    use std::time::Duration;

    const MAX_ATTEMPTS: usize = 3;
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
    const METADATA_TIMEOUT: Duration = Duration::from_secs(2);

    let client = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .map_err(|e| Error::dispatch(format!("failed to build HTTP client: {e}")))?;

    let (endpoint, bearer_token) = build_compactor_endpoint(url)?;
    let auth_header = build_auth_header(&client, &endpoint, bearer_token, METADATA_TIMEOUT).await?;

    // Basic retry for transient failures / CAS conflicts surfaced as HTTP status codes.
    let mut attempt = 0;

    loop {
        attempt += 1;

        let mut request = client
            .post(&endpoint)
            .json(&CompactRequest { event_paths })
            .timeout(REQUEST_TIMEOUT);

        if let Some(auth_header) = auth_header.as_deref() {
            request = request.header(reqwest::header::AUTHORIZATION, auth_header);
        }

        let response = request.send().await;

        match response {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();

                let retryable =
                    status.as_u16() == 409 || status.as_u16() == 429 || status.is_server_error();

                if retryable && attempt < MAX_ATTEMPTS {
                    // Exponential backoff with a small deterministic cap.
                    let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
                    let backoff_ms = 50_u64
                        .saturating_mul(2_u64.saturating_pow(exponent))
                        .min(500);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }

                return Err(Error::dispatch(format!(
                    "orchestration compaction failed (status={status}): {body}"
                )));
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

#[cfg(any(feature = "gcp", feature = "test-utils"))]
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

#[cfg(any(feature = "gcp", feature = "test-utils"))]
fn bearer_token_from_url(url: &reqwest::Url) -> Option<String> {
    let username = url.username();
    if username != "bearer" {
        return None;
    }
    url.password().map(str::to_string)
}

#[cfg(any(feature = "gcp", feature = "test-utils"))]
async fn build_auth_header(
    client: &reqwest::Client,
    endpoint: &str,
    bearer_token: Option<String>,
    metadata_timeout: std::time::Duration,
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

#[cfg(any(feature = "gcp", feature = "test-utils"))]
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

#[cfg(any(feature = "gcp", feature = "test-utils"))]
async fn fetch_gcp_identity_token(
    client: &reqwest::Client,
    audience: &str,
    timeout: std::time::Duration,
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

#[cfg(not(any(feature = "gcp", feature = "test-utils")))]
async fn compact_orchestration_events_impl(_url: &str, _event_paths: &[String]) -> Result<()> {
    Err(Error::configuration(
        "orchestration compaction requires the 'gcp' feature",
    ))
}
