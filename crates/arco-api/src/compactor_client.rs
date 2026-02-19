//! HTTP client for invoking the sync-compact endpoint.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use reqwest::{StatusCode, Url};

use arco_catalog::SyncCompactor;
use arco_catalog::error::CatalogError;
use arco_core::sync_compact::{SyncCompactRequest, SyncCompactResponse};

use crate::config::{CompactorAuthConfig, CompactorAuthMode};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_SYNC_COMPACT_ATTEMPTS: usize = 2;
const RETRY_BASE_BACKOFF_MS: u64 = 25;
const RETRY_MAX_BACKOFF_MS: u64 = 250;
#[cfg(feature = "gcp")]
const GCP_METADATA_IDENTITY_URL: &str =
    "http://metadata/computeMetadata/v1/instance/service-accounts/default/identity";

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
    pub fn new(base_url: impl Into<String>, auth: CompactorAuthConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self::with_client(base_url, auth, client)
    }

    #[cfg(test)]
    fn new_with_client(
        base_url: impl Into<String>,
        auth: CompactorAuthConfig,
        client: reqwest::Client,
    ) -> Self {
        Self::with_client(base_url, auth, client)
    }

    fn with_client(
        base_url: impl Into<String>,
        auth: CompactorAuthConfig,
        client: reqwest::Client,
    ) -> Self {
        let base_url = base_url.into();
        let (normalized_base_url, auth) = Self::normalize_legacy_auth(base_url, auth);
        Self {
            base_url: normalized_base_url,
            client,
            auth,
        }
    }

    fn normalize_legacy_auth(
        base_url: String,
        mut auth: CompactorAuthConfig,
    ) -> (String, CompactorAuthConfig) {
        let Ok(mut parsed) = Url::parse(&base_url) else {
            return (base_url, auth);
        };

        let legacy_token = {
            let username = parsed.username();
            let password = parsed.password();
            if username.is_empty() && password.is_none() {
                None
            } else {
                Some(password.unwrap_or(username).to_string())
            }
        };

        if legacy_token.is_none() {
            return (base_url, auth);
        }

        let _ = parsed.set_username("");
        let _ = parsed.set_password(None);

        if auth.mode == CompactorAuthMode::None
            || (auth.mode == CompactorAuthMode::StaticBearer && auth.static_bearer_token.is_none())
        {
            tracing::warn!(
                "ARCO_COMPACTOR_URL userinfo credentials are deprecated; use explicit ARCO_COMPACTOR_AUTH_* env config"
            );
            auth.mode = CompactorAuthMode::StaticBearer;
            auth.static_bearer_token = legacy_token;
        }

        (parsed.to_string(), auth)
    }

    fn sync_compact_url(&self) -> String {
        format!(
            "{}/internal/sync-compact",
            self.base_url.trim_end_matches('/')
        )
    }

    fn is_retryable_status(status: StatusCode) -> bool {
        matches!(
            status,
            StatusCode::REQUEST_TIMEOUT
                | StatusCode::TOO_MANY_REQUESTS
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        )
    }

    fn is_retryable_transport_error(error: &reqwest::Error) -> bool {
        error.is_connect() || error.is_timeout()
    }

    fn retry_delay(attempt: usize) -> Duration {
        let shift = u32::try_from(attempt.saturating_sub(1))
            .unwrap_or(u32::MAX)
            .min(10);
        let backoff_ms = RETRY_BASE_BACKOFF_MS
            .saturating_mul(1_u64 << shift)
            .min(RETRY_MAX_BACKOFF_MS);
        let jitter_window_ms = (backoff_ms / 2).max(1);
        let jitter_seed = u64::from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_nanos(),
        );
        let jitter_ms = jitter_seed % (jitter_window_ms + 1);
        Duration::from_millis(
            backoff_ms
                .saturating_add(jitter_ms)
                .min(RETRY_MAX_BACKOFF_MS),
        )
    }

    async fn resolve_bearer_token(&self) -> Result<Option<String>, CatalogError> {
        match self.auth.mode {
            CompactorAuthMode::None => Ok(None),
            CompactorAuthMode::StaticBearer => {
                let token = self.auth.static_bearer_token.as_ref().ok_or_else(|| {
                    CatalogError::Validation {
                        message: "compactor auth mode static_bearer requires a token".to_string(),
                    }
                })?;
                Ok(Some(token.clone()))
            }
            CompactorAuthMode::GcpIdToken => {
                #[cfg(feature = "gcp")]
                {
                    let token = self.fetch_gcp_id_token().await?;
                    Ok(Some(token))
                }

                #[cfg(not(feature = "gcp"))]
                {
                    Err(CatalogError::Validation {
                        message:
                            "compactor auth mode gcp_id_token requires the arco-api gcp feature"
                                .to_string(),
                    })
                }
            }
        }
    }

    #[cfg(feature = "gcp")]
    async fn fetch_gcp_id_token(&self) -> Result<String, CatalogError> {
        let metadata_base = self
            .auth
            .metadata_url
            .as_deref()
            .unwrap_or(GCP_METADATA_IDENTITY_URL);
        let audience = self.auth.audience.as_deref().unwrap_or(&self.base_url);

        let mut metadata_url = Url::parse(metadata_base).map_err(|e| CatalogError::Validation {
            message: format!("invalid metadata url for gcp_id_token auth: {e}"),
        })?;
        metadata_url
            .query_pairs_mut()
            .append_pair("audience", audience)
            .append_pair("format", "full");

        let response = self
            .client
            .get(metadata_url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .map_err(|e| CatalogError::Storage {
                message: format!("failed to fetch gcp id token from metadata: {e}"),
            })?;

        let status = response.status();
        let body = response.text().await.map_err(|e| CatalogError::Storage {
            message: format!("failed to read gcp id token response: {e}"),
        })?;

        if !status.is_success() {
            return Err(CatalogError::Storage {
                message: format!("failed to fetch gcp id token: {status}"),
            });
        }

        Ok(body)
    }

    async fn map_error_response(response: reqwest::Response) -> CatalogError {
        let status = response.status();
        let body = response.bytes().await.unwrap_or_default();
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
            StatusCode::CONFLICT => CatalogError::PreconditionFailed { message },
            StatusCode::BAD_REQUEST => CatalogError::Validation { message },
            StatusCode::NOT_IMPLEMENTED => CatalogError::InvariantViolation { message },
            _ => CatalogError::Storage {
                message: format!("sync compaction failed ({status}): {message}"),
            },
        }
    }
}

#[async_trait]
impl SyncCompactor for CompactorClient {
    async fn sync_compact(
        &self,
        request: SyncCompactRequest,
    ) -> Result<SyncCompactResponse, CatalogError> {
        let bearer_token = self.resolve_bearer_token().await?;
        let url = self.sync_compact_url();

        for attempt in 1..=MAX_SYNC_COMPACT_ATTEMPTS {
            let mut req = self.client.post(&url).json(&request);
            if let Some(token) = bearer_token.as_deref() {
                req = req.bearer_auth(token);
            }

            let response = match req.send().await {
                Ok(response) => response,
                Err(e) => {
                    if attempt < MAX_SYNC_COMPACT_ATTEMPTS && Self::is_retryable_transport_error(&e)
                    {
                        tokio::time::sleep(Self::retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(CatalogError::Storage {
                        message: format!("sync compaction request failed: {e}"),
                    });
                }
            };

            if response.status().is_success() {
                return response.json::<SyncCompactResponse>().await.map_err(|e| {
                    CatalogError::Serialization {
                        message: format!("invalid sync compaction response: {e}"),
                    }
                });
            }

            if attempt < MAX_SYNC_COMPACT_ATTEMPTS && Self::is_retryable_status(response.status()) {
                tokio::time::sleep(Self::retry_delay(attempt)).await;
                continue;
            }

            return Err(Self::map_error_response(response).await);
        }

        Err(CatalogError::Storage {
            message: "sync compaction failed after retry attempts".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    use tokio::net::TcpListener;
    use tokio::sync::{Mutex, oneshot};

    use axum::http::HeaderMap;
    use axum::response::IntoResponse;
    #[cfg(feature = "gcp")]
    use axum::routing::get;
    use axum::routing::post;
    use axum::{Json, Router};

    fn sample_request() -> SyncCompactRequest {
        SyncCompactRequest {
            domain: "catalog".to_string(),
            event_paths: vec!["ledger/catalog/2026-01-01/evt.json".to_string()],
            fencing_token: 7,
            request_id: Some("req-1".to_string()),
        }
    }

    fn sample_response() -> SyncCompactResponse {
        SyncCompactResponse {
            manifest_version: "v1".to_string(),
            commit_ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string(),
            events_processed: 1,
            snapshot_version: 1,
        }
    }

    async fn spawn_server(app: Router) -> (String, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener local addr");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve test app");
        });

        (format!("http://{addr}"), shutdown_tx)
    }

    #[tokio::test]
    async fn none_mode_sends_no_authorization_header() {
        let seen_auth = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
        let seen_auth_state = seen_auth.clone();

        let app = Router::new().route(
            "/internal/sync-compact",
            post(move |headers: HeaderMap, _req: Json<SyncCompactRequest>| {
                let seen_auth_state = seen_auth_state.clone();
                async move {
                    let auth = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(ToString::to_string);
                    seen_auth_state.lock().await.push(auth);
                    Json(sample_response())
                }
            }),
        );

        let (base_url, shutdown) = spawn_server(app).await;
        let client = CompactorClient::new(base_url, CompactorAuthConfig::none());
        let response = client
            .sync_compact(sample_request())
            .await
            .expect("sync compact");
        assert_eq!(response.events_processed, 1);

        let seen = seen_auth.lock().await;
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0], None);
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn static_bearer_mode_sets_authorization_header() {
        let seen_auth = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
        let seen_auth_state = seen_auth.clone();

        let app = Router::new().route(
            "/internal/sync-compact",
            post(move |headers: HeaderMap, _req: Json<SyncCompactRequest>| {
                let seen_auth_state = seen_auth_state.clone();
                async move {
                    let auth = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(ToString::to_string);
                    seen_auth_state.lock().await.push(auth);
                    Json(sample_response())
                }
            }),
        );

        let (base_url, shutdown) = spawn_server(app).await;
        let auth = CompactorAuthConfig::static_bearer("token-abc");
        let client = CompactorClient::new(base_url, auth);
        client
            .sync_compact(sample_request())
            .await
            .expect("sync compact");

        let seen = seen_auth.lock().await;
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].as_deref(), Some("Bearer token-abc"));
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn retries_once_on_503_and_succeeds() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_state = attempts.clone();

        let app = Router::new().route(
            "/internal/sync-compact",
            post(move || {
                let attempts_state = attempts_state.clone();
                async move {
                    let n = attempts_state.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(serde_json::json!({ "message": "try again" })),
                        )
                            .into_response()
                    } else {
                        (StatusCode::OK, Json(sample_response())).into_response()
                    }
                }
            }),
        );

        let (base_url, shutdown) = spawn_server(app).await;
        let client = CompactorClient::new(base_url, CompactorAuthConfig::none());
        let start = Instant::now();
        let response = client.sync_compact(sample_request()).await.expect("retry");
        assert_eq!(response.snapshot_version, 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(
            start.elapsed() >= Duration::from_millis(20),
            "retry path should include bounded backoff delay"
        );
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn legacy_url_userinfo_maps_to_static_bearer_for_deprecation_cycle() {
        let seen_auth = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
        let seen_auth_state = seen_auth.clone();

        let app = Router::new().route(
            "/internal/sync-compact",
            post(move |headers: HeaderMap, _req: Json<SyncCompactRequest>| {
                let seen_auth_state = seen_auth_state.clone();
                async move {
                    let auth = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(ToString::to_string);
                    seen_auth_state.lock().await.push(auth);
                    Json(sample_response())
                }
            }),
        );

        let (base_url, shutdown) = spawn_server(app).await;
        let legacy = base_url.replacen("http://", "http://legacy-token@", 1);
        let client = CompactorClient::new(legacy, CompactorAuthConfig::none());
        client
            .sync_compact(sample_request())
            .await
            .expect("sync compact");

        let seen = seen_auth.lock().await;
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].as_deref(), Some("Bearer legacy-token"));
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn legacy_url_userinfo_does_not_override_explicit_static_bearer() {
        let seen_auth = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
        let seen_auth_state = seen_auth.clone();

        let app = Router::new().route(
            "/internal/sync-compact",
            post(move |headers: HeaderMap, _req: Json<SyncCompactRequest>| {
                let seen_auth_state = seen_auth_state.clone();
                async move {
                    let auth = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(ToString::to_string);
                    seen_auth_state.lock().await.push(auth);
                    Json(sample_response())
                }
            }),
        );

        let (base_url, shutdown) = spawn_server(app).await;
        let legacy = base_url.replacen("http://", "http://legacy-token@", 1);
        let auth = CompactorAuthConfig::static_bearer("explicit-token");
        let client = CompactorClient::new(legacy, auth);
        client
            .sync_compact(sample_request())
            .await
            .expect("sync compact");

        let seen = seen_auth.lock().await;
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].as_deref(), Some("Bearer explicit-token"));
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn retries_transport_error_once_before_failing() {
        let probe = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = probe.local_addr().expect("listener local addr");
        drop(probe);

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .expect("http client");
        let compactor = CompactorClient::new_with_client(
            format!("http://{addr}"),
            CompactorAuthConfig::none(),
            http_client,
        );

        let start = Instant::now();
        let err = compactor
            .sync_compact(sample_request())
            .await
            .expect_err("transport failure should bubble up");
        assert!(err.to_string().contains("request failed"));
        assert!(
            start.elapsed() >= Duration::from_millis(20),
            "transport retry path should include bounded backoff delay"
        );
    }

    #[cfg(not(feature = "gcp"))]
    #[tokio::test]
    async fn gcp_mode_fails_closed_without_gcp_feature() {
        let app = Router::new().route(
            "/internal/sync-compact",
            post(|| async { Json(sample_response()) }),
        );
        let (base_url, shutdown) = spawn_server(app).await;

        let client = CompactorClient::new(base_url, CompactorAuthConfig::gcp_id_token(None));
        let err = client
            .sync_compact(sample_request())
            .await
            .expect_err("expected gcp auth failure");
        let message = err.to_string();
        assert!(message.contains("gcp_id_token"));
        assert!(message.contains("gcp feature"));

        let _ = shutdown.send(());
    }

    #[cfg(feature = "gcp")]
    #[tokio::test]
    async fn gcp_mode_fetches_id_token_and_sets_authorization_header() {
        let seen_auth = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
        let seen_auth_state = seen_auth.clone();

        let app = Router::new()
            .route(
                "/metadata",
                get(|| async { (StatusCode::OK, "id-token-from-metadata") }),
            )
            .route(
                "/internal/sync-compact",
                post(move |headers: HeaderMap, _req: Json<SyncCompactRequest>| {
                    let seen_auth_state = seen_auth_state.clone();
                    async move {
                        let auth = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .map(ToString::to_string);
                        seen_auth_state.lock().await.push(auth);
                        Json(sample_response())
                    }
                }),
            );

        let (base_url, shutdown) = spawn_server(app).await;
        let auth = CompactorAuthConfig::gcp_id_token(Some("https://compactor.internal"))
            .with_metadata_url(format!("{base_url}/metadata"));
        let client = CompactorClient::new(base_url, auth);
        client
            .sync_compact(sample_request())
            .await
            .expect("sync compact");

        let seen = seen_auth.lock().await;
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].as_deref(), Some("Bearer id-token-from-metadata"));
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn static_bearer_mode_missing_token_fails() {
        let app = Router::new().route(
            "/internal/sync-compact",
            post(|| async { Json(sample_response()) }),
        );
        let (base_url, shutdown) = spawn_server(app).await;

        let mut auth = CompactorAuthConfig::none();
        auth.mode = CompactorAuthMode::StaticBearer;

        let client = CompactorClient::new(base_url, auth);
        let err = client
            .sync_compact(sample_request())
            .await
            .expect_err("expected missing token validation error");
        assert!(err.to_string().contains("static_bearer"));

        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn timeout_is_respected_with_custom_client() {
        let app = Router::new().route(
            "/internal/sync-compact",
            post(|| async {
                tokio::time::sleep(Duration::from_millis(120)).await;
                Json(sample_response())
            }),
        );
        let (base_url, shutdown) = spawn_server(app).await;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(20))
            .build()
            .expect("http client");

        let compactor =
            CompactorClient::new_with_client(base_url, CompactorAuthConfig::none(), client);
        let err = compactor
            .sync_compact(sample_request())
            .await
            .expect_err("timeout must fail");
        assert!(err.to_string().contains("request failed"));

        let _ = shutdown.send(());
    }
}
