//! HTTP signed URL test backend.
//!
//! Production deployments should use cloud-native signed URLs (GCS/S3).
//! For integration tests, we need URLs that are:
//! - Real `http://` URLs (DuckDB-WASM / `DuckDB` engines can fetch them)
//! - Time-bounded
//! - Protected by a signature (bearer-token semantics)
//! - Range-capable (`Range: bytes=...`) for Parquet readers

use std::net::SocketAddr;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{Error, Result};
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::sync::oneshot;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
struct ServerState {
    inner: Arc<dyn StorageBackend>,
    secret: [u8; 32],
}

#[derive(Debug, serde::Deserialize)]
struct SignedQuery {
    expires: u64,
    sig: String,
}

/// Storage backend wrapper that serves objects over HTTP via signed URLs.
pub struct HttpSignedUrlBackend {
    inner: Arc<dyn StorageBackend>,
    base_url: String,
    secret: [u8; 32],
    shutdown_tx: Option<oneshot::Sender<()>>,
    _task: tokio::task::JoinHandle<()>,
}

impl std::fmt::Debug for HttpSignedUrlBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSignedUrlBackend")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl HttpSignedUrlBackend {
    /// Wraps an existing backend and starts an HTTP server on `127.0.0.1:0`.
    ///
    /// The returned backend implements `signed_url()` as a real HTTP URL, and the
    /// server supports GET/HEAD with Range requests for Parquet readers.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP listener cannot be bound or the local address cannot be read.
    pub async fn new(inner: Arc<dyn StorageBackend>) -> Result<Self> {
        let secret = new_secret();

        let state = ServerState {
            inner: inner.clone(),
            secret,
        };

        let app = Router::new()
            .route("/objects/*path", get(get_object).head(head_object))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| Error::Storage {
                message: format!("failed to bind http signed-url listener: {e}"),
                source: None,
            })?;

        let addr: SocketAddr = listener.local_addr().map_err(|e| Error::Internal {
            message: format!("failed to read listener addr: {e}"),
        })?;

        let base_url = format!("http://{addr}");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let task = tokio::spawn(async move {
            let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            });
            let _ = server.await;
        });

        Ok(Self {
            inner,
            base_url,
            secret,
            shutdown_tx: Some(shutdown_tx),
            _task: task,
        })
    }

    /// Returns the server base URL (e.g., `http://127.0.0.1:12345`).
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn sign(&self, path: &str, expires: u64) -> Result<String> {
        let mut mac = HmacSha256::new_from_slice(&self.secret).map_err(|_| Error::Internal {
            message: "failed to initialize hmac".to_string(),
        })?;
        mac.update(path.as_bytes());
        mac.update(b"\n");
        mac.update(expires.to_string().as_bytes());
        let bytes = mac.finalize().into_bytes();
        Ok(URL_SAFE_NO_PAD.encode(bytes))
    }

    fn signed_url_for(&self, path: &str, expiry: Duration) -> Result<String> {
        let expires = unix_ts_seconds().saturating_add(expiry.as_secs());
        let sig = self.sign(path, expires)?;
        Ok(format!(
            "{}/objects/{}?expires={expires}&sig={sig}",
            self.base_url, path
        ))
    }
}

impl Drop for HttpSignedUrlBackend {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // JoinHandle is aborted on drop; best-effort shutdown above.
    }
}

#[async_trait::async_trait]
impl StorageBackend for HttpSignedUrlBackend {
    async fn get(&self, path: &str) -> Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        self.signed_url_for(path, expiry)
    }
}

async fn head_object(
    State(state): State<ServerState>,
    Path(path): Path<String>,
    Query(query): Query<SignedQuery>,
) -> impl IntoResponse {
    if let Err(resp) = verify(&state, &path, &query) {
        return resp;
    }

    match state.inner.head(&path).await {
        Ok(Some(meta)) => {
            let mut headers = HeaderMap::new();
            set_common_headers(&mut headers, meta.size);
            (StatusCode::OK, headers, Bytes::new()).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn get_object(
    State(state): State<ServerState>,
    Path(path): Path<String>,
    Query(query): Query<SignedQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = verify(&state, &path, &query) {
        return resp;
    }

    let meta = match state.inner.head(&path).await {
        Ok(Some(meta)) => meta,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let range = match parse_range(headers.get(axum::http::header::RANGE), meta.size) {
        Ok(r) => r,
        Err(status) => return status.into_response(),
    };

    if let Some((start, end_inclusive)) = range {
        let end_exclusive = end_inclusive.saturating_add(1);
        let Ok(bytes) = state.inner.get_range(&path, start..end_exclusive).await else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };

        let mut out_headers = HeaderMap::new();
        set_common_headers(&mut out_headers, meta.size);
        let content_range = format!("bytes {start}-{end_inclusive}/{}", meta.size);
        let _ = out_headers.insert(
            axum::http::header::CONTENT_RANGE,
            HeaderValue::from_str(&content_range).unwrap_or(HeaderValue::from_static("bytes */*")),
        );
        let _ = out_headers.insert(
            axum::http::header::CONTENT_LENGTH,
            HeaderValue::from_str(&bytes.len().to_string())
                .unwrap_or(HeaderValue::from_static("0")),
        );

        (StatusCode::PARTIAL_CONTENT, out_headers, bytes).into_response()
    } else {
        let Ok(bytes) = state.inner.get(&path).await else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };

        let mut out_headers = HeaderMap::new();
        set_common_headers(&mut out_headers, meta.size);
        (StatusCode::OK, out_headers, bytes).into_response()
    }
}

fn set_common_headers(headers: &mut HeaderMap, size: u64) {
    let _ = headers.insert(
        axum::http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );
    let _ = headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    let _ = headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&size.to_string()).unwrap_or(HeaderValue::from_static("0")),
    );
}

fn parse_range(
    range: Option<&HeaderValue>,
    size: u64,
) -> std::result::Result<Option<(u64, u64)>, StatusCode> {
    let Some(value) = range else {
        return Ok(None);
    };

    let Ok(s) = value.to_str() else {
        return Err(StatusCode::BAD_REQUEST);
    };

    let Some(spec) = s.strip_prefix("bytes=") else {
        return Err(StatusCode::BAD_REQUEST);
    };

    let (start_s, end_s) = spec.split_once('-').ok_or(StatusCode::BAD_REQUEST)?;

    // bytes=start-end
    if !start_s.is_empty() {
        let start: u64 = start_s.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
        let end_inclusive: u64 = if end_s.is_empty() {
            size.saturating_sub(1)
        } else {
            end_s.parse().map_err(|_| StatusCode::BAD_REQUEST)?
        };

        if start >= size {
            return Err(StatusCode::RANGE_NOT_SATISFIABLE);
        }
        let end_inclusive = end_inclusive.min(size.saturating_sub(1));
        if end_inclusive < start {
            return Err(StatusCode::RANGE_NOT_SATISFIABLE);
        }

        return Ok(Some((start, end_inclusive)));
    }

    // bytes=-suffix
    if end_s.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let suffix: u64 = end_s.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    if suffix == 0 {
        return Err(StatusCode::RANGE_NOT_SATISFIABLE);
    }

    let suffix = suffix.min(size);
    let start = size.saturating_sub(suffix);
    Ok(Some((start, size.saturating_sub(1))))
}

#[allow(clippy::result_large_err)]
fn verify(
    state: &ServerState,
    path: &str,
    query: &SignedQuery,
) -> std::result::Result<(), axum::response::Response> {
    // Reject obviously invalid paths (defense-in-depth; object keys are not filesystem paths).
    if path.contains("..") {
        return Err(StatusCode::FORBIDDEN.into_response());
    }

    let now = unix_ts_seconds();
    if query.expires <= now {
        return Err(StatusCode::FORBIDDEN.into_response());
    }

    let expected = {
        let Ok(mut mac) = HmacSha256::new_from_slice(&state.secret) else {
            return Err(StatusCode::INTERNAL_SERVER_ERROR.into_response());
        };
        mac.update(path.as_bytes());
        mac.update(b"\n");
        mac.update(query.expires.to_string().as_bytes());
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    };

    if query.sig != expected {
        return Err(StatusCode::FORBIDDEN.into_response());
    }

    Ok(())
}

fn unix_ts_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}

fn new_secret() -> [u8; 32] {
    let a = Uuid::new_v4().as_bytes().to_owned();
    let b = Uuid::new_v4().as_bytes().to_owned();
    let mut out = [0_u8; 32];
    out[..16].copy_from_slice(&a);
    out[16..].copy_from_slice(&b);
    out
}
