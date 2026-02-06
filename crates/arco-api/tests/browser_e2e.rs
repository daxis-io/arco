//! E2E Browser Read and Signed URL Security Tests.
//!
//! Tests the complete browser read path:
//! 1. Create namespace and register table (via API)
//! 2. Mint signed URLs for catalog domain
//! 3. Verify URLs are returned with correct structure
//!
//! Also tests signed URL security invariants:
//! - Path traversal rejection
//! - Non-allowlist path rejection
//! - TTL bounding
//! - Domain validation

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

use arco_api::config::{Config, CorsConfig};
use arco_api::server::Server;
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_test_utils::http_signed_url::HttpSignedUrlBackend;
use reqwest::Url;

// ============================================================================
// Test Config Helper
// ============================================================================

fn test_config() -> Config {
    Config {
        http_port: 0,
        grpc_port: 0,
        debug: true, // Enable header-based auth for tests
        cors: CorsConfig {
            allowed_origins: vec!["*".to_string()], // Enable CORS for tests
            max_age_seconds: 3600,
        },
        ..Config::default()
    }
}

fn env_truthy(key: &str) -> bool {
    matches!(
        std::env::var(key),
        Ok(value)
            if value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("yes")
    )
}

fn require_http_signed_url_e2e() -> bool {
    env_truthy("ARCO_REQUIRE_HTTP_SIGNED_URL_E2E") || env_truthy("CI")
}

fn allow_http_bind_fallback() -> bool {
    env_truthy("ARCO_ALLOW_HTTP_BIND_FALLBACK")
}

fn is_listener_bind_restricted_error(err: &arco_core::Error) -> bool {
    let arco_core::Error::Storage { message, .. } = err else {
        return false;
    };

    if !message.starts_with("failed to bind http signed-url listener") {
        return false;
    }

    // The underlying bind error is formatted into `message` by HttpSignedUrlBackend.
    // We accept a small set of known strings/codes that indicate sandbox restrictions.
    //
    // Examples:
    // - Linux: "Permission denied (os error 13)"
    // - macOS: "Operation not permitted" or "Cannot assign requested address (os error 49)"
    message.contains("Permission denied")
        || message.contains("Operation not permitted")
        || message.contains("Cannot assign requested address")
        || message.contains("Address not available")
        || message.contains("os error 13")
        || message.contains("os error 49")
}

fn assert_signed_url_shape(
    url: &str,
    expected_object_path: &str,
    supports_http: bool,
) -> Result<()> {
    let parsed = Url::parse(url).context("parse signed url")?;
    assert_eq!(parsed.scheme(), "http", "signed url must use http");

    let url_path = parsed.path();
    let object_path = url_path
        .strip_prefix("/objects/")
        .with_context(|| format!("signed url path must start with /objects/: {url_path}"))?;
    // The server-side minting path may include tenant/workspace prefixes.
    assert!(
        object_path.ends_with(expected_object_path),
        "signed url object path must end with '{expected_object_path}', got '{object_path}'"
    );

    let query: std::collections::BTreeMap<String, String> = parsed
        .query_pairs()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    assert!(query.contains_key("expires"), "signed url missing expires");
    assert!(query.contains_key("sig"), "signed url missing sig");

    if supports_http {
        let host = parsed.host_str().unwrap_or_default();
        assert!(
            host == "127.0.0.1" || host == "localhost" || host == "::1",
            "expected signed url host to be loopback, got '{host}'"
        );
    } else {
        assert_eq!(
            parsed.host_str().unwrap_or_default(),
            "signed-url.invalid",
            "expected stub backend host"
        );
    }

    Ok(())
}

struct StubSignedUrlBackend {
    inner: Arc<dyn StorageBackend>,
}

impl StubSignedUrlBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl StorageBackend for StubSignedUrlBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .saturating_add(expiry)
            .as_secs();

        Ok(format!(
            "http://signed-url.invalid/objects/{path}?expires={expires}&sig=stub"
        ))
    }
}

async fn test_backend(
    inner: Arc<MemoryBackend>,
) -> Result<(Arc<dyn StorageBackend>, /* supports_http */ bool)> {
    match HttpSignedUrlBackend::new(inner.clone()).await {
        Ok(backend) => Ok((Arc::new(backend), true)),
        Err(err) => {
            // Some sandboxed environments disallow binding local TCP listeners. If we detect that
            // case we can fall back to a stub backend that still implements `signed_url()`.
            //
            // In CI, we generally want to *fail* instead of silently losing coverage.
            let require = require_http_signed_url_e2e();
            let allow_fallback = allow_http_bind_fallback();

            if is_listener_bind_restricted_error(&err) {
                if require && !allow_fallback {
                    return Err(anyhow::Error::new(err)).context(
                        "http signed-url backend is required (CI or ARCO_REQUIRE_HTTP_SIGNED_URL_E2E=1); \
set ARCO_ALLOW_HTTP_BIND_FALLBACK=1 to allow stub fallback",
                    );
                }

                let inner: Arc<dyn StorageBackend> = inner;
                Ok((Arc::new(StubSignedUrlBackend::new(inner)), false))
            } else {
                Err(anyhow::Error::new(err)).context("initialize http signed-url backend")
            }
        }
    }
}

async fn test_router() -> Result<axum::Router> {
    let inner = Arc::new(MemoryBackend::new());
    let (backend, _supports_http) = test_backend(inner).await?;
    Ok(Server::with_storage_backend(test_config(), backend).test_router())
}

async fn test_router_with_storage() -> Result<(axum::Router, Arc<MemoryBackend>, bool)> {
    let inner = Arc::new(MemoryBackend::new());
    let (backend, supports_http) = test_backend(inner.clone()).await?;
    let router = Server::with_storage_backend(test_config(), backend).test_router();
    Ok((router, inner, supports_http))
}

// ============================================================================
// Test Request Types
// ============================================================================

#[derive(Debug, Serialize)]
struct CreateNamespaceRequest {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[derive(Debug, Serialize)]
struct RegisterTableRequest {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    columns: Vec<ColumnDef>,
}

#[derive(Debug, Serialize)]
struct ColumnDef {
    name: String,
    data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    nullable: Option<bool>,
}

#[derive(Debug, Serialize)]
struct MintUrlsRequest {
    domain: String,
    paths: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ttl_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct MintUrlsResponse {
    urls: Vec<SignedUrl>,
    ttl_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct SignedUrl {
    path: String,
    url: String,
}

/// API error response (flat structure).
#[derive(Debug, Deserialize)]
struct ApiErrorResponse {
    code: String,
    message: String,
}

// ============================================================================
// Helper Functions
// ============================================================================

async fn post_json(
    router: &axum::Router,
    uri: &str,
    body: impl Serialize,
) -> Result<axum::response::Response> {
    let body_bytes = serde_json::to_vec(&body).context("serialize body")?;

    let request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("X-Tenant-Id", "test-tenant")
        .header("X-Workspace-Id", "test-workspace")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body_bytes))
        .context("build request")?;

    let response = router
        .clone()
        .oneshot(request)
        .await
        .map_err(|err| match err {})?;
    Ok(response)
}

async fn response_json<T: serde::de::DeserializeOwned>(
    response: axum::response::Response,
) -> Result<T> {
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .context("read body")?;
    serde_json::from_slice(&body)
        .with_context(|| format!("deserialize JSON: {}", String::from_utf8_lossy(&body)))
}

fn ensure_httpfs_loaded(conn: &duckdb::Connection) -> Result<()> {
    if conn.execute("LOAD httpfs", []).is_err() {
        conn.execute("INSTALL httpfs", [])
            .context("install DuckDB httpfs extension")?;
        conn.execute("LOAD httpfs", [])
            .context("load DuckDB httpfs extension")?;
    }
    Ok(())
}

// ============================================================================
// E2E Browser Read Tests (Task 5.2)
// ============================================================================

mod e2e_browser_read {
    use super::*;

    #[tokio::test]
    async fn test_browser_read_full_lifecycle() -> Result<()> {
        // This test verifies the complete browser read path:
        // 1. Initialize catalog by creating a namespace
        // 2. Register a table with columns
        // 3. Mint signed URLs for the catalog domain
        // 4. Verify URLs are returned with correct structure

        use arco_catalog::CatalogReader;
        use arco_core::CatalogDomain;
        use arco_core::ScopedStorage;
        let (router, inner, supports_http) = test_router_with_storage().await?;

        // Step 1: Create namespace (initializes catalog + writes snapshot v2)
        let create_ns = CreateNamespaceRequest {
            name: "test_ns".to_string(),
            description: Some("Test namespace".to_string()),
        };
        let response = post_json(&router, "/api/v1/namespaces", &create_ns).await?;
        assert_eq!(response.status(), StatusCode::CREATED);

        // Step 2: Register a table (writes snapshot v3)
        let register_table = RegisterTableRequest {
            name: "test_table".to_string(),
            description: Some("Test table".to_string()),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: "STRING".to_string(),
                    nullable: Some(false),
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: "STRING".to_string(),
                    nullable: Some(true),
                },
            ],
        };
        let response = post_json(
            &router,
            "/api/v1/namespaces/test_ns/tables",
            &register_table,
        )
        .await?;
        assert_eq!(response.status(), StatusCode::CREATED);

        // Determine mintable paths from the manifest allowlist.
        let storage = ScopedStorage::new(inner, "test-tenant", "test-workspace")?;
        let reader = CatalogReader::new(storage);
        let mintable = reader.get_mintable_paths(CatalogDomain::Catalog).await?;

        let namespaces_path = mintable
            .iter()
            .find(|p| p.ends_with("/namespaces.parquet"))
            .cloned()
            .context("namespaces.parquet not mintable")?;

        // Step 3: Request signed URLs for the catalog domain
        let mint_req = MintUrlsRequest {
            domain: "catalog".to_string(),
            paths: vec![namespaces_path.clone()],
            ttl_seconds: Some(300),
        };
        let response = post_json(&router, "/api/v1/browser/urls", &mint_req).await?;
        assert_eq!(response.status(), StatusCode::OK);

        let mint_response: MintUrlsResponse = response_json(response).await?;

        // Verify response structure
        assert_eq!(mint_response.urls.len(), 1);
        assert_eq!(mint_response.ttl_seconds, 300);

        let signed = mint_response
            .urls
            .into_iter()
            .next()
            .context("missing signed url")?;
        assert_eq!(signed.path, namespaces_path);
        assert!(
            signed.url.starts_with("http://"),
            "expected http signed url"
        );

        assert_signed_url_shape(&signed.url, &namespaces_path, supports_http)?;

        // Query Parquet bytes via signed URL using DuckDB (browser read analogue).
        if !supports_http {
            println!(
                "Skipping DuckDB browser read (no local HTTP listener); \
set ARCO_REQUIRE_HTTP_SIGNED_URL_E2E=1 to fail instead"
            );
            return Ok(());
        }

        let signed_url = signed.url.clone();
        let count = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::task::spawn_blocking(move || {
                let conn = duckdb::Connection::open_in_memory().context("open duckdb")?;
                ensure_httpfs_loaded(&conn)?;
                let mut stmt = conn
                    .prepare("SELECT count(*) FROM read_parquet(?) WHERE name = ?")
                    .context("prepare query")?;
                let count: i64 = stmt
                    .query_row([signed_url.as_str(), "test_ns"], |row| row.get(0))
                    .context("query namespaces parquet")?;
                Ok::<i64, anyhow::Error>(count)
            }),
        )
        .await
        .context("duckdb query timed out")?
        .context("duckdb task join")??;

        assert_eq!(count, 1, "expected namespace row to be queryable");

        Ok(())
    }

    #[tokio::test]
    async fn test_browser_read_empty_catalog() -> Result<()> {
        let router = test_router().await?;

        // Catalog isn't initialized (no manifests), so minting should fail.
        let mint_req = MintUrlsRequest {
            domain: "catalog".to_string(),
            paths: vec!["snapshots/catalog/v1/namespaces.parquet".to_string()],
            ttl_seconds: None,
        };
        let response = post_json(&router, "/api/v1/browser/urls", &mint_req).await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn test_browser_read_different_domains() -> Result<()> {
        let router = test_router().await?;

        // Initialize catalog
        let create_ns = CreateNamespaceRequest {
            name: "domain_test_ns".to_string(),
            description: None,
        };
        let response = post_json(&router, "/api/v1/namespaces", &create_ns).await?;
        assert_eq!(response.status(), StatusCode::CREATED);

        // Test each valid domain
        for domain in ["catalog", "lineage", "executions", "search"] {
            let mint_req = MintUrlsRequest {
                domain: domain.to_string(),
                paths: vec![], // Empty paths should be OK (nothing to mint)
                ttl_seconds: None,
            };
            let response = post_json(&router, "/api/v1/browser/urls", &mint_req).await?;
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "domain {domain} should be valid"
            );
        }

        Ok(())
    }
}

// ============================================================================
// Signed URL Security Tests (Task 5.3)
// ============================================================================

mod signed_url_security {
    use super::*;

    #[tokio::test]
    async fn test_path_traversal_rejected() -> Result<()> {
        let router = test_router().await?;

        // Path traversal should be rejected BEFORE any I/O
        let mint_req = MintUrlsRequest {
            domain: "catalog".to_string(),
            paths: vec!["../../../etc/passwd".to_string()],
            ttl_seconds: None,
        };
        let response = post_json(&router, "/api/v1/browser/urls", &mint_req).await?;

        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let error: ApiErrorResponse = response_json(response).await?;
        assert_eq!(error.code, "FORBIDDEN");
        assert!(
            error.message.contains("Path traversal"),
            "unexpected error message: {}",
            error.message
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_path_traversal_various_patterns() -> Result<()> {
        let router = test_router().await?;

        let traversal_patterns = vec![
            "../secret",
            "foo/../../../bar",
            "..%2F..%2Fetc",
            "snapshots/catalog/v1/../../ledger/events.json",
        ];

        for pattern in traversal_patterns {
            let mint_req = MintUrlsRequest {
                domain: "catalog".to_string(),
                paths: vec![pattern.to_string()],
                ttl_seconds: None,
            };
            let response = post_json(&router, "/api/v1/browser/urls", &mint_req).await?;
            assert_eq!(
                response.status(),
                StatusCode::FORBIDDEN,
                "pattern '{pattern}' should be rejected"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_unknown_domain_rejected() -> Result<()> {
        let router = test_router().await?;

        let mint_req = MintUrlsRequest {
            domain: "invalid_domain".to_string(),
            paths: vec!["some/path.parquet".to_string()],
            ttl_seconds: None,
        };
        let response = post_json(&router, "/api/v1/browser/urls", &mint_req).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let error: ApiErrorResponse = response_json(response).await?;
        assert_eq!(error.code, "BAD_REQUEST");
        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_bounded_to_maximum() -> Result<()> {
        use arco_catalog::CatalogReader;
        use arco_core::{CatalogDomain, ScopedStorage};

        let (router, inner) = test_router_with_storage().await?;

        // Initialize catalog first.
        let create_ns = CreateNamespaceRequest {
            name: "ttl_test_ns".to_string(),
            description: None,
        };
        let response = post_json(&router, "/api/v1/namespaces", &create_ns).await?;
        assert_eq!(response.status(), StatusCode::CREATED);

        let storage = ScopedStorage::new(inner, "test-tenant", "test-workspace")?;
        let reader = CatalogReader::new(storage);
        let mintable = reader.get_mintable_paths(CatalogDomain::Catalog).await?;
        let namespaces_path = mintable
            .iter()
            .find(|p| p.ends_with("/namespaces.parquet"))
            .cloned()
            .context("namespaces.parquet not mintable")?;

        // Request with excessive TTL (2 hours = 7200 seconds)
        let mint_req = MintUrlsRequest {
            domain: "catalog".to_string(),
            paths: vec![namespaces_path],
            ttl_seconds: Some(7200), // Exceeds MAX_TTL_SECONDS (3600)
        };
        let response = post_json(&router, "/api/v1/browser/urls", &mint_req).await?;
        assert_eq!(response.status(), StatusCode::OK);

        let mint_response: MintUrlsResponse = response_json(response).await?;
        assert_eq!(mint_response.ttl_seconds, 3600);
        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_default_applied() -> Result<()> {
        use arco_catalog::CatalogReader;
        use arco_core::{CatalogDomain, ScopedStorage};

        let (router, inner) = test_router_with_storage().await?;

        // Initialize catalog.
        let create_ns = CreateNamespaceRequest {
            name: "default_ttl_ns".to_string(),
            description: None,
        };
        let response = post_json(&router, "/api/v1/namespaces", &create_ns).await?;
        assert_eq!(response.status(), StatusCode::CREATED);

        let storage = ScopedStorage::new(inner, "test-tenant", "test-workspace")?;
        let reader = CatalogReader::new(storage);
        let mintable = reader.get_mintable_paths(CatalogDomain::Catalog).await?;
        let namespaces_path = mintable
            .iter()
            .find(|p| p.ends_with("/namespaces.parquet"))
            .cloned()
            .context("namespaces.parquet not mintable")?;

        // Request without TTL
        let mint_req = MintUrlsRequest {
            domain: "catalog".to_string(),
            paths: vec![namespaces_path],
            ttl_seconds: None,
        };
        let response = post_json(&router, "/api/v1/browser/urls", &mint_req).await?;
        assert_eq!(response.status(), StatusCode::OK);

        let mint_response: MintUrlsResponse = response_json(response).await?;
        assert_eq!(mint_response.ttl_seconds, 900);
        Ok(())
    }

    #[tokio::test]
    async fn test_non_allowlist_path_rejected() -> Result<()> {
        let router = test_router().await?;

        // Initialize catalog
        let create_ns = CreateNamespaceRequest {
            name: "allowlist_test_ns".to_string(),
            description: None,
        };
        let response = post_json(&router, "/api/v1/namespaces", &create_ns).await?;
        assert_eq!(response.status(), StatusCode::CREATED);

        // Request a path that's not in the manifest allowlist
        let mint_req = MintUrlsRequest {
            domain: "catalog".to_string(),
            paths: vec!["ledger/catalog/event-001.json".to_string()], // Ledger paths are never mintable
            ttl_seconds: None,
        };
        let response = post_json(&router, "/api/v1/browser/urls", &mint_req).await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let error: ApiErrorResponse = response_json(response).await?;
        assert_eq!(error.code, "FORBIDDEN");
        assert!(
            error.message.contains("manifest allowlist"),
            "unexpected error message: {}",
            error.message
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_manifest_paths_not_mintable() -> Result<()> {
        let router = test_router().await?;

        // Initialize catalog
        let create_ns = CreateNamespaceRequest {
            name: "manifest_test_ns".to_string(),
            description: None,
        };
        let response = post_json(&router, "/api/v1/namespaces", &create_ns).await?;
        assert_eq!(response.status(), StatusCode::CREATED);

        // Try to mint URL for manifest file (security violation)
        let mint_req = MintUrlsRequest {
            domain: "catalog".to_string(),
            paths: vec!["manifests/root.manifest.json".to_string()],
            ttl_seconds: None,
        };
        let response = post_json(&router, "/api/v1/browser/urls", &mint_req).await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        Ok(())
    }

    #[tokio::test]
    async fn test_cross_tenant_paths_not_mintable() -> Result<()> {
        let router = test_router().await?;

        // Initialize catalog so we have a manifest allowlist.
        let create_ns = CreateNamespaceRequest {
            name: "cross_tenant_test_ns".to_string(),
            description: None,
        };
        let response = post_json(&router, "/api/v1/namespaces", &create_ns).await?;
        assert_eq!(response.status(), StatusCode::CREATED);

        // Attempt to mint a scope-prefixed path (should never be allowed).
        let mint_req = MintUrlsRequest {
            domain: "catalog".to_string(),
            paths: vec![
                "tenant=other/workspace=other/snapshots/catalog/v1/namespaces.parquet".to_string(),
            ],
            ttl_seconds: None,
        };
        let response = post_json(&router, "/api/v1/browser/urls", &mint_req).await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        Ok(())
    }

    #[tokio::test]
    async fn test_missing_tenant_header_rejected() -> Result<()> {
        let router = test_router().await?;

        let mint_req = MintUrlsRequest {
            domain: "catalog".to_string(),
            paths: vec![],
            ttl_seconds: None,
        };
        let body_bytes = serde_json::to_vec(&mint_req).context("serialize")?;

        // Request without X-Tenant-Id header
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/browser/urls")
            .header("X-Workspace-Id", "test-workspace")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body_bytes))
            .context("build request")?;

        let response = router
            .clone()
            .oneshot(request)
            .await
            .map_err(|err| match err {})?;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        Ok(())
    }

    #[tokio::test]
    async fn test_missing_workspace_header_rejected() -> Result<()> {
        let router = test_router().await?;

        let mint_req = MintUrlsRequest {
            domain: "catalog".to_string(),
            paths: vec![],
            ttl_seconds: None,
        };
        let body_bytes = serde_json::to_vec(&mint_req).context("serialize")?;

        // Request without X-Workspace-Id header
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/browser/urls")
            .header("X-Tenant-Id", "test-tenant")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body_bytes))
            .context("build request")?;

        let response = router
            .clone()
            .oneshot(request)
            .await
            .map_err(|err| match err {})?;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        Ok(())
    }
}

// ============================================================================
// CORS Tests (Browser Access)
// ============================================================================

mod cors {
    use super::*;

    #[tokio::test]
    async fn test_cors_preflight() -> Result<()> {
        let router = test_router().await?;

        let request = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/v1/browser/urls")
            .header("Origin", "http://localhost:3000")
            .header("Access-Control-Request-Method", "POST")
            .header(
                "Access-Control-Request-Headers",
                "content-type,x-tenant-id,x-workspace-id",
            )
            .body(Body::empty())
            .context("build request")?;

        let response = router
            .clone()
            .oneshot(request)
            .await
            .map_err(|err| match err {})?;

        assert!(
            response.status().is_success() || response.status() == StatusCode::NO_CONTENT,
            "preflight should succeed: {}",
            response.status()
        );

        let headers = response.headers();
        assert!(headers.contains_key("access-control-allow-origin"));
        assert!(headers.contains_key("access-control-allow-methods"));
        Ok(())
    }

    #[tokio::test]
    async fn test_cors_headers_on_response() -> Result<()> {
        let router = test_router().await?;

        // Initialize catalog
        let create_ns = CreateNamespaceRequest {
            name: "cors_test_ns".to_string(),
            description: None,
        };

        let body_bytes = serde_json::to_vec(&create_ns).context("serialize")?;

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/namespaces")
            .header("Origin", "http://localhost:3000")
            .header("X-Tenant-Id", "test-tenant")
            .header("X-Workspace-Id", "test-workspace")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body_bytes))
            .context("build request")?;

        let response = router
            .clone()
            .oneshot(request)
            .await
            .map_err(|err| match err {})?;

        assert!(
            response
                .headers()
                .contains_key("access-control-allow-origin")
        );
        Ok(())
    }
}
