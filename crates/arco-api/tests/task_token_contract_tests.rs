//! Dedicated task-token contract tests.

use std::sync::Arc;

use anyhow::Result;
use axum::Json;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use tower::ServiceExt;

use arco_api::config::{Config, JwtConfig, Posture, TaskTokenConfig};
use arco_api::context::RequestContext;
use arco_api::routes::tasks::task_auth_middleware;
use arco_api::server::AppState;

fn make_state() -> Arc<AppState> {
    let config = Config {
        debug: false,
        posture: Posture::Private,
        jwt: JwtConfig {
            hs256_secret: Some("user-jwt-secret".to_string()),
            issuer: Some("https://issuer.user".to_string()),
            audience: Some("arco-api-user".to_string()),
            ..JwtConfig::default()
        },
        task_token: TaskTokenConfig {
            hs256_secret: "task-secret".to_string(),
            issuer: Some("https://issuer.task".to_string()),
            audience: Some("arco-worker-callback".to_string()),
            ttl_seconds: 900,
        },
        ..Config::default()
    };

    Arc::new(AppState::with_memory_storage(config))
}

fn token_with_claims(tenant: &str, workspace: &str, task_id: &str, exp: usize) -> String {
    let claims = serde_json::json!({
        "taskId": task_id,
        "tenantId": tenant,
        "workspaceId": workspace,
        "iss": "https://issuer.task",
        "aud": "arco-worker-callback",
        "exp": exp
    });

    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret("task-secret".as_bytes()),
    )
    .expect("token")
}

#[tokio::test]
async fn task_auth_middleware_accepts_task_token_config() -> Result<()> {
    let state = make_state();

    let app = axum::Router::new()
        .route(
            "/api/v1/tasks/task-123/started",
            get(|ctx: RequestContext| async move {
                Json(serde_json::json!({
                    "tenant": ctx.tenant,
                    "workspace": ctx.workspace,
                }))
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            task_auth_middleware,
        ))
        .with_state(state);

    let token = token_with_claims("tenant-1", "workspace-1", "task-123", 2_000_000_000);

    let request = Request::builder()
        .uri("/api/v1/tasks/task-123/started")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024).await?;
    let payload: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(payload["tenant"], "tenant-1");
    assert_eq!(payload["workspace"], "workspace-1");

    Ok(())
}

#[tokio::test]
async fn task_auth_middleware_rejects_expired_task_token() -> Result<()> {
    let state = make_state();

    let app = axum::Router::new()
        .route(
            "/api/v1/tasks/task-123/started",
            get(|_ctx: RequestContext| async move { StatusCode::OK }),
        )
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            task_auth_middleware,
        ))
        .with_state(state);

    let token = token_with_claims("tenant-1", "workspace-1", "task-123", 1_700_000_000);

    let request = Request::builder()
        .uri("/api/v1/tasks/task-123/started")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    Ok(())
}

#[tokio::test]
async fn task_auth_middleware_started_then_expired_completed_path() -> Result<()> {
    let state = make_state();

    let app = axum::Router::new()
        .route(
            "/api/v1/tasks/task-123/started",
            get(|_ctx: RequestContext| async move { StatusCode::OK }),
        )
        .route(
            "/api/v1/tasks/task-123/completed",
            get(|_ctx: RequestContext| async move { StatusCode::OK }),
        )
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            task_auth_middleware,
        ))
        .with_state(state);

    // jsonwebtoken's default leeway is 60s. Use a token that is just inside
    // leeway for the first callback, then outside leeway for the second.
    let now = chrono::Utc::now().timestamp() as usize;
    let near_expiry_token = token_with_claims("tenant-1", "workspace-1", "task-123", now - 58);

    let started_request = Request::builder()
        .uri("/api/v1/tasks/task-123/started")
        .header("authorization", format!("Bearer {near_expiry_token}"))
        .body(Body::empty())?;
    let started_response = app.clone().oneshot(started_request).await?;
    assert_eq!(started_response.status(), StatusCode::OK);

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let completed_request = Request::builder()
        .uri("/api/v1/tasks/task-123/completed")
        .header("authorization", format!("Bearer {near_expiry_token}"))
        .body(Body::empty())?;
    let completed_response = app.oneshot(completed_request).await?;
    assert_eq!(completed_response.status(), StatusCode::UNAUTHORIZED);

    Ok(())
}

#[test]
fn config_deserializes_task_token_block() -> Result<()> {
    let json = serde_json::json!({
        "http_port": 8080,
        "grpc_port": 9090,
        "debug": false,
        "posture": "private",
        "task_token": {
            "hs256Secret": "env-task-secret",
            "issuer": "https://issuer.task",
            "audience": "arco-worker-callback",
            "ttlSeconds": 1200
        }
    });

    let config: Config = serde_json::from_value(json)?;

    assert_eq!(config.task_token.hs256_secret, "env-task-secret");
    assert_eq!(
        config.task_token.issuer.as_deref(),
        Some("https://issuer.task")
    );
    assert_eq!(
        config.task_token.audience.as_deref(),
        Some("arco-worker-callback")
    );
    assert_eq!(config.task_token.ttl_seconds, 1200);

    Ok(())
}
