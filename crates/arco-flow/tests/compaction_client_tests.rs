//! Remote orchestration compaction client tests.

#![allow(clippy::expect_used)]

use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;

use arco_flow::compaction_client::compact_orchestration_events;

#[derive(Clone, Copy)]
enum ServerMode {
    RequireAuthHeader,
    DelayResponse { millis: u64 },
}

#[derive(Clone)]
struct ServerState {
    mode: ServerMode,
}

#[derive(Debug, Deserialize)]
struct CompactRequest {
    event_paths: Vec<String>,
}

async fn compact_handler(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(req): Json<CompactRequest>,
) -> StatusCode {
    if req.event_paths.is_empty() {
        return StatusCode::BAD_REQUEST;
    }

    match state.mode {
        ServerMode::RequireAuthHeader => {
            let auth = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok());

            if auth != Some("Bearer test-token") {
                return StatusCode::UNAUTHORIZED;
            }
        }
        ServerMode::DelayResponse { millis } => {
            tokio::time::sleep(Duration::from_millis(millis)).await;
        }
    }

    StatusCode::OK
}

async fn start_test_server(mode: ServerMode) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/compact", post(compact_handler))
        .with_state(ServerState { mode });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr: SocketAddr = listener.local_addr().expect("listener addr");
    let base_url = format!("http://{addr}");

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test server");
    });

    (base_url, handle)
}

#[tokio::test]
async fn compaction_client_sends_static_bearer_token_when_configured() {
    let (base_url, _handle) = start_test_server(ServerMode::RequireAuthHeader).await;
    // Encode the bearer token in the URL userinfo (user=bearer, pass=<token>).
    // The compaction client should treat this as a configuration hint and
    // translate it into a Bearer auth header (and strip userinfo from the URL).
    let authed_url = format!(
        "http://bearer:test-token@{}",
        base_url.trim_start_matches("http://")
    );

    let result =
        compact_orchestration_events(&authed_url, vec!["ledger/events/1.json".to_string()]).await;

    #[cfg(any(feature = "gcp", feature = "test-utils"))]
    assert!(
        result.is_ok(),
        "expected compaction request to succeed with auth header, got: {result:?}"
    );

    #[cfg(not(any(feature = "gcp", feature = "test-utils")))]
    {
        let err = result.expect_err("expected configuration error without gcp/test-utils feature");
        assert!(
            err.to_string()
                .contains("orchestration compaction requires the 'gcp' feature"),
            "unexpected error when gcp/test-utils feature is disabled: {err}"
        );
    }
}

#[tokio::test]
async fn compaction_client_times_out_requests() {
    // Delay long enough that a reasonable client-side timeout should trigger.
    let (base_url, _handle) = start_test_server(ServerMode::DelayResponse { millis: 10_000 }).await;

    // Wrap in an outer timeout to avoid hanging if the client has no timeout.
    // The test expects the *client* to time out first (i.e., we get Ok(Err(..))).
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        compact_orchestration_events(&base_url, vec!["ledger/events/2.json".to_string()]),
    )
    .await;

    match result {
        Ok(inner) => assert!(
            inner.is_err(),
            "expected compaction client to error on timeout, got: {inner:?}"
        ),
        Err(elapsed) => panic!(
            "outer tokio timeout elapsed; compaction client likely has no request timeout: {elapsed}"
        ),
    }
}
