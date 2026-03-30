//! Remote orchestration compaction client tests.

#![allow(clippy::expect_used)]

use std::net::SocketAddr;
use std::time::Duration;

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};

use arco_core::VisibilityStatus;
use arco_flow::compaction_client::{
    compact_orchestration_events, compact_orchestration_events_fenced,
};
use arco_flow::orchestration_compaction::OrchestrationCompactRequest;

#[derive(Clone, Copy)]
enum ServerMode {
    RequireAuthHeader,
    DelayResponse { millis: u64 },
    CaptureRequest,
}

#[derive(Clone)]
struct ServerState {
    mode: ServerMode,
    captured_request: Option<Arc<Mutex<Option<OrchestrationCompactRequest>>>>,
}

async fn compact_handler(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(req): Json<OrchestrationCompactRequest>,
) -> (
    StatusCode,
    Json<arco_flow::orchestration_compaction::OrchestrationCompactionResponse>,
) {
    if req.event_paths.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                arco_flow::orchestration_compaction::OrchestrationCompactionResponse {
                    events_processed: 0,
                    delta_id: None,
                    manifest_revision: String::new(),
                    visibility_status: VisibilityStatus::Visible,
                    repair_pending: false,
                },
            ),
        );
    }

    match state.mode {
        ServerMode::RequireAuthHeader => {
            let auth = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok());

            if auth != Some("Bearer test-token") {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(
                        arco_flow::orchestration_compaction::OrchestrationCompactionResponse {
                            events_processed: 0,
                            delta_id: None,
                            manifest_revision: String::new(),
                            visibility_status: VisibilityStatus::Visible,
                            repair_pending: false,
                        },
                    ),
                );
            }
        }
        ServerMode::DelayResponse { millis } => {
            tokio::time::sleep(Duration::from_millis(millis)).await;
        }
        ServerMode::CaptureRequest => {
            let captured = state
                .captured_request
                .as_ref()
                .expect("capture state")
                .clone();
            *captured.lock().expect("capture lock") = Some(req.clone());
        }
    }

    (
        StatusCode::OK,
        Json(
            arco_flow::orchestration_compaction::OrchestrationCompactionResponse {
                events_processed: u32::try_from(req.event_paths.len()).expect("event count fits"),
                delta_id: None,
                manifest_revision: "manifest-test".to_string(),
                visibility_status: VisibilityStatus::Visible,
                repair_pending: false,
            },
        ),
    )
}

async fn start_test_server(
    mode: ServerMode,
) -> (
    String,
    Option<Arc<Mutex<Option<OrchestrationCompactRequest>>>>,
    tokio::task::JoinHandle<()>,
) {
    let captured_request =
        matches!(mode, ServerMode::CaptureRequest).then(|| Arc::new(Mutex::new(None)));
    let app = Router::new()
        .route("/compact", post(compact_handler))
        .with_state(ServerState {
            mode,
            captured_request: captured_request.clone(),
        });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr: SocketAddr = listener.local_addr().expect("listener addr");
    let base_url = format!("http://{addr}");

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test server");
    });

    (base_url, captured_request, handle)
}

#[tokio::test]
async fn compaction_client_sends_static_bearer_token_when_configured() {
    let (base_url, _capture, _handle) = start_test_server(ServerMode::RequireAuthHeader).await;
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
    let (base_url, _capture, _handle) =
        start_test_server(ServerMode::DelayResponse { millis: 10_000 }).await;

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

#[tokio::test]
async fn compaction_client_sends_fencing_token_and_lock_path() {
    let (base_url, capture, _handle) = start_test_server(ServerMode::CaptureRequest).await;
    let capture = capture.expect("capture handle");

    let response = compact_orchestration_events_fenced(
        &base_url,
        vec!["ledger/events/3.json".to_string()],
        17,
        "locks/orchestration.compaction.lock.json",
        Some("req_fenced"),
    )
    .await;

    #[cfg(any(feature = "gcp", feature = "test-utils"))]
    {
        let response = response.expect("fenced compaction should succeed");
        assert_eq!(response.visibility_status.as_str(), "visible");
        assert!(!response.repair_pending);

        let request = capture
            .lock()
            .expect("capture lock")
            .clone()
            .expect("captured request");
        assert_eq!(request.fencing_token, Some(17));
        assert_eq!(
            request.lock_path.as_deref(),
            Some("locks/orchestration.compaction.lock.json")
        );
        assert_eq!(request.request_id.as_deref(), Some("req_fenced"));
    }

    #[cfg(not(any(feature = "gcp", feature = "test-utils")))]
    {
        let err = response.expect_err("expected configuration error without gcp/test-utils");
        assert!(
            err.to_string()
                .contains("orchestration compaction requires the 'gcp' feature"),
            "unexpected error when gcp/test-utils feature is disabled: {err}"
        );
    }
}
