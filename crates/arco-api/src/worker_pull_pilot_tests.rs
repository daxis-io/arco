use std::io::Write as _;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_flow::orchestration::run_key::{RunKeyReservation, reservation_path};
use arco_flow::{orchestration_compaction_lock_path, orchestration_manifest_pointer_path};
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use bytes::Bytes;
use chrono::Utc;
use jsonwebtoken::{EncodingKey, Header, encode};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

use crate::config::Config;
use crate::server::Server;

const TENANT: &str = "pilot-tenant";
const WORKSPACE: &str = "pilot-workspace";
const PREFIX: &str = "conformance/s3/api-pilot/test";
const MANIFESTS: &str = "/api/v1/workspaces/pilot-workspace/manifests";
const RUNS: &str = "/api/v1/workspaces/pilot-workspace/runs";
const JWT_KEY: &str = "synthetic-api-pilot-jwt-secret-32";
const CONTROL: &str = "synthetic-api-pilot-controller-32";
const SESSION: &str = "synthetic-api-pilot-session-key-32";
const TASK_KEY: &str = "synthetic-legacy-task-signing-key-32";

#[derive(Default)]
struct PointerFaultBackend {
    inner: MemoryBackend,
    fault: AtomicU8,
}

#[async_trait::async_trait]
impl StorageBackend for PointerFaultBackend {
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
        condition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        let fault = if path.ends_with(orchestration_manifest_pointer_path()) {
            self.fault.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if fault == 1 {
            return Err(arco_core::Error::storage("fixture rejected pointer write"));
        }
        let result = self.inner.put(path, data, condition).await?;
        if fault == 2 && matches!(&result, WriteResult::Success { .. }) {
            return Err(arco_core::Error::storage(
                "fixture dropped pointer write reply",
            ));
        }
        Ok(result)
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
        self.inner.signed_url(path, expiry).await
    }
}

struct Fixture {
    backend: Arc<PointerFaultBackend>,
    file: tempfile::NamedTempFile,
}

impl Fixture {
    fn new() -> Self {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(
            file,
            "{}",
            json!({
                "tenant": TENANT, "workspace": WORKSPACE, "storagePrefix": PREFIX,
                "host": {
                    "permits": 2, "callbackBaseUrl": "http://127.0.0.1:8011/worker-pull",
                    "claimKey": vec![10_u8; 32], "controlToken": CONTROL,
                    "sessions": {"first": SESSION},
                }
            })
        )
        .unwrap();
        Self {
            backend: Arc::new(PointerFaultBackend::default()),
            file,
        }
    }

    fn router(&self, pilot: bool) -> Router {
        let mut config = Config {
            debug: false,
            ..Config::default()
        };
        config.jwt.hs256_secret = Some(JWT_KEY.into());
        config.task_token.hs256_secret = TASK_KEY.into();
        config.storage.bucket = Some("s3://test-pilot".into());
        let mut config_json = serde_json::to_value(config).unwrap();
        if pilot {
            config_json["worker_pull_pilot_path"] = json!(self.file.path());
        }
        let config = serde_json::from_value(config_json).unwrap();
        Server::with_storage_backend(config, self.backend.clone()).test_router()
    }
}

fn jwt(workspace: &str) -> String {
    encode(
        &Header::default(),
        &json!({"tenant": TENANT, "workspace": workspace, "sub": "pilot-fixture", "exp": Utc::now().timestamp() + 600}),
        &EncodingKey::from_secret(JWT_KEY.as_bytes())
    ).unwrap()
}

fn manifest() -> Value {
    json!({
        "manifestVersion": "1.0", "codeVersionId": "pilot-fixture-v1",
        "assets": [{
            "key": {"namespace": "proof", "name": "alpha"},
            "id": "00000000-0000-4000-8000-000000000001",
            "execution": {"maxAttempts": 2, "heartbeatTimeoutSec": 60}
        }],
    })
}

async fn call(
    router: &Router,
    method: Method,
    path: &str,
    token: &str,
    key: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json");
    if let Some(key) = key {
        request = request.header("idempotency-key", key);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

#[tokio::test]
async fn opted_in_mount_and_scope_enforce_separate_credentials() {
    let fixture = Fixture::new();
    let legacy = fixture.router(false);
    assert_eq!(
        call(
            &legacy,
            Method::POST,
            "/worker-pull/run",
            CONTROL,
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let router = fixture.router(true);
    let jwt = jwt(WORKSPACE);
    assert_eq!(
        call(
            &router,
            Method::POST,
            "/worker-pull/run",
            &jwt,
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &router,
            Method::POST,
            "/worker-pull/run",
            CONTROL,
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &router,
            Method::POST,
            "/worker-pull/api/v1/tasks/claim",
            CONTROL,
            None,
            json!({"requestId": "bad"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &router,
            Method::GET,
            "/api/v1/workspaces/other/runs",
            &jwt_for_other_workspace(),
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

fn jwt_for_other_workspace() -> String {
    jwt("other")
}

async fn submit(router: &Router, user: &str, key: &str, selection: &[&str]) -> String {
    let (status, body) = call(
        router,
        Method::POST,
        RUNS,
        user,
        Some(key),
        json!({"selection": selection}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body["runId"].as_str().unwrap().to_owned()
}

async fn claim(router: &Router, request_id: &str) -> Value {
    let (status, body) = call(
        router,
        Method::POST,
        "/worker-pull/api/v1/tasks/claim",
        SESSION,
        None,
        json!({"requestId": request_id}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

async fn callback(router: &Router, reply: &Value, action: &str) -> (StatusCode, Value) {
    let envelope = &reply["grant"]["envelope"];
    let task_id = envelope["taskId"].as_str().unwrap();
    let mut body = json!({
        "attempt": envelope["attempt"],
        "attemptId": envelope["attemptId"],
        "workerId": "first",
    });
    if action == "completed" {
        body["outcome"] = json!("SUCCEEDED");
    }
    call(
        router,
        Method::POST,
        &format!("/worker-pull/api/v1/tasks/{task_id}/{action}"),
        envelope["taskToken"].as_str().unwrap(),
        None,
        body,
    )
    .await
}

async fn cancel(router: &Router, token: &str, run_id: &str, reason: &str) -> (StatusCode, Value) {
    call(
        router,
        Method::POST,
        &format!("{RUNS}/{run_id}/cancel"),
        token,
        None,
        json!({"reason": reason}),
    )
    .await
}

async fn selected_manifest(fixture: &Fixture) -> Value {
    let scope = format!("{PREFIX}/tenant={TENANT}/workspace={WORKSPACE}");
    let backend = &fixture.backend.inner;
    let pointer_path = format!("{scope}/{}", orchestration_manifest_pointer_path());
    let pointer: Value =
        serde_json::from_slice(&backend.get(&pointer_path).await.unwrap()).unwrap();
    let manifest_path = pointer["manifest_path"].as_str().unwrap();
    serde_json::from_slice(
        &backend
            .get(&format!("{scope}/{manifest_path}"))
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn pilot_cancel_releases_unstarted_permit_and_keeps_running_permit_through_completion() {
    let fixture = Fixture::new();
    let router = fixture.router(true);
    let user = jwt(WORKSPACE);
    let mut manifest = manifest();
    let mut beta = manifest["assets"][0].clone();
    beta["key"]["name"] = json!("beta");
    beta["id"] = json!("00000000-0000-4000-8000-000000000002");
    manifest["assets"].as_array_mut().unwrap().push(beta);
    assert_eq!(
        call(&router, Method::POST, MANIFESTS, &user, None, manifest)
            .await
            .0,
        StatusCode::CREATED
    );
    let selection = ["proof/alpha", "proof/beta"];
    let run_id = submit(&router, &user, "stop", &selection).await;
    let running = claim(&router, "running").await;
    assert_eq!(
        callback(&router, &running, "started").await.0,
        StatusCode::OK
    );
    let unstarted = claim(&router, "unstarted").await;
    assert_eq!(unstarted["status"], "granted");
    let other_run = submit(&router, &user, "continue", &selection).await;
    assert_eq!(claim(&router, "wait").await["status"], "busy");

    let (status, response) = cancel(&router, &user, &run_id, "pause pipeline").await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["runId"], run_id);
    assert_eq!(response["state"], "CANCELLING");
    assert_eq!(
        callback(&router, &unstarted, "started").await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(claim(&router, "unstarted").await["status"], "cancelled");
    let replacement = claim(&router, "wait").await;
    assert_eq!(replacement["grant"]["envelope"]["runId"], other_run);
    assert_eq!(replacement["grant"]["permit"], unstarted["grant"]["permit"]);
    assert_eq!(claim(&router, "still-waiting").await["status"], "busy");
    let (status, heartbeat) = callback(&router, &running, "heartbeat").await;
    assert_eq!(status, StatusCode::OK, "{heartbeat}");
    assert_eq!(heartbeat["shouldCancel"], true);
    let (status, completed) = callback(&router, &running, "completed").await;
    assert_eq!(status, StatusCode::OK, "{completed}");
    assert_eq!(completed["finalState"], "CANCELLED");
    let next = claim(&router, "still-waiting").await;
    assert_eq!(next["grant"]["envelope"]["runId"], other_run);
    assert_eq!(next["grant"]["permit"], running["grant"]["permit"]);
    let (status, replay) = cancel(&router, &user, &run_id, "try again").await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay["state"], "CANCELLED");

    for grant in [replacement, next] {
        assert_eq!(callback(&router, &grant, "started").await.0, StatusCode::OK);
        let (status, completed) = callback(&router, &grant, "completed").await;
        assert_eq!(status, StatusCode::OK, "{completed}");
        assert_eq!(completed["finalState"], "SUCCEEDED");
    }
    assert_eq!(
        cancel(&router, &user, &other_run, "too late").await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(claim(&router, "done").await["status"], "empty");
}

#[tokio::test]
async fn pilot_cancel_recovers_before_and_after_publication_and_keeps_the_operator_fact() {
    for fault in [1, 2] {
        let fixture = Fixture::new();
        let first = fixture.router(true);
        let user = jwt(WORKSPACE);
        assert_eq!(
            cancel(&first, &user, "missing", "unknown run").await.0,
            StatusCode::NOT_FOUND
        );
        assert!(
            fixture
                .backend
                .inner
                .list("")
                .await
                .unwrap()
                .iter()
                .all(|object| !object.path.ends_with(orchestration_manifest_pointer_path()))
        );
        assert_eq!(
            call(&first, Method::POST, MANIFESTS, &user, None, manifest())
                .await
                .0,
            StatusCode::CREATED
        );
        let run_id = submit(&first, &user, "durable-cancel", &["proof/alpha"]).await;
        let before = selected_manifest(&fixture).await;
        assert_eq!(
            cancel(&first, &jwt_for_other_workspace(), &run_id, "wrong tenant")
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        fixture.backend.fault.store(fault, Ordering::SeqCst);
        let (status, body) = cancel(&first, &user, &run_id, "first reason").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let after_fault = selected_manifest(&fixture).await;
        assert_eq!(after_fault == before, fault == 1);
        drop(first);
        let restarted = fixture.router(true);
        let (status, body) = cancel(&restarted, &user, &run_id, "recovery reason").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["state"], "CANCELLED");
        assert!(body["cancelledAt"].is_string());
        let committed = selected_manifest(&fixture).await;
        let events = committed["worker_pull"]["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["data"]["type"], "run_cancel_requested");
        assert_eq!(events[0]["data"]["requested_by"], "pilot-fixture");
        assert_eq!(
            events[0]["data"]["reason"],
            if fault == 1 {
                "recovery reason"
            } else {
                "first reason"
            }
        );
        assert_eq!(committed == after_fault, fault == 2);
        let (status, replay) = cancel(&restarted, &user, &run_id, "ignored").await;
        assert_eq!(status, StatusCode::OK, "{replay}");
        assert_eq!(replay["state"], "CANCELLED");
        assert_eq!(committed, selected_manifest(&fixture).await);
    }
}

#[tokio::test]
async fn keyed_recovery_uses_one_bound_plan_across_both_publication_boundaries() {
    for fault in [1, 2] {
        let fixture = Fixture::new();
        let first = fixture.router(true);
        let jwt = jwt(WORKSPACE);
        assert_eq!(
            call(&first, Method::POST, MANIFESTS, &jwt, None, manifest())
                .await
                .0,
            StatusCode::CREATED
        );
        fixture.backend.fault.store(fault, Ordering::SeqCst);
        let submission = json!({"selection": ["proof/alpha"]});
        let (status, body) = call(
            &first,
            Method::POST,
            RUNS,
            &jwt,
            Some("stable-key"),
            submission.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let scope = format!("{PREFIX}/tenant={TENANT}/workspace={WORKSPACE}");
        let reservation: RunKeyReservation = serde_json::from_slice(
            &fixture
                .backend
                .inner
                .get(&format!("{scope}/{}", reservation_path("stable-key")))
                .await
                .unwrap(),
        )
        .unwrap();
        drop(first);
        let restarted = fixture.router(true);
        let path = format!("{RUNS}/{}", reservation.run_id);
        let before = call(&restarted, Method::GET, &path, &jwt, None, Value::Null)
            .await
            .0;
        assert_eq!(
            before,
            if fault == 1 {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::OK
            }
        );
        let (status, reply) = call(
            &restarted,
            Method::POST,
            RUNS,
            &jwt,
            Some("stable-key"),
            submission,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reply}");
        assert_eq!(reply["runId"], reservation.run_id);
        assert_eq!(reply["planId"], reservation.plan_id);
        assert_eq!(reply["created"], false);
        let (_, run) = call(&restarted, Method::GET, &path, &jwt, None, Value::Null).await;
        assert_eq!(run["runId"], reservation.run_id);
        let plan = fixture
            .backend
            .inner
            .get(&format!(
                "{scope}/accepted_plans/{}.json",
                reservation.plan_id
            ))
            .await
            .unwrap();
        let sha = hex::encode(Sha256::digest(&plan));
        assert_eq!(
            reservation.accepted_plan_sha256.as_deref(),
            Some(sha.as_str())
        );
        let plan: arco_flow::orchestration::accepted_plan::AcceptedRunPlan =
            serde_json::from_slice(&plan).unwrap();
        assert_eq!(plan.version, 2);
        let [run, planned] = plan.events.as_slice() else {
            panic!("two accepted events");
        };
        assert_eq!(run.event_id, reservation.event_id);
        assert_eq!(Some(&planned.event_id), reservation.plan_event_id.as_ref());
        let arco_flow::orchestration::events::OrchestrationEventData::RunTriggered {
            labels, ..
        } = &run.data
        else {
            panic!("run trigger");
        };
        assert_eq!(labels["arco.retry_policy"], "rivers-d7cbc206-compat-v1");
        let arco_flow::orchestration::events::OrchestrationEventData::PlanCreated { tasks, .. } =
            &planned.data
        else {
            panic!("plan created");
        };
        assert_eq!(
            (tasks[0].max_attempts, tasks[0].heartbeat_timeout_sec),
            (2, 60)
        );
        assert!(!tasks[0].requires_visible_output);
        let objects = fixture.backend.inner.list("").await.unwrap();
        assert!(
            objects
                .iter()
                .all(|meta| meta.path.starts_with(&format!("{PREFIX}/"))
                    && !meta.path.contains("/ledger/orchestration/"))
        );
        assert!(
            objects
                .iter()
                .any(|meta| meta.path
                    == format!("{PREFIX}/{}", orchestration_compaction_lock_path()))
        );
        if fault == 2 {
            fixture
                .backend
                .inner
                .put(
                    &format!("{scope}/accepted_plans/{}.json", reservation.plan_id),
                    Bytes::from_static(b"corrupted proof"),
                    WritePrecondition::None,
                )
                .await
                .unwrap();
            assert_eq!(
                call(
                    &restarted,
                    Method::POST,
                    RUNS,
                    &jwt,
                    Some("stable-key"),
                    json!({"selection": ["proof/alpha"]})
                )
                .await
                .0,
                StatusCode::CONFLICT
            );
        }
    }
}

#[tokio::test]
async fn pilot_rejects_unkeyed_and_legacy_writes_before_creating_objects() {
    let fixture = Fixture::new();
    let router = fixture.router(true);
    let jwt = jwt(WORKSPACE);
    assert_eq!(
        call(&router, Method::POST, MANIFESTS, &jwt, None, manifest())
            .await
            .0,
        StatusCode::CREATED
    );
    let objects = fixture.backend.inner.list("").await.unwrap().len();
    let (status, body) = call(
        &router,
        Method::POST,
        RUNS,
        &jwt,
        None,
        json!({"selection": ["proof/alpha"]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let mut native = manifest();
    native["assets"][0]["io"] = json!({"output": "managed-table"});
    assert_eq!(
        call(
            &router,
            Method::POST,
            MANIFESTS,
            &jwt,
            Some("native-key"),
            native
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let mut scheduled = manifest();
    scheduled["schedules"] = json!([{"id": "s", "cron": "0 * * * *", "assets": ["proof/alpha"]}]);
    assert_eq!(
        call(
            &router,
            Method::POST,
            MANIFESTS,
            &jwt,
            Some("schedule-key"),
            scheduled
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let task_token = arco_core::mint_task_token(
        &arco_core::TaskTokenConfig {
            hs256_secret: TASK_KEY.into(),
            ..Default::default()
        },
        "legacy-task",
        TENANT,
        WORKSPACE,
        Utc::now(),
    )
    .unwrap();
    assert_eq!(
        call(
            &router,
            Method::POST,
            "/api/v1/tasks/legacy-task/started",
            &task_token.token,
            None,
            Value::Null
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(fixture.backend.inner.list("").await.unwrap().len(), objects);
    assert_eq!(
        call(
            &router,
            Method::POST,
            RUNS,
            &jwt,
            Some("same-key"),
            json!({"selection": ["proof/alpha"]})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert_eq!(
        call(
            &router,
            Method::POST,
            RUNS,
            &jwt,
            Some("same-key"),
            json!({"selection": ["proof/alpha"], "labels": {"changed": "1"}})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
}
