//! One-shot workspace invocations against the disk or loopback S3 proof fixture.

#[allow(clippy::expect_used, clippy::significant_drop_tightening)]
#[path = "operator_owned_dispatcher/persistent_flow_backend.rs"]
mod persistent_flow_backend;

#[path = "operator_owned_worker_pull/s3_fixture.rs"]
mod s3_fixture;

#[path = "operator_owned_worker_pull/hosted_diagnostics.rs"]
mod hosted_diagnostics;

use std::{
    error::Error,
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use arco_core::{DistributedLock, ScopedStorage, storage::StorageBackend};
use arco_flow::{
    orchestration::accepted_plan::AcceptedRunPlan,
    orchestration::compactor::{MicroCompactor, OrchestrationManifestPointer},
    orchestration::worker_pull::{WorkerPull, WorkerPullConfig},
    orchestration_compaction_lock_path, orchestration_manifest_pointer_path,
};
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tower::ServiceExt;

#[derive(Deserialize)]
struct Scope {
    tenant: String,
    workspace: String,
    host: WorkerPullConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let [config, fixture, action]: [String; 3] = std::env::args()
        .skip(1)
        .collect::<Vec<_>>()
        .try_into()
        .map_err(
            |_| "usage: operator_owned_worker_pull CONFIG FIXTURE accept|accept-rivers|ROUTE",
        )?;
    let scope: Scope = serde_json::from_reader(std::fs::File::open(config)?)?;
    let validation = if action == "validate-s3" {
        Some("inspect")
    } else {
        action.strip_prefix("validate-s3:")
    };
    let s3 = if fixture.starts_with("s3://") {
        let spec = s3_fixture::validate(
            &fixture,
            s3_fixture::scoped_pointer(&scope.tenant, &scope.workspace)?,
            validation.unwrap_or(&action),
        )?;
        if validation.is_some() {
            return emit(&spec.describe());
        }
        Some(spec.open(&fixture)?)
    } else {
        if validation.is_some() || std::env::var_os("ARCO_PULL_TEST_MODE").is_some() {
            return Err("S3 fixture mode requires an S3 bucket".into());
        }
        None
    };
    let backend: Arc<dyn StorageBackend> = if let Some(s3) = s3.as_ref() {
        s3.clone()
    } else {
        Arc::new(persistent_flow_backend::PersistentFlowBackend::open(
            Path::new(&fixture),
        )?)
    };
    let hosted = s3.as_ref().is_some_and(|backend| backend.is_hosted());
    if hosted {
        hosted_diagnostics::install()?;
    }
    let storage = ScopedStorage::new(backend, scope.tenant, scope.workspace)?;
    let mut result = run(storage, scope.host, &action, s3.is_some()).await?;
    if hosted && result.get("status").and_then(Value::as_u64) == Some(503) {
        hosted_diagnostics::retry_response(
            result
                .get("response")
                .and_then(|body| body.get("error"))
                .and_then(Value::as_str),
        );
    }
    if let Some(counts) = s3.as_ref().and_then(|backend| backend.counts()) {
        result
            .as_object_mut()
            .ok_or("fixture response should be an object")?
            .insert("storageCalls".to_string(), counts);
    }
    emit(&result)
}

async fn run(
    storage: ScopedStorage,
    config: WorkerPullConfig,
    action: &str,
    s3: bool,
) -> Result<Value, Box<dyn Error>> {
    if action == "inspect" {
        return inspect(storage).await;
    }
    if action == "break-fixture-lock" && s3 {
        let lock = DistributedLock::new(
            storage.backend().clone(),
            orchestration_compaction_lock_path(),
        );
        lock.force_break().await?;
        return Ok(json!({"lock": lock.read_lock_info().await?}));
    }
    if let Some(candidate) = action.strip_prefix("snapshot:") {
        if candidate.len() != 20 || !candidate.bytes().all(|b| b.is_ascii_digit()) {
            return Err("expected a canonical snapshot ID".into());
        }
        let bytes = storage
            .get_raw(&format!("state/orchestration/manifests/{candidate}.json"))
            .await?;
        return Ok(serde_json::from_slice(&bytes)?);
    }
    let host = WorkerPull::new(storage, config)?;
    let mut input = Vec::new();
    std::io::stdin()
        .take(16 * 1024 * 1024 + 1)
        .read_to_end(&mut input)?;

    if matches!(action, "accept" | "accept-rivers") {
        let plan: AcceptedRunPlan = serde_json::from_slice(&input)?;
        let plan = if action == "accept-rivers" {
            plan.with_rivers_retry_policy()?
        } else {
            plan
        };
        let revision = host.accept_plan(&plan).await?;
        return Ok(json!({"observedRevision": revision}));
    }

    let bearer = std::env::var("ARCO_PULL_BEARER")?;
    let response = host
        .router()
        .oneshot(
            Request::post(action)
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {bearer}"))
                .body(Body::from(input))?,
        )
        .await?;
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 16 * 1024 * 1024).await?;
    let body: Value = serde_json::from_slice(&body)?;
    Ok(json!({"status": status, "response": body}))
}

fn emit(result: &Value) -> Result<(), Box<dyn Error>> {
    writeln!(std::io::stdout().lock(), "{result}")?;
    Ok(())
}

async fn inspect(storage: ScopedStorage) -> Result<Value, Box<dyn Error>> {
    let (manifest, fold) = MicroCompactor::new(storage.clone()).load_state().await?;
    let path = orchestration_manifest_pointer_path();
    let pointer: OrchestrationManifestPointer =
        serde_json::from_slice(&storage.get_raw(path).await?)?;
    if pointer.manifest_id != manifest.manifest_id || pointer.epoch != manifest.epoch {
        return Err("selected pointer moved during fixture inspection".into());
    }
    let pointer_meta = storage
        .head_raw(path)
        .await?
        .ok_or("selected pointer disappeared")?;
    let lock_path = orchestration_compaction_lock_path();
    let lock_meta = storage
        .backend()
        .head(lock_path)
        .await?
        .ok_or("fixture lock missing")?;
    let lock = DistributedLock::new(storage.backend().clone(), lock_path);
    let mut tasks: Vec<_> = fold.tasks.values().collect();
    tasks.sort_by_key(|task| (&task.run_id, &task.task_key));
    let mut dispatches: Vec<_> = fold.dispatch_outbox.values().collect();
    dispatches.sort_by_key(|row| &row.dispatch_id);
    Ok(json!({
        "manifest": manifest,
        "pointer": pointer,
        "pointerVersion": pointer_meta.version,
        "pointerEtag": pointer_meta.etag,
        "lockVersion": lock_meta.version,
        "lockEtag": lock_meta.etag,
        "lock": lock.read_lock_info().await?,
        "runs": fold.runs,
        "tasks": tasks,
        "dispatches": dispatches,
    }))
}
