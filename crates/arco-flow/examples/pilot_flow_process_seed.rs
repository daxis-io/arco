//! Local fixture seeder for the packaged HTTP process proof.

use std::collections::HashMap;

use arco_core::{ScopedStorage, WritePrecondition, WriteResult};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskDef, TimerType, TriggerInfo,
};
use arco_flow::orchestration::flow_service::append_events_and_compact;
use arco_storage::from_bucket;
use arco_worker_contract::PublicationDescriptor;
use bytes::Bytes;
use sha2::{Digest, Sha256};

fn scope_from_values(
    tenant: Option<&str>,
    workspace: Option<&str>,
) -> Result<(String, String), &'static str> {
    let tenant = tenant.unwrap_or("pilot");
    let workspace = workspace.unwrap_or("flow");
    if tenant.is_empty() || workspace.is_empty() {
        return Err("ARCO_TENANT_ID and ARCO_WORKSPACE_ID cannot be empty");
    }
    Ok((tenant.to_string(), workspace.to_string()))
}

#[tokio::main]
#[allow(
    clippy::print_stdout,
    reason = "fixture stdout is the descriptor protocol consumed by the reference worker"
)]
#[allow(
    clippy::too_many_lines,
    reason = "fixture commands and their ordered event trace stay together"
)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run_id = std::env::args().nth(1).ok_or("run ID required")?;
    let backend = from_bucket(&std::env::var("ARCO_STORAGE_BUCKET")?)?;
    let (tenant, workspace) = scope_from_values(
        std::env::var("ARCO_TENANT_ID").ok().as_deref(),
        std::env::var("ARCO_WORKSPACE_ID").ok().as_deref(),
    )?;
    let storage = ScopedStorage::new(backend, tenant.clone(), workspace.clone())?;
    if run_id == "output" {
        let output_run = std::env::args().nth(2).ok_or("output run ID required")?;
        let value: i64 = std::env::args()
            .nth(3)
            .ok_or("output value required")?
            .parse()?;
        let descriptor = write_output(&storage, &output_run, value).await?;
        println!("{}", serde_json::to_string(&descriptor)?);
        return Ok(());
    }
    if run_id == "corrupt-output" {
        let output_run = std::env::args().nth(2).ok_or("output run ID required")?;
        corrupt_output(&storage, &output_run).await?;
        println!("corrupted {output_run}");
        return Ok(());
    }
    if run_id == "remove-output" {
        let output_run = std::env::args().nth(2).ok_or("output run ID required")?;
        storage
            .delete(&format!("outputs/{output_run}/result.parquet"))
            .await?;
        println!("removed {output_run}");
        return Ok(());
    }
    if run_id == "repair-output" {
        let output_run = std::env::args().nth(2).ok_or("output run ID required")?;
        let value: i64 = std::env::args()
            .nth(3)
            .ok_or("output value required")?
            .parse()?;
        repair_output(&storage, &output_run, value).await?;
        println!("repaired {output_run}");
        return Ok(());
    }
    let age_minutes: i64 = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "0".into())
        .parse()?;
    let ledger = LedgerWriter::new(storage);
    if run_id == "timer" {
        append_events_and_compact(
            &ledger,
            None,
            vec![OrchestrationEvent::new(
                &tenant,
                &workspace,
                OrchestrationEventData::TimerRequested {
                    timer_id: "pilot-timer".into(),
                    timer_type: TimerType::Retry,
                    run_id: None,
                    task_key: None,
                    attempt: None,
                    fire_at: chrono::Utc::now() + chrono::Duration::minutes(5),
                },
            )],
        )
        .await?;
        println!("seeded timer");
        return Ok(());
    }
    let run = OrchestrationEvent::new(
        &tenant,
        &workspace,
        OrchestrationEventData::RunTriggered {
            run_id: run_id.clone(),
            plan_id: format!("{run_id}-plan"),
            trigger: TriggerInfo::Manual {
                user_id: "operator".into(),
            },
            root_assets: vec!["task".into()],
            run_key: None,
            labels: HashMap::new(),
            code_version: None,
        },
    );
    let plan = OrchestrationEvent::new(
        &tenant,
        &workspace,
        OrchestrationEventData::PlanCreated {
            run_id: run_id.clone(),
            plan_id: format!("{run_id}-plan"),
            tasks: vec![TaskDef {
                key: "task".into(),
                depends_on: vec![],
                asset_key: None,
                partition_key: None,
                max_attempts: 1,
                heartbeat_timeout_sec: 300,
                requires_visible_output: false,
            }],
        },
    );
    let mut dispatch = OrchestrationEvent::new(
        &tenant,
        &workspace,
        OrchestrationEventData::DispatchRequested {
            run_id: run_id.clone(),
            task_key: "task".into(),
            attempt: 1,
            attempt_id: format!("{run_id}-attempt"),
            worker_queue: "default-queue".into(),
            dispatch_id: format!("dispatch:{run_id}:task:1"),
        },
    );
    dispatch.timestamp -= chrono::Duration::minutes(age_minutes);
    append_events_and_compact(&ledger, None, vec![run, plan, dispatch]).await?;
    println!("seeded {run_id}");
    Ok(())
}

async fn write_output(
    storage: &ScopedStorage,
    run_id: &str,
    value: i64,
) -> Result<PublicationDescriptor, Box<dyn std::error::Error>> {
    let bytes = parquet_bytes(value)?;
    let path = format!("outputs/{run_id}/result.parquet");
    let checksum = hex::encode(Sha256::digest(&bytes));
    let byte_size = u64::try_from(bytes.len())?;
    if matches!(
        storage
            .put_raw(
                &path,
                Bytes::from(bytes.clone()),
                WritePrecondition::DoesNotExist
            )
            .await?,
        WriteResult::PreconditionFailed { .. }
    ) && storage.get_raw(&path).await?.as_ref() != bytes.as_slice()
    {
        return Err("output identity conflict".into());
    }
    let meta = storage.head_raw(&path).await?.ok_or("output disappeared")?;
    Ok(PublicationDescriptor {
        version: 1,
        manifest_id: format!("sha256:{checksum}"),
        object_path: path,
        object_version: meta.version,
        checksum_sha256: checksum.clone(),
        byte_size,
        format: "parquet".into(),
        schema_ref: format!("sha256:{checksum}#parquet-schema"),
        owner_evidence: None,
    })
}

fn parquet_bytes(value: i64) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )?;
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}

async fn corrupt_output(
    storage: &ScopedStorage,
    run_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = format!("outputs/{run_id}/result.parquet");
    let meta = storage.head_raw(&path).await?.ok_or("output disappeared")?;
    let result = storage
        .put_raw(
            &path,
            Bytes::from_static(b"corrupt-parquet-output"),
            WritePrecondition::MatchesVersion(meta.version),
        )
        .await?;
    if matches!(result, WriteResult::PreconditionFailed { .. }) {
        return Err("output changed before corruption".into());
    }
    Ok(())
}

async fn repair_output(
    storage: &ScopedStorage,
    run_id: &str,
    value: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = format!("outputs/{run_id}/result.parquet");
    let meta = storage.head_raw(&path).await?.ok_or("output disappeared")?;
    let bytes = parquet_bytes(value)?;
    let result = storage
        .put_raw(
            &path,
            Bytes::from(bytes),
            WritePrecondition::MatchesVersion(meta.version),
        )
        .await?;
    if matches!(result, WriteResult::PreconditionFailed { .. }) {
        return Err("output changed before repair".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arco_flow::orchestration::callbacks::{
        PublicationVerifier, ScopedStoragePublicationVerifier,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn reference_output_is_verified_immutable_and_replayable() {
        let storage =
            ScopedStorage::new(Arc::new(arco_core::MemoryBackend::new()), "pilot", "flow")
                .expect("scope");
        let descriptor = write_output(&storage, "run-output", 7)
            .await
            .expect("output");
        ScopedStoragePublicationVerifier::new(storage.clone())
            .verify(&descriptor)
            .await
            .expect("owner verification");
        let replay = write_output(&storage, "run-output", 7)
            .await
            .expect("same output");
        assert!(descriptor.same_immutable_claim(&replay));
        assert!(write_output(&storage, "run-output", 9).await.is_err());
    }

    #[tokio::test]
    async fn repair_never_claims_the_original_publication_identity_on_a_new_generation() {
        let storage =
            ScopedStorage::new(Arc::new(arco_core::MemoryBackend::new()), "pilot", "flow")
                .expect("scope");
        let descriptor = write_output(&storage, "run-output", 7)
            .await
            .expect("output");
        corrupt_output(&storage, "run-output")
            .await
            .expect("corrupt");
        repair_output(&storage, "run-output", 7)
            .await
            .expect("repair");
        let repaired = storage
            .head_raw("outputs/run-output/result.parquet")
            .await
            .expect("head")
            .expect("object");
        assert_ne!(repaired.version, descriptor.object_version);
        ScopedStoragePublicationVerifier::new(storage)
            .verify(&descriptor)
            .await
            .expect_err("a new generation must not satisfy the old descriptor");
    }

    #[test]
    fn explicit_seed_scope_replaces_only_the_local_defaults() {
        assert_eq!(
            scope_from_values(Some("tenant-r2"), Some("workspace-r2")).expect("scope"),
            ("tenant-r2".to_string(), "workspace-r2".to_string())
        );
        assert_eq!(
            scope_from_values(None, None).expect("default scope"),
            ("pilot".to_string(), "flow".to_string())
        );
    }
}
