//! Local fixture seeder for the packaged HTTP process proof.

use std::collections::HashMap;

use arco_core::ScopedStorage;
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskDef, TimerType, TriggerInfo,
};
use arco_flow::orchestration::flow_service::append_events_and_compact;
use arco_storage::from_bucket;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run_id = std::env::args().nth(1).ok_or("run ID required")?;
    let age_minutes: i64 = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "0".into())
        .parse()?;
    let backend = from_bucket(&std::env::var("ARCO_STORAGE_BUCKET")?)?;
    let storage = ScopedStorage::new(backend, "pilot", "flow")?;
    let ledger = LedgerWriter::new(storage);
    if run_id == "timer" {
        append_events_and_compact(
            &ledger,
            None,
            vec![OrchestrationEvent::new(
                "pilot",
                "flow",
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
        "pilot",
        "flow",
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
        "pilot",
        "flow",
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
        "pilot",
        "flow",
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
