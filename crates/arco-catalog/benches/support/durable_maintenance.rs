use arco_catalog::{
    CatalogError, ControlMvpMaintenanceOutcome, DurableMaintenanceWorker, MaintenanceStatus,
};

// Shared fixture orchestration uses only the production worker's explicit operations.
// Bounds are unchanged: four invalidated jobs and 512 invocations per job.
pub async fn consolidate_pending(
    worker: &DurableMaintenanceWorker,
) -> Result<Option<ControlMvpMaintenanceOutcome>, CatalogError> {
    for _ in 0..4 {
        let Some(plan) = Box::pin(worker.prepare_at(input_now())).await? else {
            return Ok(None);
        };
        let id = plan.job_id().clone();
        let mut progress = Box::pin(worker.start_at(&plan, input_now())).await?;
        let mut invalidated = false;
        for _ in 0..512 {
            let attempt = if progress.status == MaintenanceStatus::Active {
                worker.advance_at(&id, input_now()).await.map(|next| {
                    progress = next;
                    None
                })
            } else {
                worker.publish_at(&id, input_now()).await
            };
            match attempt {
                Ok(Some(outcome)) => return Ok(Some(outcome)),
                Ok(None) | Err(CatalogError::CasFailed { .. }) => {}
                Err(CatalogError::PreconditionFailed { .. }) => {
                    // Abandonment itself authenticates lifetime, protection and
                    // selected status; unresolved publication cannot be abandoned.
                    worker.abandon_at(&id, input_now()).await?;
                    invalidated = true;
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        if !invalidated {
            return Err(CatalogError::CasFailed {
                message: "bounded maintenance driver exhausted retries for the same job".into(),
            });
        }
    }
    Err(CatalogError::CasFailed {
        message: "four maintenance jobs were invalidated".into(),
    })
}

fn input_now() -> chrono::DateTime<chrono::Utc> {
    #[cfg(feature = "test-utils")]
    {
        arco_core::test_inputs::now()
    }
    #[cfg(not(feature = "test-utils"))]
    {
        chrono::Utc::now()
    }
}
