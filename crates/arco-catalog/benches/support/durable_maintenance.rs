use arco_catalog::{
    CatalogError, ControlMvpMaintenanceOutcome, DurableMaintenanceWorker, MaintenanceStatus,
};

// Shared fixture orchestration uses only the production worker's explicit operations.
// Bounds are unchanged: four invalidated jobs and 512 invocations per job.
pub async fn consolidate_pending(
    worker: &DurableMaintenanceWorker,
) -> Result<Option<ControlMvpMaintenanceOutcome>, CatalogError> {
    drive_pending(worker, input_now(), |worker, now| {
        Box::pin(worker.prepare_at(now))
    })
    .await
}

/// Drives one `RetentionHorizon` job to publication at `now` with the same
/// bounds and invalidation handling as `consolidate_pending`. Returns `None`
/// when nothing on the root is purge-eligible.
#[allow(dead_code, reason = "not every fixture drives both job kinds")]
pub async fn horizon_pending(
    worker: &DurableMaintenanceWorker,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<ControlMvpMaintenanceOutcome>, CatalogError> {
    drive_pending(worker, now, |worker, now| {
        Box::pin(worker.prepare_horizon_at(now))
    })
    .await
}

/// The admission step [`drive_pending`] repeats for each job it drives.
pub type PrepareFuture<'a> = std::pin::Pin<
    Box<
        dyn Future<Output = Result<Option<arco_catalog::PreparedMaintenance>, CatalogError>>
            + Send
            + 'a,
    >,
>;

/// Drives jobs admitted by `prepare` until one publishes: at most four jobs,
/// each within 512 invocations. A job refused with `PreconditionFailed` is
/// invalidated (abandoned unless the refusal itself recorded it terminal)
/// and the next plan is prepared. `Ok(None)` when `prepare` admits nothing.
pub async fn drive_pending(
    worker: &DurableMaintenanceWorker,
    now: chrono::DateTime<chrono::Utc>,
    prepare: impl for<'a> Fn(
        &'a DurableMaintenanceWorker,
        chrono::DateTime<chrono::Utc>,
    ) -> PrepareFuture<'a>,
) -> Result<Option<ControlMvpMaintenanceOutcome>, CatalogError> {
    for _ in 0..4 {
        let Some(plan) = prepare(worker, now).await? else {
            return Ok(None);
        };
        let id = plan.job_id().clone();
        let mut progress = Box::pin(worker.start_at(&plan, now)).await?;
        let mut invalidated = false;
        for _ in 0..512 {
            let attempt = if progress.status == MaintenanceStatus::Active {
                worker.advance_at(&id, now).await.map(|next| {
                    progress = next;
                    None
                })
            } else {
                worker.publish_at(&id, now).await
            };
            match attempt {
                Ok(Some(outcome)) => return Ok(Some(outcome)),
                Ok(None) | Err(CatalogError::CasFailed { .. }) => {}
                Err(CatalogError::PreconditionFailed { .. }) => {
                    // A refusal that is permanent for the job (a horizon whose
                    // purged set a later commit rewrote) has already recorded
                    // it `Superseded`; abandoning a terminal job is refused.
                    // Every other job is abandoned exactly as before: a resume
                    // that fails (the source was consumed) says nothing about
                    // the status, and abandonment itself authenticates
                    // lifetime, protection and selected status; unresolved
                    // publication cannot be abandoned.
                    let status = worker.resume_at(&id, now).await.ok().map(|p| p.status);
                    if !matches!(
                        status,
                        Some(
                            MaintenanceStatus::Failed
                                | MaintenanceStatus::Superseded
                                | MaintenanceStatus::Abandoned
                        )
                    ) {
                        worker.abandon_at(&id, now).await?;
                    }
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
