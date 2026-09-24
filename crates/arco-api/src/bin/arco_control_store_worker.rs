//! Scheduled `control/` maintenance worker.
//!
//! One invocation inspects the workspace retention epoch, then for every
//! control domain runs durable layout maintenance (L0 consolidation), then
//! drains the catalog projection outbox, then runs one bounded pass of
//! conservative garbage collection per domain. The process exits `0` when
//! every phase either completed or deferred to the next run, and non-zero when
//! any phase failed with a typed error or the retention epoch is stuck.
//!
//! Maintenance runs before the drain because every drained record commits
//! ack-domain L0 segments; consolidating first keeps a backlog from pushing the
//! drain into commit backpressure before it can make progress.
//!
//! The job must run under the API service account: that account is the sole
//! writer of the `control/` prefix. The prepared maintenance job identity is
//! persisted under `locks/` (never `control/`, whose GC treats foreign objects
//! as orphans) before activation, so a kill between activation and settlement
//! is replayed by exact identity on the next run instead of leaving the
//! retention epoch in flight forever.

use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use arco_catalog::retention_coordination::{
    RETENTION_MUTATION_EPOCH_PATH, RetentionMutationKind, STALE_RECLAMATION_EPOCH_MIN_AGE_SECS,
};
use arco_catalog::state_store::projection_outbox_acks::{
    PROJECTION_OUTBOX_ACK_DOMAIN, ProjectionMaterializationStatus, ProjectionOutboxWorker,
};
use arco_catalog::{
    CATALOG_PARQUET_PROJECTION_CONSUMER_ID, CatalogError, CatalogProjectionMaterializer,
    ControlMvpMaintenanceWorker, DurableAuthorityBinding, DurableMaintenanceWorker,
    MaintenanceJobId, MaintenanceProgress, MaintenanceStatus, StateScope,
};
use arco_core::observability::{LogFormat, init_logging};
use arco_core::{ScopedStorage, WritePrecondition};
use base64::Engine as _;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Control domains maintained by every run, in execution order.
const CONTROL_DOMAINS: [&str; 2] = ["catalog", PROJECTION_OUTBOX_ACK_DOMAIN];
const DEFAULT_GC_MAX_PAGES: usize = 16;
const DEFAULT_MAINTENANCE_MAX_ADVANCES: usize = 4096;
/// Scope-relative prefix of the persisted per-domain maintenance job identity.
const SELECTED_JOB_PREFIX: &str = "locks/control-store-worker/";
/// The kernel admits a maintenance descriptor for 24 hours; after that the job
/// can be root-recovered but never resumed or published.
const MAINTENANCE_JOB_LIFETIME_MS: i64 = 24 * 60 * 60 * 1000;

/// Per-run work bounds.
#[derive(Debug, Clone, Copy)]
struct RunLimits {
    /// Maximum GC pages collected per domain per run.
    gc_max_pages: usize,
    /// Maximum `advance_at` calls per maintenance job per run.
    maintenance_max_advances: usize,
}

// ---------------------------------------------------------------------------
// Persisted maintenance job identity
// ---------------------------------------------------------------------------

/// The identity the kernel requires callers to persist before `start_at`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SelectedJobRecord {
    job_id: String,
    domain: String,
    prepared_at_ms: i64,
}

fn selected_job_path(domain: &str) -> String {
    format!("{SELECTED_JOB_PREFIX}{domain}/selected-job.json")
}

async fn load_selected_job(
    storage: &ScopedStorage,
    domain: &str,
) -> Result<Option<SelectedJobRecord>> {
    let path = selected_job_path(domain);
    if storage.head_raw(&path).await?.is_none() {
        return Ok(None);
    }
    let bytes = storage.get_raw(&path).await?;
    let record: SelectedJobRecord = serde_json::from_slice(&bytes)
        .with_context(|| format!("persisted maintenance job record at {path} is corrupt"))?;
    if record.domain != domain {
        bail!(
            "persisted maintenance job record at {path} names domain {}",
            record.domain
        );
    }
    Ok(Some(record))
}

async fn persist_selected_job(storage: &ScopedStorage, record: &SelectedJobRecord) -> Result<()> {
    let bytes = Bytes::from(serde_json::to_vec(record)?);
    storage
        .put_raw(
            &selected_job_path(&record.domain),
            bytes,
            WritePrecondition::None,
        )
        .await
        .with_context(|| format!("persist maintenance job id for domain {}", record.domain))?;
    Ok(())
}

async fn clear_selected_job(storage: &ScopedStorage, domain: &str) -> Result<()> {
    match storage.delete(&selected_job_path(domain)).await {
        Ok(()) | Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
            Ok(())
        }
        Err(error) => {
            Err(error).with_context(|| format!("clear maintenance job id for domain {domain}"))
        }
    }
}

// ---------------------------------------------------------------------------
// Retention epoch backstop
// ---------------------------------------------------------------------------

/// The fields of the workspace retention mutation epoch record this worker
/// reads. The kernel exposes no read API for the record, so unknown fields and
/// unknown operation kinds are tolerated rather than rejected.
#[derive(Debug, Clone, Deserialize)]
struct RetentionEpochRecord {
    epoch: u64,
    state: String,
    holder_id: String,
    operation_kind: String,
    operation_id: String,
    started_at: DateTime<Utc>,
}

const EPOCH_STATE_IN_FLIGHT: &str = "IN_FLIGHT";

fn kind_name(kind: RetentionMutationKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}

async fn read_epoch_record(storage: &ScopedStorage) -> Result<Option<RetentionEpochRecord>> {
    if storage
        .head_raw(RETENTION_MUTATION_EPOCH_PATH)
        .await?
        .is_none()
    {
        return Ok(None);
    }
    let bytes = storage.get_raw(RETENTION_MUTATION_EPOCH_PATH).await?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .context("retention mutation epoch record is undecodable")
}

/// Classification of the workspace retention epoch at the start of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum EpochOutcome {
    /// No epoch record exists yet.
    Absent,
    /// The last epoch settled.
    Idle,
    /// An epoch is in flight but young, or a `ControlGc` epoch the kernel adopts itself.
    InFlight,
    /// A stale maintenance-root epoch whose job id this worker persisted; the
    /// maintenance phase replays it by exact identity.
    Recoverable,
    /// A stale non-`ControlGc` epoch nothing can replay: every maintenance
    /// activation and GC page fails closed until an operator settles it.
    StuckEpoch,
}

fn classify_epoch(
    record: Option<&RetentionEpochRecord>,
    recoverable_job_ids: &[String],
    now: DateTime<Utc>,
) -> EpochOutcome {
    let Some(record) = record else {
        return EpochOutcome::Absent;
    };
    if record.state != EPOCH_STATE_IN_FLIGHT {
        return EpochOutcome::Idle;
    }
    let age_secs = now.signed_duration_since(record.started_at).num_seconds();
    if record.operation_kind == kind_name(RetentionMutationKind::ControlGc)
        || age_secs < STALE_RECLAMATION_EPOCH_MIN_AGE_SECS
    {
        return EpochOutcome::InFlight;
    }
    if record.operation_kind == kind_name(RetentionMutationKind::MaintenanceRootPublish)
        && recoverable_job_ids.contains(&record.operation_id)
    {
        return EpochOutcome::Recoverable;
    }
    EpochOutcome::StuckEpoch
}

#[derive(Debug, Serialize)]
struct EpochSummary {
    outcome: EpochOutcome,
    epoch: Option<u64>,
    operation_kind: Option<String>,
    operation_id: Option<String>,
    holder_id: Option<String>,
    in_flight_for_secs: Option<i64>,
    elapsed_ms: u64,
}

impl EpochSummary {
    fn log(&self) {
        tracing::info!(
            phase = "epoch",
            domain = "workspace",
            outcome = ?self.outcome,
            epoch = self.epoch,
            operation_kind = self.operation_kind.as_deref(),
            operation_id = self.operation_id.as_deref(),
            holder_id = self.holder_id.as_deref(),
            in_flight_for_secs = self.in_flight_for_secs,
            elapsed_ms = self.elapsed_ms,
            "control-store worker phase complete"
        );
    }
}

async fn inspect_retention_epoch(storage: &ScopedStorage) -> Result<EpochSummary> {
    let started = Instant::now();
    let now = Utc::now();
    let record = read_epoch_record(storage).await?;
    let mut recoverable = Vec::new();
    for domain in CONTROL_DOMAINS {
        if let Some(selected) = load_selected_job(storage, domain).await? {
            recoverable.push(selected.job_id);
        }
    }
    let outcome = classify_epoch(record.as_ref(), &recoverable, now);
    let in_flight = record
        .as_ref()
        .filter(|record| record.state == EPOCH_STATE_IN_FLIGHT);
    Ok(EpochSummary {
        outcome,
        epoch: record.as_ref().map(|record| record.epoch),
        operation_kind: in_flight.map(|record| record.operation_kind.clone()),
        operation_id: in_flight.map(|record| record.operation_id.clone()),
        holder_id: in_flight.map(|record| record.holder_id.clone()),
        in_flight_for_secs: in_flight
            .map(|record| now.signed_duration_since(record.started_at).num_seconds()),
        elapsed_ms: elapsed_ms(started),
    })
}

// ---------------------------------------------------------------------------
// Phase summaries
// ---------------------------------------------------------------------------

/// Terminal classification of one domain's maintenance phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum MaintenanceOutcome {
    /// The current manifest selects no maintenance intent.
    Idle,
    /// A layout was published by exact head CAS.
    Published,
    /// Coordination or a consumed publication deferred the job to the next run.
    Deferred,
    /// The job reached a terminal `Failed`, `Superseded` or `Abandoned` state.
    Terminal,
    /// The advance budget was exhausted before the job was ready to publish.
    Exhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DrainOutcome {
    /// Every pending record was materialized and acknowledged.
    Ok,
    /// Ack-domain commit backpressure stopped the drain; acknowledged records
    /// are durable and the next run continues after maintenance consolidates.
    Deferred,
}

#[derive(Debug, Serialize)]
struct DrainSummary {
    outcome: DrainOutcome,
    drained_records: usize,
    quarantined_records: usize,
    already_acknowledged: usize,
    /// Records still unacknowledged after this pass, when the backlog was readable.
    pending_records: Option<usize>,
    latest_projected_sequence: Option<u64>,
    applied_authority_sequence: Option<u64>,
    observed_authority_sequence: Option<u64>,
    /// `observed - applied`, when both are known.
    lag: Option<u64>,
    /// Seconds since the last successful materialization, when known.
    age_secs: Option<i64>,
    elapsed_ms: u64,
}

impl DrainSummary {
    fn log(&self) {
        tracing::info!(
            phase = "drain",
            domain = "catalog",
            outcome = ?self.outcome,
            drained_records = self.drained_records,
            quarantined_records = self.quarantined_records,
            already_acknowledged = self.already_acknowledged,
            pending_records = self.pending_records,
            latest_projected_sequence = self.latest_projected_sequence,
            applied_authority_sequence = self.applied_authority_sequence,
            observed_authority_sequence = self.observed_authority_sequence,
            lag = self.lag,
            age_secs = self.age_secs,
            elapsed_ms = self.elapsed_ms,
            "control-store worker phase complete"
        );
    }
}

#[derive(Debug, Serialize)]
struct DomainMaintenanceSummary {
    domain: String,
    outcome: MaintenanceOutcome,
    job_id: Option<String>,
    /// The job was replayed from the persisted identity of an earlier run.
    recovered: bool,
    advances: usize,
    completed: usize,
    total: usize,
    layout_generation: Option<u64>,
    elapsed_ms: u64,
}

impl DomainMaintenanceSummary {
    fn idle(domain: &str) -> Self {
        Self {
            domain: domain.to_owned(),
            outcome: MaintenanceOutcome::Idle,
            job_id: None,
            recovered: false,
            advances: 0,
            completed: 0,
            total: 0,
            layout_generation: None,
            elapsed_ms: 0,
        }
    }

    fn log(&self) {
        tracing::info!(
            phase = "maintenance",
            domain = %self.domain,
            outcome = ?self.outcome,
            job_id = self.job_id.as_deref(),
            recovered = self.recovered,
            advances = self.advances,
            completed = self.completed,
            total = self.total,
            layout_generation = self.layout_generation,
            elapsed_ms = self.elapsed_ms,
            "control-store worker phase complete"
        );
    }
}

#[derive(Debug, Serialize)]
struct DomainGcSummary {
    domain: String,
    pages: usize,
    objects_deleted: u64,
    bytes_reclaimed: u64,
    /// A continuation cursor remained after the page budget; it is not persisted.
    truncated: bool,
    elapsed_ms: u64,
}

impl DomainGcSummary {
    fn log(&self) {
        tracing::info!(
            phase = "gc",
            domain = %self.domain,
            outcome = "ok",
            pages = self.pages,
            objects_deleted = self.objects_deleted,
            bytes_reclaimed = self.bytes_reclaimed,
            truncated = self.truncated,
            elapsed_ms = self.elapsed_ms,
            "control-store worker phase complete"
        );
    }
}

/// Everything one invocation did. Phases that failed are absent from their
/// collection and recorded in `failures`.
#[derive(Debug, Default, Serialize)]
struct RunSummary {
    epoch: Option<EpochSummary>,
    maintenance: Vec<DomainMaintenanceSummary>,
    drain: Option<DrainSummary>,
    gc: Vec<DomainGcSummary>,
    failures: Vec<String>,
}

impl RunSummary {
    fn fail(&mut self, phase: &'static str, domain: &str, error: &anyhow::Error) {
        tracing::error!(
            phase,
            domain,
            outcome = "error",
            error = %error,
            "control-store worker phase failed"
        );
        self.failures.push(format!("{phase}[{domain}]: {error:#}"));
    }

    /// The process exit error, when any phase failed.
    fn exit_error(&self) -> Option<anyhow::Error> {
        if self.failures.is_empty() {
            None
        } else {
            Some(anyhow!(
                "{} control-store phase(s) failed: {}",
                self.failures.len(),
                self.failures.join("; ")
            ))
        }
    }
}

/// Errors that mean another actor holds or consumed the source; safe to retry
/// on the next scheduled run without operator action.
fn is_deferrable(error: &CatalogError) -> bool {
    matches!(
        error,
        CatalogError::PreconditionFailed { .. } | CatalogError::CasFailed { .. }
    )
}

#[allow(clippy::cast_possible_truncation)] // Millisecond elapsed times fit u64 for any job lifetime.
fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}

// ---------------------------------------------------------------------------
// Drain
// ---------------------------------------------------------------------------

async fn drain_catalog_projection(storage: ScopedStorage) -> Result<DrainSummary> {
    let started = Instant::now();
    let materializer = CatalogProjectionMaterializer::new(storage.clone())
        .context("construct catalog projection materializer")?;
    let (outcome, report) = match materializer.drain_once().await {
        Ok(report) => (DrainOutcome::Ok, Some(report)),
        Err(CatalogError::MaintenanceBackpressure { message }) => {
            tracing::warn!(
                phase = "drain",
                domain = "catalog",
                error = %message,
                "drain stopped by ack-domain commit backpressure; acknowledged records are durable and the next run continues after maintenance"
            );
            (DrainOutcome::Deferred, None)
        }
        Err(error) => return Err(error).context("drain catalog projection outbox"),
    };
    let status = materializer
        .status()
        .await
        .context("read catalog projection status")?;
    let pending_records = pending_catalog_records(storage).await;
    let freshness = ProjectionFreshness::from_status(status.as_ref());
    if let Some(failure) = status.as_ref().and_then(|s| s.failure_state()) {
        tracing::warn!(
            phase = "drain",
            domain = "catalog",
            failure_state = failure,
            "catalog projection status carries a failure state"
        );
    }
    Ok(DrainSummary {
        outcome,
        drained_records: report.as_ref().map_or(0, |r| r.drained_record_ids.len()),
        quarantined_records: report
            .as_ref()
            .map_or(0, |r| r.quarantined_record_ids.len()),
        already_acknowledged: report.as_ref().map_or(0, |r| r.already_acknowledged),
        pending_records,
        latest_projected_sequence: report.as_ref().and_then(|r| r.latest_projected_sequence),
        applied_authority_sequence: freshness.applied,
        observed_authority_sequence: freshness.observed,
        lag: freshness.lag,
        age_secs: freshness.age_secs,
        elapsed_ms: elapsed_ms(started),
    })
}

/// Counts still-pending catalog projection records; unavailability is logged,
/// not fatal, because the drain outcome is already known.
async fn pending_catalog_records(storage: ScopedStorage) -> Option<usize> {
    let backlog =
        ProjectionOutboxWorker::new(storage, "catalog", CATALOG_PARQUET_PROJECTION_CONSUMER_ID)
            .map_err(anyhow::Error::from);
    let backlog = match backlog {
        Ok(worker) => worker.backlog().await.map_err(anyhow::Error::from),
        Err(error) => Err(error),
    };
    match backlog {
        Ok(backlog) => Some(backlog.pending_record_ids.len()),
        Err(error) => {
            tracing::warn!(phase = "drain", domain = "catalog", error = %error, "projection backlog unavailable");
            None
        }
    }
}

/// Applied/observed sequences and derived lag/age from the durable status.
struct ProjectionFreshness {
    applied: Option<u64>,
    observed: Option<u64>,
    lag: Option<u64>,
    age_secs: Option<i64>,
}

impl ProjectionFreshness {
    fn from_status(status: Option<&ProjectionMaterializationStatus>) -> Self {
        let applied = status.and_then(ProjectionMaterializationStatus::applied_authority_sequence);
        let observed =
            status.and_then(ProjectionMaterializationStatus::observed_authority_sequence);
        let lag = match (observed, applied) {
            (Some(observed), Some(applied)) => Some(observed.saturating_sub(applied)),
            _ => None,
        };
        let age_secs = status
            .and_then(ProjectionMaterializationStatus::last_success_at_ms)
            .map(|last_ms| (Utc::now().timestamp_millis().saturating_sub(last_ms)) / 1000);
        Self {
            applied,
            observed,
            lag,
            age_secs,
        }
    }
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

/// Outcome of one kernel step: either progress, or a deferral to the next run.
enum Step<T> {
    Ready(T),
    Deferred,
}

fn classify_step<T>(
    domain: &str,
    step: &'static str,
    result: arco_catalog::Result<T>,
) -> Result<Step<T>> {
    match result {
        Ok(value) => Ok(Step::Ready(value)),
        Err(error) if is_deferrable(&error) => {
            tracing::warn!(
                phase = "maintenance",
                domain,
                step,
                error = %error,
                "maintenance step deferred to the next run"
            );
            Ok(Step::Deferred)
        }
        Err(error) => Err(error).with_context(|| format!("maintenance {step} for domain {domain}")),
    }
}

/// Result of driving a job's construction loop.
enum Advance {
    /// Every output has a receipt; the job may publish.
    Ready,
    /// The loop stopped without reaching publication.
    Stopped(MaintenanceOutcome),
}

async fn advance_until_ready(
    worker: &DurableMaintenanceWorker,
    job_id: &MaintenanceJobId,
    mut progress: MaintenanceProgress,
    max_advances: usize,
    summary: &mut DomainMaintenanceSummary,
) -> Result<Advance> {
    loop {
        summary.completed = progress.completed;
        summary.total = progress.total;
        match progress.status {
            MaintenanceStatus::ReadyToPublish
            | MaintenanceStatus::Publishing
            | MaintenanceStatus::Published => return Ok(Advance::Ready),
            MaintenanceStatus::Failed
            | MaintenanceStatus::Superseded
            | MaintenanceStatus::Abandoned => {
                tracing::warn!(
                    phase = "maintenance",
                    domain = %summary.domain,
                    job_id = job_id.as_str(),
                    status = ?progress.status,
                    "maintenance job is terminal; a fresh plan is prepared next run"
                );
                return Ok(Advance::Stopped(MaintenanceOutcome::Terminal));
            }
            MaintenanceStatus::Planned | MaintenanceStatus::Active => {}
        }
        if summary.advances >= max_advances {
            tracing::warn!(
                phase = "maintenance",
                domain = %summary.domain,
                job_id = job_id.as_str(),
                advances = summary.advances,
                completed = summary.completed,
                total = summary.total,
                "maintenance advance budget exhausted; job resumes next run"
            );
            return Ok(Advance::Stopped(MaintenanceOutcome::Exhausted));
        }
        summary.advances += 1;
        progress = match classify_step(
            &summary.domain,
            "advance",
            worker.advance_at(job_id, Utc::now()).await,
        )? {
            Step::Ready(progress) => progress,
            Step::Deferred => return Ok(Advance::Stopped(MaintenanceOutcome::Deferred)),
        };
    }
}

async fn publish_job(
    worker: &DurableMaintenanceWorker,
    job_id: &MaintenanceJobId,
    summary: &mut DomainMaintenanceSummary,
) -> Result<MaintenanceOutcome> {
    match classify_step(
        &summary.domain,
        "publish",
        worker.publish_at(job_id, Utc::now()).await,
    )? {
        Step::Ready(Some(outcome)) => {
            summary.layout_generation = Some(outcome.layout_generation());
            Ok(MaintenanceOutcome::Published)
        }
        Step::Ready(None) => {
            tracing::warn!(
                phase = "maintenance",
                domain = %summary.domain,
                job_id = job_id.as_str(),
                "maintenance publication was consumed by another publication; retry next run"
            );
            Ok(MaintenanceOutcome::Deferred)
        }
        Step::Deferred => Ok(MaintenanceOutcome::Deferred),
    }
}

/// Drives an activated job to publication and clears its persisted identity
/// once nothing remains to replay.
async fn finish_job(
    storage: &ScopedStorage,
    worker: &DurableMaintenanceWorker,
    job_id: &MaintenanceJobId,
    progress: MaintenanceProgress,
    max_advances: usize,
    summary: &mut DomainMaintenanceSummary,
) -> Result<MaintenanceOutcome> {
    let outcome = match advance_until_ready(worker, job_id, progress, max_advances, summary).await?
    {
        Advance::Ready => publish_job(worker, job_id, summary).await?,
        Advance::Stopped(outcome) => outcome,
    };
    if matches!(
        outcome,
        MaintenanceOutcome::Published | MaintenanceOutcome::Terminal
    ) {
        clear_selected_job(storage, &summary.domain).await?;
    }
    Ok(outcome)
}

/// What replaying a persisted job identity produced.
enum Recovery {
    /// The job is live again; continue construction and publication.
    Resume(MaintenanceJobId, MaintenanceProgress),
    /// Coordination deferred the replay; the record stays for the next run.
    Deferred,
    /// The record was cleared; prepare a fresh plan.
    Fresh,
}

async fn recover_selected_job(
    storage: &ScopedStorage,
    worker: &DurableMaintenanceWorker,
    record: &SelectedJobRecord,
    summary: &mut DomainMaintenanceSummary,
) -> Result<Recovery> {
    let domain = summary.domain.clone();
    let job_id = MaintenanceJobId::parse(record.job_id.clone())
        .with_context(|| format!("persisted maintenance job id for domain {domain} is invalid"))?;
    summary.job_id = Some(record.job_id.clone());
    summary.recovered = true;
    let now = Utc::now();
    let age_ms = now.timestamp_millis().saturating_sub(record.prepared_at_ms);
    let expired = age_ms >= MAINTENANCE_JOB_LIFETIME_MS;
    tracing::info!(
        phase = "maintenance",
        domain = %domain,
        job_id = job_id.as_str(),
        age_ms,
        expired,
        "replaying persisted maintenance job activation"
    );
    // A job whose activation completed resumes directly. `resume_at` observes a
    // Publishing/Published attempt without re-checking source compatibility,
    // which a publication that already reached HEAD would otherwise fail
    // (publication bumps the layout generation). Activation is replayed only
    // when the job is not directly resumable, e.g. its selector never landed.
    if !expired {
        match worker.resume_at(&job_id, now).await {
            Ok(progress) => return Ok(Recovery::Resume(job_id, progress)),
            Err(error)
                if is_deferrable(&error) || matches!(error, CatalogError::NotFound { .. }) =>
            {
                tracing::info!(
                    phase = "maintenance",
                    domain = %domain,
                    job_id = job_id.as_str(),
                    error = %error,
                    "persisted maintenance job is not directly resumable; replaying activation"
                );
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("maintenance resume for domain {domain}"));
            }
        }
    }
    let disposition = match worker.recover_activation_at(&job_id, now).await {
        Ok(_) if !expired => return resume_recovered_job(worker, &domain, job_id).await,
        Ok(_) => RecoveryDisposition::Abandon(
            "persisted maintenance job exceeded its lifetime; its root was recovered and a fresh plan follows",
        ),
        Err(CatalogError::NotFound { .. }) => RecoveryDisposition::Abandon(
            "persisted maintenance job has no durable descriptor; activation never landed and a fresh plan follows",
        ),
        Err(error) if is_deferrable(&error) && !expired => RecoveryDisposition::Defer(error),
        Err(error) if is_deferrable(&error) => RecoveryDisposition::AbandonAfterError(error),
        Err(error) => {
            return Err(error).with_context(|| format!("maintenance recovery for domain {domain}"));
        }
    };
    apply_recovery_disposition(storage, &domain, &job_id, disposition).await
}

/// How a replayed activation that did not resume should be handled.
enum RecoveryDisposition {
    /// Keep the record; coordination deferred the replay to the next run.
    Defer(CatalogError),
    /// Clear the record and prepare a fresh plan.
    Abandon(&'static str),
    /// The job is expired and its replay was refused; clear the record so a
    /// retention epoch it still holds surfaces as `stuck_epoch` next run.
    AbandonAfterError(CatalogError),
}

/// Resumes a successfully replayed, unexpired job.
async fn resume_recovered_job(
    worker: &DurableMaintenanceWorker,
    domain: &str,
    job_id: MaintenanceJobId,
) -> Result<Recovery> {
    match classify_step(
        domain,
        "resume",
        worker.resume_at(&job_id, Utc::now()).await,
    )? {
        Step::Ready(progress) => Ok(Recovery::Resume(job_id, progress)),
        Step::Deferred => Ok(Recovery::Deferred),
    }
}

async fn apply_recovery_disposition(
    storage: &ScopedStorage,
    domain: &str,
    job_id: &MaintenanceJobId,
    disposition: RecoveryDisposition,
) -> Result<Recovery> {
    match disposition {
        RecoveryDisposition::Defer(error) => {
            tracing::warn!(
                phase = "maintenance",
                domain,
                job_id = job_id.as_str(),
                error = %error,
                "persisted maintenance job replay deferred to the next run"
            );
            Ok(Recovery::Deferred)
        }
        RecoveryDisposition::Abandon(reason) => {
            tracing::warn!(
                phase = "maintenance",
                domain,
                job_id = job_id.as_str(),
                reason
            );
            clear_selected_job(storage, domain).await?;
            Ok(Recovery::Fresh)
        }
        RecoveryDisposition::AbandonAfterError(error) => {
            tracing::warn!(
                phase = "maintenance",
                domain,
                job_id = job_id.as_str(),
                error = %error,
                "expired persisted maintenance job could not be replayed; abandoning its record (a retention epoch it still holds is reported as stuck_epoch next run)"
            );
            clear_selected_job(storage, domain).await?;
            Ok(Recovery::Fresh)
        }
    }
}

/// Replays a persisted job if one exists, otherwise prepares, persists, starts
/// and drives at most one new maintenance job.
async fn drive_maintenance(
    storage: &ScopedStorage,
    worker: &DurableMaintenanceWorker,
    max_advances: usize,
    summary: &mut DomainMaintenanceSummary,
) -> Result<MaintenanceOutcome> {
    if let Some(record) = load_selected_job(storage, &summary.domain).await? {
        match recover_selected_job(storage, worker, &record, summary).await? {
            Recovery::Resume(job_id, progress) => {
                return finish_job(storage, worker, &job_id, progress, max_advances, summary).await;
            }
            Recovery::Deferred => return Ok(MaintenanceOutcome::Deferred),
            Recovery::Fresh => {
                summary.recovered = false;
                summary.job_id = None;
            }
        }
    }
    let plan = match classify_step(
        &summary.domain,
        "prepare",
        worker.prepare_at(Utc::now()).await,
    )? {
        Step::Ready(Some(plan)) => plan,
        Step::Ready(None) => return Ok(MaintenanceOutcome::Idle),
        Step::Deferred => return Ok(MaintenanceOutcome::Deferred),
    };
    let job_id = plan.job_id().clone();
    // The kernel requires the identity to be durable before activation so an
    // interrupted activation can be replayed exactly instead of orphaned.
    persist_selected_job(
        storage,
        &SelectedJobRecord {
            job_id: job_id.as_str().to_owned(),
            domain: summary.domain.clone(),
            prepared_at_ms: Utc::now().timestamp_millis(),
        },
    )
    .await?;
    summary.job_id = Some(job_id.as_str().to_owned());
    tracing::info!(
        phase = "maintenance",
        domain = %summary.domain,
        job_id = job_id.as_str(),
        "maintenance job prepared and its identity persisted"
    );
    let progress = match classify_step(
        &summary.domain,
        "start",
        worker.start_at(&plan, Utc::now()).await,
    )? {
        Step::Ready(progress) => progress,
        Step::Deferred => return Ok(MaintenanceOutcome::Deferred),
    };
    finish_job(storage, worker, &job_id, progress, max_advances, summary).await
}

async fn maintain_domain(
    storage: ScopedStorage,
    tenant: &str,
    workspace: &str,
    domain: &str,
    binding: DurableAuthorityBinding,
    max_advances: usize,
) -> Result<DomainMaintenanceSummary> {
    let started = Instant::now();
    let scope = StateScope::new(tenant, workspace, domain);
    let worker = DurableMaintenanceWorker::new(storage.clone(), scope, binding)
        .with_context(|| format!("construct maintenance worker for domain {domain}"))?;
    let mut summary = DomainMaintenanceSummary::idle(domain);
    let outcome = drive_maintenance(&storage, &worker, max_advances, &mut summary).await?;
    summary.outcome = outcome;
    summary.elapsed_ms = elapsed_ms(started);
    Ok(summary)
}

// ---------------------------------------------------------------------------
// GC
// ---------------------------------------------------------------------------

async fn collect_domain(
    storage: ScopedStorage,
    tenant: &str,
    workspace: &str,
    domain: &str,
    max_pages: usize,
) -> Result<DomainGcSummary> {
    let started = Instant::now();
    let scope = StateScope::new(tenant, workspace, domain);
    let collector = ControlMvpMaintenanceWorker::new(storage, scope)
        .with_context(|| format!("construct GC worker for domain {domain}"))?;
    let mut cursor: Option<String> = None;
    let mut pages = 0_usize;
    let mut objects_deleted = 0_u64;
    let mut bytes_reclaimed = 0_u64;
    while pages < max_pages {
        let page = match collector
            .collect_gc_page_at(Utc::now(), Vec::<String>::new(), cursor.as_deref())
            .await
        {
            Ok(page) => page,
            Err(error) if is_deferrable(&error) => {
                tracing::warn!(
                    phase = "gc",
                    domain,
                    page = pages,
                    error = %error,
                    "gc page deferred to the next run"
                );
                break;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("collect GC page for domain {domain}"));
            }
        };
        pages += 1;
        objects_deleted = objects_deleted.saturating_add(page.objects_deleted());
        bytes_reclaimed = bytes_reclaimed.saturating_add(page.bytes_reclaimed());
        cursor = page.continuation().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    Ok(DomainGcSummary {
        domain: domain.to_owned(),
        pages,
        objects_deleted,
        bytes_reclaimed,
        truncated: cursor.is_some(),
        elapsed_ms: elapsed_ms(started),
    })
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

/// Runs every phase once: epoch inspection, maintenance per domain, catalog
/// projection drain, GC per domain. Phases are independent: a failure is
/// recorded and the remaining phases still run, so one wedged domain never
/// starves another. The returned summary carries every failure; callers exit
/// non-zero through [`RunSummary::exit_error`].
async fn run_once(
    storage: ScopedStorage,
    tenant: &str,
    workspace: &str,
    binding: DurableAuthorityBinding,
    limits: RunLimits,
) -> Result<RunSummary> {
    let started = Instant::now();
    let mut summary = RunSummary::default();

    match inspect_retention_epoch(&storage).await {
        Ok(epoch) => {
            epoch.log();
            if epoch.outcome == EpochOutcome::StuckEpoch {
                summary.fail(
                    "epoch",
                    "workspace",
                    &anyhow!(
                        "retention mutation epoch {} ({} by {}) has been in flight for {}s with no persisted job to replay it; settle it with recover_stale_retention_epoch",
                        epoch.epoch.unwrap_or_default(),
                        epoch.operation_kind.as_deref().unwrap_or("unknown"),
                        epoch.holder_id.as_deref().unwrap_or("unknown"),
                        epoch.in_flight_for_secs.unwrap_or_default()
                    ),
                );
            }
            summary.epoch = Some(epoch);
        }
        Err(error) => summary.fail("epoch", "workspace", &error),
    }

    for domain in CONTROL_DOMAINS {
        match maintain_domain(
            storage.clone(),
            tenant,
            workspace,
            domain,
            binding,
            limits.maintenance_max_advances,
        )
        .await
        {
            Ok(result) => {
                result.log();
                summary.maintenance.push(result);
            }
            Err(error) => summary.fail("maintenance", domain, &error),
        }
    }

    match drain_catalog_projection(storage.clone()).await {
        Ok(drain) => {
            drain.log();
            summary.drain = Some(drain);
        }
        Err(error) => summary.fail("drain", "catalog", &error),
    }

    for domain in CONTROL_DOMAINS {
        match collect_domain(
            storage.clone(),
            tenant,
            workspace,
            domain,
            limits.gc_max_pages,
        )
        .await
        {
            Ok(result) => {
                result.log();
                summary.gc.push(result);
            }
            Err(error) => summary.fail("gc", domain, &error),
        }
    }

    let failed = summary.failures.len();
    tracing::info!(
        phase = "run",
        outcome = if failed == 0 { "ok" } else { "error" },
        failures = failed,
        elapsed_ms = elapsed_ms(started),
        "control-store worker run complete"
    );
    Ok(summary)
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

fn required_env(name: &str) -> Result<String> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value.trim().to_owned()),
        _ => bail!("{name} is required"),
    }
}

fn usize_env(name: &str, default: usize) -> Result<usize> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => {
            let parsed = value
                .trim()
                .parse::<usize>()
                .with_context(|| format!("{name} must be a positive integer"))?;
            if parsed == 0 {
                bail!("{name} must be greater than zero");
            }
            Ok(parsed)
        }
        _ => Ok(default),
    }
}

/// Decodes the per-deployment durable authority binding: base64 of exactly
/// 32 bytes. It must never change for a live root.
fn decode_binding(encoded: &str) -> Result<DurableAuthorityBinding> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim().as_bytes())
        .context("ARCO_CONTROL_STORE_MAINTENANCE_BINDING must be standard base64")?;
    let identity: [u8; 32] = decoded.as_slice().try_into().map_err(|_| {
        anyhow!(
            "ARCO_CONTROL_STORE_MAINTENANCE_BINDING must decode to exactly 32 bytes, got {}",
            decoded.len()
        )
    })?;
    Ok(DurableAuthorityBinding::new(identity))
}

fn log_format_from_env() -> LogFormat {
    match std::env::var("ARCO_LOG_FORMAT") {
        Ok(value) if value.eq_ignore_ascii_case("json") => LogFormat::Json,
        _ => LogFormat::Pretty,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging(log_format_from_env());
    let bucket = required_env("ARCO_STORAGE_BUCKET")?;
    let tenant = required_env("ARCO_CATALOG_CONTROL_V1_TENANT_ID")?;
    let workspace = required_env("ARCO_CATALOG_CONTROL_V1_WORKSPACE_ID")?;
    let binding = decode_binding(&required_env("ARCO_CONTROL_STORE_MAINTENANCE_BINDING")?)?;
    let limits = RunLimits {
        gc_max_pages: usize_env("ARCO_CONTROL_STORE_GC_MAX_PAGES", DEFAULT_GC_MAX_PAGES)?,
        maintenance_max_advances: usize_env(
            "ARCO_CONTROL_STORE_MAINTENANCE_MAX_ADVANCES",
            DEFAULT_MAINTENANCE_MAX_ADVANCES,
        )?,
    };
    let backend = arco_storage::from_bucket(&bucket)
        .with_context(|| format!("open storage bucket {bucket}"))?;
    let storage = ScopedStorage::new(backend, tenant.as_str(), workspace.as_str())
        .context("scope storage to the control root")?;
    tracing::info!(
        tenant = %tenant,
        workspace = %workspace,
        gc_max_pages = limits.gc_max_pages,
        maintenance_max_advances = limits.maintenance_max_advances,
        "control-store worker starting"
    );
    let summary = run_once(storage, &tenant, &workspace, binding, limits).await?;
    if let Some(error) = summary.exit_error() {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;

    use arco_catalog::{
        ArcoStateTxn as _, CatalogProjectionNotifier, ControlCatalogAuthority,
        ControlMvpStateStore, ProjectionIntentV1, TxnOptions, WriteOptions,
    };
    use arco_core::MemoryBackend;
    use chrono::Duration;

    use super::*;

    const BINDING: DurableAuthorityBinding = DurableAuthorityBinding::new([7; 32]);
    const TEST_LIMITS: RunLimits = RunLimits {
        gc_max_pages: 4,
        maintenance_max_advances: 512,
    };
    const TENANT: &str = "tenant";
    const WORKSPACE: &str = "workspace";

    /// The API's default notifier spawns a drain after every commit; tests
    /// need the backlog to stay put until the worker drains it.
    struct SilentNotifier;

    impl CatalogProjectionNotifier for SilentNotifier {
        fn notify(&self, _: &ProjectionIntentV1) -> arco_catalog::Result<()> {
            Ok(())
        }
    }

    fn test_storage() -> Result<ScopedStorage> {
        ScopedStorage::new(Arc::new(MemoryBackend::new()), TENANT, WORKSPACE).map_err(Into::into)
    }

    fn catalog_scope() -> StateScope {
        StateScope::new(TENANT, WORKSPACE, "catalog")
    }

    async fn run(storage: &ScopedStorage) -> Result<RunSummary> {
        run_once(storage.clone(), TENANT, WORKSPACE, BINDING, TEST_LIMITS).await
    }

    async fn commit_generation(store: &ControlMvpStateStore, generation: usize) -> Result<()> {
        let mut tx = store.begin_control_txn(TxnOptions::default()).await?;
        tx.put(b"key", Bytes::from(format!("generation-{generation}")))
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn seed_plain_commits(
        storage: &ScopedStorage,
        count: usize,
    ) -> Result<ControlMvpStateStore> {
        let store = ControlMvpStateStore::new(storage.clone(), catalog_scope())?;
        for generation in 0..count {
            commit_generation(&store, generation).await?;
        }
        Ok(store)
    }

    /// Seeds real catalog DDL commits, each staging one projection intent.
    async fn seed_catalog_intents(storage: &ScopedStorage, range: Range<usize>) -> Result<()> {
        let authority = ControlCatalogAuthority::new(storage.clone(), catalog_scope())?
            .with_projection_notifier(Arc::new(SilentNotifier));
        for index in range {
            authority
                .create_catalog(&format!("catalog-{index}"), None, WriteOptions::default())
                .await?;
        }
        Ok(())
    }

    /// Consolidates a domain directly through the kernel, without the worker.
    async fn consolidate(storage: &ScopedStorage, domain: &str) -> Result<()> {
        let worker = DurableMaintenanceWorker::new(
            storage.clone(),
            StateScope::new(TENANT, WORKSPACE, domain),
            BINDING,
        )?;
        let now = Utc::now();
        let Some(plan) = worker.prepare_at(now).await? else {
            return Ok(());
        };
        let mut progress = worker.start_at(&plan, now).await?;
        for _ in 0..512 {
            if progress.status == MaintenanceStatus::ReadyToPublish {
                break;
            }
            progress = worker.advance_at(plan.job_id(), now).await?;
        }
        worker
            .publish_at(plan.job_id(), now)
            .await?
            .ok_or_else(|| anyhow!("direct consolidation of {domain} was consumed"))?;
        Ok(())
    }

    async fn pending_records(storage: &ScopedStorage) -> Result<usize> {
        Ok(ProjectionOutboxWorker::new(
            storage.clone(),
            "catalog",
            CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
        )?
        .backlog()
        .await?
        .pending_record_ids
        .len())
    }

    async fn pending_intent(storage: &ScopedStorage) -> Result<bool> {
        Ok(
            ControlMvpMaintenanceWorker::new(storage.clone(), catalog_scope())?
                .pending_intent()
                .await?
                .is_some(),
        )
    }

    fn domain_summary<'a>(
        summary: &'a RunSummary,
        domain: &str,
    ) -> Result<&'a DomainMaintenanceSummary> {
        summary
            .maintenance
            .iter()
            .find(|entry| entry.domain == domain)
            .ok_or_else(|| anyhow!("missing maintenance summary for {domain}"))
    }

    fn epoch_record(kind: &str, operation_id: &str, age: Duration) -> Result<Bytes> {
        let record = serde_json::json!({
            "record_type": "arco.retention_mutation_epoch",
            "version": 1,
            "epoch": 1,
            "state": "IN_FLIGHT",
            "holder_id": "dead-holder",
            "operation_kind": kind,
            "operation_id": operation_id,
            "started_at": (Utc::now() - age).to_rfc3339(),
            "completed_at": null,
        });
        Ok(Bytes::from(serde_json::to_vec(&record)?))
    }

    #[tokio::test]
    async fn run_once_publishes_pending_catalog_maintenance() -> Result<()> {
        let storage = test_storage()?;
        let store = seed_plain_commits(&storage, 16).await?;
        assert!(
            pending_intent(&storage).await?,
            "16 L0 segments must select a maintenance intent"
        );

        let summary = run(&storage).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        let catalog = domain_summary(&summary, "catalog")?;
        assert_eq!(catalog.outcome, MaintenanceOutcome::Published);
        assert!(!catalog.recovered);
        assert!(catalog.job_id.is_some());
        assert!(catalog.layout_generation.is_some());
        assert_eq!(catalog.completed, catalog.total);
        let acks = domain_summary(&summary, PROJECTION_OUTBOX_ACK_DOMAIN)?;
        assert_eq!(acks.outcome, MaintenanceOutcome::Idle);
        assert_eq!(
            summary.epoch.as_ref().map(|e| e.outcome),
            Some(EpochOutcome::Absent)
        );
        assert!(summary.drain.is_some());
        assert_eq!(summary.gc.len(), CONTROL_DOMAINS.len());
        assert!(
            load_selected_job(&storage, "catalog").await?.is_none(),
            "a published job leaves no persisted identity behind"
        );
        assert!(
            !pending_intent(&storage).await?,
            "published maintenance clears the pending intent"
        );
        commit_generation(&store, 16).await?;
        Ok(())
    }

    #[tokio::test]
    async fn run_once_is_idle_on_a_consolidated_root() -> Result<()> {
        let storage = test_storage()?;
        seed_plain_commits(&storage, 1).await?;

        let summary = run(&storage).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        for domain in CONTROL_DOMAINS {
            assert_eq!(
                domain_summary(&summary, domain)?.outcome,
                MaintenanceOutcome::Idle
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn run_once_recovers_a_persisted_job_after_a_mid_activation_kill() -> Result<()> {
        let storage = test_storage()?;
        seed_plain_commits(&storage, 16).await?;
        // A previous run prepared, persisted and activated a job, then died.
        let dead = DurableMaintenanceWorker::new(storage.clone(), catalog_scope(), BINDING)?;
        let now = Utc::now();
        let plan = dead
            .prepare_at(now)
            .await?
            .ok_or_else(|| anyhow!("16 L0 segments must select a maintenance intent"))?;
        let persisted = plan.job_id().as_str().to_owned();
        persist_selected_job(
            &storage,
            &SelectedJobRecord {
                job_id: persisted.clone(),
                domain: "catalog".to_owned(),
                prepared_at_ms: now.timestamp_millis(),
            },
        )
        .await?;
        dead.start_at(&plan, now).await?;
        drop(dead);

        let summary = run(&storage).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        let catalog = domain_summary(&summary, "catalog")?;
        assert_eq!(catalog.outcome, MaintenanceOutcome::Published);
        assert!(catalog.recovered, "the persisted identity must be replayed");
        assert_eq!(catalog.job_id.as_deref(), Some(persisted.as_str()));
        assert!(load_selected_job(&storage, "catalog").await?.is_none());
        assert!(!pending_intent(&storage).await?);
        Ok(())
    }

    #[tokio::test]
    async fn run_once_finishes_a_persisted_job_whose_publication_already_landed() -> Result<()> {
        let storage = test_storage()?;
        seed_plain_commits(&storage, 16).await?;
        // A previous run prepared, persisted, activated and PUBLISHED a job,
        // then died before clearing its record. Publication bumped the layout
        // generation, so replaying activation would report the source as
        // consumed; the record must resume and finish instead of deferring.
        let dead = DurableMaintenanceWorker::new(storage.clone(), catalog_scope(), BINDING)?;
        let now = Utc::now();
        let plan = dead
            .prepare_at(now)
            .await?
            .ok_or_else(|| anyhow!("16 L0 segments must select a maintenance intent"))?;
        let persisted = plan.job_id().as_str().to_owned();
        persist_selected_job(
            &storage,
            &SelectedJobRecord {
                job_id: persisted.clone(),
                domain: "catalog".to_owned(),
                prepared_at_ms: now.timestamp_millis(),
            },
        )
        .await?;
        let mut progress = dead.start_at(&plan, now).await?;
        for _ in 0..512 {
            if progress.status == MaintenanceStatus::ReadyToPublish {
                break;
            }
            progress = dead.advance_at(plan.job_id(), now).await?;
        }
        dead.publish_at(plan.job_id(), now)
            .await?
            .ok_or_else(|| anyhow!("direct publication was consumed"))?;
        drop(dead);

        let summary = run(&storage).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        let catalog = domain_summary(&summary, "catalog")?;
        assert_eq!(catalog.outcome, MaintenanceOutcome::Published);
        assert!(catalog.recovered, "the persisted identity must be resumed");
        assert_eq!(catalog.job_id.as_deref(), Some(persisted.as_str()));
        assert!(
            load_selected_job(&storage, "catalog").await?.is_none(),
            "a finished job must clear its persisted record"
        );
        assert!(!pending_intent(&storage).await?);
        Ok(())
    }

    #[tokio::test]
    async fn run_once_reports_a_stuck_retention_epoch_and_fails() -> Result<()> {
        let storage = test_storage()?;
        seed_plain_commits(&storage, 1).await?;
        storage
            .put_raw(
                RETENTION_MUTATION_EPOCH_PATH,
                epoch_record(
                    "maintenance_root_publish",
                    &"a".repeat(64),
                    Duration::seconds(STALE_RECLAMATION_EPOCH_MIN_AGE_SECS * 2),
                )?,
                WritePrecondition::None,
            )
            .await?;

        let summary = run(&storage).await?;

        let epoch = summary
            .epoch
            .as_ref()
            .ok_or_else(|| anyhow!("epoch phase missing"))?;
        assert_eq!(epoch.outcome, EpochOutcome::StuckEpoch);
        assert_eq!(
            epoch.operation_kind.as_deref(),
            Some("maintenance_root_publish")
        );
        assert!(
            summary.exit_error().is_some(),
            "a stuck epoch must exit non-zero"
        );
        assert_eq!(summary.failures.len(), 1, "{:?}", summary.failures);
        assert!(summary.failures[0].contains("recover_stale_retention_epoch"));
        // The other phases still ran and deferred rather than wedging silently.
        assert_eq!(summary.maintenance.len(), CONTROL_DOMAINS.len());
        assert_eq!(summary.gc.len(), CONTROL_DOMAINS.len());
        assert!(summary.gc.iter().all(|gc| gc.pages == 0));
        Ok(())
    }

    #[tokio::test]
    async fn run_once_defers_drain_backpressure_and_finishes_the_backlog_across_runs() -> Result<()>
    {
        let storage = test_storage()?;
        // Commit refuses at 32 L0 segments, so the seed consolidates between batches.
        seed_catalog_intents(&storage, 0..15).await?;
        consolidate(&storage, "catalog").await?;
        seed_catalog_intents(&storage, 15..30).await?;
        consolidate(&storage, "catalog").await?;
        seed_catalog_intents(&storage, 30..40).await?;
        assert_eq!(pending_records(&storage).await?, 40);

        let first = run(&storage).await?;

        assert!(first.failures.is_empty(), "{:?}", first.failures);
        let drain = first
            .drain
            .as_ref()
            .ok_or_else(|| anyhow!("drain phase missing"))?;
        assert_eq!(
            drain.outcome,
            DrainOutcome::Deferred,
            "40 records exceed one run's ack-domain commit budget"
        );
        let mut remaining = pending_records(&storage).await?;
        assert!(
            remaining < 40,
            "the first run must acknowledge some records"
        );
        assert!(remaining > 0);
        let mut runs = 1;
        while remaining > 0 {
            assert!(runs < 6, "backlog of 40 must finish within a few runs");
            let summary = run(&storage).await?;
            assert!(summary.failures.is_empty(), "{:?}", summary.failures);
            let now_pending = pending_records(&storage).await?;
            assert!(now_pending < remaining, "every run must make progress");
            remaining = now_pending;
            runs += 1;
        }
        let last = run(&storage).await?;
        assert_eq!(
            last.drain.as_ref().map(|d| d.outcome),
            Some(DrainOutcome::Ok)
        );
        assert_eq!(last.drain.as_ref().and_then(|d| d.pending_records), Some(0));
        Ok(())
    }

    #[test]
    fn classify_epoch_distinguishes_stuck_from_recoverable_and_live() {
        let now = Utc::now();
        let stale = |kind: &str, id: &str| RetentionEpochRecord {
            epoch: 3,
            state: EPOCH_STATE_IN_FLIGHT.to_owned(),
            holder_id: "dead".to_owned(),
            operation_kind: kind.to_owned(),
            operation_id: id.to_owned(),
            started_at: now - Duration::seconds(STALE_RECLAMATION_EPOCH_MIN_AGE_SECS + 1),
        };
        let recoverable = vec!["job-1".to_owned()];

        assert_eq!(classify_epoch(None, &[], now), EpochOutcome::Absent);
        let mut idle = stale("maintenance_root_publish", "job-1");
        idle.state = "IDLE".to_owned();
        assert_eq!(classify_epoch(Some(&idle), &[], now), EpochOutcome::Idle);
        let mut young = stale("maintenance_root_publish", "job-9");
        young.started_at = now;
        assert_eq!(
            classify_epoch(Some(&young), &[], now),
            EpochOutcome::InFlight
        );
        assert_eq!(
            classify_epoch(Some(&stale("control_gc", "gc-1")), &[], now),
            EpochOutcome::InFlight,
            "the kernel adopts stale control GC epochs itself"
        );
        assert_eq!(
            classify_epoch(
                Some(&stale("maintenance_root_publish", "job-1")),
                &recoverable,
                now
            ),
            EpochOutcome::Recoverable
        );
        assert_eq!(
            classify_epoch(
                Some(&stale("maintenance_root_publish", "job-2")),
                &recoverable,
                now
            ),
            EpochOutcome::StuckEpoch
        );
        assert_eq!(
            classify_epoch(Some(&stale("catalog_gc", "job-1")), &recoverable, now),
            EpochOutcome::StuckEpoch,
            "only maintenance-root epochs are replayable by job id"
        );
    }

    #[test]
    fn binding_requires_exactly_32_base64_bytes() {
        let short = base64::engine::general_purpose::STANDARD.encode([9_u8; 31]);
        assert!(decode_binding(&short).is_err());
        assert!(decode_binding("not base64!").is_err());
        let exact = base64::engine::general_purpose::STANDARD.encode([9_u8; 32]);
        assert_eq!(
            decode_binding(&exact).ok(),
            Some(DurableAuthorityBinding::new([9; 32]))
        );
    }
}
