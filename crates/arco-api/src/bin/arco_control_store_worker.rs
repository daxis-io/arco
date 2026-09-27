//! Scheduled `control/` maintenance worker.
//!
//! One invocation inspects the workspace retention epoch, then for every
//! control domain runs durable layout maintenance (L0 consolidation, then the
//! retention horizon that purges expired rows and unobservable tombstones),
//! then drains the catalog projection outbox, then trims the records the
//! catalog consumer has acknowledged out of that outbox, then runs one bounded
//! pass of conservative garbage collection per domain. The process exits `0`
//! when every phase either completed or deferred to the next run, and non-zero
//! when any phase failed with a typed error or the retention epoch is stuck.
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
    MaintenanceJobId, MaintenanceKind, MaintenanceProgress, MaintenanceStatus, StateScope,
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
/// `kind` labels log lines while the job is replayed; a record written before
/// the field existed decodes with `None` and is replayed exactly as before.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SelectedJobRecord {
    job_id: String,
    domain: String,
    prepared_at_ms: i64,
    #[serde(default)]
    kind: Option<MaintenanceKind>,
}

impl SelectedJobRecord {
    /// The persisted kind's log name; a record written before the field
    /// existed is a consolidation.
    fn kind_label(&self) -> &'static str {
        self.kind.unwrap_or(MaintenanceKind::Consolidation).as_str()
    }
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
        .with_context(|| {
            format!(
                "persist maintenance job id ({}) for domain {}",
                record.kind_label(),
                record.domain
            )
        })?;
    Ok(())
}

/// Deletes the persisted identity of the finished `kind` job; a record that
/// is already gone is not an error.
async fn clear_selected_job(
    storage: &ScopedStorage,
    domain: &str,
    kind: &'static str,
) -> Result<()> {
    match storage.delete(&selected_job_path(domain)).await {
        Ok(()) | Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
            Ok(())
        }
        Err(error) => Err(error)
            .with_context(|| format!("clear maintenance job id ({kind}) for domain {domain}")),
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
            outcome = self.outcome.as_str(),
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
    /// The kernel admitted no plan: the current manifest selects no
    /// maintenance intent (consolidation) or no row is purge-eligible
    /// (retention horizon).
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TrimOutcome {
    /// A trim commit removed every acknowledged record from the catalog outbox.
    Ok,
    /// No acknowledged record remained in the outbox; nothing was committed.
    Idle,
    /// Catalog commit backpressure or a coordination loss stopped the trim.
    /// Nothing was lost: the records stay in the outbox, and the next run
    /// drains or trims them again.
    Deferred,
}

// Summary logs name outcomes by their stable snake_case serde names, which is
// what the runbook's Logs Explorer filters match.
impl EpochOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Idle => "idle",
            Self::InFlight => "in_flight",
            Self::Recoverable => "recoverable",
            Self::StuckEpoch => "stuck_epoch",
        }
    }
}

impl MaintenanceOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Published => "published",
            Self::Deferred => "deferred",
            Self::Terminal => "terminal",
            Self::Exhausted => "exhausted",
        }
    }

    /// The job is finished: nothing remains to replay, so sealing its entry
    /// ([`DomainMaintenanceSummary::finish_into`]) clears its persisted
    /// identity.
    const fn clears_record(self) -> bool {
        matches!(self, Self::Published | Self::Terminal)
    }

    /// A consolidation slot with this outcome lets the horizon slot run:
    /// `idle` persisted nothing and `published` cleared its record. `deferred`
    /// and `exhausted` leave a record that must be finished first; `terminal`
    /// cleared its record but withholds the horizon all the same, and a fresh
    /// plan is prepared next run.
    const fn admits_horizon(self) -> bool {
        matches!(self, Self::Idle | Self::Published)
    }
}

impl DrainOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Deferred => "deferred",
        }
    }
}

impl TrimOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Idle => "idle",
            Self::Deferred => "deferred",
        }
    }
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
            outcome = self.outcome.as_str(),
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
struct TrimSummary {
    outcome: TrimOutcome,
    /// Records the trim commit removed from the catalog outbox.
    trimmed_records: usize,
    /// Catalog logical sequence of the trim commit, when one landed.
    trim_sequence: Option<u64>,
    elapsed_ms: u64,
}

impl TrimSummary {
    fn log(&self) {
        tracing::info!(
            phase = "trim",
            domain = "catalog",
            outcome = self.outcome.as_str(),
            trimmed_records = self.trimmed_records,
            trim_sequence = self.trim_sequence,
            elapsed_ms = self.elapsed_ms,
            "control-store worker phase complete"
        );
    }
}

/// One maintenance job a run drove for a domain, or the kernel's refusal to
/// admit one (`idle`). A run records at most two entries per domain: one
/// consolidation and one retention horizon, either of which may be the
/// replayed job of an earlier run (see [`maintain_domain`]).
#[derive(Debug, Serialize)]
struct DomainMaintenanceSummary {
    domain: String,
    /// Which physical rewrite the job performs: `consolidation` or
    /// `retention_horizon` ([`MaintenanceKind::as_str`]).
    kind: &'static str,
    outcome: MaintenanceOutcome,
    job_id: Option<String>,
    /// The job was replayed from the persisted identity of an earlier run.
    recovered: bool,
    advances: usize,
    completed: usize,
    total: usize,
    layout_generation: Option<u64>,
    /// Rows a published retention horizon purged by expiry; `None` for a
    /// consolidation or when nothing was published.
    purged_expired_rows: Option<u64>,
    /// Tombstones a published retention horizon purged as unobservable;
    /// `None` for a consolidation or when nothing was published.
    purged_tombstones: Option<u64>,
    elapsed_ms: u64,
    /// When this entry's job was first considered; `elapsed_ms` is measured
    /// from it by [`Self::finish_into`].
    #[serde(skip)]
    started: Instant,
}

impl DomainMaintenanceSummary {
    /// Starts the entry for a job of `kind`; it reads `idle` until an outcome
    /// is sealed by [`Self::finish_into`].
    fn begin(domain: &str, kind: MaintenanceKind) -> Self {
        Self {
            domain: domain.to_owned(),
            kind: kind.as_str(),
            outcome: MaintenanceOutcome::Idle,
            job_id: None,
            recovered: false,
            advances: 0,
            completed: 0,
            total: 0,
            layout_generation: None,
            purged_expired_rows: None,
            purged_tombstones: None,
            elapsed_ms: 0,
            started: Instant::now(),
        }
    }

    /// Seals the entry with its outcome and elapsed time, logs it, clears the
    /// job's persisted identity when the outcome finished the job, then
    /// records the entry in `entries` and returns the clear's result. The
    /// entry is pushed even when the clear fails, and as each job finishes,
    /// so a typed error in a later step, the clear itself included, never
    /// hides an earlier publication.
    async fn finish_into(
        mut self,
        storage: &ScopedStorage,
        outcome: MaintenanceOutcome,
        entries: &mut Vec<Self>,
    ) -> Result<()> {
        self.outcome = outcome;
        self.elapsed_ms = elapsed_ms(self.started);
        self.log();
        let cleared = if outcome.clears_record() {
            clear_selected_job(storage, &self.domain, self.kind).await
        } else {
            Ok(())
        };
        entries.push(self);
        cleared
    }

    fn log(&self) {
        tracing::info!(
            phase = "maintenance",
            domain = %self.domain,
            kind = self.kind,
            outcome = self.outcome.as_str(),
            job_id = self.job_id.as_deref(),
            recovered = self.recovered,
            advances = self.advances,
            completed = self.completed,
            total = self.total,
            layout_generation = self.layout_generation,
            purged_expired_rows = self.purged_expired_rows,
            purged_tombstones = self.purged_tombstones,
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
    trim: Option<TrimSummary>,
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

/// Errors that let the catalog outbox trim retry on the next run: catalog
/// commit backpressure (the next run's maintenance consolidates first), or a
/// binding tenure, incarnation or pointer another actor moved mid-trim.
/// Deliberately separate from [`is_deferrable`]: backpressure defers only a
/// trim, never a maintenance step.
fn is_trim_deferrable(error: &CatalogError) -> bool {
    matches!(
        error,
        CatalogError::MaintenanceBackpressure { .. }
            | CatalogError::PreconditionFailed { .. }
            | CatalogError::CasFailed { .. }
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
// Trim
// ---------------------------------------------------------------------------

/// Retires the catalog consumer's acknowledgements and trims the records they
/// cover out of the catalog outbox through the materializer's fixed-consumer
/// path, which installs no binding metadata in the catalog root (a generic
/// trim's binding would make every later drain refuse the root). The kernel
/// commits the ack domain first and the catalog second, by exact event
/// incarnation, so no acknowledgement outlives its record and an interrupted
/// pass redelivers rather than loses.
async fn trim_catalog_outbox(storage: ScopedStorage) -> Result<TrimSummary> {
    let started = Instant::now();
    let materializer = CatalogProjectionMaterializer::new(storage)
        .context("construct catalog projection materializer")?;
    let (outcome, trimmed_records, trim_sequence) = match materializer.trim_once().await {
        Ok(report) => {
            let outcome = if report.trim_sequence.is_some() {
                TrimOutcome::Ok
            } else {
                TrimOutcome::Idle
            };
            (
                outcome,
                report.trimmed_record_ids.len(),
                report.trim_sequence,
            )
        }
        Err(error) if is_trim_deferrable(&error) => {
            tracing::warn!(
                phase = "trim",
                domain = "catalog",
                error = %error,
                "catalog outbox trim deferred to the next run; the records stay in the outbox and are drained or trimmed again"
            );
            (TrimOutcome::Deferred, 0, None)
        }
        Err(error) => return Err(error).context("trim acknowledged catalog outbox records"),
    };
    Ok(TrimSummary {
        outcome,
        trimmed_records,
        trim_sequence,
        elapsed_ms: elapsed_ms(started),
    })
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
    kind: &'static str,
    step: &'static str,
    result: arco_catalog::Result<T>,
) -> Result<Step<T>> {
    match result {
        Ok(value) => Ok(Step::Ready(value)),
        Err(error) if is_deferrable(&error) => {
            tracing::warn!(
                phase = "maintenance",
                domain,
                kind,
                step,
                error = %error,
                "maintenance step deferred to the next run"
            );
            Ok(Step::Deferred)
        }
        Err(error) => {
            Err(error).with_context(|| format!("maintenance {step} ({kind}) for domain {domain}"))
        }
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
                    kind = summary.kind,
                    job_id = job_id.as_str(),
                    status = ?progress.status,
                    "maintenance job is terminal; its persisted identity is cleared and a fresh plan is prepared"
                );
                return Ok(Advance::Stopped(MaintenanceOutcome::Terminal));
            }
            MaintenanceStatus::Planned | MaintenanceStatus::Active => {}
        }
        if summary.advances >= max_advances {
            tracing::warn!(
                phase = "maintenance",
                domain = %summary.domain,
                kind = summary.kind,
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
            summary.kind,
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
        summary.kind,
        "publish",
        worker.publish_at(job_id, Utc::now()).await,
    )? {
        Step::Ready(Some(outcome)) => {
            summary.layout_generation = Some(outcome.layout_generation());
            if let Some(purged) = outcome.purged_counts() {
                summary.purged_expired_rows = Some(purged.expired_rows);
                summary.purged_tombstones = Some(purged.tombstones);
            }
            Ok(MaintenanceOutcome::Published)
        }
        Step::Ready(None) => {
            tracing::warn!(
                phase = "maintenance",
                domain = %summary.domain,
                kind = summary.kind,
                job_id = job_id.as_str(),
                "maintenance publication was consumed by another publication; retry next run"
            );
            Ok(MaintenanceOutcome::Deferred)
        }
        // Boxed: the kernel re-read would otherwise push `run_once` past the
        // large-futures threshold.
        Step::Deferred => {
            Ok(Box::pin(outcome_after_deferred_publication(worker, job_id, summary)).await)
        }
    }
}

/// A deferred publication may have recorded the job terminal itself: the
/// kernel records a horizon whose purged set a later commit rewrote as
/// `Superseded` before refusing with `PreconditionFailed`, a refusal that is
/// permanent for the job. One re-read lets such a job end this run
/// (`terminal` clears its record, so consolidation proceeds and a fresh plan
/// is prepared next run) instead of being replayed into the same refusal on
/// every run until its record expires. Any other status, and a re-read that
/// is itself deferred or fails, keeps the deferral: the record stays and the
/// next run replays the job.
async fn outcome_after_deferred_publication(
    worker: &DurableMaintenanceWorker,
    job_id: &MaintenanceJobId,
    summary: &DomainMaintenanceSummary,
) -> MaintenanceOutcome {
    match classify_step(
        &summary.domain,
        summary.kind,
        "resume",
        worker.resume_at(job_id, Utc::now()).await,
    ) {
        Ok(Step::Ready(progress))
            if matches!(
                progress.status,
                MaintenanceStatus::Failed
                    | MaintenanceStatus::Superseded
                    | MaintenanceStatus::Abandoned
            ) =>
        {
            tracing::warn!(
                phase = "maintenance",
                domain = %summary.domain,
                kind = summary.kind,
                job_id = job_id.as_str(),
                status = ?progress.status,
                "maintenance job is terminal; its persisted identity is cleared and a fresh plan is prepared"
            );
            MaintenanceOutcome::Terminal
        }
        Ok(Step::Ready(_) | Step::Deferred) => MaintenanceOutcome::Deferred,
        Err(error) => {
            tracing::warn!(
                phase = "maintenance",
                domain = %summary.domain,
                kind = summary.kind,
                job_id = job_id.as_str(),
                error = format!("{error:#}"),
                "maintenance job could not be re-read after its deferred publication; the deferral stands"
            );
            MaintenanceOutcome::Deferred
        }
    }
}

/// Drives an activated job to publication. The caller seals the entry with
/// the returned outcome, which clears the persisted identity once nothing
/// remains to replay.
async fn finish_job(
    worker: &DurableMaintenanceWorker,
    job_id: &MaintenanceJobId,
    progress: MaintenanceProgress,
    max_advances: usize,
    summary: &mut DomainMaintenanceSummary,
) -> Result<MaintenanceOutcome> {
    match advance_until_ready(worker, job_id, progress, max_advances, summary).await? {
        Advance::Ready => publish_job(worker, job_id, summary).await,
        Advance::Stopped(outcome) => Ok(outcome),
    }
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
    let job_id = MaintenanceJobId::parse(record.job_id.clone()).with_context(|| {
        format!(
            "persisted maintenance job id ({}) for domain {domain} is invalid",
            record.kind_label()
        )
    })?;
    summary.job_id = Some(record.job_id.clone());
    summary.recovered = true;
    // Label the entry before any kernel call so even a deferred replay, which
    // yields no progress, reports the persisted kind (`kind_label` resolves a
    // record written before the field existed to a consolidation); the kernel
    // confirms it once the replay yields progress.
    summary.kind = record.kind_label();
    let now = Utc::now();
    let age_ms = now.timestamp_millis().saturating_sub(record.prepared_at_ms);
    let expired = age_ms >= MAINTENANCE_JOB_LIFETIME_MS;
    tracing::info!(
        phase = "maintenance",
        domain = %domain,
        kind = summary.kind,
        job_id = job_id.as_str(),
        age_ms,
        expired,
        "resuming persisted maintenance job"
    );
    // A job whose activation completed resumes directly. `resume_at` observes a
    // Publishing/Published attempt without re-checking source compatibility,
    // which a publication that already reached HEAD would otherwise fail
    // (publication bumps the layout generation). Activation is replayed only
    // when the job is not directly resumable, e.g. its selector never landed.
    if !expired {
        match worker.resume_at(&job_id, now).await {
            Ok(progress) => {
                summary.kind = progress.kind.as_str();
                return Ok(Recovery::Resume(job_id, progress));
            }
            Err(error)
                if is_deferrable(&error) || matches!(error, CatalogError::NotFound { .. }) =>
            {
                tracing::info!(
                    phase = "maintenance",
                    domain = %domain,
                    kind = summary.kind,
                    job_id = job_id.as_str(),
                    error = %error,
                    "persisted maintenance job is not directly resumable; replaying activation"
                );
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("maintenance resume ({}) for domain {domain}", summary.kind)
                });
            }
        }
    }
    let disposition = match worker.recover_activation_at(&job_id, now).await {
        Ok(progress) if !expired => {
            // The recovered descriptor's kind is authoritative over the record.
            summary.kind = progress.kind.as_str();
            return resume_recovered_job(worker, &domain, summary.kind, job_id).await;
        }
        Ok(_) => RecoveryDisposition::Abandon(
            "persisted maintenance job exceeded its lifetime; its root was recovered and a fresh plan follows",
        ),
        Err(CatalogError::NotFound { .. }) => RecoveryDisposition::Abandon(
            "persisted maintenance job has no durable descriptor; activation never landed and a fresh plan follows",
        ),
        // A deferred replay yields no progress; the entry keeps the record's
        // label (or the default for a record written without one).
        Err(error) if is_deferrable(&error) && !expired => RecoveryDisposition::Defer(error),
        Err(error) if is_deferrable(&error) => RecoveryDisposition::AbandonAfterError(error),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "maintenance recovery ({}) for domain {domain}",
                    summary.kind
                )
            });
        }
    };
    apply_recovery_disposition(storage, &domain, summary.kind, &job_id, disposition).await
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
    kind: &'static str,
    job_id: MaintenanceJobId,
) -> Result<Recovery> {
    match classify_step(
        domain,
        kind,
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
    kind: &'static str,
    job_id: &MaintenanceJobId,
    disposition: RecoveryDisposition,
) -> Result<Recovery> {
    match disposition {
        RecoveryDisposition::Defer(error) => {
            tracing::warn!(
                phase = "maintenance",
                domain,
                kind,
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
                kind,
                job_id = job_id.as_str(),
                reason
            );
            clear_selected_job(storage, domain, kind).await?;
            Ok(Recovery::Fresh)
        }
        RecoveryDisposition::AbandonAfterError(error) => {
            tracing::warn!(
                phase = "maintenance",
                domain,
                kind,
                job_id = job_id.as_str(),
                error = %error,
                "expired persisted maintenance job could not be replayed; abandoning its record (a retention epoch it still holds is reported as stuck_epoch next run)"
            );
            clear_selected_job(storage, domain, kind).await?;
            Ok(Recovery::Fresh)
        }
    }
}

/// Prepares one new job of `kind` against the current head, persists its
/// identity, activates it and drives it to publication. `Idle` when the
/// kernel admits no plan. The caller guarantees no persisted identity remains
/// for the domain: a second live job would orphan the first one's record.
async fn drive_fresh_job(
    storage: &ScopedStorage,
    worker: &DurableMaintenanceWorker,
    kind: MaintenanceKind,
    max_advances: usize,
    summary: &mut DomainMaintenanceSummary,
) -> Result<MaintenanceOutcome> {
    summary.kind = kind.as_str();
    let prepared = match kind {
        MaintenanceKind::Consolidation => worker.prepare_at(Utc::now()).await,
        MaintenanceKind::RetentionHorizon => worker.prepare_horizon_at(Utc::now()).await,
    };
    let plan = match classify_step(&summary.domain, summary.kind, "prepare", prepared)? {
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
            kind: Some(kind),
        },
    )
    .await?;
    summary.job_id = Some(job_id.as_str().to_owned());
    tracing::info!(
        phase = "maintenance",
        domain = %summary.domain,
        kind = summary.kind,
        job_id = job_id.as_str(),
        "maintenance job prepared and its identity persisted"
    );
    let progress = match classify_step(
        &summary.domain,
        summary.kind,
        "start",
        worker.start_at(&plan, Utc::now()).await,
    )? {
        Step::Ready(progress) => progress,
        Step::Deferred => return Ok(MaintenanceOutcome::Deferred),
    };
    finish_job(worker, &job_id, progress, max_advances, summary).await
}

/// Runs one domain's maintenance phase: one consolidation slot, then one
/// retention horizon slot. Each finished job is logged and recorded in
/// `entries` at once (at most two per domain), so a typed error in a later
/// step never hides an earlier publication.
///
/// 1. A persisted job identity is replayed first, whatever its kind, and
///    fills this run's slot for that kind. The domain's phase ends here
///    unless the job finished (`published` or `terminal`, which clears the
///    record); a `deferred` or `exhausted` job keeps its record and is
///    replayed next run, and nothing else may start while it exists.
/// 2. Unless a replayed consolidation filled its slot, a consolidation is
///    prepared and driven (`idle` when no intent is pending).
/// 3. Unless a replayed horizon filled its slot, a retention horizon is
///    prepared and driven over the consolidated head (`idle` when no row is
///    purge-eligible) once the consolidation slot ended `idle` or
///    `published`, or was filled by a replayed consolidation that published.
///    A `terminal` consolidation withholds the horizon although its record
///    is cleared; a fresh plan is prepared next run.
///
/// An abandoned record (expired, or never activated) is cleared during
/// recovery and gives way to a fresh consolidation on the same entry.
async fn maintain_domain(
    storage: ScopedStorage,
    tenant: &str,
    workspace: &str,
    domain: &str,
    binding: DurableAuthorityBinding,
    max_advances: usize,
    entries: &mut Vec<DomainMaintenanceSummary>,
) -> Result<()> {
    let scope = StateScope::new(tenant, workspace, domain);
    let worker = DurableMaintenanceWorker::new(storage.clone(), scope, binding)
        .with_context(|| format!("construct maintenance worker for domain {domain}"))?;
    // The replayed job's kind and outcome, once it finished and cleared its
    // record; it fills this run's slot for that kind.
    let mut replayed: Option<(MaintenanceKind, MaintenanceOutcome)> = None;
    let mut summary = DomainMaintenanceSummary::begin(domain, MaintenanceKind::Consolidation);

    if let Some(record) = load_selected_job(&storage, domain).await? {
        // `recover_selected_job` labels the entry with the replayed job's kind.
        match recover_selected_job(&storage, &worker, &record, &mut summary).await? {
            Recovery::Resume(job_id, progress) => {
                let kind = progress.kind;
                let outcome =
                    finish_job(&worker, &job_id, progress, max_advances, &mut summary).await?;
                summary.finish_into(&storage, outcome, entries).await?;
                if !outcome.clears_record() {
                    return Ok(());
                }
                replayed = Some((kind, outcome));
                summary = DomainMaintenanceSummary::begin(domain, MaintenanceKind::Consolidation);
            }
            Recovery::Deferred => {
                summary
                    .finish_into(&storage, MaintenanceOutcome::Deferred, entries)
                    .await?;
                return Ok(());
            }
            Recovery::Fresh => {
                summary.recovered = false;
                summary.job_id = None;
            }
        }
    }

    let horizon_may_follow = if let Some((MaintenanceKind::Consolidation, outcome)) = replayed {
        // The replayed consolidation filled this run's slot; the horizon
        // follows only the head it published, never a terminal job.
        outcome == MaintenanceOutcome::Published
    } else {
        let outcome = drive_fresh_job(
            &storage,
            &worker,
            MaintenanceKind::Consolidation,
            max_advances,
            &mut summary,
        )
        .await?;
        let may_follow = outcome.admits_horizon();
        summary.finish_into(&storage, outcome, entries).await?;
        may_follow
    };
    if matches!(replayed, Some((MaintenanceKind::RetentionHorizon, _))) || !horizon_may_follow {
        return Ok(());
    }

    let mut summary = DomainMaintenanceSummary::begin(domain, MaintenanceKind::RetentionHorizon);
    let outcome = drive_fresh_job(
        &storage,
        &worker,
        MaintenanceKind::RetentionHorizon,
        max_advances,
        &mut summary,
    )
    .await?;
    summary.finish_into(&storage, outcome, entries).await
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

/// Runs every phase once: epoch inspection, maintenance per domain
/// (consolidation, then retention horizon), catalog projection drain, catalog
/// outbox trim, GC per domain. Phases are independent: a failure is recorded
/// and the remaining phases still run, so one wedged domain never starves
/// another. The returned summary carries every failure; callers exit non-zero
/// through [`RunSummary::exit_error`].
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
        // Finished entries are logged and recorded by `maintain_domain` as
        // they complete, so they survive a failure in a later step.
        if let Err(error) = maintain_domain(
            storage.clone(),
            tenant,
            workspace,
            domain,
            binding,
            limits.maintenance_max_advances,
            &mut summary.maintenance,
        )
        .await
        {
            summary.fail("maintenance", domain, &error);
        }
    }

    match drain_catalog_projection(storage.clone()).await {
        Ok(drain) => {
            drain.log();
            summary.drain = Some(drain);
        }
        Err(error) => summary.fail("drain", "catalog", &error),
    }

    // The trim follows the drain whatever the drain's outcome: records a
    // deferred or failed drain had already acknowledged are exact and
    // trimmable all the same.
    match trim_catalog_outbox(storage.clone()).await {
        Ok(trim) => {
            trim.log();
            summary.trim = Some(trim);
        }
        Err(error) => summary.fail("trim", "catalog", &error),
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use arco_catalog::{
        ArcoStateReader as _, ArcoStateTxn as _, CatalogProjectionNotifier,
        ControlCatalogAuthority, ControlMvpStateStore, ProjectionIntentV1, TxnOptions,
        WriteOptions,
    };
    use arco_core::storage::{ListPage, ObjectMeta, StorageBackend};
    use arco_core::{MemoryBackend, WriteResult};
    use async_trait::async_trait;
    use chrono::Duration;

    use super::*;

    const BINDING: DurableAuthorityBinding = DurableAuthorityBinding::new([7; 32]);
    const TEST_LIMITS: RunLimits = RunLimits {
        gc_max_pages: 4,
        maintenance_max_advances: 512,
    };
    const TENANT: &str = "tenant";
    const WORKSPACE: &str = "workspace";
    const HOUR_MS: i64 = 60 * 60 * 1000;
    const DAY_MS: i64 = 24 * HOUR_MS;

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

    /// Commits one catalog row whose expiry stamp is two hours old: past the
    /// horizon's purge cutoff of now minus one hour, so the next horizon job
    /// purges it.
    async fn seed_expired_row(storage: &ScopedStorage) -> Result<ControlMvpStateStore> {
        let store = ControlMvpStateStore::new(storage.clone(), catalog_scope())?;
        let mut tx = store.begin_control_txn(TxnOptions::default()).await?;
        tx.put_with_expiry(
            b"expired",
            Bytes::from_static(b"expired"),
            Utc::now().timestamp_millis() - 2 * HOUR_MS,
        )
        .await?;
        tx.commit().await?;
        Ok(store)
    }

    /// A previous run prepared a horizon plan over the current catalog head,
    /// persisted its identity, activated it and died; returns the job id.
    async fn activate_persisted_horizon(storage: &ScopedStorage) -> Result<String> {
        let dead = DurableMaintenanceWorker::new(storage.clone(), catalog_scope(), BINDING)?;
        let now = Utc::now();
        let plan = dead
            .prepare_horizon_at(now)
            .await?
            .ok_or_else(|| anyhow!("an expired row must admit a horizon plan"))?;
        assert_eq!(plan.kind(), MaintenanceKind::RetentionHorizon);
        let persisted = plan.job_id().as_str().to_owned();
        persist_selected_job(
            storage,
            &catalog_job_record(&persisted, MaintenanceKind::RetentionHorizon, now),
        )
        .await?;
        dead.start_at(&plan, now).await?;
        Ok(persisted)
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

    /// The catalog head's logical sequence as the outbox worker observes it;
    /// `None` before the domain's first commit.
    async fn catalog_sequence(storage: &ScopedStorage) -> Result<Option<u64>> {
        Ok(ProjectionOutboxWorker::new(
            storage.clone(),
            "catalog",
            CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
        )?
        .backlog()
        .await?
        .committed_sequence)
    }

    /// Records the current visible catalog manifest still replays into the
    /// projection outbox, acknowledged or not.
    async fn catalog_outbox_len(storage: &ScopedStorage) -> Result<usize> {
        Ok(ControlMvpStateStore::new(storage.clone(), catalog_scope())?
            .current_projection_outbox()
            .await?
            .len())
    }

    fn trim_summary(summary: &RunSummary) -> Result<&TrimSummary> {
        summary
            .trim
            .as_ref()
            .ok_or_else(|| anyhow!("trim phase missing"))
    }

    /// The first maintenance entry a run recorded for `domain` with `kind`.
    fn domain_summary<'a>(
        summary: &'a RunSummary,
        domain: &str,
        kind: &str,
    ) -> Result<&'a DomainMaintenanceSummary> {
        summary
            .maintenance
            .iter()
            .find(|entry| entry.domain == domain && entry.kind == kind)
            .ok_or_else(|| anyhow!("missing {kind} maintenance summary for {domain}"))
    }

    /// Every maintenance entry a run recorded for `domain`, as `(kind, outcome)`
    /// in the order the jobs were driven.
    fn domain_entries(
        summary: &RunSummary,
        domain: &str,
    ) -> Vec<(&'static str, MaintenanceOutcome)> {
        summary
            .maintenance
            .iter()
            .filter(|entry| entry.domain == domain)
            .map(|entry| (entry.kind, entry.outcome))
            .collect()
    }

    /// A catalog-domain job record as this binary persists it.
    fn catalog_job_record(
        job_id: &str,
        kind: MaintenanceKind,
        now: DateTime<Utc>,
    ) -> SelectedJobRecord {
        SelectedJobRecord {
            job_id: job_id.to_owned(),
            domain: "catalog".to_owned(),
            prepared_at_ms: now.timestamp_millis(),
            kind: Some(kind),
        }
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
        let catalog = domain_summary(&summary, "catalog", "consolidation")?;
        assert_eq!(catalog.outcome, MaintenanceOutcome::Published);
        assert!(!catalog.recovered);
        assert!(catalog.job_id.is_some());
        assert!(catalog.layout_generation.is_some());
        assert_eq!(catalog.completed, catalog.total);
        assert_eq!(catalog.purged_expired_rows, None);
        assert_eq!(catalog.purged_tombstones, None);
        assert_eq!(
            domain_summary(&summary, "catalog", "retention_horizon")?.outcome,
            MaintenanceOutcome::Idle,
            "no row is purge-eligible after plain commits"
        );
        let acks = domain_summary(&summary, PROJECTION_OUTBOX_ACK_DOMAIN, "consolidation")?;
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
                domain_entries(&summary, domain),
                vec![
                    ("consolidation", MaintenanceOutcome::Idle),
                    ("retention_horizon", MaintenanceOutcome::Idle),
                ],
                "{domain}: a consolidation and a horizon run, both idle"
            );
        }
        assert_eq!(summary.maintenance.len(), 2 * CONTROL_DOMAINS.len());
        Ok(())
    }

    #[tokio::test]
    async fn run_once_publishes_a_horizon_that_drops_expired_rows() -> Result<()> {
        let storage = test_storage()?;
        let store = ControlMvpStateStore::new(storage.clone(), catalog_scope())?;
        let now_ms = Utc::now().timestamp_millis();
        let mut tx = store.begin_control_txn(TxnOptions::default()).await?;
        tx.put(b"live", Bytes::from_static(b"live")).await?;
        tx.put_with_expiry(
            b"expired",
            Bytes::from_static(b"expired"),
            now_ms - 2 * HOUR_MS,
        )
        .await?;
        tx.put_with_expiry(b"fresh", Bytes::from_static(b"fresh"), now_ms + DAY_MS)
            .await?;
        tx.commit().await?;
        // An expiry stamp is a hint: the row reads normally until purged.
        assert_eq!(
            store.get(b"expired").await?,
            Some(Bytes::from_static(b"expired"))
        );

        let summary = run(&storage).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        assert_eq!(
            domain_entries(&summary, "catalog"),
            vec![
                ("consolidation", MaintenanceOutcome::Idle),
                ("retention_horizon", MaintenanceOutcome::Published),
            ]
        );
        let horizon = domain_summary(&summary, "catalog", "retention_horizon")?;
        assert!(!horizon.recovered);
        assert!(horizon.job_id.is_some());
        assert!(horizon.layout_generation.is_some());
        assert_eq!(horizon.completed, horizon.total);
        assert_eq!(horizon.purged_expired_rows, Some(1));
        assert_eq!(horizon.purged_tombstones, Some(0));
        let consolidation = domain_summary(&summary, "catalog", "consolidation")?;
        assert_eq!(consolidation.purged_expired_rows, None);
        assert_eq!(consolidation.purged_tombstones, None);
        assert!(
            load_selected_job(&storage, "catalog").await?.is_none(),
            "a published horizon leaves no persisted identity behind"
        );
        let reader = ControlMvpStateStore::new(storage.clone(), catalog_scope())?;
        assert_eq!(
            reader.get(b"expired").await?,
            None,
            "the expired row is purged"
        );
        assert_eq!(
            reader.get(b"live").await?,
            Some(Bytes::from_static(b"live"))
        );
        assert_eq!(
            reader.get(b"fresh").await?,
            Some(Bytes::from_static(b"fresh"))
        );

        let second = run(&storage).await?;

        assert!(second.failures.is_empty(), "{:?}", second.failures);
        assert_eq!(
            domain_summary(&second, "catalog", "retention_horizon")?.outcome,
            MaintenanceOutcome::Idle,
            "nothing is purge-eligible once the horizon published"
        );
        Ok(())
    }

    #[tokio::test]
    async fn run_once_runs_consolidation_then_horizon_in_one_run() -> Result<()> {
        let storage = test_storage()?;
        seed_plain_commits(&storage, 16).await?;
        seed_expired_row(&storage).await?;
        assert!(
            pending_intent(&storage).await?,
            "17 L0 segments must select a maintenance intent"
        );

        let summary = run(&storage).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        assert_eq!(
            domain_entries(&summary, "catalog"),
            vec![
                ("consolidation", MaintenanceOutcome::Published),
                ("retention_horizon", MaintenanceOutcome::Published),
            ],
            "the consolidation publishes first, then the horizon over the consolidated head"
        );
        let consolidation = domain_summary(&summary, "catalog", "consolidation")?;
        let horizon = domain_summary(&summary, "catalog", "retention_horizon")?;
        assert!(consolidation.layout_generation < horizon.layout_generation);
        assert_eq!(horizon.purged_expired_rows, Some(1));
        assert_eq!(horizon.purged_tombstones, Some(0));
        assert_ne!(consolidation.job_id, horizon.job_id);
        assert!(load_selected_job(&storage, "catalog").await?.is_none());
        assert!(
            !pending_intent(&storage).await?,
            "published maintenance clears the pending intent"
        );
        let reader = ControlMvpStateStore::new(storage.clone(), catalog_scope())?;
        assert_eq!(reader.get(b"expired").await?, None);
        assert_eq!(
            reader.get(b"key").await?,
            Some(Bytes::from_static(b"generation-15"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn run_once_resumes_a_persisted_horizon_job() -> Result<()> {
        let storage = test_storage()?;
        seed_expired_row(&storage).await?;
        let persisted = activate_persisted_horizon(&storage).await?;

        let summary = run(&storage).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        assert_eq!(
            domain_entries(&summary, "catalog"),
            vec![
                ("retention_horizon", MaintenanceOutcome::Published),
                ("consolidation", MaintenanceOutcome::Idle),
            ],
            "the recovered horizon is this run's horizon; a consolidation still follows"
        );
        let horizon = domain_summary(&summary, "catalog", "retention_horizon")?;
        assert!(horizon.recovered, "the persisted identity must be replayed");
        assert_eq!(horizon.job_id.as_deref(), Some(persisted.as_str()));
        assert_eq!(horizon.purged_expired_rows, Some(1));
        assert_eq!(horizon.purged_tombstones, Some(0));
        assert!(
            load_selected_job(&storage, "catalog").await?.is_none(),
            "a finished job must clear its persisted record"
        );
        let reader = ControlMvpStateStore::new(storage.clone(), catalog_scope())?;
        assert_eq!(reader.get(b"expired").await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn run_once_supersedes_a_horizon_whose_purged_key_was_rewritten_and_recomputes_next_run()
    -> Result<()> {
        let storage = test_storage()?;
        let store = ControlMvpStateStore::new(storage.clone(), catalog_scope())?;
        let now_ms = Utc::now().timestamp_millis();
        let mut tx = store.begin_control_txn(TxnOptions::default()).await?;
        tx.put(b"live", Bytes::from_static(b"live")).await?;
        tx.put_with_expiry(
            b"rewritten",
            Bytes::from_static(b"expired"),
            now_ms - 2 * HOUR_MS,
        )
        .await?;
        tx.put_with_expiry(
            b"expired",
            Bytes::from_static(b"expired"),
            now_ms - 2 * HOUR_MS,
        )
        .await?;
        tx.commit().await?;
        // The activated job's admitted purged set holds both expired rows.
        let persisted = activate_persisted_horizon(&storage).await?;
        // A later commit rewrites one purged key, so the admitted purged set
        // no longer describes the parent: the job can never publish.
        let mut tx = store.begin_control_txn(TxnOptions::default()).await?;
        tx.put(b"rewritten", Bytes::from_static(b"reborn")).await?;
        tx.commit().await?;

        let summary = run(&storage).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        assert_eq!(
            domain_entries(&summary, "catalog"),
            vec![
                ("retention_horizon", MaintenanceOutcome::Terminal),
                ("consolidation", MaintenanceOutcome::Idle),
            ],
            "the superseded horizon ends terminal in the run that observes the refusal, and consolidation proceeds"
        );
        let horizon = domain_summary(&summary, "catalog", "retention_horizon")?;
        assert!(horizon.recovered);
        assert_eq!(horizon.job_id.as_deref(), Some(persisted.as_str()));
        assert_eq!(horizon.layout_generation, None);
        assert_eq!(horizon.purged_expired_rows, None);
        assert_eq!(horizon.purged_tombstones, None);
        assert!(
            load_selected_job(&storage, "catalog").await?.is_none(),
            "a terminal job clears its persisted record"
        );
        let kernel = DurableMaintenanceWorker::new(storage.clone(), catalog_scope(), BINDING)?;
        assert_eq!(
            kernel
                .resume_at(&MaintenanceJobId::parse(persisted.clone())?, Utc::now())
                .await?
                .status,
            MaintenanceStatus::Superseded,
            "the kernel recorded the refusal as terminal"
        );
        let reader = ControlMvpStateStore::new(storage.clone(), catalog_scope())?;
        assert_eq!(
            reader.get(b"rewritten").await?,
            Some(Bytes::from_static(b"reborn"))
        );
        assert_eq!(
            reader.get(b"expired").await?,
            Some(Bytes::from_static(b"expired")),
            "a superseded job purges nothing"
        );

        let second = run(&storage).await?;

        assert!(second.failures.is_empty(), "{:?}", second.failures);
        assert_eq!(
            domain_entries(&second, "catalog"),
            vec![
                ("consolidation", MaintenanceOutcome::Idle),
                ("retention_horizon", MaintenanceOutcome::Published),
            ],
            "a fresh horizon recomputes the purge over the rewritten parent"
        );
        let horizon = domain_summary(&second, "catalog", "retention_horizon")?;
        assert!(!horizon.recovered);
        assert_ne!(horizon.job_id.as_deref(), Some(persisted.as_str()));
        assert_eq!(horizon.purged_expired_rows, Some(1));
        assert_eq!(horizon.purged_tombstones, Some(0));
        assert!(load_selected_job(&storage, "catalog").await?.is_none());
        let reader = ControlMvpStateStore::new(storage.clone(), catalog_scope())?;
        assert_eq!(reader.get(b"expired").await?, None);
        assert_eq!(
            reader.get(b"rewritten").await?,
            Some(Bytes::from_static(b"reborn")),
            "the rewritten key is live and stays readable"
        );
        assert_eq!(
            reader.get(b"live").await?,
            Some(Bytes::from_static(b"live"))
        );
        Ok(())
    }

    /// Which storage write [`FailNthWrite`] fails.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FailedWrite {
        Put,
        Delete,
    }

    /// Wraps the memory backend and fails the `n`-th `put` or `delete` of a
    /// path ending in `suffix` with a storage error, so one worker step
    /// errors after the steps before it succeeded.
    struct FailNthWrite {
        inner: MemoryBackend,
        write: FailedWrite,
        suffix: &'static str,
        fail_on: usize,
        seen: AtomicUsize,
        fired: AtomicBool,
    }

    impl FailNthWrite {
        fn put(suffix: &'static str, fail_on: usize) -> Self {
            Self::new(FailedWrite::Put, suffix, fail_on)
        }

        fn delete(suffix: &'static str, fail_on: usize) -> Self {
            Self::new(FailedWrite::Delete, suffix, fail_on)
        }

        fn new(write: FailedWrite, suffix: &'static str, fail_on: usize) -> Self {
            Self {
                inner: MemoryBackend::new(),
                write,
                suffix,
                fail_on,
                seen: AtomicUsize::new(0),
                fired: AtomicBool::new(false),
            }
        }

        fn fired(&self) -> bool {
            self.fired.load(Ordering::SeqCst)
        }

        /// The error to inject when this `write` of `path` is the one to fail.
        fn injected(&self, write: FailedWrite, path: &str) -> Option<arco_core::Error> {
            if self.write != write || !path.ends_with(self.suffix) {
                return None;
            }
            if self.seen.fetch_add(1, Ordering::SeqCst) + 1 != self.fail_on {
                return None;
            }
            self.fired.store(true, Ordering::SeqCst);
            Some(arco_core::Error::Storage {
                message: format!("injected failure on {write:?} of {path}"),
                source: None,
            })
        }
    }

    #[async_trait]
    impl StorageBackend for FailNthWrite {
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
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            if let Some(error) = self.injected(FailedWrite::Put, path) {
                return Err(error);
            }
            self.inner.put(path, data, precondition).await
        }
        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            if let Some(error) = self.injected(FailedWrite::Delete, path) {
                return Err(error);
            }
            self.inner.delete(path).await
        }
        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }
        async fn list_page(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            limit: usize,
        ) -> arco_core::Result<ListPage> {
            self.inner.list_page(prefix, start_after, limit).await
        }
        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }
        async fn signed_url(
            &self,
            path: &str,
            expiry: std::time::Duration,
        ) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    #[tokio::test]
    async fn run_once_withholds_the_horizon_after_a_terminal_recovered_consolidation() -> Result<()>
    {
        let storage = test_storage()?;
        seed_plain_commits(&storage, 16).await?;
        // An expired row makes a horizon admissible, so its absence below is
        // the slot rule at work rather than an idle horizon.
        seed_expired_row(&storage).await?;
        // A previous run prepared, persisted and activated a consolidation;
        // the job was then abandoned before that run could finish it.
        let dead = DurableMaintenanceWorker::new(storage.clone(), catalog_scope(), BINDING)?;
        let now = Utc::now();
        let plan = dead
            .prepare_at(now)
            .await?
            .ok_or_else(|| anyhow!("17 L0 segments must select a maintenance intent"))?;
        let persisted = plan.job_id().as_str().to_owned();
        persist_selected_job(
            &storage,
            &catalog_job_record(&persisted, MaintenanceKind::Consolidation, now),
        )
        .await?;
        dead.start_at(&plan, now).await?;
        let abandoned = dead.abandon_at(plan.job_id(), now).await?;
        assert_eq!(abandoned.status, MaintenanceStatus::Abandoned);
        drop(dead);

        let summary = run(&storage).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        assert_eq!(
            domain_entries(&summary, "catalog"),
            vec![("consolidation", MaintenanceOutcome::Terminal)],
            "a terminal consolidation slot withholds the horizon"
        );
        let catalog = domain_summary(&summary, "catalog", "consolidation")?;
        assert!(catalog.recovered, "the persisted identity must be replayed");
        assert_eq!(catalog.job_id.as_deref(), Some(persisted.as_str()));
        assert_eq!(catalog.layout_generation, None);
        assert!(
            load_selected_job(&storage, "catalog").await?.is_none(),
            "a terminal job clears its persisted record"
        );
        assert!(
            pending_intent(&storage).await?,
            "nothing consolidated; a fresh plan is prepared next run"
        );
        let reader = ControlMvpStateStore::new(storage.clone(), catalog_scope())?;
        assert_eq!(
            reader.get(b"expired").await?,
            Some(Bytes::from_static(b"expired")),
            "no horizon ran"
        );
        Ok(())
    }

    #[tokio::test]
    async fn run_once_keeps_a_published_consolidation_when_the_horizon_step_fails() -> Result<()> {
        // The catalog consolidation persists the run's first job identity and
        // the catalog horizon the second; failing that second write is a
        // typed (non-deferrable) error in the horizon step after a publication.
        let backend = Arc::new(FailNthWrite::put("/selected-job.json", 2));
        let storage = ScopedStorage::new(backend.clone(), TENANT, WORKSPACE)?;
        seed_plain_commits(&storage, 16).await?;
        seed_expired_row(&storage).await?;

        let summary = run(&storage).await?;

        assert!(
            backend.fired(),
            "the injected failure must hit the horizon's identity write"
        );
        assert_eq!(
            domain_entries(&summary, "catalog"),
            vec![("consolidation", MaintenanceOutcome::Published)],
            "the published consolidation stays recorded; the failed horizon has no entry"
        );
        assert!(
            !pending_intent(&storage).await?,
            "the consolidation really published"
        );
        assert_eq!(summary.failures.len(), 1, "{:?}", summary.failures);
        assert!(
            summary.failures[0].starts_with("maintenance[catalog]:")
                && summary.failures[0].contains("(retention_horizon)"),
            "the failure names the phase, domain and job kind: {}",
            summary.failures[0]
        );
        assert!(summary.exit_error().is_some());
        // The other domain and the later phases still ran.
        assert_eq!(
            domain_entries(&summary, PROJECTION_OUTBOX_ACK_DOMAIN),
            vec![
                ("consolidation", MaintenanceOutcome::Idle),
                ("retention_horizon", MaintenanceOutcome::Idle),
            ]
        );
        assert!(summary.drain.is_some());
        assert_eq!(summary.gc.len(), CONTROL_DOMAINS.len());
        assert!(
            load_selected_job(&storage, "catalog").await?.is_none(),
            "the failed write persisted nothing to replay"
        );
        Ok(())
    }

    #[tokio::test]
    async fn run_once_keeps_a_published_job_when_clearing_its_record_fails() -> Result<()> {
        // The first record delete of the run is the published catalog
        // consolidation clearing its identity; failing it is a typed error
        // after the publication landed.
        let backend = Arc::new(FailNthWrite::delete("/selected-job.json", 1));
        let storage = ScopedStorage::new(backend.clone(), TENANT, WORKSPACE)?;
        seed_plain_commits(&storage, 16).await?;

        let summary = run(&storage).await?;

        assert!(
            backend.fired(),
            "the injected failure must hit the record clear"
        );
        assert_eq!(
            domain_entries(&summary, "catalog"),
            vec![("consolidation", MaintenanceOutcome::Published)],
            "the published consolidation stays recorded; the failed clear withholds the horizon"
        );
        assert!(
            !pending_intent(&storage).await?,
            "the consolidation really published"
        );
        assert_eq!(summary.failures.len(), 1, "{:?}", summary.failures);
        assert!(
            summary.failures[0].starts_with("maintenance[catalog]:")
                && summary.failures[0].contains("clear maintenance job id (consolidation)"),
            "the failure names the phase, domain and job kind: {}",
            summary.failures[0]
        );
        assert!(
            load_selected_job(&storage, "catalog").await?.is_some(),
            "the record outlives the failed clear"
        );

        // The next run replays the record, finds the job published and clears it.
        let second = run(&storage).await?;

        assert!(second.failures.is_empty(), "{:?}", second.failures);
        assert_eq!(
            domain_entries(&second, "catalog"),
            vec![
                ("consolidation", MaintenanceOutcome::Published),
                ("retention_horizon", MaintenanceOutcome::Idle),
            ]
        );
        assert!(domain_summary(&second, "catalog", "consolidation")?.recovered);
        assert!(load_selected_job(&storage, "catalog").await?.is_none());
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
            &catalog_job_record(&persisted, MaintenanceKind::Consolidation, now),
        )
        .await?;
        dead.start_at(&plan, now).await?;
        drop(dead);

        let summary = run(&storage).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        assert_eq!(
            domain_entries(&summary, "catalog"),
            vec![
                ("consolidation", MaintenanceOutcome::Published),
                ("retention_horizon", MaintenanceOutcome::Idle),
            ],
            "the replayed job fills the consolidation slot; the horizon follows it directly"
        );
        let catalog = domain_summary(&summary, "catalog", "consolidation")?;
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
            &catalog_job_record(&persisted, MaintenanceKind::Consolidation, now),
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
        assert_eq!(
            domain_entries(&summary, "catalog"),
            vec![
                ("consolidation", MaintenanceOutcome::Published),
                ("retention_horizon", MaintenanceOutcome::Idle),
            ]
        );
        let catalog = domain_summary(&summary, "catalog", "consolidation")?;
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
        assert_eq!(summary.maintenance.len(), 2 * CONTROL_DOMAINS.len());
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
            Some(DrainOutcome::Ok),
            "{:?}",
            last.failures
        );
        assert_eq!(last.drain.as_ref().and_then(|d| d.pending_records), Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn run_once_trims_acknowledged_catalog_records_after_the_drain() -> Result<()> {
        let storage = test_storage()?;
        seed_catalog_intents(&storage, 0..3).await?;
        assert_eq!(pending_records(&storage).await?, 3);
        assert_eq!(catalog_outbox_len(&storage).await?, 3);

        let summary = run(&storage).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        assert_eq!(
            summary.drain.as_ref().map(|drain| drain.drained_records),
            Some(3)
        );
        let trim = trim_summary(&summary)?;
        assert_eq!(trim.outcome, TrimOutcome::Ok);
        assert_eq!(trim.trimmed_records, 3);
        assert!(trim.trim_sequence.is_some());
        assert_eq!(pending_records(&storage).await?, 0);
        assert_eq!(
            catalog_outbox_len(&storage).await?,
            0,
            "the trim commit removes every acknowledged record from the replayed outbox"
        );
        let sequence = catalog_sequence(&storage).await?;
        assert_eq!(
            sequence, trim.trim_sequence,
            "the trim commit is the run's last catalog commit"
        );
        assert_eq!(summary.gc.len(), CONTROL_DOMAINS.len());

        let second = run(&storage).await?;

        assert!(second.failures.is_empty(), "{:?}", second.failures);
        let trim = trim_summary(&second)?;
        assert_eq!(trim.outcome, TrimOutcome::Idle);
        assert_eq!(trim.trimmed_records, 0);
        assert_eq!(trim.trim_sequence, None);
        assert_eq!(
            catalog_sequence(&storage).await?,
            sequence,
            "an idle trim commits nothing to the catalog"
        );
        Ok(())
    }

    #[tokio::test]
    async fn run_once_trim_is_idle_when_nothing_is_acknowledged() -> Result<()> {
        let storage = test_storage()?;
        seed_plain_commits(&storage, 3).await?;
        assert_eq!(catalog_sequence(&storage).await?, Some(3));

        let summary = run(&storage).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        let trim = trim_summary(&summary)?;
        assert_eq!(trim.outcome, TrimOutcome::Idle);
        assert_eq!(trim.trimmed_records, 0);
        assert_eq!(trim.trim_sequence, None);
        assert_eq!(
            catalog_sequence(&storage).await?,
            Some(3),
            "an idle trim commits nothing to the catalog"
        );
        Ok(())
    }

    #[tokio::test]
    async fn run_once_defers_the_trim_under_catalog_backpressure() -> Result<()> {
        let storage = test_storage()?;
        seed_catalog_intents(&storage, 0..3).await?;
        // Acknowledge the intents without trimming them, as a run whose trim
        // was deferred leaves them.
        let drained = CatalogProjectionMaterializer::new(storage.clone())?
            .drain_once()
            .await?;
        assert_eq!(drained.drained_record_ids.len(), 3);
        assert_eq!(pending_records(&storage).await?, 0);
        // Commit refuses at 32 L0 segments; fill the catalog up to the refusal.
        let store = ControlMvpStateStore::new(storage.clone(), catalog_scope())?;
        let mut refused = false;
        for generation in 0..64 {
            match commit_generation(&store, generation).await {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.downcast_ref::<CatalogError>(),
                        Some(CatalogError::MaintenanceBackpressure { .. })
                    ) =>
                {
                    refused = true;
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        assert!(refused, "the catalog must reach commit backpressure");
        // Zero advances leave the consolidation exhausted, so the catalog
        // stays at the threshold when the trim tries to commit.
        let limits = RunLimits {
            maintenance_max_advances: 0,
            ..TEST_LIMITS
        };

        let summary = run_once(storage.clone(), TENANT, WORKSPACE, BINDING, limits).await?;

        assert!(summary.failures.is_empty(), "{:?}", summary.failures);
        assert_eq!(
            domain_entries(&summary, "catalog"),
            vec![("consolidation", MaintenanceOutcome::Exhausted)]
        );
        assert_eq!(
            summary.drain.as_ref().map(|drain| drain.outcome),
            Some(DrainOutcome::Ok),
            "nothing is pending, so the drain commits nothing"
        );
        let trim = trim_summary(&summary)?;
        assert_eq!(trim.outcome, TrimOutcome::Deferred);
        assert_eq!(trim.trimmed_records, 0);
        assert_eq!(trim.trim_sequence, None);
        assert_eq!(
            catalog_outbox_len(&storage).await?,
            3,
            "the refused trim commit removed nothing"
        );
        assert_eq!(
            pending_records(&storage).await?,
            3,
            "the deferred trim retired the acks; the next run re-drains"
        );
        assert_eq!(summary.gc.len(), CONTROL_DOMAINS.len(), "GC still ran");
        Ok(())
    }

    #[test]
    fn trim_defers_on_backpressure_and_coordination_losses_only() {
        let message = || "injected".to_owned();
        assert!(is_trim_deferrable(&CatalogError::MaintenanceBackpressure {
            message: message()
        }));
        assert!(is_trim_deferrable(&CatalogError::PreconditionFailed {
            message: message()
        }));
        assert!(is_trim_deferrable(&CatalogError::CasFailed {
            message: message()
        }));
        assert!(!is_trim_deferrable(&CatalogError::Storage {
            message: message()
        }));
        assert!(!is_trim_deferrable(&CatalogError::Validation {
            message: message()
        }));
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
    fn outcome_log_names_match_their_serde_names() {
        for outcome in [
            EpochOutcome::Absent,
            EpochOutcome::Idle,
            EpochOutcome::InFlight,
            EpochOutcome::Recoverable,
            EpochOutcome::StuckEpoch,
        ] {
            assert_eq!(serde_json::to_value(outcome).unwrap(), outcome.as_str());
        }
        for outcome in [
            MaintenanceOutcome::Idle,
            MaintenanceOutcome::Published,
            MaintenanceOutcome::Deferred,
            MaintenanceOutcome::Terminal,
            MaintenanceOutcome::Exhausted,
        ] {
            assert_eq!(serde_json::to_value(outcome).unwrap(), outcome.as_str());
        }
        for outcome in [DrainOutcome::Ok, DrainOutcome::Deferred] {
            assert_eq!(serde_json::to_value(outcome).unwrap(), outcome.as_str());
        }
        for outcome in [TrimOutcome::Ok, TrimOutcome::Idle, TrimOutcome::Deferred] {
            assert_eq!(serde_json::to_value(outcome).unwrap(), outcome.as_str());
        }
    }

    #[tokio::test]
    async fn selected_job_record_loads_without_a_kind_and_round_trips_with_one() -> Result<()> {
        let storage = test_storage()?;
        // The shape this binary persisted before the record carried a kind.
        let legacy = serde_json::json!({
            "job_id": "a".repeat(64),
            "domain": "catalog",
            "prepared_at_ms": 1_700_000_000_000_i64,
        });
        storage
            .put_raw(
                &selected_job_path("catalog"),
                Bytes::from(serde_json::to_vec(&legacy)?),
                WritePrecondition::None,
            )
            .await?;
        let loaded = load_selected_job(&storage, "catalog")
            .await?
            .ok_or_else(|| anyhow!("a record without a kind must still load"))?;
        assert_eq!(loaded.job_id, "a".repeat(64));
        assert_eq!(loaded.kind, None);
        assert_eq!(
            loaded.kind_label(),
            "consolidation",
            "a record without a kind predates horizon jobs"
        );

        let record = catalog_job_record(
            &"b".repeat(64),
            MaintenanceKind::RetentionHorizon,
            Utc::now(),
        );
        persist_selected_job(&storage, &record).await?;

        assert_eq!(
            load_selected_job(&storage, "catalog").await?,
            Some(record.clone())
        );
        assert_eq!(record.kind_label(), "retention_horizon");
        Ok(())
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
