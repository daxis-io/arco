//! Scheduled `control/` maintenance worker.
//!
//! One invocation drains the catalog projection outbox, then for every control
//! domain runs durable layout maintenance (L0 consolidation) and one bounded
//! pass of conservative garbage collection. The process exits `0` when every
//! phase either completed or deferred to the next run, and non-zero when any
//! phase failed with a typed error.
//!
//! The job must run under the API service account: that account is the sole
//! writer of the `control/` prefix.

use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use arco_catalog::state_store::projection_outbox_acks::{
    PROJECTION_OUTBOX_ACK_DOMAIN, ProjectionMaterializationStatus,
};
use arco_catalog::{
    CatalogError, CatalogProjectionMaterializer, ControlMvpMaintenanceWorker,
    DurableAuthorityBinding, DurableMaintenanceWorker, MaintenanceJobId, MaintenanceProgress,
    MaintenanceStatus, StateScope,
};
use arco_core::ScopedStorage;
use arco_core::observability::{LogFormat, init_logging};
use base64::Engine as _;
use chrono::Utc;
use serde::Serialize;

/// Control domains maintained by every run, in execution order.
const CONTROL_DOMAINS: [&str; 2] = ["catalog", PROJECTION_OUTBOX_ACK_DOMAIN];
const DEFAULT_GC_MAX_PAGES: usize = 16;
const DEFAULT_MAINTENANCE_MAX_ADVANCES: usize = 4096;

/// Per-run work bounds.
#[derive(Debug, Clone, Copy)]
struct RunLimits {
    /// Maximum GC pages collected per domain per run.
    gc_max_pages: usize,
    /// Maximum `advance_at` calls per maintenance job per run.
    maintenance_max_advances: usize,
}

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

#[derive(Debug, Serialize)]
struct DrainSummary {
    drained_records: usize,
    quarantined_records: usize,
    already_acknowledged: usize,
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
            outcome = "ok",
            drained_records = self.drained_records,
            quarantined_records = self.quarantined_records,
            already_acknowledged = self.already_acknowledged,
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
    drain: Option<DrainSummary>,
    maintenance: Vec<DomainMaintenanceSummary>,
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

async fn drain_catalog_projection(storage: ScopedStorage) -> Result<DrainSummary> {
    let started = Instant::now();
    let materializer = CatalogProjectionMaterializer::new(storage)
        .context("construct catalog projection materializer")?;
    let report = materializer
        .drain_once()
        .await
        .context("drain catalog projection outbox")?;
    let status = materializer
        .status()
        .await
        .context("read catalog projection status")?;
    let applied = status
        .as_ref()
        .and_then(ProjectionMaterializationStatus::applied_authority_sequence);
    let observed = status
        .as_ref()
        .and_then(ProjectionMaterializationStatus::observed_authority_sequence);
    let lag = match (observed, applied) {
        (Some(observed), Some(applied)) => Some(observed.saturating_sub(applied)),
        _ => None,
    };
    let age_secs = status
        .as_ref()
        .and_then(ProjectionMaterializationStatus::last_success_at_ms)
        .map(|last_ms| (Utc::now().timestamp_millis().saturating_sub(last_ms)) / 1000);
    if let Some(failure) = status.as_ref().and_then(|s| s.failure_state()) {
        tracing::warn!(
            phase = "drain",
            domain = "catalog",
            failure_state = failure,
            "catalog projection status carries a failure state"
        );
    }
    Ok(DrainSummary {
        drained_records: report.drained_record_ids.len(),
        quarantined_records: report.quarantined_record_ids.len(),
        already_acknowledged: report.already_acknowledged,
        latest_projected_sequence: report.latest_projected_sequence,
        applied_authority_sequence: applied,
        observed_authority_sequence: observed,
        lag,
        age_secs,
        elapsed_ms: elapsed_ms(started),
    })
}

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

/// Prepares, starts, drives and publishes at most one maintenance job.
async fn drive_maintenance(
    worker: &DurableMaintenanceWorker,
    max_advances: usize,
    summary: &mut DomainMaintenanceSummary,
) -> Result<MaintenanceOutcome> {
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
    summary.job_id = Some(job_id.as_str().to_owned());
    tracing::info!(
        phase = "maintenance",
        domain = %summary.domain,
        job_id = job_id.as_str(),
        "maintenance job prepared"
    );
    let Step::Ready(_) = classify_step(
        &summary.domain,
        "start",
        worker.start_at(&plan, Utc::now()).await,
    )?
    else {
        return Ok(MaintenanceOutcome::Deferred);
    };
    let Step::Ready(progress) = classify_step(
        &summary.domain,
        "resume",
        worker.resume_at(&job_id, Utc::now()).await,
    )?
    else {
        return Ok(MaintenanceOutcome::Deferred);
    };
    match advance_until_ready(worker, &job_id, progress, max_advances, summary).await? {
        Advance::Ready => publish_job(worker, &job_id, summary).await,
        Advance::Stopped(outcome) => Ok(outcome),
    }
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
    let worker = DurableMaintenanceWorker::new(storage, scope, binding)
        .with_context(|| format!("construct maintenance worker for domain {domain}"))?;
    let mut summary = DomainMaintenanceSummary::idle(domain);
    let outcome = drive_maintenance(&worker, max_advances, &mut summary).await?;
    summary.outcome = outcome;
    summary.elapsed_ms = elapsed_ms(started);
    Ok(summary)
}

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

/// Runs every phase once. Phases are independent: a failure is recorded and the
/// remaining phases still run, so one wedged domain never starves another.
/// Returns an error when any phase failed with a typed error.
async fn run_once(
    storage: ScopedStorage,
    tenant: &str,
    workspace: &str,
    binding: DurableAuthorityBinding,
    limits: RunLimits,
) -> Result<RunSummary> {
    let started = Instant::now();
    let mut summary = RunSummary::default();

    match drain_catalog_projection(storage.clone()).await {
        Ok(drain) => {
            drain.log();
            summary.drain = Some(drain);
        }
        Err(error) => summary.fail("drain", "catalog", &error),
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
    if failed == 0 {
        Ok(summary)
    } else {
        Err(anyhow!(
            "{failed} control-store phase(s) failed: {}",
            summary.failures.join("; ")
        ))
    }
}

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
    run_once(storage, &tenant, &workspace, binding, limits).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arco_catalog::{ArcoStateTxn as _, ControlMvpStateStore, TxnOptions};
    use arco_core::MemoryBackend;
    use bytes::Bytes;

    use super::*;

    const TEST_LIMITS: RunLimits = RunLimits {
        gc_max_pages: 4,
        maintenance_max_advances: 512,
    };

    async fn commit_generation(store: &ControlMvpStateStore, generation: usize) -> Result<()> {
        let mut tx = store.begin_control_txn(TxnOptions::default()).await?;
        tx.put(b"key", Bytes::from(format!("generation-{generation}")))
            .await?;
        tx.commit().await?;
        Ok(())
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

    #[tokio::test]
    async fn run_once_publishes_pending_catalog_maintenance() -> Result<()> {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")?;
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone())?;
        for generation in 0..16 {
            commit_generation(&store, generation).await?;
        }
        let inspector = ControlMvpMaintenanceWorker::new(storage.clone(), scope)?;
        assert!(
            inspector.pending_intent().await?.is_some(),
            "16 L0 segments must select a maintenance intent"
        );

        let summary = run_once(
            storage.clone(),
            "tenant",
            "workspace",
            DurableAuthorityBinding::new([7; 32]),
            TEST_LIMITS,
        )
        .await?;

        let catalog = domain_summary(&summary, "catalog")?;
        assert_eq!(catalog.outcome, MaintenanceOutcome::Published);
        assert!(catalog.job_id.is_some());
        assert!(catalog.layout_generation.is_some());
        assert_eq!(catalog.completed, catalog.total);
        let acks = domain_summary(&summary, PROJECTION_OUTBOX_ACK_DOMAIN)?;
        assert_eq!(acks.outcome, MaintenanceOutcome::Idle);
        assert!(summary.drain.is_some());
        assert_eq!(summary.gc.len(), CONTROL_DOMAINS.len());
        assert!(summary.failures.is_empty());

        assert!(
            inspector.pending_intent().await?.is_none(),
            "published maintenance clears the pending intent"
        );
        commit_generation(&store, 16).await?;
        Ok(())
    }

    #[tokio::test]
    async fn run_once_is_idle_on_a_consolidated_root() -> Result<()> {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")?;
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )?;
        commit_generation(&store, 0).await?;

        let summary = run_once(
            storage,
            "tenant",
            "workspace",
            DurableAuthorityBinding::new([7; 32]),
            TEST_LIMITS,
        )
        .await?;

        for domain in CONTROL_DOMAINS {
            assert_eq!(
                domain_summary(&summary, domain)?.outcome,
                MaintenanceOutcome::Idle
            );
        }
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
