//! Catalog metrics.
//!
//! Provides metrics for catalog operations including GC, compaction, and CAS retries.
//! These metrics complement the structured logging approach already in place.

use arco_core::CatalogDomain;
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

// ============================================================================
// GC Metrics
// ============================================================================

/// GC objects deleted counter.
pub const GC_OBJECTS_DELETED: &str = "arco_gc_objects_deleted_total";

/// GC bytes reclaimed counter.
pub const GC_BYTES_RECLAIMED: &str = "arco_gc_bytes_reclaimed_total";

/// GC run duration histogram.
pub const GC_RUN_DURATION: &str = "arco_gc_run_duration_seconds";

/// GC errors counter.
pub const GC_ERRORS: &str = "arco_gc_errors_total";

// ============================================================================
// CAS Metrics
// ============================================================================

/// CAS retry counter.
pub const CAS_RETRY: &str = "cas_retry_total";

// ============================================================================
// Idempotency Metrics
// ============================================================================

/// Idempotency check result counter.
pub const IDEMPOTENCY_CHECK: &str = "arco_idempotency_check_total";

/// Idempotency marker takeover counter.
pub const IDEMPOTENCY_TAKEOVER: &str = "arco_idempotency_takeover_total";

// ============================================================================
// Storage Inventory Metrics
// ============================================================================

/// Storage objects by prefix.
pub const STORAGE_OBJECTS_TOTAL: &str = "arco_storage_objects_total";

/// Storage bytes by prefix.
pub const STORAGE_BYTES_TOTAL: &str = "arco_storage_bytes_total";

// ============================================================================
// Event Writer Metrics
// ============================================================================

/// Event writer writes counter.
pub const EVENT_WRITER_WRITTEN: &str = "arco_event_writer_written_total";

/// Event writer bytes written counter.
pub const EVENT_WRITER_BYTES: &str = "arco_event_writer_bytes_written_total";

/// Event writer sequence allocation counter.
pub const EVENT_WRITER_SEQUENCE: &str = "arco_event_writer_sequence_allocation_total";

// ============================================================================
// Read Model and Enforcement Cache Metrics
// ============================================================================

/// Catalog read model cache hit counter.
pub const CATALOG_READ_MODEL_CACHE_HIT: &str = "arco_catalog_read_model_cache_hit_total";

/// Catalog read model cache miss counter.
pub const CATALOG_READ_MODEL_CACHE_MISS: &str = "arco_catalog_read_model_cache_miss_total";

/// Catalog read model refresh duration histogram.
pub const CATALOG_READ_MODEL_REFRESH_SECONDS: &str = "arco_catalog_read_model_refresh_seconds";

/// Storage-governance enforcement cache hit counter.
pub const STORAGE_GOVERNANCE_CACHE_HIT: &str = "arco_storage_governance_cache_hit_total";

/// Storage-governance enforcement cache miss counter.
pub const STORAGE_GOVERNANCE_CACHE_MISS: &str = "arco_storage_governance_cache_miss_total";

/// Storage-governance enforcement refresh duration histogram.
pub const STORAGE_GOVERNANCE_REFRESH_SECONDS: &str = "arco_storage_governance_refresh_seconds";

/// Authorization candidate rows consulted by the compiled-permission index.
pub const AUTHZ_INDEX_CANDIDATE_ROWS: &str = "arco_authz_index_candidate_rows";

// ============================================================================
// ADR-034 Repair Metrics
// ============================================================================

/// Reconciler issues discovered by domain/type.
pub const RECONCILER_ISSUES: &str = "arco_catalog_reconciler_issues_total";

/// Reconciler repair attempts by domain/type/status.
pub const RECONCILER_REPAIRS: &str = "arco_catalog_reconciler_repairs_total";

/// Automated repair executor runs by domain/mode/scope/status.
pub const REPAIR_AUTOMATION_RUNS: &str = "arco_catalog_repair_automation_runs_total";

/// Automated repair executor findings discovered per domain/mode/scope.
pub const REPAIR_AUTOMATION_FINDINGS: &str = "arco_catalog_repair_automation_findings_total";

/// Current repair backlog size by tenant/workspace/domain/mode/scope.
pub const REPAIR_BACKLOG_COUNT: &str = "arco_catalog_repair_backlog_count";

/// Age in seconds of the current repair backlog by tenant/workspace/domain/mode/scope.
pub const REPAIR_BACKLOG_AGE_SECONDS: &str = "arco_catalog_repair_backlog_age_seconds";

/// Automated repair completion latency histogram.
pub const REPAIR_COMPLETION_LATENCY_SECONDS: &str =
    "arco_catalog_repair_completion_latency_seconds";

/// Repeated repair-needed detections for the same tenant/workspace/domain/mode/scope.
pub const REPAIR_REPEAT_FINDINGS_TOTAL: &str = "arco_catalog_repair_repeat_findings_total";

// ============================================================================
// Metric Registration
// ============================================================================

/// Registers all catalog metric descriptions.
///
/// Call this once at application startup after initializing the metrics recorder.
pub fn register_metrics() {
    describe_counter!(GC_OBJECTS_DELETED, "Total objects deleted by GC");
    describe_counter!(GC_BYTES_RECLAIMED, "Total bytes reclaimed by GC");
    describe_histogram!(GC_RUN_DURATION, "Duration of GC runs in seconds");
    describe_counter!(GC_ERRORS, "Total GC errors encountered");
    describe_counter!(CAS_RETRY, "Total CAS retry attempts");
    describe_counter!(EVENT_WRITER_WRITTEN, "Total events written by EventWriter");
    describe_counter!(EVENT_WRITER_BYTES, "Total bytes written by EventWriter");
    describe_counter!(
        EVENT_WRITER_SEQUENCE,
        "Total sequence allocation attempts by EventWriter"
    );
    describe_counter!(
        CATALOG_READ_MODEL_CACHE_HIT,
        "Total catalog read model cache hits"
    );
    describe_counter!(
        CATALOG_READ_MODEL_CACHE_MISS,
        "Total catalog read model cache misses"
    );
    describe_histogram!(
        CATALOG_READ_MODEL_REFRESH_SECONDS,
        "Catalog read model refresh latency in seconds"
    );
    describe_counter!(
        STORAGE_GOVERNANCE_CACHE_HIT,
        "Total storage governance cache hits"
    );
    describe_counter!(
        STORAGE_GOVERNANCE_CACHE_MISS,
        "Total storage governance cache misses"
    );
    describe_histogram!(
        STORAGE_GOVERNANCE_REFRESH_SECONDS,
        "Storage governance cache refresh latency in seconds"
    );
    describe_histogram!(
        AUTHZ_INDEX_CANDIDATE_ROWS,
        "Compiled authorization candidate rows considered per decision"
    );
    describe_gauge!(STORAGE_OBJECTS_TOTAL, "Total objects in storage by prefix");
    describe_gauge!(STORAGE_BYTES_TOTAL, "Total bytes in storage by prefix");
    describe_counter!(IDEMPOTENCY_CHECK, "Total idempotency checks by result");
    describe_counter!(
        IDEMPOTENCY_TAKEOVER,
        "Total idempotency marker takeover attempts by result"
    );
    describe_counter!(
        RECONCILER_ISSUES,
        "Total catalog reconciler issues discovered by domain and issue type"
    );
    describe_counter!(
        RECONCILER_REPAIRS,
        "Total catalog reconciler repair attempts by domain, issue type, and outcome"
    );
    describe_counter!(
        REPAIR_AUTOMATION_RUNS,
        "Total automated catalog repair executor runs by domain, mode, scope, and status"
    );
    describe_counter!(
        REPAIR_AUTOMATION_FINDINGS,
        "Total automated catalog repair executor findings by domain, mode, and scope"
    );
    describe_gauge!(
        REPAIR_BACKLOG_COUNT,
        "Current automated catalog repair backlog count by tenant, workspace, domain, mode, and scope"
    );
    describe_gauge!(
        REPAIR_BACKLOG_AGE_SECONDS,
        "Current automated catalog repair backlog age in seconds by tenant, workspace, domain, mode, and scope"
    );
    describe_histogram!(
        REPAIR_COMPLETION_LATENCY_SECONDS,
        "Automated catalog repair executor completion latency in seconds"
    );
    describe_counter!(
        REPAIR_REPEAT_FINDINGS_TOTAL,
        "Repeated automated catalog repair-needed detections by tenant, workspace, domain, mode, and scope"
    );
}

// ============================================================================
// GC Metric Recording
// ============================================================================

/// Records GC completion metrics.
pub fn record_gc_completion(
    phase: &str,
    objects_deleted: u64,
    bytes_reclaimed: u64,
    duration_secs: f64,
) {
    let labels = [("phase", phase.to_string())];

    counter!(GC_OBJECTS_DELETED, &labels).increment(objects_deleted);
    counter!(GC_BYTES_RECLAIMED, &labels).increment(bytes_reclaimed);
    histogram!(GC_RUN_DURATION, &labels).record(duration_secs);
}

/// Records a GC error.
pub fn record_gc_error(phase: &str) {
    counter!(GC_ERRORS, "phase" => phase.to_string()).increment(1);
}

// ============================================================================
// CAS Metric Recording
// ============================================================================

/// Records a CAS retry attempt.
pub fn record_cas_retry(operation: &str) {
    counter!(CAS_RETRY, "operation" => operation.to_string()).increment(1);
}

// ============================================================================
// Event Writer Metric Recording
// ============================================================================

/// Records an event write (success or duplicate).
pub fn inc_event_writer_written(domain: CatalogDomain, status: &str) {
    counter!(
        EVENT_WRITER_WRITTEN,
        "domain" => domain.as_str().to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

/// Records bytes written by the event writer.
pub fn add_event_writer_bytes_written(domain: CatalogDomain, bytes: u64) {
    counter!(EVENT_WRITER_BYTES, "domain" => domain.as_str().to_string()).increment(bytes);
}

/// Records a sequence allocation attempt.
pub fn inc_event_writer_sequence_allocation(status: &str) {
    counter!(EVENT_WRITER_SEQUENCE, "status" => status.to_string()).increment(1);
}

// ============================================================================
// Read Model and Enforcement Cache Recording
// ============================================================================

/// Records a catalog read model cache hit.
pub fn inc_catalog_read_model_cache_hit() {
    counter!(CATALOG_READ_MODEL_CACHE_HIT).increment(1);
}

/// Records a catalog read model cache miss.
pub fn inc_catalog_read_model_cache_miss() {
    counter!(CATALOG_READ_MODEL_CACHE_MISS).increment(1);
}

/// Records catalog read model refresh latency.
pub fn record_catalog_read_model_refresh(duration_secs: f64) {
    histogram!(CATALOG_READ_MODEL_REFRESH_SECONDS).record(duration_secs);
}

/// Records a storage-governance cache hit.
pub fn inc_storage_governance_cache_hit() {
    counter!(STORAGE_GOVERNANCE_CACHE_HIT).increment(1);
}

/// Records a storage-governance cache miss.
pub fn inc_storage_governance_cache_miss() {
    counter!(STORAGE_GOVERNANCE_CACHE_MISS).increment(1);
}

/// Records storage-governance cache refresh latency.
pub fn record_storage_governance_refresh(duration_secs: f64) {
    histogram!(STORAGE_GOVERNANCE_REFRESH_SECONDS).record(duration_secs);
}

/// Records compiled authorization candidate-row fanout.
#[allow(clippy::cast_precision_loss)]
pub fn record_authz_index_candidate_rows(rows: usize) {
    histogram!(AUTHZ_INDEX_CANDIDATE_ROWS).record(rows as f64);
}

// ============================================================================
// Storage Inventory Recording
// ============================================================================

/// Records storage inventory metrics by prefix.
#[allow(clippy::cast_precision_loss)]
pub fn record_storage_inventory(prefix: &str, objects: u64, bytes: u64) {
    gauge!(
        STORAGE_OBJECTS_TOTAL,
        "prefix" => prefix.to_string()
    )
    .set(objects as f64);
    gauge!(STORAGE_BYTES_TOTAL, "prefix" => prefix.to_string()).set(bytes as f64);
}

// ============================================================================
// Idempotency Metric Recording
// ============================================================================

/// Records an idempotency check result.
pub fn record_idempotency_check(operation: &str, result: &str) {
    counter!(
        IDEMPOTENCY_CHECK,
        "operation" => operation.to_string(),
        "result" => result.to_string()
    )
    .increment(1);
}

/// Records an idempotency marker takeover attempt.
pub fn record_idempotency_takeover(operation: &str, result: &str) {
    counter!(
        IDEMPOTENCY_TAKEOVER,
        "operation" => operation.to_string(),
        "result" => result.to_string()
    )
    .increment(1);
}

// ============================================================================
// ADR-034 Repair Metric Recording
// ============================================================================

/// Records a reconciler issue discovery.
pub fn record_reconciler_issue(domain: CatalogDomain, issue_type: &str, repairable: bool) {
    counter!(
        RECONCILER_ISSUES,
        "domain" => domain.as_str().to_string(),
        "issue_type" => issue_type.to_string(),
        "repairable" => repairable.to_string()
    )
    .increment(1);
}

/// Records a reconciler repair attempt or outcome.
pub fn record_reconciler_repair(domain: CatalogDomain, issue_type: &str, status: &str) {
    counter!(
        RECONCILER_REPAIRS,
        "domain" => domain.as_str().to_string(),
        "issue_type" => issue_type.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

/// Records an automated repair executor run.
pub fn record_repair_automation_run(domain: CatalogDomain, mode: &str, scope: &str, status: &str) {
    counter!(
        REPAIR_AUTOMATION_RUNS,
        "domain" => domain.as_str().to_string(),
        "mode" => mode.to_string(),
        "scope" => scope.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

/// Records findings discovered by the automated repair executor.
pub fn record_repair_automation_findings(
    domain: CatalogDomain,
    mode: &str,
    scope: &str,
    findings: u64,
) {
    counter!(
        REPAIR_AUTOMATION_FINDINGS,
        "domain" => domain.as_str().to_string(),
        "mode" => mode.to_string(),
        "scope" => scope.to_string()
    )
    .increment(findings);
}

/// Sets the current repair backlog count and age.
pub fn set_repair_backlog(
    domain: CatalogDomain,
    tenant_id: &str,
    workspace_id: &str,
    mode: &str,
    scope: &str,
    count: u64,
    age_seconds: f64,
) {
    #[allow(clippy::cast_precision_loss)]
    let count = count as f64;
    gauge!(
        REPAIR_BACKLOG_COUNT,
        "domain" => domain.as_str().to_string(),
        "mode" => mode.to_string(),
        "scope" => scope.to_string(),
        "tenant_id" => tenant_id.to_string(),
        "workspace_id" => workspace_id.to_string()
    )
    .set(count);
    gauge!(
        REPAIR_BACKLOG_AGE_SECONDS,
        "domain" => domain.as_str().to_string(),
        "mode" => mode.to_string(),
        "scope" => scope.to_string(),
        "tenant_id" => tenant_id.to_string(),
        "workspace_id" => workspace_id.to_string()
    )
    .set(age_seconds);
}

/// Records automated repair completion latency.
pub fn record_repair_completion_latency(
    domain: CatalogDomain,
    mode: &str,
    scope: &str,
    duration_secs: f64,
) {
    histogram!(
        REPAIR_COMPLETION_LATENCY_SECONDS,
        "domain" => domain.as_str().to_string(),
        "mode" => mode.to_string(),
        "scope" => scope.to_string()
    )
    .record(duration_secs);
}

/// Records that the same repair-needed backlog was seen again.
pub fn record_repair_repeat(
    domain: CatalogDomain,
    tenant_id: &str,
    workspace_id: &str,
    mode: &str,
    scope: &str,
) {
    counter!(
        REPAIR_REPEAT_FINDINGS_TOTAL,
        "domain" => domain.as_str().to_string(),
        "mode" => mode.to_string(),
        "scope" => scope.to_string(),
        "tenant_id" => tenant_id.to_string(),
        "workspace_id" => workspace_id.to_string()
    )
    .increment(1);
}
