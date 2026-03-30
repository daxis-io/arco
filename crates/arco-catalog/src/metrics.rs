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
// ADR-034 Repair Metrics
// ============================================================================

/// Visible commits with repairable side-effect failures.
pub const REPAIR_PENDING: &str = "arco_catalog_repair_pending_total";

/// Reconciler issues discovered by domain/type.
pub const RECONCILER_ISSUES: &str = "arco_catalog_reconciler_issues_total";

/// Reconciler repair attempts by domain/type/status.
pub const RECONCILER_REPAIRS: &str = "arco_catalog_reconciler_repairs_total";

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
    describe_gauge!(STORAGE_OBJECTS_TOTAL, "Total objects in storage by prefix");
    describe_gauge!(STORAGE_BYTES_TOTAL, "Total bytes in storage by prefix");
    describe_counter!(IDEMPOTENCY_CHECK, "Total idempotency checks by result");
    describe_counter!(
        IDEMPOTENCY_TAKEOVER,
        "Total idempotency marker takeover attempts by result"
    );
    describe_counter!(
        REPAIR_PENDING,
        "Total visible commits that require repair of post-commit side effects"
    );
    describe_counter!(
        RECONCILER_ISSUES,
        "Total catalog reconciler issues discovered by domain and issue type"
    );
    describe_counter!(
        RECONCILER_REPAIRS,
        "Total catalog reconciler repair attempts by domain, issue type, and outcome"
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

/// Records a visible commit that still needs side-effect repair.
pub fn record_repair_pending(domain: CatalogDomain, reason: &str) {
    counter!(
        REPAIR_PENDING,
        "domain" => domain.as_str().to_string(),
        "reason" => reason.to_string()
    )
    .increment(1);
}

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
