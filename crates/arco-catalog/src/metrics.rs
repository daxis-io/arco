//! Catalog metrics.
//!
//! Provides metrics for catalog operations including GC, compaction, and CAS retries.
//! These metrics complement the structured logging approach already in place.

use metrics::{counter, describe_counter, describe_histogram, histogram};

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
pub const CAS_RETRY: &str = "arco_cas_retry_total";

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
