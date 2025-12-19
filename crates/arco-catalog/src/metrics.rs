//! Catalog metrics.
//!
//! Provides metrics for catalog operations including GC, compaction, and CAS retries.
//! These metrics complement the structured logging approach already in place.

use arco_core::CatalogDomain;
use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};

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
