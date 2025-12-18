//! Garbage collection for catalog artifacts.
//!
//! This module provides retention policy management and garbage collection for:
//!
//! - **Orphaned snapshots**: Snapshot directories not referenced by any manifest
//! - **Old ledger events**: Events compacted beyond the retention window
//! - **Old snapshot versions**: Snapshot versions beyond the retention count
//!
//! # Why GC is MVP Critical
//!
//! Without retention/GC, storage grows unbounded and costs explode. Every write
//! creates artifacts that must eventually be cleaned up:
//!
//! - Tier-1 snapshots: Old versions accumulate after each DDL change
//! - Tier-2 ledger: Processed events remain after compaction
//! - Orphaned files: Failed CAS operations leave unreferenced snapshots
//!
//! Without GC, a workspace that writes 100 events/day accumulates ~36,500
//! orphaned files/year.
//!
//! # Usage
//!
//! ```rust,ignore
//! use arco_catalog::gc::{GarbageCollector, RetentionPolicy};
//!
//! let policy = RetentionPolicy::default();
//! let collector = GarbageCollector::new(storage, policy);
//!
//! // Dry run first
//! let report = collector.collect_dry_run().await?;
//! println!("Would delete {} objects", report.objects_to_delete);
//!
//! // Actually collect
//! let result = collector.collect().await?;
//! println!("Deleted {} objects, reclaimed {} bytes", result.objects_deleted, result.bytes_reclaimed);
//! ```

mod collector;
mod policy;

pub use collector::{GarbageCollector, GcReport, GcResult};
pub use policy::RetentionPolicy;
