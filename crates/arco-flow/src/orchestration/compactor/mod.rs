//! Orchestration compactor for event-to-Parquet projection.
//!
//! The compactor is the sole writer of Parquet files (IAM-enforced per ADR-018).
//! It processes events from the ledger and produces:
//!
//! - **Base snapshots**: Full Parquet files periodically merged
//! - **L0 deltas**: Small Parquet files for near-real-time visibility
//!
//! Controllers read base + L0 deltas from the manifest (never the ledger).

pub mod fold;
pub mod manifest;
pub mod parquet_util;
pub mod service;

pub use fold::*;
pub use manifest::*;
pub use parquet_util::*;
pub use service::{CompactionResult, MicroCompactor};
