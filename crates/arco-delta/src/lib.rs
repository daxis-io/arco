//! Delta Lake support for Arco.
//!
//! This crate provides Arco-native building blocks for working with Delta Lake
//! tables while preserving ARCO's serverless, object-store-first architecture.
//!
//! Current scope:
//! - Mode B commit coordination for managed Delta tables using object-store CAS
//! - Simple staging of commit payloads (server-side upload)
//!
//! Non-goals (for now):
//! - Full Delta protocol validation
//! - Delta log projection / statistics materialization

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

pub mod coordinator;
pub mod error;
pub mod types;

pub use coordinator::{DeltaCommitCoordinator, DeltaCommitCoordinatorConfig};
pub use error::{DeltaError, Result};
pub use types::{
    CommitDeltaRequest, CommitDeltaResponse, DeltaCoordinatorState, InflightCommit, StagedCommit,
};
