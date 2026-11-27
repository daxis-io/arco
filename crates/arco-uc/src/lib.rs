//! # arco-uc
//!
//! Unity Catalog OSS API facade for Arco.
//!
//! This crate is responsible for:
//! - Providing a UC-compatible HTTP surface (pinned by the vendored `OpenAPI` contract)
//! - Enforcing tenant/workspace scoping for every request
//! - Preserving Arco invariants (file-native state, compactor sole writer, no
//!   correctness-critical listing)
//! - Generating an `OpenAPI` spec for compliance testing against the pinned UC OSS spec

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

pub mod audit;
pub mod context;
#[cfg(test)]
mod contract;
pub mod error;
mod event_streams;
pub mod openapi;
pub mod permissions;
pub mod router;
pub mod routes;
pub mod state;
pub mod support;

pub use event_streams::{EventStreamBindings, is_event_stream_table};
pub use openapi::{UnityCatalogApiDoc, openapi, openapi_json};
pub use permissions::{CompiledPermissionSource, MetastorePermissionSource};
pub use router::{
    event_stream_router, governed_catalog_read_router, unity_catalog_router, usage_router,
};
pub use routes::delta::config::SUPPORTED_PROTOCOL_VERSIONS as SUPPORTED_UC_DELTA_PROTOCOL_VERSIONS;
pub use routes::event_streams::EventStreamApiDoc;
pub use routes::usage::{UsageApiDoc, UsageBindings, is_usage_table};
pub use state::{GovernedCatalogReadConfig, UnityCatalogConfig, UnityCatalogState};
