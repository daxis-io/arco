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
pub mod contract;
pub mod error;
pub mod openapi;
pub mod router;
pub mod routes;
pub mod state;

pub use openapi::{UnityCatalogApiDoc, openapi, openapi_json};
pub use router::unity_catalog_router;
pub use state::{UnityCatalogConfig, UnityCatalogState};
