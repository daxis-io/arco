//! # arco-api
//!
//! HTTP/gRPC composition layer for the Arco serverless lakehouse infrastructure.
//!
//! This crate provides the API surface for Arco, handling:
//!
//! - **Authentication**: Tenant/workspace/user identity from JWTs (user claim defaults to `sub`,
//!   configurable via `ARCO_JWT_USER_CLAIM`)
//! - **Routing**: HTTP and gRPC endpoint configuration
//! - **Service Wiring**: Composition of catalog and flow services
//! - **Observability**: Metrics, tracing, and health checks
//!
//! ## Design Principles
//!
//! This crate is primarily a composition layer, but it also owns the
//! request-scoped control-plane transaction coordinator that composes
//! `arco-catalog` and `arco-flow` into visible catalog, orchestration, and
//! root transaction commits.
//!
//! ## Endpoints
//!
//! ```text
//! HTTP:
//!   GET  /health                 - Health check
//!   GET  /ready                  - Readiness check
//!   /api/v1/namespaces           - Namespace CRUD
//!   /api/v1/namespaces/{ns}/tables - Table CRUD
//!   /api/v1/lineage              - Lineage edge APIs
//!   /api/v1/browser/urls         - Signed URL minting for browser reads
//!   /api/v1/transactions         - Control-plane transaction routes
//!
//! gRPC:
//!   arco.v1.ControlPlaneTransactionService - Transaction commit and lookup APIs
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! use arco_api::server::Server;
//!
//! let server = Server::builder()
//!     .http_port(8080)
//!     .grpc_port(9090)
//!     .build();
//!
//! server.serve().await?;
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::panic, clippy::unwrap_used))]

pub(crate) mod audit;
pub mod compactor_client;
pub mod config;
pub mod context;
pub(crate) mod control_plane_transactions;
pub mod error;
pub(crate) mod grpc_transactions;
pub mod metrics;
pub mod openapi;
pub(crate) mod orchestration_compaction;
pub(crate) mod paths;
pub mod rate_limit;
pub(crate) mod redaction;
pub mod routes;
pub mod server;

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::config::Config;
    pub use crate::context::RequestContext;
    pub use crate::error::{ApiError, ApiResult};
    pub use crate::server::Server;
}
