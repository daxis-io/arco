//! # arco-api
//!
//! HTTP/gRPC composition layer for the Arco serverless lakehouse infrastructure.
//!
//! This crate provides the API surface for Arco, handling:
//!
//! - **Authentication**: Tenant identification and authorization
//! - **Routing**: HTTP and gRPC endpoint configuration
//! - **Service Wiring**: Composition of catalog and flow services
//! - **Observability**: Metrics, tracing, and health checks
//!
//! ## Design Principles
//!
//! This crate is a **thin composition layer** with no domain policy.
//! All business logic lives in `arco-catalog` and `arco-flow`.
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
//!
//! gRPC:
//!   arco.v1.CatalogService - Catalog operations
//!   arco.v1.FlowService    - Orchestration operations
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

pub mod config;
pub mod compactor_client;
pub mod context;
pub mod error;
pub mod metrics;
pub mod openapi;
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
