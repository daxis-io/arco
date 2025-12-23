//! Iceberg REST Catalog request and response types.
//!
//! These types match the Apache Iceberg REST Catalog `OpenAPI` specification.

pub mod commit;
mod config;
mod credentials;
mod ids;
mod namespace;
mod table;

pub use config::*;
pub use credentials::*;
pub use commit::*;
pub use ids::*;
pub use namespace::*;
pub use table::*;
