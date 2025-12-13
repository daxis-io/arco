//! # arco-proto
//!
//! Protobuf definitions and generated code for Arco.
//!
//! This crate provides the cross-language contracts used by:
//!
//! - gRPC services (Rust server, Python SDK)
//! - Event serialization (Tier 2 event log)
//! - Queue messages (pub/sub, task queues)
//!
//! ## Proto File Organization
//!
//! ```text
//! proto/arco/v1/
//! ├── catalog.proto     - Catalog service definitions
//! ├── flow.proto        - Flow service definitions
//! ├── common.proto      - Shared message types
//! └── events.proto      - Event types for Tier 2
//! ```
//!
//! ## Wire Format Guarantees
//!
//! - All messages follow Protobuf evolution rules
//! - New fields are always optional or have defaults
//! - Field numbers are never reused
//! - Removed fields are reserved
//!
//! ## Example
//!
//! ```rust,ignore
//! use arco_proto::v1::{Asset, ListAssetsRequest};
//!
//! let request = ListAssetsRequest {
//!     tenant_id: "acme-corp".to_string(),
//!     ..Default::default()
//! };
//! ```

#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]
// Allow generated code patterns
#![allow(clippy::derive_partial_eq_without_eq)]
#![allow(clippy::default_trait_access)]

/// Version 1 of the Arco protocol.
pub mod v1 {
    // Include generated code here once proto files are compiled
    // tonic::include_proto!("arco.v1");

    // Placeholder types until proto generation is set up

    /// A catalog asset.
    #[derive(Debug, Clone, Default)]
    pub struct Asset {
        /// Asset identifier.
        pub id: String,
        /// Asset name.
        pub name: String,
    }

    /// Request to list assets.
    #[derive(Debug, Clone, Default)]
    pub struct ListAssetsRequest {
        /// Tenant identifier.
        pub tenant_id: String,
    }

    /// Response containing assets.
    #[derive(Debug, Clone, Default)]
    pub struct ListAssetsResponse {
        /// The assets.
        pub assets: Vec<Asset>,
    }
}
