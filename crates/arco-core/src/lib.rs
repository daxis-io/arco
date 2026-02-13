//! # arco-core
//!
//! Core abstractions for the Arco serverless lakehouse infrastructure.
//!
//! This crate provides the foundational types and traits used across all Arco components:
//!
//! - **Tenant Context**: Multi-tenant isolation primitives
//! - **Identifiers**: Strongly-typed IDs for assets, runs, and other entities
//! - **Storage Traits**: Abstract storage interfaces for catalog and orchestration
//! - **Error Types**: Shared error definitions and result types
//! - **Serialization Helpers**: Canonical encoding for deterministic snapshots
//!
//! ## Crate Boundary
//!
//! `arco-core` is the **only** crate allowed to define shared primitives.
//! All cross-component interaction happens via explicitly versioned contracts
//! defined in this crate.
//!
//! ## Example
//!
//! ```rust
//! use arco_core::prelude::*;
//!
//! // Create a tenant context
//! let tenant = TenantId::new("acme-corp");
//!
//! // Generate a unique asset ID
//! let asset_id = AssetId::generate();
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]

pub mod audit;
pub mod backpressure;
pub mod canonical_json;
pub mod catalog_event;
pub mod catalog_paths;
pub mod error;
pub mod flow_paths;
pub mod id;
pub mod internal_oidc;
pub mod lock;
pub mod observability;
pub mod partition;
pub mod publish;
pub mod scoped_storage;
pub mod storage;
pub mod storage_keys;
pub mod storage_traits;
pub mod sync_compact;
pub mod tenant;

/// Prelude module for convenient imports.
///
/// # Example
///
/// ```rust
/// use arco_core::prelude::*;
/// ```
pub mod prelude {
    pub use crate::catalog_event::{CatalogEvent, CatalogEventPayload};
    pub use crate::catalog_paths::{CatalogDomain, CatalogPaths};
    pub use crate::error::{Error, Result};
    pub use crate::id::{AssetId, EventId, MaterializationId, RunId, TaskId};
    pub use crate::lock::{DistributedLock, LockGuard, LockInfo};
    pub use crate::partition::{PartitionId, PartitionKey, PartitionKeyParseError, ScalarValue};
    pub use crate::publish::{FencingToken, PermitIssuer, PublishPermit, Publisher};
    pub use crate::scoped_storage::ScopedStorage;
    pub use crate::storage::{
        MemoryBackend, ObjectMeta, ObjectStoreBackend, StorageBackend, WritePrecondition,
        WriteResult,
    };
    pub use crate::storage_keys::{
        CommitKey, LedgerKey, LockKey, ManifestKey, QuarantineKey, SequenceKey, StateKey,
        StorageKey,
    };
    pub use crate::storage_traits::{
        CasStore, CommitPutStore, LedgerPutStore, ListStore, LockPutStore, MetaStore, ReadStore,
        SignedUrlStore, StatePutStore,
    };
    pub use crate::sync_compact::{SyncCompactRequest, SyncCompactResponse};
    pub use crate::tenant::TenantId;
}

// Re-export key types at crate root for ergonomics
pub use catalog_event::{CatalogEvent, CatalogEventPayload};
pub use catalog_paths::{CatalogDomain, CatalogPaths};
pub use error::{Error, Result};
pub use flow_paths::FlowPaths;
pub use id::{AssetId, EventId, MaterializationId, RunId, TaskId};
pub use internal_oidc::{
    InternalOidcConfig, InternalOidcError, InternalOidcVerifier, VerifiedPrincipal,
};
pub use lock::{DistributedLock, LockGuard, LockInfo};
pub use observability::{LogFormat, Redacted, init_logging};
pub use partition::{PartitionId, PartitionKey, PartitionKeyParseError, ScalarValue};
pub use scoped_storage::ScopedStorage;
pub use storage::{
    MemoryBackend, ObjectMeta, ObjectStoreBackend, StorageBackend, WritePrecondition, WriteResult,
};
pub use sync_compact::{SyncCompactRequest, SyncCompactResponse};
pub use tenant::TenantId;
