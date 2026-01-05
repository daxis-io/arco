//! # arco-iceberg
//!
//! Iceberg REST Catalog integration for Arco.
//!
//! This crate implements the [Apache Iceberg REST Catalog API](https://iceberg.apache.org/spec/#rest-catalog)
//! with Arco-specific extensions for:
//!
//! - **CAS-based commits**: Atomic pointer updates using object storage conditional writes
//! - **Durable idempotency**: Two-phase markers ensuring exactly-once commit semantics
//! - **Credential vending**: Short-lived storage credentials via `X-Iceberg-Access-Delegation`
//! - **Event receipts**: Immutable commit records for reconciliation and lineage
//!
//! ## Architecture
//!
//! The Iceberg integration follows a layered design:
//!
//! 1. **Pointer Layer**: `IcebergTablePointer` tracks current metadata location per table
//! 2. **Idempotency Layer**: Durable markers enable crash recovery and replay protection
//! 3. **Event Layer**: Pending/committed receipts feed compaction and lineage tracking
//!
//! ## Authority Model
//!
//! For tables with `format = ICEBERG`:
//! - Iceberg metadata is authoritative for schema, partitions, snapshots
//! - Arco catalog is authoritative for identity, governance, storage location
//! - Arco schema registry is a projection with bounded staleness
//!
//! ## Example
//!
//! ```rust,ignore
//! use arco_iceberg::router::iceberg_router;
//! use arco_core::ScopedStorage;
//!
//! // Mount the Iceberg REST router
//! let app = axum::Router::new()
//!     .nest("/iceberg", iceberg_router(state));
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]

pub mod commit;
pub mod context;
pub mod error;
pub mod events;
pub mod gc;
pub mod idempotency;
pub mod metrics;
pub mod openapi;
pub(crate) mod paths;
pub mod pointer;
pub mod reconciler;
pub mod router;
pub mod schema_projection;
pub mod state;
pub mod types;

// Route handlers (exposed for OpenAPI generation)
pub mod routes;

/// Prelude module for convenient imports.
pub mod prelude {
    // Error types
    pub use crate::error::{IcebergError, IcebergResult};

    // Pointer and idempotency
    pub use crate::idempotency::{IdempotencyMarker, IdempotencyStatus};
    pub use crate::pointer::{IcebergTablePointer, PointerStore};

    // Router and state
    pub use crate::router::iceberg_router;
    pub use crate::state::{CredentialProvider, IcebergConfig, IcebergState};

    // Events
    pub use crate::events::{CommittedReceipt, PendingReceipt};

    // Reconciler
    pub use crate::reconciler::{
        IcebergReconciler, Reconciler, ReconciliationReport, TableReconciliationResult,
    };

    // GC
    pub use crate::gc::{
        EventReceiptGarbageCollector, IdempotencyGarbageCollector, MetadataGcPlan,
        MetadataGcPlanner, MetadataGcResult, MetadataRetentionPolicy, OrphanMetadataCleaner,
    };

    // Schema projection
    pub use crate::schema_projection::{ColumnRecord, IcebergTypeMapper, SchemaProjector};

    // All types
    pub use crate::types::*;
}

// Re-export key types at crate root
pub use error::{IcebergError, IcebergResult};
pub use openapi::{IcebergApiDoc, openapi, openapi_json};
pub use pointer::IcebergTablePointer;
pub use router::iceberg_router;
pub use state::{
    CredentialProvider, IcebergConfig, IcebergState, SharedCompactorFactory, SyncCompactorFactory,
    Tier1CompactorFactory,
};

// Re-export reconciler types
pub use reconciler::{IcebergReconciler, Reconciler, ReconciliationReport};

// Re-export GC types
pub use gc::{
    EventReceiptGarbageCollector, IdempotencyGarbageCollector, MetadataGcPlan, MetadataGcPlanner,
    MetadataGcResult, MetadataRetentionPolicy, OrphanMetadataCleaner,
};

// Re-export schema projection types
pub use schema_projection::{ColumnRecord, IcebergTypeMapper, SchemaProjector};
