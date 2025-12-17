//! # arco-catalog
//!
//! Catalog service for the Arco serverless lakehouse infrastructure.
//!
//! This crate implements the catalog domain, providing:
//!
//! - **Asset Registry**: Discover and manage data assets (tables, views, etc.)
//! - **Lineage Tracking**: Execution-based lineage captured from real runs
//! - **Search**: Fast discovery of assets across the catalog
//! - **Parquet-Native Storage**: Metadata stored as queryable Parquet files
//!
//! ## Architecture
//!
//! The catalog uses a two-tier consistency model:
//!
//! - **Tier 1 (Strong Consistency)**: Low-frequency DDL operations (create/drop)
//!   use atomic snapshot + manifest writes for immediate consistency
//! - **Tier 2 (Eventual Consistency)**: High-volume operational facts (lineage events,
//!   run metadata) are written to an append-only event log and periodically compacted
//!
//! ## Storage Layout
//!
//! All catalog data is scoped to tenant/workspace isolation boundaries using
//! key=value path segments (grep-friendly, self-documenting):
//!
//! ```text
//! tenant={tenant}/workspace={workspace}/
//! ├── manifests/                    # Tier 1: Multi-file manifest structure
//! │   ├── root.manifest.json        # Root manifest (points to domain manifests)
//! │   ├── catalog.manifest.json     # Catalog state (locked writes)
//! │   ├── lineage.manifest.json     # Lineage state (locked writes)
//! │   ├── executions.manifest.json  # Execution state (compactor writes)
//! │   └── search.manifest.json      # Search state (locked writes)
//! ├── locks/
//! │   ├── catalog.lock.json         # Distributed lock per domain
//! │   ├── lineage.lock.json
//! │   ├── executions.lock.json
//! │   └── search.lock.json
//! ├── commits/                      # Tier 1 audit chain (per domain)
//! ├── snapshots/                    # Tier 1: Immutable snapshots (Parquet)
//! └── ledger/                       # Tier 2: Append-only event log
//! ```
//!
//! The multi-file manifest structure reduces contention by separating domains
//! (catalog, lineage, executions, search) into independent files.
//!
//! ## Example
//!
//! ```rust,ignore
//! use arco_catalog::Tier1Writer;
//! use arco_core::ScopedStorage;
//!
//! // Create workspace-scoped storage
//! let storage = ScopedStorage::new(backend, "acme-corp", "production")?;
//!
//! // Initialize catalog manifests (idempotent)
//! let writer = Tier1Writer::new(storage);
//! writer.initialize().await?;
//!
//! // Update catalog with CAS semantics
//! let commit = writer.update(|manifest| {
//!     manifest.snapshot_version = 1;
//!     Ok(())
//! }).await?;
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

pub mod asset;
pub mod compactor;
pub mod error;
pub mod event_writer;
pub mod lock;
pub mod manifest;
pub mod reader;
pub mod tier1_writer;
pub mod writer;

// Re-export main types at crate root
pub use asset::{Asset, AssetFormat, AssetKey, AssetKeyError, CreateAssetRequest};
pub use compactor::{CompactionResult, Compactor, MaterializationRecord};
pub use error::{CatalogError, Result};
pub use event_writer::EventWriter;
pub use lock::{DistributedLock, LockGuard, LockInfo};
pub use manifest::{
    CatalogDomainManifest, CatalogManifest, CommitRecord, CompactionMetadata, CoreManifest,
    ExecutionManifest, ExecutionsManifest, GovernanceManifest, LineageManifest, RootManifest,
    SearchManifest,
};
pub use tier1_writer::Tier1Writer;

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::asset::{Asset, AssetFormat, AssetKey, CreateAssetRequest};
    pub use crate::manifest::{CatalogDomainManifest, CatalogManifest, CommitRecord, RootManifest};
    pub use crate::reader::CatalogReader;
    pub use crate::tier1_writer::Tier1Writer;
    pub use crate::writer::CatalogWriter;
}
