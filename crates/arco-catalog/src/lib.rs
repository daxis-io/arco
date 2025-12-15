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
//! │   ├── core.manifest.json        # Core catalog state (assets, versions)
//! │   ├── execution.manifest.json   # Execution state (watermarks, compaction)
//! │   ├── lineage.manifest.json     # Optional: Lineage domain manifest
//! │   └── governance.manifest.json  # Optional: Governance domain manifest
//! ├── locks/
//! │   └── core.lock                 # Distributed lock for Tier 1 operations
//! ├── core/
//! │   ├── snapshots/                # Tier 1: Immutable catalog snapshots (Parquet)
//! │   └── commits/                  # Commit records (audit trail)
//! └── events/                       # Tier 2: Append-only event log
//! ```
//!
//! The multi-file manifest structure reduces contention by separating domains
//! (core, execution, lineage, governance) into independent files.
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
//!     manifest.core.snapshot_version = 1;
//!     Ok(())
//! }).await?;
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]

pub mod asset;
pub mod lock;
pub mod manifest;
pub mod reader;
pub mod tier1_writer;
pub mod writer;

// Re-export main types at crate root
pub use asset::{Asset, AssetFormat, AssetKey, AssetKeyError, CreateAssetRequest};
pub use lock::{DistributedLock, LockGuard, LockInfo};
pub use manifest::{
    CatalogManifest, CommitRecord, CoreManifest, ExecutionManifest, GovernanceManifest,
    LineageManifest, RootManifest,
};
pub use tier1_writer::Tier1Writer;

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::asset::{Asset, AssetFormat, AssetKey, CreateAssetRequest};
    pub use crate::manifest::{CatalogManifest, CommitRecord, CoreManifest, RootManifest};
    pub use crate::reader::CatalogReader;
    pub use crate::tier1_writer::Tier1Writer;
    pub use crate::writer::CatalogWriter;
}
