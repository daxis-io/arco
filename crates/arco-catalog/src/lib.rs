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
//! ```text
//! {tenant_prefix}/
//! ├── catalog/
//! │   ├── snapshots/           # Tier 1: Immutable catalog snapshots (Parquet)
//! │   ├── manifests/           # Tier 1: Manifest files pointing to snapshots
//! │   └── events/              # Tier 2: Append-only event log
//! └── compaction/
//!     └── checkpoints/         # Compaction state
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! use arco_catalog::CatalogReader;
//! use arco_core::TenantId;
//!
//! let tenant = TenantId::new("acme-corp")?;
//! let reader = CatalogReader::new(store, tenant);
//!
//! // Discover all assets
//! let assets = reader.list_assets().await?;
//!
//! // Get lineage for an asset
//! let lineage = reader.get_lineage(asset_id).await?;
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
