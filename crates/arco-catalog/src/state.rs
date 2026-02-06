//! In-memory snapshot state for Tier-1 catalog domains.
//!
//! Catalog Tier-1 writes produce immutable Parquet snapshots.
//! During a write, we load the current snapshot into these in-memory structs,
//! apply changes, then write a new snapshot.

use crate::parquet_util::{
    CatalogRecord, ColumnRecord, LineageEdgeRecord, NamespaceRecord, SearchPostingRecord,
    TableRecord,
};

/// In-memory state for the catalog domain (namespaces, tables, columns).
#[derive(Debug, Clone, Default)]
pub struct CatalogState {
    /// Catalog records.
    pub catalogs: Vec<CatalogRecord>,
    /// Namespace records.
    pub namespaces: Vec<NamespaceRecord>,
    /// Table records.
    pub tables: Vec<TableRecord>,
    /// Column records.
    pub columns: Vec<ColumnRecord>,
}

impl CatalogState {
    /// Returns an empty catalog state.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }
}

/// In-memory state for the lineage domain (edges).
#[derive(Debug, Clone, Default)]
pub struct LineageState {
    /// Lineage edges.
    pub edges: Vec<LineageEdgeRecord>,
}

impl LineageState {
    /// Returns an empty lineage state.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }
}

/// In-memory state for the search domain (token postings).
#[derive(Debug, Clone, Default)]
pub struct SearchState {
    /// Search posting records.
    pub postings: Vec<SearchPostingRecord>,
}

impl SearchState {
    /// Returns an empty search state.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }
}
