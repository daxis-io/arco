//! Catalog read operations.
//!
//! The catalog reader provides query-native access to catalog metadata.
//! It reads directly from object storage using signed URLs, enabling
//! browser and server query engines to access metadata without
//! always-on read infrastructure.

use arco_core::{Result, TenantId};

/// Reader for catalog metadata.
///
/// Provides fast, consistent reads of catalog state by reading
/// the latest snapshot and merging with recent events.
#[derive(Debug)]
pub struct CatalogReader {
    tenant: TenantId,
}

impl CatalogReader {
    /// Creates a new catalog reader for the given tenant.
    #[must_use]
    pub fn new(tenant: TenantId) -> Self {
        Self { tenant }
    }

    /// Returns the tenant ID this reader is scoped to.
    #[must_use]
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Lists all assets in the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be read.
    pub async fn list_assets(&self) -> Result<Vec<String>> {
        // TODO: Implement catalog listing
        Ok(Vec::new())
    }
}
