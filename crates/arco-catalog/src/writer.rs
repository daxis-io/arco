//! Catalog write operations.
//!
//! The catalog writer handles all mutations to catalog state,
//! implementing the two-tier consistency model:
//!
//! - Tier 1 writes (DDL) are immediately consistent via atomic snapshots
//! - Tier 2 writes (events) are append-only and eventually consistent

use arco_core::{Result, TenantId};

/// Writer for catalog mutations.
///
/// Handles both Tier 1 (strongly consistent) and Tier 2 (eventually consistent)
/// write operations to the catalog.
#[derive(Debug)]
pub struct CatalogWriter {
    tenant: TenantId,
}

impl CatalogWriter {
    /// Creates a new catalog writer for the given tenant.
    #[must_use]
    pub const fn new(tenant: TenantId) -> Self {
        Self { tenant }
    }

    /// Returns the tenant ID this writer is scoped to.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Registers a new asset in the catalog.
    ///
    /// This is a Tier 1 operation with strong consistency guarantees.
    ///
    /// # Errors
    ///
    /// Returns an error if the asset cannot be registered.
    #[allow(clippy::unused_async)]
    pub async fn register_asset(&self, _name: &str) -> Result<()> {
        // TODO: Implement asset registration
        Ok(())
    }
}
