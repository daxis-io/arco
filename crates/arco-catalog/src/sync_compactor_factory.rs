//! Sync compactor factory abstractions for Tier-1 DDL operations (ADR-018).

use std::sync::Arc;

use arco_core::ScopedStorage;

use crate::{SyncCompactor, Tier1Compactor};

/// Factory for creating per-tenant `SyncCompactor` instances.
pub trait SyncCompactorFactory: Send + Sync + 'static {
    /// Creates a `SyncCompactor` for the given scoped storage.
    fn create_compactor(&self, storage: ScopedStorage) -> Arc<dyn SyncCompactor>;
}

/// Factory that creates local `Tier1Compactor` instances.
///
/// Use this for testing or single-node deployments where sync compaction
/// can be performed locally rather than via a remote service.
pub struct Tier1CompactorFactory;

impl SyncCompactorFactory for Tier1CompactorFactory {
    fn create_compactor(&self, storage: ScopedStorage) -> Arc<dyn SyncCompactor> {
        Arc::new(Tier1Compactor::new(storage))
    }
}

/// Factory that shares a single `SyncCompactor` instance across all tenants.
///
/// Use this with remote compactor services where tenant scoping is handled
/// by the service rather than by per-tenant client instances.
pub struct SharedCompactorFactory {
    compactor: Arc<dyn SyncCompactor>,
}

impl SharedCompactorFactory {
    /// Creates a factory that shares the given compactor.
    #[must_use]
    pub fn new(compactor: Arc<dyn SyncCompactor>) -> Self {
        Self { compactor }
    }
}

impl SyncCompactorFactory for SharedCompactorFactory {
    fn create_compactor(&self, _storage: ScopedStorage) -> Arc<dyn SyncCompactor> {
        Arc::clone(&self.compactor)
    }
}
