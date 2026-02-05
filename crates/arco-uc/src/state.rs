//! Shared state and configuration for Unity Catalog facade handlers.

use std::sync::Arc;
use std::time::Duration;

use arco_core::storage::StorageBackend;

/// Server-side configuration for the Unity Catalog facade.
#[derive(Debug, Clone, Default)]
pub struct UnityCatalogConfig {
    /// Optional request timeout for handlers.
    pub request_timeout: Option<Duration>,
    /// Optional concurrency limit for handlers.
    pub concurrency_limit: Option<usize>,
}

/// Shared state for Unity Catalog facade handlers.
#[derive(Clone)]
pub struct UnityCatalogState {
    /// The storage backend for reading/writing UC facade state.
    pub storage: Arc<dyn StorageBackend>,
    /// Server-side configuration.
    pub config: UnityCatalogConfig,
}

impl UnityCatalogState {
    /// Creates new Unity Catalog state with the given storage backend.
    #[must_use]
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            storage,
            config: UnityCatalogConfig::default(),
        }
    }

    /// Creates Unity Catalog state with explicit configuration.
    #[must_use]
    pub fn with_config(storage: Arc<dyn StorageBackend>, config: UnityCatalogConfig) -> Self {
        Self { storage, config }
    }
}
