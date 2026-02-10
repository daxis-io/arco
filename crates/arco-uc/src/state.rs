//! Shared state and configuration for Unity Catalog facade handlers.

use std::sync::Arc;
use std::time::Duration;

use arco_core::audit::AuditEmitter;
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
    /// Optional audit event emitter.
    pub audit_emitter: Option<Arc<AuditEmitter>>,
}

impl UnityCatalogState {
    /// Creates new Unity Catalog state with the given storage backend.
    #[must_use]
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            storage,
            config: UnityCatalogConfig::default(),
            audit_emitter: None,
        }
    }

    /// Creates Unity Catalog state with explicit configuration.
    #[must_use]
    pub fn with_config(storage: Arc<dyn StorageBackend>, config: UnityCatalogConfig) -> Self {
        Self {
            storage,
            config,
            audit_emitter: None,
        }
    }

    /// Attaches an audit emitter used for security decision logging.
    #[must_use]
    pub fn with_audit_emitter(mut self, audit_emitter: Arc<AuditEmitter>) -> Self {
        self.audit_emitter = Some(audit_emitter);
        self
    }
}
