//! Shared state and configuration for Unity Catalog facade handlers.

use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use arco_catalog::authz::compiler::CompiledPermissionSet;
use arco_catalog::metastore::publish::PublishedStorageGovernanceCache;

use crate::permissions::CompiledPermissionSource;
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
    /// Optional statically supplied compiled permission view.
    ///
    /// Used by harnesses that pin an exact view. Production wiring supplies
    /// [`Self::permission_source`] instead, which resolves a scope-correct view
    /// per request; when both are present the source wins.
    pub compiled_permissions: Option<Arc<RwLock<Arc<CompiledPermissionSet>>>>,
    /// Authoritative per-scope compiled permission source.
    ///
    /// This is what the production server wires: without it (and without a
    /// static view) every authorized route denies closed with
    /// `permissions_unavailable`.
    pub permission_source: Option<Arc<dyn CompiledPermissionSource>>,
    /// Published storage-governance projection cache for credential decisions.
    pub storage_governance_cache: Arc<PublishedStorageGovernanceCache>,
    /// Optional security audit event emitter.
    pub audit_emitter: Option<AuditEmitter>,
}

impl UnityCatalogState {
    /// Creates new Unity Catalog state with the given storage backend.
    #[must_use]
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            storage,
            config: UnityCatalogConfig::default(),
            compiled_permissions: None,
            permission_source: None,
            storage_governance_cache: Arc::new(PublishedStorageGovernanceCache::default()),
            audit_emitter: None,
        }
    }

    /// Creates Unity Catalog state with explicit configuration.
    #[must_use]
    pub fn with_config(storage: Arc<dyn StorageBackend>, config: UnityCatalogConfig) -> Self {
        Self {
            storage,
            config,
            compiled_permissions: None,
            permission_source: None,
            storage_governance_cache: Arc::new(PublishedStorageGovernanceCache::default()),
            audit_emitter: None,
        }
    }

    /// Adds a compiled permission view for UC compatibility routes.
    #[must_use]
    pub fn with_compiled_permissions(mut self, permissions: CompiledPermissionSet) -> Self {
        self.compiled_permissions = Some(Arc::new(RwLock::new(Arc::new(permissions))));
        self
    }

    /// Wires the authoritative per-scope compiled permission source.
    ///
    /// Required for a deployed server: without it, authorization has no
    /// permission view to evaluate and fails closed for every principal,
    /// including administrators.
    #[must_use]
    pub fn with_permission_source(mut self, source: Arc<dyn CompiledPermissionSource>) -> Self {
        self.permission_source = Some(source);
        self
    }

    /// Adds an audit emitter for UC security decisions.
    #[must_use]
    pub fn with_audit_emitter(mut self, audit_emitter: AuditEmitter) -> Self {
        self.audit_emitter = Some(audit_emitter);
        self
    }
}
