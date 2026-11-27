//! Shared state and configuration for Unity Catalog facade handlers.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use arco_catalog::authz::compiler::CompiledPermissionSet;
use arco_catalog::metastore::publish::PublishedStorageGovernanceCache;
use arco_catalog::{CatalogAuthorityBindings, Table};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::event_streams::EventStreamBindings;
use crate::permissions::CompiledPermissionSource;
use arco_core::audit::AuditEmitter;
use arco_core::flow_paths::{MANAGED_DELTA_ROOT_PROPERTY, ManagedDeltaRoot};
use arco_core::storage::StorageBackend;
use arco_core::{Error, Result, ScopedStorage};

/// Operator-approved storage for Arco-allocated managed Delta data and logs.
pub struct ManagedDeltaStorage {
    root: ManagedDeltaRoot,
    backend: Arc<dyn StorageBackend>,
}

impl ManagedDeltaStorage {
    /// Binds the approved URI to a backend constructed for its bucket.
    #[must_use]
    pub fn new(root: ManagedDeltaRoot, backend: Arc<dyn StorageBackend>) -> Self {
        Self { root, backend }
    }

    /// Returns the approved root used for allocation.
    #[must_use]
    pub fn root(&self) -> &ManagedDeltaRoot {
        &self.root
    }

    /// Resolves storage for an allocated table after verifying its persisted placement.
    ///
    /// Legacy registrations return `None` and keep their existing storage mapping.
    /// # Errors
    /// Fails closed when the exact configured root, scope, or table URI differs.
    pub fn for_table(
        configured: Option<&Self>,
        catalog: &ScopedStorage,
        table: &Table,
        id: Uuid,
    ) -> Result<Option<ScopedStorage>> {
        let Some(persisted_root) = table
            .properties
            .as_ref()
            .and_then(|properties| properties.get(MANAGED_DELTA_ROOT_PROPERTY))
        else {
            return Ok(None);
        };
        let mismatch = || Error::PreconditionFailed {
            message: "allocated managed Delta root does not match the configured storage binding"
                .to_string(),
        };
        let binding = configured.ok_or_else(mismatch)?;
        let tenant = catalog.tenant_id();
        let workspace = catalog.workspace_id();
        let location = binding.root.table_location(tenant, workspace, id)?;
        if persisted_root != binding.root.as_uri()
            || table.location.as_deref() != Some(location.as_str())
            || table.table_type.as_deref() != Some("MANAGED")
            || table.format.as_deref() != Some("delta")
        {
            return Err(mismatch());
        }
        ScopedStorage::new(Arc::clone(&binding.backend), tenant, workspace)?
            .with_bucket_prefix(binding.root.prefix())
            .map(Some)
    }
}

/// Operator-approved activity tables keyed by `tenant/workspace`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActivityTableBindings(pub(crate) BTreeMap<String, Uuid>);

impl ActivityTableBindings {
    /// Looks up the table bound to the exact authenticated scope.
    #[must_use]
    pub fn resolve(&self, tenant: &str, workspace: &str) -> Option<Uuid> {
        self.0.get(&format!("{tenant}/{workspace}")).copied()
    }
}

/// Server-side configuration for the Unity Catalog facade.
#[derive(Debug, Clone, Default)]
pub struct UnityCatalogConfig {
    /// Optional request timeout for handlers.
    pub request_timeout: Option<Duration>,
    /// Optional concurrency limit for handlers.
    pub concurrency_limit: Option<usize>,
    /// Whether the bounded Arco-native table-read extension is mounted.
    pub table_reads_enabled: bool,
    /// Canonical configured backend root for table reads, if safely mappable.
    pub table_read_storage_root: Option<String>,
    /// Approved event streams, including the existing Logfood table bindings.
    pub event_streams: Arc<EventStreamBindings>,
    /// Approved hourly usage table destinations.
    pub usage_bindings: Arc<crate::UsageBindings>,
    /// Whether the verified JWT ingress serves the native activity extension.
    pub activity_events_enabled: bool,
}

/// Shared state for Unity Catalog facade handlers.
#[derive(Clone)]
pub struct UnityCatalogState {
    /// The storage backend for reading/writing UC facade state.
    pub storage: Arc<dyn StorageBackend>,
    /// Approved analytical storage for allocated Delta tables.
    pub managed_delta_storage: Option<Arc<ManagedDeltaStorage>>,
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
    /// Exact-root catalog authority bindings; unlisted roots remain legacy.
    pub catalog_authority_bindings: Arc<CatalogAuthorityBindings>,
}

impl UnityCatalogState {
    /// Creates new Unity Catalog state with the given storage backend.
    #[must_use]
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            storage,
            managed_delta_storage: None,
            config: UnityCatalogConfig::default(),
            compiled_permissions: None,
            permission_source: None,
            storage_governance_cache: Arc::new(PublishedStorageGovernanceCache::default()),
            audit_emitter: None,
            catalog_authority_bindings: Arc::new(CatalogAuthorityBindings::default()),
        }
    }

    /// Creates Unity Catalog state with explicit configuration.
    #[must_use]
    pub fn with_config(storage: Arc<dyn StorageBackend>, config: UnityCatalogConfig) -> Self {
        Self {
            storage,
            managed_delta_storage: None,
            config,
            compiled_permissions: None,
            permission_source: None,
            storage_governance_cache: Arc::new(PublishedStorageGovernanceCache::default()),
            audit_emitter: None,
            catalog_authority_bindings: Arc::new(CatalogAuthorityBindings::default()),
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

    /// Installs validated exact-root catalog authority bindings.
    #[must_use]
    pub fn with_catalog_authority_bindings(
        mut self,
        bindings: Arc<CatalogAuthorityBindings>,
    ) -> Self {
        self.catalog_authority_bindings = bindings;
        self
    }
}
