//! Shared state and configuration for Iceberg REST handlers.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use arco_catalog::SyncCompactor;
use arco_core::ScopedStorage;
use arco_core::audit::AuditEmitter;
use arco_core::storage::StorageBackend;

use crate::error::{IcebergError, IcebergResult};
use crate::types::{AccessDelegation, StorageCredential, TableIdent};

/// Server-side configuration for the Iceberg REST API.
#[derive(Debug, Clone)]
pub struct IcebergConfig {
    /// Catalog prefix advertised in `/v1/config`.
    pub prefix: String,
    /// Namespace separator advertised in `/v1/config` (URL-encoded).
    pub namespace_separator: String,
    /// Idempotency key lifetime in ISO 8601 duration format.
    pub idempotency_key_lifetime: Option<String>,
    /// Enable write endpoints in `/v1/config`.
    pub allow_write: bool,
    /// Enable namespace create/delete endpoints.
    pub allow_namespace_crud: bool,
    /// Enable table create/drop/register endpoints.
    pub allow_table_crud: bool,
    /// Optional request timeout for handlers.
    pub request_timeout: Option<Duration>,
    /// Optional concurrency limit for handlers.
    pub concurrency_limit: Option<usize>,
}

impl Default for IcebergConfig {
    fn default() -> Self {
        Self {
            prefix: "arco".to_string(),
            namespace_separator: "%1F".to_string(),
            idempotency_key_lifetime: Some("PT1H".to_string()),
            allow_write: false,
            allow_namespace_crud: false,
            allow_table_crud: false,
            request_timeout: None,
            concurrency_limit: None,
        }
    }
}

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
        Arc::new(arco_catalog::Tier1Compactor::new(storage))
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

impl IcebergConfig {
    /// Returns the decoded namespace separator.
    #[must_use]
    pub fn namespace_separator_decoded(&self) -> String {
        decode_percent(&self.namespace_separator)
    }
}

/// Shared state for Iceberg REST handlers.
#[derive(Clone)]
pub struct IcebergState {
    /// The storage backend for reading pointers and metadata.
    pub storage: Arc<dyn StorageBackend>,
    /// Server-side configuration.
    pub config: IcebergConfig,
    /// Optional credential provider for vended credentials.
    pub credential_provider: Option<Arc<dyn CredentialProvider>>,
    /// Optional factory for creating per-tenant compactors.
    pub compactor_factory: Option<Arc<dyn SyncCompactorFactory>>,
    /// Optional audit emitter for security event logging.
    pub audit_emitter: Option<AuditEmitter>,
}

impl IcebergState {
    /// Creates new Iceberg state with the given storage backend.
    #[must_use]
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            storage,
            config: IcebergConfig::default(),
            credential_provider: None,
            compactor_factory: None,
            audit_emitter: None,
        }
    }

    /// Creates Iceberg state with explicit configuration.
    #[must_use]
    pub fn with_config(storage: Arc<dyn StorageBackend>, config: IcebergConfig) -> Self {
        Self {
            storage,
            config,
            credential_provider: None,
            compactor_factory: None,
            audit_emitter: None,
        }
    }

    /// Attaches a credential provider.
    #[must_use]
    pub fn with_credentials(mut self, provider: Arc<dyn CredentialProvider>) -> Self {
        self.credential_provider = Some(provider);
        self
    }

    /// Attaches a compactor factory for namespace CRUD operations.
    #[must_use]
    pub fn with_compactor_factory(mut self, factory: Arc<dyn SyncCompactorFactory>) -> Self {
        self.compactor_factory = Some(factory);
        self
    }

    /// Attaches an audit emitter for security event logging.
    #[must_use]
    pub fn with_audit_emitter(mut self, emitter: AuditEmitter) -> Self {
        self.audit_emitter = Some(emitter);
        self
    }

    /// Returns a reference to the audit emitter, if configured.
    #[must_use]
    pub fn audit(&self) -> Option<&AuditEmitter> {
        self.audit_emitter.as_ref()
    }

    /// Returns true when credential vending is enabled.
    #[must_use]
    pub fn credentials_enabled(&self) -> bool {
        self.credential_provider.is_some()
    }

    /// Creates a compactor for the given scoped storage.
    ///
    /// # Errors
    ///
    /// Returns `IcebergError::Internal` if no compactor factory is configured.
    pub fn create_compactor(
        &self,
        storage: &ScopedStorage,
    ) -> IcebergResult<Arc<dyn SyncCompactor>> {
        self.compactor_factory
            .as_ref()
            .map(|f| f.create_compactor(storage.clone()))
            .ok_or_else(|| IcebergError::Internal {
                message: "Catalog CRUD operations require a compactor factory".to_string(),
            })
    }
}

/// Information about a table for credential vending.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Fully qualified table identifier.
    pub ident: TableIdent,
    /// Table UUID (Arco ID).
    pub table_id: String,
    /// Table storage location.
    pub location: Option<String>,
}

/// Credential vending request.
#[derive(Debug, Clone)]
pub struct CredentialRequest {
    /// Table info for which credentials are requested.
    pub table: TableInfo,
    /// Delegation mode requested by the client.
    pub delegation: AccessDelegation,
}

/// Provider for storage credentials.
#[async_trait]
pub trait CredentialProvider: Send + Sync + 'static {
    /// Returns storage credentials for a table.
    async fn vended_credentials(
        &self,
        request: CredentialRequest,
    ) -> IcebergResult<Vec<StorageCredential>>;
}

fn decode_percent(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();

    while let Some(ch) = chars.next() {
        if ch == '%' {
            let hi = chars.next();
            let lo = chars.next();
            if let (Some(hi), Some(lo)) = (hi, lo) {
                if let (Some(hi), Some(lo)) = (hi.to_digit(16), lo.to_digit(16)) {
                    // Safe: combining two 4-bit values always fits in u8
                    #[allow(clippy::cast_possible_truncation)]
                    let byte = ((hi << 4) | lo) as u8;
                    out.push(byte as char);
                    continue;
                }
                out.push('%');
                out.push(hi);
                out.push(lo);
                continue;
            }
        }
        out.push(ch);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_separator_decoding() {
        let config = IcebergConfig::default();
        let decoded = config.namespace_separator_decoded();
        assert_eq!(decoded.as_bytes(), &[0x1F]);
    }

    #[test]
    fn test_decode_percent_passthrough() {
        let decoded = decode_percent("..");
        assert_eq!(decoded, "..");
    }
}
