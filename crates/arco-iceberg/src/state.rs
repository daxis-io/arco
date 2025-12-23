//! Shared state and configuration for Iceberg REST handlers.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use arco_core::storage::StorageBackend;

use crate::error::IcebergResult;
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
            request_timeout: None,
            concurrency_limit: None,
        }
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
}

impl IcebergState {
    /// Creates new Iceberg state with the given storage backend.
    #[must_use]
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            storage,
            config: IcebergConfig::default(),
            credential_provider: None,
        }
    }

    /// Creates Iceberg state with explicit configuration.
    #[must_use]
    pub fn with_config(storage: Arc<dyn StorageBackend>, config: IcebergConfig) -> Self {
        Self {
            storage,
            config,
            credential_provider: None,
        }
    }

    /// Attaches a credential provider.
    #[must_use]
    pub fn with_credentials(mut self, provider: Arc<dyn CredentialProvider>) -> Self {
        self.credential_provider = Some(provider);
        self
    }

    /// Returns true when credential vending is enabled.
    #[must_use]
    pub fn credentials_enabled(&self) -> bool {
        self.credential_provider.is_some()
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
