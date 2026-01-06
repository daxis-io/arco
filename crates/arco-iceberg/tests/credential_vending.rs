//! Integration tests for credential vending.

use std::sync::Arc;
use std::time::Duration;

use arco_core::storage::MemoryBackend;
#[cfg(feature = "gcp")]
use arco_iceberg::credentials::{DEFAULT_CREDENTIAL_TTL, GcsCredentialConfig};
use arco_iceberg::credentials::{MAX_CREDENTIAL_TTL, MIN_CREDENTIAL_TTL, clamp_ttl};
use arco_iceberg::error::IcebergResult;
use arco_iceberg::state::CredentialRequest;
use arco_iceberg::state::TableInfo;
use arco_iceberg::types::{AccessDelegation, StorageCredential, TableIdent};
use arco_iceberg::{CredentialProvider, IcebergState};
use async_trait::async_trait;

struct MockGcsCredentialProvider {
    token: String,
}

impl MockGcsCredentialProvider {
    fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[async_trait]
impl CredentialProvider for MockGcsCredentialProvider {
    async fn vended_credentials(
        &self,
        request: CredentialRequest,
    ) -> IcebergResult<Vec<StorageCredential>> {
        let location = request.table.location.unwrap_or_default();
        if location.starts_with("gs://") {
            Ok(vec![StorageCredential::gcs(
                location,
                self.token.clone(),
                "2025-01-06T00:00:00Z",
            )])
        } else {
            Ok(vec![])
        }
    }
}

#[test]
fn test_ttl_clamping() {
    assert_eq!(clamp_ttl(Duration::from_secs(30)), MIN_CREDENTIAL_TTL);
    assert_eq!(clamp_ttl(Duration::from_secs(7200)), MAX_CREDENTIAL_TTL);
    assert_eq!(
        clamp_ttl(Duration::from_secs(1800)),
        Duration::from_secs(1800)
    );
}

#[cfg(feature = "gcp")]
#[test]
fn test_default_config() {
    let config = GcsCredentialConfig::default();
    assert_eq!(config.ttl, DEFAULT_CREDENTIAL_TTL);
}

#[cfg(feature = "gcp")]
#[test]
fn test_config_builder() {
    let config = GcsCredentialConfig::default().with_ttl(Duration::from_secs(600));
    assert_eq!(config.ttl, Duration::from_secs(600));
}

#[tokio::test]
async fn test_iceberg_state_with_credentials() {
    let storage = Arc::new(MemoryBackend::new());
    let provider = Arc::new(MockGcsCredentialProvider::new("test-token"));

    let state = IcebergState::new(storage).with_credentials(provider);

    assert!(state.credentials_enabled());
}

#[tokio::test]
async fn test_mock_provider_returns_gcs_credentials() {
    let provider = MockGcsCredentialProvider::new("ya29.test-token");

    let request = CredentialRequest {
        table: TableInfo {
            ident: TableIdent::simple("db", "orders"),
            table_id: "table-123".to_string(),
            location: Some("gs://my-bucket/warehouse/db/orders/".to_string()),
        },
        delegation: AccessDelegation::VendedCredentials,
    };

    let creds = provider.vended_credentials(request).await.unwrap();

    assert_eq!(creds.len(), 1);
    assert_eq!(creds[0].prefix, "gs://my-bucket/warehouse/db/orders/");
    assert_eq!(
        creds[0].config.get("gcs.oauth2.token"),
        Some(&"ya29.test-token".to_string())
    );
}

#[tokio::test]
async fn test_mock_provider_returns_empty_for_non_gcs() {
    let provider = MockGcsCredentialProvider::new("token");

    let request = CredentialRequest {
        table: TableInfo {
            ident: TableIdent::simple("db", "orders"),
            table_id: "table-123".to_string(),
            location: Some("s3://my-bucket/warehouse/".to_string()),
        },
        delegation: AccessDelegation::VendedCredentials,
    };

    let creds = provider.vended_credentials(request).await.unwrap();
    assert!(creds.is_empty());
}

#[test]
fn test_storage_credential_gcs_format() {
    let cred = StorageCredential::gcs("gs://bucket/prefix/", "ya29.token", "2025-01-06T12:00:00Z");

    assert_eq!(cred.prefix, "gs://bucket/prefix/");
    assert_eq!(
        cred.config.get("gcs.oauth2.token"),
        Some(&"ya29.token".to_string())
    );
    assert_eq!(
        cred.config.get("gcs.oauth2.token-expires-at"),
        Some(&"2025-01-06T12:00:00Z".to_string())
    );
}
