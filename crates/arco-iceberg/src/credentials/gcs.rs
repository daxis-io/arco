//! GCS credential provider for `OAuth2` token vending.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gcp_auth::TokenProvider;
use tracing::{debug, instrument, warn};

struct TokenWithExpiry {
    value: String,
    expires_at: DateTime<Utc>,
}

use crate::error::{IcebergError, IcebergResult};
use crate::state::{CredentialProvider, CredentialRequest};
use crate::types::StorageCredential;

use super::{DEFAULT_CREDENTIAL_TTL, clamp_ttl};

const GCS_SCOPES: &[&str] = &["https://www.googleapis.com/auth/devstorage.read_write"];

/// Configuration for GCS credential vending.
#[derive(Debug, Clone)]
pub struct GcsCredentialConfig {
    /// TTL for vended credentials (clamped to 1-60 minutes).
    pub ttl: Duration,
}

impl Default for GcsCredentialConfig {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_CREDENTIAL_TTL,
        }
    }
}

impl GcsCredentialConfig {
    /// Sets the credential TTL (clamped to allowed range).
    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = clamp_ttl(ttl);
        self
    }
}

/// GCS credential provider using Google Cloud authentication.
pub struct GcsCredentialProvider {
    config: GcsCredentialConfig,
    token_provider: Arc<dyn TokenProvider>,
}

impl std::fmt::Debug for GcsCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsCredentialProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl GcsCredentialProvider {
    /// Creates a new GCS credential provider with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if GCP authentication cannot be initialized.
    pub async fn new(config: GcsCredentialConfig) -> IcebergResult<Self> {
        let token_provider = gcp_auth::provider()
            .await
            .map_err(|e| IcebergError::Internal {
                message: format!("failed to initialize GCP authentication: {e}"),
            })?;

        Ok(Self {
            config,
            token_provider,
        })
    }

    /// Creates a GCS credential provider with default configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if GCP authentication cannot be initialized.
    pub async fn from_environment() -> IcebergResult<Self> {
        Self::new(GcsCredentialConfig::default()).await
    }

    #[instrument(skip(self), level = "debug")]
    async fn fetch_token(&self) -> IcebergResult<TokenWithExpiry> {
        debug!("fetching GCS OAuth2 token");

        let token =
            self.token_provider
                .token(GCS_SCOPES)
                .await
                .map_err(|e| IcebergError::Internal {
                    message: format!("failed to get GCS token: {e}"),
                })?;

        Ok(TokenWithExpiry {
            value: token.as_str().to_string(),
            expires_at: token.expires_at(),
        })
    }

    fn extract_gcs_prefix(location: &str) -> Option<String> {
        if !location.starts_with("gs://") {
            return None;
        }

        let path = &location[5..];
        path.find('/').map_or_else(
            || Some(format!("gs://{path}/")),
            |slash_idx| {
                let bucket = &path[..slash_idx];
                let object_path = &path[slash_idx + 1..];

                if object_path.is_empty() {
                    return Some(format!("gs://{bucket}/"));
                }

                // For directory-like locations (no file extension), include the full path
                // For file paths, extract the directory portion
                let prefix = if object_path.ends_with('/') {
                    object_path.to_string()
                } else if object_path.contains('.') && !object_path.ends_with('/') {
                    // Likely a file path - extract directory
                    let prefix_end = object_path.rfind('/').map_or(0, |idx| idx + 1);
                    format!("{}/", &object_path[..prefix_end].trim_end_matches('/'))
                        .trim_start_matches('/')
                        .to_string()
                } else {
                    // Directory-like path without trailing slash
                    format!("{object_path}/")
                };

                if prefix.is_empty() || prefix == "/" {
                    Some(format!("gs://{bucket}/"))
                } else {
                    Some(format!("gs://{bucket}/{prefix}"))
                }
            },
        )
    }
}

#[async_trait]
impl CredentialProvider for GcsCredentialProvider {
    #[instrument(skip(self), fields(table = %request.table.ident.name))]
    async fn vended_credentials(
        &self,
        request: CredentialRequest,
    ) -> IcebergResult<Vec<StorageCredential>> {
        let location = request
            .table
            .location
            .ok_or_else(|| IcebergError::BadRequest {
                message: "table location required for credential vending".to_string(),
                error_type: "BadRequestException",
            })?;

        if !location.starts_with("gs://") {
            warn!(location = %location, "credential vending requested for non-GCS location");
            return Ok(vec![]);
        }

        let token = self.fetch_token().await?;

        // Use the minimum of configured TTL and actual token expiry
        let ttl_seconds = i64::try_from(self.config.ttl.as_secs()).unwrap_or(3600);
        let configured_expiry = Utc::now() + chrono::Duration::seconds(ttl_seconds);
        let expires_at = configured_expiry.min(token.expires_at);
        let expires_at_str = expires_at.format("%Y-%m-%dT%H:%M:%SZ").to_string();

        let prefix = Self::extract_gcs_prefix(&location).unwrap_or(location);

        debug!(prefix = %prefix, expires_at = %expires_at_str, "vending GCS credentials");

        Ok(vec![StorageCredential::gcs(
            prefix,
            token.value,
            expires_at_str,
        )])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_gcs_prefix_with_path() {
        let prefix = GcsCredentialProvider::extract_gcs_prefix(
            "gs://my-bucket/warehouse/db/table/data/file.parquet",
        );
        assert_eq!(
            prefix,
            Some("gs://my-bucket/warehouse/db/table/data/".to_string())
        );
    }

    #[test]
    fn test_extract_gcs_prefix_bucket_only() {
        let prefix = GcsCredentialProvider::extract_gcs_prefix("gs://my-bucket/");
        assert_eq!(prefix, Some("gs://my-bucket/".to_string()));
    }

    #[test]
    fn test_extract_gcs_prefix_directory_no_trailing_slash() {
        let prefix = GcsCredentialProvider::extract_gcs_prefix("gs://my-bucket/warehouse");
        assert_eq!(prefix, Some("gs://my-bucket/warehouse/".to_string()));
    }

    #[test]
    fn test_extract_gcs_prefix_nested_directory() {
        let prefix = GcsCredentialProvider::extract_gcs_prefix("gs://my-bucket/warehouse/db/table");
        assert_eq!(
            prefix,
            Some("gs://my-bucket/warehouse/db/table/".to_string())
        );
    }

    #[test]
    fn test_extract_gcs_prefix_non_gcs() {
        let prefix = GcsCredentialProvider::extract_gcs_prefix("s3://my-bucket/path");
        assert!(prefix.is_none());
    }

    #[test]
    fn test_config_default() {
        let config = GcsCredentialConfig::default();
        assert_eq!(config.ttl, DEFAULT_CREDENTIAL_TTL);
    }

    #[test]
    fn test_config_with_ttl_clamping() {
        let config = GcsCredentialConfig::default().with_ttl(Duration::from_secs(30));
        assert_eq!(config.ttl, super::super::MIN_CREDENTIAL_TTL);

        let config = GcsCredentialConfig::default().with_ttl(Duration::from_secs(7200));
        assert_eq!(config.ttl, super::super::MAX_CREDENTIAL_TTL);
    }
}
