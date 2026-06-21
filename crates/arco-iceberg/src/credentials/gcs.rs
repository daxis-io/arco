//! GCS credential provider for storage access delegation.

use std::time::Duration;

use async_trait::async_trait;
use tracing::{instrument, warn};

use crate::error::{IcebergError, IcebergResult};
use crate::metrics::{record_credential_vending, record_credential_vending_duration};
use crate::state::{CredentialProvider, CredentialRequest};
use crate::types::StorageCredential;

use super::{DEFAULT_CREDENTIAL_TTL, clamp_ttl};

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
    /// This constructor is infallible today. It keeps a result return type so a
    /// future downscoped-token implementation can surface provider setup
    /// failures without changing the public API.
    #[allow(clippy::unused_async)]
    pub async fn new(config: GcsCredentialConfig) -> IcebergResult<Self> {
        Ok(Self { config })
    }

    /// Creates a GCS credential provider with default configuration.
    ///
    /// # Errors
    ///
    /// See [`Self::new`].
    pub async fn from_environment() -> IcebergResult<Self> {
        Self::new(GcsCredentialConfig::default()).await
    }

    /// Normalizes a GCS table location into a prefix for credential scoping.
    ///
    /// Iceberg table locations are expected to be directory paths. This function
    /// normalizes the path by ensuring a trailing slash, without attempting to
    /// parse or validate the path structure.
    ///
    /// # Contract
    ///
    /// - Input MUST be a directory path (table location), not a file path
    /// - Output is the same path with a normalized trailing slash
    /// - Invalid inputs (empty bucket, non-GCS scheme) return `None`
    ///
    /// # Examples
    ///
    /// - `gs://bucket/warehouse/db/table/` → `gs://bucket/warehouse/db/table/`
    /// - `gs://bucket/warehouse/db/table` → `gs://bucket/warehouse/db/table/`
    /// - `gs://bucket/` → `gs://bucket/`
    /// - `gs://bucket` → `gs://bucket/`
    /// - `gs://` → `None` (invalid: empty bucket)
    /// - `s3://bucket/path` → `None` (not GCS)
    fn extract_gcs_prefix(location: &str) -> Option<String> {
        if !location.starts_with("gs://") {
            return None;
        }

        let path_after_scheme = &location[5..];

        if path_after_scheme.is_empty() {
            return None;
        }

        let (bucket, object_path) =
            path_after_scheme
                .find('/')
                .map_or((path_after_scheme, ""), |slash_idx| {
                    (
                        &path_after_scheme[..slash_idx],
                        &path_after_scheme[slash_idx + 1..],
                    )
                });

        if bucket.is_empty() {
            return None;
        }

        if object_path.is_empty() {
            Some(format!("gs://{bucket}/"))
        } else {
            let normalized = object_path.trim_matches('/');
            if normalized.is_empty() {
                Some(format!("gs://{bucket}/"))
            } else {
                Some(format!("gs://{bucket}/{normalized}/"))
            }
        }
    }
}

#[async_trait]
impl CredentialProvider for GcsCredentialProvider {
    #[instrument(skip(self), fields(table = %request.table.ident.name))]
    async fn vended_credentials(
        &self,
        request: CredentialRequest,
    ) -> IcebergResult<Vec<StorageCredential>> {
        let start = std::time::Instant::now();

        let location = request
            .table
            .location
            .ok_or_else(|| IcebergError::BadRequest {
                message: "table location required for credential vending".to_string(),
                error_type: "BadRequestException",
            })?;

        if !location.starts_with("gs://") {
            warn!(location = %location, "credential vending requested for non-GCS location");
            record_credential_vending("gcs", "skipped_non_gcs");
            record_credential_vending_duration("gcs", start.elapsed().as_secs_f64());
            return Ok(vec![]);
        }

        let Some(prefix) = Self::extract_gcs_prefix(&location) else {
            warn!(location = %location, "credential vending requested for invalid GCS location");
            record_credential_vending("gcs", "invalid_gcs_location");
            record_credential_vending_duration("gcs", start.elapsed().as_secs_f64());
            return Err(IcebergError::BadRequest {
                message: "invalid GCS table location for credential vending".to_string(),
                error_type: "BadRequestException",
            });
        };

        warn!(
            prefix = %prefix,
            table = %request.table.ident.name,
            ttl_secs = self.config.ttl.as_secs(),
            "GCS credential vending denied because downscoped credentials are not implemented"
        );
        record_credential_vending("gcs", "denied_downscoped_unimplemented");
        record_credential_vending_duration("gcs", start.elapsed().as_secs_f64());
        Err(IcebergError::ServiceUnavailable {
            message:
                "GCS credential vending requires downscoped credentials; raw platform OAuth tokens are disabled"
                    .to_string(),
            retry_after_seconds: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::state::TableInfo;
    use crate::types::{AccessDelegation, TableIdent};

    #[test]
    fn test_extract_gcs_prefix_with_nested_path() {
        let prefix =
            GcsCredentialProvider::extract_gcs_prefix("gs://my-bucket/warehouse/db/table/data");
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
    fn test_extract_gcs_prefix_bucket_no_slash() {
        let prefix = GcsCredentialProvider::extract_gcs_prefix("gs://my-bucket");
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
    fn test_extract_gcs_prefix_directory_with_dots() {
        let prefix = GcsCredentialProvider::extract_gcs_prefix("gs://my-bucket/v1.0.0/data");
        assert_eq!(prefix, Some("gs://my-bucket/v1.0.0/data/".to_string()));

        let prefix =
            GcsCredentialProvider::extract_gcs_prefix("gs://my-bucket/releases/v2.1.3/tables");
        assert_eq!(
            prefix,
            Some("gs://my-bucket/releases/v2.1.3/tables/".to_string())
        );
    }

    #[test]
    fn test_extract_gcs_prefix_trailing_slash_normalized() {
        let prefix =
            GcsCredentialProvider::extract_gcs_prefix("gs://my-bucket/warehouse/db/table/");
        assert_eq!(
            prefix,
            Some("gs://my-bucket/warehouse/db/table/".to_string())
        );

        let prefix =
            GcsCredentialProvider::extract_gcs_prefix("gs://my-bucket/warehouse/db/table///");
        assert_eq!(
            prefix,
            Some("gs://my-bucket/warehouse/db/table/".to_string())
        );
    }

    #[test]
    fn test_extract_gcs_prefix_double_slash_normalized() {
        let prefix = GcsCredentialProvider::extract_gcs_prefix("gs://my-bucket//foo");
        assert_eq!(prefix, Some("gs://my-bucket/foo/".to_string()));

        let prefix = GcsCredentialProvider::extract_gcs_prefix("gs://my-bucket//foo/bar//");
        assert_eq!(prefix, Some("gs://my-bucket/foo/bar/".to_string()));

        let prefix = GcsCredentialProvider::extract_gcs_prefix("gs://my-bucket///");
        assert_eq!(prefix, Some("gs://my-bucket/".to_string()));
    }

    #[test]
    fn test_extract_gcs_prefix_non_gcs() {
        let prefix = GcsCredentialProvider::extract_gcs_prefix("s3://my-bucket/path");
        assert!(prefix.is_none());

        let prefix = GcsCredentialProvider::extract_gcs_prefix("abfs://container/path");
        assert!(prefix.is_none());
    }

    #[test]
    fn test_extract_gcs_prefix_empty_bucket_rejected() {
        let prefix = GcsCredentialProvider::extract_gcs_prefix("gs://");
        assert!(prefix.is_none());

        let prefix = GcsCredentialProvider::extract_gcs_prefix("gs:///path/to/data");
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

    #[tokio::test]
    async fn gcs_vending_fails_closed_before_minting_raw_platform_token() {
        let provider = GcsCredentialProvider {
            config: GcsCredentialConfig::default(),
        };
        let request = CredentialRequest {
            table: TableInfo {
                ident: TableIdent::simple("db", "orders"),
                table_id: "table-123".to_string(),
                location: Some("gs://shared-bucket/tenant=t1/workspace=w1/orders/".to_string()),
            },
            delegation: AccessDelegation::VendedCredentials,
        };

        let err = provider.vended_credentials(request).await.unwrap_err();

        assert!(matches!(err, IcebergError::ServiceUnavailable { .. }));
        assert!(
            err.to_string().contains("downscoped"),
            "error should explain why GCS credential vending is denied: {err}"
        );
    }
}
