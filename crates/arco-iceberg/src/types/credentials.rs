//! Credential vending types for storage access delegation.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Storage credential for accessing table data.
///
/// Returned in `LoadTableResponse` when `X-Iceberg-Access-Delegation: vended-credentials`
/// header is present.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct StorageCredential {
    /// The storage prefix this credential applies to.
    ///
    /// Clients should use the credential with the longest matching prefix.
    /// Example: `gs://bucket/warehouse/`
    pub prefix: String,

    /// Provider-specific configuration.
    ///
    /// Keys depend on the storage provider:
    /// - GCS: `gcs.oauth2.token`, `gcs.oauth2.token-expires-at`
    /// - S3: `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`
    /// - ADLS: `adls.sas-token`, `adls.sas-token-expires-at`
    pub config: HashMap<String, String>,
}

/// Response from `GET /credentials` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct TableCredentialsResponse {
    /// Storage credentials for accessing table data.
    #[serde(rename = "storage-credentials")]
    pub storage_credentials: Vec<StorageCredential>,
}

impl StorageCredential {
    /// Creates a GCS credential with `OAuth2` token.
    #[must_use]
    pub fn gcs(
        prefix: impl Into<String>,
        token: impl Into<String>,
        expires_at: impl Into<String>,
    ) -> Self {
        Self {
            prefix: prefix.into(),
            config: HashMap::from([
                ("gcs.oauth2.token".to_string(), token.into()),
                ("gcs.oauth2.token-expires-at".to_string(), expires_at.into()),
            ]),
        }
    }

    /// Creates an S3 credential with session token.
    #[must_use]
    pub fn s3(
        prefix: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            prefix: prefix.into(),
            config: HashMap::from([
                ("s3.access-key-id".to_string(), access_key_id.into()),
                ("s3.secret-access-key".to_string(), secret_access_key.into()),
                ("s3.session-token".to_string(), session_token.into()),
                ("client.region".to_string(), region.into()),
            ]),
        }
    }

    /// Creates an ADLS credential with SAS token.
    #[must_use]
    pub fn adls(
        prefix: impl Into<String>,
        sas_token: impl Into<String>,
        expires_at: impl Into<String>,
    ) -> Self {
        Self {
            prefix: prefix.into(),
            config: HashMap::from([
                ("adls.sas-token".to_string(), sas_token.into()),
                ("adls.sas-token-expires-at".to_string(), expires_at.into()),
            ]),
        }
    }
}

/// Access delegation modes supported by the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessDelegation {
    /// Vend short-lived storage credentials to the client.
    VendedCredentials,
    /// Sign requests on behalf of the client (future).
    RemoteSigning,
}

impl AccessDelegation {
    /// Parse from the `X-Iceberg-Access-Delegation` header value.
    ///
    /// Returns `None` for unrecognized values.
    #[must_use]
    pub fn from_header(value: &str) -> Option<Self> {
        let mut first: Option<Self> = None;
        for token in value.split(',') {
            let token = token.trim();
            let mode = if token.eq_ignore_ascii_case("vended-credentials") {
                Some(Self::VendedCredentials)
            } else if token.eq_ignore_ascii_case("remote-signing") {
                Some(Self::RemoteSigning)
            } else {
                None
            };

            if let Some(mode) = mode {
                if mode.is_supported() {
                    return Some(mode);
                }
                if first.is_none() {
                    first = Some(mode);
                }
            }
        }
        first
    }

    /// Returns true if this delegation mode is currently supported.
    #[must_use]
    pub const fn is_supported(&self) -> bool {
        matches!(self, Self::VendedCredentials)
    }
}

/// Query parameters for the credentials endpoint.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams)]
pub struct CredentialsQuery {
    /// Plan ID for server-side scan planning.
    ///
    /// This parameter is accepted but currently not used.
    /// Future versions may use it to scope credentials to specific file sets.
    #[serde(rename = "planId")]
    pub plan_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcs_credential() {
        let cred = StorageCredential::gcs(
            "gs://bucket/warehouse/",
            "ya29.token",
            "2025-01-15T15:00:00Z",
        );

        assert_eq!(cred.prefix, "gs://bucket/warehouse/");
        assert_eq!(
            cred.config.get("gcs.oauth2.token"),
            Some(&"ya29.token".to_string())
        );
        assert!(cred.config.contains_key("gcs.oauth2.token-expires-at"));
    }

    #[test]
    fn test_s3_credential() {
        let cred = StorageCredential::s3(
            "s3://bucket/warehouse/",
            "AKIAIOSFODNN7EXAMPLE",
            "secret",
            "session",
            "us-east-1",
        );

        assert_eq!(
            cred.config.get("s3.access-key-id"),
            Some(&"AKIAIOSFODNN7EXAMPLE".to_string())
        );
        assert_eq!(
            cred.config.get("client.region"),
            Some(&"us-east-1".to_string())
        );
    }

    #[test]
    fn test_access_delegation_parsing() {
        assert_eq!(
            AccessDelegation::from_header("vended-credentials"),
            Some(AccessDelegation::VendedCredentials)
        );
        assert_eq!(
            AccessDelegation::from_header("VENDED-CREDENTIALS"),
            Some(AccessDelegation::VendedCredentials)
        );
        assert_eq!(
            AccessDelegation::from_header("remote-signing"),
            Some(AccessDelegation::RemoteSigning)
        );
        assert_eq!(
            AccessDelegation::from_header("remote-signing, vended-credentials"),
            Some(AccessDelegation::VendedCredentials)
        );
        assert_eq!(AccessDelegation::from_header("unknown"), None);
    }

    #[test]
    fn test_supported_delegation() {
        assert!(AccessDelegation::VendedCredentials.is_supported());
        assert!(!AccessDelegation::RemoteSigning.is_supported());
    }

    #[test]
    fn test_credential_serialization() {
        let cred = StorageCredential::gcs("gs://bucket/", "token", "2025-01-15T15:00:00Z");
        let json = serde_json::to_string(&cred).expect("serialization failed");

        assert!(json.contains("\"prefix\""));
        assert!(json.contains("\"config\""));
        assert!(json.contains("gcs.oauth2.token"));
    }

    #[test]
    fn test_table_credentials_roundtrip() {
        let json = r#"{"storage-credentials":[{"prefix":"s3://bucket/warehouse/","config":{"client.region":"us-east-1","s3.access-key-id":"AKIAIOSFODNN7EXAMPLE","s3.secret-access-key":"secret","s3.session-token":"session"}}]}"#;

        let value: serde_json::Value = serde_json::from_str(json).expect("parse failed");
        let parsed: TableCredentialsResponse =
            serde_json::from_value(value.clone()).expect("deserialization failed");
        let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

        assert_eq!(roundtrip, value);
    }
}
