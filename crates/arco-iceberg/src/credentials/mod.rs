//! Credential providers for storage access delegation.
//!
//! This module implements the `CredentialProvider` trait with provider-specific
//! implementations for different cloud storage backends.
//!
//! ## Supported Providers
//!
//! - **GCS**: `OAuth2` tokens via metadata server or service account keys
//! - **S3**: (future) STS temporary credentials
//! - **Azure**: (future) SAS tokens
//!
//! ## Usage
//!
//! ```rust,ignore
//! use arco_iceberg::credentials::GcsCredentialProvider;
//! use arco_iceberg::IcebergState;
//! use std::sync::Arc;
//!
//! // GcsCredentialProvider::from_environment() is async
//! let gcs_provider = GcsCredentialProvider::from_environment().await?;
//! let state = IcebergState::new(storage)
//!     .with_credentials(Arc::new(gcs_provider));
//! ```

#[cfg(feature = "gcp")]
mod gcs;

#[cfg(feature = "gcp")]
pub use gcs::{GcsCredentialConfig, GcsCredentialProvider};

use std::time::Duration;

/// Default credential TTL (1 hour).
pub const DEFAULT_CREDENTIAL_TTL: Duration = Duration::from_secs(3600);

/// Minimum credential TTL (1 minute).
pub const MIN_CREDENTIAL_TTL: Duration = Duration::from_secs(60);

/// Maximum credential TTL (1 hour for security).
pub const MAX_CREDENTIAL_TTL: Duration = Duration::from_secs(3600);

/// Clamps a TTL to the allowed range.
#[must_use]
pub fn clamp_ttl(ttl: Duration) -> Duration {
    ttl.clamp(MIN_CREDENTIAL_TTL, MAX_CREDENTIAL_TTL)
}
