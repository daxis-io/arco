//! Opt-in conformance test against a real Amazon S3 bucket.

#![allow(clippy::expect_used)]

#[path = "../../arco-storage-object-store/tests/support/live_conformance.rs"]
mod live_conformance;

use std::collections::BTreeMap;
use std::sync::Arc;

use arco_core::storage::StorageBackend;
use arco_storage_s3::S3StorageBackend;

struct R2LiveConfig {
    bucket: String,
}

impl R2LiveConfig {
    fn from_env() -> Result<Self, String> {
        Self::from_pairs(std::env::vars())
    }

    fn from_pairs<K, V>(pairs: impl IntoIterator<Item = (K, V)>) -> Result<Self, String>
    where
        K: Into<String>,
        V: Into<String>,
    {
        let values = pairs
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<BTreeMap<_, _>>();
        for inherited in [
            "AWS_DEFAULT_REGION",
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_S3",
            "AWS_PROFILE",
            "AWS_SESSION_TOKEN",
        ] {
            if values.contains_key(inherited) {
                return Err(format!("R2 qualification rejects inherited {inherited}"));
            }
        }
        let required = |name: &str| {
            values
                .get(name)
                .filter(|value| !value.is_empty())
                .cloned()
                .ok_or_else(|| format!("{name} must be set"))
        };
        let account = required("ARCO_TEST_R2_ACCOUNT_ID")?;
        if account.len() != 32
            || !account
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("ARCO_TEST_R2_ACCOUNT_ID must be 32 lowercase hex characters".to_string());
        }
        let bucket = required("ARCO_TEST_R2_BUCKET")?;
        if !(3..=63).contains(&bucket.len())
            || bucket.starts_with('-')
            || bucket.ends_with('-')
            || !bucket
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(
                "ARCO_TEST_R2_BUCKET must be a lowercase DNS-style bucket name".to_string(),
            );
        }
        let expected_endpoint = format!("https://{account}.r2.cloudflarestorage.com");
        if required("AWS_ENDPOINT")? != expected_endpoint {
            return Err("AWS_ENDPOINT does not match ARCO_TEST_R2_ACCOUNT_ID".to_string());
        }
        if required("AWS_REGION")? != "auto" {
            return Err("AWS_REGION must be auto for R2 qualification".to_string());
        }
        if required("AWS_EC2_METADATA_DISABLED")? != "true" {
            return Err("AWS_EC2_METADATA_DISABLED must be true".to_string());
        }
        required("AWS_ACCESS_KEY_ID")?;
        required("AWS_SECRET_ACCESS_KEY")?;
        Ok(Self { bucket })
    }
}

#[test]
fn r2_configuration_requires_exact_account_endpoint_and_auto_region() {
    let valid = [
        (
            "ARCO_TEST_R2_ACCOUNT_ID",
            "0123456789abcdef0123456789abcdef",
        ),
        ("ARCO_TEST_R2_BUCKET", "arco-030-qualification"),
        (
            "AWS_ENDPOINT",
            "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com",
        ),
        ("AWS_REGION", "auto"),
        ("AWS_EC2_METADATA_DISABLED", "true"),
        ("AWS_ACCESS_KEY_ID", "test-access"),
        ("AWS_SECRET_ACCESS_KEY", "test-secret"),
    ];
    let config = R2LiveConfig::from_pairs(valid).expect("valid R2 qualification config");
    assert_eq!(config.bucket, "arco-030-qualification");

    for (key, value) in [
        ("AWS_REGION", "us-east-1"),
        ("AWS_ENDPOINT", "https://example.com"),
        ("ARCO_TEST_R2_ACCOUNT_ID", "not-an-account"),
    ] {
        let changed = valid.map(|(name, original)| {
            if name == key {
                (name, value)
            } else {
                (name, original)
            }
        });
        assert!(R2LiveConfig::from_pairs(changed).is_err(), "accepted {key}");
    }
}

#[test]
fn r2_configuration_rejects_aws_profile_or_session_inheritance() {
    let base = [
        (
            "ARCO_TEST_R2_ACCOUNT_ID",
            "0123456789abcdef0123456789abcdef",
        ),
        ("ARCO_TEST_R2_BUCKET", "arco-030-qualification"),
        (
            "AWS_ENDPOINT",
            "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com",
        ),
        ("AWS_REGION", "auto"),
        ("AWS_EC2_METADATA_DISABLED", "true"),
        ("AWS_ACCESS_KEY_ID", "test-access"),
        ("AWS_SECRET_ACCESS_KEY", "test-secret"),
    ];
    for inherited in ["AWS_PROFILE", "AWS_SESSION_TOKEN", "AWS_ENDPOINT_URL_S3"] {
        assert!(
            R2LiveConfig::from_pairs(base.into_iter().chain([(inherited, "unexpected")])).is_err(),
            "accepted {inherited}"
        );
    }
}

#[tokio::test]
#[ignore = "requires ARCO_TEST_S3_BUCKET and cloud credentials"]
async fn s3_backend_satisfies_storage_conformance() {
    let bucket = std::env::var("ARCO_TEST_S3_BUCKET").expect("ARCO_TEST_S3_BUCKET must be set");
    let backend: Arc<dyn StorageBackend> =
        Arc::new(S3StorageBackend::new(&bucket).expect("S3 backend"));
    live_conformance::assert_storage_conformance("s3", backend.clone()).await;
    live_conformance::assert_bounded_list_conformance("s3", &backend).await;
}

#[tokio::test]
#[ignore = "requires a dedicated ARCO_TEST_R2_BUCKET and bucket-scoped R2 credentials"]
async fn r2_backend_satisfies_storage_conformance() {
    let config = R2LiveConfig::from_env().expect("valid R2 qualification configuration");
    let backend: Arc<dyn StorageBackend> =
        Arc::new(S3StorageBackend::new(&config.bucket).expect("R2 S3-compatible backend"));
    live_conformance::assert_storage_conformance("r2", backend.clone()).await;
    live_conformance::assert_bounded_list_conformance("r2", &backend).await;
}
