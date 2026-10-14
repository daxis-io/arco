//! Opt-in conformance test against a real Amazon S3 bucket.

#![allow(clippy::expect_used)]

#[path = "../../arco-storage-object-store/tests/support/live_conformance.rs"]
mod live_conformance;

#[cfg(feature = "qualification")]
#[path = "support/native_owner_conformance.rs"]
mod native_owner_conformance;

use std::collections::BTreeMap;
use std::sync::Arc;

use arco_core::storage::StorageBackend;
use arco_storage_s3::S3StorageBackend;

struct R2LiveConfig {
    #[cfg(feature = "qualification")]
    account: String,
    bucket: String,
    #[cfg(feature = "qualification")]
    denied_bucket: Option<String>,
}

fn valid_bucket(bucket: &str) -> bool {
    (3..=63).contains(&bucket.len())
        && !bucket.starts_with('-')
        && !bucket.ends_with('-')
        && bucket
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
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
        if !valid_bucket(&bucket) {
            return Err(
                "ARCO_TEST_R2_BUCKET must be a lowercase DNS-style bucket name".to_string(),
            );
        }
        let denied_bucket = values.get("ARCO_TEST_R2_DENIED_BUCKET").cloned();
        if denied_bucket
            .as_deref()
            .is_some_and(|denied| !valid_bucket(denied) || denied == bucket)
        {
            return Err(
                "ARCO_TEST_R2_DENIED_BUCKET must name a distinct disposable bucket".to_string(),
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
        Ok(Self {
            #[cfg(feature = "qualification")]
            account,
            bucket,
            #[cfg(feature = "qualification")]
            denied_bucket,
        })
    }
}

#[cfg(feature = "qualification")]
fn validate_scope_attestation(
    raw: &str,
    account: &str,
    bucket: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<serde_json::Value, String> {
    let receipt: serde_json::Value = serde_json::from_str(raw)
        .map_err(|_| "scope probe requires a valid independent owner receipt".to_string())?;
    for (field, expected) in [
        ("schemaVersion", serde_json::json!(1)),
        ("accountId", serde_json::json!(account)),
        ("bucket", serde_json::json!(bucket)),
        ("key", serde_json::json!("scope-probe.txt")),
        ("bytes", serde_json::json!(44)),
        (
            "sha256",
            serde_json::json!("1cc07e83d23b93b49ae9716e485c904c4252643af26f8a0524c2877f1692f9b8"),
        ),
        (
            "source",
            serde_json::json!("wrangler-oauth-remote-object-read"),
        ),
        ("private", serde_json::json!(true)),
    ] {
        if receipt.get(field) != Some(&expected) {
            return Err(format!("scope owner receipt does not match {field}"));
        }
    }
    let observed = receipt
        .get("observedAt")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "scope owner receipt requires observedAt".to_string())?;
    let observed = chrono::DateTime::parse_from_rfc3339(observed)
        .map_err(|_| "scope owner receipt has invalid observedAt".to_string())?;
    let age = now.signed_duration_since(observed);
    if age < chrono::Duration::zero() || age > chrono::Duration::hours(24) {
        return Err(
            "scope owner receipt must be observed within the preceding 24 hours".to_string(),
        );
    }
    Ok(receipt)
}

#[cfg(feature = "qualification")]
#[test]
fn r2_scope_denial_requires_fresh_matching_owner_existence_evidence() {
    let now = chrono::Utc::now();
    let valid = serde_json::json!({
        "schemaVersion": 1, "accountId": "owner-account", "bucket": "known-denied-bucket",
        "key": "scope-probe.txt", "bytes": 44,
        "sha256": "1cc07e83d23b93b49ae9716e485c904c4252643af26f8a0524c2877f1692f9b8",
        "source": "wrangler-oauth-remote-object-read", "private": true,
        "observedAt": now.to_rfc3339()
    });
    let check =
        |raw: &str| validate_scope_attestation(raw, "owner-account", "known-denied-bucket", now);
    assert!(check(&valid.to_string()).is_ok());
    assert!(
        check("").is_err(),
        "a generic 403 without owner evidence cannot qualify scope"
    );
    for (field, wrong) in [
        ("bucket", serde_json::json!("nonexistent-bucket")),
        ("accountId", serde_json::json!("another-account")),
        ("sha256", serde_json::json!("changed-probe")),
        ("private", serde_json::json!(false)),
        (
            "observedAt",
            serde_json::json!((now - chrono::Duration::hours(25)).to_rfc3339()),
        ),
        (
            "observedAt",
            serde_json::json!((now + chrono::Duration::minutes(1)).to_rfc3339()),
        ),
    ] {
        let mut changed = valid.clone();
        changed[field] = wrong;
        assert!(
            check(&changed.to_string()).is_err(),
            "accepted mismatched {field}"
        );
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
    for denied in ["arco-030-qualification", "not/a/bucket"] {
        assert!(
            R2LiveConfig::from_pairs(
                base.into_iter()
                    .chain([("ARCO_TEST_R2_DENIED_BUCKET", denied)])
            )
            .is_err(),
            "scope probe must use a distinct DNS-style bucket"
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

#[cfg(feature = "qualification")]
#[tokio::test]
#[ignore = "requires bucket-scoped R2 credentials and a private denial-control bucket with scope-probe.txt"]
async fn r2_credentials_are_scoped_to_dedicated_bucket() {
    let config = R2LiveConfig::from_env().expect("valid R2 qualification configuration");
    let denied = config
        .denied_bucket
        .expect("ARCO_TEST_R2_DENIED_BUCKET must be set");
    let receipt = validate_scope_attestation(
        &std::env::var("ARCO_TEST_R2_SCOPE_ATTESTATION")
            .expect("independent owner attestation must be set"),
        &config.account,
        &denied,
        chrono::Utc::now(),
    )
    .expect("fresh matching owner-side existence evidence before any scope probe traffic");
    let backend = S3StorageBackend::new(&denied).expect("denial-control backend");
    let error = backend
        .head("scope-probe.txt")
        .await
        .expect_err("credential must not read outside its assigned bucket");
    assert!(
        matches!(&error, arco_core::Error::Storage { source: Some(source), .. }
            if matches!(source.downcast_ref::<object_store::Error>(), Some(object_store::Error::PermissionDenied { .. }))),
        "scope denial must be typed PermissionDenied, not an absent object or configuration failure: {error}"
    );
    println!(
        "{}",
        serde_json::json!({
            "credentialScope": "passed", "ownerExistenceReceipt": receipt,
            "denial": "PermissionDenied", "providerQualification": "evidence-pending-review"
        })
    );
}

#[cfg(feature = "qualification")]
#[tokio::test]
#[ignore = "requires a dedicated R2 bucket; retains immutable owner/checkpoint evidence"]
async fn r2_owner_recovers_accepted_writes_and_retained_reads() {
    let config = R2LiveConfig::from_env().expect("valid R2 qualification configuration");
    let backend: Arc<dyn StorageBackend> =
        Arc::new(S3StorageBackend::new(&config.bucket).expect("R2 backend"));
    let restarted: Arc<dyn StorageBackend> =
        Arc::new(S3StorageBackend::new(&config.bucket).expect("independent restarted R2 client"));
    native_owner_conformance::assert_owner_recovery(backend, restarted).await;
}
