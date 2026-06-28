#![allow(clippy::expect_used)]
#![allow(missing_docs)]

use std::collections::BTreeSet;

use serde::Deserialize;

const FIXTURE: &str =
    include_str!("../../../docs/spec/fixtures/provider-capability-matrix-v1.json");

#[derive(Debug, Deserialize)]
struct ProviderCapabilityMatrix {
    schema_version: String,
    providers: Vec<ProviderCapability>,
}

#[derive(Debug, Deserialize)]
struct ProviderCapability {
    id: String,
    production_provider: bool,
    certification_state: CertificationState,
    write_enabled: bool,
    semantics: ProviderSemantics,
    evidence: ProviderEvidence,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CertificationState {
    TestOnly,
    NotCertified,
    ReadOnlyCertified,
    WriteCertified,
}

#[derive(Debug, Deserialize)]
struct ProviderSemantics {
    conditional_create: bool,
    pointer_cas: bool,
    stable_version_token: bool,
    addressed_read_after_write: bool,
    checksum_or_corruption_detection: bool,
    retry_timeout_policy: bool,
    request_time_correctness_uses_listing: bool,
    cas_semantics: CasSemantics,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum CasSemantics {
    Native,
    ProvenSafe,
    Ambiguous,
    Unsupported,
}

#[derive(Debug, Deserialize)]
struct ProviderEvidence {
    conditional_create: Vec<String>,
    pointer_cas: Vec<String>,
    stable_version_token: Vec<String>,
    addressed_read_after_write: Vec<String>,
    checksum_or_corruption_detection: Vec<String>,
    retry_timeout_policy: Vec<String>,
}

#[test]
fn fixture_uses_expected_schema_version() {
    let matrix = provider_matrix();
    assert_eq!(matrix.schema_version, "arco.provider-capability-matrix.v1");
}

#[test]
fn fixture_has_required_provider_coverage() {
    let matrix = provider_matrix();

    assert_no_provider_matrix_errors(&matrix);
}

#[test]
fn write_enabled_production_providers_have_required_evidence() {
    let matrix = provider_matrix();

    for provider in matrix.providers {
        if !provider.production_provider
            || !(provider.write_enabled
                || provider.certification_state == CertificationState::WriteCertified)
        {
            continue;
        }

        assert!(
            provider.semantics.conditional_create,
            "{}: production write certification requires conditional create",
            provider.id
        );
        assert!(
            provider.semantics.pointer_cas,
            "{}: production write certification requires pointer CAS",
            provider.id
        );
        assert!(
            provider.semantics.stable_version_token,
            "{}: production write certification requires stable version tokens",
            provider.id
        );
        assert!(
            provider.semantics.addressed_read_after_write,
            "{}: production write certification requires addressed read-after-write",
            provider.id
        );
        assert!(
            provider.semantics.checksum_or_corruption_detection,
            "{}: production write certification requires checksum or corruption detection",
            provider.id
        );
        assert!(
            provider.semantics.retry_timeout_policy,
            "{}: production write certification requires retry/timeout evidence",
            provider.id
        );
        assert!(
            !provider.semantics.request_time_correctness_uses_listing,
            "{}: request-time correctness must not depend on listing",
            provider.id
        );
        assert!(
            matches!(
                provider.semantics.cas_semantics,
                CasSemantics::Native | CasSemantics::ProvenSafe
            ),
            "{}: production writes require native or proven-safe CAS",
            provider.id
        );

        assert_non_empty_evidence(
            &provider.id,
            "conditional_create",
            &provider.evidence.conditional_create,
        );
        assert_non_empty_evidence(&provider.id, "pointer_cas", &provider.evidence.pointer_cas);
        assert_non_empty_evidence(
            &provider.id,
            "stable_version_token",
            &provider.evidence.stable_version_token,
        );
        assert_non_empty_evidence(
            &provider.id,
            "addressed_read_after_write",
            &provider.evidence.addressed_read_after_write,
        );
        assert_non_empty_evidence(
            &provider.id,
            "checksum_or_corruption_detection",
            &provider.evidence.checksum_or_corruption_detection,
        );
        assert_non_empty_evidence(
            &provider.id,
            "retry_timeout_policy",
            &provider.evidence.retry_timeout_policy,
        );
    }
}

#[test]
fn ambiguous_cas_providers_are_not_write_enabled() {
    let matrix = provider_matrix();

    for provider in matrix.providers {
        if provider.semantics.cas_semantics != CasSemantics::Ambiguous {
            continue;
        }

        assert!(
            !provider.write_enabled,
            "{}: ambiguous CAS semantics must remain write-disabled",
            provider.id
        );
        assert_ne!(
            provider.certification_state,
            CertificationState::WriteCertified,
            "{}: ambiguous CAS semantics must not be write-certified",
            provider.id
        );
    }
}

#[test]
fn request_time_correctness_never_depends_on_listing() {
    let matrix = provider_matrix();

    for provider in matrix.providers {
        assert!(
            !provider.semantics.request_time_correctness_uses_listing,
            "{}: request-time correctness must not depend on listing",
            provider.id
        );
    }
}

#[test]
fn empty_provider_matrix_is_rejected() {
    let matrix = ProviderCapabilityMatrix {
        schema_version: "arco.provider-capability-matrix.v1".to_owned(),
        providers: Vec::new(),
    };

    let errors = provider_matrix_errors(&matrix);

    assert!(
        errors
            .iter()
            .any(|error| error.contains("must list representative providers")),
        "expected representative provider coverage error, got {errors:?}"
    );
}

fn provider_matrix() -> ProviderCapabilityMatrix {
    serde_json::from_str(FIXTURE).expect("provider capability fixture should parse")
}

fn assert_no_provider_matrix_errors(matrix: &ProviderCapabilityMatrix) {
    let errors = provider_matrix_errors(matrix);
    assert!(errors.is_empty(), "provider matrix errors: {errors:?}");
}

fn provider_matrix_errors(matrix: &ProviderCapabilityMatrix) -> Vec<String> {
    let mut errors = Vec::new();

    if matrix.providers.is_empty() {
        errors.push("provider matrix must list representative providers".to_owned());
        return errors;
    }

    let provider_ids: BTreeSet<&str> = matrix
        .providers
        .iter()
        .map(|provider| provider.id.as_str())
        .collect();
    for required_id in [
        "memory-backend",
        "object-store-memory-backend",
        "gcs-production",
        "s3-production",
        "azure-blob-production",
    ] {
        if !provider_ids.contains(required_id) {
            errors.push(format!(
                "provider matrix must list representative provider {required_id}"
            ));
        }
    }

    if !matrix
        .providers
        .iter()
        .any(|provider| provider.production_provider)
    {
        errors.push("provider matrix must list at least one production provider".to_owned());
    }
    if !matrix.providers.iter().any(|provider| {
        provider.production_provider && provider.semantics.cas_semantics == CasSemantics::Ambiguous
    }) {
        errors.push(
            "provider matrix must include a production provider with ambiguous CAS semantics"
                .to_owned(),
        );
    }
    if !matrix.providers.iter().any(|provider| {
        provider.production_provider
            && provider.semantics.cas_semantics == CasSemantics::Unsupported
    }) {
        errors.push(
            "provider matrix must include a production provider with unsupported CAS semantics"
                .to_owned(),
        );
    }

    errors
}

fn assert_non_empty_evidence(provider_id: &str, field: &str, values: &[String]) {
    assert!(
        values.iter().any(|value| !value.trim().is_empty()),
        "{provider_id}: missing evidence for {field}"
    );
}
