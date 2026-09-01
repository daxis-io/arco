use std::fs;
use std::path::{Path, PathBuf};

use toml::Value;

const STORAGE_CRATES: [&str; 5] = [
    "crates/arco-storage-object-store",
    "crates/arco-storage-s3",
    "crates/arco-storage-gcs",
    "crates/arco-storage-azure",
    "crates/arco-storage",
];

const LIVE_PROVIDER_WORKFLOWS: [(&str, &str, &str, &str); 3] = [
    (
        ".github/workflows/s3-conformance.yml",
        "arco-storage-s3",
        "crates/arco-storage-s3/tests/live_conformance.rs",
        "s3_backend_satisfies_storage_conformance",
    ),
    (
        ".github/workflows/adr-034-gcs-conformance.yml",
        "arco-storage-gcs",
        "crates/arco-storage-gcs/tests/live_conformance.rs",
        "gcs_backend_satisfies_storage_conformance",
    ),
    (
        ".github/workflows/azure-conformance.yml",
        "arco-storage-azure",
        "crates/arco-storage-azure/tests/live_conformance.rs",
        "azure_backend_satisfies_storage_conformance",
    ),
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("xtask must live under the workspace root")
        .to_path_buf()
}

fn manifest(path: &Path) -> Value {
    let contents =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    toml::from_str(&contents).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

fn dependency_names(manifest: &Value) -> Vec<&str> {
    manifest
        .get("dependencies")
        .and_then(Value::as_table)
        .into_iter()
        .flat_map(|dependencies| dependencies.keys().map(String::as_str))
        .collect()
}

fn normalized_whitespace(contents: &str) -> String {
    contents
        .split_whitespace()
        .filter(|token| *token != "\\")
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn storage_provider_crates_own_cloud_dependencies() {
    let root = workspace_root();
    let workspace_manifest = manifest(&root.join("Cargo.toml"));
    let members = workspace_manifest["workspace"]["members"]
        .as_array()
        .expect("workspace members")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();

    let mut violations = Vec::new();
    for storage_crate in STORAGE_CRATES {
        if !members.contains(&storage_crate) {
            violations.push(format!("workspace is missing {storage_crate}"));
        }
        if !root.join(storage_crate).join("Cargo.toml").is_file() {
            violations.push(format!("{storage_crate} has no Cargo.toml"));
        }
    }

    for provider_neutral in ["crates/arco-core", "crates/arco-catalog"] {
        let provider_neutral_manifest = manifest(&root.join(provider_neutral).join("Cargo.toml"));
        let dependencies = dependency_names(&provider_neutral_manifest);
        if dependencies.contains(&"object_store") {
            violations.push(format!(
                "{provider_neutral} must not depend directly on object_store"
            ));
        }
    }

    for provider in [
        "crates/arco-storage-s3",
        "crates/arco-storage-gcs",
        "crates/arco-storage-azure",
    ] {
        let path = root.join(provider).join("Cargo.toml");
        if !path.is_file() {
            continue;
        }
        let provider_manifest = manifest(&path);
        let dependencies = dependency_names(&provider_manifest);
        for required in ["arco-core", "arco-storage-object-store", "object_store"] {
            if !dependencies.contains(&required) {
                violations.push(format!("{provider} must depend on {required}"));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "storage provider crate boundaries are violated:\n{}",
        violations.join("\n")
    );
}

#[test]
fn provider_selection_is_not_owned_by_core_or_catalog() {
    let root = workspace_root();
    let mut violations = Vec::new();
    for source in [
        "crates/arco-core/src/storage.rs",
        "crates/arco-catalog/src/state_store/control_mvp.rs",
    ] {
        let contents = fs::read_to_string(root.join(source)).expect("read provider-neutral source");
        for forbidden in [
            "AmazonS3Builder",
            "GoogleCloudStorageBuilder",
            "MicrosoftAzureBuilder",
            "from_bucket(",
        ] {
            if contents.contains(forbidden) {
                violations.push(format!(
                    "{source} contains provider selection `{forbidden}`"
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "provider-neutral sources contain provider construction:\n{}",
        violations.join("\n")
    );
}

#[test]
fn control_state_kernel_uses_the_narrow_authority_capability() {
    let root = workspace_root();
    let source = "crates/arco-catalog/src/state_store/control_mvp.rs";
    let contents = fs::read_to_string(root.join(source)).expect("read control state kernel");

    assert!(
        contents.contains("storage: ScopedAuthorityStore"),
        "{source} must depend on ScopedAuthorityStore"
    );
    for forbidden in [".get_raw(", ".put_raw(", ".head_raw("] {
        assert!(
            !contents.contains(forbidden),
            "{source} bypasses the narrow authority capability with `{forbidden}`"
        );
    }
}

#[test]
fn live_provider_workflows_fail_closed_on_zero_test_discovery() {
    let root = workspace_root();

    for (workflow, package, test_source, test_name) in LIVE_PROVIDER_WORKFLOWS {
        let workflow_contents =
            fs::read_to_string(root.join(workflow)).expect("read live provider workflow");
        let normalized = normalized_whitespace(&workflow_contents);
        let list_command =
            format!("cargo test --locked -p {package} --test live_conformance -- --ignored --list");
        let run_command = format!(
            "cargo test --locked -p {package} --test live_conformance {test_name} -- --ignored --exact --nocapture"
        );
        assert!(
            normalized.contains(&list_command),
            "{workflow} must enumerate the owning provider test target"
        );
        assert!(
            normalized.contains(&format!("grep -Fxc '{test_name}: test'")),
            "{workflow} must count the exact discovered test"
        );
        assert!(
            normalized.contains("test \"$discovered_tests\" -eq 1"),
            "{workflow} must fail unless exactly one ignored test is discovered"
        );
        assert!(
            normalized.contains(&run_command),
            "{workflow} must execute the exact owning provider test"
        );

        let test_contents =
            fs::read_to_string(root.join(test_source)).expect("read live provider test source");
        assert_eq!(
            test_contents
                .matches(&format!("async fn {test_name}()"))
                .count(),
            1,
            "{test_source} must define exactly one {test_name}"
        );
        assert!(
            test_contents.contains("#[ignore ="),
            "{test_source} must keep live cloud access opt-in"
        );
    }
}

#[test]
fn provider_builders_preserve_conditional_write_ambiguity() {
    let root = workspace_root();

    for source in [
        "crates/arco-storage-s3/src/lib.rs",
        "crates/arco-storage-gcs/src/lib.rs",
        "crates/arco-storage-azure/src/lib.rs",
    ] {
        let contents = fs::read_to_string(root.join(source)).expect("read provider adapter");
        assert!(
            contents.contains(".with_retry(no_automatic_request_retries())"),
            "{source} must not hide conditional-write transport ambiguity behind SDK retries"
        );
        assert_eq!(
            contents
                .matches(".with_retry(no_automatic_request_retries())")
                .count(),
            1,
            "{source} must disable retries only on its conditional-write client"
        );
        assert!(
            contents.contains("conditional_write_store"),
            "{source} must keep ordinary operations on a separate retrying client"
        );
    }

    let s3 = fs::read_to_string(root.join("crates/arco-storage-s3/src/lib.rs"))
        .expect("read S3 provider adapter");
    assert!(
        s3.contains(".with_conditional_put(S3ConditionalPut::ETagMatch)"),
        "S3 must explicitly enable native ETag conditional writes"
    );
}

#[test]
fn shared_adapter_requires_an_explicit_conditional_write_client() {
    let root = workspace_root();
    let source = "crates/arco-storage-object-store/src/lib.rs";
    let contents = fs::read_to_string(root.join(source)).expect("read shared storage adapter");

    for forbidden in ["pub fn new(store:", "pub fn new_with_ordered_listing(\n"] {
        assert!(
            !contents.contains(forbidden),
            "{source} exposes a same-client constructor `{forbidden}` that can hide conditional-write retries"
        );
    }
    assert!(
        contents.contains("pub fn new_with_conditional_write_store("),
        "{source} must require callers to identify a conditional-write client"
    );
    assert!(
        contents.contains("pub fn new_with_ordered_listing_and_conditional_write_store("),
        "{source} must require ordered providers to identify a conditional-write client"
    );
}

#[test]
fn published_adapter_relocation_is_a_versioned_breaking_change() {
    let root = workspace_root();
    let workspace_manifest = manifest(&root.join("Cargo.toml"));
    let version = workspace_manifest["workspace"]["package"]["version"]
        .as_str()
        .expect("workspace package version");
    let mut components = version
        .split('.')
        .map(|component| component.parse::<u64>().expect("numeric semver component"));
    let major = components.next().expect("semver major");
    let minor = components.next().expect("semver minor");

    assert!(
        major > 0 || minor >= 3,
        "moving the published arco_core::ObjectStoreBackend API requires Rust workspace version 0.3.0 or later, got {version}"
    );

    let changelog =
        fs::read_to_string(root.join("CHANGELOG.md")).expect("read workspace changelog");
    for required in [
        "arco_core::ObjectStoreBackend",
        "arco_storage_object_store::ObjectStoreBackend",
        "arco_storage::from_bucket",
    ] {
        assert!(
            changelog.contains(required),
            "CHANGELOG.md must document the breaking storage migration using `{required}`"
        );
    }
}
