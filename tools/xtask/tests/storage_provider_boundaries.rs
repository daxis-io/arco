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
