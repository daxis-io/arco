//! Layout contract between the state kernel and platform IAM.
//!
//! `infra/terraform/iam_conditions.tf` grants the API service account
//! sole-writer authority beneath one tenant/workspace-relative prefix, and
//! `tools/xtask/tests/terraform_iam.rs` pins that Terraform local to
//! `arco_core::storage_keys::CONTROL_STATE_OBJECT_PREFIX`. This test closes the
//! loop from the other side: every object path the kernel actually writes must
//! fall under that same constant, so a layout change in the kernel that moves
//! writes outside the granted prefix fails here instead of in production with
//! a 403.

use arco_catalog::ControlMvpPaths;
use arco_core::storage_keys::CONTROL_STATE_OBJECT_PREFIX;

/// The authority-8 directory layout root, as built by
/// `crates/arco-catalog/src/state_store/control_mvp/directory.rs` (see the
/// `format!("control/directory/v1/domains/{}", ...)` in `Directory::new`).
/// There is no public accessor for the directory prefix, so the literal is
/// pinned here; keep it in sync with `directory.rs`.
const DIRECTORY_LAYOUT_PREFIX: &str = "control/directory/v1/";

#[test]
fn control_state_object_prefix_is_a_directory_prefix() {
    assert!(
        CONTROL_STATE_OBJECT_PREFIX.ends_with('/'),
        "the IAM condition uses startsWith(); a prefix without a trailing slash \
         would also match sibling top-level prefixes such as `controlX/`"
    );
    assert!(!CONTROL_STATE_OBJECT_PREFIX.starts_with('/'));
    assert!(
        !CONTROL_STATE_OBJECT_PREFIX.contains("tenant=")
            && !CONTROL_STATE_OBJECT_PREFIX.contains("workspace="),
        "the prefix is tenant/workspace-relative; ScopedStorage prepends the scope"
    );
}

#[test]
fn authority_layout_paths_fall_under_the_granted_prefix() {
    let paths = ControlMvpPaths::new("catalog");

    for (label, path) in [
        ("base_prefix", paths.base_prefix()),
        ("current_pointer", paths.current_pointer()),
        ("tx_object", paths.tx_object("x")),
        ("manifest_object", paths.manifest_object("x")),
        ("checkpoint_object", paths.checkpoint_object("x")),
        ("state_object", paths.state_object("x")),
        ("l0_segment_object", paths.l0_segment_object("x")),
        ("segment_index", paths.segment_index("x")),
    ] {
        assert!(
            path.starts_with(CONTROL_STATE_OBJECT_PREFIX),
            "ControlMvpPaths::{label}() = {path:?} must start with the IAM-granted \
             prefix {CONTROL_STATE_OBJECT_PREFIX:?}"
        );
    }
}

#[test]
fn authority_layout_is_versioned_under_the_granted_prefix() {
    // The kernel's hard-cut layout is control/v1/domains/{domain}/...; pin the
    // shape so the IAM comment in iam_conditions.tf stays truthful.
    assert_eq!(
        ControlMvpPaths::new("catalog").base_prefix(),
        format!("{CONTROL_STATE_OBJECT_PREFIX}v1/domains/catalog")
    );
}

#[test]
fn directory_layout_falls_under_the_granted_prefix() {
    assert!(
        DIRECTORY_LAYOUT_PREFIX.starts_with(CONTROL_STATE_OBJECT_PREFIX),
        "directory layout {DIRECTORY_LAYOUT_PREFIX:?} must start with the IAM-granted \
         prefix {CONTROL_STATE_OBJECT_PREFIX:?}"
    );
}

#[test]
fn retired_state_store_prefix_is_not_written() {
    // `state-store/` was the prefix Terraform granted before this contract
    // existed; nothing writes there. Guard against a regression that reintroduces
    // it into the kernel layout.
    let paths = ControlMvpPaths::new("catalog");
    for path in [
        paths.base_prefix(),
        paths.current_pointer(),
        paths.tx_object("x"),
        paths.manifest_object("x"),
    ] {
        assert!(
            !path.starts_with("state-store/"),
            "{path:?} must not use the retired state-store/ prefix"
        );
    }
    assert!(!DIRECTORY_LAYOUT_PREFIX.starts_with("state-store/"));
}
