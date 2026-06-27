//! Public API source-shape contract tests.

#[test]
fn core_error_enum_is_non_exhaustive_and_not_found_variants_are_documented() {
    let source = include_str!("../src/error.rs");
    let enum_offset = source
        .find("pub enum Error")
        .expect("missing public Error enum");
    let before_enum = &source[..enum_offset];
    assert!(
        before_enum
            .lines()
            .rev()
            .take(4)
            .any(|line| line.trim() == "#[non_exhaustive]"),
        "arco_core::Error must be #[non_exhaustive]"
    );
    assert!(
        source.contains("Use this variant for typed domain resources"),
        "ResourceNotFound must document its domain-resource role"
    );
    assert!(
        source.contains("Use this variant for storage paths and untyped compatibility errors"),
        "NotFound must document its storage/compatibility role"
    );
}

#[test]
fn public_rust_enum_policy_is_documented_and_error_enums_are_non_exhaustive() {
    let docs = include_str!("../../../docs/guide/src/reference/api.md");
    assert!(
        docs.contains("## Public Rust Enum Compatibility Policy"),
        "API docs must define the public Rust enum compatibility policy"
    );
    assert!(
        docs.contains("Public error enums exposed by stable crates are non-exhaustive"),
        "API docs must define the non_exhaustive policy for public error enums"
    );
    assert!(
        docs.contains("Protocol and wire-format enums may remain exhaustive"),
        "API docs must distinguish wire-format enums from extensible Rust error enums"
    );
    assert!(
        docs.contains("`arco_core::Error`") && docs.contains("`arco_catalog::CatalogError`"),
        "API docs must inventory the public error enums covered by the policy"
    );
    for public_enum in [
        "`arco_delta::DeltaError`",
        "`arco_iceberg::IcebergError`",
        "`arco_uc::UnityCatalogError`",
        "`arco_proto::TaskOutputContractError`",
        "`arco_proto::ControlPlaneTransactionContractError`",
        "`arco_proto::CatalogDdlContractError`",
        "`arco_proto::MetastoreMutationContractError`",
        "`arco_proto::CatalogControlPlaneScopeContractError`",
        "`arco_proto::OrchestrationEventContractError`",
        "`arco_proto::WorkerCallbackContractError`",
    ] {
        assert!(
            docs.contains(public_enum),
            "API docs must classify public enum {public_enum}"
        );
    }

    assert_non_exhaustive(
        include_str!("../../arco-catalog/src/error.rs"),
        "CatalogError",
    );
    assert_non_exhaustive(include_str!("../../arco-delta/src/error.rs"), "DeltaError");
    assert_non_exhaustive(
        include_str!("../../arco-iceberg/src/error.rs"),
        "IcebergError",
    );
    assert_non_exhaustive(
        include_str!("../../arco-uc/src/error.rs"),
        "UnityCatalogError",
    );
    let proto_source = include_str!("../../arco-proto/src/lib.rs");
    for enum_name in [
        "TaskOutputContractError",
        "ControlPlaneTransactionContractError",
        "CatalogDdlContractError",
        "MetastoreMutationContractError",
        "CatalogControlPlaneScopeContractError",
        "OrchestrationEventContractError",
        "WorkerCallbackContractError",
    ] {
        assert_non_exhaustive(proto_source, enum_name);
    }
}

fn assert_non_exhaustive(source: &str, enum_name: &str) {
    let enum_offset = source
        .find(&format!("pub enum {enum_name}"))
        .unwrap_or_else(|| panic!("missing public {enum_name} enum"));
    let before_enum = &source[..enum_offset];
    assert!(
        before_enum
            .lines()
            .rev()
            .take(4)
            .any(|line| line.trim() == "#[non_exhaustive]"),
        "{enum_name} must be #[non_exhaustive]"
    );
}
