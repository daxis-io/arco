#![allow(clippy::expect_used)]
#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

const FIXTURE: &str = include_str!("../../../docs/spec/fixtures/root-ownership-and-iam-v1.json");

#[derive(Debug, Deserialize)]
struct RootOwnershipFixture {
    schema_version: String,
    entries: Vec<RootRoleEntry>,
}

#[derive(Debug, Deserialize)]
struct RootRoleEntry {
    root: String,
    root_kind: RootKind,
    mutation_visible: bool,
    role: Role,
    cas_authority: bool,
    write_public_parquet: bool,
    create_mutation_visible_state: bool,
    write_control_plane_state: bool,
    write_table_protocol: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum RootKind {
    Control,
    Projection,
    PublicParquet,
    Export,
    TableData,
}

#[derive(Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum Role {
    ApiControlWriter,
    ProjectionCompactor,
    SnapshotExportService,
    QueryEnforcementService,
    TableEngine,
    ExternalEngine,
}

#[test]
fn fixture_uses_expected_schema_version() {
    let fixture = root_ownership_fixture();
    assert_eq!(fixture.schema_version, "arco.root-ownership-and-iam.v1");
}

#[test]
fn fixture_has_required_role_root_coverage() {
    let fixture = root_ownership_fixture();

    assert_no_root_ownership_errors(&fixture);
}

#[test]
fn mutation_visible_roots_have_at_most_one_cas_authority() {
    let fixture = root_ownership_fixture();
    let mut authorities_by_root: BTreeMap<&str, Vec<&Role>> = BTreeMap::new();

    for entry in &fixture.entries {
        if entry.mutation_visible && entry.cas_authority {
            authorities_by_root
                .entry(&entry.root)
                .or_default()
                .push(&entry.role);
        }
    }

    for (root, authorities) in authorities_by_root {
        assert!(
            authorities.len() <= 1,
            "{root}: mutation-visible roots may have at most one CAS authority, found {authorities:?}"
        );
    }
}

#[test]
fn roles_cannot_cross_authority_boundaries() {
    let fixture = root_ownership_fixture();

    for entry in fixture.entries {
        match entry.role {
            Role::ApiControlWriter => {
                assert!(
                    !entry.write_public_parquet,
                    "{}: API/control writers must not write public Parquet roots",
                    entry.root
                );
            }
            Role::ProjectionCompactor => {
                if entry.mutation_visible {
                    assert!(
                        !entry.cas_authority,
                        "{}: projection compactors must not CAS mutation-visible roots",
                        entry.root
                    );
                }
            }
            Role::SnapshotExportService => {
                assert!(
                    !entry.create_mutation_visible_state,
                    "{}: snapshot/export services must not create mutation-visible state",
                    entry.root
                );
            }
            Role::QueryEnforcementService => {
                assert!(
                    !entry.write_control_plane_state && !entry.cas_authority,
                    "{}: query/enforcement services must not mutate control-plane state",
                    entry.root
                );
            }
            Role::TableEngine | Role::ExternalEngine => {
                if matches!(entry.root_kind, RootKind::Control | RootKind::Projection) {
                    assert!(
                        !entry.write_control_plane_state && !entry.cas_authority,
                        "{}: engines must not mutate catalog/control-plane roots",
                        entry.root
                    );
                    assert!(
                        !entry.write_table_protocol,
                        "{}: table protocol writes are not valid on control/projection roots",
                        entry.root
                    );
                }
            }
        }
    }
}

#[test]
fn empty_root_matrix_is_rejected() {
    let fixture = RootOwnershipFixture {
        schema_version: "arco.root-ownership-and-iam.v1".to_owned(),
        entries: Vec::new(),
    };

    let errors = root_ownership_errors(&fixture);

    assert!(
        errors
            .iter()
            .any(|error| error.contains("must list required role/root entries")),
        "expected required matrix coverage error, got {errors:?}"
    );
}

#[test]
fn snapshot_export_service_cannot_own_mutation_authority() {
    let fixture = RootOwnershipFixture {
        schema_version: "arco.root-ownership-and-iam.v1".to_owned(),
        entries: vec![RootRoleEntry {
            root: "control/catalog-root".to_owned(),
            root_kind: RootKind::Control,
            mutation_visible: true,
            role: Role::SnapshotExportService,
            cas_authority: true,
            write_public_parquet: false,
            create_mutation_visible_state: false,
            write_control_plane_state: true,
            write_table_protocol: false,
        }],
    };

    let errors = root_ownership_errors(&fixture);

    assert!(
        errors.iter().any(|error| error
            .contains("snapshot/export services must not own mutation authority")),
        "expected snapshot/export mutation-authority error, got {errors:?}"
    );
}

#[test]
fn engines_can_write_table_protocol_only_on_table_data_roots() {
    let fixture = RootOwnershipFixture {
        schema_version: "arco.root-ownership-and-iam.v1".to_owned(),
        entries: vec![RootRoleEntry {
            root: "public-parquet/catalog-projection".to_owned(),
            root_kind: RootKind::PublicParquet,
            mutation_visible: false,
            role: Role::ExternalEngine,
            cas_authority: false,
            write_public_parquet: false,
            create_mutation_visible_state: false,
            write_control_plane_state: false,
            write_table_protocol: true,
        }],
    };

    let errors = root_ownership_errors(&fixture);

    assert!(
        errors.iter().any(|error| error
            .contains("engine table-protocol writes are only valid on table-data roots")),
        "expected engine table-protocol root error, got {errors:?}"
    );
}

#[test]
fn engines_cannot_write_public_parquet_or_export_roots() {
    let fixture = RootOwnershipFixture {
        schema_version: "arco.root-ownership-and-iam.v1".to_owned(),
        entries: vec![
            RootRoleEntry {
                root: "public-parquet/catalog-projection".to_owned(),
                root_kind: RootKind::PublicParquet,
                mutation_visible: false,
                role: Role::TableEngine,
                cas_authority: false,
                write_public_parquet: true,
                create_mutation_visible_state: false,
                write_control_plane_state: false,
                write_table_protocol: false,
            },
            RootRoleEntry {
                root: "exports/workspace-snapshots".to_owned(),
                root_kind: RootKind::Export,
                mutation_visible: false,
                role: Role::ExternalEngine,
                cas_authority: true,
                write_public_parquet: false,
                create_mutation_visible_state: false,
                write_control_plane_state: false,
                write_table_protocol: false,
            },
        ],
    };

    let errors = root_ownership_errors(&fixture);

    assert!(
        errors
            .iter()
            .any(|error| error.contains("engines must not write public Parquet or export roots")),
        "expected engine public/export root error, got {errors:?}"
    );
}

fn root_ownership_fixture() -> RootOwnershipFixture {
    serde_json::from_str(FIXTURE).expect("root ownership fixture should parse")
}

fn assert_no_root_ownership_errors(fixture: &RootOwnershipFixture) {
    let errors = root_ownership_errors(fixture);
    assert!(errors.is_empty(), "root ownership errors: {errors:?}");
}

fn root_ownership_errors(fixture: &RootOwnershipFixture) -> Vec<String> {
    let mut errors = Vec::new();

    if fixture.entries.is_empty() {
        errors.push("root ownership matrix must list required role/root entries".to_owned());
        return errors;
    }

    let present_entries: BTreeSet<(&str, &Role)> = fixture
        .entries
        .iter()
        .map(|entry| (entry.root.as_str(), &entry.role))
        .collect();
    for (root, role) in [
        ("control/catalog-root", Role::ApiControlWriter),
        ("control/catalog-root", Role::ProjectionCompactor),
        ("control/catalog-root", Role::QueryEnforcementService),
        ("control/catalog-root", Role::TableEngine),
        (
            "projection/catalog-system-tables",
            Role::ProjectionCompactor,
        ),
        ("projection/catalog-system-tables", Role::ApiControlWriter),
        (
            "public-parquet/catalog-projection",
            Role::ProjectionCompactor,
        ),
        ("public-parquet/catalog-projection", Role::ApiControlWriter),
        ("exports/workspace-snapshots", Role::SnapshotExportService),
        ("tables/managed/table-data", Role::TableEngine),
        ("tables/external/table-data", Role::ExternalEngine),
    ] {
        if !present_entries.contains(&(root, &role)) {
            errors.push(format!(
                "root ownership matrix must list required role/root entry {role:?} on {root}"
            ));
        }
    }

    let mut authorities_by_root: BTreeMap<&str, Vec<&Role>> = BTreeMap::new();
    for entry in &fixture.entries {
        if entry.mutation_visible && entry.cas_authority {
            authorities_by_root
                .entry(&entry.root)
                .or_default()
                .push(&entry.role);
        }
    }
    for (root, authorities) in authorities_by_root {
        if authorities.len() > 1 {
            errors.push(format!(
                "{root}: mutation-visible roots may have at most one CAS authority, found {authorities:?}"
            ));
        }
    }

    for entry in &fixture.entries {
        match entry.role {
            Role::ApiControlWriter => {
                if entry.write_public_parquet {
                    errors.push(format!(
                        "{}: API/control writers must not write public Parquet roots",
                        entry.root
                    ));
                }
            }
            Role::ProjectionCompactor => {
                if entry.mutation_visible && entry.cas_authority {
                    errors.push(format!(
                        "{}: projection compactors must not CAS mutation-visible roots",
                        entry.root
                    ));
                }
            }
            Role::SnapshotExportService => {
                if entry.mutation_visible
                    || entry.cas_authority
                    || entry.create_mutation_visible_state
                    || entry.write_control_plane_state
                {
                    errors.push(format!(
                        "{}: snapshot/export services must not own mutation authority",
                        entry.root
                    ));
                }
            }
            Role::QueryEnforcementService => {
                if entry.write_control_plane_state || entry.cas_authority {
                    errors.push(format!(
                        "{}: query/enforcement services must not mutate control-plane state",
                        entry.root
                    ));
                }
            }
            Role::TableEngine | Role::ExternalEngine => {
                if matches!(entry.root_kind, RootKind::PublicParquet | RootKind::Export)
                    && (entry.write_public_parquet
                        || entry.cas_authority
                        || entry.write_control_plane_state
                        || entry.create_mutation_visible_state)
                {
                    errors.push(format!(
                        "{}: engines must not write public Parquet or export roots",
                        entry.root
                    ));
                }
                if entry.write_table_protocol && entry.root_kind != RootKind::TableData {
                    errors.push(format!(
                        "{}: engine table-protocol writes are only valid on table-data roots",
                        entry.root
                    ));
                }
                if matches!(entry.root_kind, RootKind::Control | RootKind::Projection)
                    && (entry.write_control_plane_state || entry.cas_authority)
                {
                    errors.push(format!(
                        "{}: engines must not mutate catalog/control-plane roots",
                        entry.root
                    ));
                }
            }
        }
    }

    errors
}
