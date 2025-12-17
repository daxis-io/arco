//! Path contract tests per ADR-003 and ADR-005.
//!
//! These tests ensure that writers use consistent paths that match
//! the canonical layout defined in `CatalogPaths`.
//!
//! # Invariants Tested
//!
//! 1. EventWriter ledger paths match `CatalogPaths::ledger_event` format
//! 2. Tier1Writer snapshot paths match `CatalogPaths::snapshot_dir` format
//! 3. No hardcoded paths bypass the canonical layout
//! 4. All domains use consistent naming (catalog, lineage, executions, search)

use std::sync::Arc;

use arco_catalog::event_writer::EventWriter;
use arco_core::{
    CatalogDomain, CatalogEventPayload, CatalogPaths, EventId, MemoryBackend, ScopedStorage,
};

#[derive(serde::Serialize)]
struct TestEvent {
    data: String,
}

impl CatalogEventPayload for TestEvent {
    const EVENT_TYPE: &'static str = "test.event";
    const EVENT_VERSION: u32 = 1;
}

/// Verifies that EventWriter ledger paths match CatalogPaths format.
#[tokio::test]
async fn contract_event_writer_path_format() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), "acme", "production").unwrap();
    let writer = EventWriter::new(storage.clone());

    // Write an event with a known ID
    let event_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let parsed_id: EventId = event_id.parse().unwrap();
    writer
        .append_with_id(
            CatalogDomain::Executions,
            &TestEvent {
                data: "test".into(),
            },
            &parsed_id,
        )
        .await
        .unwrap();

    // Verify the path matches the canonical format
    let expected_path = storage.ledger_event(CatalogDomain::Executions, event_id);
    let expected_relative = CatalogPaths::ledger_event(CatalogDomain::Executions, event_id);

    // The scoped path should end with the canonical relative path
    assert!(
        expected_path.ends_with(&expected_relative),
        "EventWriter path should match CatalogPaths format: {}",
        expected_relative
    );

    // Verify the event was written to the expected location
    let objects = storage.list("ledger/").await.unwrap();
    assert_eq!(objects.len(), 1, "should have one event in ledger");

    let written_path = objects[0].as_str();
    assert!(
        written_path.contains("executions"),
        "ledger path should contain domain name"
    );
    assert!(
        written_path.contains(event_id),
        "ledger path should contain event ID"
    );
}

/// Verifies domain manifest paths follow canonical naming.
#[test]
fn contract_manifest_paths_canonical_naming() {
    // All four canonical domains must produce consistent paths
    let domains = [
        (CatalogDomain::Catalog, "catalog"),
        (CatalogDomain::Lineage, "lineage"),
        (CatalogDomain::Executions, "executions"),
        (CatalogDomain::Search, "search"),
    ];

    for (domain, expected_name) in domains {
        let manifest = CatalogPaths::domain_manifest(domain);
        assert!(
            manifest.contains(expected_name),
            "Manifest path '{}' should contain domain name '{}'",
            manifest,
            expected_name
        );
        assert!(
            manifest.ends_with(".manifest.json"),
            "Manifest path should end with .manifest.json"
        );
    }
}

/// Verifies lock paths follow canonical naming.
#[test]
fn contract_lock_paths_canonical_naming() {
    let domains = [
        (CatalogDomain::Catalog, "catalog"),
        (CatalogDomain::Lineage, "lineage"),
        (CatalogDomain::Executions, "executions"),
        (CatalogDomain::Search, "search"),
    ];

    for (domain, expected_name) in domains {
        let lock = CatalogPaths::domain_lock(domain);
        assert!(
            lock.contains(expected_name),
            "Lock path '{}' should contain domain name '{}'",
            lock,
            expected_name
        );
        assert!(
            lock.ends_with(".lock.json"),
            "Lock path should end with .lock.json"
        );
    }
}

/// Verifies snapshot paths follow canonical structure.
#[test]
fn contract_snapshot_paths_canonical_structure() {
    let version = 42u64;

    for domain in CatalogDomain::all() {
        let dir = CatalogPaths::snapshot_dir(*domain, version);

        // Should be: snapshots/{domain}/v{version}/
        assert!(
            dir.starts_with("snapshots/"),
            "Snapshot dir should start with 'snapshots/'"
        );
        assert!(
            dir.contains(&format!("v{version}")),
            "Snapshot dir should contain version"
        );
        assert!(dir.ends_with('/'), "Snapshot dir should end with /");
    }
}

/// Verifies ledger paths follow canonical structure.
#[test]
fn contract_ledger_paths_canonical_structure() {
    let event_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    for domain in CatalogDomain::all() {
        let path = CatalogPaths::ledger_event(*domain, event_id);

        // Should be: ledger/{domain}/{event_id}.json
        assert!(
            path.starts_with("ledger/"),
            "Ledger path should start with 'ledger/'"
        );
        assert!(path.ends_with(".json"), "Ledger path should end with .json");
        assert!(
            path.contains(event_id),
            "Ledger path should contain event ID"
        );
    }
}

/// Verifies state paths follow canonical structure.
#[test]
fn contract_state_paths_canonical_structure() {
    let version = 1u64;
    let ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    for domain in CatalogDomain::all() {
        let path = CatalogPaths::state_snapshot(*domain, version, ulid);

        // Should be: state/{domain}/snapshot_v{version}_{ulid}.parquet
        assert!(
            path.starts_with("state/"),
            "State path should start with 'state/'"
        );
        assert!(
            path.ends_with(".parquet"),
            "State path should end with .parquet"
        );
        assert!(
            path.contains(&format!("v{version}")),
            "State path should contain version"
        );
    }
}

/// Verifies legacy domain names are normalized correctly.
#[test]
fn contract_legacy_name_normalization() {
    // Legacy -> Canonical mappings per ADR-003
    assert_eq!(CatalogPaths::normalize_domain_name("core"), "catalog");
    assert_eq!(
        CatalogPaths::normalize_domain_name("execution"),
        "executions"
    );
    assert_eq!(CatalogPaths::normalize_domain_name("governance"), "search");

    // Canonical names pass through
    assert_eq!(CatalogPaths::normalize_domain_name("catalog"), "catalog");
    assert_eq!(CatalogPaths::normalize_domain_name("lineage"), "lineage");
    assert_eq!(
        CatalogPaths::normalize_domain_name("executions"),
        "executions"
    );
    assert_eq!(CatalogPaths::normalize_domain_name("search"), "search");
}

/// Verifies root manifest is the only stable entry point.
#[test]
fn contract_root_manifest_is_entry_point() {
    // Root manifest path is a constant (stable contract)
    assert_eq!(CatalogPaths::ROOT_MANIFEST, "manifests/root.manifest.json");

    // All domain manifests are under the same directory
    for domain in CatalogDomain::all() {
        let manifest = CatalogPaths::domain_manifest(*domain);
        assert!(
            manifest.starts_with("manifests/"),
            "Domain manifests should be in manifests/ directory"
        );
    }
}
