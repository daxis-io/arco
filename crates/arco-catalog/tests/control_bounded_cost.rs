//! Frozen authority-8 catalog workload shapes and explicit cost-lane entry points.
#![cfg(feature = "test-utils")]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]

#[path = "../benches/support/control_cost.rs"]
#[allow(dead_code)]
mod control_cost;

use std::future::Future;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use arco_catalog::catalog_authority::{
    BoundedCatalogTestCommand, CatalogProjectionNotifierV2, PreparedBoundedCatalogMutation,
    bounded_capacity_v2_intent, bounded_capacity_v2_record_pair,
};
use arco_catalog::state_store::SyntheticKvEntry;
use arco_catalog::state_store::{CandidateRecoveryV2, ProjectionIntentV2};
use arco_catalog::{
    ArcoStateReader, ArcoStateTxn, CatalogPatch, ColumnDefinition, ControlMvpReadCacheConfig,
    ControlMvpReadCacheStatistics, ControlMvpStateStore, RegisterTableInSchemaRequest, StateScope,
    TxnOptions, WriteOptions,
};
use arco_catalog::{CatalogError, ControlCatalogAuthority};
use arco_core::{MemoryBackend, ScopedStorage};
use bytes::Bytes;
use serde::Serialize;
use sha2::{Digest, Sha256};

const OPERATIONS_PER_SAMPLE: u64 = 200;
const REPETITIONS: u64 = 5;
const DECLARED_BLOCK_BOUND: u64 = 16;

async fn measure_catalog_request<T>(
    backend: &control_cost::CountingBackend,
    request: impl Future<Output = T>,
) -> (
    T,
    control_cost::RequestAllocations,
    control_cost::BackendCounts,
) {
    let prior_nested = backend.set_nested_allocation_measurement(false);
    let (result, allocations) = control_cost::measure_request_allocations(request).await;
    backend.set_nested_allocation_measurement(prior_nested);
    let counters = backend.take();
    (result, allocations, counters)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct FixtureAxis {
    retained_rows: u64,
    active_records: u64,
}

const FIXTURE_AXES: [FixtureAxis; 6] = [
    FixtureAxis {
        retained_rows: 4_096,
        active_records: 128,
    },
    FixtureAxis {
        retained_rows: 65_536,
        active_records: 128,
    },
    FixtureAxis {
        retained_rows: 1_048_576,
        active_records: 128,
    },
    FixtureAxis {
        retained_rows: 2_419_200,
        active_records: 128,
    },
    FixtureAxis {
        retained_rows: 4_096,
        active_records: 4_096,
    },
    FixtureAxis {
        retained_rows: 4_096,
        active_records: 65_536,
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scenario {
    CreateCatalog,
    CreateSchema,
    RegisterTable,
    PatchCatalog,
    RenameTable,
    DropTable,
    IdempotentReplay,
    StaleGenerationConflict,
    OrderedOutboxPage,
    ExactTrim,
}

impl Scenario {
    const fn label(self) -> &'static str {
        match self {
            Self::CreateCatalog => "create-catalog",
            Self::CreateSchema => "create-schema",
            Self::RegisterTable => "register-table",
            Self::PatchCatalog => "patch-catalog",
            Self::RenameTable => "rename-table",
            Self::DropTable => "drop-table",
            Self::IdempotentReplay => "idempotent-replay",
            Self::StaleGenerationConflict => "stale-generation-conflict",
            Self::OrderedOutboxPage => "ordered-outbox-page",
            Self::ExactTrim => "exact-trim",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CacheMode {
    Disabled,
    Default,
    Pressure,
}

impl CacheMode {
    const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Default => "default",
            Self::Pressure => "pressure",
        }
    }
}

const CACHE_MODES: [CacheMode; 3] = [CacheMode::Disabled, CacheMode::Default, CacheMode::Pressure];

const SCENARIOS: [Scenario; 10] = [
    Scenario::CreateCatalog,
    Scenario::CreateSchema,
    Scenario::RegisterTable,
    Scenario::PatchCatalog,
    Scenario::RenameTable,
    Scenario::DropTable,
    Scenario::IdempotentReplay,
    Scenario::StaleGenerationConflict,
    Scenario::OrderedOutboxPage,
    Scenario::ExactTrim,
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct FrozenCatalogOperation {
    scenario: Scenario,
    ordinal: u64,
    operation_id: String,
    idempotency_key: String,
    catalog: String,
    schema: String,
    table: String,
}

impl FrozenCatalogOperation {
    fn new(
        scenario: Scenario,
        axis: usize,
        cache: CacheMode,
        repetition: u64,
        ordinal: u64,
    ) -> Self {
        let prefix = format!(
            "{}-a{axis}-{}-r{repetition:02}-n{ordinal:03}",
            scenario.label(),
            cache.label(),
        );
        Self {
            scenario,
            ordinal,
            operation_id: format!("bounded-{prefix}"),
            idempotency_key: format!("bounded-idem-{prefix}"),
            catalog: format!("catalog-{prefix}"),
            schema: format!("schema-{prefix}"),
            table: format!("table-{prefix}"),
        }
    }
}

fn frozen_operations(
    scenario: Scenario,
    axis: usize,
    cache: CacheMode,
    repetition: u64,
) -> impl Iterator<Item = FrozenCatalogOperation> {
    (1..=OPERATIONS_PER_SAMPLE)
        .map(move |ordinal| FrozenCatalogOperation::new(scenario, axis, cache, repetition, ordinal))
}

/// Streams the ordered KV predecessor rows for explicit synthetic genesis.
///
/// Receipt keys precede audit keys by the production record key tags. Each row
/// is encoded by the catalog authority's production V2 record encoder.
struct V2PredecessorRows {
    next: u64,
    retained_rows: u64,
}

impl V2PredecessorRows {
    fn new(retained_rows: u64) -> Self {
        Self {
            next: 1,
            retained_rows,
        }
    }
}

impl Iterator for V2PredecessorRows {
    type Item = SyntheticKvEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let ordinal = self.next;
        if ordinal > self.retained_rows {
            return None;
        }
        self.next += 1;
        let pair_count = self.retained_rows / 2;
        let source_ordinal = if ordinal <= pair_count {
            ordinal
        } else {
            ordinal - pair_count
        };
        let (receipt, audit) = bounded_capacity_v2_record_pair(source_ordinal)
            .expect("frozen production V2 fixture record");
        let (key, value) = if ordinal <= pair_count {
            receipt
        } else {
            audit
        };
        Some(SyntheticKvEntry {
            key,
            generation: source_ordinal,
            value: Some(value),
        })
    }
}

#[test]
fn frozen_matrix_and_predecessor_rows_are_exact_and_ordered() {
    assert_eq!(FIXTURE_AXES.len(), 6);
    assert_eq!(FIXTURE_AXES[0].retained_rows, FIXTURE_AXES[4].retained_rows);
    assert_ne!(
        FIXTURE_AXES[0].active_records,
        FIXTURE_AXES[4].active_records
    );
    assert_eq!(REPETITIONS * OPERATIONS_PER_SAMPLE, 1_000);
    assert_eq!(SCENARIOS.len(), 10);
    assert_eq!(CACHE_MODES.len(), 3);

    let operations =
        frozen_operations(Scenario::CreateCatalog, 0, CacheMode::Default, 1).collect::<Vec<_>>();
    assert_eq!(
        operations.len(),
        usize::try_from(OPERATIONS_PER_SAMPLE).expect("operation count")
    );
    assert!(
        operations
            .windows(2)
            .all(|pair| pair[0].operation_id < pair[1].operation_id)
    );
    assert!(
        operations
            .windows(2)
            .all(|pair| pair[0].catalog < pair[1].catalog)
    );
    let distinct_scenario = frozen_operations(Scenario::CreateSchema, 0, CacheMode::Default, 1)
        .next()
        .expect("first scenario operation");
    let distinct_axis = frozen_operations(Scenario::CreateCatalog, 1, CacheMode::Default, 1)
        .next()
        .expect("first axis operation");
    let distinct_cache = frozen_operations(Scenario::CreateCatalog, 0, CacheMode::Pressure, 1)
        .next()
        .expect("first cache operation");
    assert_ne!(operations[0].operation_id, distinct_scenario.operation_id);
    assert_ne!(operations[0].operation_id, distinct_axis.operation_id);
    assert_ne!(operations[0].operation_id, distinct_cache.operation_id);

    let rows = V2PredecessorRows::new(6).collect::<Vec<_>>();
    assert_eq!(rows.len(), 6);
    assert!(rows.windows(2).all(|pair| pair[0].key < pair[1].key));
    let receipt: serde_json::Value =
        serde_json::from_slice(rows[0].value.as_deref().unwrap()).expect("receipt JSON");
    let audit: serde_json::Value =
        serde_json::from_slice(rows[3].value.as_deref().unwrap()).expect("audit JSON");
    assert_eq!(receipt["logicalSequence"], 1);
    assert_eq!(audit["logicalSequence"], 1);
    assert!(receipt.get("authorityManifestId").is_none());
    assert!(audit.get("authorityManifestId").is_none());
}

#[tokio::test]
async fn streamed_production_v2_predecessor_requires_authenticated_genesis_publication() {
    let storage = ScopedStorage::new(
        Arc::new(MemoryBackend::new()),
        "synthetic-tenant",
        "synthetic-workspace",
    )
    .expect("storage");
    let scope = StateScope::new("synthetic-tenant", "synthetic-workspace", "catalog");
    let store = ControlMvpStateStore::new_synthetic_bounded(storage, scope.clone()).expect("store");
    let intents = (1..=3)
        .map(|origin| bounded_capacity_v2_intent(scope.clone(), origin, 0))
        .collect::<Result<Vec<_>, _>>()
        .expect("fixed historical intents");

    let token = store
        .install_synthetic_genesis(
            "bounded-cost-predecessor",
            3,
            6,
            V2PredecessorRows::new(6),
            3,
            intents,
        )
        .await
        .expect("synthetic genesis must publish");
    let first_page = store
        .projection_outbox_page_v2(Some(token), None, 1)
        .await
        .expect("authenticated bounded delivery page");
    assert_eq!(first_page.records().len(), 1);
    let intent = first_page.records()[0].clone();
    assert_eq!(
        intent.intent_id(),
        "intent-00000000000000000001-00000000000000000000"
    );
    let mut trim = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("bounded trim transaction");
    trim.set_logical_operation_v2("fixture-trim", "exact_trim", &"44".repeat(32))
        .expect("frozen trim identity");
    trim.trim_projection_intents_v2(&[intent])
        .await
        .expect("exact live V2 incarnation");
    trim.commit_v2()
        .await
        .expect("bounded exact trim publication");
}

#[derive(Default, Debug, Serialize)]
struct CandidateInventoryClass {
    candidates: usize,
    envelope_bytes: u64,
    manifest_bytes: u64,
}

#[derive(Debug, Serialize)]
struct PreparedCandidateInventory {
    attached: CandidateInventoryClass,
    unattached: CandidateInventoryClass,
    current_root: serde_json::Value,
}

fn require_manifest_scope(value: &serde_json::Value, scope: &StateScope) {
    assert_eq!(value["scope"]["tenant_id"], scope.tenant_id());
    assert_eq!(value["scope"]["workspace_id"], scope.workspace_id());
    assert_eq!(value["scope"]["domain"], scope.domain());
}

fn manifest_path(id: &str) -> String {
    assert!(!id.is_empty() && id.len() <= 1024);
    assert!(
        id.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    );
    format!("control/v1/domains/catalog/manifests/{id}.json")
}

// This test-only inventory walks short private fixture ancestry, outside measured requests.
#[allow(clippy::too_many_lines)]
async fn prepared_candidate_inventory(
    storage: &ScopedStorage,
    scope: &StateScope,
) -> PreparedCandidateInventory {
    let store = ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope.clone())
        .expect("inventory store");
    let current_root = store
        .bounded_root_inventory_v2()
        .await
        .expect("authenticated current root");
    let mut attached = std::collections::BTreeSet::new();
    let mut manifest_id = current_root["manifestId"]
        .as_str()
        .expect("manifest ID")
        .to_string();
    let mut digest = current_root["manifestSha256"]
        .as_str()
        .expect("manifest digest")
        .to_string();
    loop {
        assert!(
            attached.len() < 32,
            "private fixture ancestry exceeded declared inventory bound"
        );
        assert!(
            attached.insert(manifest_id.clone()),
            "cyclic manifest ancestry"
        );
        let bytes = storage
            .get_raw(&manifest_path(&manifest_id))
            .await
            .expect("ancestor manifest");
        assert_eq!(format!("{:x}", Sha256::digest(&bytes)), digest);
        let manifest: serde_json::Value = serde_json::from_slice(&bytes).expect("manifest JSON");
        assert_eq!(manifest["format_version"], 8);
        assert_eq!(manifest["manifest_id"], manifest_id);
        require_manifest_scope(&manifest, scope);
        let Some(parent) = manifest["parent_manifest_id"].as_str() else {
            assert!(manifest["parent_manifest_sha256"].is_null());
            break;
        };
        manifest_id = parent.to_string();
        digest = manifest["parent_manifest_sha256"]
            .as_str()
            .expect("parent digest")
            .to_string();
    }
    let candidates = storage
        .list_meta("control/v1/domains/catalog/prepared")
        .await
        .expect("private candidates");
    let mut inventory = PreparedCandidateInventory {
        attached: CandidateInventoryClass::default(),
        unattached: CandidateInventoryClass::default(),
        current_root,
    };
    for candidate in candidates {
        let path = candidate.path.to_string();
        let id = path
            .strip_prefix("control/v1/domains/catalog/prepared/")
            .and_then(|path| path.strip_suffix(".json"))
            .expect("prepared candidate path");
        let envelope_bytes = storage.get_raw(&path).await.expect("candidate envelope");
        assert_eq!(envelope_bytes.len() as u64, candidate.size);
        let envelope: serde_json::Value =
            serde_json::from_slice(&envelope_bytes).expect("envelope JSON");
        assert_eq!(envelope["candidate_id"], id);
        assert_eq!(envelope["encoding_version"], 1);
        assert_eq!(
            envelope["record_type"],
            "arco_control_bounded_prepared_candidate"
        );
        assert_eq!(
            envelope["candidate_head"]["manifest"],
            envelope["candidate_manifest"]
        );
        require_manifest_scope(&envelope, scope);
        let candidate_manifest = manifest_path(id);
        assert_eq!(envelope["candidate_manifest"]["path"], candidate_manifest);
        let bytes = storage
            .get_raw(&candidate_manifest)
            .await
            .expect("candidate manifest");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            envelope["candidate_manifest"]["sha256"]
                .as_str()
                .expect("candidate manifest digest")
        );
        let manifest: serde_json::Value =
            serde_json::from_slice(&bytes).expect("candidate manifest JSON");
        assert_eq!(manifest["manifest_id"], id);
        assert_eq!(manifest["format_version"], 8);
        require_manifest_scope(&manifest, scope);
        let class = if attached.contains(id) {
            &mut inventory.attached
        } else {
            &mut inventory.unattached
        };
        class.candidates += 1;
        class.envelope_bytes += envelope_bytes.len() as u64;
        class.manifest_bytes += bytes.len() as u64;
    }
    inventory
}

#[tokio::test]
async fn prepared_candidates_restore_classifies_attached_and_unattached_before_inventory_fix() {
    let backend = Arc::new(control_cost::CountingBackend::new(1));
    let storage =
        ScopedStorage::new(backend.clone(), "candidate-classifier", "fixture").expect("storage");
    let scope = StateScope::new("candidate-classifier", "fixture", "catalog");
    let store =
        ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope.clone()).expect("store");
    let genesis = store
        .install_synthetic_genesis(
            "candidate-genesis",
            1,
            2,
            V2PredecessorRows::new(2),
            0,
            Vec::new(),
        )
        .await
        .expect("genesis");
    let head_path = "control/v1/domains/catalog/head/current.json";
    let genesis_head = storage.get_raw(head_path).await.expect("genesis HEAD");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("transaction");
    txn.set_logical_operation_v2("candidate-mutation", "candidate_test", &"aa".repeat(32))
        .expect("logical operation");
    txn.put(b"candidate-key", Bytes::from_static(b"candidate-value"))
        .await
        .expect("mutation");
    let committed = txn.commit_v2().await.expect("mutation commit");
    let version = storage
        .head_raw(head_path)
        .await
        .expect("HEAD meta")
        .expect("HEAD exists")
        .version;
    backend.replace_head_for_bounded_fixture(
        "tenant=candidate-classifier/workspace=fixture/control/v1/domains/catalog/head/current.json",
        &version,
        genesis_head,
    ).await.expect("restore genesis HEAD");
    assert!(
        store
            .read_at(committed.token().clone())
            .await
            .expect("retained committed token")
            .get(b"candidate-key")
            .await
            .expect("retained read")
            .is_some()
    );
    let inventory = prepared_candidate_inventory(&storage, &scope).await;
    assert_eq!(
        (
            inventory.attached.candidates,
            inventory.unattached.candidates
        ),
        (1, 1)
    );
    assert!(inventory.unattached.envelope_bytes > 0 && inventory.unattached.manifest_bytes > 0);
    assert_eq!(inventory.current_root["rootRows"]["kv"], 2);
    assert_eq!(inventory.current_root["rootRows"]["activeId"], 0);
    assert_eq!(inventory.current_root["rootRows"]["deliveryOrder"], 0);
    let restarted = ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope.clone())
        .expect("restarted store");
    assert!(matches!(
        restarted
            .reconcile_candidate_v2(committed.token().authority_manifest_id())
            .await
            .expect("restart candidate"),
        CandidateRecoveryV2::Unresolved
    ));
    assert_eq!(genesis.logical_sequence(), 1);
}

#[derive(Default)]
struct NoopV2Notifier;

impl CatalogProjectionNotifierV2 for NoopV2Notifier {
    fn notify(&self, _: &ProjectionIntentV2) -> arco_catalog::Result<()> {
        Ok(())
    }
}

#[derive(Serialize)]
struct BoundedCostSample<'a> {
    kind: &'static str,
    source_identity: &'a str,
    axis: FixtureAxis,
    cache: &'static str,
    scenario: &'static str,
    repetition: u64,
    ordinal: u64,
    operation_id: &'a str,
    idempotency_key: &'a str,
    request_digest: &'a str,
    occurred_at_ms: i64,
    elapsed_micros: u128,
    request_allocations: &'a control_cost::RequestAllocations,
    backend: &'a control_cost::BackendCounts,
    verification_allocations: Option<&'a control_cost::RequestAllocations>,
    verification_backend: Option<&'a control_cost::BackendCounts>,
    published_logical: Option<&'a serde_json::Value>,
    head_before: &'a HeadInventory,
    head_after: &'a HeadInventory,
    store_cache_before: Option<&'a ControlMvpReadCacheStatistics>,
    store_cache_after: Option<&'a ControlMvpReadCacheStatistics>,
    authority_cache_before: Option<&'a ControlMvpReadCacheStatistics>,
    authority_cache_after: Option<&'a ControlMvpReadCacheStatistics>,
    reset_count: u64,
    result: &'static str,
}

#[derive(Clone, Serialize)]
struct HeadInventory {
    version: String,
    bytes: usize,
    sha256: String,
}

#[derive(Serialize)]
struct PhaseSample<'a> {
    kind: &'static str,
    source_identity: &'a str,
    phase: &'static str,
    axis: FixtureAxis,
    cache: &'static str,
    scenario: &'static str,
    repetition: u64,
    ordinal: Option<u64>,
    backend: &'a control_cost::BackendCounts,
    head: &'a HeadInventory,
    reset: Option<&'a control_cost::FixtureHeadReset>,
}

#[derive(Serialize)]
struct SummarySample<'a> {
    kind: &'static str,
    source_identity: &'a str,
    axis: FixtureAxis,
    cache: &'static str,
    scenario: &'static str,
    repetition: u64,
    operations: u64,
    final_head: &'a HeadInventory,
}

#[derive(Serialize)]
struct PrivateArtifactInventory {
    kind: &'static str,
    source_identity: String,
    axis: FixtureAxis,
    object_count: usize,
    object_bytes: u64,
    prepared_candidate_objects: usize,
    candidate_manifest_objects: usize,
    descriptor_objects: usize,
    prepared_candidates: PreparedCandidateInventory,
    inventory_backend: control_cost::BackendCounts,
    semantics: &'static str,
}

fn append_jsonl<T: Serialize>(write_jsonl: bool, sample: &T) {
    if !write_jsonl {
        return;
    }
    let path = std::env::var("ARCO_BOUNDED_COST_JSONL").expect("bounded cost JSONL output path");
    let mut output = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("cost JSONL output");
    writeln!(
        output,
        "{}",
        serde_json::to_string(sample).expect("JSON sample")
    )
    .expect("write JSONL");
}

async fn head_inventory(storage: &ScopedStorage, head_path: &str) -> HeadInventory {
    let meta = storage
        .head_raw(head_path)
        .await
        .expect("fixture HEAD metadata")
        .expect("fixture HEAD exists");
    let bytes = storage
        .get_raw(head_path)
        .await
        .expect("fixture HEAD bytes");
    HeadInventory {
        version: meta.version,
        bytes: bytes.len(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    }
}

fn cache_statistics(store: &ControlMvpStateStore) -> Option<ControlMvpReadCacheStatistics> {
    store.read_cache().map(|cache| cache.statistics())
}

fn actual_depth(proof: &serde_json::Value) -> u64 {
    let depths = proof
        .get("actualRootDepths")
        .and_then(serde_json::Value::as_object)
        .expect("published proof must include actualRootDepths");
    ["kv", "activeId", "deliveryOrder"]
        .into_iter()
        .map(|role| {
            depths
                .get(role)
                .and_then(serde_json::Value::as_u64)
                .expect("published proof root depth")
        })
        .max()
        .expect("three published root depths")
}

fn assert_bounded_cost_contract(
    counts: &control_cost::BackendCounts,
    allocations: &control_cost::RequestAllocations,
    proof: &serde_json::Value,
    scenario: Scenario,
) {
    let depth = actual_depth(proof);
    if scenario != Scenario::OrderedOutboxPage {
        assert!(
            proof
                .get("operation")
                .is_some_and(serde_json::Value::is_object),
            "{} missing published operation",
            scenario.label()
        );
    }
    assert!(
        proof
            .get("logicalCommitId")
            .is_some_and(serde_json::Value::is_string),
        "{} missing published logical identity",
        scenario.label()
    );
    assert!(
        counts.selected_blocks <= DECLARED_BLOCK_BOUND,
        "{} selected {} blocks",
        scenario.label(),
        counts.selected_blocks
    );
    assert!(counts.rewritten_blocks <= DECLARED_BLOCK_BOUND.saturating_mul(2));
    assert!(counts.block_decode_calls <= DECLARED_BLOCK_BOUND.saturating_mul(4));
    if !matches!(
        scenario,
        Scenario::IdempotentReplay | Scenario::OrderedOutboxPage
    ) {
        assert!(
            counts.transition_proof_rows > 0,
            "{} did not record bounded commit proof rows",
            scenario.label()
        );
        assert!(
            counts
                .phases
                .values()
                .any(|phase| phase.transition_proof_rows > 0),
            "{} did not record bounded commit proof rows in a named phase",
            scenario.label()
        );
    }
    assert_eq!(0, counts.streaming_builder_inputs);
    assert_eq!(0, counts.replayed_rows);
    assert_eq!(0, counts.full_checksum_calls);
    assert!(allocations.peak_owned_bytes <= 64 * 1024 * 1024);
    let directory = counts
        .object_class_io
        .get("directory_page")
        .cloned()
        .unwrap_or_default();
    let ceiling = 4_u64
        .saturating_mul(depth)
        .saturating_mul(DECLARED_BLOCK_BOUND.saturating_add(1));
    assert!(directory.full_get_attempts + directory.range_get_attempts <= ceiling);
    assert!(directory.put_attempts <= ceiling);
}

fn fixture_scope(axis: usize) -> StateScope {
    StateScope::new("bounded-cost", format!("axis-{axis}"), "catalog")
}

fn fixture_storage(backend: Arc<control_cost::CountingBackend>, axis: usize) -> ScopedStorage {
    ScopedStorage::new(backend, "bounded-cost", format!("axis-{axis}")).expect("fixture storage")
}

async fn restore_fixture_head(
    backend: &control_cost::CountingBackend,
    storage: &ScopedStorage,
    axis: usize,
    head_path: &str,
    bytes: Bytes,
) -> control_cost::FixtureHeadReset {
    let version = storage
        .head_raw(head_path)
        .await
        .expect("fixture HEAD metadata")
        .expect("fixture HEAD exists")
        .version;
    backend
        .replace_head_for_bounded_fixture(
            &format!("tenant=bounded-cost/workspace=axis-{axis}/{head_path}"),
            &version,
            bytes,
        )
        .await
        .expect("restore fixture HEAD")
}

fn catalog_request(name: &str) -> RegisterTableInSchemaRequest {
    RegisterTableInSchemaRequest {
        name: name.to_string(),
        description: Some("bounded-cost".to_string()),
        location: Some("s3://bounded-cost/table".to_string()),
        format: Some("delta".to_string()),
        table_type: Some("EXTERNAL".to_string()),
        properties: None,
        columns: vec![ColumnDefinition {
            name: "id".to_string(),
            data_type: "BIGINT".to_string(),
            is_nullable: false,
            ordinal: 0,
            description: None,
        }],
    }
}

fn cache_config(mode: CacheMode) -> Option<ControlMvpReadCacheConfig> {
    match mode {
        CacheMode::Disabled => None,
        CacheMode::Default => Some(ControlMvpReadCacheConfig::default()),
        CacheMode::Pressure => Some(ControlMvpReadCacheConfig {
            metadata_bytes: 1024 * 1024,
            decoded_bytes: 4 * 1024 * 1024,
        }),
    }
}

async fn bootstrap(
    authority: &ControlCatalogAuthority,
    scenario: Scenario,
) -> Result<(), CatalogError> {
    if !matches!(
        scenario,
        Scenario::CreateCatalog | Scenario::IdempotentReplay
    ) {
        authority
            .create_catalog_v2(
                "base",
                None,
                WriteOptions::with_idempotency("bootstrap-catalog"),
            )
            .await?;
    }
    if matches!(
        scenario,
        Scenario::RegisterTable | Scenario::RenameTable | Scenario::DropTable
    ) {
        authority
            .create_schema(
                "base",
                "main",
                None,
                WriteOptions::with_idempotency("bootstrap-schema"),
            )
            .await?;
    }
    if matches!(scenario, Scenario::RenameTable | Scenario::DropTable) {
        authority
            .register_table_in_schema(
                "base",
                "main",
                catalog_request("base-table"),
                WriteOptions::with_idempotency("bootstrap-table"),
            )
            .await?;
    }
    Ok(())
}

async fn actual_scenario(
    authority: &ControlCatalogAuthority,
    store: &ControlMvpStateStore,
    operation: &FrozenCatalogOperation,
    prepared: Option<PreparedBoundedCatalogMutation>,
) -> Result<(), CatalogError> {
    match operation.scenario {
        Scenario::CreateCatalog
        | Scenario::CreateSchema
        | Scenario::RegisterTable
        | Scenario::PatchCatalog
        | Scenario::RenameTable
        | Scenario::DropTable
        | Scenario::IdempotentReplay => {
            authority
                .execute_prepared_synthetic_bounded_command(
                    prepared.expect("frozen catalog command"),
                )
                .await?;
        }
        Scenario::StaleGenerationConflict => {
            let mut stale = store.begin_control_txn(TxnOptions::default()).await?;
            stale
                .put(b"bounded/stale", Bytes::from_static(b"first"))
                .await?;
            let mut winner = store.begin_control_txn(TxnOptions::default()).await?;
            winner
                .put(b"bounded/stale", Bytes::from_static(b"winner"))
                .await?;
            winner.set_logical_operation_v2(
                &operation.operation_id,
                "stale_generation_conflict",
                &"11".repeat(32),
            )?;
            winner.commit_v2().await?;
            stale
                .put(b"bounded/stale", Bytes::from_static(b"stale"))
                .await?;
            stale.set_logical_operation_v2(
                &format!("{}-stale", operation.operation_id),
                "stale_generation_conflict",
                &"22".repeat(32),
            )?;
            assert!(matches!(
                stale.commit_v2().await,
                Err(CatalogError::CasFailed { .. } | CatalogError::PreconditionFailed { .. })
            ));
        }
        Scenario::OrderedOutboxPage => {
            let page = store.projection_outbox_page_v2(None, None, 1).await?;
            assert_eq!(page.records().len(), 1);
        }
        Scenario::ExactTrim => {
            let page = store.projection_outbox_page_v2(None, None, 1).await?;
            let intent = page.records().first().expect("fixture intent").clone();
            let mut txn = store.begin_control_txn(TxnOptions::default()).await?;
            txn.set_logical_operation_v2(&operation.operation_id, "exact_trim", &"33".repeat(32))?;
            txn.trim_projection_intents_v2(&[intent]).await?;
            txn.commit_v2().await?;
        }
    }
    Ok(())
}

fn fixture_uuid(operation: &FrozenCatalogOperation, tag: u16) -> String {
    format!(
        "01900000-{:04x}-7{tag:03x}-8000-{:012x}",
        operation.ordinal, operation.ordinal
    )
}

fn frozen_catalog_command(operation: &FrozenCatalogOperation) -> Option<BoundedCatalogTestCommand> {
    let id = fixture_uuid(operation, 1);
    match operation.scenario {
        Scenario::CreateCatalog | Scenario::IdempotentReplay => {
            Some(BoundedCatalogTestCommand::CreateCatalog {
                id,
                name: operation.catalog.clone(),
            })
        }
        Scenario::CreateSchema => Some(BoundedCatalogTestCommand::CreateSchema {
            id,
            catalog: "base".to_string(),
            name: operation.schema.clone(),
        }),
        Scenario::RegisterTable => Some(BoundedCatalogTestCommand::RegisterTable {
            id,
            column_id: fixture_uuid(operation, 2),
            catalog: "base".to_string(),
            schema: "main".to_string(),
            request: catalog_request(&operation.table),
        }),
        Scenario::PatchCatalog => Some(BoundedCatalogTestCommand::PatchCatalog {
            name: "base".to_string(),
            patch: CatalogPatch {
                description: Some(Some(operation.operation_id.clone())),
                ..CatalogPatch::default()
            },
        }),
        Scenario::RenameTable => Some(BoundedCatalogTestCommand::RenameTable {
            catalog: "base".to_string(),
            schema: "main".to_string(),
            name: "base-table".to_string(),
            new_name: operation.table.clone(),
        }),
        Scenario::DropTable => Some(BoundedCatalogTestCommand::DropTable {
            catalog: "base".to_string(),
            schema: "main".to_string(),
            name: "base-table".to_string(),
        }),
        Scenario::StaleGenerationConflict | Scenario::OrderedOutboxPage | Scenario::ExactTrim => {
            None
        }
    }
}

#[tokio::test]
async fn prepared_bounded_command_freezes_identity_before_execution() {
    let _fixed_inputs = arco_core::test_inputs::FixedInputs::scoped();
    let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "frozen-command", "fixture")
        .expect("storage");
    let authority = ControlCatalogAuthority::new_synthetic_bounded(
        storage,
        StateScope::new("frozen-command", "fixture", "catalog"),
        Arc::new(NoopV2Notifier),
    )
    .expect("authority")
    .with_synthetic_bounded_read_cache(None)
    .expect("disabled cache");
    let command = authority
        .prepare_synthetic_bounded_command(
            BoundedCatalogTestCommand::CreateCatalog {
                id: "01900000-0001-7001-8000-000000000001".to_string(),
                name: "frozen".to_string(),
            },
            WriteOptions::with_idempotency("frozen-command").with_request_id("frozen-request"),
        )
        .expect("prepare before execution");
    let operation_id = command.operation_id().to_string();
    let digest = command.request_digest().to_string();
    let occurred_at_ms = command.occurred_at_ms();
    authority
        .execute_prepared_synthetic_bounded_command(command.clone())
        .await
        .expect("first interpreter execution");
    authority
        .execute_prepared_synthetic_bounded_command(command)
        .await
        .expect("idempotent interpreter replay");
    assert!(!operation_id.is_empty());
    assert_eq!(digest.len(), 64);
    assert!(occurred_at_ms > 0);
    assert!(
        authority
            .get_catalog("frozen")
            .await
            .expect("catalog lookup")
            .is_some()
    );
}

// FixedInputs is thread-local; this sequential current-thread fixture keeps every phase explicit.
#[allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::future_not_send,
    clippy::large_futures
)]
async fn run_matrix(axes: &[FixtureAxis], repetitions: u64, operations: u64, write_jsonl: bool) {
    let _fixed_inputs = arco_core::test_inputs::FixedInputs::scoped();
    let source_identity = if write_jsonl {
        std::env::var("ARCO_BOUNDED_COST_SOURCE_IDENTITY")
            .expect("full matrix requires ARCO_BOUNDED_COST_SOURCE_IDENTITY")
    } else {
        "smoke".to_string()
    };
    for (axis_index, axis) in axes.iter().copied().enumerate() {
        let backend = Arc::new(control_cost::CountingBackend::new(1));
        let storage = fixture_storage(backend.clone(), axis_index);
        let scope = fixture_scope(axis_index);
        let fixture_store =
            ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope.clone())
                .expect("synthetic fixture store");
        let sequence = (axis.retained_rows / 2).max(axis.active_records);
        let intents = (1..=axis.active_records)
            .map(|origin| bounded_capacity_v2_intent(scope.clone(), origin, 0))
            .collect::<Result<Vec<_>, _>>()
            .expect("fixture intents");
        fixture_store
            .install_synthetic_genesis(
                &format!("bounded-cost-a{axis_index}"),
                sequence,
                axis.retained_rows,
                V2PredecessorRows::new(axis.retained_rows),
                axis.active_records,
                intents,
            )
            .await
            .expect("fixture genesis");
        let head_path = "control/v1/domains/catalog/head/current.json";
        let genesis = storage
            .get_raw(head_path)
            .await
            .expect("fixture HEAD bytes");
        let construction_head = head_inventory(&storage, head_path).await;
        let construction = backend.take();
        append_jsonl(
            write_jsonl,
            &PhaseSample {
                kind: "phase",
                source_identity: &source_identity,
                phase: "fixture-construction",
                axis,
                cache: "none",
                scenario: "none",
                repetition: 0,
                ordinal: None,
                backend: &construction,
                head: &construction_head,
                reset: None,
            },
        );

        for cache in CACHE_MODES {
            for scenario in SCENARIOS {
                for repetition in 1..=repetitions {
                    let reset = restore_fixture_head(
                        &backend,
                        &storage,
                        axis_index,
                        head_path,
                        genesis.clone(),
                    )
                    .await;
                    let reset_head = head_inventory(&storage, head_path).await;
                    let reset_counts = backend.take();
                    append_jsonl(
                        write_jsonl,
                        &PhaseSample {
                            kind: "phase",
                            source_identity: &source_identity,
                            phase: "fixture-reset",
                            axis,
                            cache: cache.label(),
                            scenario: scenario.label(),
                            repetition,
                            ordinal: None,
                            backend: &reset_counts,
                            head: &reset_head,
                            reset: Some(&reset),
                        },
                    );
                    let authority = ControlCatalogAuthority::new_synthetic_bounded(
                        storage.clone(),
                        scope.clone(),
                        Arc::new(NoopV2Notifier),
                    )
                    .expect("synthetic catalog authority")
                    .with_synthetic_bounded_read_cache(cache_config(cache))
                    .expect("catalog cache config");
                    bootstrap(&authority, scenario)
                        .await
                        .expect("scenario bootstrap");
                    let saved_head = storage
                        .get_raw(head_path)
                        .await
                        .expect("bootstrap HEAD bytes");
                    let bootstrap_head = head_inventory(&storage, head_path).await;
                    let bootstrap_counts = backend.take();
                    append_jsonl(
                        write_jsonl,
                        &PhaseSample {
                            kind: "phase",
                            source_identity: &source_identity,
                            phase: "bootstrap",
                            axis,
                            cache: cache.label(),
                            scenario: scenario.label(),
                            repetition,
                            ordinal: None,
                            backend: &bootstrap_counts,
                            head: &bootstrap_head,
                            reset: None,
                        },
                    );
                    let mut final_head = bootstrap_head;

                    for operation in frozen_operations(scenario, axis_index, cache, repetition)
                        .take(usize::try_from(operations).expect("operation count"))
                    {
                        // A fresh authority/store makes cache state empty after every private fixture reset.
                        let request_store = ControlMvpStateStore::new_synthetic_bounded(
                            storage.clone(),
                            scope.clone(),
                        )
                        .expect("request store");
                        let store = cache_config(cache).map_or_else(
                            || request_store.clone().without_read_cache(),
                            |config| {
                                request_store
                                    .clone()
                                    .with_read_cache_config(config)
                                    .expect("cache config")
                            },
                        );
                        drop(request_store);
                        let authority = ControlCatalogAuthority::new_synthetic_bounded(
                            storage.clone(),
                            scope.clone(),
                            Arc::new(NoopV2Notifier),
                        )
                        .expect("request authority")
                        .with_synthetic_bounded_read_cache(cache_config(cache))
                        .expect("catalog cache config");
                        let prepared = frozen_catalog_command(&operation).map(|command| {
                            authority
                                .prepare_synthetic_bounded_command(
                                    command,
                                    WriteOptions::with_idempotency(
                                        operation.idempotency_key.clone(),
                                    )
                                    .with_request_id(operation.operation_id.clone()),
                                )
                                .expect("freeze catalog command before timing")
                        });
                        if operation.scenario == Scenario::IdempotentReplay {
                            authority
                                .execute_prepared_synthetic_bounded_command(
                                    prepared.clone().expect("idempotent seed command"),
                                )
                                .await
                                .expect("idempotent seed outside timing");
                        }
                        let frozen_operation_id = prepared.as_ref().map_or_else(
                            || operation.operation_id.clone(),
                            |command| command.operation_id().to_string(),
                        );
                        let frozen_digest = prepared.as_ref().map_or_else(
                            || {
                                match scenario {
                                    Scenario::StaleGenerationConflict => "11",
                                    Scenario::ExactTrim => "33",
                                    _ => "00",
                                }
                                .repeat(32)
                            },
                            |command| command.request_digest().to_string(),
                        );
                        let frozen_occurred_at_ms = prepared
                            .as_ref()
                            .map_or(0, PreparedBoundedCatalogMutation::occurred_at_ms);
                        // Warm-up is explicitly outside the request counters.
                        let _ = authority.get_catalog("base").await;
                        let warmup = backend.take();
                        let warmup_head = head_inventory(&storage, head_path).await;
                        let warmup_inventory = backend.take();
                        append_jsonl(
                            write_jsonl,
                            &PhaseSample {
                                kind: "phase",
                                source_identity: &source_identity,
                                phase: "warmup",
                                axis,
                                cache: cache.label(),
                                scenario: scenario.label(),
                                repetition,
                                ordinal: Some(operation.ordinal),
                                backend: &warmup,
                                head: &warmup_head,
                                reset: None,
                            },
                        );
                        let _ = warmup_inventory;
                        let head_before = head_inventory(&storage, head_path).await;
                        let _start_inventory = backend.take();
                        let store_cache_before = cache_statistics(&store);
                        let authority_cache_before =
                            authority.synthetic_bounded_read_cache_statistics();
                        let started = Instant::now();
                        let prepared_for_request = &prepared;
                        let (request_result, allocations, counts) =
                            measure_catalog_request(&backend, async {
                                actual_scenario(
                                    &authority,
                                    &store,
                                    &operation,
                                    prepared_for_request.clone(),
                                )
                                .await
                            })
                            .await;
                        let elapsed_micros = started.elapsed().as_micros();
                        let (verification_result, verification_allocations, verification_counts) =
                            if request_result.is_ok() {
                                let (result, allocations, counts) = measure_catalog_request(
                                    &backend,
                                    store.export_published_logical_v2_current(),
                                )
                                .await;
                                (Some(result), Some(allocations), Some(counts))
                            } else {
                                (None, None, None)
                            };
                        let proof = verification_result
                            .as_ref()
                            .and_then(|result| result.as_ref().ok());
                        let head_after = head_inventory(&storage, head_path).await;
                        let _end_inventory = backend.take();
                        let store_cache_after = cache_statistics(&store);
                        let authority_cache_after =
                            authority.synthetic_bounded_read_cache_statistics();
                        let result_label = match (&request_result, &verification_result) {
                            (Ok(()), Some(Ok(_))) => "ok",
                            (Ok(()), Some(Err(_))) => "verification-error",
                            (Ok(()), None) => "verification-missing",
                            (Err(_), _) => "request-error",
                        };
                        if write_jsonl {
                            let sample = BoundedCostSample {
                                kind: "operation",
                                source_identity: &source_identity,
                                axis,
                                cache: cache.label(),
                                scenario: scenario.label(),
                                repetition,
                                ordinal: operation.ordinal,
                                operation_id: &frozen_operation_id,
                                idempotency_key: &operation.idempotency_key,
                                request_digest: &frozen_digest,
                                occurred_at_ms: frozen_occurred_at_ms,
                                elapsed_micros,
                                request_allocations: &allocations,
                                backend: &counts,
                                verification_allocations: verification_allocations.as_ref(),
                                verification_backend: verification_counts.as_ref(),
                                published_logical: proof,
                                head_before: &head_before,
                                head_after: &head_after,
                                store_cache_before: store_cache_before.as_ref(),
                                store_cache_after: store_cache_after.as_ref(),
                                authority_cache_before: authority_cache_before.as_ref(),
                                authority_cache_after: authority_cache_after.as_ref(),
                                reset_count: reset.reset_count,
                                result: result_label,
                            };
                            append_jsonl(true, &sample);
                        }
                        request_result.expect("bounded scenario request");
                        let proof = verification_result
                            .expect("successful request must export a published logical proof")
                            .expect("published logical proof export");
                        if scenario != Scenario::OrderedOutboxPage {
                            assert_eq!(
                                proof["operation"]["operationId"].as_str(),
                                Some(frozen_operation_id.as_str()),
                                "sample must identify the authenticated published operation"
                            );
                            assert_eq!(
                                proof["operation"]["requestDigest"].as_str(),
                                Some(frozen_digest.as_str()),
                                "sample must record the authenticated published request digest"
                            );
                        }
                        assert_bounded_cost_contract(&counts, &allocations, &proof, scenario);
                        let final_reset = restore_fixture_head(
                            &backend,
                            &storage,
                            axis_index,
                            head_path,
                            saved_head.clone(),
                        )
                        .await;
                        final_head = head_inventory(&storage, head_path).await;
                        let final_reset_counts = backend.take();
                        append_jsonl(
                            write_jsonl,
                            &PhaseSample {
                                kind: "phase",
                                source_identity: &source_identity,
                                phase: "operation-reset",
                                axis,
                                cache: cache.label(),
                                scenario: scenario.label(),
                                repetition,
                                ordinal: Some(operation.ordinal),
                                backend: &final_reset_counts,
                                head: &final_head,
                                reset: Some(&final_reset),
                            },
                        );
                    }
                    append_jsonl(
                        write_jsonl,
                        &SummarySample {
                            kind: "summary",
                            source_identity: &source_identity,
                            axis,
                            cache: cache.label(),
                            scenario: scenario.label(),
                            repetition,
                            operations,
                            final_head: &final_head,
                        },
                    );
                }
            }
        }
        let _prior_inventory_phase = backend.take();
        let prepared_candidates = prepared_candidate_inventory(&storage, &scope).await;
        let artifacts = storage
            .list_meta("")
            .await
            .expect("private artifact inventory");
        let prepared_candidate_objects = artifacts
            .iter()
            .filter(|artifact| artifact.path.to_string().contains("/prepared/"))
            .count();
        let candidate_manifest_objects = artifacts
            .iter()
            .filter(|artifact| artifact.path.to_string().contains("/manifests/"))
            .count();
        let descriptor_objects = artifacts
            .iter()
            .filter(|artifact| artifact.path.to_string().contains("/physical/descriptors/"))
            .count();
        append_jsonl(
            write_jsonl,
            &PrivateArtifactInventory {
                kind: "private-artifact-inventory",
                source_identity: source_identity.clone(),
                axis,
                object_count: artifacts.len(),
                object_bytes: artifacts.iter().map(|artifact| artifact.size).sum(),
                prepared_candidate_objects,
                candidate_manifest_objects,
                descriptor_objects,
                prepared_candidates,
                inventory_backend: backend.take(),
                semantics: "prepared candidates are classified against authenticated current HEAD manifest ancestry; class bytes count only candidate envelopes and manifests, not shared transitive objects or GC eligibility; overall physical totals retain all objects",
            },
        );
    }
}

#[tokio::test]
// The measured request is stack-pinned inside this bounded test harness frame.
#[allow(clippy::large_futures)]
async fn bounded_catalog_cost_smoke_executes_all_frozen_scenarios() {
    run_matrix(
        &[FixtureAxis {
            retained_rows: 4,
            active_records: 4,
        }],
        1,
        1,
        false,
    )
    .await;
}

#[tokio::test]
#[ignore = "explicit frozen authority-8 cost matrix; run only after prerequisite counters are proven"]
// The measured request is stack-pinned inside this bounded test harness frame.
#[allow(clippy::large_futures)]
async fn bounded_catalog_cost_full_matrix_executes_all_frozen_configurations() {
    run_matrix(&FIXTURE_AXES, REPETITIONS, OPERATIONS_PER_SAMPLE, true).await;
}

#[tokio::test]
#[ignore = "explicit real-axis authority-8 preflight; emits JSONL evidence"]
// The measured request is stack-pinned inside this bounded test harness frame.
#[allow(clippy::large_futures)]
async fn bounded_catalog_cost_real_axis_preflight() {
    run_matrix(&FIXTURE_AXES[..1], 1, 1, true).await;
}

#[tokio::test]
#[ignore = "explicit one-request-per-scenario preflight across all six frozen axes"]
#[allow(clippy::large_futures)]
async fn bounded_catalog_cost_all_axes_preflight() {
    run_matrix(&FIXTURE_AXES, 1, 1, true).await;
}
