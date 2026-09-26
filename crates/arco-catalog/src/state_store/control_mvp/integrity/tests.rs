#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
use super::super::*;
use super::*;
use arco_core::{MemoryBackend, storage::WritePrecondition};

fn fixture() -> (ScopedStorage, ControlMvpStateStore) {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let store = ControlMvpStateStore::new(
        storage.clone(),
        StateScope::new("tenant", "workspace", "catalog"),
    )
    .unwrap();
    (storage, store)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn retained_suffix_rewrite_binds_both_cuts_and_traverses_publish_history() {
    let (storage, store) = fixture();
    for _ in 0..16 {
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let render_source = manifest(&store).await;
    let render_digest = store.load_pointer().await.unwrap().manifest_checksum_sha256;
    let state = store.replay_for_successor(&render_source).await.unwrap();
    let rendered = store
        .render_state_snapshots(&state, "retained-suffix-rewrite")
        .unwrap();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.put(b"later", Bytes::from_static(b"preserved"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let source = manifest(&store).await;
    let source_digest = store.load_pointer().await.unwrap().manifest_checksum_sha256;
    let mut candidate = source.clone();
    candidate.manifest_id = "retained-suffix-rewrite".into();
    candidate.base_manifest_id = Some(source.manifest_id.clone());
    candidate.parent_manifest_sha256 = Some(source_digest.clone());
    candidate.layout_generation += 1;
    candidate.base_states = rendered.iter().map(|r| r.reference.clone()).collect();
    candidate.anchor_states.clear();
    candidate.tx_refs = source.tx_refs[render_source.tx_refs.len()..].to_vec();
    candidate.history_anchor = HistoryAnchor {
        sequence: state.logical_sequence,
        root: state.history_root,
    };
    candidate.maintenance_intent = None;
    candidate.equivalence = Some(
        serde_json::from_value(serde_json::json!({
            "encoding_version":2,
            "source_manifest_id":source.manifest_id,
            "source_manifest_sha256":source_digest,
            "source_history_root":source.history_root,
            "source_physical_root":source.physical_root,
            "logical_sequence":source.logical_sequence,
            "state_checksum_sha256":source.state_checksum_sha256,
            "render_source": {
                "manifest_id":render_source.manifest_id,
                "manifest_sha256":render_digest,
                "logical_sequence":render_source.logical_sequence,
                "history_anchor":render_source.history_anchor,
                "history_root":render_source.history_root,
                "physical_root":render_source.physical_root,
                "state_checksum_sha256":render_source.state_checksum_sha256,
                "base_states":render_source.base_states,
                "anchor_states":render_source.anchor_states,
                "tx_refs":render_source.tx_refs
            }
        }))
        .unwrap(),
    );
    candidate.physical_root = candidate.physical_digest().unwrap();
    candidate
        .validate(&store.scope, &candidate.manifest_id)
        .expect("v2 retained suffix is valid");
    store
        .write_rendered_state_snapshots(&rendered)
        .await
        .unwrap();
    assert_eq!(
        store.replay_for_successor(&candidate).await.unwrap(),
        store.replay_for_successor(&source).await.unwrap()
    );
    let bytes = encode_envelope("control-mvp-manifest", &candidate).unwrap();
    let digest = sha256_hex(&bytes);
    storage
        .put_raw(
            &store.paths.manifest_object(&candidate.manifest_id),
            bytes,
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    assert!(
        store
            .resolve_ancestor(&candidate.manifest_id, &digest, |m, _| {
                (m.manifest_id == render_source.manifest_id).then_some(())
            })
            .await
            .unwrap()
            .is_some()
    );
    let mut invalid = candidate.clone();
    invalid.equivalence.as_mut().unwrap().encoding_version = 1;
    assert!(
        invalid
            .validate(&store.scope, &invalid.manifest_id)
            .is_err()
    );
    for (field, value) in [
        ("history_root", "invalid-digest".to_string()),
        ("physical_root", "invalid-digest".to_string()),
        ("manifest_sha256", "invalid-digest".to_string()),
    ] {
        let mut forged = serde_json::to_value(&candidate).unwrap();
        forged["equivalence"]["render_source"][field] = serde_json::json!(value);
        let result = serde_json::from_value::<ControlMvpManifest>(forged)
            .map_err(|e| invariant_violation(e.to_string()))
            .and_then(|m| m.validate(&store.scope, &m.manifest_id));
        assert!(result.is_err(), "forged render source {field}");
    }
    // A retained suffix must not mask a corrupted render cut by overwriting it.
    let mut forged_base = store.replay_for_successor(&render_source).await.unwrap();
    forged_base.kv.insert(
        b"later".to_vec(),
        StoredValue {
            bytes: Bytes::from_static(b"masked corruption"),
            generation: 1,
            tombstone: false,
            expires_at_ms: None,
        },
    );
    let forged_rows = store
        .render_state_snapshots(&forged_base, "masked-render-cut")
        .unwrap();
    store
        .write_rendered_state_snapshots(&forged_rows)
        .await
        .unwrap();
    let mut forged = candidate;
    forged.base_states = forged_rows.iter().map(|r| r.reference.clone()).collect();
    forged.physical_root = forged.physical_digest().unwrap();
    forged.validate(&store.scope, &forged.manifest_id).unwrap();
    assert!(
        store.replay_for_successor(&forged).await.is_err(),
        "the valid later write must not hide a different materialized render cut"
    );
}

async fn manifest(store: &ControlMvpStateStore) -> ControlMvpManifest {
    store
        .load_manifest_for_pointer(&store.load_pointer().await.unwrap())
        .await
        .unwrap()
}

/// Canonical-vector inputs shared by the regression test and the generator so
/// the two cannot drift apart.
fn vector_tx(scope: &StateScope) -> ControlMvpTxObject {
    ControlMvpTxObject {
        history: HistoryLink::default(),
        reclamation_generation: 0,
        implementation: IMPLEMENTATION.to_string(),
        scope: scope.clone(),
        tx_id: "physical-tx".to_string(),
        base_manifest_id: None,
        sequence: 1,
        writer_epoch: 0,
        committed_at_ms: 1,
        request_id: None,
        l0_segment: unwritten_l0_segment_ref("physical-tx", 1),
        writes: Vec::new(),
        outbox: Vec::new(),
        outbox_trim: Vec::new(),
    }
}

fn populate_vector_mutation(tx: &mut ControlMvpTxObject) {
    tx.request_id = Some("request".to_string());
    tx.writes = vec![
        ControlMvpWriteEntry {
            key: vec![0, 255],
            generation: 1,
            value: Some(Vec::new()),
            expires_at_ms: None,
        },
        ControlMvpWriteEntry {
            key: Vec::new(),
            generation: 1,
            value: None,
            expires_at_ms: None,
        },
    ];
    tx.outbox = vec![ControlMvpOutboxEntry {
        record_id: "event".to_string(),
        payload: vec![255],
    }];
}

/// Segment format 2: the same mutation with one live write carrying an expiry
/// hint. Pins the `optional i64` encoding that follows the optional value.
fn populate_expiring_vector_mutation(tx: &mut ControlMvpTxObject) {
    populate_vector_mutation(tx);
    tx.writes[0].expires_at_ms = Some(1_893_456_000_000);
}

async fn nonempty_vector_layout(
    store: &ControlMvpStateStore,
) -> (ControlMvpManifest, ControlMvpStateRef) {
    store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let mut layout = manifest(store).await;
    let state = ControlMvpStateRef {
        state_id: "state-vector".into(),
        logical_sequence: 9,
        segment_size_bytes: 12345,
        index_size_bytes: 678,
        min_key_hex: Some("00ff".into()),
        max_key_hex: Some("ff00".into()),
        checksum_sha256: "11".repeat(32),
        index_checksum_sha256: "22".repeat(32),
    };
    layout.base_states = vec![state.clone()];
    layout.anchor_states.clear();
    layout.tx_refs[0].tx_id = "tx-vector".into();
    layout.tx_refs[0].sequence = 10;
    layout.tx_refs[0].size_bytes = 901;
    layout.tx_refs[0].checksum_sha256 = "33".repeat(32);
    (layout, state)
}

/// Format-9 canonical vectors. These were generated by this implementation
/// (`generate_format_canonical_vectors`), so they pin the preimages and digests
/// for regression only; independent regeneration, as the 2026-09-06 format-7
/// vectors had, is a follow-up.
const CANONICAL_VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/reports/2026-09-26-format9-canonical-vectors.json"
));

#[test]
fn canonical_hashes_match_recorded_binary_vectors() {
    let vectors: serde_json::Value = serde_json::from_str(CANONICAL_VECTORS).unwrap();
    assert_eq!(
        vectors["authority_format"].as_u64(),
        Some(u64::from(CONTROL_MVP_FORMAT_VERSION))
    );
    let expected = |name: &str| {
        vectors["vectors"][name]["sha256"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let scope = StateScope::new("tenant", "workspace", "catalog");
    assert_eq!(genesis(&scope).unwrap().root, expected("genesis"));
    let mut tx = vector_tx(&scope);
    assert_eq!(mutation_digest(&tx).unwrap(), expected("empty_mutation"));
    assert_eq!(
        HistoryLink::new(&tx, &genesis(&scope).unwrap().root)
            .unwrap()
            .resulting_root,
        expected("empty_commit_history")
    );
    populate_vector_mutation(&mut tx);
    assert_eq!(mutation_digest(&tx).unwrap(), expected("one_mutation"));
    assert_eq!(
        HistoryLink::new(&tx, &genesis(&scope).unwrap().root)
            .unwrap()
            .resulting_root,
        expected("one_commit_history")
    );
    tx.tx_id = "other-physical-tx".to_string();
    tx.writer_epoch = 12;
    tx.reclamation_generation = 32;
    tx.committed_at_ms = 1_893_456_000_000;
    assert_eq!(mutation_digest(&tx).unwrap(), expected("one_mutation"));
    let mut expiring = vector_tx(&scope);
    populate_expiring_vector_mutation(&mut expiring);
    assert_eq!(
        mutation_digest(&expiring).unwrap(),
        expected("expiring_mutation")
    );
    assert_ne!(expected("expiring_mutation"), expected("one_mutation"));
    assert_eq!(
        HistoryLink::new(&expiring, &genesis(&scope).unwrap().root)
            .unwrap()
            .resulting_root,
        expected("expiring_commit_history")
    );
    assert_eq!(
        physical_digest(&scope, &[], &[], &[], &[]).unwrap(),
        expected("empty_manifest_layout")
    );
    assert_eq!(
        checkpoint_physical_digest(&scope, &[]).unwrap(),
        expected("empty_checkpoint_layout")
    );
    let other = StateScope::new("other", "workspace", "catalog");
    assert_ne!(genesis(&scope).unwrap(), genesis(&other).unwrap());
}

/// Writes the canonical-hash vectors for the current authority format from
/// this implementation. Run once per format bump:
/// `cargo test -p arco-catalog --lib generate_format_canonical_vectors -- --ignored`
#[tokio::test]
#[ignore = "vector generator: run manually after an authority format bump"]
async fn generate_format_canonical_vectors() {
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let genesis_root = genesis(&scope).unwrap().root;
    let mut tx = vector_tx(&scope);
    let empty_mutation = mutation_digest(&tx).unwrap();
    let mut entries = vec![
        ("genesis", genesis_canonical(&scope).unwrap()),
        ("empty_mutation", mutation_canonical(&tx).unwrap()),
        (
            "empty_commit_history",
            history_step_canonical(&scope, &genesis_root, 1, &empty_mutation).unwrap(),
        ),
    ];
    populate_vector_mutation(&mut tx);
    let one_mutation = mutation_digest(&tx).unwrap();
    entries.push(("one_mutation", mutation_canonical(&tx).unwrap()));
    entries.push((
        "one_commit_history",
        history_step_canonical(&scope, &genesis_root, 1, &one_mutation).unwrap(),
    ));
    let mut expiring = vector_tx(&scope);
    populate_expiring_vector_mutation(&mut expiring);
    let expiring_mutation = mutation_digest(&expiring).unwrap();
    entries.push(("expiring_mutation", mutation_canonical(&expiring).unwrap()));
    entries.push((
        "expiring_commit_history",
        history_step_canonical(&scope, &genesis_root, 1, &expiring_mutation).unwrap(),
    ));
    entries.push((
        "empty_manifest_layout",
        physical_canonical(&scope, &[], &[], &[], &[]).unwrap(),
    ));
    entries.push((
        "empty_checkpoint_layout",
        checkpoint_physical_canonical(&scope, &[]).unwrap(),
    ));
    let (_, store) = fixture();
    let (layout, state) = nonempty_vector_layout(&store).await;
    entries.push((
        "nonempty_manifest_layout",
        physical_canonical(
            &layout.scope,
            &layout.base_states,
            &layout.anchor_states,
            &layout.tx_refs,
            &[],
        )
        .unwrap(),
    ));
    entries.push((
        "nonempty_checkpoint_layout",
        checkpoint_physical_canonical(&layout.scope, &[state]).unwrap(),
    ));

    let mut out = format!(
        "{{\n  \"authority_format\": {CONTROL_MVP_FORMAT_VERSION},\n  \"encoding_version\": 1,\n  \
         \"scope\": [\n    \"tenant\",\n    \"workspace\",\n    \"catalog\"\n  ],\n  \
         \"producer\": \"Rust implementation: arco-catalog state_store::control_mvp::integrity \
         (generate_format_canonical_vectors)\",\n  \
         \"note\": \"Generated by the implementation under test, so these vectors pin the \
         format-{CONTROL_MVP_FORMAT_VERSION} preimages and digests for regression only. \
         Independent regeneration, as the 2026-09-06 format-7 vectors had, is a follow-up.\",\n  \
         \"vectors\": {{\n"
    );
    let last = entries.len() - 1;
    for (index, (name, canonical)) in entries.iter().enumerate() {
        use std::fmt::Write as _;
        let preimage = canonical.preimage();
        write!(
            out,
            "    \"{name}\": {{\n      \"input_hex\": \"{}\",\n      \"sha256\": \"{}\"\n    }}{}\n",
            hex::encode(preimage),
            sha256_hex(preimage),
            if index == last { "" } else { "," }
        )
        .unwrap();
    }
    out.push_str("  }\n}\n");
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/reports/2026-09-26-format9-canonical-vectors.json"
        ),
        out,
    )
    .unwrap();
}

#[test]
fn mutation_preimage_encodes_the_expiry_hint_as_an_optional_i64_after_the_value() {
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let mut plain = vector_tx(&scope);
    populate_vector_mutation(&mut plain);
    let mut expiring = plain.clone();
    expiring.writes[0].expires_at_ms = Some(0x0102_0304_0506_0708);
    let plain_preimage = mutation_canonical(&plain).unwrap().preimage().to_vec();
    let expiring_preimage = mutation_canonical(&expiring).unwrap().preimage().to_vec();
    // Writes sort by key, so `vec![]` precedes `vec![0, 255]`: the expiring
    // write is the second one. Its absent hint is the single `0` byte that
    // ends the write; present, it is `1` followed by the big-endian i64.
    let value_absent = [0_u8];
    let (before, after) = split_at_second_write_hint(&plain_preimage);
    assert_eq!(after, value_absent, "absent hint is one zero byte");
    let mut expected = before.to_vec();
    expected.push(1);
    expected.extend_from_slice(&0x0102_0304_0506_0708_i64.to_be_bytes());
    expected.extend_from_slice(&plain_preimage[before.len() + 1..]);
    assert_eq!(expiring_preimage, expected);
    assert_ne!(
        mutation_digest(&plain).unwrap(),
        mutation_digest(&expiring).unwrap()
    );
}

/// Splits a two-write mutation preimage right before the second write's
/// expiry-hint discriminant, returning the trailing hint region of exactly
/// the bytes that follow that write's value up to the outbox count.
fn split_at_second_write_hint(preimage: &[u8]) -> (&[u8], &[u8]) {
    // Layout after the second write's value: hint bytes, then the u64
    // outbox count (1), then the outbox entry. Locate the outbox count.
    let outbox_count = 1_u64.to_be_bytes();
    let record_id = b"event";
    let tail = [
        &outbox_count[..],
        &(record_id.len() as u64).to_be_bytes()[..],
        record_id,
    ]
    .concat();
    let position = preimage
        .windows(tail.len())
        .rposition(|window| window == tail.as_slice())
        .expect("outbox section");
    let hint_start = position - 1;
    (&preimage[..hint_start], &preimage[hint_start..position])
}

#[test]
fn workspace_and_metastore_digests_diverge_for_equal_textual_ids() {
    let wks = StateScope::new("acme", "prod", "catalog");
    let mts = StateScope::metastore("acme", "prod", "catalog");

    assert_ne!(
        genesis(&wks).unwrap(),
        genesis(&mts).unwrap(),
        "equal textual ids must not share a genesis root"
    );
    assert_ne!(
        checkpoint_physical_digest(&wks, &[]).unwrap(),
        checkpoint_physical_digest(&mts, &[]).unwrap(),
        "equal textual ids must not share a checkpoint layout digest"
    );

    let tx_for = |scope: &StateScope| ControlMvpTxObject {
        history: HistoryLink::default(),
        reclamation_generation: 0,
        implementation: IMPLEMENTATION.to_string(),
        scope: scope.clone(),
        tx_id: "physical-tx".to_string(),
        base_manifest_id: None,
        sequence: 1,
        writer_epoch: 0,
        committed_at_ms: 1,
        request_id: None,
        l0_segment: unwritten_l0_segment_ref("physical-tx", 1),
        writes: Vec::new(),
        outbox: Vec::new(),
        outbox_trim: Vec::new(),
    };
    assert_ne!(
        mutation_digest(&tx_for(&wks)).unwrap(),
        mutation_digest(&tx_for(&mts)).unwrap(),
        "equal textual ids must not share a mutation digest"
    );
}

#[test]
fn control_mvp_envelope_accepts_legacy_workspace_scope() {
    let value = serde_json::json!({
        "reclamation_generation": 0,
        "format_version": CONTROL_MVP_FORMAT_VERSION,
        "implementation": IMPLEMENTATION,
        "scope": { "tenant_id": "acme", "workspace_id": "prod", "domain": "catalog" },
        "manifest_id": "manifest-1",
        "logical_sequence": 1,
        "manifest_checksum_sha256": "0".repeat(64),
        "writer_epoch": 0
    });

    let pointer: ControlMvpPointer = serde_json::from_value(value).unwrap();
    assert!(matches!(
        pointer.scope.root(),
        AuthorityRoot::Workspace { .. }
    ));
    assert_eq!(pointer.scope.workspace_id(), Some("prod"));
}

#[test]
fn checksummed_legacy_workspace_envelopes_remain_readable() {
    let manifest: ControlMvpManifest = decode_envelope_limited(
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/control_mvp_authority_v9/legacy_workspace_manifest.json"
        )),
        "control-mvp-manifest",
        MAX_CONTROL_JSON_BYTES,
        "legacy manifest fixture",
    )
    .expect("legacy workspace manifest");
    let transaction: ControlMvpTxObject = decode_envelope_limited(
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/control_mvp_authority_v9/legacy_workspace_transaction.json"
        )),
        "control-mvp-tx",
        MAX_TRANSACTION_JSON_BYTES,
        "legacy transaction fixture",
    )
    .expect("legacy workspace transaction");
    let checkpoint: ControlMvpCheckpoint = decode_envelope_limited(
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/control_mvp_authority_v9/legacy_workspace_checkpoint.json"
        )),
        "control-mvp-checkpoint",
        MAX_CONTROL_JSON_BYTES,
        "legacy checkpoint fixture",
    )
    .expect("legacy workspace checkpoint");

    for scope in [&manifest.scope, &transaction.scope, &checkpoint.scope] {
        assert!(matches!(scope.root(), AuthorityRoot::Workspace { .. }));
        assert_eq!(scope.workspace_id(), Some("workspace"));
    }
    assert!(manifest.committed_at_ms > 0);
    assert!(transaction.committed_at_ms > 0);
}

/// Rewrites one checksummed envelope so its payload carries the legacy
/// (unversioned) workspace `StateScope` shape, re-deriving the payload checksum
/// over the exact bytes written.
#[cfg(feature = "test-utils")]
fn legacy_scope_envelope(bytes: &[u8], scope: &StateScope) -> Vec<u8> {
    let envelope: ChecksumEnvelope<&RawValue> = serde_json::from_slice(bytes).unwrap();
    let versioned = serde_json::to_string(scope).unwrap();
    let legacy = format!(
        "{{\"tenant_id\":{},\"workspace_id\":{},\"domain\":{}}}",
        serde_json::to_string(scope.tenant_id()).unwrap(),
        serde_json::to_string(scope.workspace_id().unwrap()).unwrap(),
        serde_json::to_string(scope.domain()).unwrap(),
    );
    let payload = envelope.payload.get();
    assert_eq!(payload.matches(&versioned).count(), 1);
    let payload = payload.replacen(&versioned, &legacy, 1);
    format!(
        "{{\"format_version\":{},\"artifact_type\":{},\"checksum_sha256\":\"{}\",\"payload\":{payload}}}",
        envelope.format_version,
        serde_json::to_string(&envelope.artifact_type).unwrap(),
        sha256_hex(payload.as_bytes()),
    )
    .into_bytes()
}

/// Regenerates the legacy-scope envelope fixtures for the current authority
/// format under the deterministic fixture clock. Run once per format bump:
/// `cargo test -p arco-catalog --lib generate_control_mvp_authority_fixtures -- --ignored`
#[cfg(feature = "test-utils")]
#[tokio::test]
#[ignore = "fixture generator: run manually after an authority format bump"]
async fn generate_control_mvp_authority_fixtures() {
    let _inputs = arco_core::test_inputs::FixedInputs::scoped();
    let (storage, store) = fixture();
    store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    let pointer = store.load_pointer().await.unwrap();
    let manifest = store.load_manifest_for_pointer(&pointer).await.unwrap();
    let directory = format!(
        "{}/tests/fixtures/control_mvp_authority_v{CONTROL_MVP_FORMAT_VERSION}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::create_dir_all(&directory).unwrap();
    for (name, path) in [
        (
            "legacy_workspace_manifest",
            store.paths.manifest_object(&manifest.manifest_id),
        ),
        (
            "legacy_workspace_transaction",
            store.paths.tx_object(&manifest.tx_refs[0].tx_id),
        ),
        (
            "legacy_workspace_checkpoint",
            store.paths.checkpoint_object(checkpoint.checkpoint_id()),
        ),
    ] {
        let bytes = storage.get_raw(&path).await.unwrap();
        std::fs::write(
            format!("{directory}/{name}.json"),
            legacy_scope_envelope(&bytes, &store.scope),
        )
        .unwrap();
    }
}

#[test]
fn control_mvp_envelope_scope_is_versioned_and_round_trips() {
    let scope = StateScope::new("acme", "prod", "catalog");
    let pointer = ControlMvpPointer {
        reclamation_generation: 0,
        format_version: CONTROL_MVP_FORMAT_VERSION,
        implementation: IMPLEMENTATION.to_string(),
        scope: scope.clone(),
        manifest_id: "manifest-1".to_string(),
        logical_sequence: 1,
        manifest_checksum_sha256: "0".repeat(64),
        writer_epoch: 0,
        claim_id: None,
    };

    let value = serde_json::to_value(&pointer).unwrap();
    assert_eq!(value["scope"]["scope_version"].as_u64(), Some(2));
    assert_eq!(value["scope"]["root_kind"].as_str(), Some("workspace"));
    assert_eq!(value["scope"]["workspace_id"].as_str(), Some("prod"));
    assert!(value["scope"].get("metastore_id").is_none());

    let decoded: ControlMvpPointer = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.scope, scope);
}

#[test]
fn control_mvp_envelope_rejects_cross_root_scope() {
    let wks = StateScope::new("acme", "prod", "catalog");
    let mts = StateScope::metastore("acme", "prod", "catalog");

    let pointer = ControlMvpPointer {
        reclamation_generation: 0,
        format_version: CONTROL_MVP_FORMAT_VERSION,
        implementation: IMPLEMENTATION.to_string(),
        scope: wks,
        manifest_id: "manifest-1".to_string(),
        logical_sequence: 1,
        manifest_checksum_sha256: "0".repeat(64),
        writer_epoch: 0,
        claim_id: None,
    };
    assert!(
        pointer.validate(&mts).is_err(),
        "a workspace envelope must not validate against a metastore root"
    );
}

#[tokio::test]
async fn nonempty_physical_layouts_match_recorded_vectors() {
    let (_, store) = fixture();
    let (layout, state) = nonempty_vector_layout(&store).await;
    let vectors: serde_json::Value = serde_json::from_str(CANONICAL_VECTORS).unwrap();
    assert_eq!(
        layout.physical_digest().unwrap(),
        vectors["vectors"]["nonempty_manifest_layout"]["sha256"]
    );
    assert_eq!(
        checkpoint_physical_digest(&layout.scope, &[state]).unwrap(),
        vectors["vectors"]["nonempty_checkpoint_layout"]["sha256"]
    );
}

#[tokio::test]
async fn local_manifest_validation_rejects_duplicate_transaction_ids() {
    let (_, store) = fixture();
    for _ in 0..2 {
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let original = manifest(&store).await;
    for id in [
        original.tx_refs[0].tx_id.clone(),
        String::new(),
        "../outside".to_string(),
        "bad%id".to_string(),
    ] {
        let mut forged = original.clone();
        forged.tx_refs[1].tx_id = id;
        forged.physical_root = forged.physical_digest().unwrap();
        assert!(forged.validate(&store.scope, &forged.manifest_id).is_err());
    }
}

#[tokio::test]
async fn local_manifest_validation_rejects_invalid_owning_metadata() {
    let (storage, store) = fixture();
    for _ in 0..16 {
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let original = manifest(&store).await;
    let mut bad_checksum = original.clone();
    bad_checksum.state_checksum_sha256 = "not-a-digest".to_string();
    assert!(
        bad_checksum
            .validate(&store.scope, &bad_checksum.manifest_id)
            .is_err()
    );
    let mut bad_parent = original;
    bad_parent.base_manifest_id = Some("../parent".to_string());
    assert!(
        bad_parent
            .validate(&store.scope, &bad_parent.manifest_id)
            .is_err()
    );
    ControlMvpMaintenanceWorker::new(storage, store.scope.clone())
        .unwrap()
        .test_consolidate_pending(DurableAuthorityBinding::new([17; 32]))
        .await
        .unwrap()
        .unwrap();
    let original = manifest(&store).await;
    for id in ["", "../other", "bad\nidentity", "bad%id"] {
        let mut forged = original.clone();
        forged.base_states[0].state_id = id.to_string();
        let result = forged.physical_digest().and_then(|digest| {
            forged.physical_root = digest;
            forged.validate(&store.scope, &forged.manifest_id)
        });
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn pointer_and_checkpoint_reject_unfollowable_immutable_ids() {
    let (_, store) = fixture();
    store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let mut pointer = store.load_pointer().await.unwrap();
    pointer.manifest_id = "bad%id".to_string();
    assert!(pointer.validate(&store.scope).is_err());
    let token = store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    let mut checkpoint = store.load_checkpoint(&token).await.unwrap();
    checkpoint.checkpoint_id = "bad%id".to_string();
    assert!(checkpoint.validate(&store.scope, "bad%id").is_err());
    checkpoint.checkpoint_id = token.checkpoint_id().to_string();
    checkpoint.manifest_id = "bad%id".to_string();
    assert!(
        checkpoint
            .validate(&store.scope, token.checkpoint_id())
            .is_err()
    );
}

#[tokio::test]
async fn nonempty_restore_behind_source_continues_destination_history() {
    let (storage, store) = fixture();
    let store = store.with_checkpoint_interval(NonZeroU64::new(1).unwrap());
    let mut first = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    first
        .put(b"key", Bytes::from_static(b"destination"))
        .await
        .unwrap();
    first.commit().await.unwrap();
    let destination = manifest(&store).await;
    let destination_head = storage
        .get_raw(&store.paths.current_pointer())
        .await
        .unwrap();
    for _ in 0..3 {
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        tx.put(b"key", Bytes::from_static(b"source")).await.unwrap();
        tx.commit().await.unwrap();
    }
    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    let source = store
        .persist_checkpoint_reference(&checkpoint, Utc::now() + ChronoDuration::days(1))
        .await
        .unwrap();
    storage
        .put_raw(
            &store.paths.current_pointer(),
            destination_head,
            WritePrecondition::None,
        )
        .await
        .unwrap();
    let participant = ControlMvpRestoreParticipant::new(store.clone());
    let identity =
        RestoreAttemptIdentity::new("rst_00000000000000000000000001", 1, "catalog").unwrap();
    let plan = participant
        .plan_restore(&source, &identity, Utc::now())
        .await
        .expect("valid restore from a later retained source");
    participant.apply_restore(&plan, Utc::now()).await.unwrap();
    let after = manifest(&store).await;
    assert_eq!(after.logical_sequence, destination.logical_sequence + 1);
    assert_eq!(
        after.tx_refs.last().unwrap().history.preceding_root,
        destination.history_root
    );
    assert_eq!(
        store.get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"source"))
    );
}

#[tokio::test]
async fn corrupt_redundant_anchor_cannot_be_promoted_by_a_successor() {
    for begin_before_corruption in [false, true] {
        let (storage, store) = fixture();
        let store = store.with_checkpoint_interval(NonZeroU64::new(1).unwrap());
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        tx.put(b"key", Bytes::from_static(b"value")).await.unwrap();
        let token = tx.commit().await.unwrap().into_state_token();
        let parent = manifest(&store).await;
        let before = storage
            .get_raw(&store.paths.current_pointer())
            .await
            .unwrap();
        let pending = if begin_before_corruption {
            Some(
                store
                    .begin_control_txn(TxnOptions::default())
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        storage
            .delete(&store.paths.state_object(&parent.anchor_states[0].state_id))
            .await
            .unwrap();
        if let Some(tx) = pending {
            assert!(tx.commit().await.is_err());
        } else {
            assert!(
                store
                    .begin_control_txn(TxnOptions::default())
                    .await
                    .unwrap()
                    .commit()
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            storage
                .get_raw(&store.paths.current_pointer())
                .await
                .unwrap(),
            before
        );
        assert_eq!(
            store
                .read_at(token)
                .await
                .unwrap()
                .get(b"key")
                .await
                .unwrap(),
            Some(Bytes::from_static(b"value"))
        );
    }
}

#[tokio::test]
async fn maintenance_rejects_corrupt_redundant_inline_anchor() {
    let (storage, store) = fixture();
    let store = store.with_checkpoint_interval(NonZeroU64::new(16).unwrap());
    for _ in 0..16 {
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let source = manifest(&store).await;
    assert!(source.maintenance_intent.is_some());
    assert!(!source.anchor_states.is_empty());
    let before = storage
        .get_raw(&store.paths.current_pointer())
        .await
        .unwrap();
    storage
        .delete(&store.paths.state_object(&source.anchor_states[0].state_id))
        .await
        .unwrap();
    let worker = ControlMvpMaintenanceWorker::new(storage.clone(), store.scope.clone()).unwrap();
    assert!(
        worker
            .test_consolidate_pending(DurableAuthorityBinding::new([17; 32]))
            .await
            .is_err(),
        "maintenance must verify every owning source state"
    );
    assert_eq!(
        storage
            .get_raw(&store.paths.current_pointer())
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn convergent_state_preserves_distinct_history_and_equivalent_layouts_preserve_history() {
    let mut converged = Vec::new();
    for first in [b"a", b"b"] {
        let (_, store) = fixture();
        for value in [first.as_slice(), b"final"] {
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            tx.put(b"key", Bytes::copy_from_slice(value)).await.unwrap();
            tx.commit().await.unwrap();
        }
        converged.push(manifest(&store).await);
    }
    assert_eq!(
        converged[0].state_checksum_sha256,
        converged[1].state_checksum_sha256
    );
    assert_ne!(converged[0].history_root, converged[1].history_root);
    let mut layouts = Vec::new();
    for (rows, target) in [(512, 32 * 1024), (64, 256 * 1024)] {
        let (storage, store) = fixture();
        for sequence in 0..16 {
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            if sequence == 0 {
                for key in 0_u64..512 {
                    tx.put(&key.to_be_bytes(), Bytes::from(vec![42; 1024]))
                        .await
                        .unwrap();
                }
            }
            tx.commit().await.unwrap();
        }
        let before = manifest(&store).await;
        let mut empty_layout = before.clone();
        empty_layout.base_states.clear();
        empty_layout.anchor_states.clear();
        empty_layout.tx_refs.clear();
        let vectors: serde_json::Value = serde_json::from_str(CANONICAL_VECTORS).unwrap();
        assert_eq!(
            empty_layout.physical_digest().unwrap(),
            vectors["vectors"]["empty_manifest_layout"]["sha256"]
        );
        let worker = ControlMvpMaintenanceWorker::new(storage, store.scope.clone())
            .unwrap()
            .with_test_segment_sizing(rows, target)
            .unwrap();
        worker
            .test_consolidate_pending(DurableAuthorityBinding::new([17; 32]))
            .await
            .unwrap()
            .unwrap();
        let after = manifest(&store).await;
        assert_eq!(before.history_root, after.history_root);
        assert_eq!(before.state_checksum_sha256, after.state_checksum_sha256);
        assert_ne!(before.physical_root, after.physical_root);
        assert_eq!(before.history_root, after.history_anchor.root);
        let checkpoint = store
            .checkpoint(CheckpointOptions::default())
            .await
            .unwrap();
        let checkpoint = store.load_checkpoint(&checkpoint).await.unwrap();
        checkpoint.validate_source(&after).unwrap();
        assert_ne!(
            checkpoint.validation.checkpoint_physical_root,
            after.physical_root
        );
        let claimed = store.clone().claim_writer_authority().await.unwrap();
        let fenced = manifest(&claimed).await;
        assert_eq!(fenced.history_root, after.history_root);
        assert_eq!(fenced.physical_root, after.physical_root);
        layouts.push(after);
    }
    assert_eq!(layouts[0].history_root, layouts[1].history_root);
    assert_eq!(
        layouts[0].state_checksum_sha256,
        layouts[1].state_checksum_sha256
    );
    assert_ne!(layouts[0].physical_root, layouts[1].physical_root);
}

#[tokio::test]
async fn forged_equivalence_and_checkpoint_layout_evidence_is_rejected() {
    let (storage, store) = fixture();
    for sequence in 0..16 {
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        if sequence == 0 {
            tx.put(b"key", Bytes::from_static(b"value")).await.unwrap();
        }
        tx.commit().await.unwrap();
    }
    let source = manifest(&store).await;
    ControlMvpMaintenanceWorker::new(storage.clone(), store.scope.clone())
        .unwrap()
        .test_consolidate_pending(DurableAuthorityBinding::new([17; 32]))
        .await
        .unwrap()
        .unwrap();
    let mut after = manifest(&store).await;
    let checkpoint_token = store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    let checkpoint = store.load_checkpoint(&checkpoint_token).await.unwrap();
    let mut wrong_layout = checkpoint.clone();
    wrong_layout
        .validation
        .checkpoint_physical_root
        .clone_from(&after.physical_root);
    assert!(
        wrong_layout
            .validate(&store.scope, &wrong_layout.checkpoint_id)
            .is_err()
    );
    for which in 0..3 {
        let mut forged = checkpoint.clone();
        match which {
            0 => forged.validation.source_history_root = "0".repeat(64),
            1 => forged.validation.source_physical_root = "0".repeat(64),
            _ => forged.validation.state_checksum_sha256 = "0".repeat(64),
        }
        assert!(forged.validate_source(&after).is_err());
    }
    let mut invalid = after.clone();
    invalid.equivalence.as_mut().unwrap().source_history_root = "0".repeat(64);
    assert!(
        invalid
            .validate(&store.scope, &invalid.manifest_id)
            .is_err()
    );
    after.equivalence.as_mut().unwrap().source_physical_root = "0".repeat(64);
    let bytes = encode_envelope("control-mvp-manifest", &after).unwrap();
    let digest = sha256_hex(&bytes);
    storage
        .put_raw(
            &store.paths.manifest_object(&after.manifest_id),
            bytes,
            WritePrecondition::None,
        )
        .await
        .unwrap();
    let result = store
        .resolve_ancestor(&after.manifest_id, &digest, |candidate, _| {
            (candidate.manifest_id == source.manifest_id).then_some(())
        })
        .await;
    assert!(matches!(
        result,
        Err(CatalogError::InvariantViolation { .. })
    ));
    assert!(
        store.read_checkpoint(checkpoint_token).await.is_err(),
        "checkpoint retains its exact source witness"
    );
}

#[tokio::test]
async fn rendered_outbox_order_and_incarnations_are_part_of_equivalence() {
    let (_, store) = fixture();
    let mut expected = ReplayState::empty(&store.scope).unwrap();
    expected.logical_sequence = 4;
    expected.outbox = vec![
        ControlMvpOutboxEntry {
            record_id: "b".to_string(),
            payload: vec![1],
        }
        .to_record_with_sequence(2),
        ControlMvpOutboxEntry {
            record_id: "a".to_string(),
            payload: vec![2],
        }
        .to_record_with_sequence(3),
    ];
    let rendered = store
        .render_state_snapshots(&expected, "outbox-equivalence")
        .unwrap();
    let mut reordered = expected.clone();
    reordered.outbox.swap(0, 1);
    assert!(
        store
            .validate_rendered_state(&reordered, &rendered)
            .is_err()
    );
    let mut incarnation = expected;
    incarnation.outbox[0].origin_sequence = Some(1);
    assert!(
        store
            .validate_rendered_state(&incarnation, &rendered)
            .is_err()
    );
}

#[tokio::test]
async fn coherent_data_and_semantic_checksum_replacement_cannot_rewrite_history() {
    let (storage, store) = fixture();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.put(b"key", Bytes::from_static(b"original"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut selected = manifest(&store).await;
    let mut tx = store.load_tx(&selected.tx_refs[0]).await.unwrap();
    tx.writes[0].value = Some(b"replaced".to_vec());
    let (bytes, index, reference) = encode_segment(
        &tx.tx_id,
        ControlMvpSegmentLevel::L0,
        1,
        &store.scope,
        &segment_rows_for_tx(&tx),
        PRODUCTION_SEGMENT_LIMITS,
    )
    .unwrap();
    tx.l0_segment = reference;
    storage
        .put_raw(
            &store.paths.l0_segment_object(&tx.tx_id),
            bytes,
            WritePrecondition::None,
        )
        .await
        .unwrap();
    storage
        .put_raw(
            &store.paths.segment_index(&tx.tx_id),
            index,
            WritePrecondition::None,
        )
        .await
        .unwrap();
    let bytes = encode_envelope("control-mvp-tx", &tx).unwrap();
    selected.tx_refs[0].size_bytes = bytes.len() as u64;
    selected.tx_refs[0].checksum_sha256 = sha256_hex(&bytes);
    storage
        .put_raw(
            &store.paths.tx_object(&tx.tx_id),
            bytes,
            WritePrecondition::None,
        )
        .await
        .unwrap();
    let mut expected = ReplayState::empty(&store.scope).unwrap();
    let mut altered = tx.clone();
    altered.history = HistoryLink::new(&altered, &expected.history_root).unwrap();
    expected.apply_tx(&altered).unwrap();
    selected.state_checksum_sha256 = expected.checksum().unwrap();
    selected.physical_root = selected.physical_digest().unwrap();
    selected
        .validate(&store.scope, &selected.manifest_id)
        .unwrap();
    assert!(store.replay_manifest(&selected).await.is_err());
}

#[tokio::test]
async fn rendered_rewrites_reject_lost_altered_and_duplicated_rows() {
    let (_, store) = fixture();
    let mut expected = ReplayState::empty(&store.scope).unwrap();
    expected.logical_sequence = 2;
    for key in [b"a", b"b"] {
        expected.kv.insert(
            key.to_vec(),
            StoredValue {
                bytes: Bytes::from_static(b"value"),
                generation: 1,
                tombstone: false,
                expires_at_ms: None,
            },
        );
    }
    for mutation in 0..4 {
        let mut rendered = store
            .render_state_snapshots(&expected, "render-test")
            .unwrap();
        let reference = state_segment_reference(&rendered[0].reference);
        let mut rows = decode_segment_rows(
            &rendered[0].bytes,
            &rendered[0].index_bytes,
            &reference,
            &store.scope,
        )
        .unwrap();
        match mutation {
            0 => {
                rows.pop();
            }
            1 => rows[0].value = Some(b"other".to_vec()),
            2 => rows[1] = rows[0].clone(),
            _ => rows.swap(0, 1),
        }
        let (bytes, index, reference) = encode_segment(
            &reference.segment_id,
            reference.level,
            reference.logical_sequence,
            &store.scope,
            &rows,
            PRODUCTION_SEGMENT_LIMITS,
        )
        .unwrap();
        rendered[0].bytes = bytes;
        rendered[0].index_bytes = index;
        rendered[0].reference.segment_size_bytes = reference.segment_size_bytes;
        rendered[0].reference.index_size_bytes = reference.index_size_bytes;
        rendered[0].reference.checksum_sha256 = reference.checksum_sha256;
        rendered[0].reference.index_checksum_sha256 = reference.index_checksum_sha256;
        assert!(
            store.validate_rendered_state(&expected, &rendered).is_err(),
            "mutation {mutation}"
        );
    }
}

fn horizon_certificate() -> RetentionHorizonV1 {
    RetentionHorizonV1 {
        encoding_version: 1,
        horizon_sequence: 7,
        purge_cutoff_ms: 1_700_000_000_000,
        pinned_evidence: vec![
            PinnedSequenceV1 {
                kind: "manifest_age".into(),
                id: "manifest-00000000000000000009".into(),
                sequence: 9,
            },
            PinnedSequenceV1 {
                kind: "snapshot".into(),
                id: "snap_01ARZ3NDEKTSV4RRFFQ69G5FAV".into(),
                sequence: 7,
            },
            PinnedSequenceV1 {
                kind: "export".into(),
                id: "exp_01ARZ3NDEKTSV4RRFFQ69G5FAW".into(),
                sequence: 8,
            },
            PinnedSequenceV1 {
                kind: "checkpoint".into(),
                id: "checkpoint-00000000000000000008".into(),
                sequence: 8,
            },
        ],
        parent_state_checksum_sha256: "a".repeat(64),
        purged_rows_sha256: "b".repeat(64),
        purged_counts: PurgedCountsV1 {
            expired_rows: 3,
            tombstones: 2,
        },
    }
}

#[test]
fn retention_horizon_certificate_validation_is_fail_closed() {
    let valid = horizon_certificate();
    valid.validate(10).unwrap();
    valid.validate(7).unwrap();
    let mut no_evidence = valid.clone();
    no_evidence.pinned_evidence.clear();
    no_evidence.validate(10).unwrap();

    let mut cases: Vec<(&str, RetentionHorizonV1, u64)> = Vec::new();
    let mut forged = valid.clone();
    forged.encoding_version = 2;
    cases.push(("encoding version", forged, 10));
    let mut forged = valid.clone();
    forged.parent_state_checksum_sha256 = "A".repeat(64);
    cases.push(("uppercase parent digest", forged, 10));
    let mut forged = valid.clone();
    forged.purged_rows_sha256 = "b".repeat(63);
    cases.push(("short purged digest", forged, 10));
    cases.push(("horizon above logical sequence", valid.clone(), 6));
    let mut forged = valid.clone();
    forged.purge_cutoff_ms = 0;
    cases.push(("zero cutoff", forged, 10));
    let mut forged = valid.clone();
    forged.purge_cutoff_ms = -1;
    cases.push(("negative cutoff", forged, 10));
    let mut forged = valid.clone();
    forged.pinned_evidence[1].kind = "token".into();
    cases.push(("unknown evidence kind", forged, 10));
    let mut forged = valid.clone();
    forged.pinned_evidence[3].id = "../checkpoint".into();
    cases.push(("unfollowable evidence id", forged, 10));
    let mut forged = valid;
    forged.pinned_evidence[1].sequence = 6;
    cases.push(("evidence below the horizon", forged, 10));
    for (case, certificate, logical_sequence) in cases {
        assert!(
            matches!(
                certificate.validate(logical_sequence),
                Err(CatalogError::InvariantViolation { .. })
            ),
            "{case} must be rejected"
        );
    }
}

#[test]
fn purged_rows_digest_binds_order_count_and_every_row_field() {
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let rows = [
        PurgedRow {
            key: b"a",
            generation: 3,
            tombstone: false,
            expires_at_ms: Some(5),
        },
        PurgedRow {
            key: b"b",
            generation: 4,
            tombstone: true,
            expires_at_ms: None,
        },
    ];
    let digest = purged_rows_digest(&scope, &rows).unwrap();
    assert!(valid_raw_digest(&digest));
    assert_ne!(digest, purged_rows_digest(&scope, &rows[..1]).unwrap());
    assert_ne!(
        digest,
        purged_rows_digest(&scope, &[]).unwrap(),
        "an empty purge set still has a scope-bound digest"
    );
    let mut altered = rows.clone();
    altered[0].generation = 4;
    assert_ne!(digest, purged_rows_digest(&scope, &altered).unwrap());
    let mut altered = rows.clone();
    altered[0].expires_at_ms = None;
    assert_ne!(digest, purged_rows_digest(&scope, &altered).unwrap());
    let mut altered = rows.clone();
    altered[1].tombstone = false;
    assert_ne!(digest, purged_rows_digest(&scope, &altered).unwrap());
    let mut unordered = rows.clone();
    unordered.swap(0, 1);
    assert!(
        purged_rows_digest(&scope, &unordered).is_err(),
        "purged rows are digested in strict key order"
    );
    let duplicate = [rows[0].clone(), rows[0].clone()];
    assert!(purged_rows_digest(&scope, &duplicate).is_err());
}

type Forgery = Box<dyn FnOnce(&mut ControlMvpManifest)>;

/// Publishes a hand-built horizon child of the current (consolidated) head
/// through raw storage and returns `(child id, child digest)`.
async fn publish_horizon_child(
    storage: &ScopedStorage,
    store: &ControlMvpStateStore,
    parent: &ControlMvpManifest,
    parent_digest: &str,
    forge: impl FnOnce(&mut ControlMvpManifest),
) -> (String, String) {
    let mut child = parent.clone();
    child.manifest_id = format!("horizon-child-{}", cost::nonce());
    child.base_manifest_id = Some(parent.manifest_id.clone());
    child.parent_manifest_sha256 = Some(parent_digest.to_string());
    child.layout_generation = parent.layout_generation + 1;
    child.committed_at_ms = parent.committed_at_ms;
    child.maintenance_intent = None;
    // The hand-built child reuses the parent's shards: the walk validates
    // transitions, never replays, so the pruned checksum is any digest.
    child.state_checksum_sha256 = "c".repeat(64);
    child.equivalence = Some(RewriteEquivalence {
        encoding_version: 1,
        source_manifest_id: parent.manifest_id.clone(),
        source_manifest_sha256: parent_digest.to_string(),
        source_history_root: parent.history_root.clone(),
        source_physical_root: parent.physical_root.clone(),
        logical_sequence: parent.logical_sequence,
        state_checksum_sha256: child.state_checksum_sha256.clone(),
        render_source: None,
    });
    child.retention_horizon = Some(RetentionHorizonV1 {
        encoding_version: 1,
        horizon_sequence: parent.logical_sequence,
        purge_cutoff_ms: 1_700_000_000_000,
        pinned_evidence: vec![PinnedSequenceV1 {
            kind: "manifest_age".into(),
            id: parent.manifest_id.clone(),
            sequence: parent.logical_sequence,
        }],
        parent_state_checksum_sha256: parent.state_checksum_sha256.clone(),
        purged_rows_sha256: "d".repeat(64),
        purged_counts: PurgedCountsV1 {
            expired_rows: 1,
            tombstones: 1,
        },
    });
    forge(&mut child);
    child.physical_root = child.physical_digest().unwrap();
    child.validate(&store.scope, &child.manifest_id).unwrap();
    let mut unbound = child.clone();
    unbound.equivalence = None;
    assert!(
        unbound
            .validate(&store.scope, &unbound.manifest_id)
            .is_err(),
        "a certificate without rewrite equivalence evidence is rejected"
    );
    let bytes = encode_envelope("control-mvp-manifest", &child).unwrap();
    let digest = sha256_hex(&bytes);
    storage
        .put_raw(
            &store.paths.manifest_object(&child.manifest_id),
            bytes,
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    (child.manifest_id, digest)
}

#[tokio::test]
async fn ancestry_accepts_a_horizon_transition_bound_to_its_parent_and_rejects_forgeries() {
    let (storage, store) = fixture();
    for _ in 0..16 {
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        tx.put(b"row", Bytes::from_static(b"value")).await.unwrap();
        tx.commit().await.unwrap();
    }
    ControlMvpMaintenanceWorker::new(storage.clone(), store.scope.clone())
        .unwrap()
        .test_consolidate_pending(DurableAuthorityBinding::new([17; 32]))
        .await
        .unwrap()
        .unwrap();
    let parent = manifest(&store).await;
    assert!(parent.tx_refs.is_empty(), "consolidated parent");
    let parent_digest = store.load_pointer().await.unwrap().manifest_checksum_sha256;
    let grandparent = parent.base_manifest_id.clone().unwrap();

    let (child, digest) =
        publish_horizon_child(&storage, &store, &parent, &parent_digest, |_| {}).await;
    assert_eq!(
        store
            .resolve_ancestor(&child, &digest, |m, _| {
                (m.manifest_id == grandparent).then_some(m.logical_sequence)
            })
            .await
            .unwrap(),
        Some(parent.logical_sequence),
        "a well-formed horizon child walks through its parent to older ancestry"
    );

    let forgeries: Vec<(&str, Forgery)> = vec![
        (
            "parent state checksum",
            Box::new(|child: &mut ControlMvpManifest| {
                child
                    .retention_horizon
                    .as_mut()
                    .unwrap()
                    .parent_state_checksum_sha256 = "e".repeat(64);
            }),
        ),
        (
            "logical sequence",
            Box::new(|child: &mut ControlMvpManifest| {
                let sequence = child.logical_sequence + 1;
                child.logical_sequence = sequence;
                child.history_anchor.sequence = sequence;
                for state in &mut child.base_states {
                    state.logical_sequence = sequence;
                }
                child.equivalence.as_mut().unwrap().logical_sequence = sequence;
                child.retention_horizon.as_mut().unwrap().horizon_sequence = sequence;
                child.retention_horizon.as_mut().unwrap().pinned_evidence[0].sequence = sequence;
            }),
        ),
        (
            "history root",
            Box::new(|child: &mut ControlMvpManifest| {
                let root = "f".repeat(64);
                child.history_root.clone_from(&root);
                child.history_anchor.root.clone_from(&root);
                child.equivalence.as_mut().unwrap().source_history_root = root;
            }),
        ),
    ];
    for (case, forge) in forgeries {
        let (child, digest) =
            publish_horizon_child(&storage, &store, &parent, &parent_digest, forge).await;
        let error = store
            .resolve_ancestor(&child, &digest, |m, _| {
                (m.manifest_id == grandparent).then_some(())
            })
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid authenticated ancestry transition"),
            "{case}: {error}"
        );
    }
}

#[test]
fn age_anchor_rule_inherits_within_a_bucket_and_records_the_parent_across_one() {
    let bucket = AGE_ANCHOR_BUCKET_MS;
    assert_eq!(
        age_anchor_for_child(bucket + 1, None),
        None,
        "genesis has no anchor"
    );
    let older = AgeAnchorV1 {
        manifest_id: "manifest-older".into(),
        manifest_sha256: "1".repeat(64),
        sequence: 3,
        committed_at_ms: bucket - 5,
    };
    let inherited = Some(older.clone());
    let digest = "2".repeat(64);
    let parent = AnchorParent {
        manifest_id: "manifest-parent",
        manifest_sha256: &digest,
        sequence: 9,
        committed_at_ms: 2 * bucket + 10,
        age_anchor: &inherited,
    };
    // Same hour bucket as the parent: the parent's anchor is inherited.
    assert_eq!(
        age_anchor_for_child(2 * bucket + 500, Some(parent.clone())),
        Some(older.clone())
    );
    assert_eq!(
        age_anchor_for_child(3 * bucket - 1, Some(parent.clone())),
        Some(older)
    );
    // A later bucket: the parent itself becomes the anchor.
    assert_eq!(
        age_anchor_for_child(3 * bucket, Some(parent)),
        Some(AgeAnchorV1 {
            manifest_id: "manifest-parent".into(),
            manifest_sha256: digest,
            sequence: 9,
            committed_at_ms: 2 * bucket + 10,
        })
    );
}

/// Sixteen commits at `start`, then a consolidation two hours later: it
/// crosses a bucket, so the consolidated head carries an anchor of its own.
#[allow(
    clippy::future_not_send,
    reason = "the fixture clock guard is thread-bound by design"
)]
async fn anchored_consolidated_head(
    storage: &ScopedStorage,
    store: &ControlMvpStateStore,
    start: DateTime<Utc>,
) -> arco_core::test_inputs::FixedInputs {
    use arco_core::test_inputs::FixedInputs;
    {
        let _clock = FixedInputs::at(start);
        for _ in 0..16 {
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            tx.put(b"row", Bytes::from_static(b"value")).await.unwrap();
            tx.commit().await.unwrap();
        }
    }
    let clock = FixedInputs::at(start + ChronoDuration::hours(2));
    ControlMvpMaintenanceWorker::new(storage.clone(), store.scope.clone())
        .unwrap()
        .test_consolidate_pending(DurableAuthorityBinding::new([17; 32]))
        .await
        .unwrap()
        .unwrap();
    clock
}

async fn walks_to(
    store: &ControlMvpStateStore,
    child: &str,
    digest: &str,
    target: &str,
) -> Result<bool> {
    store
        .resolve_ancestor(child, digest, |m, _| {
            (m.manifest_id == target).then_some(())
        })
        .await
        .map(|found| found.is_some())
}

#[tokio::test]
async fn ancestry_rejects_a_wrong_anchor_and_a_stamp_before_the_parent() {
    let (storage, store) = fixture();
    let start = DateTime::from_timestamp(1_893_456_000, 0).unwrap();
    let _clock = anchored_consolidated_head(&storage, &store, start).await;
    let parent = manifest(&store).await;
    let parent_digest = store.load_pointer().await.unwrap().manifest_checksum_sha256;
    let grandparent = parent.base_manifest_id.clone().unwrap();
    assert_eq!(
        parent
            .age_anchor
            .as_ref()
            .map(|anchor| anchor.manifest_id.as_str()),
        Some(grandparent.as_str()),
        "a consolidation rendered in a later bucket records its parent"
    );
    // A well-formed child in the parent's bucket inherits the parent's anchor.
    let (child, digest) =
        publish_horizon_child(&storage, &store, &parent, &parent_digest, |_| {}).await;
    assert!(
        walks_to(&store, &child, &digest, &grandparent)
            .await
            .unwrap()
    );
    let forgeries: Vec<(&str, Forgery)> = vec![
        (
            "anchor not inherited inside the bucket",
            Box::new(|child: &mut ControlMvpManifest| {
                child.age_anchor = Some(AgeAnchorV1 {
                    manifest_id: "manifest-forged".into(),
                    manifest_sha256: "3".repeat(64),
                    sequence: 1,
                    committed_at_ms: 1,
                });
            }),
        ),
        (
            "anchor dropped inside the bucket",
            Box::new(|child: &mut ControlMvpManifest| {
                child.age_anchor = None;
            }),
        ),
        (
            "bucket crossed without recording the parent",
            Box::new(|child: &mut ControlMvpManifest| {
                child.committed_at_ms += AGE_ANCHOR_BUCKET_MS;
            }),
        ),
        (
            "stamp before the parent",
            Box::new(|child: &mut ControlMvpManifest| {
                child.committed_at_ms -= 1;
            }),
        ),
    ];
    for (case, forge) in forgeries {
        let (child, digest) =
            publish_horizon_child(&storage, &store, &parent, &parent_digest, forge).await;
        let error = walks_to(&store, &child, &digest, &grandparent)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid authenticated ancestry transition"),
            "{case}: {error}"
        );
    }
    // Crossing the bucket with the parent recorded is accepted.
    let recorded = AgeAnchorV1 {
        manifest_id: parent.manifest_id.clone(),
        manifest_sha256: parent_digest.clone(),
        sequence: parent.logical_sequence,
        committed_at_ms: parent.committed_at_ms,
    };
    let (child, digest) = publish_horizon_child(&storage, &store, &parent, &parent_digest, {
        let recorded = recorded.clone();
        move |child: &mut ControlMvpManifest| {
            child.committed_at_ms += AGE_ANCHOR_BUCKET_MS;
            child.age_anchor = Some(recorded);
        }
    })
    .await;
    assert!(
        walks_to(&store, &child, &digest, &grandparent)
            .await
            .unwrap()
    );
}
