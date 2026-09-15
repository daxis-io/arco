//! Explicit test-only streaming authority-8 fixture construction.

use super::{
    ActiveRecord8, ArtifactRef, CatalogError, ControlMvpSegmentRow, ControlMvpStateStore,
    Manifest8, ManifestKind8, Result, RootBinding, StateScope, directory, encode_json,
    encode_json_limited, invariant_violation, logical_v2, persist_role_rows, physical,
    publish_manifest_candidate, put_immutable_matching, sha256_hex, valid_raw_digest,
    validate_raw_checksum, write_projection_source,
};
use crate::state_store::ProjectionIntentV2;
use serde::{Deserialize, Serialize};

const SYNTHETIC_WITNESS_RECORD_TYPE: &str = "arco_control_synthetic_genesis";
const SYNTHETIC_WITNESS_ENCODING: u32 = 1;
// Keeps fixture-owned buffers bounded. The production encoder still owns the 64 MiB
// singleton rule; an oversized multi-row buffer is split locally below.
const SYNTHETIC_ROWS_PER_PART: usize = 256;

/// One ordered logical KV row for synthetic authority-8 genesis.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyntheticKvEntry {
    /// Exact logical key bytes.
    pub key: Vec<u8>,
    /// Explicit generation retained by the fixture.
    pub generation: u64,
    /// Opaque value bytes, or a retained tombstone.
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SyntheticGenesisWitness8 {
    record_type: String,
    encoding_version: u32,
    scope: StateScope,
    fixture_id: String,
    logical_sequence: u64,
    logical_history: String,
    kv_root: RootBinding,
    active_id_root: RootBinding,
    delivery_order_root: RootBinding,
    kv_rows: u64,
    kv_blocks: u64,
    active_id_rows: u64,
    active_id_blocks: u64,
    delivery_order_rows: u64,
    delivery_order_blocks: u64,
}

struct RoleWriter {
    role: physical::Role,
    // `persist_role_rows` assigns ordinary edit parts from a fixed role window.
    // A fixture can exceed that window, so each role owns a separate immutable ID.
    physical_manifest_id: String,
    next_part: usize,
    rows: u64,
    blocks: u64,
    builder: directory::Builder,
}

impl RoleWriter {
    fn new(directory: &directory::Directory, role: physical::Role, fixture_id: &str) -> Self {
        let role_name = match role {
            physical::Role::Kv => "kv",
            physical::Role::ActiveId => "active-id",
            physical::Role::DeliveryOrder => "delivery-order",
        };
        Self {
            role,
            physical_manifest_id: format!("synthetic-{fixture_id}-{role_name}"),
            next_part: 0,
            rows: 0,
            blocks: 0,
            builder: directory.builder(),
        }
    }

    async fn push_rows(
        &mut self,
        store: &ControlMvpStateStore,
        logical_sequence: u64,
        rows: Vec<ControlMvpSegmentRow>,
    ) -> Result<()> {
        let mut pending = vec![rows];
        while let Some(mut part) = pending.pop() {
            if part.is_empty() {
                continue;
            }
            match persist_role_rows(
                store,
                self.role,
                &self.physical_manifest_id,
                self.next_part,
                logical_sequence,
                &part,
            )
            .await
            {
                Ok(leaves) => {
                    // Do not trust the encoder's return value alone. Authenticate the
                    // descriptor/index/block path and prove the exact rows before the
                    // directory is allowed to reference the leaf.
                    let mut observed = Vec::new();
                    for leaf in &leaves {
                        observed.extend(store.resolve_physical_block(self.role, leaf).await?);
                    }
                    if observed != part {
                        return Err(invariant_violation(
                            "synthetic physical rows differ from streamed input",
                        ));
                    }
                    for leaf in leaves {
                        self.builder.push(leaf).await?;
                        self.blocks = self.blocks.checked_add(1).ok_or_else(|| {
                            invariant_violation("synthetic fixture block count overflows")
                        })?;
                    }
                    self.rows = self.rows.checked_add(part.len() as u64).ok_or_else(|| {
                        invariant_violation("synthetic fixture row count overflows")
                    })?;
                    self.next_part = self.next_part.checked_add(1).ok_or_else(|| {
                        invariant_violation("synthetic fixture part count overflows")
                    })?;
                }
                Err(CatalogError::MaintenanceBackpressure { .. }) if part.len() > 1 => {
                    let right = part.split_off(part.len() / 2);
                    // LIFO preserves sorted input order through the builder.
                    pending.push(right);
                    pending.push(part);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    async fn finish(self) -> Result<(directory::Root, u64, u64)> {
        Ok((self.builder.finish().await?, self.rows, self.blocks))
    }
}

fn root_binding(role: physical::Role, root: &directory::Root) -> RootBinding {
    RootBinding {
        role,
        leaf_encoding: super::LEAF_ENCODING.to_owned(),
        directory_root_hex: hex::encode(root.encode()),
    }
}

fn fixture_id_is_valid(fixture_id: &str) -> bool {
    !fixture_id.is_empty() && super::super::integrity::valid_immutable_id(fixture_id)
}

impl ControlMvpStateStore {
    /// Installs an explicit streaming synthetic authority-8 fixture at an absent HEAD.
    ///
    /// The input iterators are consumed once. Immutable physical artifacts are
    /// deliberately detached until the ordinary candidate publisher performs its
    /// single conditional HEAD publication.
    ///
    /// # Errors
    /// Returns an error for invalid or unordered inputs, mismatched counts, corrupt
    /// artifacts, storage failures, or a concurrently published HEAD.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn install_synthetic_genesis<K, I>(
        &self,
        fixture_id: &str,
        logical_sequence: u64,
        declared_kv_count: u64,
        ordered_kv: K,
        declared_intent_count: u64,
        ordered_intents: I,
    ) -> Result<super::StateToken>
    where
        K: IntoIterator<Item = SyntheticKvEntry>,
        I: IntoIterator<Item = ProjectionIntentV2>,
    {
        self.install_synthetic_genesis_with_part_rows(
            fixture_id,
            logical_sequence,
            declared_kv_count,
            ordered_kv,
            declared_intent_count,
            ordered_intents,
            SYNTHETIC_ROWS_PER_PART,
        )
        .await
    }

    // Keep streaming construction, paired-index verification and publication in order.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn install_synthetic_genesis_with_part_rows<K, I>(
        &self,
        fixture_id: &str,
        logical_sequence: u64,
        declared_kv_count: u64,
        ordered_kv: K,
        declared_intent_count: u64,
        ordered_intents: I,
        rows_per_part: usize,
    ) -> Result<super::StateToken>
    where
        K: IntoIterator<Item = SyntheticKvEntry>,
        I: IntoIterator<Item = ProjectionIntentV2>,
    {
        if self.authority_format != super::AUTHORITY_FORMAT {
            return Err(CatalogError::UnsupportedAuthorityFormat {
                message: "synthetic genesis requires explicit authority format 8".into(),
            });
        }
        if !fixture_id_is_valid(fixture_id) {
            return Err(invariant_violation("synthetic fixture ID is invalid"));
        }
        if rows_per_part == 0 {
            return Err(invariant_violation("synthetic fixture part size is zero"));
        }
        let base = self.pin_bounded_base().await?;
        if base.logical_sequence() != 0 || base.manifest.is_some() {
            return Err(CatalogError::PreconditionFailed {
                message: "synthetic genesis requires an absent authority-8 HEAD".into(),
            });
        }

        let directory = directory::Directory::new(self.retention.clone(), &self.scope)?;
        let mut history = logical_v2::SyntheticGenesisHistory::new(
            &self.scope,
            logical_sequence,
            declared_kv_count,
            declared_intent_count,
        )?;
        let mut kv_writer = RoleWriter::new(&directory, physical::Role::Kv, fixture_id);
        let mut kv_part = Vec::with_capacity(rows_per_part);
        let mut ordinal = 0_u64;
        for entry in ordered_kv {
            history.push_kv(&entry.key, entry.generation, entry.value.as_deref())?;
            let tombstone = entry.value.is_none();
            kv_part.push(ControlMvpSegmentRow {
                record_kind: super::super::SEGMENT_RECORD_KV,
                key: entry.key,
                value: entry.value,
                generation: entry.generation,
                tombstone,
                logical_sequence,
                logical_ordinal: ordinal,
                origin_sequence: None,
            });
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| invariant_violation("synthetic KV ordinal overflows"))?;
            if kv_part.len() == rows_per_part {
                kv_writer
                    .push_rows(self, logical_sequence, std::mem::take(&mut kv_part))
                    .await?;
            }
        }
        kv_writer.push_rows(self, logical_sequence, kv_part).await?;
        let (kv_root, kv_rows, kv_blocks) = kv_writer.finish().await?;

        let mut active_writer = RoleWriter::new(&directory, physical::Role::ActiveId, fixture_id);
        let mut delivery_writer =
            RoleWriter::new(&directory, physical::Role::DeliveryOrder, fixture_id);
        let mut active_part = Vec::with_capacity(rows_per_part);
        let mut delivery_part = Vec::with_capacity(rows_per_part);
        for intent in ordered_intents {
            history.push_intent(&intent)?;
            let source = write_projection_source(
                self,
                intent.source_logical_sequence(),
                intent.logical_commit_id(),
                &kv_root,
            )
            .await?;
            let intent_bytes = encode_json(&intent, "synthetic V2 projection intent")?;
            let active = ActiveRecord8 {
                record_id: intent.intent_id().to_owned(),
                origin_sequence: intent.source_logical_sequence(),
                ordinal: intent.ordinal(),
                source_descriptor_sha256: source.sha256.clone(),
                payload: intent_bytes.to_vec(),
            };
            active_part.push(ControlMvpSegmentRow {
                record_kind: super::super::SEGMENT_RECORD_OUTBOX,
                key: intent.intent_id().as_bytes().to_vec(),
                value: Some(encode_json(&active, "synthetic active V2 projection")?.to_vec()),
                generation: 0,
                tombstone: false,
                logical_sequence,
                logical_ordinal: intent.ordinal(),
                origin_sequence: Some(intent.source_logical_sequence()),
            });
            delivery_part.push(ControlMvpSegmentRow {
                record_kind: super::super::SEGMENT_RECORD_OUTBOX,
                key: super::delivery_key(
                    intent.source_logical_sequence(),
                    intent.ordinal(),
                    intent.intent_id(),
                )?,
                value: Some(source.sha256.into_bytes()),
                generation: 0,
                tombstone: false,
                logical_sequence,
                logical_ordinal: intent.ordinal(),
                origin_sequence: Some(intent.source_logical_sequence()),
            });
            if active_part.len() == rows_per_part {
                active_writer
                    .push_rows(self, logical_sequence, std::mem::take(&mut active_part))
                    .await?;
                delivery_writer
                    .push_rows(self, logical_sequence, std::mem::take(&mut delivery_part))
                    .await?;
            }
        }
        active_writer
            .push_rows(self, logical_sequence, active_part)
            .await?;
        delivery_writer
            .push_rows(self, logical_sequence, delivery_part)
            .await?;
        let (active_id_root, active_id_rows, active_id_blocks) = active_writer.finish().await?;
        let (delivery_order_root, delivery_order_rows, delivery_order_blocks) =
            delivery_writer.finish().await?;
        let logical_history = history.finish()?;

        let manifest_id = format!("synthetic-{fixture_id}");
        let witness = SyntheticGenesisWitness8 {
            record_type: SYNTHETIC_WITNESS_RECORD_TYPE.to_owned(),
            encoding_version: SYNTHETIC_WITNESS_ENCODING,
            scope: self.scope.clone(),
            fixture_id: fixture_id.to_owned(),
            logical_sequence,
            logical_history: logical_history.clone(),
            kv_root: root_binding(physical::Role::Kv, &kv_root),
            active_id_root: root_binding(physical::Role::ActiveId, &active_id_root),
            delivery_order_root: root_binding(physical::Role::DeliveryOrder, &delivery_order_root),
            kv_rows,
            kv_blocks,
            active_id_rows,
            active_id_blocks,
            delivery_order_rows,
            delivery_order_blocks,
        };
        let witness_bytes = encode_json_limited(
            &witness,
            super::super::MAX_CONTROL_JSON_BYTES,
            "synthetic genesis witness",
        )?;
        let witness_path = format!(
            "{}/synthetic-genesis/{fixture_id}.json",
            self.paths.base_prefix()
        );
        let witness_ref = ArtifactRef {
            path: witness_path.clone(),
            sha256: sha256_hex(&witness_bytes),
        };
        put_immutable_matching(
            &self.storage,
            &witness_path,
            witness_bytes,
            "synthetic genesis witness already exists with different bytes",
        )
        .await?;
        let manifest = Manifest8 {
            format_version: super::AUTHORITY_FORMAT,
            implementation: super::super::IMPLEMENTATION.to_owned(),
            scope: self.scope.clone(),
            manifest_id: manifest_id.clone(),
            logical_sequence,
            logical_history,
            kind: ManifestKind8::SyntheticGenesis,
            parent_manifest_id: None,
            parent_manifest_sha256: None,
            writer_epoch: base.writer_epoch(),
            reclamation_generation: base.reclamation_generation(),
            kv_root: root_binding(physical::Role::Kv, &kv_root),
            active_id_root: root_binding(physical::Role::ActiveId, &active_id_root),
            delivery_order_root: root_binding(physical::Role::DeliveryOrder, &delivery_order_root),
            transaction: None,
            transition: None,
            genesis_witness: Some(witness_ref),
            projection_source: None,
        };
        manifest.validate(&self.scope, &manifest_id)?;
        validate_manifest_witness(
            self,
            &manifest,
            (&kv_root, &active_id_root, &delivery_order_root),
        )
        .await?;
        publish_manifest_candidate(self, &base, manifest).await
    }
}

/// Authenticates a synthetic witness and its bound roots without replaying inventory.
pub(super) async fn validate_manifest_witness(
    store: &ControlMvpStateStore,
    manifest: &Manifest8,
    roots: (&directory::Root, &directory::Root, &directory::Root),
) -> Result<()> {
    if manifest.kind != ManifestKind8::SyntheticGenesis {
        return Err(invariant_violation(
            "synthetic witness is attached to a transaction manifest",
        ));
    }
    let witness_ref = manifest
        .genesis_witness
        .as_ref()
        .ok_or_else(|| invariant_violation("synthetic manifest has no genesis witness"))?;
    let fixture_id = manifest
        .manifest_id
        .strip_prefix("synthetic-")
        .filter(|id| fixture_id_is_valid(id))
        .ok_or_else(|| invariant_violation("synthetic manifest ID has no valid fixture ID"))?;
    let expected_path = format!(
        "{}/synthetic-genesis/{fixture_id}.json",
        store.paths.base_prefix()
    );
    if witness_ref.path != expected_path || !valid_raw_digest(&witness_ref.sha256) {
        return Err(invariant_violation(
            "synthetic genesis witness reference is invalid",
        ));
    }
    let bytes = store
        .get_json(&witness_ref.path, super::super::MAX_CONTROL_JSON_BYTES)
        .await?;
    validate_raw_checksum(
        &bytes,
        Some(&witness_ref.sha256),
        "synthetic genesis witness checksum",
    )?;
    let witness: SyntheticGenesisWitness8 =
        super::super::decode_json(&bytes, "synthetic genesis witness")?;
    let valid_inventory = |rows: u64, blocks: u64, root: &directory::Root| {
        rows == root.row_count() && (rows == 0) == (blocks == 0) && blocks <= rows
    };
    if witness.record_type != SYNTHETIC_WITNESS_RECORD_TYPE
        || witness.encoding_version != SYNTHETIC_WITNESS_ENCODING
        || witness.scope != store.scope
        || witness.fixture_id != fixture_id
        || witness.logical_sequence != manifest.logical_sequence
        || witness.logical_history != manifest.logical_history
        || witness.kv_root.role != physical::Role::Kv
        || witness.active_id_root.role != physical::Role::ActiveId
        || witness.delivery_order_root.role != physical::Role::DeliveryOrder
        || witness.kv_root.leaf_encoding != super::LEAF_ENCODING
        || witness.active_id_root.leaf_encoding != super::LEAF_ENCODING
        || witness.delivery_order_root.leaf_encoding != super::LEAF_ENCODING
        || witness.kv_root.directory_root_hex != hex::encode(roots.0.encode())
        || witness.active_id_root.directory_root_hex != hex::encode(roots.1.encode())
        || witness.delivery_order_root.directory_root_hex != hex::encode(roots.2.encode())
        || witness.kv_root.directory_root_hex != manifest.kv_root.directory_root_hex
        || witness.active_id_root.directory_root_hex != manifest.active_id_root.directory_root_hex
        || witness.delivery_order_root.directory_root_hex
            != manifest.delivery_order_root.directory_root_hex
        || !valid_inventory(witness.kv_rows, witness.kv_blocks, roots.0)
        || !valid_inventory(witness.active_id_rows, witness.active_id_blocks, roots.1)
        || !valid_inventory(
            witness.delivery_order_rows,
            witness.delivery_order_blocks,
            roots.2,
        )
        || witness.active_id_rows != witness.delivery_order_rows
    {
        return Err(invariant_violation(
            "synthetic genesis witness differs from its manifest roots",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arco_core::{MemoryBackend, ScopedStorage};
    use bytes::Bytes;

    use super::{
        ArtifactRef, ControlMvpStateStore, Manifest8, ManifestKind8, SYNTHETIC_ROWS_PER_PART,
        SYNTHETIC_WITNESS_ENCODING, SYNTHETIC_WITNESS_RECORD_TYPE, StateScope,
        SyntheticGenesisWitness8, SyntheticKvEntry, directory, encode_json, put_immutable_matching,
        root_binding, sha256_hex, validate_manifest_witness,
    };
    use crate::state_store::{ArcoStateTxn, ProjectionIntentV2, StateScope, TxnOptions};

    async fn empty_roots(
        store: &ControlMvpStateStore,
    ) -> (directory::Root, directory::Root, directory::Root) {
        let directory =
            directory::Directory::new(store.retention.clone(), &store.scope).expect("directory");
        let root = directory.empty_root().await.expect("empty root");
        (root.clone(), root.clone(), root)
    }

    fn synthetic_manifest(
        store: &ControlMvpStateStore,
        fixture_id: &str,
        roots: (&directory::Root, &directory::Root, &directory::Root),
        witness: ArtifactRef,
    ) -> Manifest8 {
        Manifest8 {
            format_version: super::super::AUTHORITY_FORMAT,
            implementation: super::super::super::IMPLEMENTATION.to_owned(),
            scope: store.scope.clone(),
            manifest_id: format!("synthetic-{fixture_id}"),
            logical_sequence: 7,
            logical_history: "ab".repeat(32),
            kind: ManifestKind8::SyntheticGenesis,
            parent_manifest_id: None,
            parent_manifest_sha256: None,
            writer_epoch: 0,
            reclamation_generation: 0,
            kv_root: root_binding(super::physical::Role::Kv, roots.0),
            active_id_root: root_binding(super::physical::Role::ActiveId, roots.1),
            delivery_order_root: root_binding(super::physical::Role::DeliveryOrder, roots.2),
            transaction: None,
            transition: None,
            genesis_witness: Some(witness),
            projection_source: None,
        }
    }

    #[tokio::test]
    async fn synthetic_genesis_publishes_an_authenticated_streamed_kv_fixture() {
        let storage = ScopedStorage::new(
            Arc::new(MemoryBackend::new()),
            "synthetic-tenant",
            "synthetic-workspace",
        )
        .expect("storage");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage,
            StateScope::new("synthetic-tenant", "synthetic-workspace", "catalog"),
        )
        .expect("store");

        let result = store
            .install_synthetic_genesis(
                "fixture-1",
                7,
                1,
                [SyntheticKvEntry {
                    key: b"receipt/0001".to_vec(),
                    generation: 7,
                    value: Some(b"opaque-v2-record".to_vec()),
                }],
                0,
                std::iter::empty(),
            )
            .await;

        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn synthetic_genesis_rejects_a_declared_count_before_publication() {
        let storage = ScopedStorage::new(
            Arc::new(MemoryBackend::new()),
            "synthetic-tenant",
            "synthetic-workspace",
        )
        .expect("storage");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage,
            StateScope::new("synthetic-tenant", "synthetic-workspace", "catalog"),
        )
        .expect("store");

        let result = store
            .install_synthetic_genesis(
                "fixture-2",
                7,
                2,
                [SyntheticKvEntry {
                    key: b"receipt/0001".to_vec(),
                    generation: 7,
                    value: Some(b"opaque-v2-record".to_vec()),
                }],
                0,
                std::iter::empty(),
            )
            .await;

        assert!(result.is_err(), "declared-count mismatch must not publish");
    }

    #[tokio::test]
    async fn synthetic_genesis_rejects_a_malformed_witness() {
        let storage = ScopedStorage::new(
            Arc::new(MemoryBackend::new()),
            "synthetic-tenant",
            "synthetic-workspace",
        )
        .expect("storage");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage,
            StateScope::new("synthetic-tenant", "synthetic-workspace", "catalog"),
        )
        .expect("store");
        let (kv, active, delivery) = empty_roots(&store).await;
        let bytes = Bytes::from_static(b"not a synthetic witness");
        let path = format!(
            "{}/synthetic-genesis/fixture-malformed.json",
            store.paths.base_prefix()
        );
        let witness = ArtifactRef {
            path: path.clone(),
            sha256: sha256_hex(&bytes),
        };
        put_immutable_matching(&store.storage, &path, bytes, "test witness")
            .await
            .expect("write malformed witness");
        let manifest = synthetic_manifest(
            &store,
            "fixture-malformed",
            (&kv, &active, &delivery),
            witness,
        );

        assert!(
            validate_manifest_witness(&store, &manifest, (&kv, &active, &delivery))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn synthetic_genesis_rejects_a_witness_with_an_incomplete_paired_root() {
        let storage = ScopedStorage::new(
            Arc::new(MemoryBackend::new()),
            "synthetic-tenant",
            "synthetic-workspace",
        )
        .expect("storage");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage,
            StateScope::new("synthetic-tenant", "synthetic-workspace", "catalog"),
        )
        .expect("store");
        let (kv, active, delivery) = empty_roots(&store).await;
        let fixture_id = "fixture-incomplete";
        let mut witness = SyntheticGenesisWitness8 {
            record_type: SYNTHETIC_WITNESS_RECORD_TYPE.to_owned(),
            encoding_version: SYNTHETIC_WITNESS_ENCODING,
            scope: store.scope.clone(),
            fixture_id: fixture_id.to_owned(),
            logical_sequence: 7,
            logical_history: "ab".repeat(32),
            kv_root: root_binding(super::physical::Role::Kv, &kv),
            active_id_root: root_binding(super::physical::Role::ActiveId, &active),
            delivery_order_root: root_binding(super::physical::Role::DeliveryOrder, &delivery),
            kv_rows: 0,
            kv_blocks: 0,
            active_id_rows: 0,
            active_id_blocks: 0,
            delivery_order_rows: 0,
            delivery_order_blocks: 0,
        };
        // A witness has no authority to replace either member of the outbox pair.
        witness.active_id_root.directory_root_hex = "00".to_owned();
        let bytes = encode_json(&witness, "test synthetic witness").expect("witness encoding");
        let path = format!(
            "{}/synthetic-genesis/{fixture_id}.json",
            store.paths.base_prefix()
        );
        let manifest = synthetic_manifest(
            &store,
            fixture_id,
            (&kv, &active, &delivery),
            ArtifactRef {
                path: path.clone(),
                sha256: sha256_hex(&bytes),
            },
        );
        put_immutable_matching(&store.storage, &path, bytes, "test witness")
            .await
            .expect("write tampered witness");

        assert!(
            validate_manifest_witness(&store, &manifest, (&kv, &active, &delivery))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn synthetic_genesis_rejects_a_witness_with_wrong_root_row_count() {
        let storage = ScopedStorage::new(
            Arc::new(MemoryBackend::new()),
            "synthetic-tenant",
            "synthetic-workspace",
        )
        .expect("storage");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage,
            StateScope::new("synthetic-tenant", "synthetic-workspace", "catalog"),
        )
        .expect("store");
        let (kv, active, delivery) = empty_roots(&store).await;
        let fixture_id = "fixture-row-count";
        let witness = SyntheticGenesisWitness8 {
            record_type: SYNTHETIC_WITNESS_RECORD_TYPE.to_owned(),
            encoding_version: SYNTHETIC_WITNESS_ENCODING,
            scope: store.scope.clone(),
            fixture_id: fixture_id.to_owned(),
            logical_sequence: 7,
            logical_history: "ab".repeat(32),
            kv_root: root_binding(super::physical::Role::Kv, &kv),
            active_id_root: root_binding(super::physical::Role::ActiveId, &active),
            delivery_order_root: root_binding(super::physical::Role::DeliveryOrder, &delivery),
            // The empty KV directory commits zero rows, so this inventory claim
            // must fail without replaying the root.
            kv_rows: 1,
            kv_blocks: 1,
            active_id_rows: 0,
            active_id_blocks: 0,
            delivery_order_rows: 0,
            delivery_order_blocks: 0,
        };
        let bytes = encode_json(&witness, "test synthetic witness").expect("witness encoding");
        let path = format!(
            "{}/synthetic-genesis/{fixture_id}.json",
            store.paths.base_prefix()
        );
        let manifest = synthetic_manifest(
            &store,
            fixture_id,
            (&kv, &active, &delivery),
            ArtifactRef {
                path: path.clone(),
                sha256: sha256_hex(&bytes),
            },
        );
        put_immutable_matching(&store.storage, &path, bytes, "test witness")
            .await
            .expect("write wrong-count witness");

        assert!(
            validate_manifest_witness(&store, &manifest, (&kv, &active, &delivery))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn partitioned_fixture_has_layout_independent_history_and_next_commit_identity() {
        let scope = StateScope::new("synthetic-tenant", "synthetic-workspace", "catalog");
        let left_storage = ScopedStorage::new(
            Arc::new(MemoryBackend::new()),
            "synthetic-tenant",
            "synthetic-workspace",
        )
        .expect("left storage");
        let right_storage = ScopedStorage::new(
            Arc::new(MemoryBackend::new()),
            "synthetic-tenant",
            "synthetic-workspace",
        )
        .expect("right storage");
        let left = ControlMvpStateStore::new_synthetic_bounded(left_storage, scope.clone())
            .expect("left store");
        let right =
            ControlMvpStateStore::new_synthetic_bounded(right_storage, scope).expect("right store");
        let rows = (0_u16..257)
            .map(|index| SyntheticKvEntry {
                key: format!("receipt/{index:04}").into_bytes(),
                generation: 1,
                value: Some(format!("opaque-{index:04}").into_bytes()),
            })
            .collect::<Vec<_>>();
        let intents = vec![
            ProjectionIntentV2::new(
                "partitioned-intent",
                "projection",
                left.scope.clone(),
                1,
                "ef".repeat(32),
                0,
                b"same-intent",
            )
            .expect("valid historical V2 intent"),
        ];

        let left_token = left
            .install_synthetic_genesis_with_part_rows(
                "partitioned-fixture",
                1,
                rows.len() as u64,
                rows.clone(),
                intents.len() as u64,
                intents.clone(),
                64,
            )
            .await
            .expect("64-row fixture");
        let right_token = right
            .install_synthetic_genesis_with_part_rows(
                "partitioned-fixture",
                1,
                rows.len() as u64,
                rows,
                intents.len() as u64,
                intents,
                SYNTHETIC_ROWS_PER_PART,
            )
            .await
            .expect("256-row fixture");
        let left_base = left
            .read_bounded_token(&left_token)
            .await
            .expect("left fixture");
        let right_base = right
            .read_bounded_token(&right_token)
            .await
            .expect("right fixture");
        assert_ne!(left_base.kv_root.encode(), right_base.kv_root.encode());
        assert_eq!(left_base.logical_history(), right_base.logical_history());

        let mut left_txn = left
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("left begin");
        let mut right_txn = right
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("right begin");
        for txn in [&mut left_txn, &mut right_txn] {
            txn.set_logical_operation("frozen-operation", "catalog", &"cd".repeat(32))
                .expect("frozen operation");
            txn.put(b"receipt/after", Bytes::from_static(b"same-write"))
                .await
                .expect("same write");
        }
        let (_, _, left_id) = left.commit_bounded(left_txn).await.expect("left commit");
        let (_, _, right_id) = right.commit_bounded(right_txn).await.expect("right commit");
        assert_eq!(left_id, right_id);
        let left_after = left.pin_bounded_base().await.expect("left after");
        let right_after = right.pin_bounded_base().await.expect("right after");
        assert_eq!(left_after.logical_history(), right_after.logical_history());
    }
}
