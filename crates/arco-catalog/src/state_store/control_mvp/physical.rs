//! Physical leaf interpretation for explicitly selected authority-8 roots.
use super::{
    ControlMvpBlock, ControlMvpSegmentLevel, ControlMvpSegmentRef, ControlMvpSegmentRow,
    ControlMvpStateStore, MAX_CONTROL_JSON_BYTES, Result, SEGMENT_RECORD_KV, StateScope,
    block_key_bounds, decode_json, directory, invariant_violation, read_cache,
    validate_raw_checksum,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Role {
    Kv,
    ActiveId,
    DeliveryOrder,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Descriptor {
    pub encoding_version: u32,
    pub scope: StateScope,
    pub role: Role,
    pub segment: ControlMvpSegmentRef,
    pub segment_version: String,
    pub index_version: String,
    pub block: ControlMvpBlock,
}

impl ControlMvpStateStore {
    pub(super) async fn resolve_physical_block(
        &self,
        role: Role,
        leaf: &directory::Leaf,
    ) -> Result<Vec<ControlMvpSegmentRow>> {
        let path = format!(
            "{}/physical/descriptors/{}.json",
            self.paths.base_prefix(),
            hex::encode(leaf.digest)
        );
        let bytes = self.get_json(&path, MAX_CONTROL_JSON_BYTES).await?;
        validate_raw_checksum(
            &bytes,
            Some(&hex::encode(leaf.digest)),
            "physical descriptor",
        )?;
        let descriptor: Descriptor = decode_json(&bytes, "physical descriptor")?;
        if descriptor.encoding_version != 1
            || descriptor.scope != self.scope
            || descriptor.role != role
            || descriptor.segment.level != ControlMvpSegmentLevel::L1
            || descriptor.segment_version.is_empty()
            || descriptor.index_version.is_empty()
            || descriptor.block.row_count == 0
            || descriptor.block.row_count != leaf.rows
            || descriptor.block.length != u64::from(leaf.bytes)
            || block_key_bounds(&descriptor.block)?.as_ref()
                != Some(&(leaf.first.clone(), leaf.last.clone()))
        {
            return Err(invariant_violation(
                "physical descriptor differs from its owning leaf",
            ));
        }
        read_cache::validate_owner(&descriptor.segment)?;
        let objects = [
            (
                self.paths.state_object(&descriptor.segment.segment_id),
                descriptor.segment.segment_size_bytes,
                descriptor.segment_version.as_str(),
            ),
            (
                self.paths.segment_index(&descriptor.segment.segment_id),
                descriptor.segment.index_size_bytes,
                descriptor.index_version.as_str(),
            ),
        ];
        self.validate_physical_versions(&objects).await?;
        let (_, index) = self.load_segment_index(&descriptor.segment).await?;
        if index
            .blocks
            .iter()
            .filter(|block| **block == descriptor.block)
            .count()
            != 1
        {
            return Err(invariant_violation(
                "descriptor block is absent from authenticated index",
            ));
        }
        let rows = self
            .load_block(&descriptor.segment, &descriptor.block)
            .await?;
        self.validate_physical_versions(&objects).await?;
        read_cache::validate_rows(&descriptor.segment, &rows)?;
        if role != Role::Kv {
            for row in &rows {
                super::bounded::validate_outbox_row(role, row, &self.scope)?;
            }
            return Ok(rows);
        }
        if rows.iter().any(|row| row.record_kind != SEGMENT_RECORD_KV)
            || rows.windows(2).any(|pair| match pair {
                [first, second] => {
                    first.logical_ordinal.checked_add(1) != Some(second.logical_ordinal)
                }
                _ => true,
            })
        {
            return Err(invariant_violation(
                "invalid physical KV kind or ordinal sequence",
            ));
        }
        Ok(rows)
    }

    async fn validate_physical_versions(&self, objects: &[(String, u64, &str)]) -> Result<()> {
        for (path, size, version) in objects {
            let meta = self
                .storage
                .head(path)
                .await?
                .ok_or_else(|| invariant_violation("selected physical object is missing"))?;
            if meta.version.is_empty() || meta.version != *version || meta.size != *size {
                return Err(invariant_violation(
                    "selected physical object version or size changed",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::super::*;
    use super::*;
    use arco_core::{MemoryBackend, storage::WritePrecondition};

    async fn fixture_with_rows(
        rows: Vec<ControlMvpSegmentRow>,
    ) -> (ControlMvpStateStore, Descriptor, directory::Leaf) {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        let (segment, index, reference) = encode_segment(
            "physical-test",
            ControlMvpSegmentLevel::L1,
            2,
            &store.scope,
            &rows,
            PRODUCTION_SEGMENT_LIMITS,
        )
        .unwrap();
        let decoded: ControlMvpSegmentIndex = decode_json(&index, "test index").unwrap();
        let segment_path = store.paths.state_object(&reference.segment_id);
        let index_path = store.paths.segment_index(&reference.segment_id);
        for (path, bytes) in [(&segment_path, segment), (&index_path, index)] {
            storage
                .put_raw(path, bytes, WritePrecondition::DoesNotExist)
                .await
                .unwrap();
        }
        let descriptor = Descriptor {
            encoding_version: 1,
            scope: store.scope.clone(),
            role: Role::Kv,
            segment: reference,
            segment_version: storage
                .head_raw(&segment_path)
                .await
                .unwrap()
                .unwrap()
                .version,
            index_version: storage
                .head_raw(&index_path)
                .await
                .unwrap()
                .unwrap()
                .version,
            block: decoded.blocks[0].clone(),
        };
        let leaf = persist(&store, &descriptor).await;
        (store, descriptor, leaf)
    }

    async fn fixture() -> (ControlMvpStateStore, Descriptor, directory::Leaf) {
        fixture_with_rows(vec![ControlMvpSegmentRow {
            record_kind: SEGMENT_RECORD_KV,
            key: b"key".to_vec(),
            value: Some(vec![42; 1024]),
            generation: 1,
            tombstone: false,
            logical_sequence: 2,
            logical_ordinal: 0,
            origin_sequence: None,
        }])
        .await
    }

    async fn persist(store: &ControlMvpStateStore, descriptor: &Descriptor) -> directory::Leaf {
        let bytes = encode_json(descriptor, "test descriptor").unwrap();
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        let path = format!(
            "{}/physical/descriptors/{}.json",
            store.paths.base_prefix(),
            hex::encode(digest)
        );
        store
            .retention
            .put_raw(&path, bytes, WritePrecondition::DoesNotExist)
            .await
            .unwrap();
        directory::Leaf {
            first: hex::decode(descriptor.block.min_key_hex.as_ref().unwrap()).unwrap(),
            last: hex::decode(descriptor.block.max_key_hex.as_ref().unwrap()).unwrap(),
            rows: descriptor.block.row_count,
            bytes: u32::try_from(descriptor.block.length).unwrap(),
            digest,
        }
    }

    #[tokio::test]
    async fn physical_descriptor_rejects_forged_bindings() {
        let mut accepted = Vec::new();
        for mutation in 0..7 {
            let (store, mut descriptor, mut leaf) = fixture().await;
            assert_eq!(
                store
                    .resolve_physical_block(Role::Kv, &leaf)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            match mutation {
                0 => descriptor.role = Role::ActiveId,
                1 => descriptor.scope.domain = "other".into(),
                2 => descriptor.segment_version.push_str("different"),
                3 => descriptor.index_version.push_str("different"),
                4 => descriptor.encoding_version = 2,
                5 => {
                    leaf.rows += 1;
                }
                6 => {
                    leaf.first = b"before".to_vec();
                }
                _ => unreachable!(),
            }
            if mutation < 5 {
                leaf = persist(&store, &descriptor).await;
            }
            if store.resolve_physical_block(Role::Kv, &leaf).await.is_ok() {
                accepted.push(mutation);
            }
        }
        assert!(accepted.is_empty(), "accepted forged bindings {accepted:?}");
    }

    #[tokio::test]
    async fn physical_descriptor_rejects_changed_raw_bytes() {
        let (store, descriptor, leaf) = fixture().await;
        let path = format!(
            "{}/physical/descriptors/{}.json",
            store.paths.base_prefix(),
            hex::encode(leaf.digest)
        );
        let mut bytes = encode_json(&descriptor, "descriptor").unwrap().to_vec();
        bytes.push(b' ');
        store
            .retention
            .put_raw(&path, bytes.into(), WritePrecondition::None)
            .await
            .unwrap();
        assert!(store.resolve_physical_block(Role::Kv, &leaf).await.is_err());
    }

    #[tokio::test]
    async fn physical_descriptor_rejects_missing_index() {
        let (store, descriptor, leaf) = fixture().await;
        store
            .retention
            .delete(&store.paths.segment_index(&descriptor.segment.segment_id))
            .await
            .unwrap();
        assert!(store.resolve_physical_block(Role::Kv, &leaf).await.is_err());
    }

    #[tokio::test]
    async fn physical_resolver_rejects_warm_changed_or_deleted_objects() {
        for delete in [false, true] {
            for index in [false, true] {
                let (store, descriptor, leaf) = fixture().await;
                let store = store
                    .with_read_cache_config(ControlMvpReadCacheConfig::default())
                    .unwrap();
                assert!(store.resolve_physical_block(Role::Kv, &leaf).await.is_ok());
                assert!(store.read_cache().unwrap().statistics().loads >= 2);
                let path = if index {
                    store.paths.segment_index(&descriptor.segment.segment_id)
                } else {
                    store.paths.state_object(&descriptor.segment.segment_id)
                };
                if delete {
                    store.retention.delete(&path).await.unwrap();
                } else {
                    let bytes = store.retention.get_raw(&path).await.unwrap();
                    store
                        .retention
                        .put_raw(&path, bytes, WritePrecondition::None)
                        .await
                        .unwrap();
                }
                assert!(store.resolve_physical_block(Role::Kv, &leaf).await.is_err());
            }
        }
    }

    #[tokio::test]
    async fn physical_descriptor_rejects_incomplete_authenticated_index() {
        let (store, mut descriptor, _) = fixture().await;
        let path = store.paths.segment_index(&descriptor.segment.segment_id);
        let bytes = store.retention.get_raw(&path).await.unwrap();
        let mut index: ControlMvpSegmentIndex = decode_json(&bytes, "test index").unwrap();
        index.blocks.clear();
        let bytes = encode_json(&index, "test index").unwrap();
        descriptor.segment.index_size_bytes = u64::try_from(bytes.len()).unwrap();
        descriptor.segment.index_checksum_sha256 = sha256_hex(&bytes);
        store
            .retention
            .put_raw(&path, bytes, WritePrecondition::None)
            .await
            .unwrap();
        descriptor.index_version = store
            .retention
            .head_raw(&path)
            .await
            .unwrap()
            .unwrap()
            .version;
        let leaf = persist(&store, &descriptor).await;
        assert!(store.resolve_physical_block(Role::Kv, &leaf).await.is_err());
    }

    #[tokio::test]
    async fn physical_resolver_handles_oversized_singleton_in_each_cache_mode() {
        for cache in [
            None,
            Some(ControlMvpReadCacheConfig::default()),
            Some(ControlMvpReadCacheConfig {
                metadata_bytes: 1024 * 1024,
                decoded_bytes: 1,
            }),
        ] {
            let row = ControlMvpSegmentRow {
                record_kind: SEGMENT_RECORD_KV,
                key: b"singleton".to_vec(),
                value: Some(vec![7; 300 * 1024]),
                generation: 1,
                tombstone: false,
                logical_sequence: 2,
                logical_ordinal: 0,
                origin_sequence: None,
            };
            let (store, _, leaf) = fixture_with_rows(vec![row.clone()]).await;
            assert!(leaf.bytes as usize > MAX_BLOCK_BYTES);
            let pressure = cache.is_some_and(|config| config.decoded_bytes == 1);
            let store = match cache {
                Some(config) => store.with_read_cache_config(config).unwrap(),
                None => store,
            };
            assert_eq!(
                store.resolve_physical_block(Role::Kv, &leaf).await.unwrap(),
                vec![row]
            );
            if pressure {
                assert!(store.read_cache().unwrap().statistics().fallbacks > 0);
            }
        }
    }
}
