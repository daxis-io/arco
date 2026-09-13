#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]
use super::super::{
    ControlMvpSegmentIndex, ControlMvpSegmentLevel, ControlMvpSegmentRow,
    PRODUCTION_SEGMENT_LIMITS, SEGMENT_RECORD_KV, decode_segment_rows, encode_segment,
};
use super::*;
use arco_core::AuthorityWritePrecondition;
use arco_core::MemoryBackend;
use arco_core::storage::WriteResult;
use async_trait::async_trait;
use std::sync::Arc;

fn directory(domain: &str) -> Directory {
    Directory::new(
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap(),
        &StateScope::new("tenant", "workspace", domain),
    )
    .unwrap()
}
fn leaf(n: u64) -> Leaf {
    Leaf {
        first: (n * 4).to_be_bytes().to_vec(),
        last: (n * 4 + 1).to_be_bytes().to_vec(),
        rows: 2,
        bytes: 100,
        digest: Sha256::digest(n.to_be_bytes()).into(),
    }
}
async fn build(dir: &Directory, count: u64) -> Root {
    let mut writer = dir.builder();
    for n in 0..count {
        writer.push(leaf(n)).await.unwrap();
    }
    writer.finish().await.unwrap()
}

#[tokio::test]
async fn streams_multiple_levels_and_paginates_without_omission() {
    let dir = directory("catalog");
    let root = build(&dir, 513).await;
    let mut cursor = None;
    let mut actual = Vec::new();
    loop {
        let page = dir
            .scan(
                &root,
                b"",
                None,
                17,
                cursor.as_ref(),
                &mut ReadBudget::default(),
            )
            .await
            .unwrap();
        actual.extend(page.leaves);
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(actual, (0..513).map(leaf).collect::<Vec<_>>());
    let reopened = dir.decode_root(&root.encode()).unwrap();
    assert_eq!(root, reopened);
    assert_eq!(build(&dir, 513).await, root);
    for n in [0, 127, 128, 256, 512] {
        let mut budget = ReadBudget::default();
        assert_eq!(
            dir.lookup(&reopened, &(n * 4_u64).to_be_bytes(), &mut budget)
                .await
                .unwrap(),
            Some(leaf(n))
        );
        assert!(budget.objects < 600);
        assert_eq!(
            dir.lookup(
                &root,
                &(n * 4 + 2).to_be_bytes(),
                &mut ReadBudget::default()
            )
            .await
            .unwrap(),
            None
        );
    }
    let page = dir
        .scan(
            &root,
            &511_u64.to_be_bytes(),
            Some(&521_u64.to_be_bytes()),
            128,
            None,
            &mut ReadBudget::default(),
        )
        .await
        .unwrap();
    assert_eq!(page.leaves, vec![leaf(128), leaf(129), leaf(130)]);
}

#[tokio::test]
async fn empty_and_long_keys_do_not_require_oversized_directory_pages() {
    let dir = directory("catalog");
    let empty = build(&dir, 0).await;
    assert!(
        dir.lookup(&empty, b"", &mut ReadBudget::default())
            .await
            .unwrap()
            .is_none()
    );
    let mut writer = dir.builder();
    let mut first = leaf(0);
    first.first.clear();
    first.last.clear();
    first.rows = 1;
    writer.push(first.clone()).await.unwrap();
    let mut large = leaf(1);
    large.first = vec![42; 100_000];
    large.last = large.first.clone();
    large.last.push(1);
    writer.push(large.clone()).await.unwrap();
    let root = writer.finish().await.unwrap();
    assert_eq!(
        dir.lookup(&root, b"", &mut ReadBudget::default())
            .await
            .unwrap(),
        Some(first)
    );
    assert_eq!(
        dir.lookup(&root, &large.first, &mut ReadBudget::default())
            .await
            .unwrap(),
        Some(large)
    );
}

#[tokio::test]
async fn rejects_duplicate_overlapping_invalid_and_overflowing_input() {
    let dir = directory("catalog");
    let mut writer = dir.builder();
    writer.push(leaf(2)).await.unwrap();
    for bad in [
        leaf(2),
        leaf(1),
        Leaf { rows: 0, ..leaf(3) },
        Leaf {
            first: vec![255],
            last: vec![0],
            ..leaf(3)
        },
        Leaf {
            first: vec![3; MAX_BLOCK_BYTES + 1],
            ..leaf(3)
        },
        Leaf {
            bytes: 0,
            ..leaf(3)
        },
    ] {
        assert!(writer.push(bad).await.is_err());
    }
    writer.push(leaf(3)).await.unwrap();
    let root = writer.finish().await.unwrap();
    assert_eq!(
        dir.scan(&root, b"", None, 128, None, &mut ReadBudget::default())
            .await
            .unwrap()
            .leaves,
        vec![leaf(2), leaf(3)]
    );
    let mut writer = dir.builder();
    writer
        .push(Leaf {
            rows: u64::MAX,
            ..leaf(0)
        })
        .await
        .unwrap();
    writer.push(leaf(1)).await.unwrap();
    assert!(writer.finish().await.is_err());
}

#[tokio::test]
async fn scope_version_cursor_and_budget_checks_fail_closed() {
    let dir = directory("catalog");
    let root = build(&dir, 3).await;
    let other = directory("other");
    assert!(other.decode_root(&root.encode()).is_err());
    assert!(
        other
            .lookup(&root, b"", &mut ReadBudget::default())
            .await
            .is_err()
    );
    let mut bytes = root.encode();
    bytes[7] ^= 1;
    assert!(dir.decode_root(&bytes).is_err());
    let mut bytes = root.encode();
    bytes.push(0);
    assert!(dir.decode_root(&bytes).is_err());
    let cursor = dir
        .scan(&root, b"", None, 1, None, &mut ReadBudget::default())
        .await
        .unwrap()
        .next
        .unwrap();
    assert!(
        dir.scan(
            &root,
            b"changed",
            None,
            1,
            Some(&cursor),
            &mut ReadBudget::default()
        )
        .await
        .is_err()
    );
    let different = build(&dir, 4).await;
    assert!(
        dir.scan(
            &different,
            b"",
            None,
            1,
            Some(&cursor),
            &mut ReadBudget::default()
        )
        .await
        .is_err()
    );
    let mut budget = ReadBudget::new(0, 0).unwrap();
    assert!(dir.lookup(&root, b"", &mut budget).await.is_err());
    assert_eq!((budget.objects, budget.bytes), (0, 0));
    assert!(ReadBudget::new(4097, MAX_SEGMENT_BYTES).is_err());
    assert!(
        dir.scan(&root, b"", None, 129, None, &mut ReadBudget::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn existing_arrow_block_references_preserve_bytes_and_key_ranges() {
    let dir = directory("catalog");
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let rows = (0_u64..3)
        .map(|n| ControlMvpSegmentRow {
            record_kind: SEGMENT_RECORD_KV,
            key: n.to_be_bytes().to_vec(),
            value: Some(vec![u8::try_from(n).unwrap()]),
            generation: n + 1,
            tombstone: false,
            logical_sequence: 3,
            logical_ordinal: n,
            origin_sequence: None,
        })
        .collect::<Vec<_>>();
    let (bytes, index, reference) = encode_segment(
        "directory-vector",
        ControlMvpSegmentLevel::L1,
        3,
        &scope,
        &rows,
        PRODUCTION_SEGMENT_LIMITS,
    )
    .unwrap();
    assert_eq!(
        decode_segment_rows(&bytes, &index, &reference, &scope).unwrap(),
        rows
    );
    let index: ControlMvpSegmentIndex = serde_json::from_slice(&index).unwrap();
    let block = &index.blocks[0];
    let entry = Leaf {
        first: rows[0].key.clone(),
        last: rows[2].key.clone(),
        rows: 3,
        bytes: block.length.try_into().unwrap(),
        digest: hex::decode(&block.checksum_sha256)
            .unwrap()
            .try_into()
            .unwrap(),
    };
    let mut builder = dir.builder();
    builder.push(entry.clone()).await.unwrap();
    let root = builder.finish().await.unwrap();
    assert_eq!(
        dir.lookup(&root, &rows[1].key, &mut ReadBudget::default())
            .await
            .unwrap(),
        Some(entry)
    );
}

#[tokio::test]
async fn empty_range_has_no_candidate_blocks() {
    let dir = directory("catalog");
    let root = build(&dir, 2).await;
    let key = 1_u64.to_be_bytes();
    let page = dir
        .scan(&root, &key, Some(&key), 1, None, &mut ReadBudget::default())
        .await
        .unwrap();
    assert!(page.leaves.is_empty());
    assert!(page.next.is_none());
}

async fn forged_page(dir: &Directory, root: &Root, bytes: Vec<u8>) -> Root {
    let mut forged = root.clone();
    forged.node.digest = dir.digest(b"pages", &bytes);
    forged.node.bytes = u32::try_from(bytes.len()).unwrap();
    put_immutable_matching(
        &dir.storage,
        &dir.path("pages", &forged.node.digest),
        bytes.into(),
        "test forged page",
    )
    .await
    .unwrap();
    forged
}

#[tokio::test]
async fn malformed_authenticated_pages_reject_counts_order_omission_and_summaries() {
    let dir = directory("catalog");
    let root = build(&dir, 2).await;
    let raw = dir
        .storage
        .get(&dir.path("pages", &root.node.digest))
        .await
        .unwrap();
    let mut cases = Vec::new();
    let mut bytes = raw.to_vec();
    bytes[41..43].copy_from_slice(&u16::MAX.to_le_bytes());
    cases.push(bytes);
    let mut bytes = raw.to_vec();
    bytes[40] = 9;
    cases.push(bytes);
    let mut bytes = raw.to_vec();
    bytes[HEADER_BYTES] = 1;
    cases.push(bytes);
    let mut bytes = raw.to_vec();
    bytes[HEADER_BYTES + 5..HEADER_BYTES + 13].fill(0);
    cases.push(bytes);
    let mut bytes = raw.to_vec();
    bytes[HEADER_BYTES + 5..HEADER_BYTES + 13].fill(255);
    cases.push(bytes);
    let mut bytes = raw.to_vec();
    bytes[HEADER_BYTES + 1..HEADER_BYTES + 5].fill(255);
    cases.push(bytes);
    let mut bytes = raw.to_vec();
    bytes[HEADER_BYTES + NODE_BYTES..]
        .copy_from_slice(&raw[HEADER_BYTES..HEADER_BYTES + NODE_BYTES]);
    cases.push(bytes);
    let mut bytes = raw.to_vec();
    bytes.truncate(HEADER_BYTES + NODE_BYTES);
    bytes[41..43].copy_from_slice(&1_u16.to_le_bytes());
    cases.push(bytes);
    let mut bytes = raw.to_vec();
    bytes.extend_from_slice(b"suffix");
    cases.push(bytes);
    for bytes in cases {
        let forged = forged_page(&dir, &root, bytes).await;
        assert!(
            dir.scan(&forged, b"", None, 128, None, &mut ReadBudget::default())
                .await
                .is_err()
        );
    }
    let mut forged = root.clone();
    forged.node.rows += 1;
    assert!(
        dir.lookup(&forged, b"", &mut ReadBudget::default())
            .await
            .is_err()
    );
    forged.node.depth = 9;
    let mut budget = ReadBudget::default();
    assert!(dir.lookup(&forged, b"", &mut budget).await.is_err());
    assert_eq!(budget.objects, 0);
}

#[tokio::test]
async fn warm_handle_rejects_changed_deleted_and_oversized_objects() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let dir = Directory::new(
        storage.clone(),
        &StateScope::new("tenant", "workspace", "catalog"),
    )
    .unwrap();
    let mut writer = dir.builder();
    writer.push(leaf(0)).await.unwrap();
    writer
        .push(Leaf {
            first: vec![42; 65],
            last: vec![43; 65],
            bytes: 200,
            ..leaf(1)
        })
        .await
        .unwrap();
    let root = writer.finish().await.unwrap();
    assert!(
        dir.lookup(&root, &0_u64.to_be_bytes(), &mut ReadBudget::default())
            .await
            .unwrap()
            .is_some()
    );
    for path in [
        dir.path("pages", &root.node.digest),
        dir.path("keys", &root.node.last.digest),
    ] {
        let original = dir.storage.get(&path).await.unwrap();
        let mut altered = original.to_vec();
        altered[0] ^= 1;
        for replacement in [Bytes::from(altered), Bytes::from(vec![0; PAGE_LIMIT + 1])] {
            let version = dir.storage.head(&path).await.unwrap().unwrap().version;
            dir.storage
                .put(
                    &path,
                    replacement,
                    AuthorityWritePrecondition::MatchesVersion(version),
                )
                .await
                .unwrap();
            assert!(
                dir.lookup(&root, &0_u64.to_be_bytes(), &mut ReadBudget::default())
                    .await
                    .is_err()
            );
        }
        storage.delete(&path).await.unwrap();
        assert!(
            dir.lookup(&root, &0_u64.to_be_bytes(), &mut ReadBudget::default())
                .await
                .is_err()
        );
        put_immutable_matching(&dir.storage, &path, original, "test repair")
            .await
            .unwrap();
        assert!(
            dir.lookup(&root, &0_u64.to_be_bytes(), &mut ReadBudget::default())
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test]
async fn narrow_reads_and_hostile_decoders_have_bounded_allocations() {
    let dir = directory("catalog");
    let root = build(&dir, 16_385).await;
    assert_eq!(root.node.depth, 3);
    let mut result = None;
    let mut budget = ReadBudget::default();
    let allocations = allocation_counter::measure(|| {
        result = Some(futures::executor::block_on(dir.lookup(
            &root,
            &4_u64.to_be_bytes(),
            &mut budget,
        )));
    });
    assert_eq!(result.unwrap().unwrap(), Some(leaf(1)));
    let point_peak = allocations.bytes_max;
    let point_objects = budget.objects;
    assert!(allocations.bytes_max < 512 * 1024, "{allocations:?}");
    assert_eq!(budget.objects, usize::from(root.node.depth));
    let mut bytes = vec![0; HEADER_BYTES];
    bytes[..8].copy_from_slice(PAGE_MAGIC);
    bytes[8..40].copy_from_slice(&dir.scope);
    bytes[40] = 1;
    bytes[41..43].copy_from_slice(&u16::MAX.to_le_bytes());
    let allocations = allocation_counter::measure(|| {
        assert!(decode_page(&bytes, &dir.scope, 1).is_err());
    });
    assert!(allocations.bytes_max < 1024, "{allocations:?}");
    let mut budget = ReadBudget::new(4096, root.node.bytes as usize).unwrap();
    assert!(dir.lookup(&root, b"", &mut budget).await.is_err());
    assert_eq!((budget.objects, budget.bytes), (0, 0));
    println!(
        "{}",
        serde_json::json!({"case":"directory-three-level-bounds","leaves":16385,"point_peak_bytes":point_peak,"point_objects":point_objects,"hostile_decoder_peak_bytes":allocations.bytes_max})
    );
}

#[tokio::test]
async fn maximum_key_fences_pack_into_readable_pages() {
    let dir = directory("catalog");
    let mut writer = dir.builder();
    let mut expected = None;
    for n in 0_u64..128 {
        let mut first = vec![0; MAX_BLOCK_BYTES];
        first[..8].copy_from_slice(&(2 * n).to_be_bytes());
        let mut last = first.clone();
        last[..8].copy_from_slice(&(2 * n + 1).to_be_bytes());
        let entry = Leaf {
            first,
            last,
            bytes: u32::try_from(MAX_BLOCK_BYTES).unwrap(),
            ..leaf(n)
        };
        if n == 64 {
            expected = Some(entry.clone());
        }
        writer.push(entry).await.unwrap();
    }
    let root = writer.finish().await.unwrap();
    let expected = expected.unwrap();
    let mut budget = ReadBudget::default();
    assert_eq!(
        dir.lookup(&root, &expected.first, &mut budget)
            .await
            .unwrap(),
        Some(expected)
    );
    println!(
        "{}",
        serde_json::json!({"case":"maximum-fence-point","objects":budget.objects,"probe_bytes":budget.bytes})
    );
}

#[tokio::test]
async fn exact_eight_level_capacity_can_finish() {
    let dir = directory("catalog");
    let mut writer = dir.builder();
    // Simulate the already-validated depth-seven subtrees rather than allocating128^8 leaves.
    for n in 0..128 {
        let entry = leaf(n);
        let node = Node {
            depth: 7,
            bytes: u32::try_from(HEADER_BYTES).unwrap(),
            rows: entry.rows,
            first: dir.write_key(&entry.first).await.unwrap(),
            last: dir.write_key(&entry.last).await.unwrap(),
            digest: entry.digest,
        };
        writer.push_node(node).await.unwrap();
    }
    let root = writer.finish().await.unwrap();
    assert_eq!(root.node.depth, 8);
    assert_eq!(root.node.rows, 256);
    // A real eight-level chain remains readable under the default hard budget.
    let mut root = build(&dir, 1).await;
    for depth in 2..=8 {
        root.node = dir.write_page(depth, &[root.node]).await.unwrap();
    }
    assert_eq!(
        dir.lookup(&root, &0_u64.to_be_bytes(), &mut ReadBudget::default())
            .await
            .unwrap(),
        Some(leaf(0))
    );
}

#[tokio::test]
async fn canonical_wire_vector_and_hash_domain_framing() {
    let dir = directory("catalog");
    let root = build(&dir, 1).await;
    // Independently generated from the frozen binary specification using Python struct/hashlib.
    assert_eq!(
        hex::encode(root.encode()),
        "4152434f524f5431c8ef5da2efd4880551bb4b99ac381598b72f03f62eb9d3ded7e792923a0d2fb10120010000020000000000000012f4555a104abac26774b285f7240b19d43d17cd99a65320994667341d7c0d170800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000b03a8657d6a4e4a4109f3db08ca3fe9e7b5df3a585b7bb661e65e7324bddf315080000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000043a069de87ca62ad811f251dc9164e7fa64580a480182a17d1d139b8f6f22d2a"
    );
    let make = |tenant, workspace| {
        Directory::new(
            ScopedStorage::new(Arc::new(MemoryBackend::new()), tenant, workspace).unwrap(),
            &StateScope::new(tenant, workspace, "catalog"),
        )
        .unwrap()
    };
    assert_ne!(make("ab", "c").scope, make("a", "bc").scope);
    assert_ne!(dir.digest(b"keys", b""), dir.digest(b"pages", b""));
    assert_ne!(dir.digest(b"ke", b"ys"), dir.digest(b"keys", b""));
    for scope in [
        &StateScope::new("other", "workspace", "catalog"),
        &StateScope::new("tenant", "other", "catalog"),
    ] {
        assert!(
            Directory::new(
                ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap(),
                scope
            )
            .is_err()
        );
    }
    let mut writer = dir.builder();
    assert!(
        writer
            .push(Leaf {
                bytes: u32::try_from(MAX_BLOCK_BYTES).unwrap() + 1,
                ..leaf(0)
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn exact_half_open_boundaries_and_fail_only_budget() {
    let dir = directory("catalog");
    let root = build(&dir, 129).await;
    let page = dir
        .scan(
            &root,
            &1_u64.to_be_bytes(),
            Some(&4_u64.to_be_bytes()),
            128,
            None,
            &mut ReadBudget::default(),
        )
        .await
        .unwrap();
    assert_eq!(page.leaves, vec![leaf(0)]); // last==lower included, first==upper excluded.
    let cursor = dir
        .scan(
            &root,
            b"",
            Some(&100_u64.to_be_bytes()),
            1,
            None,
            &mut ReadBudget::default(),
        )
        .await
        .unwrap()
        .next
        .unwrap();
    assert!(
        dir.scan(
            &root,
            b"",
            Some(&101_u64.to_be_bytes()),
            1,
            Some(&cursor),
            &mut ReadBudget::default()
        )
        .await
        .is_err()
    );
    assert!(
        dir.scan(&root, b"z", Some(b"a"), 1, None, &mut ReadBudget::default())
            .await
            .is_err()
    );
    let mut budget = ReadBudget::new(2, MAX_SEGMENT_BYTES).unwrap();
    assert!(
        dir.scan(&root, b"", None, 128, None, &mut budget)
            .await
            .is_err()
    );
    assert!(budget.objects > 0); // Charged work is retained, but no partial page is exposed.
    let page = dir
        .scan(
            &root,
            b"",
            None,
            17,
            None,
            &mut ReadBudget::new(2, MAX_SEGMENT_BYTES).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page.leaves, (0..17).map(leaf).collect::<Vec<_>>());
    assert!(page.next.is_some());
}

struct PutFaultBackend {
    inner: MemoryBackend,
    lost: std::sync::atomic::AtomicBool,
    puts: std::sync::atomic::AtomicUsize,
    gate: Option<tokio::sync::Barrier>,
}
#[async_trait]
impl arco_core::storage::StorageBackend for PutFaultBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }
    async fn get_range(&self, path: &str, range: std::ops::Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }
    async fn put(
        &self,
        path: &str,
        bytes: Bytes,
        condition: arco_core::storage::WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        use std::sync::atomic::Ordering::SeqCst;
        let ordinal = self.puts.fetch_add(1, SeqCst);
        if ordinal < 2 {
            if let Some(gate) = &self.gate {
                gate.wait().await;
            }
        }
        let result = self.inner.put(path, bytes, condition).await?;
        if self.lost.swap(false, SeqCst) {
            return Err(arco_core::Error::storage(
                "injected lost directory PUT response after persistence",
            ));
        }
        Ok(result)
    }
    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }
    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<arco_core::storage::ObjectMeta>> {
        self.inner.list(prefix).await
    }
    async fn head(&self, path: &str) -> arco_core::Result<Option<arco_core::storage::ObjectMeta>> {
        self.inner.head(path).await
    }
    async fn signed_url(
        &self,
        path: &str,
        expiry: std::time::Duration,
    ) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[tokio::test]
async fn rebuild_reconciles_lost_put_and_competing_identical_writers() {
    for lost in [true, false] {
        let backend = Arc::new(PutFaultBackend {
            inner: MemoryBackend::new(),
            lost: std::sync::atomic::AtomicBool::new(lost),
            puts: std::sync::atomic::AtomicUsize::new(0),
            gate: (!lost).then(|| tokio::sync::Barrier::new(2)),
        });
        let dir = Directory::new(
            ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap(),
            &StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        if lost {
            let entry = Leaf {
                first: vec![3; 65],
                last: vec![4; 65],
                bytes: 200,
                ..leaf(0)
            };
            let mut writer = dir.builder();
            assert!(writer.push(entry.clone()).await.is_err());
            assert!(writer.push(leaf(1)).await.is_err());
            assert!(writer.finish().await.is_err());
            assert_eq!(
                dir.storage
                    .get(&dir.path("keys", &dir.digest(b"keys", &entry.first)))
                    .await
                    .unwrap()
                    .as_ref(),
                entry.first
            );
            let mut rebuilt = dir.builder();
            rebuilt.push(entry.clone()).await.unwrap();
            let root = rebuilt.finish().await.unwrap();
            assert_eq!(
                dir.lookup(&root, &entry.first, &mut ReadBudget::default())
                    .await
                    .unwrap(),
                Some(entry)
            );
        } else {
            let (a, b) = tokio::join!(build(&dir, 129), build(&dir, 129));
            assert_eq!(a, b);
        }
    }
}

#[tokio::test]
async fn immutable_collision_and_oversized_fence_page_fail_closed() {
    let dir = directory("catalog");
    let root = build(&dir, 1).await;
    let path = dir.path("pages", &root.node.digest);
    let version = dir.storage.head(&path).await.unwrap().unwrap().version;
    dir.storage
        .put(
            &path,
            Bytes::from_static(b"corrupt!"),
            AuthorityWritePrecondition::MatchesVersion(version),
        )
        .await
        .unwrap();
    let mut writer = dir.builder();
    writer.push(leaf(0)).await.unwrap();
    assert!(writer.finish().await.is_err());
    let node = Node {
        depth: 0,
        bytes: u32::try_from(MAX_BLOCK_BYTES).unwrap(),
        rows: 1,
        first: KeyRef {
            digest: [1; 32],
            bytes: u32::try_from(MAX_BLOCK_BYTES).unwrap(),
            inline: [0; INLINE_KEY_BYTES],
        },
        last: KeyRef {
            digest: [2; 32],
            bytes: u32::try_from(MAX_BLOCK_BYTES).unwrap(),
            inline: [0; INLINE_KEY_BYTES],
        },
        digest: [3; 32],
    };
    let over_budget = vec![node; 128];
    assert!(dir.write_page(1, &over_budget).await.is_err());
    let mut raw = Vec::new();
    raw.extend_from_slice(PAGE_MAGIC);
    raw.extend_from_slice(&dir.scope);
    raw.push(1);
    raw.extend_from_slice(&128_u16.to_le_bytes());
    for _ in 0..128 {
        encode_node(&mut raw, &node);
    }
    let forged = forged_page(&dir, &root, raw).await;
    let mut budget = ReadBudget::default();
    let error = dir.lookup(&forged, b"", &mut budget).await.unwrap_err();
    assert!(error.to_string().contains("fence-probe budget"));
    assert_eq!(budget.objects, 1);
}

#[tokio::test]
async fn existing_oversized_singleton_block_remains_representable() {
    let dir = directory("catalog");
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let row = ControlMvpSegmentRow {
        record_kind: SEGMENT_RECORD_KV,
        key: b"key".to_vec(),
        value: Some(vec![1; 300 * 1024]),
        generation: 1,
        tombstone: false,
        logical_sequence: 1,
        logical_ordinal: 0,
        origin_sequence: None,
    };
    let (bytes, index, reference) = encode_segment(
        "oversized-directory-vector",
        ControlMvpSegmentLevel::L1,
        1,
        &scope,
        std::slice::from_ref(&row),
        PRODUCTION_SEGMENT_LIMITS,
    )
    .unwrap();
    assert_eq!(
        decode_segment_rows(&bytes, &index, &reference, &scope).unwrap(),
        vec![row.clone()]
    );
    let index: ControlMvpSegmentIndex = serde_json::from_slice(&index).unwrap();
    let block = &index.blocks[0];
    assert!(block.length > MAX_BLOCK_BYTES as u64);
    let entry = Leaf {
        first: row.key.clone(),
        last: row.key,
        rows: 1,
        bytes: block.length.try_into().unwrap(),
        digest: hex::decode(&block.checksum_sha256)
            .unwrap()
            .try_into()
            .unwrap(),
    };
    let mut writer = dir.builder();
    writer.push(entry.clone()).await.unwrap();
    let root = writer.finish().await.unwrap();
    assert_eq!(
        dir.lookup(&root, b"key", &mut ReadBudget::default())
            .await
            .unwrap(),
        Some(entry)
    );
}

#[test]
fn legacy_l1_index_rejects_keys_larger_than_directory_fence_limit() {
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let row = ControlMvpSegmentRow {
        record_kind: SEGMENT_RECORD_KV,
        key: vec![1; MAX_BLOCK_BYTES + 1],
        value: Some(vec![1]),
        generation: 1,
        tombstone: false,
        logical_sequence: 1,
        logical_ordinal: 0,
        origin_sequence: None,
    };
    let error = encode_segment(
        "large-key-directory-vector",
        ControlMvpSegmentLevel::L1,
        1,
        &scope,
        &[row],
        PRODUCTION_SEGMENT_LIMITS,
    )
    .unwrap_err();
    assert!(error.to_string().contains("index"), "{error}");
    println!("legacy oversized-key rejection: {error}");
}

#[tokio::test]
async fn inline_fences_have_one_canonical_encoding_at_all_boundaries() {
    let dir = directory("catalog");
    for bytes in [vec![], vec![0], vec![1; 64], vec![1; 65], vec![1, 0]] {
        let key = dir.write_key(&bytes).await.unwrap();
        let mut budget = ReadBudget::default();
        assert_eq!(
            dir.read_key(key, &mut budget).await.unwrap().as_ref(),
            bytes
        );
        assert_eq!(budget.objects, usize::from(bytes.len() > 64));
        assert_eq!(
            dir.storage
                .head(&dir.path("keys", &key.digest))
                .await
                .unwrap()
                .is_some(),
            bytes.len() > 64
        );
        let mut bad = key;
        bad.digest[0] ^= 1;
        assert!(dir.read_key(bad, &mut ReadBudget::default()).await.is_err());
        if bytes.len() < 64 || bytes.len() > 64 {
            let mut bad = key;
            bad.inline[if bytes.len() < 64 { bytes.len() } else { 0 }] = 1;
            assert!(dir.read_key(bad, &mut ReadBudget::default()).await.is_err());
        }
    }
    let root = build(&dir, 1).await;
    let raw = dir
        .storage
        .get(&dir.path("pages", &root.node.digest))
        .await
        .unwrap();
    let mut malformed = raw.to_vec();
    malformed[HEADER_BYTES + 13 + 32 + 4 + 8] = 1; // Nonzero short-key padding.
    let forged = forged_page(&dir, &root, malformed).await;
    assert!(
        dir.lookup(&forged, b"", &mut ReadBudget::default())
            .await
            .is_err()
    );
}
