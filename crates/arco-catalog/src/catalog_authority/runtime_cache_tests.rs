use super::*;
use crate::ArcoStateAdmin;
use arco_core::MemoryBackend;

fn registry() -> CatalogAuthorityBindings {
    CatalogAuthorityBindings::new([CatalogAuthorityBinding::control_v1("tenant", "workspace")])
        .unwrap()
}

fn bound(storage: ScopedStorage, bindings: &CatalogAuthorityBindings) -> ControlCatalogAuthority {
    let CatalogAuthority::ControlV1(authority) = CatalogAuthority::control_v1_bound(
        storage,
        StateScope::new("tenant", "workspace", "catalog"),
        bindings,
    )
    .unwrap() else {
        panic!("control authority")
    };
    *authority
}

#[tokio::test]
async fn registry_clones_retain_cache_across_independent_requests() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let bindings = registry();
    let first = bound(storage.clone(), &bindings);
    // A demand is recorded even when there is no resident payload yet.
    let mut tx = first
        .store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.put(b"key", Bytes::from_static(b"value")).await.unwrap();
    tx.commit().await.unwrap();
    let reader = first
        .store
        .read_at(first.store.current_state_token().await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        reader.get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"value"))
    );
    let cache = first.store.read_cache().unwrap();
    assert!(cache.statistics().demands > 0);
    let demands = cache.statistics().demands;
    drop(reader);
    drop(first);
    let second = bound(storage, &bindings.clone());
    assert_eq!(
        second.store.read_cache().unwrap().statistics().demands,
        demands
    );
}

#[tokio::test]
async fn fixed_shares_exclude_legacy_and_leave_remainders_unused() {
    let bindings = CatalogAuthorityBindings::new([
        CatalogAuthorityBinding::control_v1("tenant", "workspace"),
        CatalogAuthorityBinding::control_v1("tenant", "two"),
        CatalogAuthorityBinding::control_v1("tenant", "three"),
        CatalogAuthorityBinding::legacy("tenant", "legacy"),
    ])
    .unwrap();
    let capacity = crate::ControlMvpReadCacheConfig::default();
    assert_eq!(
        bindings.read_cache_config.metadata_bytes,
        capacity.metadata_bytes / 3
    );
    assert_eq!(
        bindings.read_cache_config.decoded_bytes,
        capacity.decoded_bytes / 3
    );
    let mut metadata = 0;
    let mut decoded = 0;
    let backend = Arc::new(MemoryBackend::new());
    for workspace in ["workspace", "two", "three"] {
        let CatalogAuthority::ControlV1(authority) = CatalogAuthority::control_v1_bound(
            ScopedStorage::new(backend.clone(), "tenant", workspace).unwrap(),
            StateScope::new("tenant", workspace, "catalog"),
            &bindings,
        )
        .unwrap() else {
            panic!("control")
        };
        let mut txn = authority
            .store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        txn.put(b"same-key", Bytes::copy_from_slice(workspace.as_bytes()))
            .await
            .unwrap();
        txn.commit().await.unwrap();
        let reader = authority
            .store
            .read_at(authority.store.current_state_token().await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            reader.get(b"same-key").await.unwrap(),
            Some(Bytes::copy_from_slice(workspace.as_bytes()))
        );
        let stats = authority.store.read_cache().unwrap().statistics();
        metadata += stats.metadata.capacity_bytes;
        decoded += stats.decoded.capacity_bytes;
    }
    assert_eq!(metadata, capacity.metadata_bytes / 3 * 3);
    assert_eq!(decoded, capacity.decoded_bytes / 3 * 3);
}

#[tokio::test]
async fn backend_and_registry_isolation_never_replace_first_handle() {
    let bindings = registry();
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let first = bound(storage.clone(), &bindings);
    let other = bound(
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap(),
        &bindings,
    );
    assert!(other.store.read_cache().is_none());
    let mut tx = first
        .store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    tx.put(b"key", Bytes::from_static(b"value")).await.unwrap();
    tx.commit().await.unwrap();
    let reader = first
        .store
        .read_at(first.store.current_state_token().await.unwrap())
        .await
        .unwrap();
    reader.get(b"key").await.unwrap();
    let demands = first.store.read_cache().unwrap().statistics().demands;
    assert!(demands > 0);
    assert_eq!(
        bound(storage.clone(), &bindings)
            .store
            .read_cache()
            .unwrap()
            .statistics()
            .demands,
        demands
    );
    assert_eq!(
        bound(storage, &registry())
            .store
            .read_cache()
            .unwrap()
            .statistics()
            .demands,
        0
    );
    assert!(matches!(
        other.store.current_state_token().await,
        Err(CatalogError::NotFound { .. })
    ));
}

#[test]
fn simultaneous_construction_retains_one_slot() {
    let bindings = registry();
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
    let barrier = std::sync::Barrier::new(32);
    let stores = std::thread::scope(|threads| {
        let handles: Vec<_> = (0..32)
            .map(|_| {
                threads.spawn(|| {
                    barrier.wait();
                    bound(storage.clone(), &bindings).store
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    // Arc identity of the shared slot is part of the registry clone contract.
    assert!(Arc::ptr_eq(&bindings.exact, &bindings.clone().exact));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut tx = stores[0]
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        tx.put(b"key", Bytes::from_static(b"value")).await.unwrap();
        tx.commit().await.unwrap();
        let reader = stores[0]
            .read_at(stores[0].current_state_token().await.unwrap())
            .await
            .unwrap();
        reader.get(b"key").await.unwrap();
    });
    let demands = stores[0].read_cache().unwrap().statistics().demands;
    assert!(demands > 0);
    assert!(
        stores
            .iter()
            .all(|s| s.read_cache().unwrap().statistics().demands == demands)
    );
}

#[test]
fn insufficient_shares_fall_back_but_constructor_errors_propagate() {
    let bindings = CatalogAuthorityBindings::new(
        (0..4096).map(|i| CatalogAuthorityBinding::control_v1("tenant", format!("workspace{i}"))),
    )
    .unwrap();
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace0").unwrap();
    for _ in 0..2 {
        let CatalogAuthority::ControlV1(authority) = CatalogAuthority::control_v1_bound(
            storage.clone(),
            StateScope::new("tenant", "workspace0", "catalog"),
            &bindings,
        )
        .unwrap() else {
            panic!("control")
        };
        assert!(authority.store.read_cache().is_none());
    }
    for scope in [
        StateScope::new("tenant", "workspace0", "wrong"),
        StateScope::new("tenant", "workspace1", "catalog"),
    ] {
        assert!(matches!(
            CatalogAuthority::control_v1_bound(storage.clone(), scope, &bindings),
            Err(CatalogError::Validation { .. })
        ));
    }
}
