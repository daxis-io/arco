//! Benchmarks for catalog lookup operations.
//!
//! These benchmarks measure the performance of common catalog operations
//! to ensure they meet the documented performance budgets.
//!
//! ## Performance Targets
//!
//! - Empty catalog list: < 1ms
//! - Single namespace lookup: < 5ms
//! - Table lookup (10 namespaces, 10 tables each): < 10ms
//! - Full catalog scan (100 tables): < 50ms P95

#![allow(missing_docs)]

use std::sync::Arc;

use arco_catalog::manifest::{
    CatalogDomainManifest, ExecutionsManifest, LineageManifest, RootManifest, SearchManifest,
};
use arco_catalog::parquet_util;
use arco_catalog::CatalogReader;
use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition};
use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};
use chrono::Utc;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::runtime::Runtime;

async fn setup_catalog(
    backend: Arc<dyn StorageBackend>,
    num_namespaces: usize,
    tables_per_namespace: usize,
) -> ScopedStorage {
    let storage = ScopedStorage::new(backend, "bench-tenant", "bench-workspace")
        .expect("failed to create scoped storage");

    let now_ms = Utc::now().timestamp_millis();

    let namespaces: Vec<parquet_util::NamespaceRecord> = (0..num_namespaces)
        .map(|i| parquet_util::NamespaceRecord {
            id: format!("ns-{i:04}"),
            name: format!("namespace_{i}"),
            description: Some(format!("Namespace {i} for benchmarking")),
            created_at: now_ms,
            updated_at: now_ms,
        })
        .collect();

    let tables: Vec<parquet_util::TableRecord> = namespaces
        .iter()
        .flat_map(|ns| {
            (0..tables_per_namespace).map(move |t| parquet_util::TableRecord {
                id: format!("{}-tbl-{t:04}", ns.id),
                namespace_id: ns.id.clone(),
                name: format!("table_{t}"),
                description: Some(format!("Table {t} in {}", ns.name)),
                location: Some(format!("gs://bucket/{}/{}/table_{t}", ns.id, ns.name)),
                format: Some("parquet".to_string()),
                created_at: now_ms,
                updated_at: now_ms,
            })
        })
        .collect();

    let columns: Vec<parquet_util::ColumnRecord> = tables
        .iter()
        .flat_map(|tbl| {
            (0..3).map(move |c| parquet_util::ColumnRecord {
                id: format!("{}-col-{c}", tbl.id),
                table_id: tbl.id.clone(),
                name: format!("column_{c}"),
                data_type: "STRING".to_string(),
                ordinal: c as i32,
                is_nullable: c > 0,
                description: Some(format!("Column {c} description")),
            })
        })
        .collect();

    // Write Parquet files for snapshot version 1
    let snapshot_version = 1u64;

    let ns_bytes = parquet_util::write_namespaces(&namespaces).expect("write namespaces");
    let ns_path = CatalogPaths::snapshot_file(CatalogDomain::Catalog, snapshot_version, "namespaces.parquet");
    storage
        .put_raw(&ns_path, ns_bytes.into(), WritePrecondition::None)
        .await
        .expect("put namespaces");

    let tables_bytes = parquet_util::write_tables(&tables).expect("write tables");
    let tables_path = CatalogPaths::snapshot_file(CatalogDomain::Catalog, snapshot_version, "tables.parquet");
    storage
        .put_raw(&tables_path, tables_bytes.into(), WritePrecondition::None)
        .await
        .expect("put tables");

    let columns_bytes = parquet_util::write_columns(&columns).expect("write columns");
    let columns_path = CatalogPaths::snapshot_file(CatalogDomain::Catalog, snapshot_version, "columns.parquet");
    storage
        .put_raw(&columns_path, columns_bytes.into(), WritePrecondition::None)
        .await
        .expect("put columns");

    // Write empty lineage edges
    let edges_bytes = parquet_util::write_lineage_edges(&[]).expect("write edges");
    let edges_path = CatalogPaths::snapshot_file(CatalogDomain::Lineage, snapshot_version, "edges.parquet");
    storage
        .put_raw(&edges_path, edges_bytes.into(), WritePrecondition::None)
        .await
        .expect("put edges");

    let mut catalog_manifest = CatalogDomainManifest::new();
    catalog_manifest.snapshot_version = snapshot_version;
    catalog_manifest.snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, snapshot_version);

    let mut lineage_manifest = LineageManifest::new();
    lineage_manifest.snapshot_version = snapshot_version;
    lineage_manifest.edges_path = CatalogPaths::snapshot_dir(CatalogDomain::Lineage, snapshot_version);

    let executions_manifest = ExecutionsManifest::new();
    let search_manifest = SearchManifest::new();

    let catalog_path = CatalogPaths::domain_manifest(CatalogDomain::Catalog);
    let lineage_path = CatalogPaths::domain_manifest(CatalogDomain::Lineage);
    let executions_path = CatalogPaths::domain_manifest(CatalogDomain::Executions);
    let search_path = CatalogPaths::domain_manifest(CatalogDomain::Search);

    storage
        .put_raw(
            &catalog_path,
            serde_json::to_vec(&catalog_manifest).unwrap().into(),
            WritePrecondition::None,
        )
        .await
        .expect("put catalog manifest");
    storage
        .put_raw(
            &lineage_path,
            serde_json::to_vec(&lineage_manifest).unwrap().into(),
            WritePrecondition::None,
        )
        .await
        .expect("put lineage manifest");
    storage
        .put_raw(
            &executions_path,
            serde_json::to_vec(&executions_manifest).unwrap().into(),
            WritePrecondition::None,
        )
        .await
        .expect("put executions manifest");
    storage
        .put_raw(
            &search_path,
            serde_json::to_vec(&search_manifest).unwrap().into(),
            WritePrecondition::None,
        )
        .await
        .expect("put search manifest");

    let root_manifest = RootManifest {
        version: 1,
        catalog_manifest_path: catalog_path,
        lineage_manifest_path: lineage_path,
        executions_manifest_path: executions_path,
        search_manifest_path: search_path,
        updated_at: Utc::now(),
    };
    storage
        .put_raw(
            CatalogPaths::ROOT_MANIFEST,
            serde_json::to_vec(&root_manifest).unwrap().into(),
            WritePrecondition::None,
        )
        .await
        .expect("put root manifest");

    storage
}

fn catalog_lookup_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().expect("failed to create runtime");

    let mut group = c.benchmark_group("catalog_lookup");

    // Benchmark: empty catalog list
    group.bench_function("empty_catalog_list_namespaces", |b| {
        let backend = Arc::new(MemoryBackend::new());
        let storage = rt.block_on(setup_catalog(backend.clone(), 0, 0));
        let reader = CatalogReader::new(storage);

        b.iter(|| {
            let result = rt.block_on(reader.list_namespaces());
            black_box(result)
        });
    });

    // Benchmark: list namespaces with varying sizes
    for ns_count in [1, 10, 50] {
        group.bench_with_input(
            BenchmarkId::new("list_namespaces", ns_count),
            &ns_count,
            |b, &ns_count| {
                let backend = Arc::new(MemoryBackend::new());
                let storage = rt.block_on(setup_catalog(backend.clone(), ns_count, 5));
                let reader = CatalogReader::new(storage);

                b.iter(|| {
                    let result = rt.block_on(reader.list_namespaces());
                    black_box(result)
                });
            },
        );
    }

    // Benchmark: list tables in namespace
    for tables_count in [1, 10, 50] {
        group.bench_with_input(
            BenchmarkId::new("list_tables", tables_count),
            &tables_count,
            |b, &tables_count| {
                let backend = Arc::new(MemoryBackend::new());
                let storage = rt.block_on(setup_catalog(backend.clone(), 5, tables_count));
                let reader = CatalogReader::new(storage);

                b.iter(|| {
                    let result = rt.block_on(reader.list_tables("namespace_0"));
                    black_box(result)
                });
            },
        );
    }

    // Benchmark: get single table
    group.bench_function("get_table_single", |b| {
        let backend = Arc::new(MemoryBackend::new());
        let storage = rt.block_on(setup_catalog(backend.clone(), 10, 10));
        let reader = CatalogReader::new(storage);

        b.iter(|| {
            let result = rt.block_on(reader.get_table("namespace_5", "table_5"));
            black_box(result)
        });
    });

    // Benchmark: get columns for table
    group.bench_function("get_columns", |b| {
        let backend = Arc::new(MemoryBackend::new());
        let storage = rt.block_on(setup_catalog(backend.clone(), 10, 10));
        let reader = CatalogReader::new(storage);

        b.iter(|| {
            let result = rt.block_on(reader.get_columns("ns-0005-tbl-0005"));
            black_box(result)
        });
    });

    // Benchmark: full manifest read
    group.bench_function("read_manifest", |b| {
        let backend = Arc::new(MemoryBackend::new());
        let storage = rt.block_on(setup_catalog(backend.clone(), 10, 10));
        let reader = CatalogReader::new(storage);

        b.iter(|| {
            let result = rt.block_on(reader.get_freshness(CatalogDomain::Catalog));
            black_box(result)
        });
    });

    group.finish();
}

criterion_group!(benches, catalog_lookup_benchmark);
criterion_main!(benches);
