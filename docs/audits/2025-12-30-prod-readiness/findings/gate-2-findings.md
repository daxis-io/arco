# Gate 2 Findings — Storage, Manifests & Correctness

- Audit date: 2025-12-30
- Baseline commit: a906816291347849f7c59118c0f0b235ff1efcdc

This doc records evidence-backed findings for:
- 2.1 Storage layout & path canonicalization
- 2.2 Manifest model correctness
- 2.3 Parquet schema contracts
- 2.4 Tier-1 invariants (strong consistency)
- 2.5 Tier-2 invariants (eventual consistency)

Every non-trivial claim must include a code reference (`path:line`) and/or a test/evidence link.

Evidence index: `docs/audits/2025-12-30-prod-readiness/evidence/gate-2/00-evidence-index.md`

## 2.1 Storage layout & path canonicalization

- Status: PARTIAL

### Canonical path module(s)

- Candidate modules (catalog domains):
  - `crates/arco-core/src/catalog_paths.rs` (explicit single source of truth for catalog storage layout) `crates/arco-core/src/catalog_paths.rs:3`
  - `crates/arco-core/src/scoped_storage.rs` (tenant/workspace scoping + canonical helpers) `crates/arco-core/src/scoped_storage.rs:3`, `crates/arco-core/src/scoped_storage.rs:338`
  - `crates/arco-core/src/storage_keys.rs` (typed keys for ledger/state/manifest/lock/commit) `crates/arco-core/src/storage_keys.rs:15`
- Single source of truth?
  - Catalog domains only: Yes (`catalog`, `lineage`, `executions`, `search`) `crates/arco-core/src/catalog_paths.rs:3`
  - Orchestration/flow/iceberg/API deploy manifests: No, paths are scattered (see evidence)

### Paths covered

- Manifest paths per domain: `CatalogPaths::domain_manifest` `crates/arco-core/src/catalog_paths.rs:119`
- Lock paths: `CatalogPaths::domain_lock` `crates/arco-core/src/catalog_paths.rs:138`
- Snapshot paths (versioned): `CatalogPaths::snapshot_dir` / `snapshot_file` `crates/arco-core/src/catalog_paths.rs:171`
- Ledger/state paths: `CatalogPaths::ledger_event` / `state_snapshot` `crates/arco-core/src/catalog_paths.rs:187`, `crates/arco-core/src/catalog_paths.rs:236`

### Evidence

- Canonical path contracts validated in tests: `crates/arco-catalog/tests/path_contracts.rs:33`, `crates/arco-catalog/tests/path_contracts.rs:79`, `crates/arco-catalog/tests/path_contracts.rs:103`, `crates/arco-catalog/tests/path_contracts.rs:126`, `crates/arco-catalog/tests/path_contracts.rs:147`, `crates/arco-catalog/tests/path_contracts.rs:173`
- Scoped storage isolation and traversal prevention tests: `crates/arco-core/src/scoped_storage.rs:821`, `crates/arco-core/src/scoped_storage.rs:856`, `crates/arco-core/src/scoped_storage.rs:912`
- Orchestration ledger path hardcoded outside CatalogPaths: `crates/arco-flow/src/orchestration/ledger.rs:2`, `crates/arco-flow/src/orchestration/ledger.rs:82`
- Flow execution ledger namespace hardcoded: `crates/arco-flow/src/outbox.rs:75`, `crates/arco-flow/src/outbox.rs:109`
- Orchestration compactor manifest path hardcoded: `crates/arco-flow/src/orchestration/compactor/service.rs:246`
- API deploy manifest/idempotency paths are separate from catalog manifests: `crates/arco-api/src/routes/manifests.rs:271`, `crates/arco-api/src/routes/manifests.rs:275`
- Iceberg storage paths are separate namespaces: `crates/arco-iceberg/src/pointer.rs:112`, `crates/arco-iceberg/src/events.rs:40`, `crates/arco-iceberg/src/events.rs:76`, `crates/arco-iceberg/src/idempotency.rs:144`

## 2.2 Manifest model correctness

- Status: PARTIAL

### Root manifest structure

- Root manifest type/format (camelCase JSON, paths only): `crates/arco-catalog/src/manifest.rs:66`
- Stable entry point: `CatalogPaths::ROOT_MANIFEST` `crates/arco-core/src/catalog_paths.rs:112`
- Root manifest normalizes legacy manifest paths to canonical domain names: `crates/arco-catalog/src/manifest.rs:122`, `crates/arco-catalog/src/manifest.rs:133`

### Domain manifests

- Catalog domain manifest (Tier-1): snapshot pointer/version + optional `SnapshotInfo`, fencing token, commit ULID, parent hash `crates/arco-catalog/src/manifest.rs:160`
- Lineage manifest (Tier-1): snapshot pointer/version + optional `SnapshotInfo`, parent hash, fencing token, commit ULID `crates/arco-catalog/src/manifest.rs:511`
- Executions manifest (Tier-2): watermark + checkpoint, `snapshot_path` visibility gate, compaction metadata `crates/arco-catalog/src/manifest.rs:414`
- Search manifest (Tier-1): snapshot version + base path + optional SnapshotInfo + watermark/commit metadata `crates/arco-catalog/src/manifest.rs:625`
- Orchestration compactor manifest (separate system): `crates/arco-flow/src/orchestration/compactor/manifest.rs:22`

#### Snapshot file metadata

- Per-file metadata (path/checksum/size/row_count): `SnapshotFile` `crates/arco-catalog/src/manifest.rs:317`
- Snapshot-level metadata and optional tail commit pointer: `SnapshotInfo` / `TailRange` `crates/arco-catalog/src/manifest.rs:343`, `crates/arco-catalog/src/manifest.rs:370`
- Snapshot writer populates checksums/size/row_count (no tail assignment): `crates/arco-catalog/src/tier1_snapshot.rs:40`

### Atomic updates (CAS / preconditions)

- CAS primitive: `WritePrecondition::MatchesVersion` `crates/arco-core/src/storage.rs:41`
- Permit-gated CAS publish for manifests: `Publisher::publish` `crates/arco-core/src/publish.rs:78`, `crates/arco-core/src/publish.rs:92`
- Tier-1 manifest CAS (writer/compactor): `crates/arco-catalog/src/tier1_writer.rs:300`, `crates/arco-catalog/src/tier1_compactor.rs:219`
- Tier-2 executions manifest CAS: `crates/arco-catalog/src/compactor.rs:521`

### Corruption detection / integrity verification

- Manifest hash chain (raw bytes): `compute_manifest_hash` `crates/arco-catalog/src/manifest.rs:56`
- Succession validation (rollback/concurrent modification/fencing/ULID monotonicity): `crates/arco-catalog/src/manifest.rs:240`
- Commit record hash chain: `CommitRecord::compute_hash` `crates/arco-catalog/src/manifest.rs:713`
- Snapshot file checksums and totals recorded: `crates/arco-catalog/src/tier1_snapshot.rs:40`
- Offline verification of snapshot checksums and commit chain: `tools/xtask/src/main.rs:944`, `tools/xtask/src/main.rs:1023`
- Reconciler detects orphaned/missing snapshots and enforces manifest-driven visibility: `crates/arco-catalog/src/reconciler.rs:144`, `crates/arco-catalog/src/reconciler.rs:197`
- Reader allowlist for mintable paths is manifest-driven: `crates/arco-catalog/src/reader.rs:490`

### Runbook coverage

- Storage integrity verification runbook (root manifest, domain manifests, commit chain, snapshot checksums, lock freshness): `docs/runbooks/storage-integrity-verification.md:5`
- GC failure runbook includes manifest readability + snapshotPath checks: `docs/runbooks/gc-failure.md:56`
- Integrity metrics catalog (manifest integrity + snapshot artifact verification metrics): `docs/runbooks/metrics-catalog.md:182`
- Evidence index: `docs/audits/2025-12-30-prod-readiness/evidence/gate-2/08-runbooks-integrity.txt`

### Evidence

- See code references above for structure, CAS, and integrity checks.

## 2.3 Parquet schema contracts

- Status: PARTIAL

### Schema definitions

- Catalog Tier-1 schemas (namespaces/tables/columns/lineage_edges): `crates/arco-catalog/src/parquet_util.rs:99`
- Orchestration state-table schemas (runs/tasks/dep_satisfaction/timers/outbox/sensors/etc.): `crates/arco-flow/src/orchestration/compactor/parquet_util.rs:44`
- Tier-2 executions snapshot schema (materialization facts): `crates/arco-catalog/src/compactor.rs:722`

### Contract tests

- Golden schema backward-compat tests (catalog): `crates/arco-catalog/tests/schema_contracts.rs:203`
- Required fields + nullability checks (catalog): `crates/arco-catalog/tests/schema_contracts.rs:298`
- Type constraints for timestamps and IDs: `crates/arco-catalog/tests/schema_contracts.rs:375`, `crates/arco-catalog/tests/schema_contracts.rs:403`
- Roundtrip tests (catalog): `crates/arco-catalog/tests/schema_contracts.rs:435`
- Golden fixtures (catalog): `crates/arco-catalog/tests/golden_schemas/namespaces.schema.json:2` (and peers)
- Orchestration Parquet roundtrip tests (not golden fixtures): `crates/arco-flow/src/orchestration/compactor/parquet_util.rs:1880`
- Orchestration row schema + primary key tests (schedule/run/sensor/etc.): `crates/arco-flow/tests/orchestration_schema_tests.rs:16`

### Cross-engine reads

- DuckDB read path validated via browser E2E (signed URL + `read_parquet`): `crates/arco-api/tests/browser_e2e.rs:251`
- DataFusion read path: PRESENT in tests (not production feature). Evidence corrected 2025-12-31:
  - Dependency: `crates/arco-test-utils/Cargo.toml:33` declares `datafusion = "46"`
  - Test usage: `crates/arco-test-utils/tests/tier2.rs:15` imports DataFusion
  - Parquet read test: `crates/arco-test-utils/tests/tier2.rs:1248` defines `datafusion_reads_parquet_bytes`
  - See corrected evidence: `docs/audits/2025-12-30-prod-readiness/evidence/gate-2/06-datafusion-search.txt`
  - Note: DataFusion validates Parquet compatibility in tests; production server reads use direct Parquet I/O

### Evidence

- Backward compatibility rules are name-based (field map), not ordering assertions: `crates/arco-catalog/tests/schema_contracts.rs:99`

## 2.4 Tier-1 invariants (strong consistency)

- Status: PARTIAL

### Expected write sequence (API + sync compaction)

1. Acquire lock (TTL + retries): `crates/arco-catalog/src/writer.rs:47`, `crates/arco-catalog/src/tier1_writer.rs:208`, `crates/arco-core/src/lock.rs:154`
2. Read current manifest: `crates/arco-catalog/src/writer.rs:482`, `crates/arco-catalog/src/tier1_compactor.rs:219`
3. Read current snapshot: `crates/arco-catalog/src/writer.rs:483`, `crates/arco-catalog/src/tier1_compactor.rs:244`
4. Apply mutation (ledger event fold): `crates/arco-catalog/src/tier1_compactor.rs:248`
5. Write new snapshot to NEW version path (immutable, DoesNotExist): `crates/arco-catalog/src/tier1_compactor.rs:53`, `crates/arco-catalog/src/tier1_snapshot.rs:21`
6. Write commit record (hash chain): `crates/arco-catalog/src/tier1_compactor.rs:73`, `crates/arco-catalog/src/manifest.rs:666`
7. CAS publish manifest pointer (permit-gated): `crates/arco-catalog/src/tier1_compactor.rs:86`, `crates/arco-core/src/publish.rs:78`
8. Release lock: `crates/arco-catalog/src/writer.rs:519`, `crates/arco-core/src/lock.rs:427`

### Crash safety & determinism questions

- Crash after step 5 but before step 7: snapshot may be written but manifest unchanged; test asserts snapshot remains invisible and orphaned files may exist `crates/arco-catalog/tests/failure_injection.rs:220`
- Orphaned snapshot visibility before CAS: guarded by manifest publish; `snapshot_path` is visibility gate `crates/arco-catalog/src/manifest.rs:418`
- Snapshot determinism indicators:
  - Event paths are sorted before fold (stable ordering): `crates/arco-catalog/src/tier1_compactor.rs:168`
  - CAS retry requires side-effect-free update closure: `crates/arco-catalog/src/tier1_writer.rs:151`
  - Manifest succession enforces monotonic commit ULID and fencing: `crates/arco-catalog/src/manifest.rs:262`

### Evidence

- Failure injection tests for Tier-1 crash window and lock failure: `crates/arco-catalog/tests/failure_injection.rs:229`, `crates/arco-catalog/tests/failure_injection.rs:381`
- Lock TTL/expiry/fencing tests: `crates/arco-core/src/lock.rs:663`, `crates/arco-core/src/lock.rs:680`, `crates/arco-core/src/lock.rs:828`
- Reader invariants documented (no ledger reads, no listing for correctness): `crates/arco-catalog/src/reader.rs:7`
- Search compaction is implemented in Tier-1 compactor and covered by tests: `crates/arco-catalog/src/tier1_compactor.rs:137`, `crates/arco-catalog/src/tier1_compactor.rs:444`, `crates/arco-catalog/src/tier1_compactor.rs:1226`

## 2.5 Tier-2 invariants (eventual consistency)

- Status: PARTIAL

### Ledger / event log

- Append-only enforcement (catalog): `WritePrecondition::DoesNotExist` in `EventWriter` `crates/arco-catalog/src/event_writer.rs:104`, `crates/arco-catalog/src/event_writer.rs:172`
- LedgerPutStore enforces append-only semantics at the trait level: `crates/arco-core/src/storage_traits.rs:84`, `crates/arco-core/src/storage_traits.rs:94`
- Append-only enforcement (orchestration/flow): `crates/arco-flow/src/orchestration/ledger.rs:53`, `crates/arco-flow/src/outbox.rs:116`
- Idempotency keys (catalog envelope + deterministic generation): `crates/arco-core/src/catalog_event.rs:31`, `crates/arco-core/src/catalog_event.rs:87`
- Event envelope structure + versioning:
  - Catalog: `crates/arco-core/src/catalog_event.rs:16`
  - Orchestration: `crates/arco-flow/src/orchestration/events/mod.rs:45`
  - Flow execution: `crates/arco-flow/src/events.rs:115`

### Compactor behavior

- Notification fast-path expects explicit `ledger/{domain}/{event_id}.json` paths (no listing): `crates/arco-compactor/src/notification_consumer.rs:2`, `crates/arco-compactor/src/notification_consumer.rs:65`
- Sync compaction invariants (no listing, fencing validation, sole writer): `crates/arco-compactor/src/sync_compact.rs:3`, `crates/arco-compactor/src/sync_compact.rs:17`
- Invariant 6 relaxation (compactor uses listing) documented in tests: `crates/arco-test-utils/tests/tier2.rs:1204`
- Duplicate events: dedupe by `idempotency_key` and no-op duplicates `crates/arco-catalog/src/compactor.rs:440`, `crates/arco-catalog/src/event_writer.rs:181`
- Out-of-order events: upsert by primary key with `sequence_position` preference `crates/arco-catalog/src/compactor.rs:446`, `crates/arco-catalog/src/compactor.rs:93`
- Late events: detected by watermark + `last_modified`, then skipped/logged/quarantined per policy `crates/arco-catalog/src/compactor.rs:238`, `crates/arco-catalog/src/compactor.rs:554`
- Sole-writer for compacted outputs (executions): invariant documented and tested `crates/arco-catalog/src/compactor.rs:3`, `crates/arco-test-utils/tests/tier2.rs:882`
- CAS retry/backoff: exponential backoff on CAS conflicts `crates/arco-catalog/src/compactor.rs:73`, `crates/arco-catalog/src/compactor.rs:177`
- Crash mid-run behavior: snapshot write uses DoesNotExist and is reused after crash `crates/arco-catalog/src/compactor.rs:760`, `crates/arco-test-utils/tests/tier2.rs:495`

### Operational wiring gaps (affects readiness)

- Compaction loop triggers auto anti-entropy when the notification queue is empty (bounded listing per run): `crates/arco-compactor/src/main.rs`
- Notification consumer now processes Catalog/Lineage/Executions/Search via explicit paths with Tier-1 lock acquisition for DDL domains: `crates/arco-compactor/src/notification_consumer.rs`
- Anti-entropy job is implemented with cursor + listing scan; Search uses derived rebuild (no listing) via catalog/search watermark comparison: `crates/arco-compactor/src/anti_entropy.rs:337`, `crates/arco-compactor/src/anti_entropy.rs:508`

### Evidence

- Tier-2 invariants tests: `crates/arco-test-utils/tests/tier2.rs:286`, `crates/arco-test-utils/tests/tier2.rs:495`, `crates/arco-test-utils/tests/tier2.rs:590`, `crates/arco-test-utils/tests/tier2.rs:882`, `crates/arco-test-utils/tests/tier2.rs:1092`
- Orchestration CAS test: `crates/arco-flow/src/orchestration/compactor/service.rs:1255`
- Orchestration out-of-order handling tests: `crates/arco-flow/src/orchestration/compactor/fold.rs:3696`, `crates/arco-flow/src/orchestration/compactor/fold.rs:3840`, `crates/arco-flow/src/orchestration/compactor/fold.rs:3290`

## Test inventory & gaps (Gate 2)

- Lock acquisition bounded tests: `crates/arco-core/src/lock.rs:663`, `crates/arco-core/src/lock.rs:680`, `crates/arco-core/src/lock.rs:828`
- CAS conflict tests: `crates/arco-test-utils/tests/tier2.rs:590`, `crates/arco-flow/src/orchestration/compactor/service.rs:1255`
- Crash recovery consistency tests: `crates/arco-catalog/tests/failure_injection.rs:229`, `crates/arco-test-utils/tests/tier2.rs:495`
- Out-of-order handling tests: `crates/arco-test-utils/tests/tier2.rs:286`, `crates/arco-flow/src/orchestration/compactor/fold.rs:3696`, `crates/arco-flow/src/orchestration/compactor/fold.rs:3840`
- Deterministic snapshot tests (behavioral): `crates/arco-test-utils/tests/tier2.rs:796`, `crates/arco-test-utils/tests/tier2.rs:1092`

Updates:
- DataFusion read-path evidence: Server read path implemented at `/api/v1/query` (`crates/arco-api/src/routes/query.rs`); tests pass (`release_evidence/2025-12-30-catalog-mvp/ci-logs/arco-api-query-tests-2026-01-01-rerun4.txt`).
- Byte-for-byte deterministic Parquet snapshot tests: Added in `crates/arco-catalog/tests/schema_contracts.rs` (contract_*_parquet_write_is_deterministic); evidence `release_evidence/2025-12-30-catalog-mvp/ci-logs/schema-contracts-2026-01-01.txt`.
