# Phase C Implementation Plan: Iceberg Reconciliation & GC

**Status:** In Progress
**Date:** 2025-12-25
**Based on:** docs/plans/2025-12-22-iceberg-rest-integration-design.md Section 4.5, 9.2

## Overview

Phase C implements the reconciliation and garbage collection components of the Iceberg REST integration:

1. Metadata-log driven reconciler for committed receipts
2. Idempotency marker GC
3. Orphan metadata cleanup
4. Schema projection to Arco catalog

## Task Breakdown

### Task 1: Define Reconciler Types and Traits

**File:** `crates/arco-iceberg/src/reconciler.rs` (new)

Create the foundational types for the reconciler:

- [x] 1.1 Create `IcebergReconciler` struct with storage backend
- [x] 1.2 Define `ReconciliationReport` for tracking results
- [x] 1.3 Define `ReconciliationResult` enum (Success, PartialSuccess, Failed)
- [x] 1.4 Add unit tests for type construction

**Test first:** Write tests for `ReconciliationReport` serialization and basic struct creation.

### Task 2: Implement Pointer Listing

**File:** `crates/arco-iceberg/src/pointer.rs`

Add the ability to list all pointers (needed by reconciler):

- [x] 2.1 Add `list_all` method to `PointerStore` trait
- [x] 2.2 Implement `list_all` for `PointerStoreImpl` using storage list
- [x] 2.3 Add integration test with memory backend

**Test first:** Write test that creates 3 pointers and verifies list_all returns them.

### Task 3: Implement Metadata Log Walker

**File:** `crates/arco-iceberg/src/reconciler.rs`

Implement the core metadata log chain traversal:

- [x] 3.1 Define `MetadataLogWalker` that reads metadata files
- [x] 3.2 Implement `walk_chain` to traverse metadata_log entries
- [x] 3.3 Add depth limit to prevent unbounded reads (default 1000)
- [x] 3.4 Include active refs/branches/tags in traversal
- [x] 3.5 Add unit tests with mock metadata files

**Test first:** Create test metadata files with known history, verify walker finds all entries.

### Task 4: Implement Committed Receipt Backfiller

**File:** `crates/arco-iceberg/src/reconciler.rs`

Backfill missing committed receipts from metadata log:

- [ ] 4.1 For each metadata log entry, compute `commit_key = SHA256(metadata_location)`
- [ ] 4.2 Check if committed receipt exists at expected path
- [ ] 4.3 If missing, create and write with `DoesNotExist` precondition
- [ ] 4.4 Track backfill statistics (found, created, skipped)
- [ ] 4.5 Add integration test verifying backfill works

**Test first:** Create table with 3 metadata entries, verify 2 missing receipts get backfilled.

### Task 5: Implement Full Reconciler

**File:** `crates/arco-iceberg/src/reconciler.rs`

Combine components into the full reconciler:

- [ ] 5.1 Implement `reconcile_table` for single table reconciliation
- [ ] 5.2 Implement `reconcile_all` to process all tables in tenant
- [ ] 5.3 Add configurable batch size for large tenants
- [ ] 5.4 Add tracing spans for observability
- [ ] 5.5 Add integration test with multiple tables

**Test first:** Test with 2 tables, each missing receipts, verify all get backfilled.

### Task 6: Idempotency Marker GC - Listing

**File:** `crates/arco-iceberg/src/gc.rs` (new)

Implement marker listing for GC:

- [ ] 6.1 Create `IdempotencyGarbageCollector` struct
- [ ] 6.2 Add `list_markers` method to find markers for a table
- [ ] 6.3 Add `list_all_markers` to find markers across all tables
- [ ] 6.4 Add unit tests for marker listing

**Test first:** Create markers with different ages, verify listing finds them all.

### Task 7: Idempotency Marker GC - Cleanup Logic

**File:** `crates/arco-iceberg/src/gc.rs`

Implement the GC cleanup rules from design doc Section 9.2:

- [ ] 7.1 For `Committed`/`Failed` markers: delete after `lifetime + 24h` grace
- [ ] 7.2 For `InProgress` markers older than `timeout + 24h`:
  - [ ] 7.2a Load current pointer
  - [ ] 7.2b If pointer.metadata_location != marker.metadata_location, safe to delete
  - [ ] 7.2c Otherwise, mark as Failed before deleting
- [ ] 7.3 Track GC statistics (deleted, skipped, errors)
- [ ] 7.4 Add integration tests for each GC rule

**Test first:** Create markers in each state with controlled timestamps, verify correct cleanup.

### Task 8: Orphan Metadata Cleanup

**File:** `crates/arco-iceberg/src/gc.rs`

Implement orphan metadata file cleanup:

- [ ] 8.1 List all files in table's metadata directory
- [ ] 8.2 Build allowlist from current pointer + metadata log + active refs
- [ ] 8.3 Identify orphans (files not in allowlist)
- [ ] 8.4 Delete orphans older than conservative window (24h default)
- [ ] 8.5 Add integration test

**Test first:** Create table with known metadata files, add orphan, verify cleanup.

### Task 9: Event Receipt GC

**File:** `crates/arco-iceberg/src/gc.rs`

Implement event receipt cleanup per design doc Section 9.2:

- [ ] 9.1 Pending receipts: delete after 24h
- [ ] 9.2 Committed receipts: retain 90 days or until compaction
- [ ] 9.3 Add integration test

**Test first:** Create receipts with different ages, verify correct cleanup.

### Task 10: Schema Projection - Type Mapping

**File:** `crates/arco-iceberg/src/schema_projection.rs` (new)

Map Iceberg types to Arco column records:

- [ ] 10.1 Define `IcebergTypeMapper` for type conversion
- [ ] 10.2 Map primitive types (int, long, string, timestamp, etc.)
- [ ] 10.3 Map complex types to string representation (struct, list, map)
- [ ] 10.4 Add comprehensive unit tests for type mapping

**Test first:** Test each Iceberg type maps to expected string representation.

### Task 11: Schema Projection - Full Projection

**File:** `crates/arco-iceberg/src/schema_projection.rs`

Project Iceberg schema to Arco ColumnRecords:

- [ ] 11.1 Implement `project_schema` to convert Schema to Vec<ColumnRecord>
- [ ] 11.2 Generate stable column IDs based on table_uuid + field_id
- [ ] 11.3 Handle nested fields (flatten or serialize to description)
- [ ] 11.4 Add integration test with real Iceberg schema

**Test first:** Create Iceberg schema with various field types, verify projection.

### Task 12: Wire Up Module Exports

**File:** `crates/arco-iceberg/src/lib.rs`

Integrate new modules:

- [ ] 12.1 Add `pub mod reconciler`
- [ ] 12.2 Add `pub mod gc`
- [ ] 12.3 Add `pub mod schema_projection`
- [ ] 12.4 Add re-exports to prelude
- [ ] 12.5 Update rustdoc

### Task 13: Add Metrics

**File:** `crates/arco-iceberg/src/metrics.rs`

Add observability for reconciliation and GC:

- [ ] 13.1 Add `reconciler_tables_processed` counter
- [ ] 13.2 Add `reconciler_receipts_backfilled` counter
- [ ] 13.3 Add `gc_markers_deleted` counter by status
- [ ] 13.4 Add `gc_orphans_deleted` counter
- [ ] 13.5 Add `gc_duration_seconds` histogram

### Task 14: Integration Tests

**File:** `crates/arco-iceberg/tests/reconciliation_tests.rs` (new)

End-to-end tests:

- [ ] 14.1 Test full reconciliation flow with memory backend
- [ ] 14.2 Test GC flow with controlled timestamps
- [ ] 14.3 Test schema projection with real Iceberg metadata
- [ ] 14.4 Test concurrent reconciliation safety

## Verification Gates

Before marking Phase C complete:

1. [ ] All tests pass (`cargo test --workspace`)
2. [ ] No clippy warnings (`cargo clippy --workspace -- -D warnings`)
3. [ ] Code reviewed against design doc Section 4.5, 9.2
4. [ ] Metrics emit correctly in test scenarios
5. [ ] Documentation complete for new public APIs

## Dependencies

- Phase B must be complete (pointer, idempotency, events, commit)
- arco-catalog parquet_util for ColumnRecord (existing)
- arco-core storage traits (existing)

## Risks and Mitigations

1. **Unbounded metadata log traversal** - Mitigated by depth limit (Task 3.3)
2. **Race conditions during GC** - Mitigated by CAS operations and conservative windows
3. **Large tenant performance** - Mitigated by batch processing (Task 5.3)
