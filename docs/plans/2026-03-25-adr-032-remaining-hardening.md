# ADR-032 Remaining Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Finish the remaining ADR-032 hardening work by adding orchestration orphan reconciliation/GC, moving orchestration publish onto the shared snapshot+pointer helper, deprecating the legacy catalog direct-update API, enriching orchestration artifact metadata, and triggering L0-to-base merges before read amplification grows.

**Architecture:** Keep the work centered on the orchestration compactor and manifest model. Introduce richer artifact descriptors that carry path/checksum/byte size, reuse them for both L0 and base snapshots, refactor manifest publication to `arco_core::publish::publish_snapshot_pointer_transaction`, add a small orchestration-specific reconciler/GC that protects pointer-targeted artifacts, and deprecate `Tier1Writer::update` rather than removing it outright.

**Tech Stack:** Rust (`arco-flow`, `arco-core`, `arco-catalog`), object-storage CAS semantics, repo docs under `docs/plans/`.

---

### Task 1: Add failing orchestration metadata and merge tests

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/manifest.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/service.rs`

**Step 1: Write failing artifact metadata tests**

- Add manifest/service tests proving orchestration artifacts record:
  - path
  - `checksum_sha256`
  - `byte_size`
- Add a test proving filenames are hash-suffixed/content-addressed rather than fixed `runs.parquet`.

**Step 2: Write failing L0 merge trigger tests**

- Add a service test that sets a very small `L0Limits.max_count`, runs enough compactions to cross the threshold, and expects:
  - `base_snapshot.snapshot_id` to be populated
  - `l0_deltas` to be compacted back down
  - reads to still reconstruct the same state

**Step 3: Run focused tests to verify RED**

Run: `cargo test -p arco-flow artifact_metadata -- --nocapture`

Expected: FAIL because artifacts currently store only raw paths and L0 deltas are never merged into base.

### Task 2: Add failing publish-helper and orphan-reconciler tests

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/service.rs`
- Create: `crates/arco-flow/src/orchestration/compactor/reconciler.rs`

**Step 1: Write failing publish-helper coverage**

- Add a service test that verifies shared publish-helper semantics still preserve:
  - immutable snapshot write
  - pointer CAS success
  - persisted-not-visible behavior on pointer CAS loss

**Step 2: Write failing orphan reconciliation/GC tests**

- Add reconciler tests proving:
  - orphan manifest snapshots are detected
  - orphan L0 directories are detected
  - currently pointer-targeted manifest snapshots are never marked orphaned
  - L0 files referenced by current manifest/base snapshot are never deleted

**Step 3: Run focused tests to verify RED**

Run: `cargo test -p arco-flow orphan -- --nocapture`

Expected: FAIL because no orchestration storage reconciler/GC exists yet.

### Task 3: Implement artifact metadata and base compaction

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/manifest.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/service.rs`

**Step 1: Introduce artifact descriptors**

- Replace raw table-path strings with a backward-compatible artifact struct carrying:
  - `path`
  - `checksum_sha256`
  - `byte_size`

**Step 2: Write content-addressed/hash-suffixed object names**

- Name new Parquet objects with a checksum-derived suffix before upload.

**Step 3: Trigger L0-to-base merges**

- Add a base-snapshot write path that materializes the full merged state.
- When `manifest.should_compact_l0()` is true, publish a new base snapshot and clear merged L0 deltas.

### Task 4: Implement shared publish helper and orchestration reconciler/GC

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/service.rs`
- Create: `crates/arco-flow/src/orchestration/compactor/reconciler.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/mod.rs`

**Step 1: Refactor publish path**

- Replace custom snapshot+pointer CAS logic with `publish_snapshot_pointer_transaction`.
- Keep pre-pointer succession/fencing validation in the helper callback.

**Step 2: Add orchestration reconciler/GC**

- Implement a scoped checker/collector for:
  - orphan immutable manifest snapshots
  - orphan base snapshot directories
  - orphan L0 directories
- Protect all currently referenced artifacts from deletion.

### Task 5: Deprecate legacy catalog direct-update API and add metrics/tests

**Files:**
- Modify: `crates/arco-catalog/src/tier1_writer.rs`
- Modify: `crates/arco-flow/src/metrics.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/service.rs`

**Step 1: Deprecate `Tier1Writer::update`**

- Add a `#[deprecated]` annotation with guidance toward `CatalogWriter`/sync compaction.
- Update internal tests/callers with `#[allow(deprecated)]` only where needed.

**Step 2: Add compactor hardening metrics**

- Add counters/gauges for:
  - immutable overwrite rejects
  - stale fencing rejects
  - orphan objects found/deleted

**Step 3: Add crash/orphan regression coverage**

- Add tests for:
  - crash window after L0/base write but before pointer CAS
  - crash window after immutable manifest snapshot write but before pointer CAS
  - reconciler protection of pointer-targeted artifacts

### Task 6: Verify the slice

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/service.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/manifest.rs`
- Create: `crates/arco-flow/src/orchestration/compactor/reconciler.rs`
- Modify: `crates/arco-catalog/src/tier1_writer.rs`
- Modify: `crates/arco-flow/src/metrics.rs`

**Step 1: Run compactor test suite**

Run: `cargo test -p arco-flow orchestration::compactor -- --nocapture`

Expected: PASS

**Step 2: Run catalog crate tests touching deprecation surface**

Run: `cargo test -p arco-catalog tier1_writer -- --nocapture`

Expected: PASS
