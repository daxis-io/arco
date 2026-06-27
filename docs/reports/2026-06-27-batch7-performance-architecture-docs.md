# Batch 7 Performance, Architecture, And Docs Evidence

Date: 2026-06-27
Base: `origin/main` at `b485bacc3ea13dea73a5180982f8984876417bf5`
Worktree: `.worktrees/batch7-performance-architecture-docs`

## Scope

Batch 7 issues:

- #221: Row-level query redaction for access-backed system tables
- #279: Catalog read amplification
- #280: Tier-1 DDL write amplification
- #290: God-module decomposition
- #291: API-to-flow contracts-only state access
- #292: Replay/watermark protocol consolidation
- #293: Legacy scheduler, leader, and quota audit
- #294: Scoped-storage legacy helper docs
- #295: Crate map and compactor deployment docs

Live GitHub issue state was checked before work and refreshed after rebasing
onto current `origin/main`. All Batch 7 issues remained open with no comments
at the refresh. PR #306, covering the previous Batch 6 work, is now merged into
`origin/main` as `b485bacc3ea13dea73a5180982f8984876417bf5`, so this worktree
has been rebased onto that commit.

## Status By Issue

| Issue | Local status | Evidence |
|-------|--------------|----------|
| #279 | Measured-only | Current reader behavior is better than the deep-review hypothesis for same-reader hot reads, but fresh readers still pay pointer/manifest/snapshot I/O. |
| #280 | Measured-only | Steady-state DDL still rewrites five catalog snapshot Parquet files and growing commit/snapshot bytes per DDL. |
| #291 | Still open / guardrail-only | Added an `xtask flow-boundary-check` guard that registers in `ci-parity` and allows only documented existing `arco-api` compactor-internal exceptions; contracts-only state access remains future work. |
| #292 | Still open / documented-only | Documented current replay/watermark protocols in ADR-039, including invariants and explicit non-goals. No consolidation implementation or consolidation plan landed in this slice. |
| #290 | Measured-only | Recorded module sizes, top-level item counts, and move-only extraction plan. No moves performed in this batch. |
| #293 | Fixed locally | Audited uses, marked scheduler-era leader election superseded for shipped runtime behavior, and marked scheduler-era tenant quotas deprecated because no replacement quota semantics are defined. No modules removed. |
| #221 | Blocked/deferred | Current `origin/main` explicitly defers `system.access.*` tables until authoritative projections exist. |
| #294 | Fixed locally | Updated stale scoped-storage deprecation comment: v0.2.x retains legacy helpers for source compatibility. |
| #295 | Fixed locally | Updated `crates/README.md` from the actual workspace crate list and Cloud Run Terraform deployment shape. |

## #279 Read Amplification Baseline

Focused command:

```sh
cargo test -p arco-catalog --test protocol_invariants catalog_read_amplification_current_baseline -- --nocapture
```

Raw output:

```text
batch7 #279 cold_get_table_in_schema: get_count=6 get_bytes=12436 parquet_get_count=4 parquet_get_bytes=10743 get_range_count=0 get_range_bytes=0 head_count=0
batch7 #279 hot_same_reader_get_table_in_schema: get_count=2 get_bytes=1693 parquet_get_count=0 parquet_get_bytes=0 get_range_count=0 get_range_bytes=0 head_count=0
batch7 #279 fresh_reader_get_table_in_schema: get_count=6 get_bytes=12436 parquet_get_count=4 parquet_get_bytes=10743 get_range_count=0 get_range_bytes=0 head_count=0
```

Interpretation:

- The current `CatalogReader` has a useful same-reader read-model cache: the
  second lookup avoided all Parquet GETs.
- A fresh reader still repeats the cold-read cost. That matters for request
  paths that instantiate a new reader per API request.
- The original "all four domains" claim from the deep review is not true for
  this measured catalog lookup on current `origin/main`.

Target budget:

- Preserve hot same-reader catalog lookups at `parquet_get_count=0`.
- Preserve fresh point lookup at no more than the current pointer/manifest plus
  four snapshot GETs unless a redesign explicitly changes the model.
- Future work should target cross-request reuse and conditional metadata
  validation rather than redesigning from the older unmeasured assumption.

## #280 Write Amplification Baseline

Focused command:

```sh
cargo test -p arco-catalog --test protocol_invariants tier1_catalog_write_amplification_current_baseline -- --nocapture
```

Raw output:

```text
batch7 #280 tier1 DDL write amplification samples: [
  { ddl_number: 1, put_count: 19, put_bytes: 24587, catalog_snapshot_put_count: 10, catalog_snapshot_put_bytes: 19616, commits_put_bytes: 7271 },
  { ddl_number: 2, put_count: 11, put_bytes: 13870, catalog_snapshot_put_count: 5, catalog_snapshot_put_bytes: 11128, commits_put_bytes: 3915 },
  { ddl_number: 3, put_count: 11, put_bytes: 14109, catalog_snapshot_put_count: 5, catalog_snapshot_put_bytes: 11367, commits_put_bytes: 4069 },
  { ddl_number: 4, put_count: 11, put_bytes: 14345, catalog_snapshot_put_count: 5, catalog_snapshot_put_bytes: 11602, commits_put_bytes: 4223 },
  { ddl_number: 5, put_count: 11, put_bytes: 14576, catalog_snapshot_put_count: 5, catalog_snapshot_put_bytes: 11833, commits_put_bytes: 4371 },
  { ddl_number: 6, put_count: 11, put_bytes: 14802, catalog_snapshot_put_count: 5, catalog_snapshot_put_bytes: 12059, commits_put_bytes: 4518 },
  { ddl_number: 7, put_count: 11, put_bytes: 15034, catalog_snapshot_put_count: 5, catalog_snapshot_put_bytes: 12291, commits_put_bytes: 4671 },
  { ddl_number: 8, put_count: 11, put_bytes: 15301, catalog_snapshot_put_count: 5, catalog_snapshot_put_bytes: 12558, commits_put_bytes: 4855 },
  { ddl_number: 9, put_count: 11, put_bytes: 15559, catalog_snapshot_put_count: 5, catalog_snapshot_put_bytes: 12806, commits_put_bytes: 5004 },
  { ddl_number: 10, put_count: 11, put_bytes: 15786, catalog_snapshot_put_count: 5, catalog_snapshot_put_bytes: 13032, commits_put_bytes: 5151 },
  { ddl_number: 11, put_count: 11, put_bytes: 16012, catalog_snapshot_put_count: 5, catalog_snapshot_put_bytes: 13258, commits_put_bytes: 5298 },
  { ddl_number: 12, put_count: 11, put_bytes: 16238, catalog_snapshot_put_count: 5, catalog_snapshot_put_bytes: 13484, commits_put_bytes: 5445 }
]
```

Interpretation:

- The first measured DDL includes bootstrap/default-catalog work and is not a
  steady-state sample.
- After bootstrap, each DDL wrote 11 objects and rewrote five catalog snapshot
  Parquet files.
- Both catalog snapshot bytes and `commits.parquet` bytes grow as history grows.

Target budget:

- Near-term regression guard: keep steady-state catalog DDL at exactly five
  catalog snapshot Parquet PUTs until a segmented-snapshot design intentionally
  changes the format.
- Destination budget for a future redesign: bounded per-DDL write bytes that do
  not rewrite all snapshot tables and the full commit history for each DDL.

## Architecture Decisions And Non-goals

- #291 adds a guardrail and inventory, not a contracts-only state API rewrite.
  Existing `arco-api` compactor-internal imports remain as documented
  exceptions until contract modules cover all current read shapes.
- #292 documents the current protocols and shared invariants. It remains open
  for actual consolidation or for a dedicated consolidation plan with
  behavior-equivalence tests and migration rules.
- #290 keeps decomposition as move-only follow-up work. This batch did not mix
  file moves with behavior changes.
- #293 does not delete public modules. The audited state is that
  `legacy-scheduler` gates the old scheduler/store/runner path, while
  `leader` and `quota` remain public compatibility/test-development surface.
  ADR-014 is now marked superseded for shipped runtime behavior. ADR-016 is now
  marked deprecated because shipped automation does not wire the scheduler-era
  `QuotaManager`, but no replacement tenant-quota semantics are defined.
- #221 remains blocked on authoritative access-backed system tables. Query
  redaction should not be designed against non-existent `system.access.*`
  projections.

## #290 Module Measurements

Line counts:

```text
9013 crates/arco-api/src/routes/orchestration.rs
6501 crates/arco-catalog/src/writer.rs
7169 crates/arco-flow/src/orchestration/compactor/fold.rs
3718 crates/arco-flow/src/orchestration/compactor/service.rs
```

Top-level item counts:

```text
220 crates/arco-api/src/routes/orchestration.rs
40  crates/arco-catalog/src/writer.rs
69  crates/arco-flow/src/orchestration/compactor/fold.rs
39  crates/arco-flow/src/orchestration/compactor/service.rs
```

Move-only extraction plan:

- `crates/arco-api/src/routes/orchestration.rs`: extract request/response
  types, schedule/sensor/backfill/partition route groups, cursor/pagination
  helpers, partition key normalization, and mapping helpers before changing any
  handler behavior.
- `crates/arco-catalog/src/writer.rs`: extract writer DTOs, UC property
  normalization, location formatting, and idempotency/transaction helpers in
  separate move-only commits.
- `crates/arco-flow/src/orchestration/compactor/fold.rs`: extract row schemas,
  merge helpers, and schedule/sensor/backfill fold arms after row-schema moves
  prove behavior parity.

## #293 Inventory

Current code state:

- `arco-flow` default features do not include `legacy-scheduler`.
- `scheduler`, `runner`, and `store` modules are gated by
  `#[cfg(feature = "legacy-scheduler")]`.
- `leader` and `quota` modules are public and exported through the prelude.
- Production flow binaries live under `crates/arco-flow/src/bin/` and use the
  ADR-020/024/041 orchestration services instead of the legacy in-process
  scheduler.
- Search did not find `arco-api` or integration-test consumers of
  `LeaderElector`, `QuotaManager`, or `Scheduler`; usage is confined to module
  examples, module tests, and `legacy-scheduler` integration tests.

ADR changes:

- ADR-014 status: `Superseded`
- ADR-016 status: `Deprecated`
- ADR index updated for both statuses.

## Files Changed By Issue

| Issue | Files |
|-------|-------|
| #279 | `crates/arco-core/tests/support/spy_backend.rs`, `crates/arco-core/tests/storage_backend_conformance.rs`, `crates/arco-catalog/tests/protocol_invariants.rs` |
| #280 | `crates/arco-core/tests/support/spy_backend.rs`, `crates/arco-core/tests/storage_backend_conformance.rs`, `crates/arco-catalog/tests/protocol_invariants.rs` |
| #291 | `tools/xtask/src/main.rs`, `tools/xtask/tests/ci_parity.rs` |
| #292 | `docs/adr/adr-039-catalog-consistency-model.md` |
| #290 | `docs/reports/2026-06-27-batch7-performance-architecture-docs.md` |
| #293 | `docs/adr/adr-014-leader-election.md`, `docs/adr/adr-016-tenant-quotas.md`, `docs/adr/README.md`, `docs/reports/2026-06-27-batch7-performance-architecture-docs.md` |
| #221 | `docs/reports/2026-06-27-batch7-performance-architecture-docs.md` |
| #294 | `crates/arco-core/src/scoped_storage.rs` |
| #295 | `crates/README.md` |

## Verification

Commands run:

| Command | Result |
|---------|--------|
| `git fetch origin main` | Passed. |
| `git rebase --autostash origin/main` | Passed; worktree rebased onto `b485bacc3ea13dea73a5180982f8984876417bf5`. |
| `gh pr view 306 --json number,state,mergedAt,mergeCommit,headRefName,baseRefName,title` | Passed; PR #306 is merged at `2026-06-27T14:03:01Z` with merge commit `b485bacc3ea13dea73a5180982f8984876417bf5`. |
| `for issue in 221 279 280 290 291 292 293 294 295; do gh issue view "$issue" --json number,state,comments,title; done` | Passed after retrying outside the sandbox; all issues were open with no comments. |
| `cargo test -p arco-core --test storage_backend_conformance spy_backend_records_failed_get_attempts -- --nocapture` | Failed before the `SpyBackend` fix, proving failed inner GET attempts were not recorded. |
| `cargo test -p arco-core --test storage_backend_conformance spy_backend_records_failed_get_attempts -- --nocapture` | Passed after the `SpyBackend` fix. |
| `cargo test -p arco-core --test storage_backend_conformance spy_backend_records_failed -- --nocapture` | Passed: 2 passed, 0 failed. |
| `cargo fmt --all` | Passed. |
| `cargo fmt --all --check` | Passed. |
| `cargo test -p arco-catalog --test protocol_invariants catalog_read_amplification_current_baseline -- --nocapture` | Passed; raw #279 baseline captured above. |
| `cargo test -p arco-catalog --test protocol_invariants tier1_catalog_write_amplification_current_baseline -- --nocapture` | Passed; raw #280 baseline captured above. |
| `cargo test -p arco-catalog --test metastore_replay_publication storage_governance_projection_cache_reuses_hot_state -- --nocapture` | Passed. |
| `cargo test -p arco-catalog --test protocol_invariants` | Passed: 26 passed, 0 failed. |
| `cargo test -p arco-core --test storage_backend_conformance` | Passed: 6 passed, 0 failed, 2 ignored cloud-credential tests. |
| `cargo test -p arco-core scoped_storage -- --nocapture` | Passed: 24 matching tests passed across unit/integration test binaries, 0 failed. |
| `cargo test -p xtask flow_boundary_check_is_registered_and_passes_current_allowlist -- --nocapture` | Passed. |
| `cargo xtask flow-boundary-check` | Passed with 5 documented compactor exceptions. |
| `cargo xtask adr-check` | Passed. |
| `cargo xtask repo-hygiene-check` | Passed. |
| `cargo test -p xtask` | Passed: full xtask suite passed. |
| `git diff --check` | Passed. |

Skipped by explicit Batch 7 scope: live GCP and deployed-UAT commands.

## Paste-ready GitHub Issue Comments

### #279

Measured current `origin/main` before redesigning catalog-reader caching.

Command:

```sh
cargo test -p arco-catalog --test protocol_invariants catalog_read_amplification_current_baseline -- --nocapture
```

Results:

```text
cold_get_table_in_schema: get_count=6 get_bytes=12436 parquet_get_count=4 parquet_get_bytes=10743 get_range_count=0 get_range_bytes=0 head_count=0
hot_same_reader_get_table_in_schema: get_count=2 get_bytes=1693 parquet_get_count=0 parquet_get_bytes=0 get_range_count=0 get_range_bytes=0 head_count=0
fresh_reader_get_table_in_schema: get_count=6 get_bytes=12436 parquet_get_count=4 parquet_get_bytes=10743 get_range_count=0 get_range_bytes=0 head_count=0
```

Current behavior is not the older "all four domains on every request" shape for
this measured point lookup. Same-reader hot reads already avoid Parquet GETs.
The remaining issue is fresh-reader/cross-request amplification: a new reader
repeats six GETs and about 12 KiB for the same lookup. Suggested next target is
cross-request metadata/read-model reuse or conditional validation, while
preserving the measured hot-reader budget of zero Parquet GETs.

### #280

Measured current Tier-1 catalog DDL write amplification before designing
segmented snapshots.

Command:

```sh
cargo test -p arco-catalog --test protocol_invariants tier1_catalog_write_amplification_current_baseline -- --nocapture
```

Results:

```text
ddl 1:  put_count=19 put_bytes=24587 catalog_snapshot_put_count=10 catalog_snapshot_put_bytes=19616 commits_put_bytes=7271
ddl 2:  put_count=11 put_bytes=13870 catalog_snapshot_put_count=5  catalog_snapshot_put_bytes=11128 commits_put_bytes=3915
ddl 12: put_count=11 put_bytes=16238 catalog_snapshot_put_count=5  catalog_snapshot_put_bytes=13484 commits_put_bytes=5445
```

The first sample includes bootstrap/default-catalog work. Steady state still
rewrites five catalog snapshot Parquet files per DDL, and bytes grow with DDL
history. Near-term guard: keep steady-state writes pinned at five snapshot
Parquet PUTs until a segmented-snapshot design intentionally changes the
format. Future target: bounded per-DDL write bytes rather than full snapshot
and full commit-history rewrites.

### #291

Added a local architecture guard for the current API-to-flow boundary:

```sh
cargo xtask flow-boundary-check
```

The check scans `crates/arco-api/src` for direct
`arco_flow::orchestration::compactor` imports and allows only the five current,
documented exceptions:

- `orchestration_compaction.rs`
- `routes/manifests.rs`
- `routes/orchestration.rs`
- `routes/tasks.rs`
- `system_tables.rs`

This is a guardrail, not a full fix for contracts-only state access. It
preserves runtime behavior and makes future compactor-internal coupling visible
in CI. The remaining work is to add contract/read-model APIs for the existing
exception shapes and then shrink the allowlist.

### #292

Documented the current replay and watermark protocols in ADR-039 rather than
changing runtime behavior in this batch.

The new ADR section covers Tier-1 catalog sync compaction, Tier-2 execution
compaction, orchestration flow compaction, and metastore projection replay. It
also records shared invariants: replay must be deterministic, compaction must
not regress visible watermarks, and older inputs can only be skipped, folded
idempotently, or quarantined by an explicit policy.

This is a documented-only update, not a consolidation fix. The issue should
remain open until a follow-up lands either the consolidation itself or a concrete
consolidation plan with behavior-equivalence tests and migration rules.

### #290

Measured the module sizes before doing any decomposition:

```text
9013 crates/arco-api/src/routes/orchestration.rs
6501 crates/arco-catalog/src/writer.rs
7169 crates/arco-flow/src/orchestration/compactor/fold.rs
3718 crates/arco-flow/src/orchestration/compactor/service.rs
```

No file moves were performed in this batch. Recommended next step is move-only
PRs, separated from behavior changes:

- split `routes/orchestration.rs` by API surface and helper groups;
- split `writer.rs` DTOs, normalization, and transaction helpers;
- split compactor fold row schemas and merge helpers before extracting fold
  arms.

### #293

Audited current use before deleting anything.

Findings:

- `legacy-scheduler` is not enabled by default.
- `scheduler`, `runner`, and `store` are feature-gated.
- `leader` and `quota` remain public and exported through `arco-flow::prelude`.
- Production flow binaries use the ADR-020/024/041 orchestration services, not
  the legacy in-process scheduler path.
- Search found no `arco-api` or integration-test consumers of
  `LeaderElector`, `QuotaManager`, or `Scheduler`; usage is in examples,
  module tests, and feature-gated legacy scheduler tests.

Local doc fix: ADR-014 is now marked `Superseded` for shipped runtime behavior.
ADR-016 is now marked `Deprecated` because shipped automation does not wire the
scheduler-era quota path, but no replacement tenant-quota semantics are defined.
No public modules were removed in this batch.

### #221

Deferred row-level query redaction because current `origin/main` still lacks the
required access-backed system table dependency.

Evidence:

- `crates/arco-api/src/system_tables.rs` explicitly keeps
  `system.access.{grants,compiled_permissions,audit,auth_denies,credential_mints}`
  out of the allowlist until authoritative projections exist.
- `crates/arco-uc/src/support.rs` still describes compiled permissions as
  injected application state, not manifest-published grant projections with
  writer-backed mutations.

No redaction behavior was changed. The next actionable slice is to land
authoritative access projections and system-table ACL tests first.

### #294

Fixed the stale scoped-storage legacy-helper comment. The code still includes
and tests the deprecated helpers in v0.2.x, so the comment now says v0.2.x
retains them for source compatibility and that new code should use canonical
path helpers instead.

### #295

Updated `crates/README.md` to match the current workspace and deployment shape.

Changes:

- Added the missing workspace crates: `arco-delta`, `arco-cli`,
  `arco-iceberg`, `arco-uc`, `arco-test-utils`, and
  `arco-integration-tests`.
- Changed the compactor description from Cloud Function entrypoint to Cloud Run
  service.
- Added deployment notes for the catalog compactor Cloud Run service and the
  separate flow control-plane Cloud Run services.
