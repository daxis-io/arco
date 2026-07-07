# Phase 4A Shadow Replay And Projection Equivalence Implementation Plan

**Implementation protocol:** Execute this plan task-by-task. Do not broaden
scope without updating this child plan and passing the exit gate.

**Goal:** Import the current published catalog snapshot into an isolated shadow
control-MVP scope and report deterministic equivalence for the Phase 4A catalog
object and normalized name-index subset.

**Architecture:** Current production authority remains
`ledger append -> synchronous compaction -> immutable manifest snapshot -> pointer
CAS`. The shadow importer resolves `manifests/catalog.pointer.json`, reads the
selected `CatalogDomainManifest`, loads the published Parquet snapshot through
`tier1_state::load_catalog_state`, and writes derived shadow rows into a
separate `ControlMvpStateStore` domain named `catalog-shadow`. It never writes
current catalog pointers, manifests, or production DDL paths.

**Tech Stack:** `crates/arco-catalog`, `ControlMvpStateStore`, Tier-1 catalog
manifest/snapshot helpers, crate-local unit tests, Cargo focused verification.

---

## Source Docs

- `docs/plans/2026-06-27-arco-unified-execution-roadmap.md`
- `docs/plans/2026-06-25-arco-tier1-control-store-strategy.md`
- `docs/plans/2026-06-26-arco-tier1-single-authority-combined-vision.md`
- `docs/spec/projection-watermark-contract.md`
- `docs/spec/state-token-and-checkpoint-contract.md`
- `docs/guide/src/reference/control-plane-scope.md`

## Current-State Audit

- Root checkout observed before worktree creation: `main...origin/main [ahead 16, behind 11]` with tracked deletion `docs/plans/2026-06-27-state-store-seam-current-adapter-slice.md`; root was not modified.
- `git fetch origin` left `origin/main` at `adccaa431b1436fd1f2b7c91a48ae60deb9d2387`.
- Worktree: `.worktrees/phase4a-shadow-replay`
- Branch suffix: `phase4a-shadow-replay-projection-equivalence`
- Base: `adccaa431b1436fd1f2b7c91a48ae60deb9d2387` (`Add Phase 3 state-store prototype gates (#316)`)
- Phase 3 prerequisite baseline in this worktree:
  - `cargo test -p arco-catalog --test state_store_model`: 10 passed.
  - `cargo test -p arco-catalog --test state_store_control_mvp`: 13 passed.
  - `cargo test -p arco-catalog --test state_store_promotion_gate`: 8 passed.

## Inspected Implementation Files

- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/model.rs`
- `crates/arco-catalog/src/state_store/control_mvp.rs`
- `crates/arco-catalog/src/state_store/promotion_gate.rs`
- `crates/arco-catalog/tests/state_store_model.rs`
- `crates/arco-catalog/tests/state_store_control_mvp.rs`
- `crates/arco-catalog/tests/state_store_promotion_gate.rs`
- `crates/arco-catalog/src/state.rs`
- `crates/arco-catalog/src/tier1_state.rs`
- `crates/arco-catalog/src/tier1_snapshot.rs`
- `crates/arco-catalog/src/manifest.rs`
- `crates/arco-catalog/src/parquet_util.rs`
- `crates/arco-core/src/scoped_storage.rs`
- `crates/arco-core/src/storage_keys.rs`

## Scope

In:

- Add crate-private `state_store::shadow_replay`.
- Load current catalog pointer, manifest, and snapshot state.
- Import catalog, schema, table, and column object rows into the isolated
  `catalog-shadow` control-MVP scope.
- Import normalized catalog, schema, table, and column name indexes.
- Include catalog manifest identity and watermark metadata.
- Emit deterministic comparison reports with these difference classes:
  `current_state_gap`, `unsupported_scope`, `stale_projection`,
  `bug_divergent_result`.
- Emit explicit unsupported-scope entries for deferred Phase 4A domains.

Out:

- No public API or crate-root re-export.
- No writes through the shadow backend from user or service paths.
- No current catalog pointer, manifest, or production DDL writes.
- No Phase 4B comparison-read routing.
- No Phase 5 writable-domain work.
- No grants, ownership, storage-governance, idempotency, credential vending,
  enforcement, broad catalog DDL, event replay hashes, or full Parquet
  projection equality cutover.

## Tasks

### Task 1: Add Shadow Replay Unit Tests

**Files:**

- Modify: `crates/arco-catalog/src/state_store.rs`
- Add: `crates/arco-catalog/src/state_store/shadow_replay.rs`

**Steps:**

1. Add focused tests for equivalent import, pointer immutability,
   `current_state_gap`, `bug_divergent_result`, `stale_projection`, and
   explicit unsupported deferred domains.
2. Run `cargo test -p arco-catalog state_store::shadow_replay` and confirm the
   tests fail at Phase 4A stubs.

### Task 2: Implement Catalog Shadow Import And Comparison

**Files:**

- Modify: `crates/arco-catalog/src/state_store.rs`
- Add: `crates/arco-catalog/src/state_store/shadow_replay.rs`

**Steps:**

1. Add `pub(crate) mod shadow_replay;`.
2. Resolve and parse `manifests/catalog.pointer.json`.
3. Read the selected `CatalogDomainManifest`.
4. Load catalog state through `tier1_state::load_catalog_state`.
5. Open `ControlMvpStateStore` with domain `catalog-shadow`.
6. Write deterministic object keys, name-index keys, and manifest watermark
   metadata.
7. Compare expected rows against the shadow store and classify differences.
8. Keep all deferred domains explicit as unsupported-scope report entries.

## Verification

```bash
cargo fmt --check
cargo test -p arco-catalog --test state_store_model
cargo test -p arco-catalog --test state_store_control_mvp
cargo test -p arco-catalog --test state_store_promotion_gate
cargo test -p arco-catalog state_store::shadow_replay
cargo check -p arco-catalog
git diff --check
```

## Exit Gate

- Diff is limited to this child plan and internal Phase 4A shadow replay/report
  code.
- Production authority remains the current ledger plus synchronous compactor
  path.
- Shadow writes are isolated under the `catalog-shadow` control-MVP domain.
- Deferred domains are report entries, not silent omissions.
- No public API, routing, or cutover behavior changes.
- All verification commands pass before commit.
