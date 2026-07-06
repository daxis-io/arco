# Phase 4B Internal Read-Only Comparison Reads

**Implementation protocol:** Execute this plan task-by-task. Do not broaden
scope without updating this child plan and passing the exit gate.

**Goal:** Add a crate-private catalog inventory comparison read that returns the
current-authority catalog snapshot descriptor unchanged while attaching
structured diagnostic status from the Phase 4A shadow backend.

**Selected base:** `48822d02bce707c46c5f9dd787fb897837f6f5c0`
(`Address Phase 4A shadow replay review feedback`) from
`.worktrees/phase4a-shadow-replay`.

**Worktree:** `.worktrees/phase4b-internal-read-comparison`

**Branch:** `codex/phase4b-internal-read-comparison`

## Phase 4A Evidence

- Phase 4A worktree:
  `.worktrees/phase4a-shadow-replay`
- Phase 4A branch:
  `codex/phase4a-shadow-replay-projection-equivalence`
- Phase 4A commits:
  - `743c6f6 Add Phase 4A catalog shadow replay`
  - `48822d0 Address Phase 4A shadow replay review feedback`
- Phase 4A merge base with `origin/main`:
  `adccaa431b1436fd1f2b7c91a48ae60deb9d2387`
- Phase 4A touched:
  - `docs/plans/2026-07-05-phase-4a-shadow-replay-projection-equivalence.md`
  - `crates/arco-catalog/src/state_store.rs`
  - `crates/arco-catalog/src/state_store/shadow_replay.rs`
- Baseline in this Phase 4B worktree before source edits:
  - `cargo test -p arco-catalog shadow`: 10 matching tests passed
  - `cargo test -p arco-catalog projection`: matching projection tests passed

## Inspected Files

- `docs/plans/2026-06-20-olympia-inspired-arco-strategy.md`
- `docs/plans/2026-06-28-deterministic-state-model-slice.md`
- `docs/plans/2026-07-01-object-store-control-mvp-slice.md`
- `docs/plans/2026-07-01-prototype-promotion-fallback-gate-slice.md`
- `docs/plans/2026-07-05-phase-4a-shadow-replay-projection-equivalence.md`
- `docs/guide/src/reference/control-plane-scope.md`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/shadow_replay.rs`
- `crates/arco-catalog/src/reader.rs`
- `crates/arco-catalog/src/read_model.rs`
- `crates/arco-api/src/routes/catalogs.rs`

The Phase 4A plan references several earlier roadmap/design docs that are not
present in this Phase 4A-based worktree under those paths:

- `docs/plans/2026-06-27-arco-unified-execution-roadmap.md`
- `docs/plans/2026-06-25-arco-tier1-control-store-strategy.md`
- `docs/plans/2026-06-26-arco-tier1-single-authority-combined-vision.md`
- `docs/spec/projection-watermark-contract.md`
- `docs/spec/state-token-and-checkpoint-contract.md`

This Phase 4B child plan therefore treats the local Phase 4A plan, the local
Phase 3 state-store child plans, the current control-plane scope reference, and
the inspected implementation files as the executable source.

## Modified Files

Only these files may change:

- `docs/plans/2026-07-06-phase-4b-internal-read-only-comparison-reads.md`
- `crates/arco-catalog/src/reader.rs`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/shadow_replay.rs`
- `crates/arco-catalog/src/state_store/comparison_reads.rs`

## Scope

In:

- Add crate-private `state_store::comparison_reads`.
- Add a crate-private `CatalogReader` entry point for the internal comparison
  read.
- Add a read-only comparison adapter for the current catalog inventory
  descriptor.
- Return `current` as the current-authority descriptor unchanged.
- Return `diagnostic` as structured internal status/details from the Phase 4A
  shadow backend.
- Represent these diagnostic statuses exactly:
  - `equivalent`
  - `current_state_gap`
  - `unsupported_scope`
  - `stale_projection`
  - `bug_divergent_result`
- Ensure shadow read/compare failures never fail the current-authority read once
  the current read has succeeded.

Out:

- No public API response changes.
- No OpenAPI changes.
- No UC compatibility route changes.
- No credential-vending, authorization, governance-enforcement, or mutation
  decisions.
- No writes through the shadow backend.
- No Phase 5 writable-domain work.

## Selected Read Path

The selected read path is a crate-private catalog inventory comparison read over
the current catalog snapshot descriptor. This is low risk because it compares
read-only snapshot metadata and object-family row counts, not authorization,
credential vending, mutation behavior, UC compatibility responses, or
governance enforcement.

The public `/catalog/inventory` route remains unchanged. Phase 4B adds only an
internal adapter and crate-private `CatalogReader` entry point that can be
exercised by crate-local tests and future internal callers.

## Diagnostic Shape

`CatalogInventoryComparisonRead` contains:

- `current`: the current-authority catalog snapshot descriptor result
- `diagnostic`: the shadow comparison diagnostic

`CatalogInventoryComparisonDiagnostic` contains:

- `status`: one of the exact Phase 4B status identifiers
- `details`: ordered internal comparison details

Status precedence for included comparison domains is:

1. `bug_divergent_result`
2. `stale_projection`
3. `current_state_gap`
4. `equivalent`

Unsupported deferred Phase 4A domains remain structured details. They do not
change the top-level result for the selected catalog inventory read.

## Tests

Add focused crate-local tests for:

- equivalent catalog inventory comparison
- stale shadow watermark/projection while current result is unchanged
- unsupported deferred domains surfaced as structured diagnostics
- divergent shadow object/name-index state while current result is unchanged
- current-state gap mapping if source parent/name-index inputs are incomplete
- current descriptor/source identity mismatch surfaced as `stale_projection`
- shadow backend read/compare failure surfaced as diagnostics only after current
  read success
- crate-private `CatalogReader` comparison entry point returns the current
  descriptor unchanged

## Verification

Run:

```bash
cargo fmt --check
cargo test -p arco-catalog shadow
cargo test -p arco-catalog projection
cargo test -p arco-catalog comparison_reads
cargo test -p arco-catalog internal_catalog_inventory_shadow_comparison_returns_current_descriptor
cargo check -p arco-catalog
git diff --check
```

Skip `cargo test -p arco-api control_plane` because this slice intentionally
does not touch API or control-plane route code.

## Exit Gate

- Diff is limited to this child plan and internal `arco-catalog`
  comparison-read code.
- Production authority remains current ledger append plus synchronous
  compaction plus immutable manifest snapshot plus pointer CAS.
- Current synchronous compaction remains the write authority.
- No writes are accepted through the shadow backend.
- User-visible API behavior and response bodies do not change.
- Shadow state is not used for authorization, credential vending, mutation
  decisions, governance enforcement, or compatibility responses.
- Phase 5 writable-domain work is not started.
- All verification commands pass before commit.
- Commit locally only; do not push or open a PR.
