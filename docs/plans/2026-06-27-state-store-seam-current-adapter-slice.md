# State-Store Seam Current Adapter Slice

**Status:** Phase 1A child plan.

**Scope:** Introduce the state-store interface and `arco-state-current`
capability adapter without moving production authority or changing external API
behavior.

Phase 0 contract scaffolds are committed at
`cc91d0b docs: add phase 0 contract scaffolds`. This slice starts from those
draft contracts and the pre-existing dirty roadmap/design docs listed below.

## Source Inputs

- `docs/spec/arco-storage-format-v0.md`
- `docs/spec/object-store-contract.md`
- `docs/spec/state-token-and-checkpoint-contract.md`
- `docs/spec/api-token-exposure-matrix.md`
- `docs/plans/2026-06-27-phase-0-contract-consolidation-slice.md`
- `docs/plans/2026-06-27-arco-unified-execution-roadmap.md`
- `docs/plans/2026-06-26-arco-tier1-single-authority-combined-vision.md`
- `docs/plans/2026-06-25-arco-tier1-control-store-strategy.md`
- `docs/plans/2026-06-20-olympia-inspired-arco-strategy.md`
- `docs/plans/2026-06-26-lineage-observation-projection-design.md`
- `docs/plans/2026-06-27-planner-runtime-seam-hardening-design.md`

## Current-State Audit

The current implemented Tier-1 authority path remains:

```text
ledger append
  -> synchronous compaction
  -> immutable manifest snapshot
  -> pointer CAS
```

The Phase 0 specs explicitly state that current adapters must not mint fake
tokens. Current root transactions and manifest pointers may pin selected reads,
but they are not equivalent to future retained `StateToken` semantics unless a
later ADR and implementation prove that mapping.

The current dirty worktree before this slice already contained unrelated
roadmap/design work:

```text
 M docs/README.md
 M docs/guide/src/reference/documentation-map.md
 M docs/plans/2026-06-20-olympia-inspired-arco-strategy.md
 M docs/plans/2026-06-25-arco-tier1-control-store-strategy.md
?? docs/plans/2026-06-26-arco-tier1-single-authority-combined-vision.md
?? docs/plans/2026-06-26-lineage-observation-projection-design.md
?? docs/plans/2026-06-27-arco-unified-execution-roadmap.md
?? docs/plans/2026-06-27-planner-runtime-seam-hardening-design.md
```

This slice records those docs as inputs but does not modify them.

## Owned Files

This slice owns only:

- `docs/plans/2026-06-27-state-store-seam-current-adapter-slice.md`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/lib.rs`
- `crates/arco-catalog/tests/state_store_current_adapter.rs`

## Implementation Shape

Add public seam types in `arco_catalog`:

- `ArcoStateReader`
- `ArcoStateStore`
- `ArcoStateTxn`
- `ArcoStateAdmin`
- `StateToken`
- `CheckpointToken`
- `TxnOptions`
- `CheckpointOptions`
- `VersionedValue`
- `KeyRange`
- `PredicateInputSet`
- `KvPair`
- `StateScope`
- `StateStoreCapabilities`

Add `CurrentStateStore` as the `arco-state-current` capability surface. It
must expose unsupported capabilities for retained state tokens, checkpoints,
`read_at`, transactions, range preconditions, and predicate preconditions. It
must return `CatalogError::UnsupportedOperation` for `current_state_token`,
`checkpoint`, `read_at`, and `begin_txn`.

`StateToken` and `CheckpointToken` remain opaque to external crates in this
slice. Public callers can inspect tokens returned by a future backend, but they
cannot mint token values directly.

## Non-Goals

- Do not implement an object-store txlog.
- Do not implement control-store manifests, CAS MVP, or segment formats.
- Do not move catalog DDL, credential vending, grants, lineage, or API routes to
  a new authority path.
- Do not change external API behavior.
- Do not expose public `StateToken`s from compatibility APIs.
- Do not add provider conformance tests in this slice.
- Do not mutate the pre-existing dirty roadmap/design docs listed above.
- Do not commit unless explicitly requested.

## Verification

Run:

```bash
cargo fmt --check
cargo test -p arco-catalog state_store
git diff --check
```

## Exit Gate

This slice is complete when:

- `arco_catalog` exports the state-store seam types.
- `CurrentStateStore` implements the trait-object read, admin, and store
  surfaces.
- `CurrentStateStore` capabilities explicitly mark future-only token,
  checkpoint, `read_at`, transaction, range-precondition, and
  predicate-precondition behavior unsupported.
- `current_state_token`, `checkpoint`, `read_at`, and `begin_txn` return
  `CatalogError::UnsupportedOperation`.
- No production write routing changes.
- No fake state or checkpoint tokens are minted by the current adapter.
- External crates cannot directly construct `StateToken` or `CheckpointToken`
  values.
- `StateStoreCapabilities` is inspectable through accessors, not externally
  constructible by arbitrary field values.
- Focused verification commands pass.
