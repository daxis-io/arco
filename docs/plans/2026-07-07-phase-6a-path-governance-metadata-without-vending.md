# Phase 6A Path-Governance Metadata Without Vending Authority

**Implementation protocol:** Add one crate-private path-governance metadata
domain only. Do not move credential vending, grants, broad catalog DDL, public
routes, proto, system tables, snapshots, exports, lineage/search, or projection
authority.

**Goal:** Add internal governed-path declaration metadata on the control-store
MVP. Successful declarations return retained `StateToken`s, token-pinned reads
prove read-after-write, and compiled-state readiness denies closed unless the
compiled authority evidence is fresh enough. This phase records metadata only;
it does not vend credentials or authorize access.

**Architecture:** Current production catalog and governance authority remains
`ledger append -> synchronous compaction -> immutable manifest snapshot ->
pointer CAS`. Phase 6A adds a crate-private writer over
`ControlMvpStateStore` in the `path-governance-metadata` domain. Projection lag
is diagnostic only. Enforcement-facing readiness must depend on authoritative
control-store tokens or fresh compiled state, never lagging projections.

**Selected base:** `8ce83f5d544cc482cc0dfa8511a9a581733fc3a0`
(`Add Phase 5B ack-domain hardening`) from
`.worktrees/phase5b-low-risk-writable-domain-hardening`.

**Worktree:** `.worktrees/phase6a-path-governance-metadata`

**Branch:** `codex/phase6a-path-governance-metadata`

## Prerequisite Evidence

- Root checkout observed before source edits:
  `main...origin/main [ahead 16, behind 12]` with tracked deletion
  `docs/plans/2026-06-27-state-store-seam-current-adapter-slice.md`; root was
  not modified.
- Phase 5A commits:
  - `9e4c38b Add Phase 5A projection outbox ack writes`
  - `0b56258 Address Phase 5A ack review feedback`
- Phase 5B commit:
  - `8ce83f5 Add Phase 5B ack-domain hardening`
- Baseline in this Phase 6A worktree before source edits:
  - `cargo test -p arco-catalog projection_outbox_acks`: 13 passed.

## Modified Files

Only these files may change:

- `docs/plans/2026-07-07-phase-6a-path-governance-metadata-without-vending.md`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/path_governance_metadata.rs`
- `crates/arco-catalog/src/storage_governance/path_normalization.rs`

## Scope

In:

- Add crate-private `state_store::path_governance_metadata`.
- Add domain constant `PATH_GOVERNANCE_METADATA_DOMAIN =
  "path-governance-metadata"`.
- Add metadata records with `schema_version = 1`, `declaration_id`, `name`,
  `canonical_uri`, `owner`, optional `workspace_id`, and active
  `lifecycle_state`.
- Add `PathGovernanceMetadataWriter::new(storage, scope)` that accepts only the
  selected domain and rejects catalog, grants, credential-vending,
  storage-credentials, external-locations, managed-roots, and
  projection-outbox-acks.
- Add `declare_path`, `compile_inputs`, `declare_path_with_inputs`, and
  `read_declaration_at`.
- Reject exact, ancestor, and descendant path conflicts after canonicalization.
- Accept siblings, different authorities or buckets, and non-overlapping paths.
- Store declaration record keys plus path-index keys.
- Use descendant `KeyRange` emptiness, exact ancestor point inputs, and
  predicate input-set revalidation to catch stale compiled assumptions.
- Add diagnostic projection lag helpers.
- Add compiled-state readiness that denies closed when compiled state is
  missing, scoped to a different state scope, or older than the required
  `StateToken.logical_sequence()`.

Out:

- No credential vending source changes.
- No grants, authz, API, proto, system-table, broad catalog DDL, or public route
  changes.
- No storage credential, external location, or managed root authority movement.
- No enforcement dependency on `system.*`, lineage, search, or lagging
  projections.
- No Phase 7 snapshots or exports.

## Test-First Plan

Add focused crate-local tests in
`crates/arco-catalog/src/state_store/path_governance_metadata.rs` before
implementation:

- successful declaration returns a `StateToken`
- token-pinned read-after-write returns the declaration
- ancestor conflict is rejected
- descendant conflict is rejected
- non-overlapping paths are accepted
- range-empty catches concurrent descendant conflicts
- range-unchanged catches stale compiled assumptions
- missing compiled state denies closed
- stale compiled state denies closed
- projection lag does not affect enforcement readiness
- credential vending does not read the new metadata
- unsupported domains reject Phase 6A writes

The expected red failure is missing `path_governance_metadata` module and
symbols.

## Verification

Run before final commit:

```bash
cargo fmt --check
cargo test -p arco-catalog path_governance_metadata
cargo test -p arco-catalog projection_outbox_acks
cargo test -p arco-catalog credential_vending
cargo test -p arco-catalog shadow
cargo test -p arco-catalog projection
cargo check -p arco-catalog
git diff --check
```

Skip `cargo test -p arco-api control_plane` and `cargo check -p arco-api`
because this slice intentionally does not touch API or control-plane files.

## Exit Gate

- Only path-governance metadata moved.
- Credential vending authority did not move.
- Grants and broad catalog DDL were not touched.
- Enforcement readiness does not read or depend on lagging projections.
- Phase 7 snapshots and exports were not started.
- All verification commands pass before commit.
- Commit locally only with message
  `feat(catalog): add path governance metadata slice`; do not push or open a
  PR.
