# Phase 6A Path-Governance Metadata Without Vending Authority

**Implementation protocol:** Add one internal storage-governance metadata domain
only. Do not move credential vending, grant enforcement, catalog DDL, public
routes, system tables, snapshots, exports, lineage/search, or projection
authority.

**Goal:** Add crate-private path-governance metadata declarations on the
control-store MVP with conflict-safe path predicates and deny-closed compiled
state readiness helpers. The new domain records metadata only; it does not vend
credentials or authorize access.

**Architecture:** Current production catalog and governance authority remains
`ledger append -> synchronous compaction -> immutable manifest snapshot ->
pointer CAS`. This phase adds a crate-private writer over
`ControlMvpStateStore` in the `path-governance-metadata` domain. Successful
declarations return retained `StateToken`s. Projection lag is diagnostic only.
Enforcement-style readiness checks use authoritative or compiled sequence
freshness and deny closed when missing or stale.

**Selected base:** `8ce83f5d544cc482cc0dfa8511a9a581733fc3a0`
(`Add Phase 5B ack-domain hardening`) from
`.worktrees/phase5b-low-risk-writable-domain-hardening`.

**Worktree:** `.worktrees/phase6a-path-governance-metadata`

**Branch:** `codex/phase6a-path-governance-metadata`

## Prerequisite Evidence

- Root checkout observed after fetch:
  `main...origin/main [ahead 16, behind 12]` with tracked deletion
  `docs/plans/2026-06-27-state-store-seam-current-adapter-slice.md`; root was
  not modified.
- `origin/main` after fetch:
  `9577097b0723932ee696780d1e432e7cff3fd222`
  (`Add Phase 4 shadow replay and internal comparison reads (#317)`).
- Phase 5A commits:
  - `9e4c38b Add Phase 5A projection outbox ack writes`
  - `0b56258 Address Phase 5A ack review feedback`
- Phase 5B commit:
  - `8ce83f5 Add Phase 5B ack-domain hardening`
- Baseline in this Phase 6A worktree before edits:
  - `cargo test -p arco-catalog projection_outbox_acks`: 13 passed.
  - `cargo test -p arco-catalog --test state_store_control_mvp`: 13 passed.
  - `cargo test -p arco-catalog --test state_store_model`: 10 passed.

## Source Context Inspected

Read from the clean Phase 6A worktree:

- `docs/plans/2026-07-06-phase-5a-first-low-risk-writable-domain.md`
- `docs/plans/2026-07-07-phase-5b-low-risk-writable-domain-hardening.md`
- `docs/guide/src/reference/control-plane-scope.md`
- `docs/plans/2026-06-20-olympia-inspired-arco-strategy.md`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/control_mvp.rs`
- `crates/arco-catalog/src/state_store/projection_outbox_acks.rs`
- `crates/arco-catalog/src/storage_governance/mod.rs`
- `crates/arco-catalog/src/storage_governance/path_normalization.rs`
- `crates/arco-catalog/src/credential_vending.rs`
- `crates/arco-catalog/tests/state_store_control_mvp.rs`
- `crates/arco-catalog/tests/state_store_model.rs`
- `crates/arco-catalog/tests/path_governance.rs`
- `crates/arco-catalog/tests/credential_vending_decisions.rs`

Read-only from the dirty root/local doc commit because these roadmap/design docs
are absent from the clean Phase 6A worktree:

- `docs/plans/2026-06-27-arco-unified-execution-roadmap.md`
- `docs/plans/2026-06-26-arco-tier1-single-authority-combined-vision.md`
- `docs/plans/2026-06-25-arco-tier1-control-store-strategy.md`
- `docs/plans/2026-06-26-lineage-observation-projection-design.md`
- `docs/plans/2026-06-27-planner-runtime-seam-hardening-design.md`

Important source constraints:

- The roadmap's Phase 6 allows storage credential, external location,
  path-governance, and workspace/metastore binding metadata, but explicitly does
  not move credential vending authority, revocation-sensitive grants, or broad
  catalog DDL.
- Phase 6 requires ancestor and descendant conflict checks, range-empty and
  range-unchanged predicates, predicate input-set revalidation, stale compiled
  state deny-closed behavior, and projection lag that does not affect
  enforcement.
- The control-store strategy requires exactly one writer path per migrated
  scope, `StateToken` return for successful writes, request-time correctness
  without object-store listing, and enforcement reads from authoritative or
  fresh-enough compiled state rather than lagging projections.
- `control-plane-scope.md` says storage credentials, external locations,
  permissions/authz state, and credential vending are still partial; broad
  governance should not be described as fully migrated.
- Existing credential vending decisions read `StorageGovernanceState` and deny
  on stale authorization or missing path authority; this slice must not wire the
  new metadata domain into vending.

## Modified Files

Only these files may change:

- `docs/plans/2026-07-08-phase-6a-path-governance-metadata-without-vending.md`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/control_mvp.rs`
- `crates/arco-catalog/src/state_store/path_governance_metadata.rs`

## Scope

In:

- Add crate-private `state_store::path_governance_metadata`.
- Add domain constant `PATH_GOVERNANCE_METADATA_DOMAIN =
  "path-governance-metadata"`.
- Add `PathGovernanceMetadataWriter::new(storage, scope)` that accepts only the
  selected domain.
- Add metadata declarations with fields:
  `declaration_id`, `authority_object_id`, `authority_object_type`, optional
  `workspace_id`, `canonical_uri`, `owner`, and `lifecycle_state = active`.
- Normalize declaration URIs with existing `GovernedPath`.
- Reject exact, ancestor, and descendant overlaps across active declarations.
- Accept non-overlapping sibling paths, for example `orders/` and
  `orders-archive/`.
- Use path-index keys that allow descendant `KeyRange` checks plus exact
  ancestor point preconditions.
- Assert ancestor absence, descendant `range_empty`, descendant
  `range_unchanged`, and predicate input-set revalidation over the same conflict
  inputs.
- Add a crate-private `ControlMvpTxn` range-witness accessor for concrete
  internal writers and tests only.
- Add token-pinned reads through `read_declaration_at` and
  `read_declaration_at_status`, with missing retained manifests reported as
  explicit token-unavailable status.
- Add diagnostic projection lag reporting.
- Add deny-closed compiled-state readiness helper that requires compiled
  sequence freshness at or above `StateToken.logical_sequence()`.

Out:

- No credential secrets, grant rows, vending provider data, public route fields,
  catalog DDL, proto, system-table, lineage/search, snapshot, or export changes.
- No credential vending authority movement.
- No grant enforcement movement.
- No user-visible compatibility behavior.
- No widening of the Phase 5 `projection-outbox-acks` writer.
- No public `ArcoStateTxn` trait change.
- No use of projection watermarks for enforcement readiness.

## Test-First Plan

Add focused crate-local tests in
`crates/arco-catalog/src/state_store/path_governance_metadata.rs` before
implementation:

- successful declaration returns a `StateToken`
- token-pinned read-after-write returns the declaration
- ancestor conflict is rejected
- descendant conflict is rejected
- non-overlapping sibling paths are accepted
- range-empty blocks a concurrent descendant insert
- range-unchanged catches stale assumptions
- missing compiled state denies closed
- stale compiled state denies closed
- projection lag does not affect compiled-state gate
- unsupported domains reject Phase 6A writes

The expected red failure is missing `path_governance_metadata` types/functions
and the missing concrete `ControlMvpTxn` range-witness accessor.

## Verification

Run before final commit:

```bash
cargo test -p arco-catalog state_store::path_governance_metadata
cargo test -p arco-catalog projection_outbox_acks
cargo test -p arco-catalog --test state_store_control_mvp
cargo test -p arco-catalog --test state_store_model
cargo test -p arco-catalog --test path_governance
cargo test -p arco-catalog --test credential_vending_decisions
cargo check -p arco-catalog
cargo fmt --check
git diff --check
```

## Exit Gate

- Only one new writable metadata domain was added:
  `path-governance-metadata`.
- Declarations return usable `StateToken`s and token-pinned reads work within
  the retained manifest window.
- Missing retained manifests are visible through an internal
  `TokenUnavailable` diagnostic.
- Exact, ancestor, and descendant path conflicts are rejected.
- Non-overlapping sibling paths are accepted.
- Range-empty, range-unchanged, and predicate input-set revalidation are
  exercised by tests.
- Compiled-state readiness denies closed on missing or stale compiled state.
- Projection lag is diagnostic only and does not affect compiled-state
  readiness.
- Catalog/governance production authority remains current ledger append,
  synchronous compaction, immutable manifest snapshot, and pointer CAS.
- Grants, credential vending, catalog DDL, system tables, public routes,
  snapshots, exports, lineage/search/projection authority, and user-visible API
  behavior are untouched.
- `git diff --name-only` contains only the four planned files.
- Commit locally only; do not push or open a PR.
