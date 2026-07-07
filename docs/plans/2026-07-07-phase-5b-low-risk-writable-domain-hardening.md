# Phase 5B Low-Risk Writable Domain Hardening

**Implementation protocol:** Harden only the Phase 5A selected writable domain:
`projection-outbox-acks`. Do not add a second writable domain.

**Architecture:** Current production catalog and governance authority remains
`ledger append -> synchronous compaction -> immutable manifest snapshot ->
pointer CAS`. This phase adds crate-private diagnostics and deterministic
operations evidence around the existing projection outbox acknowledgement
writer. It does not move production authority.

**Selected base:** `0b562589c1d5498523676ad6fcab6ca1d7fd6703`
(`Address Phase 5A ack review feedback`) from
`.worktrees/phase5a-projection-outbox-acks`.

**Worktree:** `.worktrees/phase5b-low-risk-writable-domain-hardening`

**Branch:** `codex/phase5b-low-risk-writable-domain-hardening`

## Phase 5A Evidence

- Root checkout observed before worktree creation:
  `main...origin/main [ahead 16, behind 11]` with tracked deletion
  `docs/plans/2026-06-27-state-store-seam-current-adapter-slice.md`; root was
  not modified.
- Phase 4B base:
  `381ebb9e96a84c43382bdc4924000a7375e43d2e`
  (`Add Phase 4B internal comparison reads`).
- Phase 5A commits:
  - `9e4c38b Add Phase 5A projection outbox ack writes`
  - `0b56258 Address Phase 5A ack review feedback`
- Selected domain: `projection-outbox-acks`.
- Phase 5A changed only:
  - `crates/arco-catalog/src/state_store.rs`
  - `crates/arco-catalog/src/state_store/projection_outbox_acks.rs`
  - `docs/plans/2026-07-06-phase-5a-first-low-risk-writable-domain.md`
- Baseline in this Phase 5B worktree before source edits:
  - `cargo test -p arco-catalog projection_outbox_acks`: 9 passed.

## Files Inspected

- `crates/arco-catalog/src/state_store/projection_outbox_acks.rs`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/control_mvp.rs`
- `crates/arco-catalog/src/error.rs`
- `crates/arco-catalog/tests/state_store_control_mvp.rs`
- `crates/arco-core/src/storage.rs`
- `crates/arco-core/src/scoped_storage.rs`
- `docs/plans/2026-07-06-phase-5a-first-low-risk-writable-domain.md`

## Modified Files

Only these files may change:

- `docs/plans/2026-07-07-phase-5b-low-risk-writable-domain-hardening.md`
- `crates/arco-catalog/src/state_store/projection_outbox_acks.rs`

## Scope

In:

- Add internal `StateToken` read status for projection ack reads.
- Treat missing retained manifests as `TokenUnavailable` diagnostics without
  changing the existing `read_ack_at` return shape.
- Add a projection watermark lag diagnostic that reports committed sequence,
  latest projected sequence, and pending sequence count separately.
- Add deterministic ack-domain evidence for warm write, warm token point-read,
  manifest-reachable replay, projection lag, compactor/projection outage
  diagnostics, and retained `StateToken` read-after-write.
- Strengthen tests that only `projection-outbox-acks` accepts writes, all
  non-selected domains reject, and `CurrentStateStore` cannot accept selected
  scope writes while the control-store writer can.

Out:

- No `arco-api` changes.
- No credential vending, grants, storage-governance metadata, broad catalog DDL,
  proto, system table, or enforcement-path changes.
- No Phase 6 governance metadata.
- No second writable domain.

## Test-First Evidence

- Red run after adding Phase 5B tests:
  `cargo test -p arco-catalog projection_outbox_acks` failed to compile because
  `ProjectionOutboxAckReadStatus`, `read_ack_at_status`,
  `ProjectionOutboxAckWatermarkLag`, and `projection_watermark_lag_for` were not
  implemented.
- Green run after implementation:
  `cargo test -p arco-catalog projection_outbox_acks`: 13 passed.

## Verification

Passed before commit:

```bash
cargo fmt --check # passed
cargo test -p arco-catalog projection_outbox_acks # 13 passed
cargo test -p arco-catalog shadow # 13 passed
cargo test -p arco-catalog projection # 15 unit + 24 integration matched tests passed
cargo test -p arco-catalog authz_decision # 3 passed
cargo test -p arco-catalog credential_vending # 7 passed
cargo check -p arco-catalog # passed
git diff --check # passed
```

Skip `cargo test -p arco-api control_plane` and `cargo check -p arco-api`
because this slice intentionally does not touch API or control-plane code.

## Exit Gate

- Phase 5B hardens exactly one writable domain: `projection-outbox-acks`.
- Writes return usable `StateToken`s and token-pinned reads work within the
  retained manifest window.
- Missing retained manifests are visible through an internal
  `TokenUnavailable` diagnostic.
- Projection freshness and authority success remain separate.
- Projection or compactor outage diagnostics do not block committed ack writes.
- Old/new write exclusivity is tested for the selected scope.
- Catalog/governance production authority remains current ledger append,
  synchronous compaction, immutable manifest snapshot, and pointer CAS.
- Grants, credential vending, broad catalog DDL, system tables, Phase 6
  governance metadata, and user-visible API behavior are untouched.
- All verification commands pass before commit.
- Commit locally only; do not push or open a PR.
