# Phase 5A First Low-Risk Writable Domain

**Implementation protocol:** Execute this plan task-by-task. Do not broaden
scope without updating this child plan and passing the exit gate.

**Goal:** Add exactly one internal low-risk writable control-store domain:
projection outbox acknowledgements.

**Architecture:** Current production authority remains `ledger append ->
synchronous compaction -> immutable manifest snapshot -> pointer CAS`. Phase 5A
adds a crate-private writer over `ControlMvpStateStore` in the
`projection-outbox-acks` domain. A successful acknowledgement commit returns a
usable `StateToken`, and projection freshness is exposed only as diagnostics.

**PR-prep base:** `9577097b0723932ee696780d1e432e7cff3fd222`
(`Add Phase 4 shadow replay and internal comparison reads (#317)`) from
current `origin/main`.

**Original implementation base:** `381ebb9e96a84c43382bdc4924000a7375e43d2e`
(`Add Phase 4B internal comparison reads`), before Phase 4 landed on
`origin/main`.

**PR-prep worktree:** `.worktrees/phase5-low-risk-writable-domains`

**PR-prep branch:** `codex/phase5-low-risk-writable-domains`

**Original implementation worktree:** `.worktrees/phase5a-projection-outbox-acks`

**Original implementation branch:** `codex/phase5a-projection-outbox-acks`

## Prerequisite Evidence

- Root checkout observed before PR-prep worktree creation:
  `main...origin/main [ahead 16, behind 12]` with tracked deletion
  `docs/plans/2026-06-27-state-store-seam-current-adapter-slice.md`; root was
  not modified.
- Phase 3 prerequisite is present in `origin/main`:
  `adccaa4 Add Phase 3 state-store prototype gates (#316)`.
- Phase 4 prerequisite is present in `origin/main`:
  `9577097 Add Phase 4 shadow replay and internal comparison reads (#317)`.
- Original Phase 5A execution was developed on the pre-merge Phase 4B commit
  `381ebb9`, then cherry-picked onto current `origin/main` for this clean PR
  branch.

## Modified Files

Only these files may change:

- `docs/plans/2026-07-06-phase-5a-first-low-risk-writable-domain.md`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/projection_outbox_acks.rs`

## Scope

In:

- Add crate-private `state_store::projection_outbox_acks`.
- Add domain constant `PROJECTION_OUTBOX_ACK_DOMAIN =
  "projection-outbox-acks"`.
- Add `ProjectionOutboxAckWriter::new(storage, scope)` that accepts only the
  selected domain.
- Add `acknowledge(ProjectionOutboxAckWrite)` that asserts the ack key is
  absent for new acknowledgements, returns an existing acknowledgement for
  duplicate retries, writes through `ControlMvpStateStore`, and returns a usable
  `StateToken`.
- Add `read_ack_at(StateToken, consumer_id, record_id)` for token-pinned
  read-after-write proof.
- Add `projection_freshness_for(&StateToken, latest_projected_sequence)` with
  `current`, `stale_projection`, and `projection_unavailable` statuses.
- Test that `CurrentStateStore` remains capability-only and rejects writes for
  the selected scope while the control MVP writer accepts it.

Out:

- No `arco-api` changes.
- No authorization, grants, credential-vending, system-table, proto, or public
  API changes.
- No broad catalog DDL writes.
- No movement of catalog or governance production authority.
- No enforcement path depends on the new backend.

## Tests

Add focused crate-local tests for:

- successful ack write returns a `StateToken`
- duplicate ack write returns an existing committed token without advancing the
  logical sequence
- token-pinned read-after-write returns the committed ack
- projection freshness is separate from authority commit
- stale projection watermark status is visible
- unavailable projection watermark status is visible
- projection outage does not block committed writes
- `CurrentStateStore` rejects the selected scope while control store accepts it
- unsupported domains reject Phase 5A writes
- authz and credential-vending regressions are covered by their focused tests

## Verification

Run:

```bash
cargo fmt --check
cargo test -p arco-catalog projection_outbox_acks
cargo test -p arco-catalog shadow
cargo test -p arco-catalog projection
cargo test -p arco-catalog authz_decision
cargo test -p arco-catalog credential_vending
cargo check -p arco-catalog
git diff --check
```

Skip `cargo test -p arco-api control_plane` and `cargo check -p arco-api`
because this slice intentionally does not touch API or control-plane code.

## Exit Gate

- Only one writable domain was added: `projection-outbox-acks`.
- Writes return usable `StateToken`s and token-pinned reads work.
- Projection watermark freshness is exposed separately from authority success.
- Compactor or projection outage does not block committed writes for this
  domain.
- No enforcement path depends on the new backend.
- Old/new write exclusivity is tested for the selected scope.
- Catalog/governance production authority remains current ledger append,
  synchronous compaction, immutable manifest snapshot, and pointer CAS.
- Grants, credential vending, broad catalog DDL, system tables, and
  user-visible API behavior are untouched.
- All verification commands pass before commit.
- Commit locally only; do not push or open a PR.
