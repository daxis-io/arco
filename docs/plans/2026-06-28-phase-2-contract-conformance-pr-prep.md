# Phase 2 Contract Conformance PR Prep

**Goal:** Prepare Phase 2 of the unified execution roadmap as a narrow,
stacked PR-ready branch that documents storage, reader, retention,
root-ownership, IAM, and provider-CAS contracts without changing production
write behavior.

**Selected base:** the remote Phase 0/1 roadmap-seams branch.

**Why stacked:** `origin/main` at preflight contains only the older Olympia
strategy doc from the requested roadmap family. The Phase 0/1 contract
scaffolds and seams are not in `origin/main`, so this branch is stacked on the
Phase 0/1 branch instead of mixing unrelated prerequisite work directly into a
main-based Phase 2 PR.

## Source Documents Re-Read

The unified roadmap and most original design docs are present only in the dirty
root checkout, not in the selected stacked base. They were used as source
material and are not included in this PR:

- `docs/plans/2026-06-27-arco-unified-execution-roadmap.md`
- `docs/plans/2026-06-25-arco-tier1-control-store-strategy.md`
- `docs/plans/2026-06-26-arco-tier1-single-authority-combined-vision.md`
- `docs/plans/2026-06-26-lineage-observation-projection-design.md`
- `docs/plans/2026-06-27-planner-runtime-seam-hardening-design.md`

The selected stacked base includes and this slice also re-read:

- `docs/plans/2026-06-20-olympia-inspired-arco-strategy.md`
- `docs/guide/src/reference/control-plane-scope.md`

The current baseline wording remains: ledger append -> synchronous compaction
-> immutable manifest snapshot -> pointer CAS.

## Owned Files

- `docs/plans/2026-06-28-phase-2-contract-conformance-pr-prep.md`
- `docs/spec/arco-storage-format-v0.md`
- `docs/spec/object-store-contract.md`
- `docs/spec/projection-watermark-contract.md`
- `docs/spec/root-ownership-and-iam-contract.md`
- `docs/spec/provider-capability-matrix.md`
- `docs/spec/domain-event-archive-retention.md`
- `docs/spec/README.md`
- `docs/spec/fixtures/provider-capability-matrix-v1.json`
- `docs/spec/fixtures/root-ownership-and-iam-v1.json`
- `crates/arco-core/tests/provider_capability_matrix_contract.rs`
- `crates/arco-core/tests/root_ownership_iam_contract.rs`

## Implementation Checklist

- Expand `arco-storage-format-v0.md` with reader contracts for authority,
  projection, root-token, and snapshot/export reads.
- Expand `object-store-contract.md` with a failure-mode table covering CAS loss,
  stale fencing, duplicate retry, orphan artifacts, stale reads, token expiry,
  checkpoint expiry, corrupt artifacts, and stale projection watermarks.
- Expand retention docs with reachability rules that protect control roots,
  checkpoints, snapshots, exports, projection roots, domain event archives, and
  orphan grace windows.
- Keep `projection-watermark-contract.md` explicit that watermarks are
  freshness metadata for read-only projections, not mutation or enforcement
  authority.
- Add fixture-level provider-CAS conformance coverage while leaving all
  production providers disabled or not certified until live evidence exists.
- Add fixture-level root ownership/IAM coverage while recording that concrete
  cloud IAM/Terraform policy tests are a follow-up once production role shapes
  are grounded.

## Explicit Exclusions

- No Phase 3A deterministic state model.
- No object-store control-store MVP.
- No production provider enablement.
- No production write behavior changes.
- No catalog DDL, grants, credential vending, governance writes, or system-table
  enforcement movement.
- No claim that the control-store path is accepted production architecture.
- No claim that current Parquet/JSON manifests remain the final Tier-1 authority
  after a migrated domain cuts over.

## Verification Gate

Run before PR handoff:

```bash
git log --oneline <phase-0-1-base>..HEAD
git diff --stat <phase-0-1-base>...HEAD
git diff --check
LC_ALL=C rg -n "[^[:ascii:]]" docs/spec docs/plans/2026-06-28-phase-2-contract-conformance-pr-prep.md crates/arco-core/tests/provider_capability_matrix_contract.rs crates/arco-core/tests/root_ownership_iam_contract.rs
cargo fmt --check
cargo test -p arco-core --test provider_capability_matrix_contract
cargo test -p arco-core --test root_ownership_iam_contract
cargo xtask repo-hygiene-check
```
