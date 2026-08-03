# Phase 0 Slice 1 Contract Consolidation

**Status:** Phase 0 Slice 1 child plan.

**Scope:** Draft contract scaffolds and narrow documentation index links only.

This slice does not complete the full Phase 0 gate in
`docs/plans/2026-06-27-arco-unified-execution-roadmap.md`. ADRs, final
cross-document vocabulary reconciliation, conformance tests, and implementation
changes remain follow-up work.

## Source Documents

- `docs/plans/2026-06-27-arco-unified-execution-roadmap.md`
- `docs/plans/2026-06-26-arco-tier1-single-authority-combined-vision.md`
- `docs/plans/2026-06-25-arco-tier1-control-store-strategy.md`
- `docs/plans/2026-06-20-olympia-inspired-arco-strategy.md`
- `docs/plans/2026-06-26-lineage-observation-projection-design.md`
- `docs/plans/2026-06-27-planner-runtime-seam-hardening-design.md`
- `docs/guide/src/reference/control-plane-scope.md`

## Current Baseline

The implemented Tier-1 authority remains:

```text
ledger append
  -> synchronous compaction
  -> immutable manifest snapshot
  -> pointer CAS
```

The control-store path is prototype-approved only, not accepted production
architecture, not accepted ADR-backed architecture, and not cutover-approved
behavior.

## Deliverables

This slice creates:

- `docs/spec/README.md`
- `docs/spec/arco-storage-format-v0.md`
- `docs/spec/object-store-contract.md`
- `docs/spec/state-token-and-checkpoint-contract.md`
- `docs/spec/projection-watermark-contract.md`
- `docs/spec/api-token-exposure-matrix.md`

It also adds narrow `docs/spec/` index links in:

- `docs/README.md`
- `docs/guide/src/reference/documentation-map.md`

## Dirty Worktree Preservation

Before this slice, the worktree already contained unrelated modified and
untracked documentation:

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

This slice must preserve that work. It owns only the new Phase 0 Slice 1 files
and the narrow `docs/spec/` index entries in the two already-modified index
files.

## Non-Goals

- Do not add ADR files in this slice.
- Do not declare Phase 0 complete.
- Do not change Rust code, protobufs, generated files, build scripts, or tests.
- Do not run broad Rust test suites for this docs-only slice.
- Do not run `cargo xtask adr-check` unless ADR files are added or modified.
- Do not settle public API token exposure by implication.
- Do not claim catalog DDL, credential vending, or compatibility APIs have cut
  over to control-store authority.

## Contract Boundaries

The draft specs must preserve these boundaries:

- The current production path is the ledger plus synchronous compactor path.
- The proposed control-store path is prototype-approved only.
- For any migrated future scope, one active authority writer owns the
  mutation-visible control root.
- The projection compactor owns projection roots and public Parquet projection
  artifacts, not mutation authority.
- Snapshot and export services create retained cuts and packages, not mutation
  visibility.
- Object-store listing is not required for request-time correctness.
- System tables, lineage projections, search indexes, audit views, and derived
  indexes are read-only projections, never mutation authority and never
  enforcement inputs.
- Lineage remains append-only observations -> deterministic projections ->
  read-only views.
- `PlanCompiler` owns semantic lowering. Runtime owns attempts and convergence.

## Banned Claims

The new docs must not claim:

- the control store is accepted production architecture;
- catalog DDL has cut over to control-store authority;
- credential vending reads the new control store;
- every current Tier-1 write returns a `StateToken`;
- Parquet or JSON manifests are a permanent peer authority after domain
  migration;
- system tables, lineage, search, audit, or indexes are enforcement inputs;
- object-store listing is required for request-time correctness;
- compatibility APIs expose `StateToken`s in response bodies by default.

## ADR Follow-Ups

These ADRs remain follow-up work after this slice:

- `docs/adr/adr-0XX-tier1-control-store-single-authority.md`
- `docs/adr/adr-0XX-plan-compiler-runtime-handoff.md`
- `docs/adr/adr-0XX-lineage-observation-projection.md`

The draft specs should make those ADRs easier to write, but they are not ADR
substitutes.

## Verification

Run focused docs-only checks:

```bash
git diff --check

LC_ALL=C awk '/[^\t -~]/ { print FILENAME ":" FNR ":" $0; found=1 } END { exit found }' \
  docs/plans/2026-06-27-phase-0-contract-consolidation-slice.md \
  docs/spec/README.md \
  docs/spec/arco-storage-format-v0.md \
  docs/spec/object-store-contract.md \
  docs/spec/state-token-and-checkpoint-contract.md \
  docs/spec/projection-watermark-contract.md \
  docs/spec/api-token-exposure-matrix.md

rg -n "docs/spec" docs/README.md docs/guide/src/reference/documentation-map.md

rg -n "prototype-approved|not accepted production|not accepted ADR|draft" \
  docs/spec docs/plans/2026-06-27-phase-0-contract-consolidation-slice.md
```

## Exit Gate

This slice is complete when:

- `docs/spec/` exists with a README and five draft contract scaffolds.
- Every draft spec starts with the Phase 0 status banner.
- The child plan records scope, non-goals, verification, dirty-worktree
  preservation, banned claims, and ADR follow-ups.
- The child plan states this is Phase 0 Slice 1, not full Phase 0 completion.
- No new doc claims control-store accepted production architecture or cutover.
- Token exposure remains an explicit compatibility matrix decision.
- Projection docs state derived, non-enforcement semantics.
- Planner/runtime and lineage wording preserve the roadmap invariants.
- Existing dirty and untracked user work is preserved.
- `git diff --check` and focused ASCII/docs checks pass.
