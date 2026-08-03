# Phase 0-1B PR Prep Plan

**Goal:** Prepare a clean PR branch from current `origin/main` that contains only Phase 0, Phase 1A, and Phase 1B roadmap work.

**Architecture:** Keep this branch as scaffold-and-seam work only. Phase 0 adds draft storage/token/projection contract scaffolds, Phase 1A introduces a current-authority state-store adapter that does not mint retained tokens, and Phase 1B moves compatibility planning behind a planner-owned seam while preserving today's `PlanCreated { tasks: Vec<TaskDef> }` wire behavior.

**Tech Stack:** Rust workspace, `arco-catalog`, `arco-flow`, mdBook/docs, focused cargo tests, `cargo fmt`, `git diff --check`.

---

## Source Docs

Read as local roadmap inputs before branch reconstruction:

- `docs/plans/2026-06-27-arco-unified-execution-roadmap.md`
- `docs/plans/2026-06-20-olympia-inspired-arco-strategy.md`
- `docs/plans/2026-06-25-arco-tier1-control-store-strategy.md`
- `docs/plans/2026-06-26-arco-tier1-single-authority-combined-vision.md`
- `docs/plans/2026-06-26-lineage-observation-projection-design.md`
- `docs/plans/2026-06-27-planner-runtime-seam-hardening-design.md`
- `docs/guide/src/reference/control-plane-scope.md`

These source docs are inputs, not automatic PR content. Include only files required by the Phase 0-1B commits below.

## Current-State Audit

- Root checkout before isolation: dirty docs in `docs/README.md`, `docs/guide/src/reference/documentation-map.md`, and several roadmap/design docs.
- PR-prep worktree: `.worktrees/phase-0-1-roadmap-seams`.
- Branch base: current `origin/main` at `10c72b8` (`chore(release): prepare v0.2.1 (#308)`).
- Current implemented authority wording from `control-plane-scope.md`: catalog DDL uses `CatalogWriter -> ledger append -> sync compaction -> immutable manifest snapshot -> pointer CAS`; lineage, search, and orchestration remain separate pointer-published domains; broader governance remains partial or planned.

## Commit Boundaries

### Commit 1: PR-prep plan

**Files:**

- Create: `docs/plans/2026-06-28-phase-0-1-pr-prep.md`

**Purpose:** Record source docs, exact included commits/files, exclusions, verification, and PR-readiness gate.

### Commit 2: Phase 0 contract scaffolds

Cherry-pick or reconstruct `cc91d0b docs: add phase 0 contract scaffolds`.

**Files:**

- Modify: `docs/README.md`
- Modify: `docs/guide/src/reference/documentation-map.md`
- Create: `docs/plans/2026-06-27-phase-0-contract-consolidation-slice.md`
- Create: `docs/spec/README.md`
- Create: `docs/spec/api-token-exposure-matrix.md`
- Create: `docs/spec/arco-storage-format-v0.md`
- Create: `docs/spec/object-store-contract.md`
- Create: `docs/spec/projection-watermark-contract.md`
- Create: `docs/spec/state-token-and-checkpoint-contract.md`

**Gate:** Draft contract scaffolds only. They must not claim a production control-store cutover, catalog DDL migration, or accepted final authority change.

### Commit 3: Phase 1A current adapter seam

Cherry-pick or reconstruct `297df03 feat(catalog): add state-store current adapter seam`.

**Files:**

- Modify: `crates/arco-catalog/src/lib.rs`
- Create: `crates/arco-catalog/src/state_store.rs`
- Create: `crates/arco-catalog/tests/state_store_current_adapter.rs`
- Create: `docs/plans/2026-06-27-state-store-seam-current-adapter-slice.md`

**Gate:** The current adapter delegates to the existing authority path and fails closed for unsupported retained reads, checkpointing, transactions, and token issuance. It must not return fake retained `StateToken`s.

### Commit 4: Phase 1B planner/runtime seam

Cherry-pick or reconstruct `1a57ed4 feat(flow): add planner runtime handoff seam`.

**Files:**

- Modify: `crates/arco-flow/src/application/mod.rs`
- Create: `crates/arco-flow/src/application/planning_snapshot_provider.rs`
- Create: `crates/arco-flow/src/application/run_planner.rs`
- Modify: `crates/arco-flow/src/lib.rs`
- Modify: `crates/arco-flow/src/orchestration/controllers/run_bridge.rs`
- Modify: `crates/arco-flow/src/orchestration/selection.rs`
- Create: `crates/arco-flow/src/planning/compiler.rs`
- Create: `crates/arco-flow/src/planning/diagnostics.rs`
- Create: `crates/arco-flow/src/planning/fingerprint.rs`
- Create: `crates/arco-flow/src/planning/mod.rs`
- Create: `crates/arco-flow/src/planning/selection.rs`
- Create: `crates/arco-flow/tests/planning_compiler_tests.rs`
- Create: `docs/plans/2026-06-28-planner-runtime-handoff-seam-slice.md`

**Gate:** `PlanCompiler` and `RunPlanner` own compatibility lowering. Runtime behavior and durable `PlanCreated { tasks: Vec<TaskDef> }` compatibility stay unchanged.

## Explicit Exclusions

- Exclude Phase 2 commit `e64d19e` and files for provider/root ownership/domain event archive contract conformance.
- Exclude Phase 3A commits `0b001b6` and `c16c684`, including deterministic state model files and tests.
- Exclude Phase 3B object-store control-store MVP work.
- Do not move catalog DDL, grants, credential vending, governance writes, or production authority.
- Do not change external API behavior.
- Do not include unrelated dirty local roadmap docs unless a conflict proves they are required by the included phase files.

## Verification

Run after reconstruction:

```bash
cargo fmt --check
git diff --check
cargo xtask adr-check
cargo test -p arco-catalog --test state_store_current_adapter
cargo test -p arco-flow --test planning_compiler_tests
cargo test -p arco-flow --test run_bridge_controller_tests
git log --oneline origin/main..HEAD
git diff --stat origin/main...HEAD
```

`cargo xtask adr-check` is included because Phase 0 updates doc indexes and contract scaffolds adjacent to ADR/documentation map surfaces.

## PR-Readiness Gate

- `git log --oneline origin/main..HEAD` contains only this plan plus Phase 0, Phase 1A, and Phase 1B commits.
- `git diff --stat origin/main...HEAD` contains only files classified above.
- No Phase 2, Phase 3A, or Phase 3B files appear in the final diff.
- `control-plane-scope.md` current-authority wording remains unchanged.
- External API behavior changed: no.
- Current production write authority remains ledger append -> sync compaction -> immutable manifest snapshot -> pointer CAS.
- Control-store strategy language remains prototype-gated, not cutover-approved.
