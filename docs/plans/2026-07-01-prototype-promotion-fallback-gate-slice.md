# Prototype Promotion/Fallback Gate Slice

Status: Phase 3C child plan.

Goal: add a deterministic advisory gate that says whether the object-store
state-store prototype has enough evidence to advance toward later service reads
or low-risk writes. This slice does not promote the prototype, cut over traffic,
or change production authority.

Base: current `origin/main` plus the Phase 3A and Phase 3B commits in this
PR-prep branch.

## Source Documents

- Roadmap: `docs/plans/2026-06-27-arco-unified-execution-roadmap.md`
- Original design doc: `docs/plans/2026-06-26-arco-tier1-single-authority-combined-vision.md`
- Original design doc: `docs/plans/2026-06-25-arco-tier1-control-store-strategy.md`
- Original design doc: `docs/plans/2026-06-20-olympia-inspired-arco-strategy.md`
- Original design doc: `docs/plans/2026-06-26-lineage-observation-projection-design.md`
- Original design doc: `docs/plans/2026-06-27-planner-runtime-seam-hardening-design.md`
- Current implementation authority: `docs/guide/src/reference/control-plane-scope.md`

The broader roadmap/design-doc family is source context for this slice. This
Phase 3-only PR does not add unrelated roadmap, Phase 4, Phase 5, or cutover
planning files.

The current production authority remains:

```text
ledger append -> synchronous compaction -> immutable manifest snapshot -> pointer CAS
```

## Scope

- Add `crates/arco-catalog/src/state_store/promotion_gate.rs`.
- Expose the gate only as `arco_catalog::state_store::promotion_gate`.
- Keep crate-root exports and prelude entries unchanged.
- Add deterministic tests in
  `crates/arco-catalog/tests/state_store_promotion_gate.rs`.
- Keep the gate pure: no state mutation, no traffic routing, no object-store
  artifacts, and no calls into production catalog or governance paths.

## Gate Inputs

`PromotionGateInput` records satisfied criteria plus measurement records. Its
`evaluate()` method returns a deterministic `PromotionGateReport`.

Required promote-only criteria:

- correctness and failure-state tests pass;
- provider CAS and retry behavior are proven;
- read-after-write by `StateToken` works;
- model replay equivalence holds;
- object-store MVP replay equivalence holds;
- projection equality can be measured through watermark;
- enforcement and vending can fail closed from authority or fresh-enough
  compiled state;
- operational complexity remains acceptable.

Required performance and operations measurements:

- warm write p99 for narrow metadata mutation;
- warm point-read p99;
- bounded prefix-scan p99;
- cold writer startup to write-ready;
- manifest-reachable replay bytes;
- projection watermark lag;
- compaction backlog before replay budget breach;
- `StateToken` read-after-write retention.

Measurement records must label their source as exactly one of:

- deterministic fixture;
- opt-in benchmark;
- unavailable.

Missing measurements are reported as unavailable. Any unavailable required
measurement rejects advancement.

## Decision

`PromotionDecision::RejectAdvancement` is returned when required evidence is
missing or a required measurement is unavailable.

`PromotionDecision::CandidateEvidenceComplete` means only that the advisory
evidence packet is complete enough for review. It does not promote, route,
publish, or cut over the prototype.

## Fallback

Every report carries the Phase 3C fallback recommendation:

- keep current synchronous-compactor authority;
- continue derived indexes and projection acceleration only;
- do not cut over catalog DDL, grants, credential vending, or broad governance.

## Tests

- Reject promotion when Phase 3B correctness/failure-state evidence is missing.
- Reject promotion when provider CAS/retry evidence is missing.
- Reject promotion when `StateToken` read-after-write evidence is missing.
- Reject promotion when model replay equivalence evidence is missing.
- Reject promotion when object-store MVP replay equivalence evidence is
  missing.
- Record unavailable projection equality, enforcement/vending freshness,
  operational complexity, and performance measurements without claiming
  benchmarks.
- Emit the required fallback recommendation.

## Verification

```bash
cargo fmt --check
cargo test -p arco-catalog --test state_store_model
cargo test -p arco-catalog --test state_store_control_mvp
cargo test -p arco-catalog --test state_store_promotion_gate
cargo check -p arco-catalog
git diff --check
```

## Non-Goals

- No production reader, writer, compactor, or API routing changes.
- No catalog DDL, grants, credential vending, or broad governance cutover.
- No real timing benchmarks in this slice.
- No Phase 4 shadow replay.
- No Phase 5 writable domains.
