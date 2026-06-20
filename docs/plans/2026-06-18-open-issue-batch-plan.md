# Open Issue Batch Plan Implementation Plan

**Goal:** Organize the live open Arco issue backlog into an execution order that reduces production risk first while keeping current WIP and UAT work from drifting.

**Architecture:** Treat the backlog as dependency batches, not independent tickets. Each batch starts with a read-only confirmation pass against current `main` and live GitHub issue state, then uses focused PRs that close small issue sets with explicit tests and evidence.

**Tech Stack:** Rust workspace, Python worker package, Terraform/GCP Cloud Run infrastructure, GitHub Actions, mdBook/docs, `gh` for live issue state.

---

## Snapshot

Captured on 2026-06-18 from `daxis-io/arco`.

- Open issues reviewed: 83.
- Open PRs checked: only Dependabot PR 237 and PR 238 were open, so no issue batch is already covered by an active feature PR.
- Primary source for #250-#297 severity: `docs/reports/2026-06-12-codebase-deep-review.md`.
- Current checkout was dirty, so this plan intentionally does not assume local WIP is merged or correct.

Refresh command:

```bash
gh issue list --repo daxis-io/arco --state open --limit 300 --json number,title,labels,updatedAt,url
```

## Ordering Rules

1. Finish or quarantine current WIP before starting new architecture work.
2. Fix commit-blocking security and IAM issues before landing ADR-041-adjacent changes.
3. Prioritize confirmed data loss, tenant-isolation, and liveness bugs over unverified cleanup leads.
4. Keep live/cloud UAT work behind a single-owner deploy gate; do not mix it with local deterministic UAT fixes.
5. Treat unverified deep-review issues as investigation tickets: reproduce first, then fix.

## Batch 0: Backlog Reconciliation And Current UAT WIP

**Issues:** #198, #215, #216, #217, #219, #220, #222, #223, #224, #225, #226, #227, #228, #229, #230.

**Why first:** Many of these are tied to local WIP branches or already-commented fixes. The fastest risk reduction is to verify what is already implemented, close stale tickets, and extract any remaining real work into smaller follow-ups.

**Order:**

1. #198: verify whether the UC compatibility support registry commit/branch is still relevant, then either package it or close as superseded.
2. #215, #216, #225, #226: re-check the UAT/projection bug fixes against current `main`; close only with merged proof.
3. #217: land or re-scope the deterministic acceptance suite as the maintained local gate.
4. #219, #229: settle response/code-version semantics before broadening assertions.
5. #220, #222: track UAT realism and richer lineage assertions after the base suite is stable.
6. #223, #224, #228, #230: separate CI/hygiene problems from the acceptance suite so the UAT gate is not blocked by unrelated tool debt.
7. #227: add evidence validation before accepting future live UAT output.

**Exit criteria:**

- Every issue in this batch is either closed with merged evidence or has a smaller follow-up issue with current-main repro steps.
- Local deterministic UAT has one documented command that does not require live GCP.
- CI hygiene checks are separate from the UAT correctness gate.

## Batch 1: ADR-041 And Land-Blocking Security/IAM Guardrails

**Issues:** #256, #257, #258, #285, #296.

**Why now:** The deep review calls these out as hard blockers for in-flight ADR-041-style work: Python worker callback incompatibility, unauthenticated dispatch, bucket-wide IAM drift, durable event version skew, and ADR governance drift.

**Order:**

1. #296: fix duplicate ADR ID handling so new ADR work cannot make governance worse.
2. #256: align Python callbacks with Rust dispatch envelopes, especially `callback_task_id`.
3. #257: authenticate Python worker dispatch, default local bind to loopback, and validate tenant/workspace envelope scope.
4. #258: restore prefix-scoped IAM conditions for anti-entropy and timer ingest.
5. #285: add unknown-event tolerance or an explicit rollout rule before durable event schemas expand.

**Exit criteria:**

- Python and Rust worker contracts agree on callback task IDs and auth boundaries.
- Terraform no longer grants the affected service accounts bucket-wide write access.
- ADR check fails on duplicate ADR IDs.
- Durable orchestration event reads tolerate or quarantine unknown tags.

## Batch 2: Confirmed Highest-Risk Correctness And Tenant Isolation

**Issues:** #250, #251, #252, #253, #254, #255, #259, #260, #261, #262, #265, #266, #267.

**Why now:** These issues map to the highest-impact confirmed findings: orchestration liveness wedges, catalog snapshot deletion/corruption windows, credential vending tenant isolation, per-event compaction cost, ambiguous transaction outcomes, and idempotency recovery hazards.

**Order:**

1. #254: fail closed or downscope GCS credential vending before wider Iceberg/UC exposure work.
2. #251, #252, #253: fix Tier-1 reconciler deletion safety, event watermarking, and orphan snapshot retry wedges together because they share catalog snapshot invariants.
3. #250: restore retry and zombie recovery bootstrapping, or explicitly document it as unbuilt and block affected flows.
4. #259: separate definite aborts from ambiguous sync-compact outcomes.
5. #260: claim backfill idempotency before appending events.
6. #261, #267: align Iceberg stale/in-progress marker recovery between commit and GC paths.
7. #265, #266: harden catalog writer idempotency around transient conflicts and crash-after-commit recovery.
8. #262: make task completion and output visibility atomic from the caller's perspective.
9. #255: plan the compaction architecture fix after the safety bugs above are understood, because it is broader than a route patch.

**Exit criteria:**

- Each confirmed data-loss or liveness path has a failing test before the fix.
- Idempotency marker states distinguish deterministic failure, transient contention, ambiguous success, and committed recovery.
- Any credential vending path is tenant/workspace scoped by enforceable credentials, not advisory prefixes.

## Batch 3: Orchestration Concurrency, Rebuild, And Test Trustworthiness

**Issues:** #263, #264, #268, #272, #273, #274, #275, #281, #282.

**Why now:** These reinforce Batch 2 by making projection rebuilds, dispatch, token validation, and storage tests trustworthy under multi-instance and skewed-clock conditions.

**Order:**

1. #281: align test storage CAS semantics with production before trusting concurrency tests.
2. #282: strengthen fold determinism assertions beyond collection counts.
3. #264: settle canonical ledger rebuild ordering.
4. #263: move idempotency-key recording after successful event application or widen key discriminators.
5. #272: implement or remove dead dispatch status transitions and prune outbox rows.
6. #273: bind task tokens to dispatch attempts.
7. #274: fix sweeper redispatch Cloud Tasks de-duplication names.
8. #268: replace offset pagination where mutable orchestration rows can skip or duplicate.
9. #275: add deterministic multi-instance and clock-skew tests; then decide whether nightly chaos becomes real CI or remains opt-in.

**Exit criteria:**

- Storage conformance includes MemoryBackend and at least local object-store semantics for CAS edge cases.
- Rebuild and live fold ordering are tested against timestamp skew and batch splits.
- Dispatch lifecycle tests cover Created, enqueued, acked/failed or the equivalent retained state model.

## Batch 4: Live And Deployed UAT Chain

**Issues:** #218, #231, #232, #233, #234, #235, #236, #239, #240, #241, #242, #243, #244, #245, #246, #247, #248, #249.

**Why here:** Live UAT depends on a coherent deployed environment. It should start after local UAT is stable and after current correctness/security blockers are controlled.

**Order:**

1. #239, #249: define single-owner live deploy protocol and document sandbox escalation boundaries.
2. #236: guard against unmanaged Terraform resources before applying dev changes.
3. #233, #247: make evidence paths and failure artifacts reliable before more live runs.
4. #232: expose deployed API build provenance.
5. #235: decide the deployable worker binary story.
6. #240: make internal-only Cloud Run access a recurring workflow.
7. #241, #242: ensure API-to-compactor Cloud Run invoker IAM and ID-token auth both work.
8. #245, #248: align catalog and flow service tenant/workspace scope.
9. #243: fix API-to-flow-compactor internal routing.
10. #244: investigate and repair cyclic orchestration manifest state before trusting live evidence.
11. #231: re-check deployed `system.catalog` query visibility after provenance and deploy freshness are proven.
12. #246: debug READY root task dispatch in the coherent environment.
13. #234, #218: rerun and accept deployed UAT only after validated evidence artifacts prove API, worker, catalog/metastore, dispatch, and storage behavior.

**Exit criteria:**

- Live UAT has a single-owner runbook and clear mutation boundary.
- `/version` or equivalent provenance proves the deployed code under test.
- A fresh `deployed_api_worker_*.json` artifact passes validation and is tied to the expected tenant/workspace and revision.

## Batch 5: Iceberg, UC, And External Compatibility

**Issues:** #214, #269, #270, #271, #276, #277, #278, #283, #297.

**Why here:** These affect downstream users and externally-facing protocols. Some are security-adjacent, but most need compatibility fixtures before code changes.

**Order:**

1. #276, #277: confirm Iceberg/UC router reachability, rate limiting, and public error redaction.
2. #278: compare vendored thrift against upstream allocation guards and patch if reachable from external metadata.
3. #283: add single-table Iceberg conflict coverage before changing commit compatibility.
4. #269, #270, #271: investigate Iceberg marker cleanup, snapshot-log compatibility, and UC-drop pointer cleanup.
5. #297: decide whether standard Iceberg REST clients must commit without UUIDv7 `Idempotency-Key`; then add fixtures or document the requirement.
6. #214: make `jsonwebtoken` backend choice compatible with downstream crates.

**Exit criteria:**

- Standard Iceberg/UC route tests cover security middleware and sanitized public errors.
- Iceberg commit behavior is covered for conflicts, stale markers, requirement failures, and external-client compatibility.
- Downstream Rust consumers can choose the JWT crypto backend without forking Arco.

## Batch 6: Public API Contract Consistency

**Issues:** #284, #286, #287, #288, #289.

**Why here:** These are important for API stability, but they should follow correctness and compatibility fixes so the contract reflects the stable behavior.

**Order:**

1. #284: decide `#[non_exhaustive]` policy and clean duplicate error variants.
2. #286: normalize orchestration idempotency behavior.
3. #289: preserve 409 lock-race semantics instead of rewriting them to 412.
4. #288: add bounded pagination for catalog list endpoints.
5. #287: normalize JSON casing only after compatibility/migration rules are explicit.

**Exit criteria:**

- OpenAPI or serialization tests pin public route casing, pagination, idempotency, and error semantics.
- Any breaking public API changes are documented with migration guidance.

## Batch 7: Performance, Architecture, And Documentation Debt

**Issues:** #221, #279, #280, #290, #291, #292, #293, #294, #295.

**Why last:** These are real maintainability and performance concerns, but they should not distract from safety, liveness, tenant isolation, and public contract fixes.

**Order:**

1. #279, #280: measure catalog read/write amplification before redesigning caches or segmented snapshots.
2. #291: define contracts-only state access between `arco-api` and `arco-flow`.
3. #292: consolidate or explicitly justify divergent replay/watermark protocols.
4. #290: decompose god modules with move-only PRs before behavior changes.
5. #293: audit legacy scheduler, leader, and quota modules and update ADR status.
6. #221: defer row-level query redaction until access-backed system tables exist.
7. #294, #295: clean docs that misstate v0.2.x helper status, crate ownership, and compactor deployment mode.

**Exit criteria:**

- Performance changes have object-store GET/byte baselines and target budgets.
- Module decomposition PRs are behavior-preserving and test-neutral.
- Docs match current crate boundaries, release compatibility, and Cloud Run deployment reality.

## Global Verification Pattern

For each future batch:

1. Refresh issue state and confirm no issue was already closed or superseded.
2. Create a narrow worktree from current `origin/main`.
3. Reproduce or refute each bug before writing the fix.
4. Keep one PR per coherent issue cluster; do not bundle unrelated docs, infra, and Rust changes unless the issue dependency requires it.
5. Run the narrowest relevant test first, then the package/workspace gates needed by touched surfaces.
6. Add a closing issue comment with commands, results, and any remaining scope boundary.

Suggested baseline commands:

```bash
git fetch origin main
git status --short --branch
cargo fmt --check
cargo test -p <crate> <focused-filter>
git diff --check
```

Use live/cloud commands only for Batch 4 or when an issue explicitly depends on deployed state.

## Accounting Checklist

- Batch 0: #198, #215, #216, #217, #219, #220, #222, #223, #224, #225, #226, #227, #228, #229, #230.
- Batch 1: #256, #257, #258, #285, #296.
- Batch 2: #250, #251, #252, #253, #254, #255, #259, #260, #261, #262, #265, #266, #267.
- Batch 3: #263, #264, #268, #272, #273, #274, #275, #281, #282.
- Batch 4: #218, #231, #232, #233, #234, #235, #236, #239, #240, #241, #242, #243, #244, #245, #246, #247, #248, #249.
- Batch 5: #214, #269, #270, #271, #276, #277, #278, #283, #297.
- Batch 6: #284, #286, #287, #288, #289.
- Batch 7: #221, #279, #280, #290, #291, #292, #293, #294, #295.
