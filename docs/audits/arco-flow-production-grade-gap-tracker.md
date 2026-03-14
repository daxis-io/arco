# Arco Flow — Production Grade Audit Gap Tracker

## Purpose
This document captures the current audit findings and gaps against `arco-flow-production-grade-review-checklist.md` (source provided out-of-repo as part of the review packet).

It is intended to be a single, living source of truth for:
- production-readiness risks
- prioritized fixes
- verification evidence (tests, CI gates, and runbooks)

## Current Verdict
- **Verdict:** **Block**
- **Top reasons (P0):**
  1. Cross-language identity drift (IDs + partitions) makes fingerprints/validation unsafe.
  2. “Canonical” serialization is inconsistent across Rust/Python; Python currently permits floats in a path labeled canonical.
  3. CI does not run Python unit tests where these invariants should be enforced.

## Scope / Assumptions
- **Assumption:** treat the *current implementation* as canonical for ID wire formats (e.g., `AssetId` as implemented in Rust), and update proto/docs/ADRs to match.
- **Assumption (tentative):** ADR-020 orchestration domain is the production path.
- **Open decision:** whether the legacy `Scheduler/Store/LeaderElector` stack is production or a local/test harness.

## Production Surface Area (Observed)
- **ADR-020 orchestration domain appears to be the production path**:
  - `crates/arco-flow/src/orchestration/mod.rs:1`
  - Services/binaries:
    - `crates/arco-flow/src/bin/arco_flow_compactor.rs:1`
    - `crates/arco-flow/src/bin/arco_flow_dispatcher.rs:1`
    - `crates/arco-flow/src/bin/arco_flow_sweeper.rs:1`
- **Legacy scheduler stack appears primarily in tests/docs**:
  - `crates/arco-flow/tests/integration.rs:85`
  - `crates/arco-flow/src/lib.rs:33`

If we confirm ADR-020 is the sole production path, we should explicitly document/feature-gate the legacy stack to prevent accidental production use.

---

## Execution Plan

### Phase P0 — Identity + CI correctness (Blocking)
**Goal:** eliminate cross-language identity drift and make regressions impossible to merge.

1. **ID wire-format alignment (treat implementation as canonical)**
   - Update proto comments/docs/ADRs to match current Rust implementation.
   - Resolve conflicts between:
     - `proto/arco/v1/common.proto:23`
     - `docs/adr/adr-013-id-wire-formats.md:20`
     - `docs/adr/adr-002-id-strategy.md:20`
     - `crates/arco-core/src/id.rs:33`

2. **PartitionKey parity (ADR-011) across Rust/Python**
   - Align Python `PartitionKey` and scalar encoding to match Rust `PartitionKey::canonical_string()`.
   - Primary refs:
     - `crates/arco-core/src/partition.rs:20`
     - `python/arco/src/arco_flow/types/partition.py:190`
     - `python/arco/src/arco_flow/types/scalar.py:78`

3. **Canonical JSON parity (ADR-010) where identity/signing depends on it**
   - Implement strict ADR-010 canonical JSON behavior in Python for any identity/signing contexts.
   - Primary refs:
     - `docs/adr/adr-010-canonical-json.md:23`
     - `crates/arco-core/src/canonical_json.rs:6`
     - `python/arco/src/arco_flow/manifest/serialization.py:15`

4. **CI: add Python unit tests**
   - CI currently runs Python CLI integration only; add unit tests.
   - Ref: `.github/workflows/ci.yml:126`

**P0 Exit Criteria**
- Rust + Python pass the same **PartitionKey** fixture vectors: `crates/arco-core/tests/fixtures/partition_key_cases.json:1`.
- Rust + Python pass the same **canonical JSON** vectors: `crates/arco-core/tests/golden/canonical_json_vectors.json:1`.
- CI runs Python unit tests and blocks regressions.

### Phase P1 — Operational reliability + perf hardening
**Goal:** prevent silent failures, reduce load-sensitive hotspots, and make observability truthful.

1. **Clarify production surface area**
   - Decide and document whether `Scheduler/Store/LeaderElector` are production.
   - If non-prod: deprecate/feature-gate or move under an explicit `test-utils` boundary.

2. **Fix misleading Cloud Tasks queue depth metrics**
   - `queue_depth()` returns `0` sentinel which can read as “healthy”.
   - Ref: `crates/arco-flow/src/dispatch/cloud_tasks.rs:709`

3. **Callback task-state lookup performance**
   - Current lookup scans tasks linearly per callback.
   - Ref: `crates/arco-api/src/routes/tasks.rs:419`

4. **Remove obvious O(n²) tie-breakers where they matter**
   - Scheduler `.position()` inside comparator: `crates/arco-flow/src/scheduler.rs:242`
   - DAG insertion-order `.position()` scans: `crates/arco-flow/src/dag.rs:165`

**P1 Exit Criteria**
- Dashboards/alerts no longer interpret “unknown” as “0”.
- Callback endpoints use indexed lookups; perf evidence exists (benchmark or load test).

### Phase P2 — Evidence bundles + staging proof
**Goal:** close the “paper trail” gap so production-ready claims are auditable.

- Adopt a lightweight evidence convention (e.g., `docs/evidence/arco-flow-prod-grade/*`) that includes:
  - staging soak results
  - failure drill results (kill worker/compactor/dispatcher; validate anti-entropy)
  - runbooks + dashboards links

---

## Gap Tracker

### How to use this tracker
- Add **Owner**, **Target date**, and **PR** as work starts.
- Only mark a gap “Done” when the **Verification** column has been satisfied.

| Gap ID | Priority | Status | Finding | Evidence (repo-addressable) | Fix Plan (what to do) | Verification (what proves it) | Owner | Target | PR |
|---|---:|---:|---|---|---|---|---|---|---|
| GT-001 | P0 | DONE | Canonical JSON is inconsistent across Rust/Python; Python “canonical” permits floats (ADR-010 bans floats for deterministic identity). | `docs/adr/adr-010-canonical-json.md:23`, `crates/arco-core/src/canonical_json.rs:6`, `python/arco/src/arco_flow/canonical_json.py:1`, `python/arco/src/arco_flow/manifest/serialization.py:1` | Implement strict ADR‑010 canonical JSON in Python for identity/signing contexts. | `python -m pytest tests/unit/test_canonical_json_vectors.py -q` (vectors: `crates/arco-core/tests/golden/canonical_json_vectors.json`) and CI runs Python unit tests. | TBD | TBD | (uncommitted) |
| GT-002 | P0 | DONE | Partition identity drift: Rust `PartitionKey` canonical encoding differs from Python PartitionKey representation/encoding. | `crates/arco-core/src/partition.rs:20`, `docs/adr/adr-011-partition-identity.md:1`, `python/arco/src/arco_flow/types/partition.py:1`, `python/arco/src/arco_flow/types/scalar.py:1` | Treat Rust as canonical: update Python `PartitionKey` + scalar encoding to match ADR‑011 grammar/encoding (base64url strings; micros timestamps; deterministic dimension ordering). | `python -m pytest tests/unit/test_partition_key_vectors.py -q` (vectors: `crates/arco-core/tests/fixtures/partition_key_cases.json`) and CI runs Python unit tests. | TBD | TBD | (uncommitted) |
| GT-003 | P0 | DONE | ID wire format drift: proto/docs claim `AssetId` ULID but Rust uses UUIDv7; ADRs conflict. | `proto/arco/v1/common.proto:23`, `crates/arco-core/src/id.rs:33`, `docs/adr/adr-013-id-wire-formats.md:1`, `python/arco/src/arco_flow/types/ids.py:1` | Treat implementation as canonical: update proto comments + docs/ADRs + Python validators/generators to reflect UUIDv7 `AssetId` and ULID `RunId/TaskId`. | `python -m pytest tests/unit/test_ids.py -q` and docs/proto comments updated to match `crates/arco-core/src/id.rs:33`. | TBD | TBD | (uncommitted) |
| GT-004 | P0 | DONE | CI does not run Python unit tests, so identity/canonicalization regressions can merge silently. | `.github/workflows/ci.yml:126` | Add CI step for Python unit tests under `python/arco` (in addition to existing integration test). | GitHub Actions runs `python -m pytest tests/unit -v` under `python/arco` in `.github/workflows/ci.yml:126`. | TBD | TBD | (uncommitted) |
| GT-005 | P0 | DONE | REST TriggerRun partitions are currently treated as string values only; type semantics must be explicitly defined to avoid drift. | `crates/arco-api/src/routes/orchestration.rs:71`, `python/arco/src/arco_flow/cli/commands/run.py:18`, `python/arco/src/arco_flow/client.py:152` | Contract: TriggerRun accepts `partitionKey` as an ADR‑011 canonical partition key string (preferred). `partitions` key/value remains supported for backward-compat, but cannot be combined with `partitionKey`. | `cargo test -p arco-api --all-features resolve_partition_key` and `python -m pytest tests/unit/test_client_trigger_run.py -q` and `python -m pytest tests/integration/test_cli_api.py -q`. | TBD | TBD | (uncommitted) |
| GT-006 | P1 | RISK | Production surface area ambiguous: two orchestration stacks are exported (`orchestration` vs `Scheduler/Store/LeaderElector`). | `crates/arco-flow/src/orchestration/mod.rs:1`, `crates/arco-flow/tests/integration.rs:85`, `crates/arco-flow/src/lib.rs:33` | Decide and document: “Prod path = ADR‑020 orchestration domain”; deprecate/feature-gate legacy stack if non-prod. | Docs updated + build-time gating if chosen; no prod binary depends on legacy stack. |  |  |  |
| GT-007 | P1 | RISK | Cloud Tasks `queue_depth()` returns `0` sentinel (“unknown”), which can mislead dashboards/alerts. | `crates/arco-flow/src/dispatch/cloud_tasks.rs:709` | Change metrics semantics: don’t emit misleading depth; emit separate gauge for unknown or remove depth signal. | Unit test + dashboard update demonstrating no false “0 depth”. |  |  |  |
| GT-008 | P1 | RISK | Callback task-state lookup scans all tasks per request (O(N)); ambiguous matches can error. | `crates/arco-api/src/routes/tasks.rs:419` | Add indexed lookup keyed by `task_key` in folded state. | Perf evidence (benchmark/load) + correctness tests (no ambiguous matches). |  |  |  |
| GT-009 | P1 | PERF | Scheduler uses `.position()` inside sort comparator; can degrade to O(n²). | `crates/arco-flow/src/scheduler.rs:242` | Precompute `task_id -> order_index` map; avoid repeated scans during sorting. | Benchmark or targeted perf test; code review confirms no O(n²) comparator. |  |  |  |
| GT-010 | P1 | PERF | DAG uses repeated `.position()` scans for insertion-order neighbor sorting. | `crates/arco-flow/src/dag.rs:165` | Store `NodeIndex -> insertion_rank` map; use rank directly. | Benchmark or complexity review. |  |  |  |
| GT-011 | P1 | HYGIENE | Evidence docs appear stale/contradict code (example: flow metrics label claims). | `docs/guide/src/reference/evidence-policy.md:1`, `crates/arco-flow/src/metrics.rs:90`, `.github/workflows/ci.yml:159` | Refresh evidence docs or mark them time-scoped; ensure evidence references remain repo-correct. | Updated evidence with correct paths/claims; optional CI checks. |  |  |  |
| GT-012 | P2 | GAP | No repo-local “gate evidence bundle” convention exists; checklist expects auditable evidence. | `arco-flow-production-grade-review-checklist.md` (review packet, out-of-repo) | Add a lightweight evidence bundle convention and attach evidence per phase. | Evidence bundle exists and is kept up to date as gaps close. |  |  |  |

---

## Suggested Next Iteration (P0 broken into PR-sized tasks)
1. PR: “Run Python unit tests in CI” (GT-004).
2. PR: “Align proto/docs with canonical ID formats (UUIDv7 AssetId)” (GT-003).
3. PR: “Python PartitionKey parity with Rust fixture vectors” (GT-002).
4. PR: “Python strict canonical JSON parity with Rust vectors” (GT-001).
5. PR: “Decide and document partitions API contract (typed vs canonical-string)” (GT-005).
