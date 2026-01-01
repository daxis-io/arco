# Arco Flow vs Dagster — Exception Planning Document

**Date:** 2025-12-19 (v2 — revised per architecture feedback)
**Status:** Draft for stakeholder approval
**Related Audit:** [2025-12-19-arco-flow-vs-dagster-parity-audit.md](../plans/2025-12-19-arco-flow-vs-dagster-parity-audit.md)

---

## Purpose

This document captures **deliberate parity exceptions** — Dagster features that Arco Flow will **not match** (either permanently or for a defined deferral period). Each exception requires explicit stakeholder sign-off before the platform can claim "production ready."

**Approval workflow:**

1. Engineering drafts exception card
2. Product reviews user impact + migration path
3. Architecture reviews technical rationale
4. Stakeholders sign off (or reject → becomes a gap to close)

---

## Milestone Mapping

> **Note:** This document uses **Milestone M1/M2/M3** to avoid collision with other "Phase 1/2/3" roadmaps in platform docs.

| This Document | Timeline | Corresponds To |
|---|---|---|
| Milestone M1 | 0-30 days | Orchestration MVP (deploy→run→observe) |
| Milestone M2 | 31-60 days | Automation + Partitions |
| Milestone M3 | 61-90 days | Production Hardening |

---

## Exception Summary Matrix

| # | Feature | Exception Type | Decision | Risk | Sunset/Revisit | Sign-off |
|---|---|---|---|---|---|---|
| EX-01 | Op/Job orchestration model | Permanent | Won't Match | Low | N/A | ⬜ |
| EX-02 | Customer-operated daemon | Permanent | Won't Match (ops model) | Medium | N/A | ⬜ |
| EX-03 | Orchestration UI (run/task/backfill) | Deferral | Defer → M2 | Medium | M2 | ⬜ |
| EX-04 | IO Managers (declarative I/O) | Deferral | Defer → M2 | Low | M2 | ⬜ |
| EX-05a | Multi-asset / graph-asset execution | Deferral | Defer → M2 | Low | M2 | ⬜ |
| EX-05b | Observable sources / external assets | Deferral | Defer → M3 | Low | M3 | ⬜ |
| EX-06 | Multi-code-location workspace | Simplified | Single location per workspace | Medium | Enterprise demand | ⬜ |
| EX-07 | GraphQL API | Permanent | Won't Match | Low | N/A | ⬜ |
| EX-08 | Run launcher plugins (K8s, ECS) | Deferral | Defer → M3 | Medium | M3 | ⬜ |
| EX-09 | Asset reconciliation engine | Deferral | Phased delivery | High | M2/M3 | ⬜ |

> **Removed from exception list:** "Dagster Cloud managed infrastructure" — not a parity exception but a product-category mismatch. See **Appendix B: Out of Scope**.
>
> **Numbering note:** EX-05 is split into **EX-05a** and **EX-05b** to allow independent scope decisions and sign-off while still keeping 10 total exception cards.

---

## Exception Cards

### EX-01: Op/Job Orchestration Model

| Field | Value |
|---|---|
| **Exception Type** | Permanent |
| **Feature/Capability** | Dagster "op" and "job" primitives as first-class alternatives to assets |
| **Dagster Behavior** | Users can define ops (compute units) and compose them into jobs independently of assets. Jobs can be scheduled, have their own configs, and represent non-asset-producing workflows. |
| **Decision** | **Won't Match** |
| **Rationale (Business)** | Arco Flow is intentionally asset-first to reduce cognitive surface and focus product messaging. Supporting two parallel models fragments documentation, examples, and community. |
| **Rationale (Technical)** | Asset-first architecture simplifies lineage tracking, freshness computation, and catalog integration. Op/job model requires separate state machines, UI views, and scheduling semantics. |
| **Evidence** | `docs/plans/2025-01-12-arco-orchestration-design.md` — "Developer Experience (SDK & CLI)" establishes asset decorator as primary authoring surface. |
| **User Impact** | Users migrating Dagster job-first codebases must refactor to asset-producing patterns. Non-asset workflows (e.g., cleanup scripts, notifications) need asset wrappers. |
| **Migration Path** | 1. Document "job-to-asset" migration patterns<br>2. Provide `@asset(kind="activity")` for side-effect-only assets (still assets, different materialization kind)<br>3. **No separate `@task` decorator** — this would reintroduce dual-model complexity |
| **Workaround** | Define "activity assets" that produce a status/completion marker (remains asset-first) |
| **Trigger to Revisit** | Enterprise customer requires non-asset orchestration for >20% of their pipelines |
| **Risk if Permanent** | Low — asset-first is our differentiation, not a gap |
| **Effort to Match Later** | **L** (new DSL, scheduler semantics, UI surfaces, state machines) |
| **Stakeholder Sign-off** | ⬜ Product: ⬜ Arch: ⬜ Eng: |

---

### EX-02: Customer-Operated Daemon

| Field | Value |
|---|---|
| **Exception Type** | Permanent |
| **Feature/Capability** | Dagster's `dagster-daemon` process that customers deploy and operate for schedules, sensors, and run coordination |
| **Dagster Behavior** | Long-running daemon polls schedules, evaluates sensors, and coordinates run queue. Requires HA deployment and monitoring of daemon health by the customer. |
| **Decision** | **Won't Match** (customer-operated daemon), **Will Match** (semantic parity) |
| **Rationale (Business)** | Managed-control-plane offering — customers don't operate infrastructure. Operational simplicity is a product differentiator. |
| **Rationale (Technical)** | Control plane runs an always-on scheduler internally (per `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — "Scheduler Deployment Model" with `min_instances=1`). Customers interact via API/CLI, not daemon management. |
| **Evidence** | Platform is GCP-native with managed Cloud Run services; customer never touches daemon ops. |
| **User Impact** | Semantically equivalent — users define schedules/sensors the same way via SDK. No daemon to monitor/upgrade. |

**Semantic Parity Checklist (must match Dagster behavior):**

| Capability | Status | Verification |
|---|---|---|
| Tick history (durable) | Required | `GetScheduleHistory` API returns tick records |
| Cursor durability and replay | Required | Sensor cursor persists across restarts |
| run_key / idempotency | Required | Duplicate run_key returns existing run_id |
| Backoff and retry of tick evaluation | Required | Failed tick retries with exponential backoff |
| Catch-up behavior after outages | Required | Missed ticks evaluated on recovery |

| **Migration Path** | N/A — transparent to users |
| **Workaround** | N/A — managed implementation provides equivalent functionality |
| **Trigger to Revisit** | Event-driven model cannot meet platform SLO (currently: <5s sensor evaluation p99) |
| **Risk if Permanent** | Medium — must validate latency characteristics under load |
| **Effort to Match Later** | **M** (expose daemon for self-hosted; adds operational burden) |
| **Stakeholder Sign-off** | ⬜ Product: ⬜ Arch: ⬜ Eng: |

---

### EX-03: Orchestration UI (Run/Task/Backfill Debugging)

| Field | Value |
|---|---|
| **Exception Type** | Deferral |
| **Sunset Date** | Milestone M2 |
| **Feature/Capability** | Dagit-style web UI for run monitoring, task debugging, backfill management, sensor/schedule debugging |
| **Dagster Behavior** | Full-featured React UI with asset lineage graph, run timeline, partition matrix, log viewer, launchpad for ad-hoc runs |
| **Decision** | **Defer → M2** |

**What exists vs what's deferred:**

| Capability | Status | Notes |
|---|---|---|
| Catalog browsing (asset discovery) | Exists | Platform browser with DuckDB-WASM read path |
| Asset lineage graph | Exists | Via catalog UI |
| Run list / detail | **Deferred** | CLI-only in M1 |
| Task timeline / attempts | **Deferred** | CLI-only in M1 |
| Partition matrix | **Deferred** | No M1 UI |
| Backfill management | **Deferred** | CLI-only in M1 |
| Log viewer | **Deferred** | `arco-flow logs` CLI in M1 |

| **Rationale (Business)** | MVP scope constraint; CLI-first validates core semantics before UI investment |
| **Rationale (Technical)** | UI depends on stable API contracts, run/task projections, and event log queries — all P0 gaps |
| **User Impact** | Power users acceptable with CLI; casual users will struggle. Metrics via Grafana integration. |
| **Migration Path** | M1: CLI-only (`arco-flow status --watch`, `arco-flow logs`)<br>M2: Minimal viable UI (run list, task detail, logs)<br>M3: Full partition matrix + launchpad |
| **Workaround** | `arco-flow status --watch` for live updates; Grafana dashboards for metrics |
| **Trigger to Revisit** | User research indicates >50% of target users require visual UI for adoption |
| **Risk if Deferred** | Medium — DX and adoption friction |
| **Effort to Match Later** | **L** (full React app, real-time updates, graph rendering) |
| **Stakeholder Sign-off** | ⬜ Product: ⬜ Arch: ⬜ Eng: |

---

### EX-04: IO Managers (Declarative I/O)

| Field | Value |
|---|---|
| **Exception Type** | Deferral |
| **Sunset Date** | Milestone M2 |
| **Feature/Capability** | Dagster IO Managers for declarative input/output handling |
| **Dagster Behavior** | IO Managers abstract storage concerns. Supports `FilesystemIOManager`, `BigQueryIOManager`, etc. with automatic serialization. |
| **Decision** | **Defer → M2** |
| **Rationale (Business)** | MVP focuses on explicit I/O via `AssetContext.output()`. Declarative I/O is productivity, not correctness. |
| **Rationale (Technical)** | Requires stable storage layout, plugin interface, and output commit protocol (G5 prerequisite). |

**Minimum Stable IO Contract for M1:**

| Requirement | M1 Behavior | Purpose |
|---|---|---|
| Standard output path | `/{tenant}/{workspace}/assets/{asset_key}/{partition_key}/` | Future IO managers can rely on path convention |
| Metadata on write | `row_count`, `schema_hash`, `content_hash` emitted | Quality/lineage integration ready |
| Atomic commit marker | `.commit` marker written after successful output | Prevents partial read; promotion fencing ready |

| **User Impact** | Users explicitly manage I/O in asset code. More boilerplate but full control. |
| **Migration Path** | M1: Explicit `context.output(df, path)` with above contract<br>M2: `IOManager` interface + GCS/BigQuery implementations<br>M3: Community IO managers |
| **Workaround** | Helper functions: `write_parquet()`, `write_bigquery()` |
| **Trigger to Revisit** | >3 enterprise customers request declarative BigQuery/Snowflake IO |
| **Risk if Deferred** | Low — explicit I/O is functional |
| **Effort to Match Later** | **M** (interface design, 3-5 built-in managers, plugin system) |
| **Stakeholder Sign-off** | ⬜ Product: ⬜ Arch: ⬜ Eng: |

---

### EX-05a: Multi-Asset / Graph-Asset Execution

| Field | Value |
|---|---|
| **Exception Type** | Deferral |
| **Sunset Date** | Milestone M2 |
| **Feature/Capability** | `@multi_asset` (one computation → multiple assets) and `@graph_asset` (op composition into asset) |
| **Dagster Behavior** | `@multi_asset` produces multiple assets from single execution. `@graph_asset` composes ops into an asset (conceptually tied to op model). |
| **Decision** | **Defer → M2** (`@multi_asset`); **Won't Match** (`@graph_asset` — tied to op model per EX-01) |
| **Rationale (Technical)** | `@multi_asset` requires multi-output task semantics in planner. `@graph_asset` reintroduces op composition. |
| **User Impact** | Fan-out patterns (one query → multiple tables) require separate assets or wrapper in M1. |
| **Migration Path** | M1: Single `@asset` only<br>M2: `@multi_asset` with multi-output task support |
| **Workaround** | Wrapper asset calls shared logic, outputs to multiple paths |
| **Trigger to Revisit** | ELT/dbt integration requires multi-output asset support |
| **Risk if Deferred** | Low — workarounds exist |
| **Effort to Match Later** | **M** |
| **Stakeholder Sign-off** | ⬜ Product: ⬜ Arch: ⬜ Eng: |

---

### EX-05b: Observable Sources / External Assets

| Field | Value |
|---|---|
| **Exception Type** | Deferral |
| **Sunset Date** | Milestone M3 |
| **Feature/Capability** | Observable source assets (track external data freshness) and external assets (assets managed outside Dagster) |
| **Dagster Behavior** | Observable sources poll external systems to update freshness metadata. External assets represent data not materialized by Dagster. |
| **Decision** | **Defer → M3** |
| **Rationale (Technical)** | Observable sources require sensor-like polling infrastructure + freshness signals (F4). External assets require catalog metadata for non-managed data. |
| **User Impact** | Cannot track freshness of upstream data not materialized by Arco Flow in M1/M2. |
| **Migration Path** | M1-M2: Manual freshness tracking or external sensors<br>M3: Observable source assets + external asset registration |
| **Workaround** | Sensor that polls external system and triggers downstream runs |
| **Risk if Deferred** | Low — advanced feature |
| **Effort to Match Later** | **M** |
| **Stakeholder Sign-off** | ⬜ Product: ⬜ Arch: ⬜ Eng: |

---

### EX-06: Multi-Code-Location Workspace Model

| Field | Value |
|---|---|
| **Exception Type** | Simplified |
| **Feature/Capability** | Dagster workspace.yaml with multiple code locations (repos), each independently deployable |
| **Dagster Behavior** | Large organizations deploy assets from multiple repositories. Each code location has isolated Python environment. |
| **Decision** | **Simplified Model** |

**Supported vs Not Supported:**

| Capability | M1 Status | Notes |
|---|---|---|
| One deploy artifact per workspace | **Supported** | Standard model |
| One runtime environment per workspace | **Supported** | Single Python env |
| Multiple code locations in one workspace | **Not supported** | Use separate workspaces |
| Cross-repo asset dependencies inside workspace | **Not supported** | — |
| Cross-workspace "external dependency" edges | **Planned M2** | Explicit external asset references |

| **Rationale (Business)** | MVP targets single-team deployments. Multi-repo is enterprise feature. |
| **Rationale (Technical)** | Multi-code-location requires registry, cross-location planning, isolated worker pools. |
| **Evidence** | `python/arco/src/arco_flow/manifest/builder.py` — single manifest per deploy |
| **User Impact** | Teams consolidate assets into single unit or use separate workspaces. |
| **Migration Path** | M1: Single location per workspace<br>M2: External dependency edges to other workspaces<br>M3: Multiple locations per workspace if demand |
| **Workaround** | One workspace per repo; external orchestration for cross-workspace deps |
| **Trigger to Revisit** | Enterprise customer with >5 data teams needs unified graph |
| **Risk if Deferred** | Medium — enterprise adoption |
| **Effort to Match Later** | **L** (registry, cross-location planning, isolated workers) |
| **Stakeholder Sign-off** | ⬜ Product: ⬜ Arch: ⬜ Eng: |

---

### EX-07: GraphQL API

| Field | Value |
|---|---|
| **Exception Type** | Permanent |
| **Feature/Capability** | Dagster's GraphQL API for programmatic access |
| **Dagster Behavior** | Dagit and external tools use GraphQL for flexible queries. |
| **Decision** | **Won't Match** |

**API Strategy:**

| Phase | API Surface |
|---|---|
| M1 (MVP) | REST/JSON over HTTP (per unified platform design) |
| M2+ | Add gRPC streaming where performance-critical |
| Never | GraphQL |

| **Rationale (Business)** | REST for MVP simplicity; gRPC for streaming where needed. GraphQL adds complexity without clear benefit. |
| **Rationale (Technical)** | GraphQL requires schema maintenance, resolver implementation, N+1 prevention. |
| **Evidence** | `docs/plans/2025-01-12-arco-orchestration-design.md` — "Service Interface (gRPC API)" (API direction); `proto/arco/v1/orchestration.proto` contains domain messages but no GraphQL schema/server is present in-repo |
| **User Impact** | Programmatic access via REST (M1) or gRPC SDK (M2+). No GraphQL clients. |
| **Workaround** | REST endpoints; gRPC SDK (Python, Go, Rust) in M2+ |
| **Trigger to Revisit** | GraphQL-native ecosystem becomes strategic |
| **Risk if Permanent** | Low — REST/gRPC covers use cases |
| **Effort to Match Later** | **M** (GraphQL layer over existing services) |
| **Stakeholder Sign-off** | ⬜ Product: ⬜ Arch: ⬜ Eng: |

---

### EX-08: Run Launcher Plugins (K8s, ECS, Docker)

| Field | Value |
|---|---|
| **Exception Type** | Deferral |
| **Sunset Date** | Milestone M3 |
| **Feature/Capability** | Pluggable run launchers for Kubernetes, ECS, Docker, Celery |
| **Dagster Behavior** | Pluggable interface allows deployment on various compute platforms. |
| **Decision** | **Defer → M3** |
| **Rationale (Business)** | MVP uses Cloud Run. K8s is enterprise feature for on-prem/hybrid. |
| **Rationale (Technical)** | Worker contract (ORCH-ADR-004) abstracts compute backend. Launcher plugins can be added without breaking API. |

**Backend-Neutral Worker Contract (ORCH-ADR-004):**

| Requirement | Description |
|---|---|
| Callback endpoints | Standard result/heartbeat/failure endpoints |
| Auth model | Short-lived tokens scoped to task |
| Heartbeat/cancel semantics | `heartbeat_interval_ms`, `cancel_grace_period_ms` |
| Retry attempt identifiers | `attempt_id` in envelope; late results discarded |

| **Evidence** | `crates/arco-flow/src/dispatch/cloud_tasks.rs` — Cloud Tasks dispatcher |
| **User Impact** | Cloud Run only in M1/M2. K8s-only customers blocked until M3. |
| **Migration Path** | M1: Cloud Run<br>M2: Cloud Run + local (for dev)<br>M3: K8s launcher plugin |
| **Workaround** | Cloud Run with VPC connector |
| **Trigger to Revisit** | Enterprise customer with K8s-only policy and >$100k ARR |
| **Risk if Deferred** | Medium — enterprise adoption |
| **Effort to Match Later** | **M** (K8s operator + job management) |
| **Stakeholder Sign-off** | ⬜ Product: ⬜ Arch: ⬜ Eng: |

---

### EX-09: Asset Reconciliation Engine

| Field | Value |
|---|---|
| **Exception Type** | Deferral (Phased) |
| **Sunset Date** | M2 (partial), M3 (full) |
| **Feature/Capability** | Dagster's auto-materialize and freshness-based reconciliation |
| **Dagster Behavior** | Declarative automation: define freshness policies, Dagster figures out what's stale. |
| **Decision** | **Phased Delivery** |

**Phased Deliverable:**

| Phase | Capability | Description |
|---|---|---|
| M1 | None | Explicit runs only (schedules, sensors, manual) |
| M2 | Read-only staleness | "Who's stale and why" — `GetStalenessReport` API |
| M3 | Reconcile + propose | Reconcile → propose runs → optionally auto-trigger |

| **Rationale (Business)** | High-value but requires foundation (freshness tracking, partition status) |
| **Rationale (Technical)** | Depends on: partition status (C5), freshness signals (F4), sensor framework (B2) — all P0/P1 gaps |
| **Evidence** | Parity audit F4: freshness "not implemented"; C5: partition status "Designed" |
| **User Impact** | No "make everything fresh" button in M1. More operational overhead. |
| **Migration Path** | M1: Explicit triggers<br>M2: Staleness detection + explain<br>M3: Full reconciliation |
| **Workaround** | Schedule-driven materializations |
| **Trigger to Revisit** | Freshness policies + partition status implemented |
| **Risk if Deferred** | High — major Dagster DX differentiator |
| **Effort to Match Later** | **L** (requires freshness, partition status, planning) |
| **Stakeholder Sign-off** | ⬜ Product: ⬜ Arch: ⬜ Eng: |

---

## Architectural Decisions Required

> **Note:** Uses `ORCH-ADR-###` namespace to avoid collision with platform ADRs (ADR-001..006 in existing docs).

| ADR # | Decision Topic | Blocking Exceptions | Owner | Due Date | Status |
|---|---|---|---|---|---|
| ORCH-ADR-001 | PartitionKey canonical encoding | EX-05a, C3 gap | @platform | M1 | 🔴 Needed |
| ORCH-ADR-002 | ID wire formats (Rust/Python/proto) | All cross-language tests | @platform | M1 | 🔴 Needed |
| ORCH-ADR-003 | Scheduler deployment model | EX-02 (semantic parity) | @orchestration | M1 | 🔴 Needed |
| ORCH-ADR-004 | Worker contract specification | EX-08 (launcher plugins) | @orchestration | M1 | 🔴 Needed |
| ORCH-ADR-005 | IO Manager interface | EX-04 | @dx | M2 | 🟡 M2 |
| ORCH-ADR-006 | Multi-code-location registry | EX-06 | @platform | M3 | 🟡 M3 |

**Mapping to existing repo ADRs (where already defined):**

| ORCH-ADR | Existing ADR | Notes |
|---|---|---|
| ORCH-ADR-001 | ADR-011 | PartitionKey canonical encoding (see `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — "ADR-011: Canonical Partition Identity") |
| ORCH-ADR-002 | ADR-013 | ID wire formats across Rust/Python/proto (see `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — "ADR-013: ID Type Wire Formats") |
| ORCH-ADR-003 | ADR-014 (related) | Scheduler deployment model depends on HA/leadership; leader election strategy is ADR-014 (see `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — "ADR-014: Leader Election Strategy") |
---

## Milestone Gates

### Milestone M1: Orchestration MVP (0-30 days)

**Goal:** Deploy → Run → Observe loop works end-to-end

**Exit Criteria:**

- [ ] `arco-flow deploy` registers manifest with control plane
- [ ] `arco-flow run` triggers execution and returns run_id
- [ ] `arco-flow status` shows run/task states in real-time
- [ ] `arco-flow logs <run_id>` retrieves stdout/stderr
- [ ] `arco-flow status --watch` provides live updates
- [ ] All P0 acceptance criteria pass
- [ ] Exceptions EX-01 through EX-09 signed off by stakeholders

**Uncheatable Test:**

```text
TEST: m1_e2e_smoke
SETUP:
  - Clean workspace (no prior runs)
  - Python project with @asset definitions: A → B → C
EXECUTE:
  1. arco-flow deploy --workspace=test
  2. TriggerRun(selection={A}) — explicit selection
  3. Poll arco-flow status until completion (timeout 60s)
  4. TriggerRun(selection={A}, run_key="test-idempotency-key")
  5. TriggerRun(selection={A}, run_key="test-idempotency-key") — duplicate
  6. arco-flow logs <run_id>
ASSERT:
  - Deploy returns manifest_id
  - First run returns run_id_1 with state=SUCCEEDED
  - Plan contains ONLY tasks for A (no B, no C — no auto-materialize)
  - Second TriggerRun returns same run_id_1 (idempotency)
  - Logs contain expected output string
  - B and C remain unmaterialized
CHEAT DETECTION:
  - Plan inspection verifies selection semantics
  - Idempotency verified by run_id equality
  - Event log confirms B and C never entered RUNNING
```

### Milestone M2: Automation + Partitions (31-60 days)

**Goal:** Schedules, sensors, and partitioned assets work

**Exit Criteria:**

- [ ] Schedule fires and creates run at expected time
- [ ] Sensor with cursor evaluates correctly
- [ ] Partitioned asset creates correct number of tasks
- [ ] Backfill creates, progresses, and handles failures
- [ ] EX-03 (UI) has delivery timeline
- [ ] EX-04 (IO Managers) interface designed
- [ ] EX-09 (Reconciliation) staleness detection implemented

**Uncheatable Test:**

```text
TEST: m2_backfill_e2e
SETUP:
  - Partitioned asset with 30 daily partitions
  - Partition 15 configured to fail on first attempt
EXECUTE:
  1. Create backfill for all 30 partitions
  2. Wait for 10 partitions to complete
  3. Pause backfill
  4. Verify no new tasks start for 30s
  5. Resume backfill
  6. Wait for partition 15 to fail
  7. Invoke "retry failed partitions only"
  8. Wait for completion
ASSERT:
  - Exactly 30 distinct partition tasks completed
  - Partition 15 attempted twice (original + retry)
  - Pause stopped new task dispatch
  - Resume continued from correct position
  - No duplicate materializations (verified by counter side-effect)
CHEAT DETECTION:
  - Counter increment per partition detects duplicates
  - Timing assertions verify pause behavior
  - Retry-failed selection verified by task list
```

### Milestone M3: Production Hardening (61-90 days)

**Goal:** Enterprise-ready with quality gates and operational tooling

**Exit Criteria:**

- [ ] Check execution gates promotion
- [ ] Quarantine workflow implemented
- [ ] Rerun-from-failure works
- [ ] HA failover tested
- [ ] All acceptance criteria pass
- [ ] Runbooks exist for all failure modes
- [ ] EX-08 (K8s launcher) if enterprise demand

**Uncheatable Test:**

```text
TEST: m3_failover_e2e
SETUP:
  - Scheduler running on instance A
  - 10 concurrent runs with retryable tasks
EXECUTE:
  1. Submit 10 runs
  2. Wait for 5 tasks to be in RUNNING
  3. Kill instance A (ungraceful)
  4. Wait for instance B to acquire leadership
  5. Wait for all runs to complete
ASSERT:
  - New leader elected within 30s
  - No duplicate COMMITTED materializations (commit protocol)
  - Tasks may have retried (at-least-once) but output is exactly-once
  - All 10 runs reach terminal state (SUCCEEDED or FAILED)
  - Event log shows leadership transition
CHEAT DETECTION:
  - Materialization count verified via storage inspection
  - Commit markers prevent duplicate outputs
  - Leadership transition verified by distinct worker_ids in events
```

---

## Sign-off Tracking

| Exception | Engineering | Architecture | Product | Date |
|---|---|---|---|---|
| EX-01 | ⬜ | ⬜ | ⬜ | |
| EX-02 | ⬜ | ⬜ | ⬜ | |
| EX-03 | ⬜ | ⬜ | ⬜ | |
| EX-04 | ⬜ | ⬜ | ⬜ | |
| EX-05a | ⬜ | ⬜ | ⬜ | |
| EX-05b | ⬜ | ⬜ | ⬜ | |
| EX-06 | ⬜ | ⬜ | ⬜ | |
| EX-07 | ⬜ | ⬜ | ⬜ | |
| EX-08 | ⬜ | ⬜ | ⬜ | |
| EX-09 | ⬜ | ⬜ | ⬜ | |

**All exceptions signed off:** ⬜ Ready for production readiness review

---

## Appendix A: Exception Decision Framework

When evaluating whether a Dagster feature should be an exception vs a gap to close:

1. **Is it core to our value proposition?** If yes → must match
2. **Does it block >50% of target users?** If yes → must match
3. **Is there a reasonable workaround?** If yes → consider exception
4. **Is it a different architectural choice with equivalent semantics?** If yes → exception (not gap)
5. **Is it a power-user/enterprise feature?** If yes → consider deferral

**Default stance:** When in doubt, it's a gap to close, not an exception.

---

## Appendix B: Out of Scope (Not Parity Exceptions)

The following are **not parity exceptions** but product-category mismatches that don't require stakeholder sign-off:

### Dagster Cloud Managed Infrastructure

| Field | Value |
|---|---|
| **Why out of scope** | Dagster Cloud's managed control plane is a product offering, not an OSS feature. Arco Flow IS the managed offering — we are the equivalent of Dagster Cloud, not Dagster OSS. |
| **Our equivalent** | Platform is GCP-native with Cloud Run, Cloud Tasks, managed control plane. Customers never operate infrastructure. |
| **Implication** | Not a gap; this is our value proposition. |

---

## Appendix C: Cross-References

| This Document | Related Platform Doc |
|---|---|
| EX-02 (scheduler model) | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — "Scheduler Deployment Model" |
| EX-03 (UI) | `docs/plans/ARCO_ARCHITECTURE_PART2_OPERATIONS.md` — "REST API Design" |
| EX-07 (API strategy) | `docs/plans/2025-01-12-arco-unified-platform-design.md` — "MVP contracts are JSON over HTTP" |
| ORCH-ADR-001..006 | Does not conflict with platform ADR-001..006 (different namespace) |
| Milestone M1/M2/M3 | Maps to Orchestration MVP → Automation → Hardening (not platform Phase 1/2/3) |

