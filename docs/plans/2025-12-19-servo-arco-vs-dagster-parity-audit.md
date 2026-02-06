# Servo/Arco vs Dagster Parity Audit — Full Review + Gap Analysis + Exception Planning Inputs

**Date:** 2025-12-19  
**Author:** Codex (lead architect audit)  

## Methodology + rigor rules (how to read this report)

- **Evidence rule:** Every Servo/Arco parity claim is backed by a concrete repo citation: `filepath — heading` (docs) or `filepath — symbol` (code/proto). If I couldn’t find evidence, I mark it **UNKNOWN**.
- **Drift rule:** If something exists only in docs (not code), I mark it **Designed** (not Implemented) and call out doc↔code drift as a risk.
- **Conflict rule:** When docs conflict, I prefer: **Approved > Draft; later-date docs > earlier-date**; conflicts are called out explicitly.
- **Dagster baseline:** The “robust feature set list” referenced in the prompt was **not present in this repo/thread context**, so I used the checklist template in **Appendix A** and extended it where needed (**added by architect**).

## Evidence index (primary sources used)

**Design + architecture**
- `docs/plans/2025-01-12-arco-orchestration-design.md` — “Execution Plan & State Machine”, “Event Sourcing & Storage”, “Developer Experience (SDK & CLI)”
- `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Scheduler & Dispatcher Loop”, “Fairness & Quota Enforcement”, “Backfill Orchestration”, “Observability”, “Output Commit Protocol”, “Cancellation Semantics”, “Projection Rebuild Strategy” (draft)
- `docs/plans/2025-01-12-arco-unified-platform-design.md` — “Deterministic Planning Using Catalog”, “MVP Cut + Roadmap”, “Reliability + Consistency Guarantees”
- `docs/plans/ARCO_ARCHITECTURE_PART1_CORE.md` — “Operational Types”, “Manifest and Commit Types”
- `docs/plans/ARCO_ARCHITECTURE_PART2_OPERATIONS.md` — “Servo Integration”
- `docs/plans/ARCO_TECHNICAL_VISION.md` — “Orchestration Features (via Servo)”

**Recent deltas / hardening plans**
- `docs/plans/2025-12-14-part3-orchestration-mvp.md` — “Task 7: Implement Dependency-Aware Scheduler”, “Task 9: Add Execution Events for Tier 2 Persistence”
- `docs/plans/2025-12-16-part4-integration-testing.md` — “Canonical Contracts (Non-Negotiable)”
- `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — “Gate A: Determinism”, “Gate B: State Machine Observability”, “Gate C: Distributed Correctness”, “Gate D: Production Features”
- `docs/plans/2025-12-18-gate5-hardening.md` — “Uncheatable Tests Summary” (platform reliability)
- `docs/plans/2025-12-19-gate5-hardening-plan-patch.md` — “Critical Contradictions to Resolve” (authoritative delta)

**Code/proto evidence (selected)**
- `crates/arco-flow/src/plan.rs` — `PlanBuilder::build`, `compute_fingerprint`, `TaskSpec`, `ResourceRequirements`
- `crates/arco-flow/src/task.rs` — `TaskState`, `TransitionReason`, `TaskExecution::try_terminal_transition`, `TaskExecution::is_zombie`
- `crates/arco-flow/src/run.rs` — `Run`, `RunState`, `RunTrigger`
- `crates/arco-flow/src/scheduler.rs` — `Scheduler::dispatch_ready_tasks`, `Scheduler::record_task_result`, `RetryPolicy`
- `crates/arco-flow/src/events.rs` — `EventEnvelope`, `ExecutionEventData`, `EventBuilder`
- `crates/arco-flow/src/outbox.rs` — `LedgerWriter::append`, `LedgerWriter::append_all`
- `crates/arco-flow/src/dispatch/mod.rs` — `TaskQueue`, `TaskEnvelope`, `EnqueueResult`
- `crates/arco-flow/src/store/mod.rs` — `Store` (CAS semantics), `CasResult`
- `crates/arco-flow/src/leader/mod.rs` — `LeaderElector`
- `proto/arco/v1/orchestration.proto` — `Plan`, `RunState`, `TaskState`, `TriggerType`
- `proto/arco/v1/common.proto` — `AssetId`, `RunId`, `TaskId`, `PartitionKey`
- `python/arco/src/servo/asset.py` — `asset` decorator
- `python/arco/src/servo/manifest/builder.py` — `ManifestBuilder.build`
- `python/arco/src/servo/cli/commands/deploy.py` — `run_deploy` (deploy is manifest-only today)

---

## 1) Executive Summary

### Overall parity verdict: **Partial**

Servo/Arco is **strong on deterministic planning primitives + event envelope design + multi-tenant fairness intent**, but is **not yet in operational/DX parity with Dagster** for the workflows users rely on daily: schedules/sensors, backfills UX, reexecution tooling, run storage + UI, and “deploy/run/status” end-to-end.

**Key reason:** Much of the Dagster-like surface area is **Designed** in docs (Jan 12 + later hardening plans) but **not Implemented** in code paths that users can exercise today (notably the Python CLI and a control-plane API for triggering and observing runs).

### Top 10 parity-blocking gaps (prioritized)

1. **Run/task/event control plane is missing** (**E1–E2, E9–E10**): without it, users can’t trigger/observe/cancel runs or debug “what happened”. Designed in `docs/plans/2025-01-12-arco-orchestration-design.md` — “Service Interface (gRPC API)”, but not found implemented in code (see **UNKNOWNs**).
2. **CLI is not end-to-end** (**H2, D10**): without a working deploy→run→status/logs loop, adoption is blocked. Evidence: `python/arco/src/servo/cli/main.py` — `deploy`, `run`, `status` and `python/arco/src/servo/cli/commands/deploy.py` — `run_deploy` (manifest-only).
3. **Durable distributed Store + leader election are missing** (**G6**): without them, multi-instance correctness/HA scheduling isn’t possible. Evidence: `crates/arco-flow/src/store/memory.rs` — `InMemoryStore` and `crates/arco-flow/src/leader/mod.rs` — `LeaderElector`; planned in `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — “Gate C: Distributed Correctness”.
4. **Worker runtime contract is missing** (**D1, D9**): without a real executor + callbacks, timeouts/cancel/IO/logs remain unvalidated and unshippable. Designed in `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Worker Runtime”; envelope exists in `crates/arco-flow/src/dispatch/mod.rs` — `TaskEnvelope`.
5. **Schedules + sensors runtime is missing** (**B1–B2, B7**): automation (cron/event-driven) is not delivered. Placeholder trigger types exist in `crates/arco-flow/src/run.rs` — `TriggerType`; design in `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Scheduler & Dispatcher Loop”.
6. **Partition/backfill operator workflows are missing** (**C5–C10**): without backfills + partition status, partitioned orchestration isn’t usable in practice. Designed in `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Backfill Orchestration”.
7. **Reexecution workflows are missing** (**E6–E8**): Dagster-grade debugging loops (rerun from failure, subset selection) aren’t available. Retry primitives exist (`crates/arco-flow/src/scheduler.rs` — `RetryPolicy`), but rerun semantics are only Designed in docs.
8. **Cross-language determinism drift is unresolved** (**A5–A7, C3, G7**): state enums and IDs/partition keys diverge across Rust/Python/proto, creating correctness + upgrade hazards. Example drift: `proto/arco/v1/orchestration.proto` — `TaskState` vs `crates/arco-flow/src/task.rs` — `TaskState`.
9. **End-to-end delivery guarantees are incomplete** (**G4–G5**): without outbox/dedupe/reconciliation + commit protocol, duplicates/torn writes are likely. Evidence: `crates/arco-flow/src/outbox.rs` — `LedgerWriter::append`; designed protocol in `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Output Commit Protocol”.
10. **Quality execution + gating are missing** (**F2–F3**): checks don’t actually gate promotion/materialization, weakening data trust. Defined in `python/arco/src/servo/types/check.py` — `Check`; execution/gating Designed in `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Gating Semantics”.

### Top 5 areas where we exceed Dagster (if any)

1. **Serverless-first, event-sourced catalog + projection rebuild as a first-class invariant** is more deeply specified than Dagster OSS, especially around manifest CAS + publish fencing (see `docs/plans/2025-12-19-gate5-hardening-plan-patch.md` — “Patch Delta Checklist” and `crates/arco-core/src/publish.rs` — `PublishPermit`).
2. **Compile-time capability boundaries for storage access** (planned as “uncheatable tests” for IAM/prefix enforcement) is unusually rigorous (`docs/plans/2025-12-18-gate5-hardening.md` — “Uncheatable Tests Summary”).
3. **Deterministic plan fingerprinting using canonical JSON + semantic TaskKey** is implemented in `crates/arco-flow/src/plan.rs` — `compute_fingerprint` and is designed as a platform invariant (see `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — “Gate A: Determinism”).
4. **Multi-tenant fairness as a core scheduling primitive** (DRR) is implemented as a module (`crates/arco-flow/src/quota/drr.rs` — `DrrScheduler`) and heavily emphasized in design (`docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Fairness & Quota Enforcement”).
5. **Ledger-first operational metadata model** (append-only log + compaction) is foundational across Catalog + Flow and lends itself to forensic “what happened” reconstruction when completed (`docs/plans/2025-12-16-part4-integration-testing.md` — “Canonical Contracts (Non-Negotiable)”).

---

## 2) Glossary mapping (Dagster → Servo/Arco)

> **Note:** Some terms are not stabilized in code yet; those are marked **UNKNOWN**.

| Dagster term | Servo/Arco term | Evidence |
|---|---|---|
| Asset | Asset (`@asset`) | `python/arco/src/servo/asset.py` — `asset` |
| Asset key | `AssetKey` | `crates/arco-flow/src/plan.rs` — `AssetKey`; `python/arco/src/servo/types/asset.py` — `AssetKey` |
| Run | `Run` | `crates/arco-flow/src/run.rs` — `Run`; `proto/arco/v1/orchestration.proto` — `Run` |
| Step/Op (Dagster) | Task | `crates/arco-flow/src/task.rs` — `TaskExecution`, `TaskState`; `proto/arco/v1/orchestration.proto` — `TaskExecution` |
| Schedule | ScheduleDefinition / RunTrigger(SCHEDULED) | `docs/plans/2025-01-12-arco-orchestration-design.md` — “Domain Types (Protobuf)”; `crates/arco-flow/src/run.rs` — `TriggerType` |
| Sensor | External dependency / RunTrigger(SENSOR) | `docs/plans/2025-01-12-arco-orchestration-design.md` — “ExecutionPlan” (external_dependencies); `crates/arco-flow/src/run.rs` — `TriggerType` |
| Partition | `PartitionKey` | `crates/arco-core/src/partition.rs` — `PartitionKey`; `proto/arco/v1/common.proto` — `PartitionKey` |
| Backfill | Backfill (planned) | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Backfill Orchestration” |
| Event log | Tier-2 ledger events / execution events | `crates/arco-flow/src/events.rs` — `EventEnvelope`; `docs/plans/2025-12-16-part4-integration-testing.md` — “Event Envelope (CloudEvents-compatible)” |
| Dagit UI | **UNKNOWN** (Arco Browser focuses on Catalog) | `docs/plans/ARCO_ARCHITECTURE_PART2_OPERATIONS.md` — “REST API Design” (catalog); orchestration UI not found |

---

## 3) Parity Matrix (Dagster capability checklist → Servo/Arco)

Legend:
- **Servo/Arco equivalent?**: Yes / Partial / No / Unknown
- **Current status**: Implemented / Designed / Planned / Not Started / Unknown
- **ID**: Maps to Appendix A

### A) Core orchestration model + semantics

| ID | Dagster capability | What “good” looks like in Dagster (short) | Servo/Arco equivalent? | Current status | Evidence | Gap description (if Partial/No) | Recommended approach (1–3 bullets) | Complexity + key dependencies | Risk if missing |
|---|---|---|---|---|---|---|---|---|---|
| A1 | Asset-centric authoring model | Assets are first-class; dependencies drive execution | Partial | Implemented | `python/arco/src/servo/asset.py` — `asset`; `python/arco/src/servo/types/asset.py` — `AssetDefinition` | SDK exists; no end-to-end runtime to execute assets from manifests | - Implement control plane `TriggerRun` + worker runtime<br>- Wire manifest → planner → scheduler | L — deps: API, worker runtime, durable store | DX/correctness |
| A2 | Dependency resolution + DAG construction | System derives a DAG, validates dependencies, detects cycles | Partial | Implemented | `python/arco/src/servo/_internal/introspection.py` — `extract_dependencies`; `crates/arco-flow/src/dag.rs` — `Dag::toposort_by_key`; `crates/arco-flow/src/plan.rs` — `PlanBuilder::build` | No end-to-end planner from manifest definitions to a task DAG; cycle *path* reporting is minimal | - Implement planner: manifest → task graph → `Plan`<br>- Improve cycle reporting (path) | M — deps: planner API + canonical `TaskKey` | correctness/DX |
| A3 | Deterministic planning + stable ordering | Same inputs yield same ordered plan (stable topo sort) | Yes | Implemented | `crates/arco-flow/src/plan.rs` — `PlanBuilder::build`; `crates/arco-flow/src/dag.rs` — `Dag::toposort_by_key` | — | - Bind “inputs” precisely (see A4) | M — deps: catalog snapshot binding | correctness |
| A4 | Plan fingerprinting / reproducibility primitives | Plan spec has stable fingerprint; supports reproducibility and caching | Partial | Implemented | `crates/arco-flow/src/plan.rs` — `compute_fingerprint`, `Plan.fingerprint`; `docs/plans/2025-01-12-arco-unified-platform-design.md` — “Section F: Deterministic Planning Using Catalog” | Fingerprint exists, but plan inputs/anchors (code version + catalog snapshot binding) are not implemented end-to-end | - Add `code_version_id` + `metadata_snapshot_id` to plan/run records<br>- Persist snapshot binding metadata | M — deps: catalog snapshot store + API + proto alignment | correctness/auditability |
| A5 | Semantic task identity (asset + partition + operation) | Stable step keys enable deterministic ordering and reruns | Partial | Implemented | `crates/arco-flow/src/task_key.rs` — `TaskKey`, `TaskOperation`; `crates/arco-flow/src/plan.rs` — `build_task_keys` | Not surfaced in user-facing APIs/UI; cross-language identity is not standardized | - Expose `TaskKey` in API/event views<br>- Standardize cross-language `AssetKey`/`PartitionKey` formats | S — deps: proto + SDK updates | correctness/DX |
| A6 | Run lifecycle states + transitions | Clear states incl. cancelling/cancelled/timeouts with valid transitions | Partial | Implemented | `crates/arco-flow/src/run.rs` — `RunState`; `docs/plans/2025-01-12-arco-orchestration-design.md` — “State Machine”; `proto/arco/v1/orchestration.proto` — `RunState` | Proto lacks `CANCELLING`; state machine is not exposed via any API/UI | - Align proto↔engine run states<br>- Implement run persistence + API surfaces | M — deps: `Store`, control plane API, proto changes | correctness/ops |
| A7 | Task lifecycle states + transitions | Rich task lifecycle incl. READY/DISPATCHED/RETRY_WAIT; explicit transition reasons | Partial | Implemented | `crates/arco-flow/src/task.rs` — `TaskState`, `TransitionReason`; `docs/plans/2025-01-12-arco-orchestration-design.md` — “State Machine”; `proto/arco/v1/orchestration.proto` — `TaskState` | Proto omits key states; end-to-end transition enforcement not wired through durable store | - Align proto↔engine task states<br>- Enforce transitions via `Store::cas_task_state` | M — deps: store implementation + API | correctness/ops |
| A8 | Failure semantics (skip downstream) | Downstream skipped on upstream failure; independent branches continue | Partial | Implemented | `crates/arco-flow/src/scheduler.rs` — `SchedulerConfig.continue_on_failure`, `Scheduler::skip_downstream_tasks` | Semantics exist, but not user-configurable via SDK/API and not visible in UX | - Add failure policy to manifest/plan<br>- Expose skip reasons in event log + UI | M — deps: planner + API + event projections | DX/correctness |
| A9 | Retry semantics (policy, backoff, max attempts) | Step-level retries with backoff; configurable; visible history | Partial | Implemented | `crates/arco-flow/src/scheduler.rs` — `RetryPolicy`; `crates/arco-flow/src/events.rs` — `ExecutionEventData::TaskRetryScheduled` | Not connected to per-asset execution policy; no worker retry coordination; no UX | - Plumb retry config from SDK manifest to plan/task execution<br>- Persist attempts + expose in UI | M — deps: worker runtime + API + store | correctness/DX |
| A10 | Idempotent terminal transitions + late result handling | Duplicate/late callbacks safe; exactly-once effects for outputs | Partial | Implemented | `crates/arco-flow/src/task.rs` — `TaskExecution::try_terminal_transition`; `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Handling Late Results”; `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Output Commit Protocol” | Task-level idempotency exists; system-wide commit/promotion protocol not implemented | - Implement output commit + promotion fencing<br>- Enforce idempotency keys end-to-end | L — deps: worker runtime + storage model + publish fencing | correctness |

### B) Scheduling, sensors, triggers

| ID | Dagster capability | What “good” looks like in Dagster (short) | Servo/Arco equivalent? | Current status | Evidence | Gap description (if Partial/No) | Recommended approach (1–3 bullets) | Complexity + key dependencies | Risk if missing |
|---|---|---|---|---|---|---|---|---|---|
| B1 | Cron schedules | Declarative schedules with tick history + error visibility | Partial | Designed | `docs/plans/2025-01-12-arco-orchestration-design.md` — “Domain Types (Protobuf)”; `crates/arco-flow/src/run.rs` — `TriggerType::Scheduled`; `python/arco/src/servo/manifest/model.py` — `AssetManifest.schedules` | No schedule definitions in SDK/CLI; no tick storage; no evaluator/service | - Add schedule definition surface (SDK/manifest/config)<br>- Build schedule evaluator + tick store | L — deps: control plane API + durable store + leader election | DX/operability |
| B2 | Sensors (event-driven) | Sensors with cursors, reliable evaluation, and idempotent triggering | Partial | Designed | `docs/plans/2025-01-12-arco-orchestration-design.md` — “ExecutionPlan” (external_dependencies); `crates/arco-flow/src/run.rs` — `TriggerType::Sensor` | No sensor abstraction/cursor storage; no evaluation runtime | - Define sensors + cursor model<br>- Integrate event sources (GCS/PubSub) | L — deps: API + store + event integrations | correctness/DX |
| B3 | Conditional run triggering | Trigger only when conditions match (e.g., data arrived, quality gate) | No | Not Started | Not found in provided design docs or code | Conditions must be encoded somewhere (schedule/sensor definitions or policies) | - Extend schedules/sensors with condition predicates<br>- Add dry-run/explain tooling for conditions | M — deps: sensor framework + policy engine | DX |
| B4 | Trigger dedupe / run idempotency (`run_key`) | Prevent duplicate runs from repeated sensor ticks | No | Not Started | Not found; task idempotency exists (`crates/arco-flow/src/dispatch/mod.rs` — `TaskEnvelope::idempotency_key`) but not run-level | Duplicate triggers will create duplicate runs and costs | - Add `RunIdempotencyKey` to TriggerRun API/store<br>- Enforce uniqueness per tenant/workspace | M — deps: TriggerRun API + durable store | cost/correctness |
| B5 | Run queue semantics + backpressure | Global run queue, queued state, controlled dispatch under load | Partial | Planned | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Scheduler & Dispatcher Loop”; `crates/arco-flow/src/dispatch/mod.rs` — `TaskQueue` | Task queue exists; run queue/coordinator is missing | - Implement global scheduler loop (multi-run)<br>- Add backpressure signals + queue depth metrics | L — deps: `Store` + `LeaderElector` + dispatch backend | operability/cost |
| B6 | Concurrency gates + prioritization | Concurrency limits + prioritization across runs/steps | Partial | Implemented | `crates/arco-flow/src/scheduler.rs` — `SchedulerConfig.max_parallelism`; `crates/arco-flow/src/plan.rs` — `TaskSpec.priority`; `crates/arco-flow/src/quota/mod.rs` — `QuotaManager` | Only per-run limits; no global/run-level prioritization and fairness across tenants | - Add global scheduling + per-scope concurrency limits<br>- Define prioritization policy (run vs task) | L — deps: store + scheduler service + API | operability/cost |
| B7 | Scheduler/sensor observability (ticks/cursors) | Tick logs, cursor state, and error visibility | Partial | Designed | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Observability”; `crates/arco-flow/src/metrics.rs` — `FlowMetrics` | Metrics exist, but no tick/cursor entities exist to debug automation | - Add Tick/Cursor entities to store<br>- Surface via CLI/UI | M — deps: schedule/sensor framework + API + UI | DX/operability |

### C) Partitions + backfills

| ID | Dagster capability | What “good” looks like in Dagster (short) | Servo/Arco equivalent? | Current status | Evidence | Gap description (if Partial/No) | Recommended approach (1–3 bullets) | Complexity + key dependencies | Risk if missing |
|---|---|---|---|---|---|---|---|---|---|
| C1 | Partition definitions (time/static) | First-class partition definitions and selection | Partial | Implemented | `python/arco/src/servo/types/partition.py` — `DailyPartition`, `HourlyPartition`, `PartitionStrategy`; `python/arco/src/servo/asset.py` — `asset` | Partition definitions exist, but planner/runtime doesn’t expand partitions into task plans | - Implement partition expansion in planner<br>- Persist partition sets/status in catalog | L — deps: planner + catalog snapshot + API | correctness/DX |
| C2 | Multi-dimensional partitions | Multiple dimensions per asset; correct dependency resolution | Partial | Implemented | `crates/arco-core/src/partition.rs` — `PartitionKey`; `python/arco/src/servo/types/partition.py` — `PartitionKey` | No runtime semantics; no partition-aware scheduling/backfills | - Add partition-aware planning + scheduling<br>- Add partition status model | L — deps: planner + store + catalog | correctness |
| C3 | Cross-language canonical partition identity | Identical partition key encoding across languages/services | Partial | Implemented | `crates/arco-core/src/partition.rs` — `PartitionKey::canonical_string`; `crates/arco-core/tests/cross_language/partition_key_test.rs` — `test_canonical_is_path_safe`; `python/arco/tests/unit/test_partition.py` — `TestPartitionKey.test_canonical_order` | Rust and Python canonical forms differ (base64+`key=tag:val` vs JSON map) | - Choose one canonical encoding (ADR) and enforce everywhere<br>- Add shared golden fixtures consumed by Rust + Python | M — deps: ADR-011/encoding decision + SDK + engine updates | correctness/auditability |
| C4 | Partition mappings | Map upstream partitions to downstream (identity/latest/window) | Partial | Implemented | `python/arco/src/servo/types/asset.py` — `DependencyMapping`; `docs/plans/2025-01-12-arco-orchestration-design.md` — “Key Decisions” | Mapping exists only as types; planner semantics not implemented | - Implement mapping expansion in planner<br>- Add determinism + correctness tests | L — deps: planner + snapshot binding | correctness |
| C5 | Partition status tracking + UX | View missing/failed/stale partitions; drive incremental runs | Partial | Designed | `docs/plans/ARCO_TECHNICAL_VISION.md` — “Partition-Aware Scheduling”; `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Backfill Orchestration” | No partition status store, UI, or APIs | - Add partition status model + APIs<br>- Integrate status into scheduling decisions | L — deps: catalog schema + compaction + UI | DX |
| C6 | Backfill creation (range) | Create backfills over partition ranges with correctness | Partial | Designed | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Backfill Orchestration”; `crates/arco-flow/src/run.rs` — `TriggerType::Backfill` | No backfill service/controller implemented | - Implement Backfill entity + controller<br>- Provide CLI/API surfaces | L — deps: planner + API + durable store | DX/operability |
| C7 | Backfill chunking + parallelism | Chunk and parallelize safely with progress accounting | No | Designed | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Backfill Chunking” | Missing implementation | - Implement chunk planner + run fan-out<br>- Add throttles + quotas | L — deps: backfill controller + quotas | operability/cost |
| C8 | Backfill pause/resume/cancel | Operator control over long backfills | No | Designed | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Backfill CLI” | Missing implementation | - Add Backfill state machine + commands<br>- Ensure idempotent resume | L — deps: backfill store + API | DX/operability |
| C9 | Retry failed partitions only | Retry only failed partitions from a backfill | No | Designed | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Backfill CLI” | Missing implementation | - Track per-partition outcomes<br>- Add “failed-only” selector | M — deps: partition status tracking | DX/correctness |
| C10 | Partial backfills (subset) | Backfill subset of assets/partitions without full rerun | No | Designed | `docs/plans/2025-01-12-arco-orchestration-design.md` — “Key Decisions” (partition mapping MVP implies subsets) | Missing implementation | - Support asset subset selection + partition subsets<br>- Persist backfill spec + fingerprint | L — deps: planner subset selection + UI | DX |

### D) Compute/execution + resource model

| ID | Dagster capability | What “good” looks like in Dagster (short) | Servo/Arco equivalent? | Current status | Evidence | Gap description (if Partial/No) | Recommended approach (1–3 bullets) | Complexity + key dependencies | Risk if missing |
|---|---|---|---|---|---|---|---|---|---|
| D1 | Worker/runtime contract | Stable step execution contract (payload, callbacks, auth) | Partial | Implemented | `crates/arco-flow/src/dispatch/mod.rs` — `TaskEnvelope`; `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Worker Runtime”; `python/arco/src/servo/cli/commands/run.py` — `run_asset` (TODO) | No worker runtime exists in repo; no callback endpoints; no auth story implemented | - Implement worker executor (Python) + callback API<br>- Wire dispatch backend (Cloud Tasks) | L — deps: API + worker + dispatch + auth | correctness |
| D2 | Local dev parity vs cloud parity | Same semantics locally and in cloud (fast iteration) | Partial | Designed | `docs/plans/2025-01-12-arco-orchestration-design.md` — “Local Development”; `python/arco/src/servo/cli/main.py` — `deploy`, `run`, `status`, `validate`, `init` | “servo dev”/local runtime not implemented; only library-level tests exist | - Implement local executor (in-memory store/queue)<br>- Add `servo dev` CLI + local UI hooks | L — deps: local runtime + API client stubs | DX |
| D3 | Heartbeats + zombie detection | Detect stalled steps; requeue/fail safely | Partial | Implemented | `crates/arco-flow/src/task.rs` — `TaskExecution::is_zombie`; `crates/arco-flow/src/scheduler.rs` — `record_task_heartbeat_at`; `crates/arco-flow/src/store/mod.rs` — `Store::get_zombie_tasks` | Not wired into a real distributed scheduler loop; two-timeout model inconsistently applied | - Drive zombie detection from durable store + timeouts<br>- Add worker heartbeat protocol | L — deps: store + scheduler service + worker | operability |
| D4 | Timeouts (dispatch-ack, heartbeat, execution) | Enforced timeouts with clear semantics and visibility | Partial | Implemented | `crates/arco-flow/src/task.rs` — `TaskTimeoutConfig`; `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — “PR-7: Two-Timeout Heartbeat Model” | Enforcement not end-to-end (no worker/runtime); scheduler uses a separate stale-heartbeat path | - Make timeouts first-class in scheduler loop<br>- Expose timeout reasons via events/UI | M — deps: worker + store + event projections | operability/correctness |
| D5 | Cancellation (graceful + force) | Cancel runs/steps reliably; force-cancel stuck work | Partial | Implemented | `crates/arco-flow/src/scheduler.rs` — `Scheduler::cancel_run`; `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Cancellation Semantics” | Cancel only affects some states; no worker signal/force cancel integrated | - Implement worker cancel signal + force cancel<br>- Ensure late results handled safely | L — deps: worker + scheduler loop + store | correctness/operability |
| D6 | Resource model (cpu/mem/time) | Deterministic resource hints and enforcement | Partial | Implemented | `crates/arco-flow/src/plan.rs` — `ResourceRequirements`; `proto/arco/v1/orchestration.proto` — `ResourceRequirements`; `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — “ADR-010: Canonical JSON Serialization” | No enforcement path; resource hints not applied to runtime | - Enforce resources in worker (Cloud Run/Tasks)<br>- Validate at deploy/plan time | M — deps: worker runtime + cloud deploy plumbing | cost/operability |
| D7 | IO abstraction | Pluggable IO managers / storage integration for inputs/outputs | Partial | Implemented | `python/arco/src/servo/types/asset.py` — `IoConfig`; `python/arco/src/servo/context.py` — `AssetContext.output`; `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Output Commit Protocol” | No runtime IO implementation; no stable IO manager interface | - Define IO manager interface (Python)<br>- Implement storage layout + commit protocol | L — deps: worker runtime + storage design | correctness |
| D8 | Secrets/variables + env separation | Secure secret resolution; environment-specific config | Partial | Designed | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Secret Resolution”; `python/arco/src/servo/cli/config.py` — `Config.api_key` | No secret resolution implementation for worker execution | - Implement secret provider abstraction<br>- Add env/workspace scoping rules | M — deps: auth/RBAC + secret backend | security |
| D9 | Retry coordination (worker + scheduler) | Scheduler and worker cooperate on retries and dedupe | Partial | Implemented | `crates/arco-flow/src/scheduler.rs` — `record_task_failure` (RetryWait); `crates/arco-flow/src/events.rs` — `TaskRetryScheduled` | Worker/runtime not implemented, so retry coordination semantics are unvalidated | - Implement worker result callbacks with attempt IDs<br>- Enforce idempotency at state/event layers | L — deps: worker + API + store | correctness |
| D10 | Deployment model (workspaces, code versions) | Code locations/versions; deploy once, run many; promotion story | Partial | Implemented | `python/arco/src/servo/manifest/builder.py` — `ManifestBuilder.build`; `python/arco/src/servo/cli/commands/deploy.py` — `run_deploy`; `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Build & Artifact Lifecycle” | Deploy is manifest-only today; artifact build/versioning/runtime loading not implemented | - Implement DeployService + artifact build flow<br>- Persist code version + provenance in runs | L — deps: control plane API + build system + worker image strategy | DX/operability |

### E) Observability + debugging + reexecution

| ID | Dagster capability | What “good” looks like in Dagster (short) | Servo/Arco equivalent? | Current status | Evidence | Gap description (if Partial/No) | Recommended approach (1–3 bullets) | Complexity + key dependencies | Risk if missing |
|---|---|---|---|---|---|---|---|---|---|
| E1 | Execution event logs (structured, queryable) | Durable event log supports “what happened” reconstruction | Partial | Implemented | `crates/arco-flow/src/events.rs` — `EventEnvelope`; `crates/arco-flow/src/outbox.rs` — `LedgerWriter::append`; `docs/plans/2025-12-16-part4-integration-testing.md` — “Event Envelope (CloudEvents-compatible)” | Write path exists; read/projection/UI path is missing for flow | - Implement flow projections (compactor) + query API<br>- Add run/task/event views | L — deps: flow compactor + API + UI | operability/DX |
| E2 | Run/task views | Run list/detail, task list/detail, attempts, timings | No | Not Started | `python/arco/src/servo/cli/commands/status.py` — `show_status` (TODO); `proto/arco/v1/orchestration.proto` — `Run`, `TaskExecution` | No implemented API/UI to view runs/tasks | - Implement `GetRun/ListRuns/ListTasks` APIs<br>- Back projections with durable store | L — deps: store + API + projection schema | DX/operability |
| E3 | Structured logs + compute log retrieval | Per-step logs retrievable in UI/CLI | Partial | Designed | `crates/arco-core/src/observability.rs` — `init_logging`; `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Logging” | Logging infra exists, but log persistence + retrieval UX is missing | - Decide log storage model (events vs objects)<br>- Add `servo logs` + API endpoints | L — deps: worker runtime + storage + indexing | DX |
| E4 | Metrics (dashboards/alerts) | Metrics exported and dashboards/alerts defined | Partial | Implemented | `crates/arco-flow/src/metrics.rs` — `FlowMetrics`; `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Metrics (Prometheus)” | Instrumentation exists; exporters/dashboards/alerts not wired | - Add Prometheus exporter + dashboards<br>- Define SLOs and alerts | M — deps: runtime services + ops stack | operability |
| E5 | Tracing (context propagation) | Distributed tracing across scheduler/worker + UI correlation | Partial | Designed | `crates/arco-flow/src/plan.rs` (tracing instrumentation); `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Tracing (OpenTelemetry)” | No OTel export or worker propagation implemented | - Add OTel exporter + trace context in TaskEnvelope<br>- Propagate in worker | L — deps: worker runtime + tracing infra | operability/DX |
| E6 | Retry history + timeline reconstruction | Retry/attempt history visible; timeline reconstructable | Partial | Implemented | `crates/arco-flow/src/events.rs` — `TaskRetryScheduled`, `TaskRetried`; `crates/arco-flow/src/run.rs` — `Run::next_sequence` | Timeline can exist in events but no read model/UI exists | - Build projection that materializes retry timelines<br>- Surface in run/task views | M — deps: flow projections + UI | DX |
| E7 | Rerun from failure | Rerun only failed subset with preserved context/config | Partial | Designed | `docs/plans/2025-01-12-arco-orchestration-design.md` — “ExecutionService” (RetryRun); no implementation found | Core Dagster workflow missing | - Implement “reexecute-from-failure” planning mode<br>- Persist lineage links between runs | L — deps: planner + durable store + UI | DX/correctness |
| E8 | Subset reexecution | Execute subset of steps/assets (selection) | No | Not Started | Not found in provided design docs or code | No subset selection semantics | - Add selection spec to TriggerRun/RetryRun<br>- Ensure determinism + auditability | L — deps: planner + API + UI | DX |
| E9 | Audit trail (actor + correlation/causation) | Who/what triggered; correlation across events | Partial | Implemented | `crates/arco-flow/src/events.rs` — `EventEnvelope` (`correlation_id`, `causation_id`); `docs/plans/2025-01-12-arco-orchestration-design.md` — “Event Envelope” | Actor identity fields are not present in flow event envelope; no user-facing audit views | - Extend event envelopes with actor fields<br>- Add audit query endpoints | M — deps: auth system + event schema evolution | governance |
| E10 | Projections + rebuild tooling | Rebuild read models from event log safely | Partial | Implemented | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Projection Rebuild Strategy”; `crates/arco-catalog/src/compactor.rs` — `Compactor::compact_domain` | Flow projections/rebuild tooling not implemented | - Implement flow compactor + rebuild CLI<br>- Add “shadow rebuild” rails | L — deps: flow schema + compactor + ops tooling | operability |

### F) Data quality + governance integration

| ID | Dagster capability | What “good” looks like in Dagster (short) | Servo/Arco equivalent? | Current status | Evidence | Gap description (if Partial/No) | Recommended approach (1–3 bullets) | Complexity + key dependencies | Risk if missing |
|---|---|---|---|---|---|---|---|---|---|
| F1 | Asset checks definition model | Checks defined alongside assets with severity/phase metadata | Yes | Implemented | `python/arco/src/servo/types/check.py` — `Check`, `CheckSeverity`; `python/arco/src/servo/asset.py` — `asset` (`checks=`) | — | - Ensure check schema is carried through manifest/plans | M — deps: planner + worker | correctness |
| F2 | Check execution + results persistence | Checks execute and results are queryable and durable | No | Designed | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Check Execution” (references `python/.../worker/checks.py` which is not in repo) | Designed but not implemented; doc↔code drift | - Implement check runner in worker<br>- Persist results as Tier-2 events + projections | L — deps: worker runtime + catalog/executions schema | correctness |
| F3 | Gating/quarantine + promotion workflow | Blocking checks prevent promotion; quarantine + manual review | No | Designed | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Gating Semantics”; `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Quarantine Flow”; `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Output Commit Protocol” | No implementation of gating/quarantine/promotion; high correctness risk | - Implement commit protocol + promotion gate<br>- Add quarantine approve/reject tooling | L — deps: storage model + worker + API | correctness |
| F4 | Freshness/SLAs/health signals | First-class freshness policies; health rollups for assets | Partial | Designed | `docs/plans/2025-01-12-arco-unified-platform-design.md` — “Health Model”; `docs/plans/ARCO_TECHNICAL_VISION.md` — “Quality Integration” | No implemented freshness evaluation or health computation in code | - Define freshness signals in catalog state<br>- Add evaluators + UI surfacing | L — deps: catalog state + event ingestion + UI | DX |
| F5 | Lineage-by-execution + metadata capture | Execution-derived lineage + materialization metadata captured | Partial | Designed | `docs/plans/ARCO_TECHNICAL_VISION.md` — “Lineage-by-Execution”; `docs/plans/2025-01-12-arco-unified-platform-design.md` — “Flow 4: Catalog Update”; `crates/arco-catalog/src/lib.rs` (claims execution-based lineage) | Integration path flow→catalog not found implemented; executions domain currently compacts only `MaterializationRecord` | - Define flow outbox event types for lineage/quality/executions<br>- Implement ingest + compaction + reconciliation tests | L — deps: event schema + ingest + compactor | governance/DX |

### G) Multi-tenancy, isolation, and ops hardening

| ID | Dagster capability | What “good” looks like in Dagster (short) | Servo/Arco equivalent? | Current status | Evidence | Gap description (if Partial/No) | Recommended approach (1–3 bullets) | Complexity + key dependencies | Risk if missing |
|---|---|---|---|---|---|---|---|---|---|
| G1 | Multi-tenant isolation boundaries | Strong tenant/workspace isolation for storage + APIs | Yes | Implemented | `crates/arco-core/src/scoped_storage.rs` — `ScopedStorage`; `docs/plans/2025-01-12-arco-unified-platform-design.md` — “Multi-Tenancy, Security, and Governance” | — | - Extend isolation to orchestration APIs when built | M — deps: auth + API | correctness/security |
| G2 | Per-tenant quotas + fairness | Noisy-neighbor protection via quotas/fair scheduling | Partial | Implemented | `crates/arco-flow/src/quota/mod.rs` — `QuotaManager`; `crates/arco-flow/src/quota/drr.rs` — `DrrScheduler`; `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Fairness & Quota Enforcement” | Global enforcement across runs/tenants not implemented | - Implement multi-run scheduler loop + quota enforcement points<br>- Add per-tenant dashboards | L — deps: store + scheduler service | operability/cost |
| G3 | Rate limiting + backpressure | Protect APIs and control-plane under load | Partial | Implemented | `crates/arco-api/src/rate_limit.rs` — `RateLimitState`; `docs/plans/2025-12-18-gate5-hardening.md` — “Task 7: Backpressure API” | Rate limiting exists for catalog API; orchestration backpressure not implemented | - Extend rate limiting to orchestration APIs<br>- Implement backpressure signals from watermarks/queue depth | M — deps: orchestration API + metrics | operability/cost |
| G4 | Outbox + dedupe + reconciliation | At-least-once delivery with durable dedupe + reconciliation jobs | Partial | Implemented | `crates/arco-flow/src/outbox.rs` — `LedgerWriter::append`; `crates/arco-catalog/src/event_writer.rs` — `EventWriter::append`; `crates/arco-catalog/src/reconciler.rs` — `Reconciler` | Flow→catalog reconciliation and end-to-end delivery guarantees not implemented | - Standardize outbox event schemas + idempotency keys<br>- Implement reconciliation jobs + tests | L — deps: event schemas + ingest + reconciler | correctness/ops |
| G5 | Exactly-once effects for materializations | Output commit protocol + promotion fencing prevents duplicates/torn writes | Partial | Designed | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Output Commit Protocol”; `crates/arco-core/src/publish.rs` — `PublishPermit` | Protocol exists in docs; not implemented in worker/control plane | - Implement worker staging/commit markers<br>- Enforce promotion via fenced manifest publish | L — deps: worker + storage + publish fencing | correctness |
| G6 | HA scheduler + leader election | Single active scheduler; safe failover | Partial | Planned | `crates/arco-flow/src/leader/mod.rs` — `LeaderElector`; `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — “ADR-014: Leader Election Strategy” | No production leader election/store implementation | - Implement Postgres (or equivalent) leader elector + store<br>- Wire into scheduler service | M — deps: Postgres store + deployment | correctness/ops |
| G7 | Upgrade/migration story (schema evolution) | Safe event/proto evolution with migrations + compatibility rules | Partial | Designed | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “API Versioning & Evolution”; `crates/arco-flow/src/events.rs` — `EventEnvelope.schema_version`; `crates/arco-catalog/src/manifest.rs` — `RootManifest.version` | Versioning rules exist but drift is present (IDs/state machine) and enforcement tooling is missing | - Resolve ADRs for IDs + state machine<br>- Add compatibility tests/gates in CI | M — deps: ADRs + contract tests | correctness |
| G8 | Disaster recovery + projection rebuild | Rebuild derived views; recover from corruption | Partial | Implemented | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Projection Rebuild Strategy”; `crates/arco-catalog/src/compactor.rs` — `Compactor::compact_domain`; `docs/plans/2025-12-19-gate5-hardening-plan-patch.md` — “Patch Delta Checklist” | Flow projections/rebuild not implemented; catalog rebuild story in progress | - Implement flow rebuild tooling<br>- Add DR runbooks + verification tests | L — deps: flow compactor + ops docs | operability |

### H) Developer experience (DX)

| ID | Dagster capability | What “good” looks like in Dagster (short) | Servo/Arco equivalent? | Current status | Evidence | Gap description (if Partial/No) | Recommended approach (1–3 bullets) | Complexity + key dependencies | Risk if missing |
|---|---|---|---|---|---|---|---|---|---|
| H1 | SDK ergonomics (assets/checks/partitions) | Ergonomic authoring, good errors, strong typing | Partial | Implemented | `python/arco/src/servo/asset.py` — `asset`; `python/arco/tests/unit/test_introspection.py` — `TestExtractDependencies.test_extracts_asset_in_hints`; `python/arco/tests/unit/test_check.py` — `TestCheckTypes.test_row_count_check` | SDK is strong for definitions; runtime/deploy/run surfaces missing; partition canonical drift exists | - Align partition/ID canonicalization across languages<br>- Add schedule/sensor/backfill authoring surfaces | M — deps: ADRs + SDK + planner | DX/correctness |
| H2 | CLI workflows (deploy/run/status/logs/backfill) | One CLI to deploy, run, monitor, debug, backfill | Partial | Implemented | `python/arco/src/servo/cli/main.py` — `deploy`, `run`, `status`; `python/arco/src/servo/cli/commands/run.py` — `run_asset` (TODO); `python/arco/src/servo/cli/commands/status.py` — `show_status` (TODO) | CLI exists but is manifest-only; run/status/logs/backfill not implemented | - Implement API client + auth wiring<br>- Add commands: logs/backfill/dev | L — deps: control plane API + worker + projections | DX |
| H3 | Validation tooling | Validate assets/manifest/config before deploy | Partial | Implemented | `python/arco/src/servo/cli/commands/validate.py` — `run_validate`; `python/arco/src/servo/manifest/serialization.py` — `serialize_to_manifest_json` | Validation exists but only for local definitions; no server-side contract validation | - Add server-side manifest validation endpoint<br>- Add contract tests across Rust/Python/proto | M — deps: API + contract tests | correctness/DX |
| H4 | Local dev server experience | Local UI/API loop for rapid iteration | Partial | Designed | `docs/plans/2025-01-12-arco-orchestration-design.md` — “Local Development”; `python/arco/src/servo/cli/main.py` — `deploy`, `run`, `status`, `validate`, `init` | Not implemented | - Build local runtime + dev server<br>- Provide local event log + views | L — deps: local runtime + UI | DX |
| H5 | Testing story (unit/integration harness) | First-class testing utilities and integration tests | Partial | Implemented | `docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Testing Strategy”; `python/arco/tests/unit/test_partition.py` — `TestPartitionKey.test_canonical_order`; `python/arco/tests/unit/test_introspection.py` — `TestComputeTransformFingerprint.test_deterministic`; `crates/arco-flow/src/task.rs` — `tests::task_state_full_lifecycle`; `docs/plans/2025-12-16-part4-integration-testing.md` — “D.6 Acceptance Test Coverage” | Tests exist, but no end-to-end orchestration E2E tests (deploy→run→observe) | - Add orchestration integration tests once API exists<br>- Add golden cross-language tests | M — deps: API + worker + test harness | correctness/ops |

---

## 4) Gap Inventory (prioritized; grouped A–H)

> This inventory groups the highest-impact parity gaps by categories **A–H**, with each row referencing the Parity Matrix IDs it blocks.

### A) Core orchestration model + semantics

| Matrix IDs | Problem statement | Why it matters | Proposed solution direction | Alternatives considered | Dependencies / sequencing notes | Priority |
|---|---|---|---|---|---|---|
| A2–A5 | No end-to-end planner from user definitions → `Plan` (DAG construction, stable `TaskKey`, deterministic ordering, dependency errors) | Without a real planner, Servo is “definitions-only” and cannot provide Dagster-grade correctness/UX | Implement planner pipeline: manifest + catalog snapshot → task graph → `Plan` → persisted run record | “Plan in Python only”, “hand-author plans”, “accept non-deterministic planning”, **do nothing** | Requires canonical ID/partition formats (A5/C3) + snapshot binding (A4) + API surfaces (E) | P0 |
| A5–A7, G7 | Cross-language determinism drift (ID wire formats + state machine enums differ across Rust/Python/proto) | Breaks reproducibility, signatures, and interoperability; creates upgrade hazards | Resolve ADRs (IDs/state enums), align proto ↔ Rust engine, and add cross-language golden fixtures + compatibility gates | “Allow multiple formats forever”, “translate at boundaries only”, **do nothing** | Sequence before shipping control plane APIs (E) and before multi-instance/HA (G6) | P0 |
| A4, D10 | Plan reproducibility anchors missing (code version + catalog snapshot binding attached to runs/plans) | Fingerprints are less useful without stable “what inputs produced this” anchors | Persist `code_version` + `catalog_snapshot_id` on plan/run; include in fingerprints and audit views | “Best-effort hashing only”, “store unversioned blobs”, **do nothing** | Depends on deploy artifact/version model (D10/H2) + catalog snapshot semantics | P1 |

### B) Scheduling, sensors, triggers

| Matrix IDs | Problem statement | Why it matters | Proposed solution direction | Alternatives considered | Dependencies / sequencing notes | Priority |
|---|---|---|---|---|---|---|
| B1–B2, B7 | Schedules/sensors runtime missing (definitions, tick/cursor store, evaluator, idempotent triggers) | Core automation and event-driven orchestration are missing; forces manual runs | Implement schedule + sensor definition surfaces, tick/cursor persistence, evaluation loop, and run-keyed idempotency | “Rely on external cron only”, “users build custom daemons”, **do nothing** | Needs TriggerRun API (E) + durable store + leader election (G6) | P0 |
| B4–B6, G2–G3 | Run dedupe + global queue + concurrency gates are not enforced across runs/tenants | Duplicate work, runaway costs, and noisy-neighbor behavior | Add run-level idempotency keys, global run queue semantics, and per-scope concurrency limits integrated with quotas/rate limits | “Per-run only limits”, “manual operator throttling”, **do nothing** | Depends on durable store (G6) + fairness/quota integration (G2) + metrics (E4) | P1 |

### C) Partitions + backfills

| Matrix IDs | Problem statement | Why it matters | Proposed solution direction | Alternatives considered | Dependencies / sequencing notes | Priority |
|---|---|---|---|---|---|---|
| C1–C2, C4 | Partition-aware planning and dependency mapping semantics not implemented (types exist, runtime doesn’t expand/resolve) | Prevents partitioned assets from running correctly; blocks incremental/backfill workflows | Implement partition expansion + mapping evaluation in the planner; persist partition sets in catalog | “Single partition dimension only”, “no mapping semantics”, **do nothing** | Must align with canonical partition identity (C3) and backfill controller (C5–C10) | P0 |
| C3 | PartitionKey canonical mismatch between Rust and Python | Breaks cross-language determinism and makes partition addressing unreliable | Choose one canonical encoding (ADR), enforce in proto/Python/Rust, and ship shared golden fixtures | “Support both encodings indefinitely”, “translate on ingest only”, **do nothing** | Should be resolved alongside ID wire formats (A5/G7) | P0 |
| C5–C10 | Partition status + backfill controller/UX missing (range backfills, chunking, pause/resume/cancel, failed-only, subset) | Backfills are a daily operator workflow; without them partitioned orchestration is not usable | Introduce Backfill entity + controller (range→chunks→runs), durable progress accounting, and CLI/UI surfaces | “One-run-per-partition”, “manual scripts”, **do nothing** | Requires planner (A) + durable store (G6) + run/task views (E2) | P0 |

### D) Compute/execution + resource model

| Matrix IDs | Problem statement | Why it matters | Proposed solution direction | Alternatives considered | Dependencies / sequencing notes | Priority |
|---|---|---|---|---|---|---|
| D1, D9 | Worker runtime contract is not implemented (no executor + callbacks/attempt IDs/auth) | Without a worker, execution semantics (timeouts, cancellation, IO, logs) can’t be validated or delivered | Implement Python worker executor + callback/reporting API; include attempt IDs + idempotency keys | “Only local execution”, “run everything in control plane”, **do nothing** | Depends on control plane API (E) + auth/IAM model (G) + IO/output protocol (D7/G5) | P0 |
| D3–D5 | End-to-end heartbeats/timeouts/cancellation not wired through a real distributed scheduler loop | Causes zombie work, stuck runs, and unreliable cancellation (major operability risk) | Define worker heartbeat + cancel protocols; enforce in scheduler loop with durable store + events | “Best-effort timeouts”, “kill workers only”, **do nothing** | Requires worker runtime (D1) + durable store (G6) + event projections (E) | P0 |
| D7, G5 | IO abstraction + output commit protocol not implemented end-to-end | Prevents reliable materializations and exactly-once effects; blocks promotion workflows | Define IO manager interface; implement staged writes + commit markers + promotion fencing | “Assume idempotent transforms”, “overwrite in place”, **do nothing** | Depends on storage model + publish fencing (G5) + check gating (F3) | P1 |
| D6, D8 | Resource enforcement + secrets/env separation not implemented | Cost blowups and security leaks are likely in multi-tenant execution | Enforce resource hints at runtime; implement secret provider abstraction + workspace scoping rules | “Hardcode env vars”, “manual secret injection”, **do nothing** | Depends on worker runtime + IAM/RBAC + deploy config surfaces | P1 |

### E) Observability + debugging + reexecution

| Matrix IDs | Problem statement | Why it matters | Proposed solution direction | Alternatives considered | Dependencies / sequencing notes | Priority |
|---|---|---|---|---|---|---|
| E1–E2, E9–E10 | No flow read model/projections + query API for run/task/event views (incl. audit actor) | Without run/task views, users cannot debug, trust, or operate orchestration | Build flow projections/compactor + query APIs (Get/List Run/Task/Event) and minimal UI surfaces | “Raw event dumps only”, “use catalog UI only”, **do nothing** | Depends on durable store/event persistence + schema alignment (A/G7) + CLI integration (H2) | P0 |
| E3–E5 | Logs/metrics/tracing not end-to-end (storage, retrieval, exporters, context propagation) | Slows incident response; blocks SLO-driven ops; degrades DX | Decide log storage model (events vs objects); add log retrieval APIs; wire metrics exporters + OTel propagation | “Console logs only”, “no tracing”, **do nothing** | Needs worker runtime (D1) + runtime services/infra + stable correlation IDs | P1 |
| E6–E8 | Reexecution workflows missing (rerun from failure, subset selection) | One of Dagster’s highest-productivity debugging workflows is absent | Implement reexecution planning mode(s), preserve lineage links, and surface attempt history in views | “Just retry whole run”, “manual selection scripts”, **do nothing** | Requires planner enhancements (A) + run/task views (E2) + durable store | P1 |

### F) Data quality + governance integration

| Matrix IDs | Problem statement | Why it matters | Proposed solution direction | Alternatives considered | Dependencies / sequencing notes | Priority |
|---|---|---|---|---|---|---|
| F2 | Check execution + results persistence not implemented | Checks exist as definitions but don’t protect users from bad data | Implement check runner in worker and persist results as Tier-2 events + projections | “Only static checks”, “log-only checks”, **do nothing** | Depends on worker runtime (D1) + events/projections (E) | P1 |
| F3, G5 | Gating/quarantine/promotion workflow not implemented | Without gating, “quality” is advisory and failures propagate downstream | Implement gating semantics integrated with output commit/promotion fencing; add quarantine approve/reject tooling | “Always promote”, “manual review outside platform”, **do nothing** | Depends on commit protocol (D7/G5) + auth/RBAC + UI | P1 |
| F4–F5 | Freshness/health + lineage-by-execution capture and ingestion not implemented | Governance parity requires execution-derived lineage and health signals | Define execution→catalog event types; implement ingest + compaction + health evaluators | “Lineage-by-definition only”, “external governance tools”, **do nothing** | Depends on outbox/reconciliation (G4) + projections/UI | P2 |

### G) Multi-tenancy, isolation, and ops hardening

| Matrix IDs | Problem statement | Why it matters | Proposed solution direction | Alternatives considered | Dependencies / sequencing notes | Priority |
|---|---|---|---|---|---|---|
| G6 | Durable store + leader election for HA scheduler not implemented | Multi-instance correctness and safe failover are blocked | Implement Postgres store + leader elector; wire scheduler service around it | “Single instance forever”, “best-effort HA”, **do nothing** | Foundational for schedules/sensors (B) and backfills (C); blocks production readiness | P0 |
| G4–G5 | End-to-end delivery guarantees missing (outbox/dedupe/reconciliation + exactly-once effects) | Causes duplicates, missing events, and torn writes across Flow↔Catalog | Standardize outbox schemas + idempotency keys; implement reconciliation jobs + tests; complete commit/promotion protocol | “At-least-once with no dedupe”, “manual repair”, **do nothing** | Requires event schema decisions + durable stores + compaction/projections | P0 |
| G2–G3, B5–B6 | Multi-tenant fairness/backpressure not enforced across runs | Noisy-neighbor and cost incidents will occur under load | Integrate quotas (DRR) with global run/task scheduling; add rate limiting/backpressure signals + dashboards | “Static per-tenant limits”, “operator throttling”, **do nothing** | Depends on HA scheduler (G6) + metrics/exporters (E4) | P1 |
| G7–G8 | Compatibility gates + DR runbooks for events/protos/projections are incomplete | Drift (IDs/states) will recur; recovery will be manual and risky | Add schema evolution rules + CI compatibility tests; build flow projection rebuild tooling + runbooks | “Breaking changes tolerated”, “rebuild by hand”, **do nothing** | Depends on resolving current drift (A/C) and having projection tooling (E10) | P1 |

### H) Developer experience (DX)

| Matrix IDs | Problem statement | Why it matters | Proposed solution direction | Alternatives considered | Dependencies / sequencing notes | Priority |
|---|---|---|---|---|---|---|
| H2, D10 | CLI is not end-to-end (deploy is manifest-only; run/status/logs/backfill incomplete) | Users can’t adopt Servo without a working deploy→run→observe loop | Implement API client + auth; make `servo deploy/run/status/logs/backfill` real workflows | “Docs-only flows”, “manual API calls”, **do nothing** | Depends on control plane APIs + projections (E) and deploy/version model (D10) | P0 |
| D2, H4 | Local dev server/runtime (`servo dev`) is missing | Slows iteration and makes parity claims hard to validate locally | Build local executor (in-memory store/queue) + local API/UI stubs; add `servo dev` | “Use cloud for all dev”, “unit tests only”, **do nothing** | Depends on defining worker contract and minimal API surfaces | P1 |
| H3, H5 | Contract validation + integration test harness not end-to-end | Drift keeps reappearing without “uncheatable” cross-language acceptance tests | Add server-side manifest validation + golden fixtures; build deploy→run→observe integration suite | “Rely on unit tests”, “manual QA”, **do nothing** | Depends on API availability and stable wire formats (A/C/G7) | P1

---

## 5) Exception Planning Inputs (exception cards)

> Exception cards are for features we recommend Servo/Arco **does not match** (now or ever), with rationale and mitigations.

### Exception Card 1
- Feature/capability: Dagster “op/job” model as a first-class alternative to assets (**added by architect**)
- Decision: **Won’t match**
- Rationale (business + technical): Servo is intentionally asset-first; supporting two parallel authoring models increases cognitive surface and fragments UX. This aligns with `docs/plans/2025-01-12-arco-orchestration-design.md` — “Developer Experience (SDK & CLI)” (asset decorator is primary).
- User impact: Users migrating “job-first” code will need an asset wrapper/mapping strategy.
- Mitigation / workaround: Provide a lightweight “job wrapper” that materializes a synthetic asset representing the job output; document migration patterns.
- Trigger to revisit: If enterprise customers require op-level orchestration for non-asset workflows.
- Risk acceptance statement: Accept reduced flexibility in exchange for a tighter asset-centric product.
- Estimated effort if we later choose to match: **L** (requires new DSL, UI semantics, scheduler semantics).

### Exception Card 2
- Feature/capability: Dagster-style always-on daemon architecture for sensors/schedules (vs serverless event-driven) (**added by architect**)
- Decision: **Won’t match** (architecture), **will match** (user-facing semantics)
- Rationale (business + technical): Platform constraint is serverless-first; we can implement schedule/sensor semantics with Cloud Scheduler + Pub/Sub + durable cursors without long-lived daemons (`docs/plans/2025-01-12-arco-orchestration-design-part2.md` — “Scheduler Deployment Model” discusses always-on as MVP but allows future models).
- User impact: Semantics should be equivalent; operational footprint differs.
- Mitigation / workaround: Provide deterministic tick storage + cursoring + retries; document latency expectations.
- Trigger to revisit: If event-driven model cannot meet latency/SLO requirements.
- Risk acceptance statement: Accept different runtime architecture to preserve cost profile.
- Estimated effort if we later choose to match: **M** (run a daemon fleet + HA + ops burden).

---

## 6) Unknowns list (explicit)

| UNKNOWN | What’s missing (doc/code) | Why it blocks parity assessment | What artifact resolves it |
|---|---|---|---|
| Orchestration API implementation | No implemented `TriggerRun/GetRun/StreamRunEvents` service found | Without it, can’t judge real UX parity | API crate + routes/gRPC server + client SDK |
| Worker runtime contract | No implemented Python worker/executor module found under `python/arco/src/servo/` (docs reference it) | Execution semantics (timeouts, cancellation, IO, logs) can’t be validated | Worker contract spec + implementation + integration tests |
| Flow projections/read model | No flow compactor/projection code found for run/task views | Observability/reexecution parity depends on it | Flow state schema + compactor + query API |
| Lineage-by-execution ingestion | Flow→Catalog sync path not found in code | Governance parity depends on execution lineage capture | Outbox event types + catalog ingest handler + tests |

---

## 7) Recommended next steps (30/60/90)

### 0–30 days (P0: make orchestration usable)
- Build minimal control plane API for `TriggerRun/GetRun/ListRuns/ListTasks/Cancel/Retry` aligned with `proto/arco/v1/orchestration.proto` and `crates/arco-flow` state machine.
- Implement durable run/task storage + leader election (Postgres) per `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — “Gate C: Distributed Correctness”.
- Make `servo deploy` actually deploy (manifest upload + register), and implement `servo run`/`servo status` against the API (`python/arco/src/servo/cli/main.py` — `deploy`, `run`, `status`).

### 31–60 days (P1: automation + partitions)
- Implement schedules + sensors with tick/cursor storage + run_key dedupe.
- Implement partition expansion in planner + basic backfill controller (chunking + progress).
- Close determinism drift: align ID formats + PartitionKey encoding across Rust/Python/proto; add golden cross-language fixtures.

### 61–90 days (P1/P2: Dagster-grade debugging + quality)
- Implement reexecution workflows (rerun from failure, subset selection).
- Implement quality checks execution + gating + quarantine + promotion protocol.
- Add operational dashboards/alerts + deterministic simulation harness hooks (Gate 5 / Part 2 observability standards).

**Suggested owners/teams (placeholder):**
- Flow control plane (Rust): Platform/Orchestration
- Python SDK/CLI + worker runtime: Data Platform DX
- Catalog/Lineage integration: Catalog team
- Security/IAM hardening: Platform Security

**Architectural decisions to lock early:**
- Canonical ID wire formats across Rust/Python/proto (see `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — "ADR-013")
- PartitionKey canonical encoding (see `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — "ADR-011" and `crates/arco-core/src/partition.rs` — `PartitionKey`)
- Scheduler HA mechanism (leader election choice) (see `docs/plans/2025-12-17-arco-flow-production-grade-remediation.md` — "ADR-014")

---

## 8) Acceptance Criteria (Testable Parity Gates)

> Each acceptance criterion is a **testable condition** that can be verified through code review, integration test, or production observation. Format: `[WHEN condition] [THEN expected behavior] [VERIFIED BY method]`
>
> **Notes:**
> - **Milestones:** M1 (0–30d), M2 (31–60d), M3 (61–90d) — aligned with `docs/audits/servo-dagster-exception-planning.md`.
> - **TBD artifacts:** If a `VERIFIED BY` test/API/file does not exist yet, it is explicitly marked **(TBD)** (to avoid “phantom citations”).

### A) Core Orchestration Model

| ID | Milestone | Acceptance Criterion | Verification Method |
|---|---|---|---|
| A1-AC | M1 | WHEN a Python `@asset` decorator is applied to a function THEN manifest export includes the asset with dependencies extracted VERIFIED BY `python/arco/tests/integration/test_e2e.py` — `TestDiscoveryE2E.test_dependency_extraction`, `TestManifestE2E.test_generates_valid_manifest` | Integration test |
| A2-AC | M1 | WHEN a manifest with cyclic dependencies is submitted THEN planner rejects with error including cycle path VERIFIED BY `python/arco/tests/integration/test_planner_cycle_path.py` (TBD) | Integration test |
| A3-AC | M1 | WHEN the same manifest + catalog snapshot is planned twice THEN both plans have identical `fingerprint` values VERIFIED BY `crates/arco-flow/src/plan.rs` — `tests::plan_fingerprint_is_deterministic` | Unit test |
| A4-AC | M1 | WHEN a plan is created THEN it includes `code_version_id` and `catalog_snapshot_id` fields VERIFIED BY API rejects missing fields (contract test; TBD) | Contract test |
| A5-AC | M1 | WHEN `TaskKey` is serialized in Rust and Python THEN byte-identical canonical JSON output VERIFIED BY shared golden fixtures consumed by both language test suites (TBD) | Contract test |
| A6-AC | M1 | WHEN run transitions through all states THEN proto `RunState` enum matches engine `RunState` 1:1 VERIFIED BY enum exhaustiveness contract test (TBD) | Contract test |
| A7-AC | M1 | WHEN task transitions through all states THEN proto `TaskState` enum matches engine `TaskState` 1:1 VERIFIED BY enum exhaustiveness contract test (TBD) | Contract test |
| A8-AC | M1 | WHEN upstream task fails AND `continue_on_failure=false` THEN downstream tasks transition to SKIPPED with an upstream-failed reason VERIFIED BY scheduler integration test (TBD) | Integration test |
| A9-AC | M1 | WHEN task fails with retryable error THEN scheduler applies `RetryPolicy` backoff before requeue VERIFIED BY scheduler integration test (TBD) | Integration test |
| A10-AC | M1 | WHEN duplicate terminal result arrives THEN no state change AND idempotency is recorded VERIFIED BY late-result/idempotency integration test (TBD) | Integration test |

### B) Scheduling, Sensors, Triggers

| ID | Milestone | Acceptance Criterion | Verification Method |
|---|---|---|---|
| B1-AC | M2 | WHEN schedule tick fires THEN run is created with `TriggerType::Scheduled` AND tick history persisted VERIFIED BY `GetScheduleHistory` API (TBD) | Integration test |
| B2-AC | M2 | WHEN sensor evaluates THEN cursor is persisted AND next evaluation starts from cursor VERIFIED BY cursor value in store (TBD) | Integration test |
| B3-AC | M2 | WHEN sensor condition returns `SkipReason` THEN no run created AND skip logged VERIFIED BY audit log (TBD) | Integration test |
| B4-AC | M2 | WHEN duplicate `run_key` is submitted THEN no new run created AND existing run ID returned VERIFIED BY `TriggerRun` response (TBD) | Integration test |
| B5-AC | M3 | WHEN run queue depth > threshold THEN backpressure signal emitted AND new runs queued (not dispatched) VERIFIED BY queue depth metrics + run state (TBD) | Load test |
| B6-AC | M2 | WHEN concurrency limit reached THEN additional ready tasks remain QUEUED/READY (not dispatched) VERIFIED BY task state inspection (TBD) | Integration test |
| B7-AC | M2 | WHEN schedule/sensor fails THEN tick error is queryable via API VERIFIED BY `ListScheduleTicks` response (TBD) | Integration test |

### C) Partitions + Backfills

| ID | Milestone | Acceptance Criterion | Verification Method |
|---|---|---|---|
| C1-AC | M2 | WHEN partitioned asset is planned THEN plan contains one task per partition in range VERIFIED BY `len(plan.tasks) == len(partitions)` (TBD) | Integration test |
| C2-AC | M1 | WHEN multi-dimensional partition key is serialized THEN all dimensions are present in the canonical encoding VERIFIED BY `crates/arco-core/tests/fixtures/partition_key_cases.json` | Contract test |
| C3-AC | M1 | WHEN a `PartitionKey` is serialized in Rust and Python THEN outputs match the ADR-011 canonical encoding for the shared fixture suite VERIFIED BY `crates/arco-core/tests/fixtures/partition_key_cases.json` (Rust: `crates/arco-core/tests/cross_language/partition_key_test.rs`; Python: fixture-driven contract test TBD) | Contract test |
| C4-AC | M2 | WHEN upstream has daily partitions AND downstream has monthly THEN planner resolves mapping correctly VERIFIED BY task `upstream_task_ids` (TBD) | Integration test |
| C5-AC | M2 | WHEN partition completes THEN status queryable via `GetPartitionStatus` API VERIFIED BY API response (TBD) | Integration test |
| C6-AC | M2 | WHEN backfill created over range [2024-01-01, 2024-01-31] THEN 31 tasks created (daily partitions) VERIFIED BY plan task count (TBD) | Integration test |
| C7-AC | M3 | WHEN backfill `chunk_size=10` THEN max 10 tasks run concurrently per backfill VERIFIED BY peak concurrent task count (TBD) | Load test |
| C8-AC | M2 | WHEN backfill paused THEN no new tasks dispatched AND resume continues from last progress VERIFIED BY task states + backfill state (TBD) | Integration test |
| C9-AC | M2 | WHEN "retry failed" invoked on backfill THEN only FAILED partition tasks rerun VERIFIED BY run task list (TBD) | Integration test |
| C10-AC | M2 | WHEN backfill subset = [asset_a, asset_b] THEN only those assets materialize VERIFIED BY plan asset IDs (TBD) | Integration test |

### D) Compute/Execution + Resource Model

| ID | Milestone | Acceptance Criterion | Verification Method |
|---|---|---|---|
| D1-AC | M1 | WHEN a `TaskEnvelope` is created for dispatch THEN it includes task identity + routing fields and a deterministic idempotency key VERIFIED BY `crates/arco-flow/src/dispatch/mod.rs` — `TaskEnvelope`, `TaskEnvelope::idempotency_key` | Contract test |
| D2-AC | M2 | WHEN `servo dev` runs locally THEN same asset code executes as cloud with identical outputs VERIFIED BY output comparison (TBD) | E2E test |
| D3-AC | M1 | WHEN worker heartbeat missing > `heartbeat_timeout_ms` THEN task marked ZOMBIE VERIFIED BY `crates/arco-flow/src/task.rs` — `TaskExecution::is_zombie` + integration test (TBD) | Integration test |
| D4-AC | M1 | WHEN task exceeds `execution_timeout_ms` THEN task transitioned to FAILED with reason TIMEOUT VERIFIED BY timeout integration test (TBD) | Integration test |
| D5-AC | M1 | WHEN cancel requested AND worker running THEN graceful cancel signal sent AND task CANCELLED within grace period VERIFIED BY cancellation integration test (TBD) | Integration test |
| D6-AC | M2 | WHEN task `ResourceRequirements.memory_bytes` set THEN Cloud Run task configured with that limit VERIFIED BY deployment spec inspection (TBD) | Deployment validation |
| D7-AC | M2 | WHEN worker writes output via `AssetContext.output()` THEN staged write created AND commit marker written on success VERIFIED BY storage inspection test (TBD) | Integration test |
| D8-AC | M2 | WHEN asset requires secret `DB_PASSWORD` THEN worker receives resolved value without exposing in logs VERIFIED BY log redaction test (TBD) | Security test |
| D9-AC | M1 | WHEN a late result arrives for a different attempt THEN it is discarded and does not change terminal state VERIFIED BY idempotency/attempt guard test (TBD) | Integration test |
| D10-AC | M1 | WHEN `servo deploy` completes THEN manifest registered with `code_version` AND subsequent runs reference it VERIFIED BY E2E metadata assertion (TBD) | E2E test |

### E) Observability + Debugging + Reexecution

| ID | Milestone | Acceptance Criterion | Verification Method |
|---|---|---|---|
| E1-AC | M1 | WHEN run completes THEN all events queryable via `ListRunEvents` in causal order VERIFIED BY event sequence (TBD) | Integration test |
| E2-AC | M1 | WHEN `GetRun` called THEN response includes run state, task states, timing, trigger info VERIFIED BY response schema (TBD) | API test |
| E3-AC | M1 | WHEN `servo logs <run_id>` executed THEN stdout/stderr from worker execution returned VERIFIED BY log content (TBD) | CLI test |
| E4-AC | M2 | WHEN scheduler running THEN Prometheus metrics for run_count, task_duration_ms, queue_depth exported VERIFIED BY `/metrics` endpoint (TBD) | Observability test |
| E5-AC | M3 | WHEN task dispatched THEN trace_id propagated to worker AND spans linked VERIFIED BY trace backend query (TBD) | Tracing test |
| E6-AC | M2 | WHEN task retried 3 times THEN all 3 attempts visible in task timeline VERIFIED BY `GetTaskAttempts` response (TBD) | API test |
| E7-AC | M3 | WHEN "rerun from failure" invoked THEN new run executes only FAILED tasks + their downstreams VERIFIED BY new run task list (TBD) | Integration test |
| E8-AC | M3 | WHEN subset reexecution with selection=[asset_a] THEN only asset_a tasks execute VERIFIED BY run task list (TBD) | Integration test |
| E9-AC | M2 | WHEN run triggered by a user THEN audit event includes actor identity VERIFIED BY event payload (TBD) | Audit test |
| E10-AC | M2 | WHEN projection rebuild invoked THEN read model matches event log state VERIFIED BY reconciliation test (TBD) | DR test |

### F) Data Quality + Governance

| ID | Milestone | Acceptance Criterion | Verification Method |
|---|---|---|---|
| F1-AC | M1 | WHEN `@asset(checks=[row_count_check])` defined THEN check appears in manifest VERIFIED BY manifest JSON (TBD) | Schema test |
| F2-AC | M3 | WHEN check executes AND fails THEN result persisted with severity + message VERIFIED BY `GetCheckResults` API (TBD) | Integration test |
| F3-AC | M3 | WHEN BLOCKING check fails THEN downstream assets not promoted VERIFIED BY materialization state (TBD) | Integration test |
| F4-AC | M3 | WHEN asset freshness SLA breached THEN health signal transitions to STALE VERIFIED BY `GetAssetHealth` API (TBD) | Integration test |
| F5-AC | M3 | WHEN materialization completes THEN lineage event emitted to catalog with upstream refs VERIFIED BY catalog event ingestion test (TBD) | Integration test |

### G) Multi-tenancy + Ops Hardening

| ID | Milestone | Acceptance Criterion | Verification Method |
|---|---|---|---|
| G1-AC | M1 | WHEN tenant_a writes to storage THEN path contains `/tenants/tenant_a/` AND tenant_b cannot read VERIFIED BY IAM test (TBD) | Security test |
| G2-AC | M3 | WHEN tenant exceeds quota THEN additional runs queued (not rejected) AND fairness maintained VERIFIED BY DRR scheduling test (TBD) | Load test |
| G3-AC | M2 | WHEN API rate limit exceeded THEN 429 returned with Retry-After header VERIFIED BY rate limit test (TBD) | API test |
| G4-AC | M2 | WHEN event written to outbox THEN eventually delivered to subscriber with idempotency key VERIFIED BY reconciliation job (TBD) | Integration test |
| G5-AC | M3 | WHEN materialization succeeds THEN exactly one output visible (no duplicates/torn writes) VERIFIED BY storage inspection (TBD) | Correctness test |
| G6-AC | M3 | WHEN leader fails THEN new leader elected within 30s AND scheduling resumes VERIFIED BY failover test (TBD) | HA test |
| G7-AC | M2 | WHEN proto schema updated with new field THEN old clients continue to work VERIFIED BY backwards compatibility test (TBD) | Contract test |
| G8-AC | M2 | WHEN projection rebuild triggered THEN read model consistent with event log within SLA VERIFIED BY reconciliation test (TBD) | DR test |

### H) Developer Experience

| ID | Milestone | Acceptance Criterion | Verification Method |
|---|---|---|---|
| H1-AC | M1 | WHEN `@asset` decorator used incorrectly THEN clear error message with fix suggestion VERIFIED BY `python/arco/tests/unit/test_introspection.py` | DX test |
| H2-AC | M1 | WHEN `servo deploy && servo run && servo status` executed THEN full workflow completes successfully VERIFIED BY deploy→run→observe E2E test (TBD) | E2E test |
| H3-AC | M1 | WHEN invalid manifest submitted THEN validation error returned before any state change VERIFIED BY validation test (TBD) | API test |
| H4-AC | M2 | WHEN `servo dev` started THEN local UI accessible at localhost:3000 with asset graph VERIFIED BY local dev E2E test (TBD) | DX test |
| H5-AC | M2 | WHEN asset test helper used THEN asset executes in isolation with mock IO VERIFIED BY SDK test helpers (TBD) | Unit test |

---

## 9) Uncheatable Test Specifications

> These are integration tests that **cannot be faked** — they exercise the full stack and verify system behavior end-to-end. Each test has a **cheat detection mechanism** that catches common shortcuts.
>
> **Format note:** Each test spec includes a **planned location** and an expected **runner** (CI vs nightly/cloud), so we can wire them up without ambiguity.

### 9.0 Summary (13 tests)

| Test | Verifies | Planned location | Runs |
|---|---|---|---|
| cross_language_taskkey_serialization | Task identity canonicalization matches across Rust/Python | `crates/arco-flow/tests/contract/task_key_fixtures.rs` (TBD), `python/arco/tests/contract/test_task_key_fixtures.py` (TBD) | CI |
| cross_language_partition_key_encoding | PartitionKey canonical encoding matches shared fixtures | `crates/arco-core/tests/cross_language/partition_key_test.rs`, `python/arco/tests/contract/test_partition_key_fixtures.py` (TBD) | CI |
| plan_fingerprint_stability | Plan fingerprint stable across processes | `crates/arco-flow/tests/contract/plan_fingerprint_stability.rs` (TBD) | CI |
| run_state_machine_exhaustive | Run state machine exhaustive + proto alignment | `crates/arco-flow/tests/contract/run_state_machine_exhaustive.rs` (TBD) | CI |
| task_state_machine_exhaustive | Task state machine exhaustive + proto alignment | `crates/arco-flow/tests/contract/task_state_machine_exhaustive.rs` (TBD) | CI |
| deploy_run_observe_e2e | `servo deploy/run/status/logs` works end-to-end | `python/arco/tests/integration/test_orchestration_e2e.py` (TBD) | Nightly (cloud) |
| backfill_pause_resume_e2e | Backfill pause/resume correctness + no duplicates | `python/arco/tests/integration/test_backfill_e2e.py` (TBD) | Nightly (cloud) |
| upstream_failure_skips_downstream | Upstream failure produces deterministic downstream SKIPPED | `crates/arco-flow/tests/integration/upstream_failure_skip.rs` (TBD) | CI |
| zombie_detection_and_recovery | Zombie detection triggers retry/fail without double effects | `crates/arco-flow/tests/integration/zombie_detection.rs` (TBD) | CI |
| cross_tenant_isolation | Storage isolation enforced at provider level | `infra/tests/cross_tenant_isolation_e2e.md` (TBD harness) | Nightly (cloud) |
| duplicate_callback_idempotency | Duplicate callbacks do not produce double transitions/effects | `crates/arco-flow/tests/integration/duplicate_callback_idempotency.rs` (TBD) | CI |
| promotion_fencing_prevents_torn_writes | Promotion fencing prevents visibility on failed gate | `crates/arco-core/tests/integration/promotion_fencing.rs` (TBD) | Nightly (cloud) |
| flow_outbox_to_catalog_reconciliation_e2e | Flow outbox events are delivered + visible in catalog projections | `crates/arco-flow/tests/integration/outbox_to_catalog_reconciliation.rs` (TBD) | Nightly (cloud) |

### 9.1 Cross-Language Determinism Tests

```
TEST: cross_language_taskkey_serialization
LOCATION:
  - Rust: crates/arco-flow/tests/contract/task_key_fixtures.rs (TBD)
  - Python: python/arco/tests/contract/test_task_key_fixtures.py (TBD)
RUNS:
  - CI (no cloud)
SETUP:
  - Define TaskKey in Python (SDK type to be added): asset_key="ns.asset", partition_key="2024-01-01", operation=MATERIALIZE
  - Define identical TaskKey in Rust (`crates/arco-flow/src/task_key.rs` — `TaskKey`)
EXECUTE:
  - Serialize both to canonical JSON
  - Compute SHA-256 of both outputs
ASSERT:
  - Hashes are byte-identical
CHEAT DETECTION:
  - Single golden fixture file consumed by both suites (no “dual truth”)
  - Suites run independently; either side can fail CI
```

```
TEST: cross_language_partition_key_encoding
LOCATION:
  - Rust: crates/arco-core/tests/cross_language/partition_key_test.rs
  - Python: python/arco/tests/contract/test_partition_key_fixtures.py (TBD)
RUNS:
  - CI (no cloud)
SETUP:
  - Fixture-driven suite from `crates/arco-core/tests/fixtures/partition_key_cases.json`
EXECUTE:
  - Python: for each fixture case, build `PartitionKey` and compute canonical string (ADR-011 encoder; TBD)
  - Rust: for each fixture case, build `PartitionKey` via `PartitionKey::new()` + `insert()` and call `canonical_string()`
ASSERT:
  - Python output equals fixture `expected_canonical`
  - Rust output equals fixture `expected_canonical`
CHEAT DETECTION:
  - Shared fixture is single source of truth
  - Failures surface independently in Rust and Python suites (no cross-process masking)
```

### 9.2 Plan Fingerprint Stability Tests

```
TEST: plan_fingerprint_stability
LOCATION:
  - Rust: crates/arco-flow/tests/contract/plan_fingerprint_stability.rs (TBD)
RUNS:
  - CI (no cloud)
SETUP:
  - Manifest M1 with assets [A, B, C] where A→B→C
  - Catalog snapshot S1
EXECUTE:
  - Generate Plan P1 from (M1, S1)
  - Generate Plan P2 from (M1, S1) (same inputs)
  - Generate Plan P3 from (M1, S1) in different process
ASSERT:
  - P1.fingerprint == P2.fingerprint == P3.fingerprint
CHEAT DETECTION:
  - Plans generated in separate processes (no shared state)
  - Fingerprint preimage excludes timestamps/ULIDs; test fails if nondeterminism leaks
```

### 9.3 Scheduler State Machine Exhaustive Tests

```
TEST: run_state_machine_exhaustive
LOCATION:
  - crates/arco-flow/tests/contract/run_state_machine_exhaustive.rs (TBD)
RUNS:
  - CI (no cloud)
SETUP:
  - All valid (current_state, event) → next_state transitions documented
EXECUTE:
  - For each valid transition: apply event, verify state
  - For each invalid transition: apply event, verify rejection
ASSERT:
  - All valid transitions succeed
  - All invalid transitions fail with appropriate error
CHEAT DETECTION:
  - Test generated from proto enum + transition matrix (not hand-coded)
  - Adding new state to proto without updating matrix fails CI
```

```
TEST: task_state_machine_exhaustive
LOCATION:
  - crates/arco-flow/tests/contract/task_state_machine_exhaustive.rs (TBD)
RUNS:
  - CI (no cloud)
SETUP:
  - All valid (TaskState, TransitionReason) → TaskState documented in state_matrix.rs
EXECUTE:
  - Exhaustive cartesian product test
ASSERT:
  - All expected transitions succeed
  - All unexpected transitions fail
CHEAT DETECTION:
  - `#[cfg(test)]` static assertion that all enum variants covered
  - Compiler error if new variant added without matrix update
```

### 9.4 End-to-End Workflow Tests

```
TEST: deploy_run_observe_e2e
LOCATION:
  - python/arco/tests/integration/test_orchestration_e2e.py (TBD)
RUNS:
  - Nightly (cloud)
SETUP:
  - Clean workspace (no prior runs)
  - Python project with @asset("test_asset") that writes known output
EXECUTE:
  - servo deploy --workspace=test
  - servo run --asset=test_asset
  - Poll servo status until completion (timeout 60s)
  - servo logs <run_id>
ASSERT:
  - Deploy returns manifest_id
  - Run returns run_id with state=RUNNING initially
  - Final state=SUCCEEDED
  - Logs contain expected output string
  - Output file exists in storage with correct content
CHEAT DETECTION:
  - Test runs against real (ephemeral) cloud infra, not mocks
  - Manifest ID verified in control plane database
  - Output verified in actual object storage
```

```
TEST: backfill_pause_resume_e2e
LOCATION:
  - python/arco/tests/integration/test_backfill_e2e.py (TBD)
RUNS:
  - Nightly (cloud)
SETUP:
  - Partitioned asset with 10 daily partitions
EXECUTE:
  - Create backfill for all 10
  - Wait for 3 partitions to complete
  - Pause backfill
  - Verify no new tasks start for 30s
  - Resume backfill
  - Wait for completion
ASSERT:
  - Exactly 10 tasks completed (no duplicates)
  - Pause stopped new task dispatch
  - Resume continued from correct position
CHEAT DETECTION:
  - Task execution has side-effect (counter increment) that detects duplicates
  - Timing assertions verify pause actually stopped work
```

### 9.5 Failure Mode Tests

```
TEST: upstream_failure_skips_downstream
LOCATION:
  - crates/arco-flow/tests/integration/upstream_failure_skip.rs (TBD)
RUNS:
  - CI (no cloud)
SETUP:
  - DAG: A → B → C, continue_on_failure=false
EXECUTE:
  - Inject failure in A
  - Wait for run completion
ASSERT:
  - A: FAILED
  - B: SKIPPED (reason: upstream_failed)
  - C: SKIPPED (reason: upstream_failed)
  - Run: FAILED
CHEAT DETECTION:
  - Skip reason must reference A by name/id
  - B and C never transition to RUNNING (verified by event log)
```

```
TEST: zombie_detection_and_recovery
LOCATION:
  - crates/arco-flow/tests/integration/zombie_detection.rs (TBD)
RUNS:
  - CI (no cloud)
SETUP:
  - Task with heartbeat_timeout_ms=5000
EXECUTE:
  - Start task
  - Simulate worker crash (no heartbeats)
  - Wait 10s
ASSERT:
  - Task marked ZOMBIE within 6s of last heartbeat
  - Task retried or failed based on retry policy
CHEAT DETECTION:
  - Real clock used (not mocked time)
  - Heartbeat protocol verified via network capture
```

### 9.6 Multi-Tenant Isolation Tests

```
TEST: cross_tenant_isolation
LOCATION:
  - infra/tests/cross_tenant_isolation_e2e.md (TBD harness)
RUNS:
  - Nightly (cloud)
SETUP:
  - Tenant A workspace with asset writing to /tenants/A/data/
  - Tenant B workspace with asset reading from /tenants/A/data/
EXECUTE:
  - Tenant B attempts to read Tenant A data
ASSERT:
  - Read fails with PERMISSION_DENIED
  - Audit log records unauthorized access attempt
CHEAT DETECTION:
  - IAM policies verified at cloud provider level (not app-level mock)
  - Storage prefix enforcement verified via actual GCS/S3 call
```

### 9.7 Exactly-Once Semantics Tests

```
TEST: duplicate_callback_idempotency
LOCATION:
  - crates/arco-flow/tests/integration/duplicate_callback_idempotency.rs (TBD)
RUNS:
  - CI (no cloud)
SETUP:
  - Task that completes successfully
EXECUTE:
  - Send SUCCESS callback
  - Send duplicate SUCCESS callback with same idempotency_key
ASSERT:
  - Only one state transition occurs
  - Second callback logged as duplicate but no effect
CHEAT DETECTION:
  - Materialization counter = 1 (not 2)
  - Audit log shows idempotency hit
```

```
TEST: promotion_fencing_prevents_torn_writes
LOCATION:
  - crates/arco-core/tests/integration/promotion_fencing.rs (TBD)
RUNS:
  - Nightly (cloud)
SETUP:
  - Asset with gating check
EXECUTE:
  - Worker writes staged output
  - Check fails
  - Attempt promotion
ASSERT:
  - Staged output NOT promoted to live path
  - Check failure visible in results
CHEAT DETECTION:
  - Live path verified empty/unchanged via direct storage read
  - Staged path contains data but fenced
```

### 9.8 Outbox + Reconciliation Tests

```
TEST: flow_outbox_to_catalog_reconciliation_e2e
LOCATION:
  - crates/arco-flow/tests/integration/outbox_to_catalog_reconciliation.rs (TBD)
RUNS:
  - Nightly (cloud)
SETUP:
  - One run that emits an execution/materialization event into Flow outbox
  - Catalog ingest/subscriber enabled
EXECUTE:
  - Execute run end-to-end
  - Wait for outbox delivery + catalog ingest/compaction
  - Query catalog projection for the derived record
ASSERT:
  - Event is delivered at-least-once with an idempotency key (no duplicates in projection)
  - Catalog read model shows the derived record within SLA
CHEAT DETECTION:
  - Projection is queried via the real read path (Parquet/DuckDB), not mocks
  - Delivery verified by matching event IDs across Flow and Catalog projections
```

---

## Appendix A — Dagster capability checklist (used for parity matrix)

> The “robust Dagster feature set list” referenced in the prompt was not available in the current repo/thread context. This appendix is the reconstructed checklist used for the parity matrix. Items marked **(added by architect)** extend beyond a minimal “core orchestration” checklist to include production-grade operability and multi-tenant SaaS concerns.

### A) Core orchestration model + semantics
- **A1** Asset-centric authoring model (assets are first-class)
- **A2** Dependency resolution + DAG construction
- **A3** Deterministic planning + stable ordering
- **A4** Plan fingerprinting / reproducibility primitives
- **A5** Semantic task identity (asset + partition + operation)
- **A6** Run lifecycle states + transitions (incl. cancel/timeouts)
- **A7** Task lifecycle states + transitions (incl. retries/cancel/skip)
- **A8** Failure semantics (skip downstream; continue independent work)
- **A9** Retry semantics (policy, backoff, max attempts)
- **A10** Idempotent terminal transitions + late result handling **(added by architect)**

### B) Scheduling, sensors, triggers
- **B1** Cron schedules (timezone, tick history)
- **B2** Sensors (event-driven triggers with cursors)
- **B3** Conditional run triggering (filters/criteria)
- **B4** Trigger dedupe / idempotency (`run_key`-like semantics)
- **B5** Run queue semantics (queued state, backpressure)
- **B6** Concurrency gates + prioritization (global/per-scope)
- **B7** Scheduler/sensor observability (tick logs, cursor state, errors)

### C) Partitions + backfills
- **C1** Partition definitions (time/static)
- **C2** Multi-dimensional partitions
- **C3** Cross-language canonical partition identity (stable encoding)
- **C4** Partition mappings (identity/latest/window)
- **C5** Partition status tracking + UX (missing/failed/stale)
- **C6** Backfill creation (range selection)
- **C7** Backfill chunking + parallelism controls
- **C8** Backfill pause/resume/cancel
- **C9** Retry failed partitions only
- **C10** Partial backfills (subset partitions/assets)

### D) Compute/execution + resource model
- **D1** Worker/runtime contract (task envelope + callbacks)
- **D2** Local dev parity vs cloud parity (same semantics)
- **D3** Heartbeats + zombie detection
- **D4** Timeouts (dispatch-ack, heartbeat, execution)
- **D5** Cancellation (graceful + force)
- **D6** Resource model (cpu/mem/time) + deterministic serialization
- **D7** IO abstraction (inputs/outputs, storage integration)
- **D8** Secrets/variables + environment separation
- **D9** Retry semantics at runtime (worker + scheduler coordination)
- **D10** Deployment model (workspaces, code locations/versions)

### E) Observability + debugging + reexecution
- **E1** Execution event logs (structured, queryable)
- **E2** Run/task views (state/timing/attempts)
- **E3** Structured logs + compute log retrieval
- **E4** Metrics (dashboards/alerts)
- **E5** Tracing (context propagation)
- **E6** Retry history + timeline reconstruction
- **E7** Rerun from failure (reexecution)
- **E8** Subset reexecution (select subset of steps/assets)
- **E9** Audit trail (actor + correlation/causation)
- **E10** Event-sourcing projections + rebuild tooling **(added by architect)**

### F) Data quality + governance integration
- **F1** Asset checks definition model
- **F2** Check execution + results persistence
- **F3** Gating/quarantine semantics + promotion workflow
- **F4** Freshness/SLAs/health signals
- **F5** Lineage-by-execution + metadata capture

### G) Multi-tenancy, isolation, ops hardening
- **G1** Multi-tenant isolation boundaries (tenant/workspace scoping)
- **G2** Per-tenant quotas + fairness **(added by architect)**
- **G3** Rate limiting + backpressure **(added by architect)**
- **G4** Reliability pattern: outbox + dedupe + reconciliation **(added by architect)**
- **G5** Exactly-once effects for materializations (commit/promotion) **(added by architect)**
- **G6** HA scheduler + leader election
- **G7** Upgrade/migration story (schema evolution)
- **G8** Disaster recovery + projection rebuild

### H) Developer experience (DX)
- **H1** SDK ergonomics (assets/checks/partitions)
- **H2** CLI workflows (deploy/run/status/logs/backfill)
- **H3** Validation tooling (manifest/config)
- **H4** Local dev server experience
- **H5** Testing story (unit/integration harness)
