# Servo Event-Driven Orchestration Execution Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement event-driven orchestration for Servo, replacing the MVP Postgres-backed scheduler with a serverless, Parquet-native approach aligned with Arco's unified platform invariants.

**Architecture:** Stateless controllers reconcile from Parquet projections (base snapshot + L0 deltas). Durable timers via Cloud Tasks replace tick loops. Per-edge dependency satisfaction provides duplicate-safe readiness tracking. All writes go through the ledger → compactor pipeline (IAM-enforced sole writer).

**Tech Stack:** Rust 1.85+, Parquet/Arrow, GCS, Cloud Tasks, Cloud Run, Pub/Sub, DuckDB (reads)

**Runtime Packaging Decision:** Orchestration runtime services will be delivered under `arco-flow`
as Cloud Run services (not integrated into `arco-compactor`). Entry points will be added as
`servo_compactor`, `servo_dispatcher`, and `servo_sweeper` binaries (or a single
`servo_orchestrator` binary if consolidation is preferred).

---

## Document Review Summary

### Source Documents Reviewed

1. **servo-orchestration-unified-design.md** (v1.0, Approved)
   - Status: PRIMARY - Use as authoritative source
   - Aligns with unified platform invariants

2. **servo-orchestration-event-driven-design.md** (Draft)
   - Status: NOT FOUND in repository
   - Note: If located, mark it superseded by the unified design. Otherwise, treat as missing source.

### Alignment with Arco Platform Invariants

| Invariant | Design Doc | Codebase | Status |
|-----------|------------|----------|--------|
| Append-only ledger | `ledger/orchestration/*.json` | `ledger/{domain}/{ulid}.json` |  Aligned |
| Compactor sole writer | Controllers emit events, compactor writes Parquet | ADR-018 enforces | Aligned |
| Compaction is idempotent | `idempotency_key` + `row_version` dedup | Existing compactor pattern | Aligned |
| Manifest CAS publish | `revision_ulid` + storage versioning | `publish.rs` CAS pattern | Aligned |
| Controllers read Parquet only | "Controllers NEVER read JSON ledger" | Reader pattern in `reader.rs` | Aligned |
| No bucket listing | Manifest-driven paths | Anti-entropy only lists | Aligned |

### Critical Corrections Required

#### 1. Cloud Tasks Task ID Constraints (HIGH PRIORITY)

**Issue:** Design documents show patterns with colons that are invalid Cloud Tasks task IDs:
- `dispatch:{run_id}:{task_key}:{attempt}`
- `timer:retry:{run_id}:{task_key}:{attempt}:{due_epoch}`

**Cloud Tasks Constraints:**
- Task IDs can only contain `[A-Za-z0-9_-]` (no colons, no slashes)
- **Deduplication window: up to 24 hours** (not 9 days as some docs imply - that applies to legacy App Engine queue.yaml queues)
- Sequential prefixes increase latency (Google recommendation: use hashed prefixes)

**Existing Solution:** `crates/arco-flow/src/dispatch/cloud_tasks.rs:383-404` already has `sanitize_task_id()` that replaces invalid characters.

**Recommended Approach (per feedback):** Use deterministic hash-based IDs with dual-identifier pattern:

```rust
// Internal ID (for Parquet PKs, idempotency keys, debugging)
dispatch_id = "dispatch:{run_id}:{task_key}:{attempt}"

// Cloud Tasks-safe ID (for actual task names)
cloud_task_id = format!("d_{}", base32_encode(sha256(dispatch_id))[..26])
```

**Schema Impact:** Both `dispatch_outbox` and `timers` tables must store BOTH identifiers:
- `dispatch_id` / `timer_id` - human-readable internal ID
- `cloud_task_id` - API-compliant ID used in Cloud Tasks API calls

This keeps internal IDs human-readable for debugging/Parquet queries while ensuring Cloud Tasks compliance.

#### 2. dep_satisfaction Scalability

**Issue:** Current design does `count(dep_satisfaction WHERE downstream_task_key = X AND satisfied = true)` which can be O(all edges per run) if not optimized.

**Correction:** Add derived field `deps_satisfied_count` to `tasks_current` and update it idempotently:
```sql
deps_satisfied_count = COUNT(satisfied edges WHERE downstream = this_task)
```

Only recompute for impacted downstream tasks in each micro-compaction batch.

#### 3. Derived Events Location

**Clarification Needed:** Events like `TaskBecameReady`, `TaskSkipped`, `RunCompleted` are labeled "Derived" but their storage location is ambiguous.

**Decision:** Derived events are **projection-only** - they materialize into Parquet rows but are NOT appended to the ledger. They are computed during compaction fold, not emitted as separate ledger events.

**Clarification:** Remove `TaskBecameReady`, `TaskSkipped`, `RunCompleted` from any "Event Types" table or clearly mark them as "NOT persisted to ledger - projection-only state transitions."

#### 4. attempt_id as Concurrency Guard (NEW)

**Issue:** Out-of-order event delivery can cause state regression (e.g., "attempt 1 finished" arrives after "attempt 2 started").

**Decision:** Use `attempt_id` (ULID) as a first-class concurrency token:
1. Dispatcher includes `attempt_id` in the dispatch payload
2. Worker callbacks (`TaskStarted`, `TaskHeartbeat`, `TaskFinished`) MUST echo `attempt_id`
3. Compactor fold **rejects/no-ops** events whose `attempt_id` doesn't match the currently active `attempt_id` in `tasks.parquet`

This prevents state regression from stale events without relying solely on `max(attempt)` merge semantics.

#### 5. Watermark Freshness Guard for ALL Timer Actions (NEW)

**Issue:** The heartbeat freshness guard is correct, but the same class of bug exists for retry timers - if compaction is behind, a retry timer might fire "early" and schedule a retry that shouldn't happen.

**Decision:** Apply the watermark lag gate to ALL timer-driven actions:
- Retry timer fire → check watermark freshness before incrementing attempt
- Heartbeat timeout → check watermark freshness before failing task
- "Stuck DISPATCHED" repairs → check watermark freshness before re-dispatching

This makes timer actions safe under compaction lag, which is the #1 real-world failure mode in serverless reconciliation systems.

#### 6. Failure/Skip Propagation Semantics (NEW)

**Issue:** The design implies failure propagation but doesn't define crisp rules.

**Decision:** Define explicit edge resolution semantics:

| Upstream Outcome | Edge Resolution | Downstream Effect |
|------------------|-----------------|-------------------|
| Succeeded | `SUCCESS` | Satisfies dependency |
| Failed (terminal) | `FAILED` | Downstream → SKIPPED (fail-fast policy) |
| Skipped | `SKIPPED` | Downstream → SKIPPED (transitive) |
| Cancelled | `CANCELLED` | Downstream → CANCELLED (transitive) |

Readiness rule:
```
downstream.state = READY iff:
  ALL upstream edges have resolution = SUCCESS
  AND downstream.deps_satisfied_count == downstream.deps_total
```

#### 7. Controller Event Categories (NEW)

**Decision:** Controllers only emit **Intent** and **Acknowledgement** facts to the ledger:

| Category | Events | Description |
|----------|--------|-------------|
| Intent | `DispatchRequested`, `TimerRequested` | "I want this to happen" |
| Acknowledgement | `DispatchEnqueued`, `TimerEnqueued` | "External system accepted" |
| Worker Facts | `TaskStarted`, `TaskHeartbeat`, `TaskFinished` | "This happened" |

Derived state changes (`TaskBecameReady`, `TaskSkipped`, `RunCompleted`) are **projection-only** - computed during compaction fold and written to Parquet, not emitted as ledger events.

#### 8. Micro-Compactor Trigger Path (CLARIFICATION)

**Decision:** Two-path event processing:

| Path | Trigger | Use Case |
|------|---------|----------|
| Primary | GCS finalize notification → process specific file(s) | Hot path, low latency |
| Fallback | Periodic listing using watermark | Catch-up, anti-entropy |

The micro-compactor should NOT rely on listing as the hot path (cost + latency at scale).

#### 9. Quota/Fairness (EXPLICIT DECISION)

**v1 Decision:** Global concurrency cap only.

Per-tenant quota enforcement is deferred to v2. Options for v2:
- Queue-per-tenant (expensive operationally)
- Quota-aware dispatcher (reads tenant usage projection, only enqueues when capacity exists)
- Worker-side admission control (simpler but can waste Cloud Tasks invocations)

This is explicitly called out so we don't accidentally ship "no isolation."

#### 10. Document Supersession

Add to `servo-orchestration-event-driven-design.md`:
```markdown
> **SUPERSEDED:** This document is superseded by `servo-orchestration-unified-design.md` (v1.0).
> The unified design removes per-run `sequence` ordering and consolidates all patterns.
```

---

## Dagster Parity Alignment

> **Reference Documents:**
> - [2025-12-19-servo-arco-vs-dagster-parity-audit.md](2025-12-19-servo-arco-vs-dagster-parity-audit.md) - Full feature parity audit
> - [servo-dagster-exception-planning.md](../audits/servo-dagster-exception-planning.md) - Deliberate exceptions with stakeholder sign-off

This execution plan builds **Layer 1: Execution Engine Primitives** (runs/tasks/retries/dispatch/timers/anti-entropy). The Dagster parity documents cover **Layer 2: Asset Automation** (asset graph, partitions/backfills, sensors/schedules, auto-materialize, freshness).

**Key insight:** We don't duplicate parity work here. Instead, we ensure this plan provides the correct **hooks and schema decisions** so Layer 2 slots in cleanly.

### Dagster-Shaped Decisions Locked in This Plan

| Decision | How This Plan Supports It | Dagster Feature Enabled |
|----------|---------------------------|------------------------|
| **Asset + partition identity in tasks** | `tasks.parquet` includes `asset_key`, `partition_key` columns (nullable for non-asset tasks) | Asset graph visualization, partition status, staleness tracking |
| **Materialization-driven triggers** | `TriggerFired` event type with `type=materialization` + upstream references | Auto-materialize, downstream propagation |
| **Partition sets in RunTriggered** | `RunTriggered` payload supports `partition_selection: List<PartitionKey>` | Backfills, incremental runs, partition-aware scheduling |
| **Schedules as timer chains** | Cron timers emit `TriggerFired(type=cron)` → `RunTriggered` | Schedule tick history, catch-up behavior |
| **Sensors as event handlers** | Sensor definitions stored in catalog; evaluation emits `TriggerFired(type=sensor)` | Event-driven automation, cursor durability |
| **run_key idempotency** | `RunTriggered` includes `run_key` for deduplication + payload fingerprint consistency (cutoff-based) | Duplicate trigger prevention + conflict on mismatched payloads (B4 in parity audit) |
| **Partition status tracking** | `partition_status.parquet` projection (schema reserved in Epic 3) | "What's stale/missing" queries (C5 in parity audit) |

### Schema Extensions for Asset Automation (Epic 3)

Add these columns/tables to support Layer 2:

**tasks.parquet** (extend existing schema):

| Column | Type | Description |
|--------|------|-------------|
| asset_key | STRING (nullable) | Fully-qualified asset key (e.g., `analytics.daily_summary`) |
| partition_key | STRING (nullable) | Canonical partition key (ADR-011 encoding) |

**partition_status.parquet** (new table for Layer 2, schema reserved):

| Column | Type | Description |
|--------|------|-------------|
| tenant_id | STRING | Tenant identifier |
| workspace_id | STRING | Workspace identifier |
| asset_key | STRING | Asset identifier |
| partition_key | STRING | Partition identifier |
| status | STRING | MISSING/MATERIALIZING/MATERIALIZED/FAILED/STALE |
| last_materialization_id | STRING | Run that last materialized this partition |
| last_materialization_at | TIMESTAMP | When last materialized |
| freshness_deadline | TIMESTAMP | When this partition becomes stale (optional) |
| row_version | STRING | ULID of last update |

**schedules.parquet** (new table for Layer 2, schema reserved):

| Column | Type | Description |
|--------|------|-------------|
| tenant_id | STRING | Tenant identifier |
| workspace_id | STRING | Workspace identifier |
| schedule_id | STRING | Schedule identifier |
| cron_expression | STRING | Cron pattern |
| asset_selection | STRING | JSON array of asset keys to materialize |
| last_tick_at | TIMESTAMP | Last evaluation time |
| next_tick_at | TIMESTAMP | Next scheduled evaluation |
| cursor | STRING | Schedule cursor state |
| status | STRING | ACTIVE/PAUSED/ERROR |
| row_version | STRING | ULID of last update |

**sensors.parquet** (new table for Layer 2, schema reserved):

| Column | Type | Description |
|--------|------|-------------|
| tenant_id | STRING | Tenant identifier |
| workspace_id | STRING | Workspace identifier |
| sensor_id | STRING | Sensor identifier |
| cursor | STRING | Durable cursor state |
| last_evaluation_at | TIMESTAMP | Last evaluation time |
| status | STRING | ACTIVE/PAUSED/ERROR |
| row_version | STRING | ULID of last update |

### Layer 2 Schema Status (Deferred)

The following Layer 2 schemas are documented for parity alignment but are deferred to M2.
No placeholder implementations are added in this plan:

- `partition_status.parquet` - Asset partition tracking
- `schedules.parquet` - Cron schedule state
- `sensors.parquet` - Sensor cursor state

### Event Types for Asset Automation

These events are emitted by Layer 2 controllers, processed by the same compactor:

| Event Type | Producer | Consumer | Purpose |
|------------|----------|----------|---------|
| `ScheduleTicked` | Timer Controller | Compactor | Schedule evaluated at time T |
| `SensorEvaluated` | Sensor Controller | Compactor | Sensor evaluated with cursor C |
| `PartitionMaterialized` | Compactor (derived) | Asset Automation | Partition completed successfully |
| `AssetBecameStale` | Freshness Evaluator | Asset Automation | Asset freshness exceeded |

### Controller Interfaces for Layer 2

Layer 2 controllers use the same patterns established in this plan:

```rust
// Asset Automation controllers (Layer 2) follow the same interface
trait AssetController {
    /// Read base + L0 deltas from manifest
    fn read_state(&self, manifest: &Manifest) -> Result<State>;

    /// Compute actions based on state
    fn reconcile(&self, state: &State) -> Vec<Action>;

    /// Execute actions (emit events, create timers)
    fn execute(&self, actions: Vec<Action>) -> Result<Vec<Event>>;
}
```

**Controllers to implement in Layer 2 (not this plan):**

- `ScheduleEvaluator` - Fires schedule triggers on cron tick
- `SensorEvaluator` - Evaluates sensors and fires triggers
- `FreshnessEvaluator` - Computes staleness and may trigger auto-materialize
- `BackfillController` - Manages backfill lifecycle (pause/resume/cancel)
- `ReconciliationController` - The "auto-materialize" engine

### What This Plan DOES NOT Implement

Per the exception planning document, these are deferred or excluded:

| Feature | Status | Reference |
|---------|--------|-----------|
| Op/Job model (non-asset) | Permanent exception | EX-01 |
| Customer-operated daemon | Permanent exception | EX-02 |
| Orchestration UI | Deferred to M2 | EX-03 |
| IO Managers | Deferred to M2 | EX-04 |
| Multi-asset / graph-asset | Deferred to M2 | EX-05a |
| Observable sources | Deferred to M3 | EX-05b |
| Multi-code-location | Simplified | EX-06 |
| GraphQL API | Permanent exception | EX-07 |
| K8s/ECS launchers | Deferred to M3 | EX-08 |
| Full asset reconciliation | Phased M2/M3 | EX-09 |

### Milestone Mapping

| This Plan | Parity Audit | Exception Planning |
|-----------|--------------|-------------------|
| Epic 0-4 | P0 gaps (A1-A7, E1-E2) | Milestone M1 |
| Epic 5-7 | P0/P1 gaps (B1-B2, C1-C5) | Milestone M1/M2 |
| Layer 2 (future) | P1/P2 gaps (C6-C10, F1-F5) | Milestone M2/M3 |

---

## Epic Structure

### Epic 0: Architecture Decision Records

**Goal:** Lock all design decisions before implementation.

**Files:**
- Create: `docs/adr/adr-020-orchestration-domain.md`
- Create: `docs/adr/adr-021-cloud-tasks-naming.md`
- Create: `docs/adr/adr-022-dependency-satisfaction.md`
- Modify: `docs/plans/servo-orchestration-event-driven-design.md` (add supersession notice)

---

**Task 0.1: ADR-020 Orchestration Domain**

**Step 1: Write ADR documenting orchestration as unified domain**

```markdown
# ADR-020: Orchestration as Unified Domain

## Status
Accepted

## Context
Servo orchestration needs storage for runs, tasks, timers, and dispatch outbox.
Following the unified platform pattern, orchestration becomes another domain
with its own manifest, ledger path, and Parquet projections.

## Decision
Add `orchestration` domain following existing patterns:
- Manifest: `manifests/orchestration.manifest.json`
- Ledger: `ledger/orchestration/{ulid}.json`
- State: `state/orchestration/{table}/`
- Controllers read base + L0 deltas (never ledger)

Tables:
- `runs.parquet` - Run state and counters
- `tasks.parquet` - Task state machine
- `dep_satisfaction.parquet` - Per-edge dependency facts
- `timers.parquet` - Active durable timers
- `dispatch_outbox.parquet` - Pending dispatch intents

All tables include `row_version` (event_id ULID) for deterministic merge.

## Consequences
- Follows established patterns (minimal new concepts)
- Reuses compactor infrastructure
- IAM-enforced sole writer automatically applies
```

**Step 2: Commit**

```bash
git add docs/adr/adr-020-orchestration-domain.md
git commit -m "docs(adr): ADR-020 orchestration as unified domain"
```

---

**Task 0.2: ADR-021 Cloud Tasks Naming**

**Step 1: Write ADR documenting naming strategy**

```markdown
# ADR-021: Cloud Tasks Naming Convention

## Status
Accepted

## Context
Cloud Tasks task IDs have constraints:
- Only `[A-Za-z0-9_-]` allowed (no colons, slashes)
- 500 character max
- Cannot reuse names for ~9 days after deletion
- Sequential prefixes increase latency (Google recommendation)

Our internal IDs use colons for readability:
- `dispatch:{run_id}:{task_key}:{attempt}`
- `timer:retry:{run_id}:{task_key}:{attempt}:{due_epoch}`

## Decision
Maintain TWO identifiers:

1. **Internal ID** (idempotency_key, Parquet PK): Human-readable with colons
   - `dispatch:{run_id}:{task_key}:{attempt}`
   - `timer:retry:{run_id}:{task_key}:{attempt}:{due_epoch}`

2. **Cloud Tasks ID**: Hash-based, deterministic, compliant
   - `d_{base32(sha256(internal_id))[0..26]}` for dispatch
   - `t_{base32(sha256(internal_id))[0..26]}` for timers

Properties:
- Deterministic: Same internal ID always produces same Cloud Tasks ID
- Unique: SHA-256 collision is practically impossible
- Non-sequential: Hash prefix avoids latency issues
- Compliant: Only uses `[A-Za-z0-9_]`

Implementation:
```rust
fn cloud_task_id(kind: &str, internal_id: &str) -> String {
    let hash = sha256(internal_id.as_bytes());
    let encoded = base32::encode(Base32::RFC4648_NOPAD, &hash);
    format!("{}_{}", kind, &encoded[..26])
}
```

## Consequences
- Internal IDs remain readable for debugging/Parquet queries
- Cloud Tasks IDs are opaque but deterministic
- Existing `sanitize_task_id` in dispatcher can be updated
```

**Step 2: Commit**

```bash
git add docs/adr/adr-021-cloud-tasks-naming.md
git commit -m "docs(adr): ADR-021 Cloud Tasks naming convention"
```

---

**Task 0.3: ADR-022 Dependency Satisfaction**

**Step 1: Write ADR documenting per-edge satisfaction pattern**

```markdown
# ADR-022: Per-Edge Dependency Satisfaction

## Status
Accepted

## Context
Counter-based dependency tracking (`unmet_deps_count -= 1`) breaks under duplicates.
If `TaskFinished` is delivered twice, the counter decrements twice, causing
premature readiness.

## Decision
Use **per-edge satisfaction facts** in `dep_satisfaction.parquet`:

Primary key: `(run_id, upstream_task_key, downstream_task_key)`

On `TaskFinished(succeeded)`:
1. Upsert `dep_satisfaction[upstream, downstream].satisfied = true`
2. If edge was **newly satisfied** (not already true):
   - Increment `tasks[downstream].deps_satisfied_count`
   - If `deps_satisfied_count == deps_total`, mark READY

Duplicate `TaskFinished` re-upserts the same edge (no-op).
The conditional increment only fires on first satisfaction.

### Scalability
For large DAGs, avoid recomputing all edges:
- Cluster `dep_satisfaction` by `downstream_task_key` for efficient pruning
- Only process impacted downstream tasks in each compaction batch
- Derived `deps_satisfied_count` avoids COUNT queries at read time

## Consequences
- Duplicate-safe readiness (critical correctness property)
- O(out_degree) work per task completion
- Requires edge table in addition to task table
```

**Step 2: Commit**

```bash
git add docs/adr/adr-022-dependency-satisfaction.md
git commit -m "docs(adr): ADR-022 per-edge dependency satisfaction"
```

---

**Task 0.4: REMOVED**

The document `servo-orchestration-event-driven-design.md` was not found in this repository.
If a predecessor design document exists, add a supersession notice there. Otherwise, skip this task.

---

### Epic 1: Orchestration Event Envelope + Ledger Append

**Goal:** Establish orchestration as a domain with event ingestion.

**Files:**
- Create: `crates/arco-flow/src/orchestration/mod.rs`
- Create: `crates/arco-flow/src/orchestration/events/mod.rs`
- Create: `crates/arco-flow/src/orchestration/events/envelope.rs`
- Create: `crates/arco-flow/src/orchestration/events/run_events.rs`
- Create: `crates/arco-flow/src/orchestration/events/task_events.rs`
- Create: `crates/arco-flow/src/orchestration/events/timer_events.rs`
- Test: `crates/arco-flow/tests/orchestration/event_tests.rs`

---

**Task 1.1: Event Envelope Schema**

**Step 1: Write failing test for event envelope**

```rust
// crates/arco-flow/tests/orchestration/event_tests.rs
#[test]
fn test_event_envelope_serialization() {
    let event = OrchestrationEvent {
        event_id: Ulid::new().to_string(),
        event_type: "TaskFinished".to_string(),
        event_version: 1,
        timestamp: Utc::now(),
        source: "servo/tenant-abc/workspace-prod".to_string(),
        tenant_id: "tenant-abc".to_string(),
        workspace_id: "workspace-prod".to_string(),
        idempotency_key: "finished:run123:task456:1".to_string(),
        correlation_id: Some("run123".to_string()),
        causation_id: None,
        payload: serde_json::json!({"task_id": "task456", "outcome": "succeeded"}),
    };

    let json = serde_json::to_string(&event).unwrap();
    let parsed: OrchestrationEvent = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.event_type, "TaskFinished");
    assert_eq!(parsed.idempotency_key, "finished:run123:task456:1");
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test -p arco-flow orchestration::event_tests::test_event_envelope_serialization
```

Expected: FAIL with "cannot find value `OrchestrationEvent`"

**Step 3: Write envelope implementation**

```rust
// crates/arco-flow/src/orchestration/events/envelope.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Orchestration event envelope following ADR-004 pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationEvent {
    pub event_id: String,
    pub event_type: String,
    pub event_version: u32,
    pub timestamp: DateTime<Utc>,
    pub source: String,
    pub tenant_id: String,
    pub workspace_id: String,
    pub idempotency_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    pub payload: Value,
}
```

**Step 4: Run test to verify it passes**

```bash
cargo test -p arco-flow orchestration::event_tests::test_event_envelope_serialization
```

Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-flow/src/orchestration/
git add crates/arco-flow/tests/orchestration/
git commit -m "feat(orchestration): add event envelope schema"
```

---

**Task 1.2: Run Lifecycle Events**

**Step 1: Write failing test for RunTriggered event**

```rust
#[test]
fn test_run_triggered_event() {
    let payload = RunTriggeredPayload {
        run_id: "01HQXYZ123RUN".to_string(),
        plan_id: "01HQXYZ123PLN".to_string(),
        trigger: TriggerInfo::Cron { schedule_id: "daily-etl".to_string() },
        root_assets: vec!["analytics.daily_summary".to_string()],
    };

    let event = OrchestrationEvent::run_triggered(
        "tenant-abc",
        "workspace-prod",
        payload,
    );

    assert_eq!(event.event_type, "RunTriggered");
    assert!(event.idempotency_key.starts_with("run:"));
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test -p arco-flow orchestration::event_tests::test_run_triggered_event
```

Expected: FAIL

**Step 3: Implement run events**

```rust
// crates/arco-flow/src/orchestration/events/run_events.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunTriggeredPayload {
    pub run_id: String,
    pub plan_id: String,
    pub trigger: TriggerInfo,
    pub root_assets: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerInfo {
    Cron { schedule_id: String },
    Manual { user_id: String },
    Materialization { upstream_materialization_id: String },
    Webhook { webhook_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunCompletedPayload {
    pub run_id: String,
    pub outcome: RunOutcome,
    pub tasks_succeeded: u32,
    pub tasks_failed: u32,
    pub tasks_skipped: u32,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutcome {
    Succeeded,
    Failed,
    Cancelled,
}
```

**Step 4: Run test to verify it passes**

```bash
cargo test -p arco-flow orchestration::event_tests::test_run_triggered_event
```

**Step 5: Commit**

```bash
git add crates/arco-flow/src/orchestration/events/run_events.rs
git commit -m "feat(orchestration): add run lifecycle events"
```

---

**Task 1.3: Task Lifecycle Events**

**Step 1: Write failing tests for task events**

```rust
#[test]
fn test_task_finished_idempotency_key() {
    let payload = TaskFinishedPayload {
        task_id: "01HQXYZ123TSK".to_string(),
        run_id: "01HQXYZ123RUN".to_string(),
        task_key: "extract".to_string(),
        attempt: 1,
        outcome: TaskOutcome::Succeeded,
        materialization_id: Some("01HQXYZ456MAT".to_string()),
        error_message: None,
        started_at: Utc::now(),
        completed_at: Utc::now(),
    };

    let key = payload.idempotency_key();
    assert_eq!(key, "finished:01HQXYZ123RUN:extract:1");
}
```

**Step 2: Run test, verify fail, implement, verify pass, commit**

(Follow standard TDD cycle)

**Step 3: Commit**

```bash
git add crates/arco-flow/src/orchestration/events/task_events.rs
git commit -m "feat(orchestration): add task lifecycle events"
```

---

### Epic 2: Micro-Compactor + L0 Delta Pipeline

**Goal:** Near-real-time Parquet visibility without ledger reads.

**Files:**
- Create: `crates/arco-flow/src/orchestration/compactor/mod.rs`
- Create: `crates/arco-flow/src/orchestration/compactor/micro.rs`
- Create: `crates/arco-flow/src/orchestration/compactor/manifest.rs`
- Create: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Test: `crates/arco-flow/tests/orchestration/compaction_tests.rs`

---

**Task 2.1: Orchestration Manifest Schema**

**Step 1: Write failing test for manifest structure**

```rust
#[test]
fn test_orchestration_manifest_schema() {
    let manifest = OrchestrationManifest {
        schema_version: 2,
        revision_ulid: Ulid::new().to_string(),
        published_at: Utc::now(),
        watermarks: Watermarks {
            events_processed_through: "01HQX...".to_string(),
            last_processed_file: "20250115-103000-01HQX.json".to_string(),
            last_processed_at: Utc::now(),
        },
        base_snapshot: BaseSnapshot {
            snapshot_id: "01HQX...SNAP".to_string(),
            published_at: Utc::now(),
            tables: Tables::default(),
        },
        l0_deltas: vec![],
        l0_count: 0,
        l0_limits: L0Limits::default(),
    };

    let json = serde_json::to_string_pretty(&manifest).unwrap();
    assert!(json.contains("watermarks"));
    assert!(json.contains("base_snapshot"));
    assert!(json.contains("l0_deltas"));
}
```

**Step 2: Run test, verify fail, implement manifest struct**

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add manifest schema with L0 deltas"
```

---

**Task 2.2: Event Fold Logic**

**Step 1: Write failing test for fold on PlanCreated**

```rust
#[test]
fn test_fold_plan_created_initializes_tasks() {
    let event = plan_created_event(
        "run123",
        vec![
            TaskDef { key: "extract", depends_on: vec![] },
            TaskDef { key: "transform", depends_on: vec!["extract"] },
            TaskDef { key: "load", depends_on: vec!["transform"] },
        ],
    );

    let mut state = FoldState::new();
    state.fold_event(&event);

    // Extract has no deps, should be READY
    assert_eq!(state.tasks["extract"].state, TaskState::Ready);
    assert_eq!(state.tasks["extract"].deps_total, 0);

    // Transform depends on extract, should be BLOCKED
    assert_eq!(state.tasks["transform"].state, TaskState::Blocked);
    assert_eq!(state.tasks["transform"].deps_total, 1);

    // Dep satisfaction edges should be created
    assert!(state.dep_satisfaction.contains_key(&("extract", "transform")));
}
```

**Step 2: Implement fold logic following design doc section 6.2**

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement PlanCreated fold logic"
```

---

**Task 2.3: Deterministic Merge with row_version**

**Step 1: Write failing test for merge ordering**

```rust
#[test]
fn test_merge_uses_row_version_not_file_order() {
    // Simulate base + L0 delta with out-of-order row_version
    let base_row = TaskRow {
        task_key: "extract".to_string(),
        state: TaskState::Running,
        row_version: "01A...".to_string(), // older
    };

    let delta_row = TaskRow {
        task_key: "extract".to_string(),
        state: TaskState::Succeeded,
        row_version: "01B...".to_string(), // newer
    };

    let merged = merge_task_rows(vec![base_row, delta_row]);

    // Should pick row with max row_version
    assert_eq!(merged.state, TaskState::Succeeded);
}
```

**Step 2: Implement merge logic**

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement deterministic row_version merge"
```

---

### Epic 3: Parquet Schemas

**Goal:** Production-ready schemas with correctness guarantees.

**Files:**
- Create: `crates/arco-flow/src/orchestration/schemas/mod.rs`
- Create: `crates/arco-flow/src/orchestration/schemas/runs.rs`
- Create: `crates/arco-flow/src/orchestration/schemas/tasks.rs`
- Create: `crates/arco-flow/src/orchestration/schemas/dep_satisfaction.rs`
- Create: `crates/arco-flow/src/orchestration/schemas/timers.rs`
- Create: `crates/arco-flow/src/orchestration/schemas/dispatch_outbox.rs`

---

#### Schema Reference (Updated with Feedback)

**tasks.parquet** - Task state machine with attempt_id guard

| Column | Type | Description |
|--------|------|-------------|
| tenant_id | STRING | Tenant identifier |
| workspace_id | STRING | Workspace identifier |
| run_id | STRING | Run identifier |
| task_key | STRING | Task name within run (PK component) |
| state | STRING | PLANNED/BLOCKED/READY/DISPATCHED/RUNNING/RETRY_WAIT/SKIPPED/CANCELLED/FAILED/SUCCEEDED |
| attempt | INT32 | Current attempt number (1-indexed) |
| attempt_id | STRING | **ULID for current attempt - concurrency guard** |
| deps_total | INT32 | Total number of dependencies |
| deps_satisfied_count | INT32 | **Derived: count of satisfied deps (updated idempotently)** |
| max_attempts | INT32 | Maximum retry attempts allowed |
| last_heartbeat_at | TIMESTAMP | Last heartbeat from worker |
| heartbeat_timeout_sec | INT32 | Heartbeat timeout threshold |
| ready_at | TIMESTAMP | When task became READY (for anti-entropy) |
| row_version | STRING | ULID of last event that modified this row |

**dispatch_outbox.parquet** - Dual-identifier pattern

| Column | Type | Description |
|--------|------|-------------|
| tenant_id | STRING | Tenant identifier |
| workspace_id | STRING | Workspace identifier |
| run_id | STRING | Run identifier |
| task_key | STRING | Task name |
| attempt | INT32 | Attempt number |
| dispatch_id | STRING | **Internal ID: `dispatch:{run_id}:{task_key}:{attempt}`** |
| cloud_task_id | STRING | **Cloud Tasks ID: `d_{hash}` (API-compliant)** |
| status | STRING | PENDING/CREATED/ACKED/FAILED |
| attempt_id | STRING | **Attempt ULID to include in payload** |
| worker_queue | STRING | Target queue name |
| created_at | TIMESTAMP | When outbox entry was created |
| row_version | STRING | ULID of last event |

**timers.parquet** - Dual-identifier pattern

| Column | Type | Description |
|--------|------|-------------|
| tenant_id | STRING | Tenant identifier |
| workspace_id | STRING | Workspace identifier |
| timer_id | STRING | **Internal ID: `timer:{type}:{run_id}:{task_key}:{attempt}:{epoch}`** |
| cloud_task_id | STRING | **Cloud Tasks ID: `t_{hash}` (API-compliant)** |
| timer_type | STRING | RETRY/HEARTBEAT_CHECK/CRON/SLA_CHECK |
| run_id | STRING | Associated run (nullable for cron) |
| task_key | STRING | Associated task (nullable for cron) |
| attempt | INT32 | Associated attempt (for retry/heartbeat) |
| fire_at | TIMESTAMP | Scheduled fire time |
| state | STRING | SCHEDULED/FIRED/CANCELLED |
| payload | STRING | JSON payload for handler |
| row_version | STRING | ULID of last event |

**dep_satisfaction.parquet** - Per-edge facts with resolution

| Column | Type | Description |
|--------|------|-------------|
| tenant_id | STRING | Tenant identifier |
| workspace_id | STRING | Workspace identifier |
| run_id | STRING | Run identifier |
| upstream_task_key | STRING | Upstream task (PK component) |
| downstream_task_key | STRING | Downstream task (PK component) |
| satisfied | BOOL | Whether edge is satisfied |
| resolution | STRING | **SUCCESS/FAILED/SKIPPED/CANCELLED** |
| satisfied_at | TIMESTAMP | When edge was satisfied |
| satisfying_attempt | INT32 | Which attempt satisfied the edge |
| row_version | STRING | ULID of satisfying event |

---

**Task 3.1: Tasks Schema with State Ranks and attempt_id**

**Step 1: Write failing test for state rank ordering**

```rust
#[test]
fn test_task_state_ranks_are_monotonic() {
    // Higher rank = more terminal
    assert!(TaskState::Pending.rank() < TaskState::Ready.rank());
    assert!(TaskState::Ready.rank() < TaskState::Running.rank());
    assert!(TaskState::Running.rank() < TaskState::Succeeded.rank());

    // Terminal states have highest ranks
    assert!(TaskState::Failed.rank() > TaskState::Running.rank());
    assert!(TaskState::Skipped.rank() > TaskState::Running.rank());
}

#[test]
fn test_merge_prefers_higher_state_rank() {
    let row1 = TaskRow { state: TaskState::Running, row_version: "01A".into() };
    let row2 = TaskRow { state: TaskState::Succeeded, row_version: "01A".into() };

    // Same row_version, should use state_rank as tiebreaker
    let merged = merge_by_state_rank(row1, row2);
    assert_eq!(merged.state, TaskState::Succeeded);
}
```

**Step 2: Implement TaskState enum with rank method**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskState {
    Planned,      // 0
    Blocked,      // 1
    Ready,        // 2
    Dispatched,   // 3
    Running,      // 4
    RetryWait,    // 5
    Skipped,      // 10
    Cancelled,    // 11
    Failed,       // 12
    Succeeded,    // 13
}

impl TaskState {
    pub const fn rank(self) -> u8 {
        match self {
            Self::Planned => 0,
            Self::Blocked => 1,
            Self::Ready => 2,
            Self::Dispatched => 3,
            Self::Running => 4,
            Self::RetryWait => 5,
            Self::Skipped => 10,
            Self::Cancelled => 11,
            Self::Failed => 12,
            Self::Succeeded => 13,
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Skipped | Self::Cancelled)
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add task state with monotonic ranks"
```

---

**Task 3.2: dep_satisfaction Schema**

**Step 1: Write failing test for edge satisfaction**

```rust
#[test]
fn test_dep_satisfaction_primary_key() {
    let edge1 = DepSatisfactionRow {
        tenant_id: "t1".into(),
        workspace_id: "ws1".into(),
        run_id: "run1".into(),
        upstream_task_key: "extract".into(),
        downstream_task_key: "transform".into(),
        satisfied: true,
        satisfied_at: Some(Utc::now()),
        outcome: Some(DepOutcome::Succeeded),
        row_version: "01A".into(),
    };

    let edge2 = DepSatisfactionRow {
        upstream_task_key: "extract".into(),
        downstream_task_key: "load".into(),
        ..edge1.clone()
    };

    // Different downstream = different edge
    assert_ne!(edge1.primary_key(), edge2.primary_key());
}
```

**Step 2: Implement dep_satisfaction schema**

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add dep_satisfaction schema"
```

---

### Epic 4: Dispatch Pipeline End-to-End

**Goal:** Prove the complete execution loop.

**Files:**
- Create: `crates/arco-flow/src/orchestration/controllers/mod.rs`
- Create: `crates/arco-flow/src/orchestration/controllers/dispatcher.rs`
- Modify: `crates/arco-flow/src/dispatch/cloud_tasks.rs` (add hash-based ID)
- Test: `crates/arco-flow/tests/orchestration/dispatch_tests.rs`

---

**Task 4.1: Cloud Tasks ID Hash Function**

**Step 1: Write failing test for deterministic hash ID**

```rust
#[test]
fn test_cloud_task_id_is_deterministic() {
    let internal_id = "dispatch:run123:extract:1";

    let id1 = cloud_task_id("d", internal_id);
    let id2 = cloud_task_id("d", internal_id);

    assert_eq!(id1, id2);  // Same input = same output
    assert!(id1.starts_with("d_"));
    assert!(id1.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
}

#[test]
fn test_cloud_task_id_is_unique() {
    let id1 = cloud_task_id("d", "dispatch:run123:extract:1");
    let id2 = cloud_task_id("d", "dispatch:run123:extract:2");

    assert_ne!(id1, id2);  // Different input = different output
}
```

**Step 2: Implement hash function**

```rust
use sha2::{Sha256, Digest};
use base32::Alphabet;

pub fn cloud_task_id(prefix: &str, internal_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(internal_id.as_bytes());
    let hash = hasher.finalize();

    let encoded = base32::encode(Alphabet::RFC4648 { padding: false }, &hash);
    format!("{}_{}", prefix, &encoded[..26].to_lowercase())
}
```

**Step 3: Commit**

```bash
git commit -m "feat(dispatch): add deterministic hash-based Cloud Tasks ID"
```

---

**Task 4.2: Dispatcher Controller**

**Step 1: Write failing test for dispatcher reading outbox**

```rust
#[test]
async fn test_dispatcher_creates_cloud_tasks_for_pending_outbox() {
    let mut dispatcher = MockDispatcher::new();

    let outbox_rows = vec![
        DispatchOutboxRow {
            dispatch_id: "dispatch:run1:extract:1".into(),
            status: DispatchStatus::Pending,
            ..Default::default()
        },
    ];

    let events = dispatcher.reconcile(outbox_rows).await;

    // Should emit DispatchEnqueued event
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "DispatchEnqueued");
}
```

**Step 2: Implement dispatcher controller**

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add dispatcher controller"
```

---

### Epic 5: Dependency Resolution

**Goal:** DAG execution with duplicate-safe readiness.

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Test: `crates/arco-flow/tests/orchestration/dependency_tests.rs`

---

**Task 5.1: TaskFinished Fold with DepSatisfied**

**Step 1: Write failing test for duplicate-safe satisfaction**

```rust
#[test]
fn test_duplicate_task_finished_does_not_double_decrement() {
    let mut state = test_state_with_dag("A -> B");

    // First TaskFinished(A, succeeded)
    state.fold_event(&task_finished("A", TaskOutcome::Succeeded));
    assert_eq!(state.tasks["B"].deps_satisfied_count, 1);
    assert_eq!(state.tasks["B"].state, TaskState::Ready);

    // Duplicate TaskFinished(A, succeeded)
    state.fold_event(&task_finished("A", TaskOutcome::Succeeded));

    // Should still be 1, not 2
    assert_eq!(state.tasks["B"].deps_satisfied_count, 1);
}
```

**Step 2: Implement per-edge satisfaction fold**

```rust
fn fold_task_finished(&mut self, event: &TaskFinishedPayload) {
    // Update task state
    self.tasks.get_mut(&event.task_key).map(|task| {
        task.state = match event.outcome {
            TaskOutcome::Succeeded => TaskState::Succeeded,
            TaskOutcome::Failed => TaskState::Failed,
            _ => task.state,
        };
        task.row_version = event.event_id.clone();
    });

    // Emit DepSatisfied for each downstream
    if event.outcome == TaskOutcome::Succeeded {
        let dependents = self.get_dependents(&event.task_key);
        for downstream_key in dependents {
            self.emit_dep_satisfied(
                &event.task_key,
                &downstream_key,
                DepOutcome::Succeeded,
                &event.event_id,
            );
        }
    }
}

fn fold_dep_satisfied(&mut self, event: &DepSatisfiedPayload) {
    let edge_key = (&event.upstream_task_key, &event.downstream_task_key);

    // Check if edge was already satisfied
    let was_satisfied = self.dep_satisfaction
        .get(edge_key)
        .map(|e| e.satisfied)
        .unwrap_or(false);

    // Upsert edge
    self.dep_satisfaction.insert(edge_key, DepSatisfactionRow {
        satisfied: true,
        row_version: event.event_id.clone(),
        ..
    });

    // Only increment if newly satisfied
    if !was_satisfied {
        let downstream = self.tasks.get_mut(&event.downstream_task_key).unwrap();
        downstream.deps_satisfied_count += 1;

        // Check readiness
        if downstream.deps_satisfied_count == downstream.deps_total
            && downstream.state == TaskState::Blocked
        {
            downstream.state = TaskState::Ready;
            self.emit_dispatch_request(&event.downstream_task_key);
        }
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement duplicate-safe dependency satisfaction"
```

---

### Epic 6: Timer System

**Goal:** Durable timers for retry, heartbeat, cron.

**Files:**
- Create: `crates/arco-flow/src/orchestration/controllers/timer.rs`
- Create: `crates/arco-flow/src/orchestration/controllers/timer_handlers.rs`
- Test: `crates/arco-flow/tests/orchestration/timer_tests.rs`

---

**Task 6.1: Timer Controller**

**Step 1: Write failing test for timer creation**

```rust
#[test]
fn test_timer_id_includes_epoch_for_uniqueness() {
    let timer1 = RetryTimer::new("run1", "task1", 1, Utc::now());
    let timer2 = RetryTimer::new("run1", "task1", 1, Utc::now() + Duration::seconds(60));

    // Different fire times = different timer IDs
    assert_ne!(timer1.timer_id, timer2.timer_id);

    // Timer ID format: timer:retry:{run_id}:{task_key}:{attempt}:{epoch}
    assert!(timer1.timer_id.starts_with("timer:retry:run1:task1:1:"));
}
```

**Step 2: Implement timer ID generation**

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add timer controller with deterministic IDs"
```

---

**Task 6.2: Heartbeat Freshness Guard**

**Step 1: Write failing test for compaction lag protection**

```rust
#[test]
async fn test_heartbeat_check_guards_against_compaction_lag() {
    let manifest = Manifest {
        watermarks: Watermarks {
            last_processed_at: Utc::now() - Duration::seconds(45), // 45s lag
        },
        ..Default::default()
    };

    let task = TaskRow {
        state: TaskState::Running,
        last_heartbeat_at: Some(Utc::now() - Duration::seconds(30)),
        heartbeat_timeout_sec: 60,
        ..Default::default()
    };

    let handler = HeartbeatHandler::new(MAX_COMPACTION_LAG_SECS);
    let action = handler.check(&manifest, &task).await;

    // Should reschedule, not fail (compaction lag too high)
    assert!(matches!(action, HeartbeatAction::Reschedule { .. }));
}

#[test]
async fn test_heartbeat_check_fails_when_compaction_fresh() {
    let manifest = Manifest {
        watermarks: Watermarks {
            last_processed_at: Utc::now() - Duration::seconds(5), // 5s lag (fresh)
        },
        ..Default::default()
    };

    let task = TaskRow {
        state: TaskState::Running,
        last_heartbeat_at: Some(Utc::now() - Duration::seconds(120)), // 120s since heartbeat
        heartbeat_timeout_sec: 60, // 60s timeout
        ..Default::default()
    };

    let handler = HeartbeatHandler::new(MAX_COMPACTION_LAG_SECS);
    let action = handler.check(&manifest, &task).await;

    // Should fail (compaction is fresh, task is genuinely stale)
    assert!(matches!(action, HeartbeatAction::FailTask { .. }));
}
```

**Step 2: Implement heartbeat handler with freshness guard**

```rust
pub struct HeartbeatHandler {
    max_compaction_lag: Duration,
}

impl HeartbeatHandler {
    pub async fn check(&self, manifest: &Manifest, task: &TaskRow) -> HeartbeatAction {
        let compaction_lag = Utc::now() - manifest.watermarks.last_processed_at;

        // Guard: If compaction is lagging, don't make timeout decisions
        if compaction_lag > self.max_compaction_lag {
            return HeartbeatAction::Reschedule {
                delay: Duration::seconds(10),
                reason: "compaction_lag_guard",
            };
        }

        // Check actual heartbeat freshness
        let Some(last_heartbeat) = task.last_heartbeat_at else {
            return HeartbeatAction::Reschedule { delay: Duration::seconds(30), reason: "no_heartbeat_yet" };
        };

        let heartbeat_age = Utc::now() - last_heartbeat;
        let timeout = Duration::seconds(task.heartbeat_timeout_sec as i64);
        let grace = Duration::seconds(30);

        if heartbeat_age > timeout + grace {
            HeartbeatAction::FailTask {
                reason: format!("heartbeat_timeout: {}s since last heartbeat", heartbeat_age.num_seconds()),
            }
        } else {
            HeartbeatAction::Reschedule {
                delay: timeout - heartbeat_age + grace,
                reason: "task_healthy",
            }
        }
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add heartbeat handler with compaction lag guard"
```

---

### Epic 7: Anti-Entropy + Production Hardening

**Goal:** System self-heals from failures.

**Files:**
- Create: `crates/arco-flow/src/orchestration/controllers/anti_entropy.rs`
- Test: `crates/arco-flow/tests/orchestration/anti_entropy_tests.rs`

---

**Task 7.1: Anti-Entropy Sweeper**

**Step 1: Write failing test for stuck task recovery**

```rust
#[test]
async fn test_anti_entropy_recovers_stuck_ready_tasks() {
    let tasks = vec![
        TaskRow {
            task_key: "extract".into(),
            state: TaskState::Ready,
            ready_at: Some(Utc::now() - Duration::minutes(10)), // Stuck for 10 min
            ..Default::default()
        },
    ];

    let outbox = vec![]; // No dispatch outbox entry

    let sweeper = AntiEntropySweeper::new(Duration::minutes(5));
    let repairs = sweeper.scan(&tasks, &outbox).await;

    assert_eq!(repairs.len(), 1);
    assert!(matches!(repairs[0], Repair::CreateDispatchOutbox { task_key: "extract" }));
}
```

**Step 2: Implement anti-entropy sweeper**

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add anti-entropy sweeper"
```

---

## Correctness Test Matrix

These tests MUST pass before production deployment:

### INV-1: Order Independence
```rust
#[test]
fn test_order_independence() {
    let events = vec![task_finished("A"), task_finished("B"), task_finished("C")];

    // Process in different orders
    let state1 = fold_events(events.clone());
    let state2 = fold_events(events.into_iter().rev().collect());

    // Final state must be identical
    assert_eq!(state1, state2);
}
```

### INV-2: Duplicate Safety
```rust
#[test]
fn test_duplicate_event_is_noop() {
    let event = task_finished("A");

    let mut state = FoldState::new();
    state.fold_event(&event);
    let state_after_first = state.clone();

    state.fold_event(&event); // Duplicate

    assert_eq!(state, state_after_first);
}
```

### INV-3: Controller Determinism
```rust
#[test]
fn test_controller_determinism() {
    let parquet_state = load_test_state();

    let events1 = dispatcher.reconcile(&parquet_state).await;
    let events2 = dispatcher.reconcile(&parquet_state).await;

    // Same idempotency keys
    assert_eq!(
        events1.iter().map(|e| &e.idempotency_key).collect::<Vec<_>>(),
        events2.iter().map(|e| &e.idempotency_key).collect::<Vec<_>>(),
    );
}
```

### INV-4: Side Effect Idempotency
```rust
#[test]
async fn test_cloud_tasks_idempotency() {
    let dispatcher = CloudTasksDispatcher::new(config).await.unwrap();

    let envelope = test_envelope("task1", 1);
    let result1 = dispatcher.enqueue(envelope.clone(), Default::default()).await;
    let result2 = dispatcher.enqueue(envelope, Default::default()).await;

    // First should enqueue, second should deduplicate
    assert!(matches!(result1, Ok(EnqueueResult::Enqueued { .. })));
    assert!(matches!(result2, Ok(EnqueueResult::Deduplicated { .. })));
}
```

### INV-5: No Lost Work
```rust
#[test]
async fn test_anti_entropy_recovers_orphaned_tasks() {
    // Simulate Pub/Sub message loss
    let mut state = test_state_with_ready_task("task1");
    state.dispatch_outbox.clear(); // Simulate lost dispatch

    let sweeper = AntiEntropySweeper::new(Duration::minutes(5));
    let repairs = sweeper.scan(&state).await;

    // Should create repair action
    assert!(!repairs.is_empty());
}
```

### INV-6: Dependency Correctness
```rust
#[test]
fn test_task_cannot_run_before_dependencies() {
    let mut state = test_state_with_dag("A -> B -> C");

    // Try to dispatch B before A completes
    let b_ready = state.tasks["B"].state == TaskState::Ready;
    assert!(!b_ready);

    // Complete A
    state.fold_event(&task_finished("A", TaskOutcome::Succeeded));

    // Now B should be ready
    assert_eq!(state.tasks["B"].state, TaskState::Ready);

    // But C is still blocked
    assert_eq!(state.tasks["C"].state, TaskState::Blocked);
}
```

### INV-7: Parquet-Only Reads
```rust
#[test]
fn test_controller_never_reads_ledger() {
    let controller = DispatcherController::new(storage.clone());

    // Controller should only call manifest/parquet methods
    controller.reconcile().await;

    // Verify no ledger reads
    assert_eq!(storage.ledger_read_count(), 0);
    assert!(storage.parquet_read_count() > 0);
}
```

### INV-8: Stale Attempt Event (NEW - CRITICAL)

Tests that out-of-order attempt events don't cause state regression.

```rust
#[test]
fn test_stale_attempt_event_is_rejected() {
    let mut state = test_state_with_task("extract");

    // Start attempt 1
    let attempt_1_id = Ulid::new().to_string();
    state.fold_event(&task_started("extract", 1, &attempt_1_id));
    assert_eq!(state.tasks["extract"].attempt, 1);
    assert_eq!(state.tasks["extract"].attempt_id, attempt_1_id);

    // Start attempt 2 (retry)
    let attempt_2_id = Ulid::new().to_string();
    state.fold_event(&task_started("extract", 2, &attempt_2_id));
    assert_eq!(state.tasks["extract"].attempt, 2);
    assert_eq!(state.tasks["extract"].attempt_id, attempt_2_id);
    assert_eq!(state.tasks["extract"].state, TaskState::Running);

    // Late TaskFinished for attempt 1 arrives (out-of-order)
    state.fold_event(&task_finished("extract", 1, &attempt_1_id, TaskOutcome::Failed));

    // State should NOT regress - attempt 2 is still running
    assert_eq!(state.tasks["extract"].attempt, 2);
    assert_eq!(state.tasks["extract"].attempt_id, attempt_2_id);
    assert_eq!(state.tasks["extract"].state, TaskState::Running);  // NOT Failed!

    // No duplicate dispatch should be created
    assert!(state.dispatch_outbox.iter()
        .filter(|d| d.task_key == "extract" && d.attempt == 3)
        .count() == 0);
}
```

### INV-9: Retry Timer Lag (NEW - CRITICAL)

Tests that retry timers respect watermark freshness like heartbeat timers.

```rust
#[test]
async fn test_retry_timer_respects_watermark_freshness() {
    let manifest = Manifest {
        watermarks: Watermarks {
            last_processed_at: Utc::now() - Duration::seconds(45), // 45s lag
        },
        ..Default::default()
    };

    let task = TaskRow {
        task_key: "extract".into(),
        state: TaskState::RetryWait,
        attempt: 1,
        retry_not_before: Some(Utc::now() - Duration::seconds(10)), // Due 10s ago
        ..Default::default()
    };

    let handler = RetryTimerHandler::new(MAX_COMPACTION_LAG_SECS);
    let action = handler.fire(&manifest, &task).await;

    // Should reschedule, NOT increment attempt (compaction lag too high)
    // If we incremented attempt, we might miss events in the lag window
    assert!(matches!(action, RetryAction::Reschedule { .. }));
    assert!(!matches!(action, RetryAction::IncrementAttempt { .. }));
}

#[test]
async fn test_retry_timer_increments_when_compaction_fresh() {
    let manifest = Manifest {
        watermarks: Watermarks {
            last_processed_at: Utc::now() - Duration::seconds(5), // Fresh
        },
        ..Default::default()
    };

    let task = TaskRow {
        task_key: "extract".into(),
        state: TaskState::RetryWait,
        attempt: 1,
        retry_not_before: Some(Utc::now() - Duration::seconds(10)), // Due 10s ago
        ..Default::default()
    };

    let handler = RetryTimerHandler::new(MAX_COMPACTION_LAG_SECS);
    let action = handler.fire(&manifest, &task).await;

    // Compaction is fresh, safe to increment attempt
    assert!(matches!(action, RetryAction::IncrementAttempt { new_attempt: 2, .. }));
}
```

### INV-10: Failure Propagation

Tests that failure correctly skips downstream tasks.

```rust
#[test]
fn test_upstream_failure_skips_downstream() {
    let mut state = test_state_with_dag("A -> B -> C");

    // A fails terminally (no more retries)
    state.fold_event(&task_finished("A", TaskOutcome::Failed, /*terminal=*/true));

    // A should be FAILED
    assert_eq!(state.tasks["A"].state, TaskState::Failed);

    // B should be SKIPPED (direct downstream)
    assert_eq!(state.tasks["B"].state, TaskState::Skipped);

    // C should also be SKIPPED (transitive)
    assert_eq!(state.tasks["C"].state, TaskState::Skipped);

    // dep_satisfaction edges should have correct resolution
    assert_eq!(state.dep_satisfaction[("A", "B")].resolution, DepResolution::Failed);
    assert_eq!(state.dep_satisfaction[("B", "C")].resolution, DepResolution::Skipped);
}
```

---

## Module Structure (Actual)

```
crates/arco-flow/src/orchestration/
├── mod.rs                    # Domain root
├── events/mod.rs             # Event envelope + all event types
├── ids.rs                    # Cloud Tasks ID + attempt_id generation
├── ledger.rs                 # Ledger writer
├── run_key.rs                # Run key reservation
├── callbacks/
│   ├── mod.rs
│   ├── handlers.rs
│   └── types.rs
├── compactor/
│   ├── mod.rs
│   ├── fold.rs               # Schemas + fold logic (combined)
│   ├── manifest.rs           # Manifest schema
│   ├── parquet_util.rs       # Parquet helpers
│   └── service.rs            # Compaction service
└── controllers/
    ├── mod.rs
    ├── dispatch.rs           # ID helpers
    ├── dispatcher.rs         # Dispatch controller
    ├── ready_dispatch.rs     # Ready task dispatch
    ├── timer.rs              # Timer controller
    ├── timer_handlers.rs     # Heartbeat + retry handlers
    └── anti_entropy.rs       # Anti-entropy sweeper

crates/arco-flow/src/bin/
├── servo_compactor.rs
├── servo_dispatcher.rs
└── servo_sweeper.rs
```

---

## Summary

This execution plan implements the Servo event-driven orchestration architecture in 7 epics:

| Epic | Goal | Key Deliverables |
|------|------|------------------|
| 0 | Lock decisions | ADR-020/021/022, supersession notice |
| 1 | Event foundation | Event envelope, run/task/timer events |
| 2 | Compaction | Micro-compactor, L0 deltas, manifest CAS |
| 3 | Schemas | runs, tasks, dep_satisfaction, timers, outbox |
| 4 | Dispatch | Dispatcher controller, hash-based Cloud Tasks IDs |
| 5 | Dependencies | Per-edge satisfaction, duplicate-safe readiness |
| 6 | Timers | Retry, heartbeat, cron; freshness guard |
| 7 | Hardening | Anti-entropy sweeper, correctness tests |

**Critical Corrections Applied (10 total):**

1. **Cloud Tasks naming** - Dual-identifier pattern (internal_id + cloud_task_id hash) per ADR-021
2. **Deduplication window** - Corrected to 24 hours (not 9 days, which applies to legacy queues)
3. **Per-edge dep_satisfaction** - Duplicate-safe readiness per ADR-022
4. **Derived events** - Explicitly projection-only (NOT ledger events)
5. **attempt_id guard** - Concurrency token to prevent state regression from stale events
6. **Watermark freshness for ALL timers** - Retry, heartbeat, and stuck-task repairs
7. **Failure/skip propagation** - Explicit edge resolution semantics (SUCCESS/FAILED/SKIPPED/CANCELLED)
8. **Controller event categories** - Only Intent + Acknowledgement facts to ledger
9. **Micro-compactor trigger path** - GCS finalize (primary) vs listing (fallback)
10. **Quota/fairness** - Explicit v1 decision: global cap only, per-tenant deferred

**New Correctness Tests:**

- INV-8: STALE_ATTEMPT_EVENT - Ensures out-of-order attempt events don't regress state
- INV-9: RETRY_TIMER_LAG - Ensures retry timers respect watermark freshness
- INV-10: FAILURE_PROPAGATION - Ensures upstream failure correctly skips downstream

**Schema Updates:**

- `dispatch_outbox.parquet` - Added `cloud_task_id` column (dual-identifier)
- `timers.parquet` - Added `cloud_task_id` column (dual-identifier)
- `tasks.parquet` - Added `attempt_id` (concurrency guard) and `deps_satisfied_count` (derived)
- `dep_satisfaction.parquet` - Added `resolution` column (SUCCESS/FAILED/SKIPPED/CANCELLED)

**Alignment with Platform:**

- All 6 unified platform invariants preserved
- Two-tier consistency model respected
- IAM-enforced sole writer maintained
- Existing patterns reused (compactor, CAS publish, Cloud Tasks dispatcher)
