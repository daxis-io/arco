# Layer 2: Dagster Parity Automation Execution Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement Layer 2 automation features (schedules, sensors, backfills, partition status) on top of the completed Layer 1 execution engine, achieving Milestone M2 Dagster parity.

**Architecture:** Three new controllers (Schedule, Sensor, Backfill) emit events to the same ledger. The micro-compactor folds these into 5 new Parquet projections. Hybrid automation approach: Cloud Tasks timers for schedules, Pub/Sub for push sensors, polling timers for pull sensors. Run-per-chunk backfill model with deterministic run_id derived from run_key.

**Tech Stack:** Rust 1.85+, Parquet/Arrow, GCS, Cloud Tasks, Cloud Run, Pub/Sub, DuckDB (reads)

**Prerequisites:** Layer 1 complete (runs, tasks, retries, dispatch, timers, anti-entropy)

---

## Reference Documents

- [2025-12-19-servo-event-driven-orchestration-execution-plan.md](2025-12-19-servo-event-driven-orchestration-execution-plan.md) - Layer 1 (complete)
- [2025-12-19-servo-arco-vs-dagster-parity-audit.md](2025-12-19-servo-arco-vs-dagster-parity-audit.md) - Full parity audit
- [servo-dagster-exception-planning.md](../audits/servo-dagster-exception-planning.md) - Deliberate exceptions
- **[2025-12-22-layer2-p0-corrections.md](2025-12-22-layer2-p0-corrections.md) - P0 corrections (MUST READ)**

---

## Critical Architectural Constraints

> **⚠️ P0 CORRECTIONS:** See [P0 corrections document](2025-12-22-layer2-p0-corrections.md) for detailed fixes.

| Constraint | Requirement |
|------------|-------------|
| **C1: Pure Projection** | Compactor MUST NOT emit events. Controllers emit all events atomically. |
| **C2: Stable IDs** | All entity IDs are ULIDs, not names. `schedule_id: "01HQ123..."` |
| **C3: HMAC Run IDs** | Run IDs namespaced by tenant + HMAC(secret, run_key) |
| **C4: Cursor CAS** | Poll sensor cursor updates use compare-and-swap semantics |
| **C5: Separate Status** | `last_materialization_*` (success only) vs `last_attempt_*` (every attempt) |
| **C6: Compact Payloads** | BackfillCreated uses range/selector, not full partition list |

---

## Design Decisions Summary

### Architecture Decisions (from brainstorming)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Automation model | Hybrid: Cloud Tasks + Pub/Sub + polling | Best tool for each trigger type |
| Backfill model | Run-per-chunk | Granular pause/resume, bounded blast radius |
| Partition status source | Explicit `PartitionMaterialized` events | Decoupled from task lifecycle |
| Run identity | Deterministic from run_key hash | Replay-safe, no server-generated IDs |
| Conflict detection | Fingerprint in payload, not run_key | Stable run_key enables proper dedup |
| Fresh reads | Snapshot + bounded L0 tail | Configurable recency without full replay |
| Schedule tick payload | Snapshot definition fields into `ScheduleTicked` | Deterministic replays after definition edits |
| RunRequested idempotency | Include fingerprint hash in idempotency key | Conflicts surface while still deduping identical requests |
| Backfill partition payload | Range/spec in event, resolve partitions per chunk | Avoid oversized events and support non-daily partitions |
| Poll sensor serialization | Optimistic state_version gating | Prevent duplicate effects under concurrent polls |
| Schedule time semantics | Explicit timezone + catchup window | DST-safe evaluation and predictable catchup |

### Idempotency Key Formulas

| Event | Idempotency Key | Run Key |
|-------|-----------------|---------|
| ScheduleTicked | `sched_tick:{tick_id}` | `sched:{schedule_id}:{epoch}` |
| SensorEvaluated (push) | `sensor_eval:{sensor_id}:msg:{message_id}` | `sensor:{sensor_id}:msg:{message_id}` |
| SensorEvaluated (poll) | `sensor_eval:{sensor_id}:poll:{poll_epoch}:{cursor_before_hash}` | per-request from sensor |
| BackfillChunkPlanned | `backfill_chunk:{backfill_id}:{chunk_index}` | `backfill:{backfill_id}:chunk:{chunk_index}` |
| BackfillStateChanged | `backfill_state:{backfill_id}:{state_version}` | N/A |
| RunRequested | `runreq:{run_key}:{fingerprint_hash}` | stable per trigger source |
| PartitionMaterialized (projection-only) | N/A | N/A |

Notes:
- `fingerprint_hash` = `sha256(request_fingerprint)` (full 256-bit hash, no truncation)
- `PartitionMaterialized` is projection-only (not a ledger event); idempotency is enforced by fold/update semantics.
- Idempotency keys stored with 7-day TTL; full hash storage cost is negligible vs collision risk.

### Deterministic Run ID Generation

> **⚠️ Updated per C3:** Uses HMAC with tenant namespace.

```rust
use hmac::{Hmac, Mac};
use sha2::Sha256;
type HmacSha256 = Hmac<Sha256>;

/// Generate deterministic run_id from run_key (replay-safe, tenant-scoped).
pub fn run_id_from_run_key(
    tenant_id: &str,
    workspace_id: &str,
    run_key: &str,
    tenant_secret: &[u8],
) -> String {
    let input = format!("{}:{}:{}", tenant_id, workspace_id, run_key);
    let mut mac = HmacSha256::new_from_slice(tenant_secret).unwrap();
    mac.update(input.as_bytes());
    let hash = mac.finalize().into_bytes();
    let encoded = base32::encode(base32::Alphabet::RFC4648 { padding: false }, &hash[..16]);
    format!("run_{}", &encoded[..26].to_lowercase())
}
```

---

## Epic Structure

### Epic 0: Architecture Decision Records

**Goal:** Lock Layer 2 design decisions before implementation.

**Files:**
- Create: `docs/adr/adr-024-schedule-sensor-automation.md`
- Create: `docs/adr/adr-025-backfill-controller.md`
- Create: `docs/adr/adr-026-partition-status-tracking.md`

---

**Task 0.1: ADR-024 Schedule/Sensor Automation**

**Step 1: Write ADR documenting automation approach**

```markdown
# ADR-024: Schedule and Sensor Automation

## Status
Accepted

## Context
Layer 2 requires automation triggers: schedules (cron) and sensors (event-driven).
Dagster uses a long-running daemon; we need a serverless alternative.

## Decision
**Hybrid approach:**

1. **Schedules**: Cloud Tasks timers fire at cron intervals
   - Timer emits `ScheduleTicked` event
   - Controller evaluates tick and emits `RunRequested` if due
   - Tick history persisted in `schedule_ticks.parquet`

2. **Push Sensors**: Pub/Sub subscription triggers evaluation
   - Message arrives → sensor evaluates → emits `SensorEvaluated`
   - Message ID is the idempotency key (exactly-once processing)
   - Cursorless by default (message is the cursor)

3. **Poll Sensors**: Cloud Tasks polling timer
   - Timer fires → sensor evaluates with cursor → emits `SensorEvaluated`
   - Cursor persisted in `sensor_state.parquet`
   - Serialized evaluation (one at a time per sensor)

### Tick History Pattern
Every schedule tick creates a `schedule_ticks` row with:
- `tick_id`: `{schedule_id}:{scheduled_for_epoch}`
- `run_key`: `sched:{schedule_id}:{epoch}`
- `run_id`: Filled by compactor when `RunRequested` correlates
- `definition_version`: schedule definition row_version used at tick time
- `asset_selection`: snapshot used to build the run
- `partition_selection`: snapshot used to build the run (optional)

### Catch-up Behavior
On scheduler startup after downtime:
1. Query `last_scheduled_for` from `schedule_state.parquet`
2. Enumerate missed ticks up to `max_catchup_ticks` and within `catchup_window_minutes`
3. Emit `ScheduleTicked` for each, which creates runs

## Consequences
- No long-running daemon to operate
- Latency bounded by Cloud Tasks/Pub/Sub delivery
- Cursor durability via Parquet (not in-memory)
- Tick history enables "what happened" debugging
```

**Step 2: Commit**

```bash
git add docs/adr/adr-024-schedule-sensor-automation.md
git commit -m "docs(adr): ADR-024 schedule and sensor automation"
```

---

**Task 0.2: ADR-025 Backfill Controller**

**Step 1: Write ADR documenting backfill model**

```markdown
# ADR-025: Backfill Controller

## Status
Accepted

## Context
Backfills materialize partition ranges. Need: chunking, concurrency control,
pause/resume/cancel, retry-failed.

## Decision
**Run-per-chunk model:**

### Chunk Planning
1. User requests backfill: `BackfillCreated` event
2. Controller divides partitions into chunks of `chunk_size`
3. Each chunk → `BackfillChunkPlanned` event with:
   - `chunk_id`: `{backfill_id}:{chunk_index}`
   - `run_key`: `backfill:{backfill_id}:chunk:{chunk_index}`
   - `partition_keys`: List of partitions in this chunk
4. Each chunk run_key → `RunRequested` event

### Concurrency Control
- `max_concurrent_runs`: Maximum parallel chunk runs
- Controller counts active chunks, emits next when below limit

### State Machine
```
PENDING → RUNNING → SUCCEEDED
              ↓
           PAUSED → RUNNING
              ↓
           FAILED
              ↓
           CANCELLED
```

### Pause/Resume
- **Pause**: Stop emitting new `BackfillChunkPlanned`, let active chunks finish
- **Resume**: Continue from next unplanned chunk

### Retry-Failed
- Create NEW backfill with `parent_backfill_id`
- Only include failed partitions from parent
- `retry_request_id` for idempotent retry creation

### State Version
Each state transition increments `state_version` for idempotent transitions:
```rust
BackfillStateChanged {
    backfill_id: String,
    from_state: BackfillState,
    to_state: BackfillState,
    state_version: u32, // Monotonic
}
// idempotency_key = backfill_state:{backfill_id}:{state_version}
```

## Consequences
- Granular progress tracking (per-chunk)
- Bounded blast radius (chunk fails, not whole backfill)
- Clean pause/resume semantics
- Retry-failed creates new backfill (no in-place mutation)
```

**Step 2: Commit**

```bash
git add docs/adr/adr-025-backfill-controller.md
git commit -m "docs(adr): ADR-025 backfill controller"
```

---

**Task 0.3: ADR-026 Partition Status Tracking**

> **NOTE:** ADR-026 already exists at `docs/adr/adr-026-partition-status-tracking.md` with the correct P0-5 separation of concerns. This task verifies the ADR is complete and consistent.

**Step 1: Verify existing ADR-026 is complete**

The ADR must include:
- Separation of `last_materialization_*` (success only) vs `last_attempt_*` (every attempt)
- `last_materialization_code_version` for CODE_CHANGED staleness
- Update logic: `record_attempt()` always, `mark_materialized()` only on success
- Computed display status function

```bash
# Verify ADR exists and has key sections
grep -q "last_materialization_code_version" docs/adr/adr-026-partition-status-tracking.md && \
grep -q "record_attempt" docs/adr/adr-026-partition-status-tracking.md && \
grep -q "mark_materialized" docs/adr/adr-026-partition-status-tracking.md && \
echo "ADR-026 verified" || echo "ADR-026 needs updates"
```

**Reference Schema (from ADR-026):**

| Column | Type | Description |
|--------|------|-------------|
| tenant_id | STRING | Tenant identifier |
| workspace_id | STRING | Workspace identifier |
| asset_key | STRING | Asset identifier |
| partition_key | STRING | Partition identifier |
| last_materialization_run_id | STRING | Run that last materialized (success only) |
| last_materialization_at | TIMESTAMP | When last materialized (success only) |
| last_materialization_code_version | STRING | Code version used (success only) |
| last_attempt_run_id | STRING | Most recent attempt (any outcome) |
| last_attempt_at | TIMESTAMP | When last attempted (any outcome) |
| last_attempt_outcome | TaskOutcome | SUCCEEDED/FAILED/CANCELLED (SKIPPED permitted) |
| stale_since | TIMESTAMP | When became stale (nullable; derived or precomputed) |
| stale_reason_code | STRING | FRESHNESS_POLICY/UPSTREAM_CHANGED/CODE_CHANGED (derived or precomputed) |
| partition_values | MAP<STRING,STRING> | Dimension key-values |
| row_version | STRING | ULID of last update |

### Staleness Computation
**Computed at query time** (not during fold):
1. Check freshness policy deadline
2. Check if upstream partitions materialized after this one
3. Check if code version changed

Requires `last_materialization_code_version` from materialization metadata and `current_code_version` from asset definitions.

Optional: Periodic staleness sweep controller for pre-computation.

### Decoupling from Tasks
- Task state: "execution attempt"
- Partition status: "data freshness"
- A task can fail but partition remains MATERIALIZED from prior run

## Consequences
- Clean separation of execution vs data status
- Query-time staleness enables dynamic policies
- Explicit events provide audit trail
- Additional storage for materialization history
```

**Step 2: Commit**

```bash
git add docs/adr/adr-026-partition-status-tracking.md
git commit -m "docs(adr): ADR-026 partition status tracking"
```

---

### Epic 1: Event Types + Schema Extensions

**Goal:** Add Layer 2 events and Parquet schemas.

**Files:**
- Modify: `crates/arco-flow/src/orchestration/events/mod.rs`
- Create: `crates/arco-flow/src/orchestration/events/automation_events.rs`
- Create: `crates/arco-flow/src/orchestration/events/backfill_events.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Test: `crates/arco-flow/tests/orchestration_automation_tests.rs`

---

**Task 1.1: Schedule Event Types**

**Step 1: Write failing test for ScheduleTicked event**

```rust
// crates/arco-flow/tests/orchestration_automation_tests.rs
#[test]
fn test_schedule_ticked_event_idempotency_key() {
    use chrono::{TimeZone, Utc};

    let scheduled_for = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleTicked {
            schedule_id: "daily-etl".into(),
            scheduled_for,
            tick_id: "daily-etl:1736935200".into(),
            definition_version: "def_01HQ123".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: None,
            status: TickStatus::Triggered,
            run_key: Some("sched:daily-etl:1736935200".into()),
            request_fingerprint: Some("abc123".into()),
        },
    );

    assert_eq!(event.event_type, "ScheduleTicked");
    // Idempotency key uses tick_id
    assert_eq!(event.idempotency_key, "sched_tick:daily-etl:1736935200");
}

#[test]
fn test_schedule_ticked_skipped_has_no_run_key() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleTicked {
            schedule_id: "daily-etl".into(),
            scheduled_for: Utc::now(),
            tick_id: "daily-etl:1736935200".into(),
            definition_version: "def_01HQ123".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: None,
            status: TickStatus::Skipped { reason: "paused".into() },
            run_key: None,
            request_fingerprint: None,
        },
    );

    assert!(matches!(
        &event.data,
        OrchestrationEventData::ScheduleTicked { run_key: None, .. }
    ));
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test -p arco-flow test_schedule_ticked_event
```

Expected: FAIL with "ScheduleTicked not found"

**Step 3: Implement schedule event types**

```rust
// crates/arco-flow/src/orchestration/events/automation_events.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Status of a schedule tick evaluation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TickStatus {
    /// Tick triggered a run.
    Triggered,
    /// Tick was skipped (e.g., paused, condition not met).
    Skipped { reason: String },
    /// Tick evaluation failed.
    Failed { error: String },
}

/// Trigger source for sensor evaluations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum TriggerSource {
    /// Push sensor triggered by Pub/Sub message.
    Push { message_id: String },
    /// Poll sensor triggered by timer.
    Poll { poll_epoch: i64 },
}

// Add to OrchestrationEventData enum:
// ScheduleTicked {
//     schedule_id: String,
//     scheduled_for: DateTime<Utc>,
//     tick_id: String,
//     definition_version: String,
//     asset_selection: Vec<String>,
//     partition_selection: Option<Vec<String>>,
//     status: TickStatus,
//     run_key: Option<String>,
//     request_fingerprint: Option<String>,
// },
```

Idempotency for ScheduleTicked:
```rust
format!("sched_tick:{}", tick_id)
```

**Step 4: Run test to verify it passes**

```bash
cargo test -p arco-flow test_schedule_ticked_event
```

**Step 5: Commit**

```bash
git add crates/arco-flow/src/orchestration/events/
git commit -m "feat(orchestration): add schedule tick event types"
```

---

**Task 1.2: Sensor Event Types**

**Step 1: Write failing test for SensorEvaluated event**

```rust
#[test]
fn test_sensor_evaluated_push_idempotency() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: "new-files-sensor".into(),
            eval_id: "eval_01HQ123".into(),
            cursor_before: None,
            cursor_after: Some("file://bucket/path/file.parquet".into()),
            expected_state_version: None,
            trigger_source: TriggerSource::Push {
                message_id: "msg_abc123".into(),
            },
            run_requests: vec![
                RunRequest {
                    run_key: "sensor:new-files-sensor:msg:msg_abc123".into(),
                    request_fingerprint: "fp1".into(),
                    asset_selection: vec!["raw.events".into()],
                    partition_selection: None,
                },
            ],
            status: SensorEvalStatus::Triggered,
        },
    );

    // Push sensor idempotency based on message_id
    assert!(event.idempotency_key.contains("msg:msg_abc123"));
}

#[test]
fn test_sensor_evaluated_poll_idempotency_uses_cursor_before() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: "poll-sensor".into(),
            eval_id: "eval_01HQ456".into(),
            cursor_before: Some("cursor_v1".into()),
            cursor_after: Some("cursor_v2".into()),
            expected_state_version: Some(7),
            trigger_source: TriggerSource::Poll { poll_epoch: 1736935200 },
            run_requests: vec![],
            status: SensorEvalStatus::NoNewData,
        },
    );

    // Poll sensor idempotency based on cursor_before (input), not cursor_after
    let cursor_hash = &sha256_hex("cursor_v1");
    assert!(event.idempotency_key.contains(cursor_hash));
}
```

Note: Poll sensors set `expected_state_version` from `SensorStateRow.state_version`; fold drops mismatches.

**Step 2: Run test, verify fail, implement, verify pass, commit**

```bash
git commit -m "feat(orchestration): add sensor evaluation event types"
```

---

**Task 1.3: Backfill Event Types**

**Step 1: Write failing test for BackfillChunkPlanned**

```rust
#[test]
fn test_backfill_chunk_planned_idempotency() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::BackfillChunkPlanned {
            backfill_id: "bf_01HQ123".into(),
            chunk_id: "bf_01HQ123:0".into(),
            chunk_index: 0,
            partition_keys: vec!["2025-01-01".into(), "2025-01-02".into()],
            run_key: "backfill:bf_01HQ123:chunk:0".into(),
            request_fingerprint: "fp_chunk0".into(),
        },
    );

    assert_eq!(event.event_type, "BackfillChunkPlanned");
    assert!(event.idempotency_key.contains("backfill_chunk:bf_01HQ123:0"));
}

#[test]
fn test_backfill_state_changed_uses_state_version() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::BackfillStateChanged {
            backfill_id: "bf_01HQ123".into(),
            from_state: BackfillState::Running,
            to_state: BackfillState::Paused,
            state_version: 3,
            changed_by: Some("user@example.com".into()),
        },
    );

    // Idempotency uses state_version (monotonic)
    assert!(event.idempotency_key.contains("backfill_state:bf_01HQ123:3"));
}
```

**Step 2: Implement backfill event types**

```rust
// crates/arco-flow/src/orchestration/events/backfill_events.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BackfillState {
    Pending,
    Running,
    Paused,
    Succeeded,
    Failed,
    Cancelled,
}

// Add to OrchestrationEventData:
// BackfillCreated {
//     backfill_id: String,
//     client_request_id: String,
//     asset_selection: Vec<String>,
//     partition_selector: PartitionSelector,
//     total_partitions: u32,
//     chunk_size: u32,
//     max_concurrent_runs: u32,
//     parent_backfill_id: Option<String>,
// }
// BackfillChunkPlanned { ... }
// BackfillStateChanged { ... }
```

Note: Thread `code_version` through `TaskFinished` from worker metadata (e.g., git SHA); if absent, leave `None`.

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add backfill event types"
```

---

**Task 1.4: RunRequested Event with fingerprinted Idempotency**

**Step 1: Write failing test for RunRequested conflict detection**

```rust
#[test]
fn test_run_requested_stable_run_key_fingerprint_in_payload() {
    let event1 = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: "sched:daily-etl:1736935200".into(),
            request_fingerprint: "fingerprint_v1".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: None,
            trigger_source_ref: SourceRef::Schedule {
                schedule_id: "daily-etl".into(),
                tick_id: "daily-etl:1736935200".into(),
            },
            labels: HashMap::new(),
        },
    );

    // run_key is stable (no fingerprint in it)
    assert!(!event1.data.run_key().contains("fingerprint"));

    // Fingerprint is in payload for conflict detection
    assert_eq!(event1.data.request_fingerprint(), Some("fingerprint_v1"));
}

#[test]
fn test_run_requested_idempotency_includes_fingerprint_hash() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: "sched:daily-etl:1736935200".into(),
            request_fingerprint: "fingerprint_v1".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: None,
            trigger_source_ref: SourceRef::Schedule {
                schedule_id: "daily-etl".into(),
                tick_id: "daily-etl:1736935200".into(),
            },
            labels: HashMap::new(),
        },
    );

    let fp_hash = sha256_hex("fingerprint_v1");
    assert!(event.idempotency_key.starts_with("runreq:sched:daily-etl:1736935200:"));
    assert!(event.idempotency_key.contains(&fp_hash));
}

#[test]
fn test_run_id_deterministic_from_run_key() {
    let run_key = "sched:daily-etl:1736935200";

    let run_id_1 = run_id_from_run_key(run_key);
    let run_id_2 = run_id_from_run_key(run_key);

    // Same run_key always produces same run_id
    assert_eq!(run_id_1, run_id_2);
    assert!(run_id_1.starts_with("run_"));
}
```

**Step 2: Implement RunRequested with SourceRef**

```rust
/// Reference to the source that triggered a run request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceRef {
    /// Triggered by a schedule tick.
    Schedule {
        schedule_id: String,
        tick_id: String,
    },
    /// Triggered by a sensor evaluation.
    Sensor {
        sensor_id: String,
        eval_id: String,
    },
    /// Triggered by a backfill chunk.
    Backfill {
        backfill_id: String,
        chunk_id: String,
    },
    /// Manual trigger.
    Manual {
        user_id: String,
        request_id: String,
    },
}
```

Also update `OrchestrationEvent::idempotency_key()` for `RunRequested`:
```rust
// runreq:{run_key}:{fingerprint_hash}
let fp_hash = sha256_hex(request_fingerprint);
format!("runreq:{}:{}", run_key, fp_hash)
```

Note: manual trigger should load the current `ScheduleDefinitionRow` to embed the snapshot; return 404 if missing.

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add RunRequested with source reference"
```

---

**Task 1.5: Parquet Schemas for Layer 2**

**Step 1: Write failing test for schedule_definitions schema**

```rust
#[test]
fn test_schedule_definition_row_schema() {
    let row = ScheduleDefinitionRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        schedule_id: "daily-etl".into(),
        cron_expression: "0 10 * * *".into(),
        timezone: "UTC".into(),
        catchup_window_minutes: 60 * 24,
        asset_selection: vec!["analytics.summary".into()],
        max_catchup_ticks: 3,
        enabled: true,
        created_at: Utc::now(),
        row_version: "01HQ123".into(),
    };

    // Primary key
    assert_eq!(row.primary_key(), ("tenant-abc", "workspace-prod", "daily-etl"));
}
```

**Step 2: Implement schema structs**

```rust
// crates/arco-flow/src/orchestration/compactor/fold.rs

/// Schedule definition (configuration, not state).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleDefinitionRow {
    pub tenant_id: String,
    pub workspace_id: String,
    pub schedule_id: String,
    pub cron_expression: String,
    pub timezone: String, // IANA TZ, e.g. "UTC"
    pub catchup_window_minutes: u32,
    pub asset_selection: Vec<String>,
    pub max_catchup_ticks: u32,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub row_version: String,
}

/// Schedule runtime state (separate from definition).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleStateRow {
    pub tenant_id: String,
    pub workspace_id: String,
    pub schedule_id: String,
    pub last_scheduled_for: Option<DateTime<Utc>>,
    pub last_tick_id: Option<String>,
    pub last_run_key: Option<String>,
    pub row_version: String,
}

/// Schedule tick history (one row per tick).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleTickRow {
    pub tenant_id: String,
    pub workspace_id: String,
    pub tick_id: String, // PK: {schedule_id}:{scheduled_for_epoch} or {schedule_id}:manual:{ts}
    pub schedule_id: String,
    pub scheduled_for: DateTime<Utc>,
    pub definition_version: String,
    pub asset_selection: Vec<String>,
    pub partition_selection: Option<Vec<String>>,
    pub status: TickStatus,
    pub run_key: Option<String>,
    pub run_id: Option<String>, // Filled when RunRequested correlates
    pub request_fingerprint: Option<String>,
    pub row_version: String,
}

/// Sensor runtime state (universal for push + poll).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorStateRow {
    pub tenant_id: String,
    pub workspace_id: String,
    pub sensor_id: String,
    pub cursor: Option<String>,
    pub last_evaluation_at: Option<DateTime<Utc>>,
    pub last_eval_id: Option<String>,
    pub status: SensorStatus, // ACTIVE/PAUSED/ERROR
    pub state_version: u32,
    pub row_version: String,
}

/// Selector for backfill partitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartitionSelector {
    Range { start: String, end: String },
    Explicit { partition_keys: Vec<String> },
}

/// Backfill entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillRow {
    pub tenant_id: String,
    pub workspace_id: String,
    pub backfill_id: String,
    pub asset_selection: Vec<String>,
    pub partition_selector: PartitionSelector,
    pub chunk_size: u32,
    pub max_concurrent_runs: u32,
    pub state: BackfillState,
    pub state_version: u32,
    pub total_partitions: u32,
    pub planned_chunks: u32,
    pub completed_chunks: u32,
    pub failed_chunks: u32,
    pub parent_backfill_id: Option<String>, // For retry-failed
    pub created_at: DateTime<Utc>,
    pub row_version: String,
}

/// Backfill chunk tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillChunkRow {
    pub tenant_id: String,
    pub workspace_id: String,
    pub chunk_id: String, // PK: {backfill_id}:{chunk_index}
    pub backfill_id: String,
    pub chunk_index: u32,
    pub partition_keys: Vec<String>,
    pub run_key: String,
    pub run_id: Option<String>, // Filled when run resolves
    pub state: ChunkState, // PENDING/PLANNED/RUNNING/SUCCEEDED/FAILED
    pub row_version: String,
}

/// Partition status tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionStatusRow {
    pub tenant_id: String,
    pub workspace_id: String,
    pub asset_key: String,
    pub partition_key: String,
    pub last_materialization_run_id: Option<String>,
    pub last_materialization_at: Option<DateTime<Utc>>,
    pub last_materialization_code_version: Option<String>,
    pub last_attempt_run_id: Option<String>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub last_attempt_outcome: Option<TaskOutcome>,
    pub stale_since: Option<DateTime<Utc>>,
    pub stale_reason_code: Option<String>,
    pub partition_values: HashMap<String, String>,
    pub row_version: String,
}

/// Run key index for deduplication and conflict detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunKeyIndexRow {
    pub tenant_id: String,
    pub workspace_id: String,
    pub run_key: String,
    pub run_id: String,
    pub request_fingerprint: String,
    pub created_at: DateTime<Utc>,
    pub row_version: String,
}

/// Run key conflicts (when same run_key has different fingerprint).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunKeyConflictRow {
    pub tenant_id: String,
    pub workspace_id: String,
    pub run_key: String,
    pub existing_fingerprint: String,
    pub conflicting_fingerprint: String,
    pub conflicting_event_id: String,
    pub detected_at: DateTime<Utc>,
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add Layer 2 Parquet schemas"
```

---

### Epic 2: Schedule Controller

**Goal:** Implement cron schedule evaluation with tick history.

**Files:**
- Create: `crates/arco-flow/src/orchestration/controllers/schedule.rs`
- Note: schedule evaluation logic is implemented in `schedule.rs` (no separate evaluator module).
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Test: module tests in `crates/arco-flow/src/orchestration/controllers/schedule.rs`

---

**Task 2.1: Schedule Controller Reconcile**

**Step 1: Write failing test for schedule controller**

```rust
#[test]
fn test_schedule_controller_emits_tick_when_due() {
    let now = Utc::now();
    let scheduled_for = now - Duration::seconds(60); // Due 1 min ago

    let definitions = vec![
        ScheduleDefinitionRow {
            schedule_id: "daily-etl".into(),
            cron_expression: "0 10 * * *".into(),
            timezone: "UTC".into(),
            catchup_window_minutes: 60 * 24,
            enabled: true,
            ..Default::default()
        },
    ];

    let state = vec![
        ScheduleStateRow {
            schedule_id: "daily-etl".into(),
            last_scheduled_for: Some(scheduled_for - Duration::hours(24)),
            ..Default::default()
        },
    ];

    let controller = ScheduleController::new();
    let events = controller.reconcile(&definitions, &state, now);

    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0].data,
        OrchestrationEventData::ScheduleTicked { schedule_id, .. }
        if schedule_id == "daily-etl"
    ));
}

#[test]
fn test_schedule_controller_respects_max_catchup() {
    let now = Utc::now();
    // Missed 10 ticks (daily schedule, 10 days behind)
    let last_tick = now - Duration::days(10);

    let definitions = vec![
        ScheduleDefinitionRow {
            schedule_id: "daily-etl".into(),
            cron_expression: "0 10 * * *".into(),
            max_catchup_ticks: 3,
            timezone: "UTC".into(),
            catchup_window_minutes: 60 * 24,
            enabled: true,
            ..Default::default()
        },
    ];

    let state = vec![
        ScheduleStateRow {
            schedule_id: "daily-etl".into(),
            last_scheduled_for: Some(last_tick),
            ..Default::default()
        },
    ];

    let controller = ScheduleController::new();
    let events = controller.reconcile(&definitions, &state, now);

    // Should only emit 3 ticks (max_catchup_ticks)
    assert_eq!(events.len(), 3);
}

#[test]
fn test_schedule_controller_skips_paused_schedule() {
    let definitions = vec![
        ScheduleDefinitionRow {
            schedule_id: "daily-etl".into(),
            timezone: "UTC".into(),
            catchup_window_minutes: 60 * 24,
            enabled: false, // Paused
            ..Default::default()
        },
    ];

    let controller = ScheduleController::new();
    let events = controller.reconcile(&definitions, &[], Utc::now());

    assert!(events.is_empty());
}
```

**Step 2: Implement schedule controller**

```rust
// crates/arco-flow/src/orchestration/controllers/schedule.rs
use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use std::str::FromStr;

pub struct ScheduleController {
    default_catchup_window: Duration,
}

impl ScheduleController {
    pub fn new() -> Self {
        Self {
            default_catchup_window: Duration::hours(24),
        }
    }

    pub fn reconcile(
        &self,
        definitions: &[ScheduleDefinitionRow],
        state: &[ScheduleStateRow],
        now: DateTime<Utc>,
    ) -> Vec<OrchestrationEvent> {
        let state_map: HashMap<_, _> = state
            .iter()
            .map(|s| (&s.schedule_id, s))
            .collect();

        let mut events = Vec::new();

        for def in definitions {
            if !def.enabled {
                continue;
            }

            let schedule = match Schedule::from_str(&def.cron_expression) {
                Ok(s) => s,
                Err(e) => {
                    // Emit error tick
                    events.push(self.error_tick(def, &e.to_string()));
                    continue;
                }
            };

            let tz = match def.timezone.parse::<Tz>() {
                Ok(tz) => tz,
                Err(e) => {
                    events.push(self.error_tick(def, &format!("invalid timezone: {}", e)));
                    continue;
                }
            };

            let last_scheduled_for = state_map
                .get(&def.schedule_id)
                .and_then(|s| s.last_scheduled_for);

            let ticks = self.compute_due_ticks(
                &schedule,
                last_scheduled_for,
                now,
                def.max_catchup_ticks,
                Duration::minutes(def.catchup_window_minutes as i64),
                tz,
            );

            for scheduled_for in ticks {
                events.push(self.create_tick_event(def, scheduled_for));
            }
        }

        events
    }

    fn compute_due_ticks(
        &self,
        schedule: &Schedule,
        last_scheduled_for: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
        max_catchup: u32,
        catchup_window: Duration,
        tz: Tz,
    ) -> Vec<DateTime<Utc>> {
        let catchup_window = if catchup_window.is_zero() {
            self.default_catchup_window
        } else {
            catchup_window
        };

        let start = last_scheduled_for
            .map(|t| t + Duration::seconds(1))
            .unwrap_or(now - catchup_window);

        let start_tz = start.with_timezone(&tz);
        let now_tz = now.with_timezone(&tz);

        schedule
            .after(&start_tz)
            .take_while(|t| *t <= now_tz)
            .take(max_catchup as usize)
            .map(|t| t.with_timezone(&Utc))
            .collect()
    }

    fn create_tick_event(
        &self,
        def: &ScheduleDefinitionRow,
        scheduled_for: DateTime<Utc>,
    ) -> OrchestrationEvent {
        let tick_id = format!("{}:{}", def.schedule_id, scheduled_for.timestamp());
        let run_key = format!("sched:{}:{}", def.schedule_id, scheduled_for.timestamp());
        let fingerprint = compute_request_fingerprint(&def.asset_selection, None);

        OrchestrationEvent::new(
            &def.tenant_id,
            &def.workspace_id,
            OrchestrationEventData::ScheduleTicked {
                schedule_id: def.schedule_id.clone(),
                scheduled_for,
                tick_id,
                definition_version: def.row_version.clone(),
                asset_selection: def.asset_selection.clone(),
                partition_selection: None, // Schedules may add this later
                status: TickStatus::Triggered,
                run_key: Some(run_key),
                request_fingerprint: Some(fingerprint),
            },
        )
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement schedule controller"
```

---

**Task 2.2: Schedule Tick Fold Logic**

**Step 1: Write failing test for fold_schedule_ticked**

```rust
#[test]
fn test_fold_schedule_ticked_updates_state_and_creates_tick_row() {
    let mut state = FoldState::new_for_test();

    let event = schedule_ticked_event(
        "daily-etl",
        Utc::now(),
        TickStatus::Triggered,
        Some("sched:daily-etl:1736935200"),
    );

    state.fold_event(&event);

    // Should update schedule_state
    let schedule_state = state.schedule_state.get("daily-etl").unwrap();
    assert!(schedule_state.last_scheduled_for.is_some());
    assert_eq!(schedule_state.last_run_key, Some("sched:daily-etl:1736935200".into()));

    // Should create tick history row
    let tick = state.schedule_ticks.get("daily-etl:1736935200").unwrap();
    assert!(matches!(tick.status, TickStatus::Triggered));
}

/// NOTE: Per P0-1, fold does NOT emit events. Controllers emit all events atomically.
/// This test verifies fold only updates projections.
#[test]
fn test_fold_schedule_ticked_is_pure_projection() {
    let mut state = FoldState::new_for_test();

    let event = schedule_ticked_event(
        "daily-etl",
        Utc::now(),
        TickStatus::Triggered,
        Some("sched:daily-etl:1736935200"),
    );

    // fold_event returns nothing (pure projection)
    state.fold_event(&event);

    // Verify projections updated
    assert!(state.schedule_ticks.contains_key("daily-etl:1736935200"));
    // RunRequested should come from controller, not fold
}

/// Test that controller emits BOTH ScheduleTicked AND RunRequested atomically
#[test]
fn test_schedule_controller_emits_tick_and_run_requested_atomically() {
    let controller = ScheduleController::new();
    let definitions = vec![ScheduleDefinitionRow {
        schedule_id: "01HQ123SCHEDXYZ".into(),
        cron_expression: "0 10 * * *".into(),
        asset_selection: vec!["asset_b".into()],
        enabled: true,
        ..Default::default()
    }];
    let state = vec![];
    let now = Utc::now();

    let events = controller.reconcile(&definitions, &state, now);

    // Controller must emit BOTH events in same batch
    let has_tick = events.iter().any(|e|
        matches!(&e.data, OrchestrationEventData::ScheduleTicked { .. })
    );
    let has_run_req = events.iter().any(|e|
        matches!(&e.data, OrchestrationEventData::RunRequested { .. })
    );

    assert!(has_tick, "Controller must emit ScheduleTicked");
    assert!(has_run_req, "Controller must emit RunRequested with tick");

    // Verify RunRequested uses snapshotted asset_selection
    let run_req = events.iter().find(|e|
        matches!(&e.data, OrchestrationEventData::RunRequested { .. })
    ).unwrap();
    if let OrchestrationEventData::RunRequested { asset_selection, .. } = &run_req.data {
        assert_eq!(asset_selection, &vec!["asset_b".into()]);
    }
}
```

**Step 2: Implement fold_schedule_ticked**

```rust
fn fold_schedule_ticked(&mut self, event: &OrchestrationEvent) {
    let OrchestrationEventData::ScheduleTicked {
        schedule_id,
        scheduled_for,
        tick_id,
        definition_version,
        asset_selection,
        partition_selection,
        status,
        run_key,
        request_fingerprint,
    } = &event.data else {
        return;
    };

    // Update schedule state
    self.schedule_state
        .entry(schedule_id.clone())
        .or_insert_with(|| ScheduleStateRow::new(schedule_id))
        .update_from_tick(scheduled_for, tick_id, run_key, &event.event_id);

    // Create tick history row
    self.schedule_ticks.insert(
        tick_id.clone(),
        ScheduleTickRow {
            tick_id: tick_id.clone(),
            schedule_id: schedule_id.clone(),
            scheduled_for: *scheduled_for,
            definition_version: definition_version.clone(),
            asset_selection: asset_selection.clone(),
            partition_selection: partition_selection.clone(),
            status: status.clone(),
            run_key: run_key.clone(),
            run_id: None, // Filled by fold_run_requested correlation
            request_fingerprint: request_fingerprint.clone(),
            row_version: event.event_id.clone(),
            ..Default::default()
        },
    );

    // NOTE: Per P0-1, fold does NOT emit RunRequested.
    // Controller emits ScheduleTicked + RunRequested atomically.
    // fold_run_requested handles the RunRequested event separately.
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement schedule tick fold logic"
```

---

**Task 2.3: Manual Trigger Endpoint**

**Step 1: Write failing test for manual schedule trigger**

```rust
#[test]
fn test_schedule_manual_trigger_creates_tick() {
    let controller = ScheduleController::new();
    let def = ScheduleDefinitionRow {
        schedule_id: "daily-etl".into(),
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        row_version: "def_01HQ123".into(),
        asset_selection: vec!["analytics.summary".into()],
        ..Default::default()
    };

    let event = controller.manual_trigger(&def, Utc::now());

    assert!(matches!(
        &event.data,
        OrchestrationEventData::ScheduleTicked {
            status: TickStatus::Triggered,
            ..
        }
    ));
}
```

**Step 2: Implement manual_trigger**

```rust
impl ScheduleController {
    /// Manually trigger a schedule (for debugging/testing).
    pub fn manual_trigger(
        &self,
        def: &ScheduleDefinitionRow,
        now: DateTime<Utc>,
    ) -> OrchestrationEvent {
        let tick_id = format!("{}:manual:{}", def.schedule_id, now.timestamp());
        let run_key = format!("sched:{}:manual:{}", def.schedule_id, now.timestamp());

        OrchestrationEvent::new(
            &def.tenant_id,
            &def.workspace_id,
            OrchestrationEventData::ScheduleTicked {
                schedule_id: def.schedule_id.clone(),
                scheduled_for: now,
                tick_id,
                definition_version: def.row_version.clone(),
                asset_selection: def.asset_selection.clone(),
                partition_selection: None,
                status: TickStatus::Triggered,
                run_key: Some(run_key),
                request_fingerprint: None, // Manual triggers don't have fingerprint
            },
        )
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add manual schedule trigger"
```

---

### Epic 3: Sensor Controller

**Goal:** Implement push and poll sensor evaluation.

**Files:**
- Create: `crates/arco-flow/src/orchestration/controllers/sensor.rs`
- Create: `crates/arco-flow/src/orchestration/controllers/sensor_evaluator.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Test: `crates/arco-flow/tests/orchestration_sensor_tests.rs`

---

**Task 3.1: Push Sensor Handler**

**Step 1: Write failing test for push sensor**

```rust
#[test]
fn test_push_sensor_uses_message_id_for_idempotency() {
    let handler = PushSensorHandler::new();

    let message = PubSubMessage {
        message_id: "msg_abc123".into(),
        data: b"new file arrived".to_vec(),
        attributes: HashMap::new(),
        publish_time: Utc::now(),
    };

    let event = handler.handle_message(
        "new-files-sensor",
        "tenant-abc",
        "workspace-prod",
        &message,
    );

    // Idempotency key includes message_id
    assert!(event.idempotency_key.contains("msg:msg_abc123"));
}

#[test]
fn test_push_sensor_is_cursorless_by_default() {
    let handler = PushSensorHandler::new();

    let event = handler.handle_message(
        "new-files-sensor",
        "tenant-abc",
        "workspace-prod",
        &test_message("msg_001"),
    );

    if let OrchestrationEventData::SensorEvaluated { cursor_before, cursor_after, .. } = &event.data {
        // Push sensors don't track cursor (message is the cursor)
        assert!(cursor_before.is_none());
        // cursor_after may contain message info for debugging
    }
}
```

**Step 2: Implement push sensor handler**

```rust
// crates/arco-flow/src/orchestration/controllers/sensor.rs
pub struct PushSensorHandler {
    /// Sensor definitions (loaded from projection).
    definitions: HashMap<String, SensorDefinitionRow>,
}

impl PushSensorHandler {
    pub fn handle_message(
        &self,
        sensor_id: &str,
        tenant_id: &str,
        workspace_id: &str,
        message: &PubSubMessage,
    ) -> OrchestrationEvent {
        let eval_id = format!("eval_{}_{}", sensor_id, Ulid::new());

        // Evaluate sensor logic (user-defined)
        let run_requests = self.evaluate_sensor(sensor_id, message);

        let status = if run_requests.is_empty() {
            SensorEvalStatus::NoNewData
        } else {
            SensorEvalStatus::Triggered
        };

        OrchestrationEvent::new(
            tenant_id,
            workspace_id,
            OrchestrationEventData::SensorEvaluated {
                sensor_id: sensor_id.to_string(),
                eval_id,
                cursor_before: None, // Push is cursorless
                cursor_after: Some(message.message_id.clone()),
                expected_state_version: None,
                trigger_source: TriggerSource::Push {
                    message_id: message.message_id.clone(),
                },
                run_requests,
                status,
            },
        )
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement push sensor handler"
```

---

**Task 3.2: Poll Sensor Controller**

**Step 1: Write failing test for poll sensor idempotency**

```rust
#[test]
fn test_poll_sensor_idempotency_uses_cursor_before() {
    let controller = PollSensorController::new();

    let state = SensorStateRow {
        sensor_id: "poll-sensor".into(),
        cursor: Some("cursor_v1".into()),
        state_version: 7,
        ..Default::default()
    };

    let event = controller.evaluate(
        "poll-sensor",
        "tenant-abc",
        "workspace-prod",
        &state,
        1736935200, // poll_epoch
    );

    // Idempotency based on cursor_before (input), not cursor_after (output)
    let cursor_hash = sha256_hex("cursor_v1");
    assert!(event.idempotency_key.contains(&cursor_hash));
    if let OrchestrationEventData::SensorEvaluated { expected_state_version, .. } = &event.data {
        assert_eq!(*expected_state_version, Some(7));
    }
}

#[test]
fn test_poll_sensor_respects_min_interval() {
    let controller = PollSensorController::new();

    // If last_evaluation is recent, skip this poll
    let state = SensorStateRow {
        sensor_id: "poll-sensor".into(),
        last_evaluation_at: Some(Utc::now() - Duration::seconds(10)),
        status: SensorStatus::Active,
        ..Default::default()
    };

    let should_evaluate = controller.should_evaluate(&state, Utc::now());

    assert!(!should_evaluate, "Should not evaluate before min_poll_interval elapses");
}
```

**Step 2: Implement poll sensor controller**

```rust
pub struct PollSensorController {
    min_poll_interval: Duration,
}

impl PollSensorController {
    pub fn should_evaluate(&self, state: &SensorStateRow, now: DateTime<Utc>) -> bool {
        // Skip if paused
        if state.status == SensorStatus::Paused {
            return false;
        }

        // Check min interval
        if let Some(last_eval) = state.last_evaluation_at {
            if now - last_eval < self.min_poll_interval {
                return false;
            }
        }

        true
    }

    pub fn evaluate(
        &self,
        sensor_id: &str,
        tenant_id: &str,
        workspace_id: &str,
        state: &SensorStateRow,
        poll_epoch: i64,
    ) -> OrchestrationEvent {
        let eval_id = format!("eval_{}_{}", sensor_id, Ulid::new());
        let cursor_before = state.cursor.clone();

        // Hash cursor_before for idempotency
        let cursor_hash = cursor_before
            .as_ref()
            .map(|c| sha256_hex(c))
            .unwrap_or_else(|| "none".to_string());

        // Evaluate sensor (user-defined logic)
        let (cursor_after, run_requests) = self.evaluate_sensor_logic(sensor_id, &cursor_before);

        let status = if run_requests.is_empty() {
            SensorEvalStatus::NoNewData
        } else {
            SensorEvalStatus::Triggered
        };

        OrchestrationEvent::new(
            tenant_id,
            workspace_id,
            OrchestrationEventData::SensorEvaluated {
                sensor_id: sensor_id.to_string(),
                eval_id,
                cursor_before,
                cursor_after,
                expected_state_version: Some(state.state_version),
                trigger_source: TriggerSource::Poll { poll_epoch },
                run_requests,
                status,
            },
        )
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement poll sensor controller"
```

---

**Task 3.3: Sensor Fold Logic with Multiple Run Requests**

**Step 1: Write failing test for multiple run requests**

```rust
/// NOTE: Per P0-1, fold does NOT emit events. Controllers emit all events atomically.
/// This test verifies fold only updates projections (cursor, state_version).
#[test]
fn test_fold_sensor_evaluated_is_pure_projection() {
    let mut state = FoldState::new_for_test();

    let event = sensor_evaluated_event(
        "multi-sensor",
        vec![], // run_requests are in the event but NOT emitted by fold
    );

    // fold_event updates projections only
    state.fold_event(&event);

    // Verify cursor updated
    let sensor = state.sensor_state.get("multi-sensor").unwrap();
    assert!(sensor.cursor.is_some());
}

/// Test that controller emits SensorEvaluated + RunRequested(s) atomically
#[test]
fn test_sensor_controller_emits_eval_and_run_requests_atomically() {
    let handler = PushSensorHandler::new();
    let message = PubSubMessage {
        message_id: "msg_abc".into(),
        data: b"test".to_vec(),
        attributes: HashMap::new(),
        publish_time: Utc::now(),
    };

    let events = handler.handle_message("01HQ123SENSORXYZ", "tenant", "workspace", &message);

    // Controller must emit SensorEvaluated AND any RunRequested events
    let has_eval = events.iter().any(|e|
        matches!(&e.data, OrchestrationEventData::SensorEvaluated { .. })
    );
    assert!(has_eval, "Controller must emit SensorEvaluated");

    // All RunRequested must be in same batch (not emitted by fold)
}

#[test]
fn test_fold_sensor_evaluated_drops_on_version_mismatch() {
    let mut state = FoldState::new_for_test();

    state.sensor_state.insert(
        "poll-sensor".into(),
        SensorStateRow {
            sensor_id: "poll-sensor".into(),
            state_version: 2,
            ..Default::default()
        },
    );

    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: "poll-sensor".into(),
            eval_id: "eval_01HQ999".into(),
            cursor_before: Some("cursor_v1".into()),
            cursor_after: Some("cursor_v2".into()),
            expected_state_version: Some(1), // stale
            trigger_source: TriggerSource::Poll { poll_epoch: 1736935200 },
            run_requests: vec![RunRequest {
                run_key: "sensor:poll-sensor:req1".into(),
                request_fingerprint: "fp1".into(),
                asset_selection: vec!["asset_a".into()],
                partition_selection: None,
            }],
            status: SensorEvalStatus::Triggered,
        },
    );

    let emitted = state.fold_event(&event);
    assert!(emitted.is_empty(), "stale eval should be dropped");
}
```

**Step 2: Implement fold_sensor_evaluated**

```rust
fn fold_sensor_evaluated(&mut self, event: &OrchestrationEvent) {
    let OrchestrationEventData::SensorEvaluated {
        sensor_id,
        eval_id,
        cursor_before,
        cursor_after,
        expected_state_version,
        trigger_source,
        run_requests,
        status,
    } = &event.data else {
        return;
    };

    // Update sensor state
    let state = self.sensor_state
        .entry(sensor_id.clone())
        .or_insert_with(|| SensorStateRow::new(sensor_id));

    if let Some(expected) = expected_state_version {
        if state.state_version != *expected {
            return; // Drop stale/duplicate evals
        }
    }

    state.update_from_eval(cursor_after, eval_id, &event.timestamp, &event.event_id); // increments state_version

    // NOTE: Per P0-1, fold does NOT emit RunRequested.
    // Controller emits SensorEvaluated + RunRequested(s) atomically.
    // fold_run_requested handles each RunRequested event separately.
    // run_requests field is preserved in eval history for correlation.
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement sensor fold with multiple run requests"
```

---

**Task 3.4: Manual Sensor Evaluate Endpoint**

**Step 1: Write failing test**

```rust
#[test]
fn test_sensor_manual_evaluate_with_sample_payload() {
    let controller = PushSensorHandler::new();

    let sample_payload = json!({
        "bucket": "test-bucket",
        "object": "data/file.parquet"
    });

    let event = controller.manual_evaluate(
        "new-files-sensor",
        "tenant-abc",
        "workspace-prod",
        sample_payload,
    );

    assert!(matches!(
        &event.data,
        OrchestrationEventData::SensorEvaluated { .. }
    ));
}
```

**Step 2: Implement manual_evaluate**

```rust
impl PushSensorHandler {
    /// Manually evaluate a sensor with a sample payload (for testing).
    pub fn manual_evaluate(
        &self,
        sensor_id: &str,
        tenant_id: &str,
        workspace_id: &str,
        sample_payload: serde_json::Value,
    ) -> OrchestrationEvent {
        let message = PubSubMessage {
            message_id: format!("manual_{}", Ulid::new()),
            data: serde_json::to_vec(&sample_payload).unwrap_or_default(),
            attributes: HashMap::new(),
            publish_time: Utc::now(),
        };

        self.handle_message(sensor_id, tenant_id, workspace_id, &message)
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add manual sensor evaluate"
```

---

### Epic 4: Backfill Controller

**Goal:** Implement run-per-chunk backfill with pause/resume/cancel.

**Files:**
- Create: `crates/arco-flow/src/orchestration/controllers/backfill.rs`
- Create: `crates/arco-flow/src/orchestration/controllers/partition_resolver.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Test: `crates/arco-flow/tests/orchestration_backfill_tests.rs`

---

**Task 4.1: Backfill Preview**

**Step 1: Write failing test for backfill preview**

```rust
#[test]
fn test_backfill_preview_returns_partition_count_and_chunks() {
    let controller = BackfillController::new(test_partition_resolver());

    let preview = controller.preview(
        "analytics.daily",
        "2025-01-01",
        "2025-01-31",
        10, // chunk_size
    );

    assert_eq!(preview.total_partitions, 31);
    assert_eq!(preview.total_chunks, 4); // ceil(31/10)
    assert_eq!(preview.first_chunk_partitions.len(), 10);
    assert_eq!(preview.estimated_runs, 4);
}
```

Note: tests assume a stub `test_partition_resolver()` that returns a deterministic daily partition resolver for the test asset.

```rust
struct TestPartitionResolver;

impl PartitionResolver for TestPartitionResolver {
    fn count_range(&self, _asset_key: &str, start: &str, end: &str) -> u32 {
        let start = NaiveDate::parse_from_str(start, "%Y-%m-%d").unwrap();
        let end = NaiveDate::parse_from_str(end, "%Y-%m-%d").unwrap();
        (end - start).num_days() as u32 + 1
    }

    fn list_range_chunk(
        &self,
        _asset_key: &str,
        start: &str,
        end: &str,
        offset: u32,
        limit: u32,
    ) -> Vec<String> {
        let start = NaiveDate::parse_from_str(start, "%Y-%m-%d").unwrap();
        let end = NaiveDate::parse_from_str(end, "%Y-%m-%d").unwrap();

        (0..)
            .map(|i| start + chrono::Duration::days(i))
            .take_while(|d| *d <= end)
            .skip(offset as usize)
            .take(limit as usize)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .collect()
    }
}

fn test_partition_resolver() -> Arc<dyn PartitionResolver + Send + Sync> {
    Arc::new(TestPartitionResolver)
}
```

**Step 2: Implement backfill preview**

```rust
// crates/arco-flow/src/orchestration/controllers/backfill.rs
pub struct BackfillController {
    default_chunk_size: u32,
    default_max_concurrent: u32,
    partition_resolver: Arc<dyn PartitionResolver + Send + Sync>,
}

/// Resolves partitions for an asset across multiple partitioning schemes.
pub trait PartitionResolver {
    fn count_range(&self, asset_key: &str, start: &str, end: &str) -> u32;
    fn list_range_chunk(
        &self,
        asset_key: &str,
        start: &str,
        end: &str,
        offset: u32,
        limit: u32,
    ) -> Vec<String>;
}
// Implementations can read partition specs from asset definitions (daily/hourly/multi-dim).

#[derive(Debug, Clone)]
pub struct BackfillPreview {
    pub total_partitions: u32,
    pub total_chunks: u32,
    pub first_chunk_partitions: Vec<String>,
    pub estimated_runs: u32,
}

impl BackfillController {
    pub fn new(partition_resolver: Arc<dyn PartitionResolver + Send + Sync>) -> Self {
        Self {
            default_chunk_size: 10,
            default_max_concurrent: 2,
            partition_resolver,
        }
    }

    pub fn preview(
        &self,
        asset_key: &str,
        partition_start: &str,
        partition_end: &str,
        chunk_size: u32,
    ) -> BackfillPreview {
        let total_partitions =
            self.partition_resolver.count_range(asset_key, partition_start, partition_end);
        let total_chunks = (total_partitions + chunk_size - 1) / chunk_size;
        let first_chunk = self.partition_resolver.list_range_chunk(
            asset_key,
            partition_start,
            partition_end,
            0,
            chunk_size,
        );

        BackfillPreview {
            total_partitions,
            total_chunks,
            first_chunk_partitions: first_chunk,
            estimated_runs: total_chunks,
        }
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add backfill preview"
```

---

**Task 4.2: Backfill Creation**

**Step 1: Write failing test for backfill creation**

```rust
#[test]
fn test_backfill_create_emits_backfill_created_event() {
    let controller = BackfillController::new(test_partition_resolver());

    let event = controller.create(
        "bf_01HQ123",
        "tenant-abc",
        "workspace-prod",
        &["analytics.daily".into()],
        "2025-01-01",
        "2025-01-31",
        10, // chunk_size
        2,  // max_concurrent_runs
        "client_req_001", // idempotency key
    );

    if let OrchestrationEventData::BackfillCreated {
        backfill_id,
        partition_selector,
        total_partitions,
        chunk_size,
        max_concurrent_runs,
        ..
    } = &event.data {
        assert_eq!(backfill_id, "bf_01HQ123");
        assert_eq!(*total_partitions, 31);
        assert!(matches!(
            partition_selector,
            PartitionSelector::Range { start, end }
            if start == "2025-01-01" && end == "2025-01-31"
        ));
        assert_eq!(*chunk_size, 10);
        assert_eq!(*max_concurrent_runs, 2);
    } else {
        panic!("Expected BackfillCreated");
    }
}
```

**Step 2: Implement backfill creation**

```rust
impl BackfillController {
    pub fn create(
        &self,
        backfill_id: &str,
        tenant_id: &str,
        workspace_id: &str,
        asset_selection: &[String],
        partition_start: &str,
        partition_end: &str,
        chunk_size: u32,
        max_concurrent_runs: u32,
        client_request_id: &str,
    ) -> OrchestrationEvent {
        let total_partitions = self.partition_resolver.count_range(
            asset_selection.first().unwrap_or(&"".to_string()),
            partition_start,
            partition_end,
        );

        OrchestrationEvent::new(
            tenant_id,
            workspace_id,
            OrchestrationEventData::BackfillCreated {
                backfill_id: backfill_id.to_string(),
                client_request_id: client_request_id.to_string(),
                asset_selection: asset_selection.to_vec(),
                partition_selector: PartitionSelector::Range {
                    start: partition_start.to_string(),
                    end: partition_end.to_string(),
                },
                total_partitions,
                chunk_size,
                max_concurrent_runs,
            },
        )
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add backfill creation"
```

---

**Task 4.3: Backfill Chunk Planning**

**Step 1: Write failing test for chunk planning**

```rust
#[test]
fn test_backfill_controller_plans_chunks_respecting_concurrency() {
    let mut state = FoldState::new_for_test();

    // Create backfill with 10 partitions, chunk_size=3, max_concurrent=2
    state.backfills.insert("bf_001".into(), BackfillRow {
        backfill_id: "bf_001".into(),
        state: BackfillState::Running,
        partition_selector: PartitionSelector::Range {
            start: "2025-01-01".into(),
            end: "2025-01-10".into(),
        },
        chunk_size: 3,
        max_concurrent_runs: 2,
        total_partitions: 10,
        planned_chunks: 0,
        completed_chunks: 0,
        ..Default::default()
    });

    let controller = BackfillController::new(test_partition_resolver());
    let events = controller.reconcile(&state.backfills, &state.backfill_chunks, &state.runs);

    // Should plan 2 chunks (max_concurrent)
    let chunk_events: Vec<_> = events.iter()
        .filter(|e| matches!(&e.data, OrchestrationEventData::BackfillChunkPlanned { .. }))
        .collect();

    assert_eq!(chunk_events.len(), 2);
}

#[test]
fn test_backfill_chunk_run_key_is_deterministic() {
    let controller = BackfillController::new(test_partition_resolver());

    let event = controller.plan_chunk(
        "bf_001",
        0, // chunk_index
        &["2025-01-01".into(), "2025-01-02".into()],
        "tenant-abc",
        "workspace-prod",
    );

    if let OrchestrationEventData::BackfillChunkPlanned { run_key, chunk_id, .. } = &event.data {
        assert_eq!(chunk_id, "bf_001:0");
        assert_eq!(run_key, "backfill:bf_001:chunk:0");
    }
}
```

**Step 2: Implement chunk planning**

```rust
impl BackfillController {
    pub fn reconcile(
        &self,
        backfills: &HashMap<String, BackfillRow>,
        chunks: &HashMap<String, BackfillChunkRow>,
        runs: &HashMap<String, RunRow>,
    ) -> Vec<OrchestrationEvent> {
        let mut events = Vec::new();

        for (backfill_id, backfill) in backfills {
            if backfill.state != BackfillState::Running {
                continue;
            }

            // Count active chunks
            let active_chunks = self.count_active_chunks(backfill_id, chunks, runs);

            // Plan more chunks if below concurrency limit
            let chunks_to_plan = backfill.max_concurrent_runs.saturating_sub(active_chunks);

            for i in 0..chunks_to_plan {
                let next_chunk_index = backfill.planned_chunks + i;
                if next_chunk_index >= self.total_chunks(backfill) {
                    break;
                }

                let partition_keys = self.get_chunk_partitions(backfill, next_chunk_index);
                events.push(self.plan_chunk(
                    backfill_id,
                    next_chunk_index,
                    &partition_keys,
                    &backfill.tenant_id,
                    &backfill.workspace_id,
                ));
            }
        }

        events
    }

    pub fn plan_chunk(
        &self,
        backfill_id: &str,
        chunk_index: u32,
        partition_keys: &[String],
        tenant_id: &str,
        workspace_id: &str,
    ) -> OrchestrationEvent {
        let chunk_id = format!("{}:{}", backfill_id, chunk_index);
        let run_key = format!("backfill:{}:chunk:{}", backfill_id, chunk_index);
        let fingerprint = compute_request_fingerprint(partition_keys, None);

        OrchestrationEvent::new(
            tenant_id,
            workspace_id,
            OrchestrationEventData::BackfillChunkPlanned {
                backfill_id: backfill_id.to_string(),
                chunk_id,
                chunk_index,
                partition_keys: partition_keys.to_vec(),
                run_key,
                request_fingerprint: fingerprint,
            },
        )
    }

    fn count_active_chunks(
        &self,
        backfill_id: &str,
        chunks: &HashMap<String, BackfillChunkRow>,
        runs: &HashMap<String, RunRow>,
    ) -> u32 {
        chunks
            .values()
            .filter(|c| c.backfill_id == backfill_id)
            .filter(|c| {
                matches!(c.state, ChunkState::Planned | ChunkState::Running)
                    || c.run_id.as_ref().map(|id| {
                        runs.get(id).map(|r| !r.state.is_terminal()).unwrap_or(false)
                    }).unwrap_or(false)
            })
            .count() as u32
    }

    fn get_chunk_partitions(&self, backfill: &BackfillRow, chunk_index: u32) -> Vec<String> {
        let offset = chunk_index * backfill.chunk_size;
        let limit = backfill.chunk_size;
        let asset_key = backfill.asset_selection.first().map(|s| s.as_str()).unwrap_or("");

        match &backfill.partition_selector {
            PartitionSelector::Range { start, end } => self.partition_resolver.list_range_chunk(
                asset_key,
                start,
                end,
                offset,
                limit,
            ),
            PartitionSelector::Explicit { partition_keys } => partition_keys
                .iter()
                .skip(offset as usize)
                .take(limit as usize)
                .cloned()
                .collect(),
        }
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement backfill chunk planning"
```

---

**Task 4.4: Backfill State Transitions (Pause/Resume/Cancel)**

> Note: Chunk failures while `PAUSED` do not transition the backfill to `FAILED`.
> Increment `failed_chunks` and surface a display status like `PAUSED_WITH_FAILURES`
> so the user can resume + retry-failed or cancel.

**Step 1: Write failing test for state transitions with version**

```rust
#[test]
fn test_backfill_pause_uses_state_version_for_idempotency() {
    let controller = BackfillController::new(test_partition_resolver());

    let current_state = BackfillRow {
        backfill_id: "bf_001".into(),
        state: BackfillState::Running,
        state_version: 2,
        ..Default::default()
    };

    let event = controller.pause("bf_001", &current_state, "tenant-abc", "workspace-prod");

    if let OrchestrationEventData::BackfillStateChanged {
        from_state,
        to_state,
        state_version,
        ..
    } = &event.data {
        assert_eq!(*from_state, BackfillState::Running);
        assert_eq!(*to_state, BackfillState::Paused);
        assert_eq!(*state_version, 3); // Incremented
    }

    // Idempotency key uses state_version
    assert!(event.idempotency_key.contains("backfill_state:bf_001:3"));
}

#[test]
fn test_backfill_pause_is_idempotent_with_expected_version() {
    let controller = BackfillController::new(test_partition_resolver());

    // Pause request with expected_version=2 (current is 2)
    let result = controller.pause_with_version(
        "bf_001",
        2, // expected_version
        &BackfillRow {
            state_version: 2,
            state: BackfillState::Running,
            ..Default::default()
        },
    );

    assert!(result.is_ok());

    // Pause request with expected_version=1 (current is 2) - conflict
    let result = controller.pause_with_version(
        "bf_001",
        1, // wrong expected_version
        &BackfillRow {
            state_version: 2,
            state: BackfillState::Running,
            ..Default::default()
        },
    );

    assert!(matches!(result, Err(BackfillError::VersionConflict { .. })));
}
```

**Step 2: Implement state transitions**

```rust
impl BackfillController {
    pub fn pause(
        &self,
        backfill_id: &str,
        current: &BackfillRow,
        tenant_id: &str,
        workspace_id: &str,
    ) -> OrchestrationEvent {
        self.transition(
            backfill_id,
            current,
            BackfillState::Paused,
            tenant_id,
            workspace_id,
            None,
        )
    }

    pub fn resume(
        &self,
        backfill_id: &str,
        current: &BackfillRow,
        tenant_id: &str,
        workspace_id: &str,
    ) -> OrchestrationEvent {
        self.transition(
            backfill_id,
            current,
            BackfillState::Running,
            tenant_id,
            workspace_id,
            None,
        )
    }

    pub fn cancel(
        &self,
        backfill_id: &str,
        current: &BackfillRow,
        tenant_id: &str,
        workspace_id: &str,
    ) -> OrchestrationEvent {
        self.transition(
            backfill_id,
            current,
            BackfillState::Cancelled,
            tenant_id,
            workspace_id,
            None,
        )
    }

    fn transition(
        &self,
        backfill_id: &str,
        current: &BackfillRow,
        to_state: BackfillState,
        tenant_id: &str,
        workspace_id: &str,
        changed_by: Option<String>,
    ) -> OrchestrationEvent {
        let new_version = current.state_version + 1;

        OrchestrationEvent::new(
            tenant_id,
            workspace_id,
            OrchestrationEventData::BackfillStateChanged {
                backfill_id: backfill_id.to_string(),
                from_state: current.state,
                to_state,
                state_version: new_version,
                changed_by,
            },
        )
    }

    pub fn pause_with_version(
        &self,
        backfill_id: &str,
        expected_version: u32,
        current: &BackfillRow,
    ) -> Result<OrchestrationEvent, BackfillError> {
        if current.state_version != expected_version {
            return Err(BackfillError::VersionConflict {
                expected: expected_version,
                actual: current.state_version,
            });
        }

        Ok(self.pause(backfill_id, current, &current.tenant_id, &current.workspace_id))
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement backfill state transitions"
```

---

**Task 4.5: Retry-Failed Backfill**

**Step 1: Write failing test for retry-failed**

```rust
#[test]
fn test_retry_failed_creates_new_backfill_with_parent() {
    let controller = BackfillController::new(test_partition_resolver());

    let parent = BackfillRow {
        backfill_id: "bf_001".into(),
        state: BackfillState::Failed,
        ..Default::default()
    };

    let failed_chunks = vec![
        BackfillChunkRow {
            chunk_id: "bf_001:2".into(),
            state: ChunkState::Failed,
            partition_keys: vec!["2025-01-03".into()],
            ..Default::default()
        },
        BackfillChunkRow {
            chunk_id: "bf_001:5".into(),
            state: ChunkState::Failed,
            partition_keys: vec!["2025-01-06".into(), "2025-01-07".into()],
            ..Default::default()
        },
    ];

    let event = controller.retry_failed(
        "bf_002", // new backfill_id
        &parent,
        &failed_chunks,
        "retry_req_001", // idempotency
        "tenant-abc",
        "workspace-prod",
    );

    if let OrchestrationEventData::BackfillCreated {
        backfill_id,
        partition_selector,
        parent_backfill_id,
        ..
    } = &event.data {
        assert_eq!(backfill_id, "bf_002");
        // Only failed partitions
        if let PartitionSelector::Explicit { partition_keys } = partition_selector {
            assert_eq!(partition_keys.len(), 3);
            assert!(partition_keys.contains(&"2025-01-03".to_string()));
        } else {
            panic!("Expected explicit partition selector");
        }
        // Links to parent
        assert_eq!(parent_backfill_id, &Some("bf_001".to_string()));
    }
}

#[test]
fn test_retry_failed_is_idempotent_with_retry_request_id() {
    let controller = BackfillController::new(test_partition_resolver());

    // Same retry_request_id should produce same idempotency_key
    let event1 = controller.retry_failed(
        "bf_002",
        &parent_backfill(),
        &failed_chunks(),
        "retry_req_001",
        "tenant-abc",
        "workspace-prod",
    );

    let event2 = controller.retry_failed(
        "bf_003", // Different ID, same request
        &parent_backfill(),
        &failed_chunks(),
        "retry_req_001", // Same request ID
        "tenant-abc",
        "workspace-prod",
    );

    // Idempotency key based on parent + retry_request_id
    assert!(event1.idempotency_key.contains("retry_req_001"));
    assert_eq!(event1.idempotency_key, event2.idempotency_key);
}
```

**Step 2: Implement retry_failed**

```rust
impl BackfillController {
    pub fn retry_failed(
        &self,
        new_backfill_id: &str,
        parent: &BackfillRow,
        failed_chunks: &[BackfillChunkRow],
        retry_request_id: &str,
        tenant_id: &str,
        workspace_id: &str,
    ) -> OrchestrationEvent {
        // Collect only failed partitions
        let partition_keys: Vec<String> = failed_chunks
            .iter()
            .flat_map(|c| c.partition_keys.clone())
            .collect();

        OrchestrationEvent::new_with_idempotency_key(
            tenant_id,
            workspace_id,
            OrchestrationEventData::BackfillCreated {
                backfill_id: new_backfill_id.to_string(),
                client_request_id: retry_request_id.to_string(),
                asset_selection: parent.asset_selection.clone(),
                partition_selector: PartitionSelector::Explicit { partition_keys },
                total_partitions: partition_keys.len() as u32,
                chunk_size: parent.chunk_size,
                max_concurrent_runs: parent.max_concurrent_runs,
                parent_backfill_id: Some(parent.backfill_id.clone()),
            },
            // Idempotency based on parent + retry_request_id
            format!("backfill_retry:{}:{}", parent.backfill_id, retry_request_id),
        )
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement retry-failed backfill"
```

---

### Epic 5: Partition Status Tracking

**Goal:** Track partition materialization status.

**Files:**
- Create: `crates/arco-flow/src/orchestration/controllers/partition_status.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Test: `crates/arco-flow/tests/orchestration_partition_tests.rs`

---

**Task 5.1: PartitionMaterialized Event (Projection-Only)**

**Step 1: Write failing test**

```rust
#[test]
fn test_partition_materialized_updates_status() {
    let mut state = FoldState::new_for_test();

    // Task succeeds with asset_key and partition_key
    let event = task_finished_event(
        "extract",
        TaskOutcome::Succeeded,
        Some("analytics.daily"),
        Some("2025-01-15"),
        Some("mat_01HQ123"),
    );

    state.fold_event(&event);

    // Should update partition_status
    let status = state.partition_status.get(&("analytics.daily", "2025-01-15"));
    assert!(status.is_some());

    let status = status.unwrap();
    let display_status = compute_display_status(status);
    assert_eq!(display_status, PartitionMaterializationStatus::Materialized);
    assert_eq!(status.last_materialization_run_id, Some("run_123".into()));
}
```

**Step 2: Implement partition status update in fold**

```rust
fn fold_task_finished(&mut self, event: &OrchestrationEvent) {
    let OrchestrationEventData::TaskFinished {
        task_key,
        run_id,
        outcome,
        asset_key,
        partition_key,
        materialization_id,
        code_version,
        ..
    } = &event.data else {
        return;
    };

    // ... existing task state update ...

    // Update partition status if asset task succeeded
    if *outcome == TaskOutcome::Succeeded {
        if let (Some(asset_key), Some(partition_key)) = (asset_key, partition_key) {
            self.update_partition_status(
                asset_key,
                partition_key,
                run_id,
                code_version.as_deref(),
                &event.timestamp,
                &event.event_id,
            );
        }
    }
}

fn update_partition_status(
    &mut self,
    asset_key: &str,
    partition_key: &str,
    run_id: &str,
    code_version: Option<&str>,
    materialized_at: &DateTime<Utc>,
    event_id: &str,
) {
    let key = (asset_key.to_string(), partition_key.to_string());

    self.partition_status
        .entry(key)
        .or_insert_with(|| PartitionStatusRow::new(asset_key, partition_key))
        .mark_materialized(run_id, materialized_at, code_version, event_id);
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement partition status tracking"
```

---

**Task 5.2: Staleness Query (Read-Time Computation)**

**Step 1: Write failing test for staleness computation**

```rust
#[test]
fn test_staleness_computed_at_query_time() {
    let now = Utc::now();

    let partition = PartitionStatusRow {
        asset_key: "analytics.daily".into(),
        partition_key: "2025-01-15".into(),
        last_materialization_at: Some(now - Duration::hours(25)),
        last_materialization_code_version: Some("v1".into()),
        ..Default::default()
    };

    let freshness_policy = FreshnessPolicy {
        maximum_lag_minutes: 60 * 24, // 24 hours
    };

    let staleness = compute_staleness(&partition, &freshness_policy, Some("v1"), now);

    assert!(staleness.is_stale);
    assert_eq!(staleness.reason, StalenessReason::FreshnessPolicy);
    assert!(staleness.stale_since.is_some());
}

#[test]
fn test_staleness_detects_upstream_changed() {
    let now = Utc::now();

    let downstream = PartitionStatusRow {
        asset_key: "analytics.summary".into(),
        partition_key: "2025-01".into(),
        last_materialization_at: Some(now - Duration::hours(2)),
        last_materialization_code_version: Some("v1".into()),
        ..Default::default()
    };

    let upstream = PartitionStatusRow {
        asset_key: "analytics.daily".into(),
        partition_key: "2025-01-15".into(),
        last_materialization_at: Some(now - Duration::hours(1)), // More recent
        last_materialization_code_version: Some("v1".into()),
        ..Default::default()
    };

    let staleness = compute_staleness_with_upstreams(&downstream, &[upstream], Some("v1"), now);

    assert!(staleness.is_stale);
    assert_eq!(staleness.reason, StalenessReason::UpstreamChanged);
}

#[test]
fn test_staleness_detects_code_changed() {
    let now = Utc::now();

    let partition = PartitionStatusRow {
        asset_key: "analytics.daily".into(),
        partition_key: "2025-01-15".into(),
        last_materialization_at: Some(now - Duration::hours(1)),
        last_materialization_code_version: Some("v1".into()),
        ..Default::default()
    };

    let freshness_policy = FreshnessPolicy {
        maximum_lag_minutes: 60 * 24,
    };

    let staleness = compute_staleness(&partition, &freshness_policy, Some("v2"), now);

    assert!(staleness.is_stale);
    assert_eq!(staleness.reason, StalenessReason::CodeChanged);
}
```

**Step 2: Implement staleness computation**

```rust
// crates/arco-flow/src/orchestration/controllers/partition_status.rs

#[derive(Debug, Clone)]
pub struct StalenessResult {
    pub is_stale: bool,
    pub reason: Option<StalenessReason>,
    pub stale_since: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StalenessReason {
    FreshnessPolicy,
    UpstreamChanged,
    CodeChanged,
    NeverMaterialized,
}

pub fn compute_staleness(
    partition: &PartitionStatusRow,
    policy: &FreshnessPolicy,
    current_code_version: Option<&str>,
    now: DateTime<Utc>,
) -> StalenessResult {
    // Check if never materialized
    let Some(last_mat) = partition.last_materialization_at else {
        return StalenessResult {
            is_stale: true,
            reason: Some(StalenessReason::NeverMaterialized),
            stale_since: None,
        };
    };

    // Check code version change
    if let (Some(current), Some(last)) = (
        current_code_version,
        partition.last_materialization_code_version.as_deref(),
    ) {
        if current != last {
            return StalenessResult {
                is_stale: true,
                reason: Some(StalenessReason::CodeChanged),
                stale_since: Some(now),
            };
        }
    }

    // Check freshness policy
    let deadline = last_mat + Duration::minutes(policy.maximum_lag_minutes as i64);
    if now > deadline {
        return StalenessResult {
            is_stale: true,
            reason: Some(StalenessReason::FreshnessPolicy),
            stale_since: Some(deadline),
        };
    }

    StalenessResult {
        is_stale: false,
        reason: None,
        stale_since: None,
    }
}

pub fn compute_staleness_with_upstreams(
    partition: &PartitionStatusRow,
    upstreams: &[PartitionStatusRow],
    current_code_version: Option<&str>,
    now: DateTime<Utc>,
) -> StalenessResult {
    let Some(last_mat) = partition.last_materialization_at else {
        return StalenessResult {
            is_stale: true,
            reason: Some(StalenessReason::NeverMaterialized),
            stale_since: None,
        };
    };

    // Check code version change
    if let (Some(current), Some(last)) = (
        current_code_version,
        partition.last_materialization_code_version.as_deref(),
    ) {
        if current != last {
            return StalenessResult {
                is_stale: true,
                reason: Some(StalenessReason::CodeChanged),
                stale_since: Some(now),
            };
        }
    }

    // Check if any upstream materialized after this partition
    for upstream in upstreams {
        if let Some(upstream_mat) = upstream.last_materialization_at {
            if upstream_mat > last_mat {
                return StalenessResult {
                    is_stale: true,
                    reason: Some(StalenessReason::UpstreamChanged),
                    stale_since: Some(upstream_mat),
                };
            }
        }
    }

    StalenessResult {
        is_stale: false,
        reason: None,
        stale_since: None,
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement staleness computation"
```

---

**Task 5.3: Code Version Plumbing for Staleness**

**Step 1: Add current_code_version to staleness query interfaces**

```rust
// crates/arco-flow/src/orchestration/controllers/partition_status.rs
pub fn compute_staleness(
    partition: &PartitionStatusRow,
    policy: &FreshnessPolicy,
    current_code_version: Option<&str>,
    now: DateTime<Utc>,
) -> StalenessResult { /* ... */ }

pub fn compute_staleness_with_upstreams(
    partition: &PartitionStatusRow,
    upstreams: &[PartitionStatusRow],
    current_code_version: Option<&str>,
    now: DateTime<Utc>,
) -> StalenessResult { /* ... */ }
```

**Step 2: Thread current_code_version from asset definitions into API query handlers**

```rust
// crates/arco-api/src/routes/orchestration.rs
// Look up asset definition -> code_version; pass to compute_staleness
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): thread code version into staleness queries"
```

---

### Epic 6: Run Key Idempotency + Conflict Detection

**Goal:** Implement run_key deduplication with conflict detection.

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Test: `crates/arco-flow/tests/orchestration_run_key_tests.rs`

---

**Task 6.1: Deterministic Run ID from Run Key**

**Step 1: Write failing test**

```rust
#[test]
fn test_run_id_is_deterministic_from_run_key() {
    let run_key = "sched:daily-etl:1736935200";

    let run_id_1 = run_id_from_run_key(run_key);
    let run_id_2 = run_id_from_run_key(run_key);

    assert_eq!(run_id_1, run_id_2);
    assert!(run_id_1.starts_with("run_"));
    assert_eq!(run_id_1.len(), 30); // "run_" + 26 chars
}

#[test]
fn test_different_run_keys_produce_different_ids() {
    let id1 = run_id_from_run_key("sched:daily-etl:1736935200");
    let id2 = run_id_from_run_key("sched:daily-etl:1736935201");

    assert_ne!(id1, id2);
}
```

**Step 2: Implement run_id_from_run_key**

```rust
// crates/arco-flow/src/orchestration/ids.rs
use sha2::{Sha256, Digest};

/// Generate deterministic run_id from run_key.
///
/// This ensures replay safety: the same run_key always produces
/// the same run_id, regardless of when or how often it's processed.
pub fn run_id_from_run_key(run_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(run_key.as_bytes());
    let hash = hasher.finalize();

    let encoded = base32::encode(
        base32::Alphabet::RFC4648 { padding: false },
        &hash,
    );

    format!("run_{}", &encoded[..26].to_lowercase())
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): add deterministic run_id from run_key"
```

---

**Task 6.2: Run Key Index + Conflict Detection**

**Step 1: Write failing test for conflict detection**

```rust
#[test]
fn test_fold_run_requested_creates_run_key_index() {
    let mut state = FoldState::new_for_test();

    let event = run_requested_event(
        "sched:daily-etl:1736935200",
        "fingerprint_v1",
    );

    state.fold_event(&event);

    // Should create run_key_index entry
    let index = state.run_key_index.get("sched:daily-etl:1736935200");
    assert!(index.is_some());

    let index = index.unwrap();
    assert_eq!(index.request_fingerprint, "fingerprint_v1");
    assert!(!index.run_id.is_empty());
}

#[test]
fn test_fold_run_requested_detects_conflict_on_fingerprint_mismatch() {
    let mut state = FoldState::new_for_test();

    // First request
    let event1 = run_requested_event(
        "sched:daily-etl:1736935200",
        "fingerprint_v1",
    );
    state.fold_event(&event1);

    // Second request with same run_key but different fingerprint
    let event2 = run_requested_event(
        "sched:daily-etl:1736935200",
        "fingerprint_v2", // Different!
    );
    state.fold_event(&event2);

    // Should record conflict
    assert_eq!(state.run_key_conflicts.len(), 1);

    let conflict = &state.run_key_conflicts[0];
    assert_eq!(conflict.run_key, "sched:daily-etl:1736935200");
    assert_eq!(conflict.existing_fingerprint, "fingerprint_v1");
    assert_eq!(conflict.conflicting_fingerprint, "fingerprint_v2");
}

#[test]
fn test_fold_run_requested_is_idempotent_on_same_fingerprint() {
    let mut state = FoldState::new_for_test();

    let event = run_requested_event(
        "sched:daily-etl:1736935200",
        "fingerprint_v1",
    );

    // Process twice
    state.fold_event(&event.clone());
    let runs_after_first = state.runs.len();

    state.fold_event(&event);
    let runs_after_second = state.runs.len();

    // Should not create duplicate run
    assert_eq!(runs_after_first, runs_after_second);
    assert!(state.run_key_conflicts.is_empty());
}
```

**Step 2: Implement fold_run_requested with conflict detection**

```rust
fn fold_run_requested(&mut self, event: &OrchestrationEvent) {
    let OrchestrationEventData::RunRequested {
        run_key,
        request_fingerprint,
        asset_selection,
        partition_selection,
        trigger_source_ref,
        labels,
    } = &event.data else {
        return;
    };

    // Check if run_key already exists
    if let Some(existing) = self.run_key_index.get(run_key) {
        // Check for fingerprint conflict
        if existing.request_fingerprint != *request_fingerprint {
            self.run_key_conflicts.push(RunKeyConflictRow {
                tenant_id: event.tenant_id.clone(),
                workspace_id: event.workspace_id.clone(),
                run_key: run_key.clone(),
                existing_fingerprint: existing.request_fingerprint.clone(),
                conflicting_fingerprint: request_fingerprint.clone(),
                conflicting_event_id: event.event_id.clone(),
                detected_at: event.timestamp,
            });
            return;
        }

        // Same fingerprint = idempotent, skip
        return;
    }

    // Generate deterministic run_id
    let run_id = run_id_from_run_key(run_key);

    // Create run_key_index entry
    self.run_key_index.insert(
        run_key.clone(),
        RunKeyIndexRow {
            tenant_id: event.tenant_id.clone(),
            workspace_id: event.workspace_id.clone(),
            run_key: run_key.clone(),
            run_id: run_id.clone(),
            request_fingerprint: request_fingerprint.clone(),
            created_at: event.timestamp,
            row_version: event.event_id.clone(),
        },
    );

    // Create run
    self.runs.insert(
        run_id.clone(),
        RunRow {
            tenant_id: event.tenant_id.clone(),
            workspace_id: event.workspace_id.clone(),
            run_id: run_id.clone(),
            run_key: Some(run_key.clone()),
            state: RunState::Pending,
            asset_selection: asset_selection.clone(),
            partition_selection: partition_selection.clone(),
            trigger_source_ref: Some(trigger_source_ref.clone()),
            labels: labels.clone(),
            created_at: event.timestamp,
            row_version: event.event_id.clone(),
            ..Default::default()
        },
    );

    // Correlate back to source (tick/chunk)
    self.correlate_run_to_source(run_key, &run_id, trigger_source_ref);
}

fn correlate_run_to_source(&mut self, run_key: &str, run_id: &str, source: &SourceRef) {
    match source {
        SourceRef::Schedule { tick_id, .. } => {
            if let Some(tick) = self.schedule_ticks.get_mut(tick_id) {
                tick.run_id = Some(run_id.to_string());
            }
        }
        SourceRef::Backfill { chunk_id, .. } => {
            if let Some(chunk) = self.backfill_chunks.get_mut(chunk_id) {
                chunk.run_id = Some(run_id.to_string());
            }
        }
        _ => {}
    }
}
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): implement run_key index with conflict detection"
```

---

**Task 6.3: Align Ledger Idempotency Enforcement**

**Step 1: Add test to ensure RunRequested conflicts are not deduped**

```rust
#[test]
fn test_run_requested_conflicts_are_visible_to_fold() {
    // Append two RunRequested events with same run_key but different fingerprints.
    // Ensure both reach compactor so conflict detection can fire.
}
```

**Step 2: Update ledger append/dedup layer to use new RunRequested idempotency key**

```rust
// crates/arco-flow/src/orchestration/ledger.rs (or writer)
// Idempotency should be event-specific: RunRequested uses run_key + fingerprint hash
```

**Step 3: Commit**

```bash
git commit -m "feat(orchestration): align ledger idempotency with run_key conflict detection"
```

---

### Epic 7: API Routes + CLI

**Goal:** Implement REST API and CLI commands.

**Files:**
- Create: `crates/arco-api/src/routes/orchestration.rs`
- Modify: `crates/arco-api/src/routes/mod.rs`
- Test: `crates/arco-api/tests/orchestration_api_tests.rs`

---

**Task 7.1: API Path Convention + Idempotency Header**

**Step 1: Define API routes structure**

```rust
// crates/arco-api/src/routes/orchestration.rs

// Path convention: /api/v1/orchestration/...
pub fn routes() -> Router {
    Router::new()
        // Schedules
        .route("/schedules", post(create_schedule).get(list_schedules))
        .route("/schedules/:schedule_id", get(get_schedule))
        .route("/schedules/:schedule_id/ticks", get(list_schedule_ticks))
        .route("/schedules/:schedule_id/trigger", post(trigger_schedule))
        .route("/schedules/:schedule_id/pause", post(pause_schedule))
        .route("/schedules/:schedule_id/resume", post(resume_schedule))

        // Sensors
        .route("/sensors", post(create_sensor).get(list_sensors))
        .route("/sensors/:sensor_id", get(get_sensor))
        .route("/sensors/:sensor_id/evals", get(list_sensor_evals))
        .route("/sensors/:sensor_id/evaluate", post(evaluate_sensor))
        .route("/sensors/:sensor_id/pause", post(pause_sensor))
        .route("/sensors/:sensor_id/resume", post(resume_sensor))
        .route("/sensors/:sensor_id/reset-cursor", post(reset_sensor_cursor))

        // Backfills
        .route("/backfills", post(create_backfill).get(list_backfills))
        .route("/backfills/preview", post(preview_backfill))
        .route("/backfills/:backfill_id", get(get_backfill))
        .route("/backfills/:backfill_id/chunks", get(list_backfill_chunks))
        .route("/backfills/:backfill_id/pause", post(pause_backfill))
        .route("/backfills/:backfill_id/resume", post(resume_backfill))
        .route("/backfills/:backfill_id/cancel", post(cancel_backfill))
        .route("/backfills/:backfill_id/retry-failed", post(retry_failed_backfill))

        // Partitions
        .route("/partitions", get(list_partitions))
        .route("/partitions/stale", get(list_stale_partitions))
        .route("/assets/:asset_key/partitions", get(list_asset_partitions))
        .route("/assets/:asset_key/partitions/summary", get(get_asset_partition_summary))

        // Conflicts
        .route("/conflicts", get(list_conflicts))
}
```

Note: schedule create/update payloads should include `timezone` and `catchup_window_minutes` (default to `UTC`/`1440`).

**Step 2: Implement Idempotency-Key header handling**

```rust
/// Extract idempotency key from header or body.
pub async fn extract_idempotency_key(
    headers: &HeaderMap,
    body: &CreateRequest,
) -> Option<String> {
    // Prefer header
    if let Some(key) = headers.get("Idempotency-Key") {
        return key.to_str().ok().map(String::from);
    }

    // Fall back to body field
    body.client_request_id.clone()
}

/// Create backfill with idempotency.
pub async fn create_backfill(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateBackfillRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let idempotency_key = extract_idempotency_key(&headers, &req)
        .ok_or(ApiError::MissingIdempotencyKey)?;

    // Check for existing request with same idempotency key
    if let Some(existing) = state.idempotency_store.get(&idempotency_key).await? {
        return Ok((StatusCode::OK, Json(existing)));
    }

    // Create backfill
    let event = state.backfill_controller.create(
        &req.backfill_id.unwrap_or_else(|| Ulid::new().to_string()),
        &state.tenant_id,
        &state.workspace_id,
        &req.asset_selection,
        &req.partition_start,
        &req.partition_end,
        req.chunk_size.unwrap_or(10),
        req.max_concurrent_runs.unwrap_or(2),
        &idempotency_key,
    );

    let event_id = event.event_id.clone();
    state.ledger_writer.append(event).await?;

    // Store idempotency response
    let response = CreateBackfillResponse {
        backfill_id: req.backfill_id.clone().unwrap_or_else(|| event_id.clone()),
        accepted_event_id: event_id,
        accepted_at: Utc::now(),
    };

    state.idempotency_store.put(&idempotency_key, &response).await?;

    Ok((StatusCode::ACCEPTED, Json(response)))
}
```

**Step 3: Commit**

```bash
git commit -m "feat(api): add orchestration routes with idempotency"
```

---

**Task 7.2: Pagination + Filters on History Endpoints**

**Step 1: Implement pagination for tick history**

```rust
#[derive(Debug, Deserialize)]
pub struct ListTicksQuery {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub status: Option<String>,
}

pub async fn list_schedule_ticks(
    State(state): State<AppState>,
    Path(schedule_id): Path<String>,
    Query(query): Query<ListTicksQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = query.limit.unwrap_or(50).min(100);

    let ticks = state
        .projection_reader
        .list_schedule_ticks(
            &schedule_id,
            query.since,
            query.until,
            query.status.as_deref(),
            query.cursor.as_deref(),
            limit + 1, // Fetch one extra for next_cursor
        )
        .await?;

    let has_more = ticks.len() > limit as usize;
    let ticks: Vec<_> = ticks.into_iter().take(limit as usize).collect();
    let next_cursor = if has_more {
        ticks.last().map(|t| t.tick_id.clone())
    } else {
        None
    };

    Ok(Json(ListTicksResponse {
        ticks,
        next_cursor,
    }))
}
```

**Step 2: Commit**

```bash
git commit -m "feat(api): add pagination and filters to history endpoints"
```

---

**Task 7.3: Accept-Event-ID Response Pattern**

**Step 1: Implement accepted response with event handle**

```rust
#[derive(Debug, Serialize)]
pub struct AcceptedResponse {
    pub message: String,
    pub accepted_event_id: String,
    pub accepted_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
}

pub async fn pause_backfill(
    State(state): State<AppState>,
    Path(backfill_id): Path<String>,
    Query(query): Query<MutationQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // Read current state
    let backfill = state.projection_reader.get_backfill(&backfill_id).await?
        .ok_or(ApiError::NotFound)?;

    // Validate state transition
    if backfill.state != BackfillState::Running {
        return Err(ApiError::InvalidStateTransition {
            current: backfill.state,
            requested: BackfillState::Paused,
        });
    }

    // Check expected_version if provided
    if let Some(expected) = query.expected_state_version {
        if backfill.state_version != expected {
            return Err(ApiError::VersionConflict {
                expected,
                actual: backfill.state_version,
            });
        }
    }

    // Emit event
    let event = state.backfill_controller.pause(
        &backfill_id,
        &backfill,
        &state.tenant_id,
        &state.workspace_id,
    );

    let event_id = event.event_id.clone();
    state.ledger_writer.append(event).await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(AcceptedResponse {
            message: "Pause requested; will take effect within ~5 seconds".into(),
            accepted_event_id: event_id,
            accepted_at: Utc::now(),
            correlation_id: None,
        }),
    ))
}
```

**Step 2: Implement wait-for-event endpoint**

```rust
#[derive(Debug, Deserialize)]
pub struct WaitQuery {
    pub wait_for_event_id: Option<String>,
    pub timeout_ms: Option<u64>,
    pub fresh: Option<bool>,
}

pub async fn get_backfill(
    State(state): State<AppState>,
    Path(backfill_id): Path<String>,
    Query(query): Query<WaitQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // Wait for event if requested
    if let Some(event_id) = &query.wait_for_event_id {
        let timeout = Duration::from_millis(query.timeout_ms.unwrap_or(5000));
        state.watermark_waiter.wait_for_event(event_id, timeout).await?;
    }

    // Fresh read if requested
    let backfill = if query.fresh.unwrap_or(false) {
        state.projection_reader.get_backfill_fresh(&backfill_id).await?
    } else {
        state.projection_reader.get_backfill(&backfill_id).await?
    };

    backfill
        .map(|b| Json(b))
        .ok_or(ApiError::NotFound)
}
```

**Step 3: Commit**

```bash
git commit -m "feat(api): add accepted-event-id and wait-for-event pattern"
```

---

**Task 7.4: CLI Commands**

**Step 1: Implement servo backfill commands**

```python
# python/arco/src/servo/cli/commands/backfill.py
import click
from servo.client import ServoClient

@click.group()
def backfill():
    """Manage backfills."""
    pass

@backfill.command()
@click.argument("asset_key")
@click.option("--start", required=True, help="Partition range start")
@click.option("--end", required=True, help="Partition range end")
@click.option("--chunk-size", default=10, help="Partitions per chunk")
@click.option("--concurrency", default=2, help="Max concurrent runs")
@click.option("--wait", is_flag=True, help="Wait for completion")
@click.option("--json", "output_json", is_flag=True, help="JSON output")
def create(asset_key, start, end, chunk_size, concurrency, wait, output_json):
    """Create a backfill."""
    client = ServoClient.from_config()

    response = client.create_backfill(
        asset_selection=[asset_key],
        partition_start=start,
        partition_end=end,
        chunk_size=chunk_size,
        max_concurrent_runs=concurrency,
    )

    if output_json:
        click.echo(json.dumps(response))
    else:
        click.echo(f"Backfill created: {response['backfill_id']}")
        click.echo(f"Event ID: {response['accepted_event_id']}")

    if wait:
        click.echo("Waiting for completion...")
        client.wait_for_backfill(response['backfill_id'])
        click.echo("Backfill completed.")

@backfill.command()
@click.argument("asset_key")
@click.option("--start", required=True)
@click.option("--end", required=True)
@click.option("--chunk-size", default=10)
def preview(asset_key, start, end, chunk_size):
    """Preview a backfill before creating it."""
    client = ServoClient.from_config()

    response = client.preview_backfill(
        asset_selection=[asset_key],
        partition_start=start,
        partition_end=end,
        chunk_size=chunk_size,
    )

    click.echo(f"Total partitions: {response['total_partitions']}")
    click.echo(f"Total chunks: {response['total_chunks']}")
    click.echo(f"Estimated runs: {response['estimated_runs']}")
    click.echo(f"\nFirst chunk ({len(response['first_chunk_partitions'])} partitions):")
    for p in response['first_chunk_partitions'][:5]:
        click.echo(f"  - {p}")
    if len(response['first_chunk_partitions']) > 5:
        click.echo(f"  ... and {len(response['first_chunk_partitions']) - 5} more")

@backfill.command()
@click.argument("backfill_id")
def pause(backfill_id):
    """Pause a running backfill."""
    client = ServoClient.from_config()
    response = client.pause_backfill(backfill_id)
    click.echo(f"Pause requested: {response['accepted_event_id']}")

@backfill.command()
@click.argument("backfill_id")
def resume(backfill_id):
    """Resume a paused backfill."""
    client = ServoClient.from_config()
    response = client.resume_backfill(backfill_id)
    click.echo(f"Resume requested: {response['accepted_event_id']}")

@backfill.command()
@click.argument("backfill_id")
def cancel(backfill_id):
    """Cancel a backfill."""
    client = ServoClient.from_config()
    response = client.cancel_backfill(backfill_id)
    click.echo(f"Cancel requested: {response['accepted_event_id']}")

@backfill.command("retry-failed")
@click.argument("backfill_id")
def retry_failed(backfill_id):
    """Create a new backfill for failed partitions only."""
    client = ServoClient.from_config()
    response = client.retry_failed_backfill(backfill_id)
    click.echo(f"Retry backfill created: {response['backfill_id']}")
```

**Step 2: Commit**

```bash
git commit -m "feat(cli): add backfill CLI commands"
```

---

**Task 7.5: Schedule API/CLI Time Semantics**

**Step 1: Extend schedule create/update payloads**

```rust
// crates/arco-api/src/routes/orchestration.rs
// Add timezone + catchup_window_minutes fields with defaults (UTC/1440)
```

**Step 2: Update schedule CLI flags**

```python
# python/arco/src/servo/cli/commands/schedule.py
# Add --timezone and --catchup-window-minutes flags
```

**Step 3: Commit**

```bash
git commit -m "feat(api,cli): add schedule timezone and catchup window"
```

---

**Task 7.6: Backfill API/CLI Partition Selector**

**Step 1: Update API request/response schemas**

```rust
// crates/arco-api/src/routes/orchestration.rs
// CreateBackfillRequest: accept partition_selector (range/explicit)
// CreateBackfillResponse: include total_partitions + selector echo
```

**Step 2: Update CLI to accept explicit partition lists**

```python
# python/arco/src/servo/cli/commands/backfill.py
# Add --partitions (comma-separated) to set PartitionSelector::Explicit
```

**Step 3: Commit**

```bash
git commit -m "feat(api,cli): add backfill partition selector support"
```

---

## Correctness Test Matrix

### Layer 2 Invariants

| ID | Invariant | Description | Test |
|----|-----------|-------------|------|
| L2-INV-1 | Schedule idempotency | Same tick_id processed twice = no-op | test_schedule_tick_idempotent |
| L2-INV-2 | Sensor idempotency (push) | Same message_id processed twice = no-op | test_push_sensor_idempotent |
| L2-INV-3 | Sensor idempotency (poll) | Same cursor_before at same epoch = no-op | test_poll_sensor_idempotent |
| L2-INV-4 | Run key idempotency | Same run_key + fingerprint = same run_id | test_run_key_idempotent |
| L2-INV-5 | Run key conflict | Same run_key + different fingerprint = conflict | test_run_key_conflict |
| L2-INV-6 | Backfill state version | State change with wrong version = rejected | test_backfill_version_conflict |
| L2-INV-7 | Chunk idempotency | Same chunk_id planned twice = no-op | test_chunk_planning_idempotent |
| L2-INV-8 | Partition status atomic | Concurrent materializations resolve correctly | test_partition_status_concurrent |
| L2-INV-9 | Deterministic run_id | run_id_from_run_key is pure function | test_run_id_deterministic |
| L2-INV-10 | Correlation integrity | tick → run_key → run_id chain maintained | test_correlation_chain |
| L2-INV-11 | Schedule snapshot | Tick uses embedded asset_selection/definition_version | test_schedule_tick_uses_snapshot |
| L2-INV-12 | Sensor eval CAS | Stale expected_state_version is dropped | test_sensor_eval_version_mismatch |

---

## Module Structure

```
crates/arco-flow/src/orchestration/
├── mod.rs
├── events/
│   ├── mod.rs
│   ├── automation_events.rs   # Schedule, Sensor events
│   └── backfill_events.rs     # Backfill events
├── ids.rs                     # run_id_from_run_key
├── ledger.rs
├── run_key.rs
├── callbacks/
├── compactor/
│   ├── mod.rs
│   ├── fold.rs                # Extended with L2 fold handlers
│   ├── manifest.rs
│   ├── parquet_util.rs
│   └── service.rs
└── controllers/
    ├── mod.rs
    ├── dispatch.rs
    ├── dispatcher.rs
    ├── ready_dispatch.rs
    ├── timer.rs
    ├── timer_handlers.rs
    ├── anti_entropy.rs
    ├── schedule.rs            # NEW: Schedule controller
    ├── schedule_evaluator.rs  # NEW: Cron evaluation
    ├── sensor.rs              # NEW: Sensor handlers
    ├── backfill.rs            # NEW: Backfill controller
    ├── partition_resolver.rs  # NEW: Partition enumeration adapter
    └── partition_status.rs    # NEW: Staleness computation

crates/arco-api/src/routes/
├── mod.rs
├── orchestration.rs           # NEW: Automation API routes
└── tasks.rs                   # Existing

python/arco/src/servo/cli/commands/
├── backfill.py                # NEW: Backfill CLI
├── schedule.py                # NEW: Schedule CLI
└── sensor.py                  # NEW: Sensor CLI
```

---

## Summary

This execution plan implements Layer 2 Dagster parity features:

| Epic | Goal | Key Deliverables |
|------|------|------------------|
| 0 | Lock decisions | ADR-024/025/026 |
| 1 | Event foundation | Schedule, Sensor, Backfill events + schemas |
| 2 | Schedule controller | Cron evaluation, tick history, catch-up |
| 3 | Sensor controller | Push (Pub/Sub) + Poll sensors, cursor |
| 4 | Backfill controller | Run-per-chunk, pause/resume/cancel, retry-failed |
| 5 | Partition status | Materialization tracking, staleness queries |
| 6 | Run key idempotency | Deterministic run_id, conflict detection |
| 7 | API + CLI | REST routes, pagination, --wait, --json |

**Key Design Decisions:**

1. **Hybrid automation** - Cloud Tasks + Pub/Sub + polling
2. **Run-per-chunk backfill** - Granular progress, bounded blast radius (range/spec resolved per chunk)
3. **Deterministic run_id** - Derived from run_key hash (replay-safe)
4. **RunRequested idempotency** - run_key + fingerprint hash preserves conflict detection
5. **Schedule snapshotting** - Schedule ticks embed definition version + asset selection
6. **State version gating** - Monotonic version for backfills and sensors
7. **Fresh reads** - Snapshot + bounded L0 tail (max 5k events, 60s, 200ms)
8. **API idempotency** - Idempotency-Key header + client_request_id
9. **Schedule time semantics** - Explicit timezone + catchup window

**Acceptance Criteria per M2:**

- [ ] Schedule fires and creates run at expected time
- [ ] Sensor with cursor evaluates correctly
- [ ] Backfill creates, progresses, handles pause/resume/cancel
- [ ] Retry-failed creates new backfill with only failed partitions
- [ ] Partition status queryable via API
- [ ] Stale partitions identifiable with reason
- [ ] run_key deduplication works (same key = same run_id)
- [ ] run_key conflicts detected and recorded
- [ ] All history endpoints have pagination
- [ ] CLI commands work end-to-end

---

## Execution Options

**Plan complete and saved to `docs/plans/2025-12-22-layer2-automation-execution-plan.md`.**

Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
