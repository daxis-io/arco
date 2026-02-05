# Layer 2 Execution Plan: P0 Corrections

> **Status:** These corrections MUST be incorporated before implementation begins.

This document captures critical corrections to the Layer 2 execution plan based on architectural review. These address violations of the platform's ledger+compactor model and correctness hazards.

---

## P0-1: Compactor Must Not Emit Events

### Problem

The current plan has fold logic that emits `RunRequested` from within `fold_schedule_ticked` and `fold_sensor_evaluated`. This violates the event-sourcing architecture:

```rust
// WRONG - fold emits new events
fn fold_schedule_ticked(&mut self, event: &OrchestrationEvent) {
    // ... update projections ...
    self.emit_run_requested(...);  // ❌ Compactor should not emit
}
```

**Why this breaks the system:**
- Replay determinism: Does replay re-emit events?
- Idempotency boundaries span across "derived" events
- Auditability: Who decided to request a run?
- Partial fold progress could generate duplicate events

### Correction

Controllers MUST emit both trigger events AND `RunRequested` atomically in the same ledger segment:

```rust
// CORRECT - controller emits all events atomically
impl ScheduleController {
    pub fn reconcile(&self, ...) -> Vec<OrchestrationEvent> {
        let mut events = Vec::new();

        for tick in due_ticks {
            // Emit tick event
            events.push(OrchestrationEvent::new(
                tenant_id, workspace_id,
                OrchestrationEventData::ScheduleTicked { ... }
            ));

            // ALSO emit RunRequested in same batch
            if tick.status == TickStatus::Triggered {
                events.push(OrchestrationEvent::new(
                    tenant_id, workspace_id,
                    OrchestrationEventData::RunRequested {
                        run_key: tick.run_key.clone(),
                        request_fingerprint: tick.fingerprint.clone(),
                        trigger_source_ref: SourceRef::Schedule { ... },
                        ...
                    }
                ));
            }
        }

        events // Appended atomically to ledger
    }
}
```

Compactor fold becomes pure projection:

```rust
// CORRECT - fold only updates projections
fn fold_schedule_ticked(&mut self, event: &OrchestrationEvent) {
    let OrchestrationEventData::ScheduleTicked { ... } = &event.data else { return };

    // Update schedule_state projection
    self.schedule_state.entry(schedule_id.clone())
        .or_insert_with(|| ScheduleStateRow::new(schedule_id))
        .update_from_tick(scheduled_for, tick_id, run_key, &event.event_id);

    // Create tick history row
    self.schedule_ticks.insert(tick_id.clone(), ScheduleTickRow { ... });

    // NO emit_run_requested() call here!
}

fn fold_run_requested(&mut self, event: &OrchestrationEvent) {
    // Separate fold handler for RunRequested
    // Creates run, updates run_key_index, etc.
}
```

### Updated Tests

Replace tests that expect compactor to emit:

```rust
// WRONG
#[test]
fn test_fold_schedule_ticked_emits_run_requested() { ... }

// CORRECT
#[test]
fn test_schedule_controller_emits_tick_and_run_requested_atomically() {
    let controller = ScheduleController::new();
    let events = controller.reconcile(&definitions, &state, now);

    // Both events in same batch
    assert!(events.iter().any(|e| matches!(&e.data, OrchestrationEventData::ScheduleTicked { .. })));
    assert!(events.iter().any(|e| matches!(&e.data, OrchestrationEventData::RunRequested { .. })));
}

#[test]
fn test_fold_schedule_ticked_updates_projection_only() {
    let mut state = FoldState::new_for_test();
    let event = schedule_ticked_event(...);

    state.fold_event(&event);

    // Verify projection updated
    assert!(state.schedule_ticks.contains_key("tick_id"));

    // Verify NO events emitted (fold returns nothing)
}
```

---

## P0-2: Poll Sensor Requires CAS for Cursor

### Problem

Current poll sensor idempotency key `(poll_epoch, cursor_before_hash)` is insufficient when polls overlap:

- Poll at epoch `t` starts (cursor_before = v1)
- Poll at epoch `t+1` starts before first completes (also reads cursor = v1)
- Both have different idempotency keys (different epochs)
- Both generate runs and try to advance cursor → **duplicate triggers**

### Correction

Add compare-and-swap semantics for cursor advancement:

```rust
// In SensorEvaluated event
OrchestrationEventData::SensorEvaluated {
    sensor_id: String,
    eval_id: String,
    cursor_before: Option<String>,  // What cursor was when eval started
    cursor_after: Option<String>,   // New cursor value
    state_version_before: u32,      // For CAS
    // ...
}
```

```rust
// In compactor fold
fn fold_sensor_evaluated(&mut self, event: &OrchestrationEvent) {
    let OrchestrationEventData::SensorEvaluated {
        sensor_id,
        cursor_before,
        cursor_after,
        state_version_before,
        run_requests,
        ..
    } = &event.data else { return };

    let sensor_state = self.sensor_state.entry(sensor_id.clone())
        .or_insert_with(|| SensorStateRow::new(sensor_id));

    // CAS: Only update if state hasn't changed
    if sensor_state.state_version != *state_version_before {
        // Stale evaluation - log and skip
        log::warn!(
            "Stale sensor evaluation: expected version {}, found {}",
            state_version_before, sensor_state.state_version
        );
        // Mark evaluation as skipped in history
        self.sensor_evals.insert(eval_id.clone(), SensorEvalRow {
            status: SensorEvalStatus::SkippedStaleCursor,
            ..
        });
        return; // Do NOT process run_requests
    }

    // CAS passed - update cursor and version
    sensor_state.cursor = cursor_after.clone();
    sensor_state.state_version += 1;
    sensor_state.last_evaluation_at = Some(event.timestamp);

    // Process run_requests normally (via controller, not fold emission)
}
```

### Updated Controller

```rust
impl PollSensorController {
    pub fn evaluate(&self, sensor_id: &str, state: &SensorStateRow, ...) -> Vec<OrchestrationEvent> {
        let mut events = Vec::new();

        let cursor_before = state.cursor.clone();
        let state_version_before = state.state_version;

        // Evaluate sensor logic
        let (cursor_after, run_requests) = self.evaluate_sensor_logic(sensor_id, &cursor_before);

        // Emit SensorEvaluated (with CAS fields)
        events.push(OrchestrationEvent::new(
            tenant_id, workspace_id,
            OrchestrationEventData::SensorEvaluated {
                sensor_id: sensor_id.to_string(),
                cursor_before,
                cursor_after,
                state_version_before, // For CAS check
                run_requests: run_requests.clone(),
                ..
            }
        ));

        // ALSO emit RunRequested for each run (atomic with SensorEvaluated)
        for req in run_requests {
            events.push(OrchestrationEvent::new(
                tenant_id, workspace_id,
                OrchestrationEventData::RunRequested {
                    run_key: req.run_key,
                    trigger_source_ref: SourceRef::Sensor { sensor_id, eval_id },
                    ..
                }
            ));
        }

        events
    }
}
```

---

## P0-3: Use Stable Opaque IDs

### Problem

The plan uses human-readable names as primary keys:

```rust
schedule_id: "daily-etl"     // ❌ Name as ID
sensor_id: "new-files-sensor" // ❌ Name as ID
```

This breaks when users rename schedules/sensors and corrupts run_key stability.

### Correction

All entity IDs MUST be stable ULIDs. Add separate name fields:

```rust
// Correct schema
pub struct ScheduleDefinitionRow {
    pub tenant_id: String,
    pub workspace_id: String,
    pub schedule_id: String,      // ULID, e.g., "01HQ123ABCDEFGHIJ"
    pub schedule_name: String,    // Display name, can change
    pub cron_expression: String,
    // ...
}
```

Run keys use IDs, not names:

```rust
// Correct run_key format
run_key: "sched:01HQ123ABCDEFGHIJ:1736935200"  // Uses schedule_id
// NOT: "sched:daily-etl:1736935200"
```

### Updated Tests

```rust
#[test]
fn test_schedule_ticked_event_uses_stable_id() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleTicked {
            schedule_id: "01HQ123SCHEDXYZ".into(),  // ✓ ULID
            scheduled_for,
            tick_id: "01HQ123SCHEDXYZ:1736935200".into(),
            run_key: Some("sched:01HQ123SCHEDXYZ:1736935200".into()),
            // ...
        },
    );

    // Verify ID is ULID format (26 chars, alphanumeric)
    assert_eq!(event.data.schedule_id().len(), 15);
    assert!(event.data.schedule_id().chars().all(|c| c.is_alphanumeric()));
}
```

---

## P0-4: Namespace and HMAC Run IDs

### Problem

Current implementation uses raw SHA256:

```rust
fn run_id_from_run_key(run_key: &str) -> String {
    let hash = sha256(run_key.as_bytes());  // ❌ No namespace, predictable
    // ...
}
```

This allows cross-tenant collisions and predictable ID enumeration.

### Correction

Namespace by tenant/workspace and use HMAC:

```rust
/// Generate deterministic run_id from run_key (replay-safe, tenant-scoped).
///
/// Uses HMAC to prevent enumeration attacks while maintaining determinism.
pub fn run_id_from_run_key(
    tenant_id: &str,
    workspace_id: &str,
    run_key: &str,
    tenant_secret: &[u8],  // Per-tenant stable secret
) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    // Namespace the input
    let input = format!("{}:{}:{}", tenant_id, workspace_id, run_key);

    // HMAC instead of raw hash
    let mut mac = HmacSha256::new_from_slice(tenant_secret)
        .expect("HMAC can take key of any size");
    mac.update(input.as_bytes());
    let result = mac.finalize();
    let hash = result.into_bytes();

    // Base32 encode for URL-safe ID
    let encoded = base32::encode(
        base32::Alphabet::RFC4648 { padding: false },
        &hash[..16],  // 128 bits sufficient
    );

    format!("run_{}", &encoded[..26].to_lowercase())
}
```

### Updated Fold Logic

```rust
fn fold_run_requested(&mut self, event: &OrchestrationEvent) {
    let run_key = &event.data.run_key();

    // Generate run_id with tenant context
    let run_id = run_id_from_run_key(
        &event.tenant_id,
        &event.workspace_id,
        run_key,
        &self.tenant_secret,  // Loaded from config
    );

    // ... rest of fold logic
}
```

---

## P0-5: Separate Materialization from Attempt

### Problem

Current partition status conflates data freshness with execution status:

```rust
// WRONG - task failure overwrites prior materialization
if *outcome == TaskOutcome::Failed {
    partition.status = PartitionMaterializationStatus::Failed;
}
```

### Correction

Track separately:

```rust
pub struct PartitionStatusRow {
    pub tenant_id: String,
    pub workspace_id: String,
    pub asset_key: String,
    pub partition_key: String,

    // Data freshness (only updated on SUCCESS)
    pub last_materialization_run_id: Option<String>,
    pub last_materialization_at: Option<DateTime<Utc>>,
    pub last_materialization_code_version: Option<String>,

    // Execution status (updated on every attempt)
    pub last_attempt_run_id: Option<String>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub last_attempt_outcome: Option<TaskOutcome>,

    // Staleness (computed or cached)
    pub stale_since: Option<DateTime<Utc>>,
    pub stale_reason_code: Option<String>,

    pub partition_values: HashMap<String, String>,
    pub row_version: String,
}

impl PartitionStatusRow {
    /// Called when task finishes (any outcome)
    pub fn record_attempt(&mut self, run_id: &str, at: DateTime<Utc>, outcome: TaskOutcome) {
        self.last_attempt_run_id = Some(run_id.to_string());
        self.last_attempt_at = Some(at);
        self.last_attempt_outcome = Some(outcome);
    }

    /// Called ONLY when task succeeds with materialization
    pub fn mark_materialized(&mut self, run_id: &str, at: DateTime<Utc>, code_version: &str) {
        self.last_materialization_run_id = Some(run_id.to_string());
        self.last_materialization_at = Some(at);
        self.last_materialization_code_version = Some(code_version.to_string());
        self.stale_since = None; // Clear staleness on fresh materialization
    }
}
```

### Updated Fold Logic

```rust
fn fold_task_finished(&mut self, event: &OrchestrationEvent) {
    let OrchestrationEventData::TaskFinished {
        run_id, outcome, asset_key, partition_key, ..
    } = &event.data else { return };

    if let (Some(asset_key), Some(partition_key)) = (asset_key, partition_key) {
        let key = (asset_key.clone(), partition_key.clone());
        let status = self.partition_status.entry(key)
            .or_insert_with(|| PartitionStatusRow::new(asset_key, partition_key));

        // Always record the attempt
        status.record_attempt(run_id, event.timestamp, outcome.clone());

        // Only update materialization on success
        if *outcome == TaskOutcome::Succeeded {
            if let Some(code_version) = &event.data.code_version() {
                status.mark_materialized(run_id, event.timestamp, code_version);
            }
        }
        // Failed attempts do NOT overwrite last_materialization_*
    }
}
```

### Updated Status Enum

```rust
// Computed status based on both fields
pub fn compute_status(partition: &PartitionStatusRow) -> PartitionDisplayStatus {
    match (&partition.last_materialization_at, &partition.last_attempt_outcome) {
        (None, _) => PartitionDisplayStatus::NeverMaterialized,
        (Some(_), Some(TaskOutcome::Failed)) if partition.last_attempt_at > partition.last_materialization_at => {
            PartitionDisplayStatus::MaterializedButLastAttemptFailed
        }
        (Some(_), _) if partition.stale_since.is_some() => PartitionDisplayStatus::Stale,
        (Some(_), _) => PartitionDisplayStatus::Materialized,
    }
}
```

---

## P0-6: Compact BackfillCreated Payload

### Problem

Current plan embeds full partition list in `BackfillCreated`:

```rust
OrchestrationEventData::BackfillCreated {
    partition_keys: partitions,  // ❌ Could be millions of partitions
    // ...
}
```

### Correction

Store range/selector in event, resolve partitions per-chunk:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSelector {
    /// For time-based: "2025-01-01" to "2025-12-31"
    pub range_start: Option<String>,
    pub range_end: Option<String>,

    /// For explicit selection
    pub explicit_keys: Option<Vec<String>>,

    /// For filter-based: e.g., {"region": "us-*"}
    pub filters: Option<HashMap<String, String>>,
}

OrchestrationEventData::BackfillCreated {
    backfill_id: String,
    asset_selection: Vec<String>,
    partition_selector: PartitionSelector,  // ✓ Compact
    chunk_size: u32,
    max_concurrent_runs: u32,
    total_partitions: u32,  // Pre-computed count
    // NO partition_keys list
}
```

Partition resolution happens at chunk planning time:

```rust
OrchestrationEventData::BackfillChunkPlanned {
    backfill_id: String,
    chunk_id: String,
    chunk_index: u32,
    partition_keys: Vec<String>,  // ✓ Only this chunk's partitions (bounded by chunk_size)
    run_key: String,
    // ...
}
```

### Updated Controller

```rust
impl BackfillController {
    pub fn create(&self, ..., selector: PartitionSelector, ...) -> OrchestrationEvent {
        // Pre-compute total for progress tracking
        let total_partitions = self.count_partitions(&selector);

        OrchestrationEvent::new(
            tenant_id, workspace_id,
            OrchestrationEventData::BackfillCreated {
                backfill_id: backfill_id.to_string(),
                partition_selector: selector,  // Store selector, not expanded list
                total_partitions,
                // ...
            }
        )
    }

    pub fn plan_next_chunks(&self, backfill: &BackfillRow, planned_count: u32) -> Vec<OrchestrationEvent> {
        let mut events = Vec::new();

        // Resolve partitions for this batch of chunks
        let partitions = self.resolve_partitions(&backfill.partition_selector);
        let start = (planned_count * backfill.chunk_size) as usize;

        for (i, chunk) in partitions[start..].chunks(backfill.chunk_size as usize).enumerate() {
            let chunk_index = planned_count + i as u32;

            events.push(OrchestrationEvent::new(
                tenant_id, workspace_id,
                OrchestrationEventData::BackfillChunkPlanned {
                    chunk_index,
                    partition_keys: chunk.to_vec(),  // Just this chunk
                    // ...
                }
            ));

            // Also emit RunRequested
            events.push(/* RunRequested for this chunk */);

            if events.len() >= backfill.max_concurrent_runs as usize {
                break;
            }
        }

        events
    }
}
```

---

## Should-Fix Improvements

### SF-1: Add SLOs for M2

Document explicit SLOs:

| Metric | Target | Measurement |
|--------|--------|-------------|
| Schedule tick delay | p95 < 5s | `scheduled_for` → `evaluated_at` |
| Pub/Sub sensor latency | p95 < 2s | message publish → `SensorEvaluated` |
| Compaction lag | p95 < 10s | Under normal load |
| Fresh read latency | p95 < 200ms | With bounded L0 tail |

### SF-2: Instrumentation Plan

Required metrics:
- `schedule_ticks_total{status=triggered|skipped|failed}`
- `sensor_evals_total{sensor_type=push|poll, status=triggered|no_data|error|skipped_stale}`
- `backfill_chunks_total{status=planned|running|succeeded|failed}`
- `run_requests_total{source=schedule|sensor|backfill|manual}`
- `run_key_conflicts_total`
- `compaction_lag_seconds`

Required trace propagation:
- Cloud Tasks/Pub/Sub message → controller → ledger append → compactor fold

### SF-3: Schedule Pause Semantics

When schedule is paused:
1. Create `ScheduleTicked` with `status: Skipped { reason: "paused" }` for visible history
2. On resume, do NOT catch up missed ticks while paused (unless `catch_up_on_resume: true`)

### SF-4: Backfill Cancel Semantics

When backfill is cancelled:
1. Stop planning new chunks
2. Let active chunk runs finish (or emit `CancelRun` for each if Layer 1 supports)
3. Mark backfill as `CANCELLED` after all active runs complete or timeout

### SF-5: Durable Idempotency Store

API idempotency store MUST be database-backed (Postgres/Spanner), not in-memory.

---

## Enhanced Test Categories

### Property-Based Tests

```rust
#[test]
fn prop_backfill_state_transitions_always_valid() {
    proptest!(|(from in arb_backfill_state(), to in arb_backfill_state())| {
        let valid = BackfillState::is_valid_transition(from, to);
        if !valid {
            prop_assert!(BackfillState::blocked_transitions().contains(&(from, to)));
        }
    });
}

#[test]
fn prop_cursor_advancement_is_monotonic() {
    proptest!(|(evals in arb_sensor_evals(10))| {
        let mut state = SensorStateRow::default();
        for eval in evals {
            let result = state.apply_eval(&eval);
            if result.is_ok() {
                prop_assert!(state.state_version > eval.state_version_before);
            }
        }
    });
}
```

### Failure-Mode Integration Tests

```rust
#[tokio::test]
async fn test_duplicate_pubsub_delivery_handled() {
    let message = PubSubMessage { message_id: "msg_001", .. };

    // Deliver same message twice
    handler.handle_message(&message).await;
    handler.handle_message(&message).await;

    // Should only create one run
    assert_eq!(runs_created(), 1);
}

#[tokio::test]
async fn test_compactor_crash_mid_fold_recovers() {
    // Start fold with 10 events
    let mut folder = Folder::new();
    folder.fold_events(&events[..5]).await;

    // Simulate crash (drop folder without commit)
    drop(folder);

    // Restart - should replay all 10 events
    let mut folder = Folder::new();
    folder.fold_events(&events).await;
    folder.commit().await;

    // Verify correct final state
    assert_eq!(folder.runs.len(), expected_runs);
}
```

### Load Tests (k6)

```javascript
// Schedule burst after downtime
export function scheduleBurstTest() {
    // Simulate 100 missed schedule ticks
    const ticks = [];
    for (let i = 0; i < 100; i++) {
        ticks.push({
            schedule_id: `sched_${i % 10}`,
            scheduled_for: new Date(Date.now() - i * 60000).toISOString(),
        });
    }

    const response = http.post(`${BASE_URL}/schedules/catchup`, JSON.stringify({ ticks }));

    check(response, {
        'status is 202': (r) => r.status === 202,
        'all ticks accepted': (r) => JSON.parse(r.body).accepted === 100,
    });
}

// Threshold: p95 < 5s for 100 tick catchup
export const options = {
    thresholds: {
        http_req_duration: ['p(95)<5000'],
    },
};
```

---

## Implementation Checklist

Before each task, verify:

- [ ] Controllers emit all related events atomically (no compactor emission)
- [ ] Entity IDs are ULIDs, not names
- [ ] Run IDs use namespaced HMAC
- [ ] Poll sensors include state_version for CAS
- [ ] Partition status separates materialization from attempt
- [ ] Backfill events use selector, not partition list
- [ ] Tests use ULID format IDs in assertions
- [ ] Metrics instrumented per SF-2
