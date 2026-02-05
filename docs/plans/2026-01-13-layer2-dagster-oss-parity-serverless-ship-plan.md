# Layer 2: Dagster OSS Parity (Serverless) — Ship Plan

Date: 2026-01-13  
Scope: `arco-flow` orchestration / Servo automation  

## Goal

Ship Dagster OSS–level *user-facing semantics* for Layer 2 automation on Arco’s serverless architecture (ledger → compactor → Parquet projections), with production-grade durability, wiring, and operator ergonomics.

This document is a revision of the earlier Layer 2 execution plan (`docs/plans/2025-12-22-layer2-automation-execution-plan.md`) to reflect what is already implemented and what remains to ship parity in production.

## Positioning / Scope (locked)

**Dagster parity scope**  
We target parity with Dagster OSS user-facing semantics (ticks, cursors, run keys, backfills, partition status), implemented within Arco’s serverless constraints. We will not promise Dagster+’s exact operational limits; instead we publish explicit Arco platform limits and SLOs as part of “production-ready.”

**Automation run identity**  
For any run with a `run_key`, we standardize on deterministic `run_id` derived from `(tenant_id, workspace_id, run_key)` so retries and replay are inherently safe. We keep the reservation-blob pattern only for endpoints that need strong synchronous “create-or-get + conflict now” semantics.

Code anchors:
- Deterministic IDs: `crates/arco-flow/src/orchestration/ids.rs:95`
- Run-key reservation: `crates/arco-flow/src/orchestration/run_key.rs:1`
- Manual trigger path uses reservation: `crates/arco-api/src/routes/orchestration.rs:1670`

## Current implementation status (avoid duplicating work)

Already present in repo:
- ADRs accepted:
  - `docs/adr/adr-024-schedule-sensor-automation.md`
  - `docs/adr/adr-025-backfill-controller.md`
  - `docs/adr/adr-026-partition-status-tracking.md`
- Event types:
  - `crates/arco-flow/src/orchestration/events/mod.rs`
  - `crates/arco-flow/src/orchestration/events/automation_events.rs`
  - `crates/arco-flow/src/orchestration/events/backfill_events.rs`
- Controllers:
  - Schedules: `crates/arco-flow/src/orchestration/controllers/schedule.rs`
  - Sensors (push + poll + CAS): `crates/arco-flow/src/orchestration/controllers/sensor.rs`
  - Backfills: `crates/arco-flow/src/orchestration/controllers/backfill.rs`
  - Partition staleness computation: `crates/arco-flow/src/orchestration/controllers/partition_status.rs`
- Fold logic (CAS drop behavior, run_key index, etc.): `crates/arco-flow/src/orchestration/compactor/fold.rs`

Known production blockers:
- Layer 2 projections exist in `FoldState` but are not persisted by the micro-compactor (manifest/parquet/service omissions).
- No Cloud Tasks timer callback ingestion service exists (TimerFired not wired end-to-end).
- No service bridges `RunRequested` → actual run creation (`RunTriggered` + `PlanCreated`).
- API rejects compact backfill selectors (`Range`, `Filter`), blocking large backfill claims.

## Platform Limits & SLOs (publishable contract)

These are explicit serverless constraints, not “Dagster+ parity.” Default values are placeholders; choose initial defaults and make configurable.

### Hard limits (initial defaults)
- Max `RunRequested` per sensor eval: TBD (suggest 100)
- Max schedule catch-up ticks per reconcile: TBD (suggest 3–50 per schedule)
- Catch-up window minutes: TBD (suggest 24h default; configurable per schedule)
- Max partitions per backfill chunk: TBD (suggest 1k–10k)
- Max partitions per backfill total (Range/Filter resolved): TBD (suggest 1M+, with streaming resolution)
- Max controller execution budget per invocation: TBD (suggest 20–60s)
- Poll sensor min interval: TBD (suggest ≥ 60s)
- History retention:
  - `schedule_ticks`: TBD (suggest 30–90d)
  - `sensor_evals`: TBD (suggest 7–30d)
  - `run_key_conflicts`: TBD (suggest 180–365d)
  - `idempotency_keys`: TBD (suggest 7–30d)
- Payload sizing:
  - Max Pub/Sub payload size: enforce + validate
  - Max backfill create payload size: enforce

### SLOs (initial targets)
- P95 schedule tick → `RunRequested` appended: TBD (suggest < 5s)
- P95 `RunRequested` → `RunTriggered` appended: TBD (suggest < 10s)
- P95 poll sensor timer → `SensorEvaluated` appended: TBD (suggest < 10s)
- Compaction watermark freshness for controllers: keep `Watermarks` lag under TBD (suggest 30s)

## Security / determinism notes

Deterministic `run_id_from_run_key` uses a stable per-tenant secret. Secret rotation changes IDs and can break idempotency unless handled intentionally.

We must document and enforce:
- Production requires non-empty tenant secret.
- Rotation policy: either “never rotate” (preferred) or “versioned secret with dual-accept window” (complex; avoid unless necessary).

Reference: tenant secret loading / warnings in `crates/arco-flow/src/bin/arco_flow_compactor.rs`.

## Correctness semantics (Dagster OSS parity)

- **Schedules**: deterministic ticks, tick history, configurable catch-up policy, skip vs error semantics.
- **Sensors**:
  - Cursor = progress (poll sensors), durable in Parquet
  - run_key = idempotency for triggered runs
  - Reset cursor does not imply replay unless run_keys change; document explicitly.
- **Backfills**:
  - Run-per-chunk baseline (bounded blast radius)
  - Optional “coalesced/single-run backfill” mode (competitive feature)
  - Pause/resume/cancel/retry-failed semantics
- **Partition status**: separation of materialization vs attempt; staleness computed at query time (optional sweep for precompute)

## ADR alignment tasks (doc/impl mismatch)

ADR-024 implies “ledger rejects duplicate idempotency keys,” but `LedgerWriter` only dedupes by `event_id` write precondition. Today, idempotency-key dedupe is enforced during fold via `FoldState.idempotency_keys`.

We must choose one:
- Option A: update ADR wording to match reality (fold-level dedupe).
- Option B: add optional ingress reservation for `idempotency_key` (recommended for high-volume Pub/Sub cost control).

Either way, define explicit semantics:
- Control plane state transitions are idempotent.
- Delivery is at-least-once.
- Worker side effects must be idempotent.

## Milestones

### M2.0 — Make Layer 2 durable (Parquet + manifest + compactor)

**Outcome**: schedule ticks/state, backfills/chunks, and run_key index/conflicts survive restart and are queryable.

Tasks:
1. Add Parquet schemas + read/write helpers for:
   - `schedule_state`, `schedule_ticks`
   - `backfills`, `backfill_chunks`
   - `run_key_index`, `run_key_conflicts`
2. Extend orchestration manifest table paths to include new tables.
3. Extend micro-compactor load/write paths to read/write these tables.
4. Decide schedule definition storage:
   - Option A (recommended): add `schedule_definitions.parquet` + CRUD events and API.
   - Option B: treat definitions as out-of-band config; document explicitly and accept reduced “UI/API parity.”
5. Add schema evolution notes for new Parquet tables.

Acceptance:
- Cold restart: state loads with non-empty ticks/backfills after events exist.
- Orchestration API list endpoints return stable data across restarts.

### M2.1 — Add serverless timer-ingest runtime (Cloud Tasks callback)

**Outcome**: timers can actually “fire” and drive automation without daemons.

Tasks:
1. Implement a Cloud Run service endpoint to receive Cloud Tasks timer callbacks.
2. On callback:
   - Append `TimerFired` (idempotent on `timer_id`)
   - Route to handlers:
     - existing: `RetryHandler`, `HeartbeatHandler`
     - new: schedule cron tick handler, poll sensor tick handler, (optional) backfill reconcile tick
   - Emit follow-up ledger events (`TimerRequested`, `DispatchRequested`, etc.) as needed.
3. Enforce watermark freshness guardrail (reschedule if compaction lag too high).

Acceptance:
- Retry/heartbeat timers produce expected events and state changes.
- Cron and poll sensor timers produce `ScheduleTicked` / `SensorEvaluated` + `RunRequested`.

### M2.2 — Implement `RunRequested` → `RunTriggered` processor (run creation bridge)

**Outcome**: `RunRequested` results in real runs (like Dagster).

Tasks:
1. Implement a serverless processor that consumes pending `RunRequested` and appends:
   - `RunTriggered` + `PlanCreated` (+ initial task defs) for that run
2. Standardize deterministic run IDs for automation:
   - compute `run_id` from `(tenant_id, workspace_id, run_key, tenant_secret)`
3. Handle conflicts:
   - same run_key with different fingerprint → record conflict row + surface via API/metrics
4. Add admission control hooks (see M2.3): priority, rate limits, pools, backlog.

Acceptance:
- For a schedule tick, a run is created and appears in `runs.parquet`.
- Duplicate `RunRequested` does not create duplicate runs.

### M2.3 — API parity + operator contract

**Outcome**: external UX parity and operational excellence.

Tasks:
1. Fix API mismatches:
   - Backfills: accept `PartitionSelector::Range` (and plan `Filter`), aligning with ADR-025
   - Add backfill pause/resume/cancel/retry-failed endpoints + pagination/filters
   - Schedules: CRUD/enable/disable/trigger (as chosen), tick history pagination
   - Sensors: pause/resume/reset cursor endpoints + documented semantics
2. Publish limits/SLOs document and expose via API/config.
3. Add observability endpoints:
   - backlog depth + oldest age per lane
   - compaction watermark freshness
   - tick/eval latency distributions (or summarized counters)
4. Add dead-letter/quarantine flows:
   - invalid Pub/Sub messages
   - invalid cron expressions (already generate error ticks; add operator surfacing)
   - repeated failing evals/planning
5. Competitive features:
   - optional coalesced/single-run backfill mode
   - priority lanes + fairness + rate limiting + resource pools

Acceptance:
- Backfill create supports compact selectors and large ranges.
- Operator can see backlog + lag + conflicts + retention behavior.

## Testing & Quality Gates

- Add/update invariant tests for:
  - schedule tick idempotency
  - push/poll sensor idempotency & CAS
  - run_key conflict detection
  - backfill state version idempotency
  - correlation chain: tick/eval/chunk → run_key → run_id
- Add end-to-end harness for:
  - timer callback → TimerFired → handler → state changes
  - RunRequested → RunTriggered creation path

Gates:
- `cargo test --workspace`
- `cargo clippy --workspace`
- `buf lint` (if protobuf touched)

## Rollout Plan

- Feature flags for new services (timer ingest, runrequest processor).
- Start with conservative defaults (caps + rate limits).
- Enable for one tenant/workspace first.
- Runbooks:
  - compaction lag incident response
  - backlog growth / cost spike response
  - idempotency conflict investigation

## Open questions (must resolve during implementation)

- Where do schedule definitions live (Parquet vs code config)?
- How do we compute request fingerprints consistently across API/controller paths?
- Do we implement ingress idempotency-key reservation for Pub/Sub (cost) or rely on fold?
