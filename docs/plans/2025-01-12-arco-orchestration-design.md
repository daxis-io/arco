# Servo Orchestration Design

> Arco Orchestration Component - Complete Technical Design

**Status:** Approved
**Date:** 2025-01-12
**Authors:** Architecture Team

---

## Executive Summary

Servo is the orchestration component of the Arco unified data platform. It provides:

- **Asset-centric DAG execution** with partition-aware scheduling
- **Serverless architecture** using Cloud Run + Cloud Tasks
- **Multi-tenant isolation** with per-tenant quotas and fairness
- **Event-sourced state** for audit, replay, and debugging
- **Pythonic SDK** for defining assets with type-safe dependencies

This document captures the complete technical design validated through collaborative brainstorming.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Crate Structure](#2-crate-structure)
3. [Domain Types (Protobuf)](#3-domain-types-protobuf)
4. [Execution Plan & State Machine](#4-execution-plan--state-machine)
5. [Service Interface (gRPC API)](#5-service-interface-grpc-api)
6. [Event Sourcing & Storage](#6-event-sourcing--storage)
7. [Developer Experience (SDK & CLI)](#7-developer-experience-sdk--cli)
8. [Key Decisions](#8-key-decisions)
9. [Implementation Roadmap](#9-implementation-roadmap)

---

## 1. Architecture Overview

### 1.1 Hybrid Architecture

Servo uses a **Hybrid Architecture** (Option D from design exploration):

| Layer | Language | Responsibility |
|-------|----------|----------------|
| **Control Plane** | Rust | Planner, scheduler, state machine, API |
| **Data Plane** | Python | User code execution in serverless workers |

**Rationale:**
- Rust provides performance, safety, and small container images for the control plane
- Python provides ecosystem access (pandas, polars, dbt) for user code
- Clear boundary: Rust orchestrates, Python executes

### 1.2 Protocol Format

**Protobuf as canonical IDL + JSON as default operational format**

- Single `.proto` source generates both Rust (prost) and Python (betterproto) types
- JSON encoding for debuggability (can inspect payloads in logs/queues)
- Path to binary Protobuf for performance-critical paths without migration

### 1.3 Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Control Plane (Rust)                      │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │   Deploy    │  │  Execution  │  │    Asset    │   gRPC API   │
│  │   Service   │  │   Service   │  │   Service   │              │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘              │
│         │                │                │                      │
│  ┌──────▼────────────────▼────────────────▼──────┐              │
│  │              Core Domain Logic                 │              │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────────┐   │              │
│  │  │ Planner │  │Scheduler│  │ State Machine│   │              │
│  │  └─────────┘  └─────────┘  └─────────────┘   │              │
│  └───────────────────────┬───────────────────────┘              │
│                          │                                       │
│  ┌───────────────────────▼───────────────────────┐              │
│  │              Event Store (Postgres)            │              │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────────┐   │              │
│  │  │ Events  │  │  Runs   │  │   Tasks     │   │              │
│  │  └─────────┘  └─────────┘  └─────────────┘   │              │
│  └───────────────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────────┘
                              │
                    Cloud Tasks Queue
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Data Plane (Python)                         │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   Serverless Workers                     │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │    │
│  │  │  Worker 1   │  │  Worker 2   │  │  Worker N   │     │    │
│  │  │  (Cloud Run)│  │  (Cloud Run)│  │  (Cloud Run)│     │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘     │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Crate Structure

```
arco/
├── proto/servo/v1/              # Protobuf source of truth
│   ├── common.proto             # IDs, timestamps, metadata
│   ├── task.proto               # TaskEnvelope, TaskRequest, TaskResult, TaskEvent
│   ├── asset.proto              # AssetDefinition, dependencies
│   ├── plan.proto               # ExecutionPlan, PlannedTask
│   ├── state.proto              # TaskState, RunState, transitions
│   ├── event.proto              # Event sourcing events
│   └── service.proto            # gRPC service definitions
│
├── crates/
│   ├── servo-proto/             # Generated Rust types (prost)
│   │   ├── build.rs             # prost-build configuration
│   │   └── src/lib.rs           # Re-exports generated types
│   │
│   ├── servo-core/              # Domain logic (no I/O)
│   │   ├── src/
│   │   │   ├── manifest.rs      # Manifest validation
│   │   │   ├── graph.rs         # DAG construction
│   │   │   ├── partition.rs     # Partition resolution
│   │   │   └── check.rs         # Check evaluation
│   │   └── Cargo.toml
│   │
│   ├── servo-planner/           # Deterministic planning
│   │   ├── src/
│   │   │   ├── planner.rs       # Plan generation
│   │   │   ├── resolver.rs      # Dependency resolution
│   │   │   └── snapshot.rs      # Metadata snapshot
│   │   └── Cargo.toml
│   │
│   ├── servo-runtime/           # Execution engine
│   │   ├── src/
│   │   │   ├── scheduler.rs     # Task scheduling loop
│   │   │   ├── dispatcher.rs    # Cloud Tasks dispatch
│   │   │   ├── state.rs         # State machine
│   │   │   └── quota.rs         # Quota management
│   │   └── Cargo.toml
│   │
│   ├── servo-store/             # Event store + projections
│   │   ├── src/
│   │   │   ├── events.rs        # Event append
│   │   │   ├── projections.rs   # Projection updates
│   │   │   ├── streams.rs       # Stream management
│   │   │   └── outbox.rs        # Outbox worker
│   │   └── Cargo.toml
│   │
│   └── servo-api/               # gRPC service implementations
│       ├── src/
│       │   ├── deploy.rs        # DeployService
│       │   ├── execution.rs     # ExecutionService
│       │   └── asset.rs         # AssetService
│       └── Cargo.toml
│
└── python/arco/
    ├── pyproject.toml
    └── servo/
        ├── __init__.py          # Public API
        ├── _proto/              # Generated Python types (betterproto)
        ├── asset.py             # @asset decorator
        ├── check.py             # @check decorator
        ├── types.py             # Public types
        ├── context.py           # AssetContext
        ├── manifest.py          # Manifest generation
        ├── cli/                 # CLI commands
        │   └── main.py
        └── worker/              # Worker runtime
            ├── executor.py
            └── callbacks.py
```

---

## 3. Domain Types (Protobuf)

### 3.1 common.proto

```protobuf
syntax = "proto3";
package servo.v1;

import "google/protobuf/timestamp.proto";
import "google/protobuf/struct.proto";

// Strongly-typed IDs
message TenantId { string value = 1; }
message AssetId { string value = 1; }
message RunId { string value = 1; }
message TaskId { string value = 1; }
message PlanId { string value = 1; }
message CodeVersionId { string value = 1; }
message WorkspaceId { string value = 1; }
message MaterializationId { string value = 1; }
message IdempotencyKey { string value = 1; }
message MetadataSnapshotId { string value = 1; }
message EventId { string value = 1; }

// Multi-dimensional partition key
message PartitionKey {
  map<string, string> dimensions = 1;
}

// Asset identifier (namespace + name)
message AssetKey {
  string namespace = 1;
  string name = 2;
}

// Extensible metadata
message Metadata {
  google.protobuf.Struct fields = 1;
}

enum Severity {
  SEVERITY_UNSPECIFIED = 0;
  SEVERITY_INFO = 1;
  SEVERITY_WARNING = 2;
  SEVERITY_ERROR = 3;
  SEVERITY_CRITICAL = 4;
}
```

### 3.2 task.proto (Worker Contract)

```protobuf
syntax = "proto3";
package servo.v1;

import "google/protobuf/timestamp.proto";
import "google/protobuf/duration.proto";
import "servo/v1/common.proto";

// TaskEnvelope wraps all worker communication
message TaskEnvelope {
  string contract_version = 1;
  AuthMaterial auth = 2;
  TraceContext trace = 3;

  // Routing (duplicated for queue routing without parsing)
  TenantId tenant_id = 4;
  RunId run_id = 5;
  TaskId task_id = 6;

  // Typed payload (enables JSON debugging)
  oneof body {
    TaskRequest request = 20;
    TaskResult result = 21;
    TaskEvent event = 22;
  }
}

message AuthMaterial {
  string key_id = 1;
  bytes signature = 2;  // HMAC-SHA256
  google.protobuf.Timestamp expires_at = 3;
  string nonce = 4;
}

message TraceContext {
  string traceparent = 1;  // W3C
  string tracestate = 2;
  string baggage = 3;
}

message TaskRequest {
  TenantId tenant_id = 1;
  WorkspaceId workspace_id = 2;
  RunId run_id = 3;
  TaskId task_id = 4;
  PlanId plan_id = 5;

  AssetKey asset_key = 10;
  PartitionKey partition_key = 11;

  CodeVersionId code_version_id = 20;
  CodeArtifact artifact = 21;

  uint32 attempt = 30;
  uint32 max_attempts = 31;

  repeated InputRef inputs = 40;
  OutputRef output = 41;
  repeated CheckSpec checks = 50;

  google.protobuf.Duration timeout = 60;
  IdempotencyKey idempotency_key = 70;
  WorkerCallbacks callbacks = 80;

  map<string, string> variables = 90;
  map<string, SecretRef> secrets = 91;

  Metadata metadata = 100;
}

message WorkerCallbacks {
  string status_url = 1;    // Presigned URL for status updates
  string events_url = 2;    // Presigned URL for streaming events
  string heartbeat_url = 3; // Presigned URL for heartbeats
  google.protobuf.Timestamp urls_expire_at = 4;
}

message TaskResult {
  TaskId task_id = 1;
  RunId run_id = 2;
  TenantId tenant_id = 3;
  IdempotencyKey idempotency_key = 4;
  uint32 attempt = 5;

  TaskOutcome outcome = 10;

  google.protobuf.Timestamp started_at = 20;
  google.protobuf.Timestamp completed_at = 21;

  MaterializationInfo materialization = 30;
  repeated CheckResult check_results = 40;
  GatingDecision gating_decision = 41;
  TaskError error = 50;
  TaskMetrics metrics = 60;

  Metadata metadata = 100;
}

enum TaskOutcome {
  TASK_OUTCOME_UNSPECIFIED = 0;
  TASK_OUTCOME_SUCCEEDED = 1;
  TASK_OUTCOME_FAILED = 2;
  TASK_OUTCOME_SKIPPED = 3;
  TASK_OUTCOME_CANCELLED = 4;
  TASK_OUTCOME_TIMED_OUT = 5;
}
```

### 3.3 asset.proto

```protobuf
syntax = "proto3";
package servo.v1;

import "google/protobuf/duration.proto";
import "google/protobuf/timestamp.proto";
import "servo/v1/common.proto";

message AssetManifest {
  string manifest_version = 1;
  TenantId tenant_id = 2;
  WorkspaceId workspace_id = 3;
  CodeVersionId code_version = 4;
  GitContext git = 5;

  repeated AssetDefinition assets = 10;
  repeated ScheduleDefinition schedules = 11;

  google.protobuf.Timestamp deployed_at = 20;
  string deployed_by = 21;
  Metadata metadata = 100;
}

message AssetDefinition {
  AssetKey key = 1;
  AssetId id = 2;
  string description = 3;

  repeated string owners = 4;
  map<string, string> tags = 5;

  PartitionStrategy partitioning = 10;
  repeated AssetDependency dependencies = 11;

  CodeLocation code = 20;
  repeated CheckDefinition checks = 30;
  ExecutionPolicy execution = 40;
  IoConfig io = 50;

  string transform_fingerprint = 60;
  Metadata metadata = 100;
}

message PartitionStrategy {
  repeated PartitionDimension dimensions = 1;
}

message PartitionDimension {
  string name = 1;
  oneof kind {
    TimeDimension time = 10;
    StaticDimension static = 11;
    DynamicDimension dynamic = 12;
    TenantDimension tenant = 13;
  }
}

message AssetDependency {
  AssetKey upstream_asset = 1;
  string parameter_name = 2;
  repeated DimensionMapping mappings = 3;
  bool require_quality = 4;
}

message PartitionMapping {
  oneof strategy {
    IdentityMapping identity = 1;
    AllUpstreamMapping all = 2;
    TimeWindowMapping window = 3;
    LatestMapping latest = 4;
  }
}

message TimeWindowMapping {
  int32 start_offset = 1;  // -7 = 7 periods ago
  int32 end_offset = 2;    // 0 = current
}

message ScheduleDefinition {
  string name = 1;
  repeated AssetKey targets = 2;

  oneof trigger {
    CronTrigger cron = 10;
    EventTrigger event = 11;
  }

  PartitionScope partition_scope = 20;
  Metadata metadata = 100;
}
```

---

## 4. Execution Plan & State Machine

### 4.1 ExecutionPlan

The plan is split into **header** (runtime-generated) and **spec** (deterministic):

```protobuf
message ExecutionPlan {
  PlanHeader header = 1;
  ExecutionPlanSpec spec = 2;
  string fingerprint = 3;  // SHA256 of spec for quick equality
}

message PlanHeader {
  PlanId id = 1;
  RunId run_id = 2;
  TenantId tenant_id = 3;
  WorkspaceId workspace_id = 4;
  google.protobuf.Timestamp created_at = 10;
  string planner_version = 11;
}

message ExecutionPlanSpec {
  uint32 version = 1;

  // Determinism anchors
  CodeVersionId code_version_id = 10;
  MetadataSnapshotId metadata_snapshot_id = 11;

  TriggerContext trigger = 20;
  ExecutionRequest request = 21;

  repeated PlannedTask tasks = 30;

  google.protobuf.Duration timeout = 40;
  int32 max_parallelism = 41;
  FailurePolicy failure_policy = 42;

  PlanEstimates estimates = 50;
}

message PlannedTask {
  TaskId task_id = 1;

  AssetKey asset_key = 10;
  PartitionKey partition_key = 11;
  CodeVersionId code_version_id = 20;
  ResourceHints resources = 21;
  uint32 priority = 22;

  // Graph structure
  repeated TaskId depends_on = 30;
  uint32 stage = 31;  // 0 = roots, 1 = depends on stage 0, etc.

  // External dependencies (sensor pattern)
  repeated ExternalDependency external_dependencies = 40;

  // Pre-resolved inputs
  repeated ResolvedInput inputs = 50;
  string output_uri = 60;
}
```

### 4.2 State Machine

```protobuf
enum RunState {
  RUN_STATE_UNSPECIFIED = 0;
  RUN_STATE_PENDING = 1;
  RUN_STATE_RUNNING = 2;
  RUN_STATE_SUCCEEDED = 3;
  RUN_STATE_FAILED = 4;
  RUN_STATE_CANCELLING = 5;
  RUN_STATE_CANCELLED = 6;
  RUN_STATE_TIMED_OUT = 7;
}

enum TaskState {
  TASK_STATE_UNSPECIFIED = 0;

  // Phase 1: Planning & Waiting
  TASK_STATE_PLANNED = 1;       // Exists in plan, scheduler hasn't evaluated
  TASK_STATE_PENDING = 2;       // Scheduler evaluated, blocked on dependencies

  // Phase 2: Scheduling
  TASK_STATE_READY = 3;         // Dependencies met, waiting for quota
  TASK_STATE_QUEUED = 4;        // Quota acquired, pushed to queue

  // Phase 3: Execution
  TASK_STATE_DISPATCHED = 5;    // Sent to worker, awaiting ack
  TASK_STATE_RUNNING = 6;       // Worker acknowledged, executing

  // Phase 4: Retry
  TASK_STATE_RETRY_WAIT = 11;   // Attempt failed, waiting for backoff

  // Phase 5: Terminal
  TASK_STATE_SUCCEEDED = 7;     // Execution + checks passed
  TASK_STATE_FAILED = 8;        // Terminal failure
  TASK_STATE_SKIPPED = 9;       // Upstream failed
  TASK_STATE_CANCELLED = 10;    // User cancelled
}
```

### 4.3 State Transitions

```
                              ┌──────────────────────────────────────────────┐
                              │                                              │
                              ▼                                              │
┌─────────┐  run starts  ┌─────────┐  deps met   ┌───────┐  quota   ┌────────┐
│ PLANNED │─────────────►│ PENDING │────────────►│ READY │─────────►│ QUEUED │
└─────────┘              └─────────┘             └───────┘          └────────┘
                              │                      │                   │
                         upstream                    │              dispatched
                         failed                      │                   │
                              │                      │                   ▼
                              ▼                      │             ┌────────────┐
                         ┌─────────┐                 │             │ DISPATCHED │
                         │ SKIPPED │                 │             └────────────┘
                         └─────────┘                 │                   │
                              ▲                      │               ack received
                              │                 cancelled               │
                              │                      ▼                   ▼
                              │               ┌───────────┐        ┌─────────┐
                              │               │ CANCELLED │        │ RUNNING │
                              │               └───────────┘        └─────────┘
                              │                      ▲                   │
                              │                  cancelled         succeeded │ failed
                              │                      │                   │       │
                              │                      │                   ▼       ▼
                              │                      │             ┌───────────────────┐
                              │                      └─────────────│    SUCCEEDED /    │
                              │                                    │      FAILED       │
                              │                                    └───────────────────┘
                              │                                           │
                              │       ┌────────────┐                      │ (if retriable)
                              │       │ RETRY_WAIT │◄─────────────────────┘
                              │       └────────────┘
                              │             │ backoff expires
                              │             ▼
                              │         ┌───────┐
                              └─────────│ READY │
                                        └───────┘
```

---

## 5. Service Interface (gRPC API)

### 5.1 DeployService

```protobuf
service DeployService {
  rpc Deploy(DeployRequest) returns (DeployResponse);
  rpc ValidateManifest(ValidateManifestRequest) returns (ValidateManifestResponse);
  rpc GetManifest(GetManifestRequest) returns (GetManifestResponse);
  rpc ListDeployments(ListDeploymentsRequest) returns (ListDeploymentsResponse);
  rpc Rollback(RollbackRequest) returns (RollbackResponse);
}
```

### 5.2 ExecutionService

```protobuf
service ExecutionService {
  rpc TriggerRun(TriggerRunRequest) returns (TriggerRunResponse);
  rpc GetRun(GetRunRequest) returns (GetRunResponse);
  rpc ListRuns(ListRunsRequest) returns (ListRunsResponse);
  rpc CancelRun(CancelRunRequest) returns (CancelRunResponse);
  rpc RetryRun(RetryRunRequest) returns (RetryRunResponse);
  rpc ListTasks(ListTasksRequest) returns (ListTasksResponse);
  rpc GetTask(GetTaskRequest) returns (GetTaskResponse);
  rpc StreamRunEvents(StreamRunEventsRequest) returns (stream RunEvent);
}
```

### 5.3 AssetService

```protobuf
service AssetService {
  rpc GetAsset(GetAssetRequest) returns (GetAssetResponse);
  rpc ListAssets(ListAssetsRequest) returns (ListAssetsResponse);
  rpc GetMaterializationHistory(GetMaterializationHistoryRequest)
      returns (GetMaterializationHistoryResponse);
  rpc GetAssetHealth(GetAssetHealthRequest) returns (GetAssetHealthResponse);
  rpc GetDependencies(GetDependenciesRequest) returns (GetDependenciesResponse);
}
```

---

## 6. Event Sourcing & Storage

### 6.1 Event Envelope

```protobuf
message Event {
  EventId id = 1;
  TenantId tenant_id = 2;
  uint64 sequence = 3;

  google.protobuf.Timestamp timestamp = 4;
  google.protobuf.Timestamp ingested_at = 5;

  string stream_type = 10;
  string stream_id = 11;

  string event_type = 20;
  uint32 schema_version = 21;
  bytes payload = 22;

  EventId causation_id = 30;
  EventId correlation_id = 31;

  string actor_type = 40;
  string actor_id = 41;

  IdempotencyKey idempotency_key = 50;
}
```

### 6.2 Storage Schema (Postgres)

```sql
-- Event store (append-only)
CREATE TABLE events (
    event_id        TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    stream_type     TEXT NOT NULL,
    stream_id       TEXT NOT NULL,
    sequence        BIGINT NOT NULL,
    global_position BIGSERIAL,
    event_time      TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type      TEXT NOT NULL,
    schema_version  INT NOT NULL,
    payload         BYTEA NOT NULL,
    causation_id    TEXT,
    correlation_id  TEXT,
    actor_type      TEXT NOT NULL,
    actor_id        TEXT NOT NULL,
    idempotency_key TEXT,

    UNIQUE (tenant_id, stream_type, stream_id, sequence),
    UNIQUE (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL
);

-- Sequence allocation
CREATE TABLE streams (
    tenant_id     TEXT NOT NULL,
    stream_type   TEXT NOT NULL,
    stream_id     TEXT NOT NULL,
    next_sequence BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (tenant_id, stream_type, stream_id)
);

-- Run projection
CREATE TABLE runs (
    run_id          TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    workspace_id    TEXT NOT NULL,
    plan_id         TEXT NOT NULL,
    state           TEXT NOT NULL,
    tasks_total     INT NOT NULL DEFAULT 0,
    tasks_pending   INT NOT NULL DEFAULT 0,
    tasks_ready     INT NOT NULL DEFAULT 0,
    tasks_running   INT NOT NULL DEFAULT 0,
    tasks_succeeded INT NOT NULL DEFAULT 0,
    tasks_failed    INT NOT NULL DEFAULT 0,
    tasks_skipped   INT NOT NULL DEFAULT 0,
    tasks_cancelled INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    last_activity_at TIMESTAMPTZ NOT NULL,
    last_event_seq  BIGINT NOT NULL
);

-- Task projection
CREATE TABLE tasks (
    task_id         TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES runs(run_id),
    tenant_id       TEXT NOT NULL,
    asset_key       TEXT NOT NULL,
    partition_key   TEXT,
    state           TEXT NOT NULL,
    attempt         INT NOT NULL DEFAULT 1,
    max_attempts    INT NOT NULL DEFAULT 3,
    worker_id       TEXT,
    created_at      TIMESTAMPTZ NOT NULL,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    last_heartbeat  TIMESTAMPTZ,
    heartbeat_timeout_secs INT NOT NULL DEFAULT 300,
    retry_not_before TIMESTAMPTZ,
    outcome         TEXT,
    error_message   TEXT,
    materialization_id TEXT,
    last_event_seq  BIGINT NOT NULL
);

-- Note: High-frequency heartbeat writes use a separate task_heartbeats table
-- (see Part 2, Section 14). tasks.last_heartbeat is updated by projection
-- from that table for query convenience.

-- Outbox for reliable publishing
CREATE TABLE event_outbox (
    event_id        TEXT PRIMARY KEY REFERENCES events(event_id),
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error      TEXT
);

-- Row-Level Security
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
ALTER TABLE runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_events ON events
    USING (tenant_id = current_setting('app.tenant_id'));
CREATE POLICY tenant_isolation_runs ON runs
    USING (tenant_id = current_setting('app.tenant_id'));
CREATE POLICY tenant_isolation_tasks ON tasks
    USING (tenant_id = current_setting('app.tenant_id'));
```

### 6.3 Retention Strategy

| Data | Retention | Strategy |
|------|-----------|----------|
| `events` | 90 days hot | Archive to GCS Parquet |
| `runs` | 90 days | Delete with cascade |
| `tasks` | 90 days | Cascade from runs |
| `asset_states` | Forever | Latest only |
| `materializations` | 1 year | Archive to Parquet |

---

## 7. Developer Experience (SDK & CLI)

### 7.1 Python SDK

```python
from servo import asset, AssetIn, AssetOut, AssetContext
from servo.types import DailyPartition, LastN
from servo.check import row_count, not_null

@asset(
    description="Daily aggregated user metrics",
    partitions=DailyPartition("date"),
    owners=["data-team@example.com"],
    tags={"domain": "analytics", "tier": "gold"},
    checks=[
        row_count(min=1),
        not_null("user_id", "metric_value"),
    ],
)
def user_metrics(
    ctx: AssetContext,
    raw_events: AssetIn["raw_events"],
    user_profiles: AssetIn["user_profiles"],
) -> AssetOut:
    """Process raw events into user metrics."""
    import polars as pl

    events = raw_events.read()
    profiles = user_profiles.read()

    result = (
        events
        .join(profiles, on="user_id")
        .group_by("user_id")
        .agg(pl.sum("value").alias("metric_value"))
    )

    ctx.report_progress(100, "Aggregation complete")
    return ctx.output(result)
```

### 7.2 CLI Commands

```
servo
├── deploy       # Deploy assets to workspace
├── run          # Trigger a run
├── status       # Check run/task status
├── logs         # View task logs
├── assets       # List/inspect assets
├── backfill     # Run backfills
├── dev          # Local development server
├── init         # Initialize new project
├── validate     # Validate assets
└── explain      # Explain asset state
```

### 7.3 Local Development

```bash
# Initialize project
servo init my-project
cd my-project

# Start dev server
servo dev

# Trigger local run
servo run user_metrics -p date=2024-01-15

# Watch status
servo status --watch
```

---

## 8. Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Architecture** | Hybrid (Rust control + Python workers) | Performance + ecosystem access |
| **Protocol** | Protobuf canonical + JSON encoding | Type safety + debuggability |
| **State storage** | Event-sourced Postgres | ACID, exactly-once, rebuildable |
| **Partition mapping MVP** | Identity, TimeWindow, AllUpstream, Latest | Covers 95% of cases |
| **Multi-dimensional partitions** | Yes (including tenant) | Core for multi-tenant SaaS |
| **Schedules** | Separate from assets | Same asset, different schedules per env |
| **Retry handling** | RETRY_WAIT state | Clear distinction from terminal FAILED |
| **Partial failure** | Continue non-dependent tasks | Maximize value, skip collateral damage |
| **Worker callbacks** | Presigned URLs | Serverless-friendly, no worker auth |
| **Event publishing** | Outbox pattern | Guaranteed delivery |
| **Projection rebuild** | Required capability | Schema evolution, bug fixes |
| **Local dev** | Direct execution | Fast iteration, zero cloud |

---

## 9. Implementation Roadmap

### Phase 1: Walking Skeleton

1. **Proto definitions** — Commit `.proto` files
2. **servo-proto** — Generate Rust types with prost
3. **servo-core** — Manifest validation, DAG construction
4. **servo-planner** — Plan generation (unit tests)
5. **Python SDK** — `@asset` decorator, manifest export

**Milestone:** `servo deploy --dry-run` prints valid JSON manifest

### Phase 2: Local Runtime

1. **servo-store** — SQLite event store for local dev
2. **servo-runtime** — Scheduler, state machine
3. **Python worker** — Task execution, callbacks
4. **CLI** — `servo dev`, `servo run`, `servo status`

**Milestone:** End-to-end local execution of multi-asset DAG

### Phase 3: Cloud Runtime

1. **servo-store** — Postgres implementation
2. **servo-api** — gRPC services
3. **Cloud Tasks** — Dispatcher integration
4. **Cloud Run** — Worker container

**Milestone:** Deploy and execute in GCP

### Phase 4: Production Hardening

1. **Quotas** — Per-tenant limits
2. **Quality framework** — Check execution, gating
3. **Backfill service** — Multi-run orchestration
4. **Observability** — Metrics, tracing, alerting

**Milestone:** Production-ready for pilot customers

---

## Appendix A: Complete Proto Files

The complete protobuf definitions are available in `proto/servo/v1/`:

- `common.proto` — Shared types
- `task.proto` — Worker contract
- `asset.proto` — Asset definitions
- `plan.proto` — Execution plans
- `state.proto` — State machine
- `event.proto` — Event sourcing
- `service.proto` — gRPC services

## Appendix B: Python SDK Reference

Full SDK documentation available at `python/arco/servo/README.md`

---

*End of Design Document*
