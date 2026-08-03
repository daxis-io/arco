# Planner/Runtime Seam Hardening Design

**Date:** 2026-06-27

**Status:** Proposed design for future planning and execution.

**Audience:** Arco orchestration, planning, runtime, API, and platform reviewers.

## Executive Summary

Arco should harden the seam between declarative planning and runtime
orchestration around one architectural rule:

```text
The planner owns semantic truth.
The runtime owns convergence.
```

The planner decides what work exists and why. It owns user intent, asset graph
semantics, selection resolution, partition expansion, freshness interpretation,
policy normalization, deterministic task identity, plan fingerprinting, and
explanations.

The runtime decides how to make compiled work happen safely, repeatedly, and
observably. It owns event ingestion, projections, readiness, leases, fencing,
capacity checks, retries, dispatch, cancellation, callback validation, and
worker evidence.

The contract between those two modules is an immutable, versioned,
fingerprinted plan artifact bound to a runtime run through `PlanCreated`.
Runtime controllers may inspect compiled plan facts and create attempts for
tasks already declared by the plan. They must not call the compiler, resolve
asset selections, expand partitions, traverse the asset graph, interpret
freshness policy, or synthesize semantic tasks.

This design takes inspiration from Dagster's declarative asset model, Spark's
separation between structured planning and execution, Kubernetes' controller
model, and Tokio-style async runtime discipline. The goal is not to copy any
one system. The goal is to keep Arco's planner and runtime deep, testable, and
independently evolvable.

## Decision

Arco will move semantic lowering out of runtime controllers and into a
planner-owned module interface.

`RunRequested` is declarative intent. A planning/application module consumes the
intent, takes consistent planning snapshots, invokes `PlanCompiler`, persists a
plan artifact, and emits either `PlanCreated` or `PlanRejected`.

`PlanCreated` is the handoff into the runtime. Runtime controllers consume
compiled plan facts and projection state. They do not import planner internals
or call `PlanCompiler`.

## Non-Goals

- Do not redesign the entire orchestration runtime in the first slice.
- Do not introduce a full logical/physical optimizer in the first slice.
- Do not migrate every `PlanCreated` consumer in the first slice.
- Do not make the control store the primary task telemetry ledger.
- Do not change ADR-020 readiness semantics without a separate ADR.
- Do not remove compatibility `TaskDef` support before downstream consumers
  have a plan-reference path.

## Current Pressure Points

The current code already has the concepts, but the seam is leaky.

- `crates/arco-flow/src/plan.rs` describes deterministic, serializable, and
  explainable plans.
- `crates/arco-flow/src/orchestration/mod.rs` defines an event-driven runtime
  with projections and stateless controllers.
- `crates/arco-flow/src/orchestration/controllers/run_request_processor.rs`
  bridges `RunRequested` into `RunTriggered` and `PlanCreated`, and contains
  direct task-building logic.
- `crates/arco-flow/src/orchestration/controllers/run_bridge.rs` imports
  `AssetGraph`, `SelectionOptions`, `build_task_defs_for_selection`, and
  `canonicalize_asset_key`, then lowers `asset_selection` into task
  definitions inside the runtime controller.
- `crates/arco-flow/src/orchestration/selection.rs` mixes selection semantics
  with runtime event `TaskDef` creation.

Those are useful compatibility paths, but they put semantic lowering in the
runtime. That makes controllers answer questions that should belong to the
planner:

- Which assets did this selection mean?
- Which partitions exist?
- Which tasks should exist?
- Why does this task exist?
- Which policies apply to this task?

The first design goal is to move those answers behind a planner-owned module
interface.

## Target Architecture

```text
User / API / Sensor / Schedule / Backfill
        |
        v
RunIntent / RunRequested
        |
        v
Planning Application Boundary
  - snapshot acquisition
  - compilation orchestration
  - plan artifact persistence
  - PlanCreated / PlanRejected emission
        |
        v
Declarative Planning Layer
  - asset graph
  - selection
  - partitions
  - freshness
  - schedules, sensors, backfills
  - policy normalization
        |
        v
PlanCompiler
        |
        v
PlanArtifact + RunPlanBinding
PlanCreated / PlanRejected
        |
        v
Orchestration Runtime
  - event ingestion
  - projections
  - readiness
  - leases and fencing
  - retries
  - dispatch
  - cancellation
  - callbacks
        |
        v
WorkerDispatchEnvelope
TaskStarted / TaskFinished / TaskFailed / TaskRetryScheduled
```

The target dependency direction is:

```text
domain        -> no Arco flow dependencies
declarative   -> domain
plan          -> domain
planning      -> declarative + plan + domain
application   -> planning + plan + domain
orchestration -> plan + domain
```

Forbidden dependency direction:

```text
orchestration -X-> declarative
orchestration -X-> planning
orchestration -X-> planning internals
orchestration -X-> PlanCompiler
orchestration -X-> asset graph traversal
orchestration -X-> partition expansion
orchestration -X-> freshness interpretation
```

If the crate remains monolithic initially, enforce this with import-boundary
tests or a small repository hygiene check. If the seam continues to carry
weight, split it into crates later:

```text
arco-flow-domain
arco-flow-declarative
arco-flow-plan
arco-flow-planning
arco-flow-runtime
```

## Architectural Invariants

These invariants should become design rules, tests, and eventually CI checks.

1. Runtime controllers may inspect the bound compiled plan artifact.
2. Runtime controllers may not call `PlanCompiler`.
3. Runtime controllers may not resolve `AssetSelection`.
4. Runtime controllers may not expand partitions.
5. Runtime controllers may not synthesize semantic tasks.
6. Runtime controllers may only create attempts for tasks already declared by
   the compiled plan.
7. Retries create new `TaskAttempt`s, not new `PlanTask`s.
8. `PlanCreated` is the declarative/runtime handoff.
9. `RunRequested` is declarative intent, not directly executable runtime work.
10. Readiness remains projection-derived unless ADR-020 is explicitly changed.
11. The control store should not become the primary write path for
    high-frequency task telemetry.

## Boundary Ownership

Planner-owned modules own semantic truth:

- user intent normalization;
- asset graph semantics;
- selection and partition semantics;
- freshness and reconciliation semantics;
- policy normalization;
- deterministic task identity;
- plan fingerprinting;
- plan explanations.

Runtime-owned modules own convergence:

- projection reads;
- readiness projection updates;
- leases and fencing;
- capacity checks;
- dispatch;
- retries;
- cancellation;
- callback validation;
- worker evidence;
- run completion.

Application/API modules own orchestration between the two:

- ingesting `RunRequested` or API trigger requests;
- obtaining planning snapshots;
- invoking `PlanCompiler`;
- storing plan artifacts;
- emitting `PlanCreated` or `PlanRejected`.

## Core Artifacts

### RunIntent

`RunIntent` is the caller's declarative request. It says what the caller wants,
not which tasks will exist.

```rust
pub enum RunIntent {
    Materialize {
        selection: AssetSelection,
        partitions: PartitionSelection,
        reason: RunReason,
    },
    Observe {
        selection: AssetSelection,
        partitions: PartitionSelection,
        reason: RunReason,
    },
    Backfill {
        selection: AssetSelection,
        partition_range: PartitionRangeSelection,
        reason: RunReason,
    },
    FreshnessReconcile {
        selection: AssetSelection,
        as_of: LogicalTime,
        reason: RunReason,
    },
}
```

Existing `RunRequested` events can remain the compatibility carrier while the
planner module is introduced. Over time, `RunRequested` should carry a
normalized `RunIntent` or an equivalent stable wire shape.

### PlanArtifact

`PlanArtifact` is the reusable compiler-owned artifact. It is immutable and
fingerprinted. It excludes runtime instance identity.

```rust
pub struct PlanArtifact {
    pub schema_version: PlanSchemaVersion,
    pub plan_fingerprint: PlanFingerprint,
    pub graph_snapshot_id: GraphSnapshotId,
    pub planning_snapshot_token: PlanningSnapshotToken,
    pub compiler_version: CompilerVersion,
    pub source_intent: RunIntentDigest,
    pub tasks: BTreeMap<TaskKey, PlanTask>,
    pub edges: Vec<PlanEdge>,
    pub policies: PlanPolicies,
    pub explanation: PlanExplanation,
}
```

Use deterministic data structures where ordering affects fingerprinting:
`BTreeMap`, `BTreeSet`, sorted vectors, canonical JSON, and explicit logical
time. Avoid any hash-map iteration leaking into plan fingerprints.

### RunPlanBinding

`RunPlanBinding` binds a runtime run to a compiled plan artifact.

```rust
pub struct RunPlanBinding {
    pub run_id: RunId,
    pub plan_ref: PlanRef,
    pub plan_fingerprint: PlanFingerprint,
    pub created_from: RunIntentDigest,
    pub created_at: EventTime,
}
```

This keeps plan identity and runtime run history separate:

```text
PlanArtifact   = reusable semantic/execution contract
RunPlanBinding = this run's binding to that plan
TaskAttempt    = runtime effort against a task in the bound plan
```

`ExecutablePlan` may remain as a compatibility type name, but its durable shape
should mean `PlanArtifact` plus a `RunPlanBinding`, not a fingerprinted plan
whose canonical identity includes `run_id`.

### PlanTask

`PlanTask` is semantic work declared by the compiler.

```rust
pub struct PlanTask {
    pub task_key: TaskKey,
    pub semantic: TaskSemanticRef,
    pub execution: TaskExecutionSpec,
    pub inputs: Vec<TaskInput>,
    pub outputs: Vec<TaskOutput>,
    pub retry_policy: RetryPolicy,
    pub concurrency_key: Option<ConcurrencyKey>,
    pub priority: Priority,
    pub idempotency_key: IdempotencyKey,
    pub explanation: TaskExplanation,
}

pub enum TaskSemanticRef {
    AssetMaterialization {
        asset_key: AssetKey,
        partition_key: Option<PartitionKey>,
    },
    AssetObservation {
        asset_key: AssetKey,
        partition_key: Option<PartitionKey>,
    },
    Check {
        asset_key: AssetKey,
        check_key: CheckKey,
        partition_key: Option<PartitionKey>,
    },
    Operation {
        op_key: OpKey,
    },
}

pub struct TaskExecutionSpec {
    pub code_location: CodeLocationRef,
    pub executable: ExecutableRef,
    pub resource_requirements: ResourceRequirements,
    pub environment: ExecutionEnvironment,
}
```

This gives the runtime enough execution information to dispatch work without
knowing how asset selection, partition definitions, or freshness rules were
interpreted.

### TaskAttempt

`TaskAttempt` is runtime effort to execute compiled work.

```rust
pub struct TaskAttempt {
    pub run_id: RunId,
    pub task_key: TaskKey,
    pub plan_fingerprint: PlanFingerprint,
    pub attempt: AttemptNumber,
    pub lease_token: LeaseToken,
    pub fencing_token: FencingToken,
}
```

The distinction is critical:

```text
PlanTask      = semantic work declared by compiler
TaskAttempt   = runtime effort to execute that work
RunPlanBinding = runtime binding between run_id and plan artifact
```

## PlanCompiler Interface

`PlanCompiler` should be a real module interface, not a helper function.

```rust
pub trait PlanCompiler: Send + Sync {
    fn compile(&self, request: CompileRequest) -> Result<CompileResult, CompileError>;
}

pub struct CompileRequest {
    pub intent: RunIntent,
    pub snapshot: PlanningSnapshot,
    pub as_of: LogicalTime,
    pub compiler_version: CompilerVersion,
    pub correlation_id: Option<CorrelationId>,
}

pub struct CompileResult {
    pub plan: PlanArtifact,
    pub diagnostics: Vec<PlanDiagnostic>,
}
```

If a `RunId` is passed to the compiler during compatibility migration, it is
correlation metadata only. It must not influence task keys, edge ordering, plan
fingerprints, explanations, or any canonical serialized plan material.

### PlanningSnapshotProvider

The compiler should receive consistent snapshots rather than reading ambient
mutable state.

```rust
pub trait PlanningSnapshotProvider {
    fn snapshot_for(
        &self,
        intent: &RunIntent,
        as_of: LogicalTime,
    ) -> Result<PlanningSnapshot, SnapshotError>;
}

pub struct PlanningSnapshot {
    pub snapshot_token: PlanningSnapshotToken,
    pub asset_graph: AssetGraphSnapshot,
    pub partition_state: PartitionStateSnapshot,
    pub freshness_state: FreshnessSnapshot,
    pub code_location_state: CodeLocationSnapshot,
}
```

The snapshot token should be included in `CompileRequest` through
`PlanningSnapshot` and carried on `PlanCreated`. It is the evidence that the
planner compiled against a named cut of asset, partition, freshness, and code
location state.

The compiler owns:

- selection resolution;
- partition expansion;
- backfill expansion;
- upstream and downstream closure;
- asset dependency traversal;
- task identity;
- task cardinality;
- task grouping;
- skip, materialize, observe, and check decisions;
- freshness policy interpretation;
- retry and concurrency policy attachment;
- execution location and resource requirement attachment;
- explainability metadata;
- plan fingerprinting.

The runtime owns:

- when a task is ready;
- whether capacity exists;
- which attempt number this is;
- which worker or execution location gets the task;
- whether a lease is valid;
- when to retry;
- when to cancel;
- how to record task evidence;
- how to validate callbacks.

## Logical Plan Later

Do not force a full Spark-style logical/physical split in the first refactor.
Design the plan artifact so the split can appear later.

Near-term:

```text
RunIntent -> PlanArtifact
```

Future:

```text
RunIntent
  -> LogicalPlan
  -> OptimizedLogicalPlan
  -> PhysicalPlan
  -> PlanArtifact
```

Future definitions:

- `LogicalPlan`: asset/partition semantics and dependency relationships.
- `PhysicalPlan`: code locations, resource requirements, retry policy,
  grouping, and execution placement.
- `PlanArtifact`: immutable serialized runtime contract with stable task keys,
  edges, policies, and dispatch specs.

For now, `PlanArtifact` can contain both semantic and execution sections.

## Event, Command, and Projection Taxonomy

The model should separate intent, planning facts, runtime facts, worker
evidence, and derived projection state. The first compatibility slice does not
need to change the wire schema, but it should make ownership explicit.

| Thing | Type | Durable ledger event? | Owner |
|---|---|---:|---|
| `RunRequested` | declarative intent / compatibility event | yes today | API or planning application |
| `PlanCreated` | planning fact and runtime handoff | yes | planner |
| `PlanRejected` | planning fact | yes | planner |
| `RunStarted` | runtime fact | yes | runtime |
| `ReadyQueueEntry` | projection/cache row | no | runtime projection |
| `TaskLeaseAcquired` | runtime fact | yes | runtime |
| `TaskDispatched` | runtime fact | yes | runtime |
| `TaskStarted` | worker evidence fact | yes | runtime callback path |
| `TaskFinished` | worker evidence fact | yes | runtime callback path |
| `TaskRetryScheduled` | runtime timing fact | yes, if it affects durable timing | runtime |
| `RunSucceeded` / `RunFailed` / `RunCanceled` | runtime fact | yes | runtime |

Readiness is represented as projection state in the first implementation
slice. Controllers may update the ready projection or enqueue internal dispatch
work, but they must not emit `TaskReady` as durable runtime evidence unless
ADR-020 is changed.

### Planning Events

```rust
pub enum PlanningEvent {
    RunRequested(RunRequested),
    PlanCreated(PlanCreated),
    PlanRejected(PlanRejected),
}
```

Long term, `PlanCreated` should avoid inlining large plans:

```rust
pub struct PlanCreated {
    pub run_id: RunId,
    pub plan_fingerprint: PlanFingerprint,
    pub plan_schema_version: PlanSchemaVersion,
    pub plan_ref: PlanRef,
    pub graph_snapshot_id: GraphSnapshotId,
    pub planning_snapshot_token: PlanningSnapshotToken,
    pub summary: PlanSummary,
    pub created_at: EventTime,
}
```

The full plan should live in a plan store, object store, or
control-store-backed artifact because it is low-frequency immutable metadata.
That does not make the control store the primary telemetry path.

### Runtime Events

Runtime events represent operational evidence and convergence decisions:

```rust
pub enum RuntimeEvent {
    RunStarted(RunStarted),
    TaskLeaseAcquired(TaskLeaseAcquired),
    TaskDispatched(TaskDispatched),
    TaskStarted(TaskStarted),
    TaskHeartbeat(TaskHeartbeat),
    TaskFinished(TaskFinished),
    TaskFailed(TaskFailed),
    TaskRetryScheduled(TaskRetryScheduled),
    TaskCanceled(TaskCanceled),
    RunSucceeded(RunSucceeded),
    RunFailed(RunFailed),
    RunCanceled(RunCanceled),
}
```

`TaskReady` should remain projection-derived in the first slice because ADR-020
currently says derived state changes are projection-only and not ledger events.
A future ADR can promote readiness to an explicit runtime event if the
operational evidence model needs it.

Runtime event envelopes should carry correlation and attempt identity:

```rust
pub struct EventEnvelope<T> {
    pub event_id: EventId,
    pub run_id: RunId,
    pub plan_fingerprint: Option<PlanFingerprint>,
    pub task_key: Option<TaskKey>,
    pub attempt: Option<AttemptNumber>,
    pub causation_id: Option<EventId>,
    pub correlation_id: CorrelationId,
    pub occurred_at: EventTime,
    pub payload: T,
}
```

## Runtime Projections

The runtime should be projection-driven. Controllers should read projections,
not scan raw events on every loop.

Recommended projections:

- `PlanProjection`: `run_id -> plan metadata, task graph, task specs`.
- `TaskStateProjection`: `run_id/task_key -> waiting | ready | leased |
  running | succeeded | failed | skipped | canceled`.
- `DependencyProjection`: task key to upstream and downstream task keys.
- `ReadyQueueProjection`: tasks whose dependencies are satisfied and whose run
  is active.
- `LeaseProjection`: active leases, fencing tokens, expiration, holder.
- `RetryProjection`: failed attempts, retry eligibility, next retry time.
- `RunSummaryProjection`: run-level status, progress, terminal reason.
- `CallbackProjection`: external callbacks awaiting notification.

Controllers should stay small and generic. They should consume compiled plan
facts and projection rows, then append runtime facts or update derived
projection/cache state according to their role.

- `PlanActivationController`: `PlanCreated -> RunStarted` plus initial
  `ReadyQueueEntry` projection rows for root tasks.
- `ReadinessController`: task terminal facts -> newly unblocked
  `ReadyQueueEntry` projection rows.
- `DispatchController`: ready queue plus capacity -> lease and dispatch.
- `RetryController`: failed attempts plus retry policy -> retry schedule.
- `LeaseController`: lease projection plus clock -> expired lease handling.
- `CancellationController`: cancel requests -> task/run cancellation.
- `CompletionController`: task state projection -> terminal run status.

This is the Kubernetes-style runtime analogy: many simple controllers over
desired/current state, not one monolithic interlinked controller.

## Control Store Alignment

The Tier-1 control store should not become the first home for high-frequency
orchestration telemetry.

Recommended state split:

```text
Control store:
  asset graph snapshots
  plan artifacts
  plan indexes
  run summary snapshots
  state tokens
  low-frequency durable metadata
  read-after-write coordination metadata

Runtime event stream:
  task lifecycle evidence
  dispatch evidence
  retry evidence
  cancellation evidence
  callback evidence

Ephemeral / fast coordination store:
  active leases
  worker liveness
  capacity signals
  short-lived readiness/cache state

Object / log store:
  task logs
  large payloads
  worker stdout/stderr
  metrics traces
```

The control store can hold low-frequency records:

```rust
pub struct PlanSnapshotRecord {
    pub plan_fingerprint: PlanFingerprint,
    pub plan_schema_version: PlanSchemaVersion,
    pub graph_snapshot_id: GraphSnapshotId,
    pub plan_ref: PlanRef,
    pub created_at: EventTime,
}

pub struct RunSummarySnapshot {
    pub run_id: RunId,
    pub plan_fingerprint: PlanFingerprint,
    pub state_token: StateToken,
    pub status: RunStatus,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub total_tasks: u64,
}
```

It should not store every heartbeat, log line, worker-level telemetry point, or
dispatch transition as its primary write path.

## Determinism and Fingerprinting

The same compile inputs should always produce the same plan fingerprint:

```text
RunIntent
+ asset graph snapshot id/content
+ partition state snapshot
+ freshness state snapshot
+ code location snapshot
+ compiler version
+ explicit logical time
= deterministic PlanArtifact
```

Avoid these inside `PlanCompiler`:

- `SystemTime::now()`;
- random IDs;
- database reads not represented as snapshots;
- unordered map iteration;
- ambient environment variables;
- network calls;
- worker availability checks;
- runtime capacity checks.

If time matters, pass it in:

```rust
pub struct CompileRequest {
    pub as_of: LogicalTime,
    // ...
}
```

If a dynamic partition set matters, snapshot it before compile:

```rust
pub struct PartitionStateSnapshot {
    pub snapshot_id: PartitionStateSnapshotId,
    pub dynamic_partitions: BTreeMap<PartitionDefId, BTreeSet<PartitionKey>>,
}
```

Keep these identities distinct:

- `plan_fingerprint`: content/semantic identity of the compiled plan;
- `run_id`: runtime instance identity;
- `task_key`: stable semantic key inside the plan;
- `attempt_id`: runtime execution attempt identity.

Two run requests can produce the same plan fingerprint while still having
separate runtime histories.

### Fingerprint Material

The plan fingerprint is computed from:

- normalized `RunIntent` digest;
- graph snapshot identity and/or content, according to the chosen snapshot
  contract;
- partition snapshot identity and/or content;
- freshness snapshot identity and/or content;
- code location snapshot identity and/or content;
- compiler version;
- explicit logical time;
- canonicalized tasks, edges, policies, execution specs, and explanations where
  applicable.

The plan fingerprint excludes:

- `run_id`;
- attempt numbers;
- lease tokens;
- worker identity;
- wall-clock compilation time;
- event IDs;
- callback IDs;
- runtime capacity;
- queue state;
- dispatch adapter state.

If `compiler_version` changes only diagnostic wording, the compiler must either
preserve the old fingerprint material or explicitly annotate that the new
version changes fingerprint behavior. Silent fingerprint drift is a planning
contract break.

## Explainability

Because planning already claims to be deterministic, serializable, and
explainable, explanation should be part of the plan contract.

```rust
pub struct PlanExplanation {
    pub summary: String,
    pub selected_assets: Vec<AssetSelectionExplanation>,
    pub partition_expansions: Vec<PartitionExpansionExplanation>,
    pub skipped_assets: Vec<SkipExplanation>,
    pub task_explanations: BTreeMap<TaskKey, TaskExplanation>,
    pub diagnostics: Vec<PlanDiagnostic>,
}

pub struct TaskExplanation {
    pub why_exists: Vec<Reason>,
    pub asset_key: Option<AssetKey>,
    pub partition_key: Option<PartitionKey>,
    pub upstream_tasks: Vec<TaskKey>,
    pub policies_applied: Vec<PolicyExplanation>,
}
```

Planner-owned explanations answer semantic questions:

- Why is this task in the run?
- Why is this asset skipped?
- Why did this partition expand?
- Which upstream tasks explain this task?
- Which policy attached this retry or concurrency setting?

Runtime-owned evidence answers operational questions:

- Why is this task waiting?
- Why was this task leased?
- Why was this task retried?
- Why was this task canceled?
- Which callback or fencing check rejected this attempt?

## Large Backfills

Backfills should scale without teaching runtime controllers partition
semantics.

Level 1: enumerated plan.

```text
Backfill request
  -> compiler expands every asset partition
  -> PlanArtifact contains all tasks
```

Level 2: sharded plan.

```text
PlanArtifact
  shard 0: tasks 0..9999
  shard 1: tasks 10000..19999
  shard 2: tasks 20000..29999
```

Level 3: declarative backfill plan with deterministic expansion cursor.

```rust
pub struct BackfillPlan {
    pub compiled_selection: CompiledSelection,
    pub compiled_partition_expression: CompiledPartitionExpression,
    pub deterministic_expansion_cursor: ExpansionCursor,
    pub graph_snapshot_id: GraphSnapshotId,
    pub compiler_version: CompilerVersion,
}
```

Even at Level 3, expansion remains planner-owned. The runtime must not call a
planner expansion interface directly. Shard expansion should be event-driven:

```text
BackfillPlanCreated
  -> PlanShardNeeded or PlanShardRequested
  -> PlanShardCreated
  -> runtime activates compiled shard
```

The runtime can record or observe that more compiled work is needed. The
planning application responds by producing `PlanShardCreated`. The runtime then
consumes compiled shard facts in the same way it consumes `PlanCreated`.

This keeps large backfills from reintroducing a runtime-to-planner dependency.

## Worker Dispatch Envelope

The dispatch layer should consume a compiled task and produce an execution
envelope.

```rust
pub struct WorkerDispatchEnvelope {
    pub run_id: RunId,
    pub task_key: TaskKey,
    pub attempt: AttemptNumber,
    pub plan_fingerprint: PlanFingerprint,
    pub plan_ref: PlanRef,
    pub executable: ExecutableRef,
    pub code_location: CodeLocationRef,
    pub inputs: Vec<TaskInput>,
    pub outputs: Vec<TaskOutput>,
    pub idempotency_key: IdempotencyKey,
    pub lease_token: LeaseToken,
    pub fencing_token: FencingToken,
    pub callback: RuntimeCallbackRef,
}
```

The envelope can carry asset metadata as labels for logs and UI:

```rust
pub struct DispatchLabels {
    pub asset_key: Option<AssetKey>,
    pub partition_key: Option<PartitionKey>,
    pub run_reason: Option<String>,
}
```

Labels are observability metadata, not semantic inputs.

### Worker Outcome Acceptance

A worker outcome is accepted only if:

- `run_id` matches an active or idempotently terminal run;
- `plan_fingerprint` matches the run's bound plan;
- `task_key` exists in the bound plan;
- attempt number is current, or the callback is an idempotent duplicate of an
  already accepted terminal attempt;
- lease token is valid, or was valid at accepted completion time under the
  configured completion policy;
- fencing token matches the latest accepted lease for that attempt;
- idempotency key matches the dispatched envelope;
- a terminal outcome has not already been accepted for a different attempt that
  supersedes this callback.

Rejected callback facts should be explicit:

- invalid fencing token -> `TaskCallbackRejected`;
- duplicate terminal callback -> ignored or `TaskCallbackDuplicateObserved`;
- late callback after a retry wins -> `TaskCallbackStale`;
- task not found in bound plan -> `TaskCallbackRejected`.

The exact event names can follow the existing callback event vocabulary, but
the acceptance rule should be testable at the runtime interface.

## First Refactor Target

The first implementation slice should be narrow:

1. Introduce `planning::PlanCompiler`.
2. Move asset-selection-to-task-definition logic out of runtime controllers.
3. Introduce a planning/application adapter, such as `RunPlanner` or
   `PlanCompilationController`, that consumes `RunRequested`, calls the
   compiler, persists the plan artifact, and emits `PlanCreated` or
   `PlanRejected`.
4. Move or deprecate `RunBridgeController` and `RunRequestProcessor` so they are
   not runtime controllers responsible for semantic lowering. During the
   compatibility phase, any remaining shim must live outside `orchestration/`
   or be marked transitional.
5. Preserve today's `PlanCreated { tasks: Vec<TaskDef> }` wire shape.
6. Add seam tests that fail if runtime controllers import planning semantics or
   call the compiler.

In the first slice, `CompileResult` can return compatibility `TaskDef`s:

```rust
pub struct CompileResult {
    pub tasks: Vec<TaskDef>,
    pub diagnostics: Vec<PlanDiagnostic>,
    pub plan_fingerprint: PlanFingerprint,
}
```

That is not the final shape, but it moves semantic lowering across the seam
without forcing a schema migration.

Compatibility `TaskDef` rules:

1. Only planner-owned modules may create `TaskDef` from selection, partition,
   freshness, or asset graph semantics.
2. Runtime may treat `TaskDef` only as a compiled task contract.
3. Runtime may not infer asset-selection semantics from `TaskDef` fields.
4. New semantic fields must not be added to `TaskDef` outside planning-owned
   code.
5. Every `TaskDef` must carry or map to a stable `TaskKey` and plan
   fingerprint.
6. Compatibility `TaskDef` should be removed or wrapped once `PlanCreated`
   moves to `plan_ref`.

The second slice can promote the event contract:

```text
PlanCreated { tasks: Vec<TaskDef> }
  -> PlanCreated {
       plan_ref,
       plan_fingerprint,
       plan_schema_version,
       planning_snapshot_token,
       summary
     }
```

## Proposed Module Layout

Initial monolithic layout:

```text
crates/arco-flow/src/
  domain/
    ids.rs
    time.rs
    errors.rs
  declarative/
    mod.rs
    assets.rs
    asset_graph.rs
    selection.rs
    partitions.rs
    freshness.rs
    schedules.rs
    sensors.rs
    backfills.rs
    run_intent.rs
  planning/
    mod.rs
    compiler.rs
    diagnostics.rs
    normalize.rs
    validate.rs
    expand_selection.rs
    expand_partitions.rs
    lower.rs
    fingerprint.rs
    explain.rs
    events.rs
    plan_store.rs
  application/
    mod.rs
    run_planner.rs
    plan_compilation_controller.rs
    planning_snapshot_provider.rs
  plan/
    mod.rs
    executable_plan.rs
    task.rs
    policies.rs
    explanation.rs
    schema.rs
  orchestration/
    mod.rs
    events.rs
    runtime.rs
    projections/
      plan_projection.rs
      task_state_projection.rs
      ready_queue_projection.rs
      lease_projection.rs
      retry_projection.rs
      run_summary_projection.rs
    controllers/
      plan_activation_controller.rs
      readiness_controller.rs
      dispatch_controller.rs
      retry_controller.rs
      lease_controller.rs
      cancellation_controller.rs
      completion_controller.rs
      callback_controller.rs
    dispatch/
      envelope.rs
      adapters.rs
      local.rs
      kubernetes.rs
      serverless.rs
    leases/
      fencing.rs
      lease_store.rs
    workers/
      protocol.rs
```

This layout is aspirational. The first PR should avoid broad file movement
unless needed to enforce the seam. A compatibility adapter around today's
`TaskDef` is the right first step.

## Testing Strategy

Planner tests:

- same input snapshots produce the same plan fingerprint;
- selection expansion is deterministic;
- partition expansion is deterministic;
- compiled graph is acyclic;
- every task has a stable task key;
- every task has an idempotency key;
- every edge references existing tasks;
- every materialized asset partition has exactly one producing task;
- explanations exist for every task;
- compile failures produce `PlanRejected`, not partial runtime state.

Golden tests:

```text
tests/golden/plans/simple_asset_selection.plan.json
tests/golden/plans/partitioned_backfill.plan.json
tests/golden/plans/freshness_reconcile.plan.json
```

Property tests:

```text
random asset graph + random valid selection
  -> compile
  -> all edges valid
  -> topo sort exists
  -> fingerprints stable
```

Boundary tests:

- `orchestration/**` cannot import `planning`, `declarative`, `AssetGraph`,
  `SelectionOptions`, or partition expansion modules;
- compatibility controllers cannot call `build_task_defs_for_selection`
  directly;
- only planning-owned modules can construct compatibility `TaskDef` from
  selection semantics;
- runtime controllers cannot call `PlanCompiler`.

Fingerprint tests:

- same semantic plan with different `run_id` has the same `plan_fingerprint`;
- same semantic plan with different event ID has the same `plan_fingerprint`;
- changed `compiler_version` changes or explicitly annotates fingerprint
  behavior;
- canonical JSON or plan serialization round trip preserves fingerprint.

Runtime tests:

- runtime controllers never import declarative selection modules;
- `PlanCreated` with one root task makes that task ready in projection;
- `TaskFinished` unblocks downstream tasks;
- failed task with retry policy schedules retry;
- retry creates a new attempt, not a new task;
- expired lease cannot finish without a valid fencing token;
- duplicate `TaskFinished` is idempotent;
- canceled run stops new dispatch;
- completion emits exactly one terminal run state.

Replay tests:

- replaying runtime events from `PlanCreated` reconstructs the same run
  projection;
- duplicate `PlanCreated` is idempotent;
- out-of-order worker callback is rejected, ignored, or held according to the
  documented policy.

Migration tests:

- old `PlanCreated { tasks }` and new `PlanCreated { plan_ref }` produce
  equivalent runtime projections;
- compatibility `TaskDef` maps one-to-one to stable `TaskKey`;
- compatibility `TaskDef` carries or maps to the plan fingerprint.

Backfill tests:

- large backfill shard boundaries are deterministic;
- shard retry does not duplicate `PlanTask`s;
- runtime never expands partition ranges itself.

Integration tests:

```text
RunRequested
  -> PlanCreated
  -> RunStarted
  -> ready projection
  -> dispatch
  -> worker callback facts
  -> terminal run summary
```

Add variants for retries, cancellation, lease expiry, and backfill fanout.

## Migration Plan

### PR 1: Introduce planner boundary types

Add:

- `planning/compiler.rs`
- `planning/diagnostics.rs`
- `planning/fingerprint.rs`
- `application/run_planner.rs`
- `application/planning_snapshot_provider.rs`
- compatibility `CompileRequest` and `CompileResult`

Keep the existing event schema. The first compiler implementation may preserve
current behavior. The application adapter owns compiler invocation.

### PR 2: Move semantic lowering out of runtime ownership

Extract logic from:

- `crates/arco-flow/src/orchestration/controllers/run_bridge.rs`
- `crates/arco-flow/src/orchestration/controllers/run_request_processor.rs`
- `crates/arco-flow/src/orchestration/selection.rs`

into planner-owned modules.

`RunPlanner` or `PlanCompilationController` consumes `RunRequested`, calls
`PlanCompiler`, persists or references the plan artifact, and emits
`PlanCreated` or `PlanRejected`. Runtime controllers consume `PlanCreated`.

Any remaining `run_bridge` or `run_request_processor` shim is transitional and
must not be treated as a runtime controller seam.

### PR 3: Add seam enforcement

Add a CI or repository hygiene check:

```text
orchestration/** may import:
  domain/**
  plan/**
  orchestration/**

orchestration/** may not import:
  declarative/**
  planning/**
  PlanCompiler
  AssetGraph
  SelectionOptions
  build_task_defs_for_selection
```

### PR 4: Promote plan artifact storage

Add a `PlanRepository` or plan artifact store. Keep `PlanCreated` backward
compatible until consumers are ready.

### PR 5: Change `PlanCreated` handoff shape

Move from inline `Vec<TaskDef>` toward `plan_ref`, `plan_fingerprint`, schema
version, planning snapshot token, graph snapshot, and summary.

### PR 6: Add plan explanations and golden tests

Add first-class `PlanExplanation` and golden serialized plans. Use this as the
review surface for future planner changes.

### PR 7: Align low-frequency plan metadata with control-store strategy

Store immutable plan metadata, indexes, and run summaries in the appropriate
low-frequency durable metadata path. Keep task lifecycle telemetry on runtime
events/projections.

## ADRs To Write

### ADR: PlanCompiler owns semantic lowering

Decision: all asset selection, partition expansion, freshness interpretation,
and task synthesis occur in `PlanCompiler`.

Consequence: planning/application modules call `PlanCompiler`; runtime
controllers consume compiled plan contracts only.

### ADR: Runtime creates attempts, not tasks

Decision: the runtime may create `TaskAttempt` records/events for compiled
`PlanTask`s. It may not create new `PlanTask`s.

Consequence: retries, leases, and dispatch remain operational concerns.

### ADR: PlanCreated is the declarative/runtime handoff

Decision: `PlanCreated` is the event that activates a run in the orchestration
runtime.

Consequence: `RunRequested` remains declarative intent and is not directly
executable.

### ADR: Planning snapshots are named compiler inputs

Decision: plan compilation uses an explicit `PlanningSnapshot` with a
`PlanningSnapshotToken`.

Consequence: plan fingerprints and explanations are tied to named asset graph,
partition, freshness, and code-location cuts instead of ambient mutable state.

### ADR: Control store is not primary telemetry storage

Decision: the control store stores low-frequency durable metadata and
snapshots. Runtime events and projections remain the evidence path for
high-frequency orchestration state.

Consequence: heartbeats, logs, and worker telemetry do not become Tier-1
control-store writes.

## Acceptance Criteria

The seam is hardened when:

1. `RunBridgeController` no longer imports selection or asset graph semantics.
2. `RunRequestProcessor` no longer builds task definitions directly.
3. Runtime controllers do not call `PlanCompiler`.
4. A planning/application adapter consumes `RunRequested` and emits
   `PlanCreated` or `PlanRejected`.
5. Runtime controllers can create attempts only for plan-declared task keys.
6. Planner tests prove deterministic task identity and fingerprinting.
7. Runtime tests prove retries create attempts, not new tasks.
8. A repo check prevents runtime code from importing planner/declarative
   modules.
9. Public docs describe `PlanCreated` as the handoff between declarative
   planning and runtime convergence.

## Open Questions

- Should `ExecutablePlan` remain as a compatibility type name, or should durable
  docs switch directly to `PlanArtifact` and `RunPlanBinding`?
- What is the minimum transactionality guarantee for `PlanningSnapshot` across
  asset graph, partition, freshness, and code-location state?
- Is `PlanCompiler` synchronous in-process, async in-process, or service-backed
  in the first implementation slice?
- How are cancellation requests handled while compilation is in progress?
- How long are plan artifacts retained?
- How are old plan schema versions dispatched?
- Are ready queue entries stored as projection rows, internal queue commands, or
  both?
- What is the exact event vocabulary for rejected, duplicate, and stale worker
  callbacks?
- What is the first large-backfill shard protocol that avoids runtime importing
  planning?

## Summary

Arco should treat planner/runtime separation as a hard module design rule:

```text
Declarative planning control plane:
  user intent, asset graph semantics, partition semantics, freshness semantics,
  deterministic compilation, fingerprints, and explanations.

Runtime reconciliation control plane:
  event processing, projections, readiness, dispatch, retries, leases, fencing,
  cancellation, callbacks, and worker adapters.

Handoff:
  immutable, versioned, serializable, fingerprinted plan artifact bound to a run.
```

This gives Arco a durable architecture. The planner can evolve toward
Dagster/Spark-style declarative compilation without absorbing dispatch
mechanics. The runtime can evolve toward Kubernetes/Tokio/serverless-style
convergence without absorbing asset graph semantics.
