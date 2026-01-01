# Arco Flow vs Dagster Parity Audit

> **SUPERSEDED (2025-12-19):** This document is superseded by `docs/plans/2025-12-19-arco-flow-vs-dagster-parity-audit.md` and `docs/audits/arco-flow-dagster-exception-planning.md`. It uses older assumptions and non-repo-addressable citations; do not use it for current parity claims.

**Date:** 2025-01-19
**Status:** Complete
**Author:** Architecture Team

---

## 1. Executive Summary

### 1.1 Overall Parity Verdict: **NEAR PARITY** (with strategic gaps)

Arco Flow demonstrates strong architectural alignment with Dagster's core orchestration model while making deliberate serverless-first trade-offs. The hybrid Rust control plane + Python data plane architecture positions us well for performance and ecosystem compatibility.

**Summary Assessment:**
- **Core Orchestration Model:** Full parity on asset-centric design, dependency resolution, and deterministic planning
- **Partitions & Backfills:** Near parity with designed multi-dimensional support and backfill orchestration
- **Quality Framework:** Near parity with asset checks, gating semantics, and quarantine flows
- **Scheduling/Sensors:** Partial parity - schedules designed, sensors minimal
- **Developer Experience:** Partial parity - SDK designed, tooling gaps exist
- **Operational Features:** Near parity on core ops, gaps in advanced debugging UX

### 1.2 Top 10 Gaps That Prevent Full Parity

| Priority | Gap | Impact | Complexity |
|----------|-----|--------|------------|
| 1 | **No UI/Dagit equivalent** | Critical for UX - users cannot visualize runs, assets, lineage | L |
| 2 | **Sensors not fully implemented** | Cannot react to external events (file arrival, etc.) | M |
| 3 | **No declarative automation/freshness policies** | Dagster auto-materializes based on freshness; Arco Flow requires explicit triggers | M |
| 4 | **Limited run re-execution UX** | Dagster: re-run from failure in UI; Arco Flow: API only, no subset re-execution | M |
| 5 | **No code locations/workspace concept** | Multiple code repositories cannot be deployed independently | M |
| 6 | **IO Manager abstraction not implemented** | Users must handle I/O directly vs declarative IO managers | M |
| 7 | **No dbt integration** | Dagster has first-class dagster-dbt; Arco Flow lacks this | M |
| 8 | **Asset groups/selection not implemented** | Cannot select subsets of assets by tag/group for runs | S |
| 9 | **No built-in secrets management UI** | Secrets are resolved but not visible/manageable in platform | S |
| 10 | **Limited testing framework** | No `materialize_to_memory` or quick asset testing utilities | S |

### 1.3 Areas Where We Exceed Dagster

| Capability | Arco Flow Advantage | Evidence |
|------------|---------------------|----------|
| **Serverless-first architecture** | True scale-to-zero workers, ~$0 at rest | Cloud Run workers, min_instances=0 |
| **Event sourcing** | Full audit trail, projection rebuild, time-travel debugging | Event store with outbox pattern (Part 1 §6) |
| **Multi-tenant isolation** | Native tenant isolation with fairness scheduling | Row-level security, deficit round-robin (Part 2 §5) |
| **Cost attribution** | Built-in per-tenant/asset cost tracking | Part 2 §22 |
| **Unified catalog + orchestration** | Single metadata layer for catalog and execution | Arco Architecture Part 1 + Arco Flow integration |
| **Canonical serialization** | Deterministic hashing across languages | ADR-010, Part 2 §15 |

---

## 2. Glossary: Dagster Term → Arco Flow Term

| Dagster Term | Arco Flow Equivalent | Notes |
|--------------|----------------------|-------|
| Asset | Asset | Direct equivalent |
| Op | Task | Arco Flow uses asset-centric model; ops are tasks within runs |
| Job | Run | A run executes a plan |
| Graph | Plan | DAG of tasks to execute |
| Run | Run | Same concept |
| Schedule | ScheduleDefinition | Cron-based triggers |
| Sensor | EventTrigger | Partially designed, not fully implemented |
| Partition | Partition (multi-dimensional) | More flexible in Arco Flow |
| Backfill | Backfill | Same concept with chunking |
| Asset Check | CheckDefinition | Quality checks (pre/post) |
| Resource | Variables/Secrets | External configuration |
| IO Manager | (not implemented) | Users handle I/O directly |
| Code Location | Workspace | Scoped deployment unit |
| Dagit | (not implemented) | No native UI |
| Run Coordinator | Scheduler | Manages task dispatch |
| Executor | Worker | Serverless Cloud Run workers |
| Run Queue | Cloud Tasks Queue | Priority-based dispatch |

---

## 3. Parity Matrix

### A) Core Orchestration Model + Semantics

| Dagster Capability | Good in Dagster | Arco Flow Equivalent | Status | Evidence | Gap Description | Recommended Approach | Complexity | Risk if Missing |
|--------------------|-----------------|----------------------|--------|----------|-----------------|---------------------|------------|-----------------|
| Asset-centric model | Data assets as first-class citizens with lineage | AssetDefinition + AssetKey + lineage tracking | **Yes** | Part 1 §3 asset.proto | - | - | - | - |
| @asset decorator | Python decorator defines assets | @asset decorator in Python SDK | **Yes** | Part 1 §7.1 | - | - | - | - |
| @multi_asset | Multiple outputs from one op | Not designed | **No** | Not in docs | No equivalent for single function producing multiple assets | Add MultiAssetDefinition to SDK | M | Medium - limits complex transforms |
| Software-defined assets | Declarative asset definitions | AssetDefinition proto | **Yes** | Part 1 §3.3 | - | - | - | - |
| Asset dependencies (deps) | Explicit upstream dependencies | AssetDependency with mappings | **Yes** | Part 1 §3.3 | - | - | - | - |
| Asset groups | Logical grouping of assets | Tags (map<string, string>) | **Partial** | Part 1 §3.3 | No first-class groups, only tags | Add asset_group field to AssetDefinition | S | Low - UX convenience |
| Asset selection | Select subset for materialization | Not implemented | **No** | Not in docs | Cannot select by tag/group | Add asset selection to TriggerRunRequest | M | Medium - ops convenience |
| Deterministic planning | Same inputs = same plan | Plan fingerprint via SHA256 | **Yes** | Part 1 §4.1, Part 2 §15 | - | - | - | - |
| Dependency resolution | Topological sort of DAG | Planner with stage computation | **Yes** | Part 1 §4.1 | - | - | - | - |
| Idempotency | At-least-once with dedup | IdempotencyKey in events | **Yes** | Part 1 §6, Part 2 §14 | - | - | - | - |
| Retry semantics | Configurable retries with backoff | RetryPolicy with exponential backoff | **Yes** | Part 2 §8.2 | - | - | - | - |
| Partial failure handling | Continue non-dependent tasks | Tasks skipped on upstream failure | **Yes** | Part 1 §4.3, Part 2 §4.3 | - | - | - | - |

### B) Scheduling, Sensors, Triggers

| Dagster Capability | Good in Dagster | Arco Flow Equivalent | Status | Evidence | Gap Description | Recommended Approach | Complexity | Risk if Missing |
|--------------------|-----------------|----------------------|--------|----------|-----------------|---------------------|------------|-----------------|
| Cron schedules | Cron-based materialization triggers | ScheduleDefinition with CronTrigger | **Yes** | Part 1 §3.3 | - | - | - | - |
| Sensors | Event-driven triggers (file arrival, etc.) | EventTrigger (designed, not implemented) | **Partial** | Part 1 §3.3 mentions sensors | Only basic event trigger, no sensor daemon | Implement sensor evaluation loop in scheduler | M | High - no event-driven automation |
| Asset sensors | Trigger on upstream materialization | ExternalDependency in PlannedTask | **Partial** | Part 1 §4.1 | External deps exist but no materialization sensors | Add asset materialization event subscription | M | Medium - limits reactive patterns |
| Multi-asset sensors | Watch multiple assets | Not designed | **No** | Not in docs | No multi-asset sensor concept | Design as part of sensor implementation | M | Medium |
| Run deduplication | Skip duplicate RunRequests | run_key via idempotency_key | **Yes** | Part 1 §6 | - | - | - | - |
| Sensor cursors | Stateful sensor evaluation | Not designed | **No** | Not in docs | Sensors need state between evaluations | Add cursor field to sensor config | S | Medium - complex sensors impossible |
| Freshness policies | Auto-materialize when stale | Not designed | **No** | Not in docs | No automatic freshness-based triggering | Design freshness policy framework | M | High - major Dagster differentiator |
| Run concurrency limits | Max concurrent runs | max_concurrent_runs quota | **Yes** | Part 2 §5.2 | - | - | - | - |
| Run prioritization | Priority ordering in queue | Priority field + Cloud Tasks | **Yes** | Part 1 §4.1, Part 2 §4.3 | - | - | - | - |

### C) Partitions + Backfills

| Dagster Capability | Good in Dagster | Arco Flow Equivalent | Status | Evidence | Gap Description | Recommended Approach | Complexity | Risk if Missing |
|--------------------|-----------------|----------------------|--------|----------|-----------------|---------------------|------------|-----------------|
| Time-based partitions | Daily, weekly, monthly partitions | TimeDimension in PartitionStrategy | **Yes** | Part 1 §3.3 | - | - | - | - |
| Multi-dimensional partitions | Partition by multiple dimensions | Multiple PartitionDimension | **Yes** | Part 1 §3.3, ADR-011 | - | - | - | - |
| Dynamic partitions | Runtime-generated partitions | DynamicDimension | **Yes** | Part 1 §3.3 | - | - | - | - |
| Partition mappings | Map between different partition schemes | IdentityMapping, TimeWindowMapping, AllUpstreamMapping, LatestMapping | **Yes** | Part 1 §3.3 | - | - | - | - |
| Backfill orchestration | Process historical partitions | BackfillService with chunking | **Yes** | Part 2 §7 | - | - | - | - |
| Backfill pause/resume | Stop and continue backfills | PauseBackfill/ResumeBackfill RPCs | **Yes** | Part 2 §7.1 | - | - | - | - |
| Partial backfills | Backfill subset of partitions | PartitionRange in CreateBackfillRequest | **Yes** | Part 2 §7.1 | - | - | - | - |
| Backfill cancellation | Cancel in-progress backfills | CancelBackfill RPC | **Yes** | Part 2 §7.1 | - | - | - | - |
| Latest partition strategy | Use latest upstream partition | LatestMapping | **Yes** | Part 1 §3.3 | - | - | - | - |
| Single-run backfills | Execute backfill as single query | Not designed | **No** | Not in docs | Large backfills always chunked | Add single_run_mode to BackfillConfig | S | Low - optimization |

### D) Compute/Execution + Resource Model

| Dagster Capability | Good in Dagster | Arco Flow Equivalent | Status | Evidence | Gap Description | Recommended Approach | Complexity | Risk if Missing |
|--------------------|-----------------|----------------------|--------|----------|-----------------|---------------------|------------|-----------------|
| Worker execution | Task execution in compute | Cloud Run serverless workers | **Yes** | Part 2 §3 | - | - | - | - |
| Timeouts | Per-task execution limits | timeout_ms in ResourceRequirements | **Yes** | Part 1 TaskRequest, Part 2 §3 | - | - | - | - |
| Cancellation | Cancel running tasks | CancelRun RPC, graceful shutdown | **Yes** | Part 2 §3.5, §19 | - | - | - | - |
| Heartbeats | Detect stuck workers | 30s heartbeat loop, 5m timeout | **Yes** | Part 2 §3.3 | - | - | - | - |
| Retries with backoff | Exponential retry delays | RetryPolicy with backoff_multiplier | **Yes** | Part 2 §8.2 | - | - | - | - |
| Resources | External system connections | Variables + Secrets | **Partial** | Part 2 §3.4 | No Resource class with configuration | Consider ConfigurableResource pattern | M | Medium - DX impact |
| IO Managers | Declarative I/O handling | Not implemented | **No** | Not in docs | Users must handle I/O manually | Design IOManager abstraction | M | High - major DX gap |
| Environment separation | Different configs per env | workspace_id scoping | **Partial** | Part 1 §3 | Workspaces exist but no env config | Add environment config to manifest | S | Medium |
| Local dev execution | Run locally for testing | arco-flow dev command | **Yes** | Part 1 §7.2-7.3 | - | - | - | - |
| Cloud execution parity | Same behavior local vs cloud | SQLite (local) vs Postgres (cloud) | **Yes** | Part 1 §9 | - | - | - | - |
| Secrets management | Secure secret access | SecretRef resolved at runtime | **Yes** | Part 2 §3.4 | - | - | - | - |
| Config schema | Typed configuration | Not designed | **Partial** | TaskRequest.variables | Flat key-value, no schema validation | Add config schema to AssetDefinition | S | Low - validation gap |

### E) Observability + Debugging + Run Re-execution

| Dagster Capability | Good in Dagster | Arco Flow Equivalent | Status | Evidence | Gap Description | Recommended Approach | Complexity | Risk if Missing |
|--------------------|-----------------|----------------------|--------|----------|-----------------|---------------------|------------|-----------------|
| Event logs | Structured execution events | Event store with streaming | **Yes** | Part 1 §6, Part 2 §9 | - | - | - | - |
| Structured logs | JSON-formatted logging | Structured log format | **Yes** | Part 2 §9.3 | - | - | - | - |
| Prometheus metrics | Metrics for monitoring | arco_flow_* metrics defined | **Yes** | Part 2 §9.1 | - | - | - | - |
| OpenTelemetry tracing | Distributed tracing | TraceContext propagation | **Yes** | Part 2 §9.2 | - | - | - | - |
| Run view UI | Visualize run progress | Not implemented | **No** | Not in docs | No UI for run visualization | Build web UI or integrate with external | L | Critical - ops impossible without |
| Asset materialization history | Track asset versions | materializations table | **Yes** | Part 2 §14 | - | - | - | - |
| Re-run from failure | Continue from failed step | RetryRun RPC | **Partial** | Part 1 §5.2 | API exists but no subset selection | Add failed_tasks_only option | M | High - debugging friction |
| Re-execute subset | Run specific steps | Not designed | **No** | Not in docs | Cannot re-execute specific tasks | Add task_ids to TriggerRunRequest | M | Medium - debugging friction |
| Step isolation | Run single step for debugging | Not designed | **No** | Not in docs | Cannot isolate single task | Add single_task_mode to run trigger | S | Medium - debugging friction |
| Audit trail | Who changed what, when | Event sourcing + actor_id | **Yes** | Part 1 §6.1 | - | - | - | - |
| Event sourcing/projections | Rebuild state from events | Projection rebuild strategy | **Yes** | Part 2 §20 | - | - | - | - |

### F) Data Quality + Governance Integration

| Dagster Capability | Good in Dagster | Arco Flow Equivalent | Status | Evidence | Gap Description | Recommended Approach | Complexity | Risk if Missing |
|--------------------|-----------------|----------------------|--------|----------|-----------------|---------------------|------------|-----------------|
| Asset checks | Quality validation on assets | CheckDefinition with CheckType | **Yes** | Part 2 §6.1 | - | - | - | - |
| Check execution | Run checks with assets | CheckRunner in worker | **Yes** | Part 2 §6.2 | - | - | - | - |
| Severity levels | INFO/WARNING/ERROR/CRITICAL | Severity enum | **Yes** | Part 2 §6.1 | - | - | - | - |
| Blocking checks | Prevent downstream on failure | GatingDecision.BLOCK | **Yes** | Part 2 §6.3 | - | - | - | - |
| Freshness checks | Validate data currency | FreshnessCheck in CheckDefinition | **Yes** | Part 2 §6.1 | - | - | - | - |
| Custom SQL checks | Arbitrary SQL validation | CustomSqlCheck | **Yes** | Part 2 §6.1 | - | - | - | - |
| Custom Python checks | Python validation logic | CustomPythonCheck | **Yes** | Part 2 §6.1 | - | - | - | - |
| Quarantine flow | Isolate failed data | Quarantine path + manual review | **Yes** | Part 2 §6.4 | - | - | - | - |
| SLAs/Freshness policies | Auto-trigger on staleness | Not designed | **No** | Not in docs | No automatic freshness enforcement | Design with freshness policies | M | High - Dagster differentiator |
| Lineage capture | Track data flow | Lineage-by-execution in Arco catalog | **Yes** | Technical Vision §3.2 | - | - | - | - |
| Metadata capture | Attach metadata to assets | Metadata in protos | **Yes** | Part 1 §3.3 | - | - | - | - |

### G) Multi-tenancy, Isolation, and Ops Hardening

| Dagster Capability | Good in Dagster | Arco Flow Equivalent | Status | Evidence | Gap Description | Recommended Approach | Complexity | Risk if Missing |
|--------------------|-----------------|----------------------|--------|----------|-----------------|---------------------|------------|-----------------|
| Per-tenant quotas | Limit resource usage | TenantQuotas | **Yes** | Part 2 §5.2 | - | - | - | - |
| Fairness scheduling | Prevent tenant starvation | Deficit round-robin | **Yes** | Part 2 §5.1 | - | - | - | - |
| Noisy neighbor prevention | Isolate tenant impact | Quotas + fairness | **Yes** | Part 2 §5 | - | - | - | - |
| Rate limiting | Throttle API requests | max_daily_runs, API limits | **Yes** | Part 2 §5.2 | - | - | - | - |
| Row-level security | Database tenant isolation | Postgres RLS policies | **Yes** | Part 1 §6.2 | - | - | - | - |
| Exactly-once semantics | Reliable delivery | Outbox pattern + idempotency | **Yes** | Part 1 §6.2 | - | - | - | - |
| Disaster recovery | Recovery from failures | Event replay, projection rebuild | **Yes** | Part 2 §20 | - | - | - | - |
| Schema evolution | Upgrade without downtime | Proto evolution policy, migrations | **Yes** | Part 2 §13, §14.4 | - | - | - | - |
| Projection rebuild | Reconstruct from events | verify_and_rebuild_projection() | **Yes** | Part 2 §20 | - | - | - | - |

### H) Developer Experience (DX)

| Dagster Capability | Good in Dagster | Arco Flow Equivalent | Status | Evidence | Gap Description | Recommended Approach | Complexity | Risk if Missing |
|--------------------|-----------------|----------------------|--------|----------|-----------------|---------------------|------------|-----------------|
| @asset decorator | Declarative asset definition | @asset decorator | **Yes** | Part 1 §7.1 | - | - | - | - |
| Type-safe dependencies | Typed asset inputs | AssetIn type hints | **Yes** | Part 1 §7.1 | - | - | - | - |
| CLI workflows | Command-line tooling | arco-flow CLI commands | **Yes** | Part 1 §7.2 | - | - | - | - |
| Local dev server | arco-flow dev | **Yes** | Part 1 §7.3 | - | - | - | - |
| Manifest validation | Validate before deploy | arco-flow deploy --dry-run | **Yes** | Part 1 §2.1 | - | - | - | - |
| Code locations | Deploy multiple repos | WorkspaceId scoping | **Partial** | Part 1 §1.3 | No dynamic code location loading | Design code location registry | M | Medium - monorepo only |
| Workspace concept | Multiple code locations | Not fully designed | **Partial** | Not in docs | Single manifest per workspace | Add code location config | M | Medium - limits scaling |
| dbt integration | dagster-dbt | Not designed | **No** | Not in docs | No dbt integration | Build arco-flow-dbt package | M | High - dbt is ubiquitous |
| Testing utilities | materialize_to_memory | Not designed | **No** | Not in docs | No quick testing utilities | Add test mode to SDK | S | Medium - DX friction |
| Asset type inference | Auto-detect output types | Not designed | **No** | Not in docs | Must specify output types | Add type inference to SDK | S | Low - convenience |

---

## 4. Gap Inventory (Prioritized)

### Category A: Core Orchestration (Low Gaps)

**A1: No @multi_asset equivalent**
- **Problem:** Cannot produce multiple assets from a single function
- **Why it matters:** Complex transforms often produce multiple outputs; forces artificial separation
- **Proposed solution:** Add MultiAssetDefinition to SDK with multiple AssetOut returns
- **Alternatives:** Use separate assets with shared upstream (verbose but works)
- **Dependencies:** SDK changes only
- **Complexity:** M

### Category B: Scheduling/Sensors (Significant Gaps)

**B1: Sensors not fully implemented** (Priority: High)
- **Problem:** Cannot trigger runs based on external events (file arrival, database changes)
- **Why it matters:** Event-driven pipelines are critical for real-time use cases; Dagster excels here
- **Proposed solution:**
  1. Implement sensor evaluation loop in scheduler
  2. Add sensor definition to manifest
  3. Support cursor-based state
- **Alternatives:**
  - External orchestration (Cloud Scheduler calling API) - loses unified model
  - Pub/Sub triggers - adds infrastructure complexity
- **Dependencies:** Scheduler changes, new proto messages, SDK additions
- **Complexity:** M

**B2: No freshness policies/declarative automation** (Priority: High)
- **Problem:** Cannot auto-materialize assets when stale
- **Why it matters:** Key Dagster differentiator; reduces manual scheduling burden
- **Proposed solution:**
  1. Add FreshnessPolicy to AssetDefinition
  2. Implement freshness evaluation in scheduler
  3. Auto-trigger materializations on policy violation
- **Alternatives:**
  - Frequent schedules (wasteful)
  - External monitoring + API triggers (complex)
- **Dependencies:** Scheduler, materialization tracking
- **Complexity:** M

### Category C: Partitions + Backfills (Low Gaps)

**C1: No single-run backfills**
- **Problem:** Large backfills in systems like Snowflake could run as one query
- **Why it matters:** Performance optimization for compute-efficient engines
- **Proposed solution:** Add single_run_mode to BackfillConfig
- **Alternatives:** Accept chunked execution (works, just slower)
- **Dependencies:** Backfill service changes
- **Complexity:** S

### Category D: Compute/Execution (Medium Gaps)

**D1: No IO Manager abstraction** (Priority: High)
- **Problem:** Users must handle I/O directly; no declarative storage handling
- **Why it matters:** IO managers enable easy storage swapping, testing, and consistent patterns
- **Proposed solution:**
  1. Design IOManager protocol
  2. Implement ParquetIOManager, DeltaIOManager, etc.
  3. Add io_manager_key to AssetDefinition
- **Alternatives:**
  - Users handle I/O (current) - works but verbose
  - Provide utilities (partial solution)
- **Dependencies:** SDK, worker runtime
- **Complexity:** M

**D2: No ConfigurableResource pattern**
- **Problem:** No structured way to define configurable resources
- **Why it matters:** Resources enable environment-specific configs, testing, and DRY patterns
- **Proposed solution:** Add Resource base class to SDK with configuration schema
- **Alternatives:** Use variables/secrets (flat, no schema)
- **Dependencies:** SDK changes
- **Complexity:** M

### Category E: Observability/Debugging (Critical Gaps)

**E1: No UI/Dagit equivalent** (Priority: Critical)
- **Problem:** No visual interface for runs, assets, lineage
- **Why it matters:** Operations impossible without visibility; debugging requires log diving
- **Proposed solution:**
  1. Build web UI for run visualization
  2. Asset dependency graph view
  3. Run/task detail pages
- **Alternatives:**
  - CLI only (poor UX)
  - Third-party integration (Grafana dashboards - partial)
- **Dependencies:** Major frontend work
- **Complexity:** L

**E2: Limited run re-execution UX** (Priority: High)
- **Problem:** Cannot easily re-run from failure or re-execute subsets
- **Why it matters:** Debugging failures requires full re-runs; wastes compute
- **Proposed solution:**
  1. Add failed_tasks_only to RetryRun
  2. Add task_ids selection to TriggerRun
  3. Build UI for step selection
- **Alternatives:** Manual API calls (works but poor UX)
- **Dependencies:** API changes, potential UI
- **Complexity:** M

### Category F: Data Quality (Low Gaps)

**F1: No SLA-based automation**
- **Problem:** Cannot automatically trigger when SLA violated
- **Why it matters:** Freshness SLAs should auto-remediate, not just alert
- **Proposed solution:** Integrate SLA violations with freshness policy auto-triggers
- **Alternatives:** Alert + manual trigger (current)
- **Dependencies:** Freshness policy implementation
- **Complexity:** M

### Category H: Developer Experience (Medium Gaps)

**H1: No code locations/workspace concept** (Priority: Medium)
- **Problem:** Cannot deploy multiple code repositories independently
- **Why it matters:** Large teams need modular deployments; microservices pattern
- **Proposed solution:**
  1. Design code location registry
  2. Support multiple manifests per workspace
  3. Add code location scoping to runs
- **Alternatives:** Monorepo (works for small teams)
- **Dependencies:** Deploy service, manifest structure
- **Complexity:** M

**H2: No dbt integration** (Priority: High)
- **Problem:** dbt users cannot easily integrate dbt models as assets
- **Why it matters:** dbt is ubiquitous in data teams; dagster-dbt is popular
- **Proposed solution:** Build arco-flow-dbt package with asset generation from dbt manifest
- **Alternatives:**
  - Shell execution (loses lineage)
  - Manual asset wrappers (verbose)
- **Dependencies:** External package work
- **Complexity:** M

**H3: No testing utilities**
- **Problem:** No quick way to test assets without full execution
- **Why it matters:** TDD is harder; iteration is slower
- **Proposed solution:** Add materialize_to_memory mode to SDK
- **Alternatives:** Local dev server (works but heavier)
- **Dependencies:** SDK changes
- **Complexity:** S

---

## 5. Exception Planning Cards

### Exception Card 1: Browser-side Dagit UI

| Field | Value |
|-------|-------|
| **Feature/capability** | Full-featured browser-based Dagit UI |
| **Decision** | Deferred - build minimal ops UI first |
| **Rationale (business)** | Daxis has its own UI; Arco Flow is infrastructure. MVP needs CLI + API. |
| **Rationale (technical)** | Full UI requires significant frontend investment outside core competency |
| **User impact** | Operators must use CLI; debugging is harder |
| **Mitigation/workaround** | Provide CLI commands for all operations; integrate with Grafana for viz |
| **Trigger to revisit** | >5 customers requesting standalone Arco Flow deployment |
| **Risk acceptance** | Accepted for MVP; defer to Phase 5+ |
| **Effort if later matched** | L (3-6 months frontend work) |

### Exception Card 2: Declarative Automation / Auto-materialize

| Field | Value |
|-------|-------|
| **Feature/capability** | Auto-materialize on parent update or freshness violation |
| **Decision** | Deferred to Phase 5 |
| **Rationale (business)** | Explicit scheduling covers 80% of use cases initially |
| **Rationale (technical)** | Requires complex state tracking and evaluation loop |
| **User impact** | Must configure explicit schedules for all materialization |
| **Mitigation/workaround** | Provide schedule templates for common patterns |
| **Trigger to revisit** | Customer requests for reactive pipelines |
| **Risk acceptance** | Medium risk - competitive disadvantage vs Dagster |
| **Effort if later matched** | M (1-2 months) |

### Exception Card 3: Full IO Manager Abstraction

| Field | Value |
|-------|-------|
| **Feature/capability** | Pluggable IO managers like Dagster |
| **Decision** | Partial implementation - provide utilities, not full abstraction |
| **Rationale (business)** | Most users write to one format (Parquet); abstraction overhead not justified |
| **Rationale (technical)** | Clean abstraction requires careful design; can add incrementally |
| **User impact** | Must handle I/O explicitly; harder to swap storage |
| **Mitigation/workaround** | Provide ParquetUtils, DeltaUtils helper classes |
| **Trigger to revisit** | Users with multiple output formats or storage layers |
| **Risk acceptance** | Low risk - utilities cover common cases |
| **Effort if later matched** | M (1 month) |

### Exception Card 4: Multi-asset Sensors

| Field | Value |
|-------|-------|
| **Feature/capability** | Sensors watching multiple assets for changes |
| **Decision** | Not in scope for MVP |
| **Rationale (business)** | Complex reactive patterns are advanced use cases |
| **Rationale (technical)** | Basic sensors first; multi-asset adds complexity |
| **User impact** | Cannot build complex event-driven pipelines |
| **Mitigation/workaround** | Use multiple single-asset sensors |
| **Trigger to revisit** | Sensor implementation complete, user demand |
| **Risk acceptance** | Low risk - edge case |
| **Effort if later matched** | S (2 weeks) |

### Exception Card 5: @multi_asset Decorator

| Field | Value |
|-------|-------|
| **Feature/capability** | Single function producing multiple assets |
| **Decision** | Deferred |
| **Rationale (business)** | Can model with separate assets; pattern is less common |
| **Rationale (technical)** | Adds complexity to SDK and planning |
| **User impact** | Must split complex transforms into multiple functions |
| **Mitigation/workaround** | Use separate assets with shared computation |
| **Trigger to revisit** | User feedback on verbose patterns |
| **Risk acceptance** | Low risk |
| **Effort if later matched** | M (2-3 weeks) |

---

## 6. Unknowns List

| Item | What's Missing | Why It Blocks Assessment | Artifact to Resolve |
|------|----------------|--------------------------|---------------------|
| **Sensor implementation details** | No detailed sensor proto or daemon design | Cannot assess sensor parity completeness | Design addendum for sensor system |
| **Python SDK completeness** | SDK design docs are high-level | Cannot verify all Dagster patterns are supported | SDK API reference document |
| **Performance benchmarks** | No scheduler throughput data | Cannot compare with Dagster at scale | Load test results |
| **UI/Web interface plans** | No UI design docs | Cannot assess ops UX gap closure | UI requirements doc |
| **dbt integration approach** | Not designed | Cannot estimate effort for major integration | dbt integration design |
| **Freshness policy design** | Not in current docs | Key Dagster feature with no design | Freshness policy RFC |
| **Code location architecture** | Workspace concept unclear | Cannot assess multi-repo deployment | Code location design |
| **Materialization events for sensors** | Event schema for asset updates | Needed for asset sensors | Event schema addendum |

---

## 7. Recommended Next Steps

### 30-Day Plan

| Priority | Task | Owner | Dependencies |
|----------|------|-------|--------------|
| P0 | Design sensor system (proto, daemon, SDK) | Core Team | - |
| P0 | Implement basic sensor evaluation loop | Core Team | Sensor design |
| P1 | Add run re-execution from failure (API) | Core Team | - |
| P1 | Design freshness policy framework | Core Team | - |
| P1 | Create basic CLI run visualization | Core Team | - |
| P2 | Add asset selection to run triggers | Core Team | - |

### 60-Day Plan

| Priority | Task | Owner | Dependencies |
|----------|------|-------|--------------|
| P0 | Implement freshness policies | Core Team | Freshness design |
| P0 | Implement cursor-based sensors | Core Team | Sensor implementation |
| P1 | Design IO Manager abstraction | Core Team | - |
| P1 | Build minimal web UI for run status | Frontend | API complete |
| P2 | Add testing utilities to SDK | SDK Team | - |
| P2 | Design code locations architecture | Core Team | - |

### 90-Day Plan

| Priority | Task | Owner | Dependencies |
|----------|------|-------|--------------|
| P0 | Implement IO Managers (Parquet, Delta) | Core Team | IO Manager design |
| P1 | Build asset graph visualization | Frontend | Web UI |
| P1 | Design and prototype dbt integration | Core Team | - |
| P2 | Implement code locations | Core Team | Code location design |
| P2 | Add multi-asset sensors | Core Team | Sensor implementation |

### Architectural Decisions to Lock Early

1. **Sensor evaluation model:** Push (event-driven) vs Pull (polling)? Recommend: Polling with configurable interval
2. **IO Manager interface:** How do managers receive context? Design before implementation
3. **Freshness policy evaluation:** Where does freshness check run? Recommend: Scheduler, not workers
4. **Code location isolation:** Process vs container isolation? Recommend: Container (Cloud Run per location)

---

## Appendix A: Document Sources

| Document | Path | Sections Referenced |
|----------|------|---------------------|
| Arco Flow Orchestration Design (Part 1) | docs/plans/2025-01-12-arco-orchestration-design.md | §1-9 (all) |
| Arco Flow Orchestration Design (Part 2) | docs/plans/2025-01-12-arco-orchestration-design-part2.md | §1-22 (all) |
| Arco Architecture Part 1 | docs/plans/ARCO_ARCHITECTURE_PART1_CORE.md | §1-9 (all) |
| Arco Technical Vision | docs/plans/ARCO_TECHNICAL_VISION.md | §1-12 (all) |
| orchestration.proto | proto/arco/v1/orchestration.proto | All messages |
| common.proto | proto/arco/v1/common.proto | ID types, keys |
| ADR-011 Partition Identity | docs/adr/adr-011-partition-identity.md | Partition key format |
| ADR-015 Postgres Store | docs/adr/adr-015-postgres-store.md | State storage |
| ADR-017 Cloud Tasks Dispatcher | docs/adr/adr-017-cloud-tasks-dispatcher.md | Task dispatch |

## Appendix B: Dagster Sources

- [Dagster Concepts Documentation](https://docs.dagster.io/concepts)
- [Dagster Software-Defined Assets](https://docs.dagster.io/concepts/assets/software-defined-assets)
- [Dagster Asset Checks](https://docs.dagster.io/concepts/assets/asset-checks)
- [Dagster Partitions and Backfills](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions)
- [Dagster Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors)
- [Dagster Resources](https://docs.dagster.io/concepts/resources)
- [Dagster Jobs](https://docs.dagster.io/concepts/ops-jobs-graphs/jobs)

---

*End of Parity Audit*
