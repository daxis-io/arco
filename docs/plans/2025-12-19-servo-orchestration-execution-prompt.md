# Servo Orchestration MVP — Execution Prompt

> **Purpose:** Comprehensive execution prompt for implementing Servo/Arco orchestration with Dagster parity, following industry-standard practices for operational excellence.

---

## Document Hierarchy (Read in Order)

| Priority | Document | Purpose |
|----------|----------|---------|
| 1 | `docs/audits/servo-dagster-exception-planning.md` | **Scope boundaries** — what we're NOT building (10 exception cards) |
| 2 | `docs/plans/2025-12-19-servo-arco-vs-dagster-parity-audit.md` | **Capability matrix** — what we ARE building (8 categories, acceptance criteria) |
| 3 | `docs/adr/adr-020-orchestration-domain.md` | **Architecture** — orchestration as unified domain (Parquet projections) |
| 4 | `docs/adr/adr-023-worker-contract.md` | **Worker Contract** — callback endpoints, auth, heartbeat protocol |
| 5 | `proto/arco/v1/orchestration.proto` | **Contracts** — canonical message definitions |

**Critical ADRs for M1:**

- **ADR-011**: PartitionKey canonical encoding ✅ Accepted
- **ADR-013**: ID wire formats (ULID with prefixes) ✅ Accepted
- **ADR-014**: Leader election strategy ✅ Accepted
- **ADR-020**: Orchestration as unified domain ✅ Accepted
- **ADR-021**: Cloud Tasks naming convention ✅ Accepted
- **ADR-022**: Per-edge dependency satisfaction ✅ Accepted
- **ADR-023**: Worker contract specification ✅ Accepted

**Critical:** Read the exception planning document FIRST. It defines what's out of scope and prevents wasted effort.

---

## Core Execution Prompt

```text
You are implementing Milestone M1 (Orchestration MVP) of the Servo/Arco platform.

## Mission

Deliver a production-ready deploy → run → observe loop that:
1. Passes the M1 exit criteria from the exception planning document
2. Passes the uncheatable M1 E2E smoke test
3. Achieves Dagster semantic parity for in-scope capabilities
4. Respects all 10 exception cards (deliberate scope limitations)

## Scope Boundaries (Non-Negotiable)

### BUILDING (M1 Deliverables)

**Architecture: ADR-020 (Parquet-first, event-sourced)**

- Control plane API: TriggerRun, GetRun, ListRuns, CancelRun (REST/JSON)
- Orchestration domain storage (per ADR-020):
  - Ledger: `ledger/orchestration/{ulid}.json` (append-only events)
  - Projections: `state/orchestration/*.parquet` (runs, tasks, dep_satisfaction, timers, dispatch_outbox)
  - Stateless controllers reconciling from manifests
- Worker dispatch via Cloud Tasks with callback handling (ADR-021, ADR-023)
- CLI: `servo deploy`, `servo run`, `servo status`, `servo logs`
- Cross-language determinism (Rust/Python golden fixtures)
- State machine correctness (Run + Task lifecycles)
- Per-edge dependency satisfaction (ADR-022, duplicate-safe)
- run_key idempotency for trigger deduplication

### NOT BUILDING (Exceptions — Do Not Implement)
- EX-01: Op/Job model (asset-first only)
- EX-02: Customer-operated daemon (managed internally)
- EX-03: Orchestration UI (CLI-only in M1)
- EX-04: IO Managers (explicit I/O only)
- EX-05a: @multi_asset (single asset only)
- EX-05b: Observable sources (defer M3)
- EX-06: Multi-code-location (single location per workspace)
- EX-07: GraphQL API (REST only, gRPC in M2+)
- EX-08: K8s/ECS launchers (Cloud Run only)
- EX-09: Auto-materialize (explicit triggers only)

If you find yourself implementing anything from the "NOT BUILDING" list, STOP immediately.

## Quality Mandates

### 1. Test-Driven Development (TDD) — Mandatory

For EVERY feature:
1. Write the failing test FIRST
2. Run the test to confirm it fails for the RIGHT reason
3. Write MINIMAL code to make it pass
4. Run the test to confirm it passes
5. Refactor only if needed
6. Commit with atomic, descriptive messages

Never write implementation code without a failing test. "I'll add tests later" is not acceptable.

### 2. Cross-Language Determinism — Enforced

These MUST produce byte-identical output across Rust and Python:

| Type | Canonical Form | Verification |
|------|----------------|--------------|
| TaskKey | `{"asset_key":"ns/asset","operation":"MATERIALIZE","partition_key":"2024-01-01"}` | SHA-256 match |
| PartitionKey | Sorted key=value pairs | Golden fixture |
| Plan fingerprint | Canonical JSON of inputs | Stable across processes |

Implementation pattern:
1. Create golden fixtures in `fixtures/cross_language/`
2. Both Rust and Python tests consume same fixtures
3. CI fails if either diverges

### 3. State Machine Correctness — Exhaustive

**Run States:** PENDING → RUNNING → {SUCCEEDED | FAILED | CANCELLED | TIMED_OUT}
**Task States:** PENDING → QUEUED → RUNNING → {SUCCEEDED | FAILED | SKIPPED | CANCELLED}

Requirements:
- All valid transitions tested
- All invalid transitions rejected with error
- Terminal states are final (no transitions out)
- Duplicate callbacks are idempotent (no state change)

### 4. Idempotency — Everywhere

| Operation | Idempotency Key | Behavior |
|-----------|-----------------|----------|
| TriggerRun | run_key | Returns existing run_id if duplicate; 409 if payload differs (post-cutoff) |
| Task callback | idempotency_key + attempt_id | No-op if already processed |
| Event write | event_id | Deduplicated by store |

### 5. Error Categorization — Required

Every error MUST have:
- Category: UserCode | DataQuality | Infrastructure | Configuration
- Retryable: true/false
- correlation_id for tracing
- Structured log entry

### 6. Observability — Built-In

Every component MUST export:
- Request latency histogram
- Error rate counter
- Queue depth gauge (where applicable)
- Trace context propagation (traceparent header)

## Implementation Sequence

### Phase 1: Foundation (Days 1-10)

**ADRs (all resolved):**

- ADR-011: PartitionKey canonical encoding ✅
- ADR-013: ID wire formats ✅
- ADR-014: Leader election strategy ✅
- ADR-020: Orchestration as unified domain ✅
- ADR-021: Cloud Tasks naming convention ✅
- ADR-022: Per-edge dependency satisfaction ✅
- ADR-023: Worker contract specification ✅

**Deliverables:**

1. Orchestration domain schema (per ADR-020):
   - `runs.parquet`: Run state and counters
   - `tasks.parquet`: Task state machine with `row_version` for deterministic merge
   - `dep_satisfaction.parquet`: Per-edge dependency facts (ADR-022)
   - `timers.parquet`: Active durable timers
   - `dispatch_outbox.parquet`: Pending dispatch intents
2. Ledger event schema (Intent/Acknowledgement/Worker Facts categories)
3. Cross-language golden fixtures (TaskKey, PartitionKey, AssetKey)
4. Run + Task state machine implementations with exhaustive tests

### Phase 2: Core Loop (Days 11-20)

**Deliverables:**
1. REST API: TriggerRun, GetRun, ListRuns, CancelRun
2. run_key idempotency enforcement + payload fingerprint check (409 on mismatch after cutoff)
3. Cloud Tasks dispatch with TaskEnvelope
4. Worker callback handling (success, failure, heartbeat)
5. Retry scheduling with backoff

### Phase 3: CLI + Integration (Days 21-30)

**Deliverables:**
1. `servo deploy` — manifest upload + registration
2. `servo run` — trigger with selection support
3. `servo status` — run/task state display
4. `servo logs` — log retrieval
5. Integration tests (M1 E2E smoke test)

## Verification Gates

### Gate 1: Compilation + Linting
```bash
cargo check --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --workspace --check
buf lint
```

### Gate 2: Unit Tests
```bash
cargo test --workspace
pytest python/arco/tests/unit/
```

### Gate 3: Cross-Language Tests
```bash
cargo test -p arco-core cross_language
pytest python/arco/tests/contract/
```

### Gate 4: Integration Tests
```bash
cargo test -p arco-integration-tests

# IAM smoke tests (requires deployed GCP + credentials)
cargo test -p arco-integration-tests --features iam-smoke -- --ignored
```

### Gate 5: M1 Exit Criteria
- [ ] `servo deploy` registers manifest with control plane
- [ ] `servo run` triggers execution and returns run_id
- [ ] `servo status` shows run/task states in real-time
- [ ] `servo logs <run_id>` retrieves stdout/stderr
- [ ] `servo status --watch` provides live updates
- [ ] All P0 acceptance criteria pass
- [ ] M1 E2E smoke test passes

## M1 E2E Smoke Test (Uncheatable)

```
SETUP:
  - Clean workspace (no prior runs)
  - Python project with @asset definitions: A → B → C

EXECUTE:
  1. servo deploy --workspace=test
  2. TriggerRun(selection={A}) — explicit selection
  3. Poll servo status until completion (timeout 60s)
  4. TriggerRun(selection={A}, run_key="test-idempotency-key")
  5. TriggerRun(selection={A}, run_key="test-idempotency-key") — duplicate
  6. servo logs <run_id>

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

## Anti-Patterns to Avoid

| Anti-Pattern | Why It's Bad | Do This Instead |
|--------------|--------------|-----------------|
| "I'll add tests later" | You won't. Bugs ship. | TDD: test first, always |
| Using HashMap for ordered iteration | Non-deterministic | Use BTreeMap |
| Floats in serialization | Breaks determinism | Use integers (millicores, bytes) |
| `unwrap()` in production paths | Panic is not error handling | Use Result with proper errors |
| Mocking in integration tests | Hides real bugs | Use ephemeral real infrastructure |
| Implementing exception items | Scope creep | Check exception list first |
| Hardcoded timeouts | Inflexible | Config with sensible defaults |
| "Log and continue" | State inconsistency | Define recovery behavior |
| Sequential Cloud Tasks IDs | Rejected/delayed | Hash-based IDs |
| Skipping golden fixtures | Cross-language bugs hide | CI gate on byte-identical output |

## Commit Message Format

```
<type>(<scope>): <description>

[optional body explaining WHY, not WHAT]
```

Types: feat, fix, test, refactor, docs, chore
Scopes: orchestration, scheduler, dispatcher, worker, cli, schemas

Examples:
- `feat(orchestration): add TriggerRun REST endpoint`
- `test(scheduler): add run state machine exhaustive coverage`
- `fix(dispatcher): use hash-based Cloud Tasks IDs`

## When You're Stuck

1. **Re-read the exception planning doc** — is this in scope?
2. **Check the parity audit** — what's the acceptance criterion?
3. **Look at existing patterns** — `arco-catalog/src/compactor.rs` is the template
4. **Check the ADRs** — design decisions are documented
5. **Ask** — unclear requirements should be clarified, not guessed

## Success Criteria

M1 is complete when:
1. M1 E2E smoke test passes with all assertions
2. Cross-language determinism tests pass
3. State machine exhaustive tests pass
4. All P0 acceptance criteria from parity audit pass
5. CLI workflow works: deploy → run → status → logs
6. No blocking bugs in critical path
7. Stakeholder demo completed

---

BEGIN EXECUTION

Start by reading:
1. `docs/audits/servo-dagster-exception-planning.md` (scope)
2. `docs/plans/2025-12-19-servo-arco-vs-dagster-parity-audit.md` (requirements)

Then resolve the blocking ADRs (ORCH-ADR-001 through ORCH-ADR-004).

Use the superpowers:executing-plans skill to track progress task-by-task.
```

---

## Quick Reference

### Commands

```bash
# Build
cargo check --workspace
cargo build --workspace

# Lint
cargo clippy --workspace -- -D warnings
cargo fmt --workspace --check
buf lint

# Test
cargo test --workspace
cargo test -p arco-flow
cargo test -p arco-integration-tests
pytest python/arco/tests/

# Coverage
cargo llvm-cov --workspace
```

### Key Files

| Purpose | Location |
|---------|----------|
| Exception Planning | `docs/audits/servo-dagster-exception-planning.md` |
| Parity Audit | `docs/plans/2025-12-19-servo-arco-vs-dagster-parity-audit.md` |
| **ADR-020** (Architecture) | `docs/adr/adr-020-orchestration-domain.md` |
| **ADR-023** (Worker Contract) | `docs/adr/adr-023-worker-contract.md` |
| Proto Contracts | `proto/arco/v1/orchestration.proto` |
| Task Dispatch | `crates/arco-flow/src/dispatch/mod.rs` |
| Events Module | `crates/arco-flow/src/events.rs` |
| Task State Machine | `crates/arco-flow/src/task.rs` |
| Run State Machine | `crates/arco-flow/src/run.rs` |
| Python Asset Decorator | `python/arco/src/servo/asset.py` |

### Storage Paths (ADR-020 Layout)

```
{bucket}/{tenant_id}/{workspace_id}/
├── ledger/orchestration/{ulid}.json      # Append-only events
├── state/orchestration/
│   ├── runs.parquet                       # Run state projections
│   ├── tasks.parquet                      # Task state projections
│   ├── dep_satisfaction.parquet           # Per-edge dependency facts
│   ├── timers.parquet                     # Active durable timers
│   └── dispatch_outbox.parquet            # Pending dispatch intents
├── manifests/orchestration.manifest.json  # Domain manifest
└── assets/{asset_key}/{partition_key}/    # Materialized outputs
```

### API Endpoints (M1)

**Control Plane (Client-facing):**

```
POST /v1/workspaces/{workspace_id}/runs           # TriggerRun
GET  /v1/workspaces/{workspace_id}/runs/{run_id}  # GetRun
GET  /v1/workspaces/{workspace_id}/runs           # ListRuns
POST /v1/workspaces/{workspace_id}/runs/{run_id}/cancel  # CancelRun
```

**Worker Callbacks (ADR-023):**

```
POST /v1/tasks/{task_id}/started    # Worker began execution
POST /v1/tasks/{task_id}/heartbeat  # Worker liveness + cancel signal
POST /v1/tasks/{task_id}/completed  # Task finished (success/failure)
```

---

## Execution Modes

### Mode 1: Subagent-Driven (Recommended)

```
Use superpowers:subagent-driven-development to dispatch fresh agents per task
with code review between each. Best for complex multi-file changes.
```

### Mode 2: Plan Execution

```
/superpowers:execute-plan

Follow the plan in batches with review checkpoints.
```

### Mode 3: Manual with TDD

```
Work through tasks manually using this prompt as a checklist.
Write test → implement → verify → commit for each feature.
```

---

## Checklist Before Starting

- [x] Exception planning document read and understood
- [x] Parity audit acceptance criteria reviewed
- [x] All blocking ADRs resolved (ADR-011, 013, 014, 020, 021, 022, 023)
- [ ] Orchestration domain Parquet schema defined (per ADR-020)
- [ ] Ledger event schema defined (Intent/Acknowledgement/Worker Facts)
- [ ] Cloud Tasks dispatch configuration available
- [ ] Golden fixtures directory created
- [x] Worker callback endpoints specified (ADR-023)

If any item is incomplete, resolve it before writing code.
