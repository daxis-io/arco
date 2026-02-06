# Dagster Parity Execution Plan (02–12)

This plan is subordinate to the scope and “done” rules in:
- `docs/parity/dagster-parity-01-charter.md`

## Guiding Principle
Every claimed parity milestone must be **provable via repo-addressable artifacts**: code + CI-gated tests + explicit evidence links.

## dagster-parity-02 — Make parity audit repo-addressable
**Objective**: ensure parity progress can be proven via file paths/tests, not ignored docs or external links.

**Deliverables**
- A tracked parity index (this `docs/parity/` directory is the starting point).
- Any “authoritative” content currently living under `docs/plans/**` that we still rely on is either:
  - copied into tracked docs (recommended), or
  - replaced by ADRs/tests.

**Acceptance criteria**
- No parity claim requires `docs/plans/**`.
- CI-gated tests exist for any “Implemented” claim.

**Evidence targets**
- `.github/workflows/ci.yml`
- `docs/parity/dagster-parity-matrix-template.md`

---

## dagster-parity-03 — Build parity matrix
**Objective**: produce a single matrix mapping Dagster capabilities to Arco implementation status and proof.

**Deliverables**
- A parity matrix doc/file with columns:
  - Dagster capability
  - Arco feature
  - Status
  - Required proof level (unit/integration/e2e)
  - Evidence links (code + tests + CI job)
  - Owner

**Acceptance criteria**
- Every row has a status and an owner.
- Any row marked “Implemented” includes at least one CI-gated proving test.

**Evidence targets**
- `docs/parity/dagster-parity-matrix-template.md`

---

## dagster-parity-04 — Automate M1/M2/M3 “uncheatable tests” as gates
**Objective**: translate milestone tests into reproducible scripts/tests and run them in CI (or as clearly separated environment-dependent gates).

**Deliverables**
- A set of runnable commands (prefer `cargo test …` / `pytest …`) for each milestone gate.
- CI wiring:
  - PR-gated (fast, hermetic): unit/integration tests.
  - main-only or scheduled (requires infra): e2e/smoke tests.

**Acceptance criteria**
- A developer can run the same commands locally.
- CI fails when parity invariants regress.

**Evidence targets**
- `.github/workflows/ci.yml` (add/ensure targeted jobs)
- Existing Rust integration tests already run in CI:
  - `crates/arco-flow/tests/orchestration_automation_tests.rs`
  - `crates/arco-flow/tests/orchestration_sensor_tests.rs`
  - `crates/arco-flow/tests/orchestration_schema_tests.rs`

---

## dagster-parity-05 — Land current PRs (timer callback + orchestration invariants)
**Objective**: ensure the reliability foundation is merged before deeper parity work depends on it.

**Deliverables**
- Merge and stabilize the timer callback ingestion + orchestration invariants PRs.

**Acceptance criteria**
- CI green on main with these changes.
- Parity work items 06–11 list these as prerequisites where applicable.

---

## dagster-parity-06 — Schedules E2E
**Objective**: persisted definitions → ticks → runs → history APIs.

**Deliverables**
- Persisted schedule definitions (storage model + CRUD).
- Tick evaluation runtime (serverless timers or equivalent).
- History API surface that allows operators to answer: “what fired, when, and why?”

**Acceptance criteria (semantic)**
- Tick history is durable and queryable.
- Catch-up semantics are defined and tested.
- Duplicate delivery does not create duplicate ticks or runs.

**Evidence targets**
- ADR scope: `docs/adr/adr-024-schedule-sensor-automation.md`
- CI proofs (at least): idempotency keys + fold semantics + schema invariants.

---

## dagster-parity-07 — Sensors E2E
**Objective**: push/poll support, durable cursor, CAS/backoff semantics.

**Deliverables**
- Push sensor ingest (e.g. Pub/Sub) and poll sensor timers.
- Durable cursor and CAS semantics for poll sensors.
- run_key-based idempotency for triggered runs.

**Acceptance criteria (semantic)**
- Duplicate push delivery does not create duplicate effects.
- Poll sensors enforce CAS cursor updates.
- Cursor reset semantics are documented and tested.

**Evidence targets**
- `crates/arco-flow/tests/orchestration_sensor_tests.rs`

---

## dagster-parity-08 — Run UX gaps
**Objective**: selection semantics + rerun-from-failure/subset support.

**Deliverables**
- Selection syntax/contract (subset of assets/tasks).
- Rerun-from-failure semantics (same config; only failed subset).
- Operator surfaces (CLI/API) to exercise these workflows.

**Acceptance criteria**
- The uncheatable “selection does not auto-materialize downstream” invariant is enforced.
- Subset rerun is deterministic and auditable.

---

## dagster-parity-09 — Partitions/backfills parity
**Objective**: pause/resume, retry-failed partitions, mappings.

**Deliverables**
- Backfill controller/service with state machine.
- Chunk planning for large ranges/selectors.
- Pause/resume/cancel and retry-failed flows.
- Partition mapping semantics (as required).

**Acceptance criteria**
- Duplicate-safe state transitions.
- Retry-failed only affects failed partitions.

**Evidence targets**
- ADR scope: `docs/adr/adr-025-backfill-controller.md`

---

## dagster-parity-10 — Staleness report + reconciliation
**Objective**: EX-09 phased deliverable: staleness report first, reconciliation later.

**Deliverables**
- Read-only staleness report (“who is stale and why”).
- Reconciliation engine (optional auto-trigger, controlled/flagged).

**Acceptance criteria**
- Staleness is explainable, deterministic, and tied to partition status + upstream changes.

---

## dagster-parity-11 — Production hardening
**Objective**: HA/leader election, DR, load tests, runbooks/observability.

**Deliverables**
- Verified leader election and safe failover.
- DR/projection rebuild procedures.
- Load tests that cover controller throughput + event delivery duplication.
- Runbooks and core observability signals.

**Acceptance criteria**
- Clear SLOs and documented limits.
- Failover does not create duplicate committed effects.

---

## dagster-parity-12 — Stakeholder sign-off on declared parity exceptions
**Objective**: keep exception list stable and explicit.

**Note**: you indicated no tracker/approval is required right now. This item becomes relevant only when we want to make external parity claims.

**Deliverables**
- A stable set of exceptions in `docs/audits/arco-flow-dagster-exception-planning.md`.
- A short “public parity statement” (optional) enumerating what’s in scope vs exceptions.

**Acceptance criteria**
- Exceptions are explicit, consistent with the charter, and referenced by the parity matrix.
