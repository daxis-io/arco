# dagster-parity-03 — Parity Matrix

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`
- Matrix template: `docs/parity/dagster-parity-matrix-template.md`

## Goal
Produce a single parity matrix that maps:

Dagster capability → Arco implementation → status → proof/evidence → owner.

This matrix becomes the operational interface for parity work (planning, review, and proof).

## Non-Goals
- Perfect taxonomy on day 1.
- Recreating Dagster docs.
- Re-litigating exception scope (use exception planning doc).

## Deliverables
1. A concrete matrix file:
   - `docs/parity/dagster-parity-matrix.md`
2. A small set of matrix conventions:
   - consistent capability naming
   - status taxonomy (from the charter)
   - evidence link rules

## Work Breakdown
### Step 0 — Choose format
Default:
- Markdown table in `docs/parity/dagster-parity-matrix.md`

Optional (recommended for later automation):
- A CSV sibling `docs/parity/dagster-parity-matrix.csv`

### Step 1 — Seed the matrix (top-level domains)
Start with these sections:
- Scheduling
- Sensors
- Run identity (`run_key` idempotency + conflict detection)
- Partitions
- Backfills
- Partition status
- Staleness/reconciliation
- Operator UX (selection, reruns)
- Production hardening

### Step 2 — Define “capabilities” at semantic granularity
Guideline: a row should correspond to one testable semantic statement.
Examples:
- “Schedule tick history is durable and idempotent.”
- “Poll sensor cursor update uses CAS semantics.”
- “Backfill pause/resume is duplicate-safe.”

### Step 3 — Attach evidence links
For each row:
- Evidence (Code): primary modules
- Evidence (Tests): CI-gated tests
- Evidence (CI): which job/command

Use existing anchors where possible:
- Schedules: `crates/arco-flow/src/orchestration/controllers/schedule.rs`, `crates/arco-api/src/routes/orchestration.rs`
- Sensors: `crates/arco-flow/src/orchestration/controllers/sensor.rs`
- Backfills: `crates/arco-flow/src/orchestration/controllers/backfill.rs`
- Partition status: `crates/arco-flow/src/orchestration/controllers/partition_status.rs`
- run_key: `crates/arco-flow/src/orchestration/run_key.rs`

### Step 4 — Owner assignment
- If ownership is unknown, use `TBD`.
- Avoid “shared” unless a team explicitly owns it.

### Step 5 — Keep the matrix honest
- “Implemented” requires CI evidence.
- Anything without proof is “Partial/Designed/Planned”.
- Exceptions must link to `docs/audits/arco-flow-dagster-exception-planning.md`.

## Acceptance Criteria
- Every parity item 04–11 can be decomposed into matrix rows with clear proofs.
- Reviewers can quickly determine what changed in parity by diffing the matrix.

## Suggested Next Action
After seeding the matrix, immediately:
- pick 3–5 “Implemented” rows
- verify each has CI evidence
- downgrade any that don’t

This prevents the matrix from becoming aspirational.
