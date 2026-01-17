# dagster-parity-08 — Run UX Gaps (Selection + Reruns)

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`
- TriggerRun API: `crates/arco-api/src/routes/orchestration.rs`
- run_key semantics: `crates/arco-flow/src/orchestration/run_key.rs`
- CLI: `crates/arco-cli/src/commands/run.rs`

## Goal
Close user-facing run UX gaps that define Dagster parity for operators:

- selection semantics (subset execution)
- rerun-from-failure / subset reruns

## Non-Goals
- Dagit parity.
- Auto-materialize (phased elsewhere).

## Deliverables
1. A selection contract:
   - how a user selects a subset of assets/tasks
   - how that affects plan generation
2. Rerun contract:
   - rerun from failure
   - rerun only a subset
   - deterministic run_key / run lineage semantics
3. Proof gates:
   - tests that prevent regression (uncheatable)

## Work Breakdown
### Step 0 — Lock selection semantics
- Define “selection” as:
  - the set of roots to execute
  - and the closure rules (do we include upstream dependencies? downstream? neither?)

Parity-critical invariant (from prior parity docs):
- Selection must not silently auto-materialize downstream unless explicitly requested.

### Step 1 — Implement selection in planning
- Ensure plan generation respects selection.
- Ensure the plan fingerprint includes selection parameters.

Likely touch points:
- API request schema for TriggerRun
- orchestration plan builder

### Step 2 — Implement rerun semantics
- Define a rerun as:
  - same logical job/asset graph
  - new run_id (or derived lineage)
  - only failed subset re-executed
- Ensure run history shows lineage.

### Step 3 — Operator surface
- CLI support:
  - run with selection
  - rerun failed
  - show parent/child run relation

### Step 4 — Proof (tests + CI)
Add “uncheatable” tests proving:
- selecting `{A}` does not execute `{B,C}` when downstream exists
- rerun-from-failure does not re-run succeeded tasks
- run_key behavior remains deterministic

Suggested locations:
- `crates/arco-flow/tests/orchestration_selection_tests.rs` (new)
- `python/arco/tests/integration/` (optional, if Python CLI is used)

## Acceptance Criteria
- Selection semantics are explicit and enforced.
- Rerun workflows behave as operators expect.
- CI prevents regressions.

## Risks / Edge Cases
- Ambiguity around upstream dependency inclusion (must be explicit).
- Deterministic lineage identifiers.
- Interaction with partitions/backfills.
