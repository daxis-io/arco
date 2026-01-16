# dagster-parity-05 — Land Reliability Foundation PRs

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`

## Goal
Merge the reliability foundation work (timer callback + orchestration invariants) before higher-level parity work depends on it.

This item is explicitly about **sequencing and stability**.

## Non-Goals
- Adding new parity features.
- Broad refactors.

## Deliverables
- The currently-open PRs for:
  - timer callback ingestion
  - orchestration invariants
  are merged (or reworked and merged) into main.

## Work Breakdown
### Step 0 — Identify the PRs
- List PR URLs here (TBD).
- Record what each PR changes (modules/tests).

### Step 1 — Confirm prerequisites and invariants
For each PR:
- Identify what parity items depend on it (likely 06–11).
- Ensure the PR includes (or is paired with) CI-gated tests.

### Step 2 — Review for semantic risk
- Confirm idempotency semantics are consistent:
  - event idempotency
  - timer dedupe
  - run_key dedupe
- Confirm out-of-order delivery is handled safely.

### Step 3 — Merge sequencing
- Merge the most foundational PR first (usually invariants → timers → higher-level).
- If PRs conflict, rebase carefully and keep changes minimal.

### Step 4 — Post-merge stabilization
- Run full CI.
- Update parity matrix rows that now become “Implemented” with evidence links.

## Acceptance Criteria
- CI green on main.
- Parity items 06–11 can rely on stable timer callback and invariant behavior.

## Evidence Targets
- CI: `.github/workflows/ci.yml`
- Correctness invariants tests: `crates/arco-flow/tests/orchestration_correctness_tests.rs`
