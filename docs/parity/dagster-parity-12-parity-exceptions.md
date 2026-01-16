# dagster-parity-12 — Parity Exceptions (EX-01..EX-09)

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`
- Exceptions: `docs/audits/arco-flow-dagster-exception-planning.md`

## Goal
Keep parity exceptions explicit, stable, and consistent with the declared Dagster OSS parity scope.

You indicated we **do not need a tracker or approval process** right now. This plan is therefore focused on documentation hygiene and consistency.

## Non-Goals
- Adding a formal stakeholder approval workflow.
- Expanding the exception set unless required.

## Deliverables
1. A canonical list of exceptions (already exists):
   - `docs/audits/arco-flow-dagster-exception-planning.md`
2. Matrix integration:
   - parity matrix rows marked “Exception” link back to the relevant EX card.
3. A consistent public posture (optional):
   - a short “what we match vs don’t match” statement if needed later

## Work Breakdown
### Step 0 — Normalize exception references
- Ensure every exception has a stable ID (EX-01..EX-09).
- Ensure matrix rows referencing exceptions use the same IDs.

### Step 1 — Ensure exceptions are compatible with the charter
- Confirm the charter scope doesn’t conflict with exceptions.
- If there is a conflict, update the charter or exception doc.

### Step 2 — Keep exceptions from leaking into scope
- When implementing parity features, add explicit “non-goals” sections in per-item plans.
- Prefer tests that ensure exception behavior is not accidentally implemented.

## Acceptance Criteria
- Exceptions are easy to find and cross-reference.
- No parity claim is made for exception items.

## Optional Enhancements (later)
- A lightweight `cargo xtask` that validates:
  - all Exception matrix rows reference a known EX id
  - no Implemented row references an EX item
