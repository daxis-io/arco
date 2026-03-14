# dagster-parity-02 — Repo-Addressable Parity Audit

## References

- Charter: `docs/parity/dagster-parity-01-charter.md`
- Exceptions: `docs/audits/arco-flow-dagster-exception-planning.md`
- CI: `.github/workflows/ci.yml`

## Goal

Make parity claims provable from tracked repository artifacts only.

## Deliverables

1. Parity documentation under `docs/parity/` with explicit evidence fields.
2. Clear rule set: implemented claims require code + tests + CI command.
3. Migration away from legacy planning-only sources.

## Canonical Evidence Sources

- Code: `crates/**`, `python/**`
- ADRs: `docs/adr/**`
- Parity docs: `docs/parity/**`
- Exception planning: `docs/audits/arco-flow-dagster-exception-planning.md`
- CI wiring: `.github/workflows/ci.yml`

## Migration Map (Legacy Planning to Canonical Sources)

| Legacy content type | Canonical replacement |
|---|---|
| Historical parity scope prose | `docs/parity/dagster-parity-01-charter.md` + `docs/parity/dagster-parity-03-parity-matrix.md` |
| Historical automation/controller sketches | `docs/adr/adr-024-schedule-sensor-automation.md`, `docs/adr/adr-025-backfill-controller.md`, `docs/adr/adr-026-partition-status-tracking.md` |
| Historical hardening notes | ADR updates + CI-gated tests in `crates/arco-flow/tests/**` |
| Historical implementation prompts/checklists | CI commands and test targets in `.github/workflows/ci.yml` + parity gates docs |

## Hard Rules

- No implemented parity claim without code, tests, and CI command references.
- Legacy planning-only artifacts are background context, not proof.
- Evidence links must point to tracked paths present in CI checkouts.

## Acceptance Criteria

- A contributor can determine parity status from:
  - `docs/parity/dagster-parity-01-charter.md`
  - `docs/parity/dagster-parity-03-parity-matrix.md`
  - linked code/tests/CI paths.
- No parity proof depends on removed legacy planning docs.
