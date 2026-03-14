# Orchestration

The orchestration domain plans and executes data work with deterministic behavior and replay-safe identity semantics.

## Core Responsibilities

- Compile asset/work definitions into deterministic execution plans.
- Track run/task lifecycle transitions with explicit state semantics.
- Persist execution events for replay, debugging, and parity checks.
- Enforce idempotency keys and deterministic partition identity.

## Execution Semantics

- Plans are generated from canonical inputs.
- Events are append-first and suitable for projection/rebuild workflows.
- Scheduler and automation controls are represented as explicit domain entities.

## Reliability Requirements

- Stable IDs and canonical encoding across components.
- Deterministic dependency satisfaction and partition status tracking.
- Clear failure states, retries, and reconciliation paths.

## Canonical References

- `docs/adr/adr-010-canonical-json.md`
- `docs/adr/adr-011-partition-identity.md`
- `docs/adr/adr-022-dependency-satisfaction.md`
- `docs/adr/adr-024-schedule-sensor-automation.md`
- `docs/adr/adr-025-backfill-controller.md`
- `docs/adr/adr-026-partition-status-tracking.md`
