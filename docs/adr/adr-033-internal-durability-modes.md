# ADR-033: Internal Durability Modes for Orchestration Compaction

## Status

Proposed

## Context

Compaction acknowledgement semantics previously implied a single durability level:
acknowledge only after manifest publish was visible. Internal workflows need a
configurable policy to trade latency vs visibility guarantees without exposing
unstable public API knobs yet.

## Decision

Arco introduces an internal durability mode policy for orchestration compaction.

### Modes

- `Visible` (default): acknowledge after pointer CAS publish succeeds.
- `Persisted`: acknowledge after immutable manifest snapshot write succeeds, even if
  pointer CAS visibility has not advanced yet.

### Watermarks

Watermarks now track both commit and visibility boundaries:

- `last_committed_event_id`
- `last_visible_event_id`

For backward compatibility, `events_processed_through` is maintained as an alias of
`last_visible_event_id` in visible mode.

### Initial scope

- Internal-only configuration via compactor construction.
- Tier-1 defaults remain conservative (`Visible`).
- Legacy stable manifest path remains mirrored while migration is active.

## Consequences

- Pros:
  - Internal services can choose latency/visibility trade-offs intentionally.
  - Control plane can distinguish “durably recorded” from “read-visible”.
  - Migration path toward richer public durability contracts is unblocked.
- Trade-offs:
  - More complex operational reasoning (committed vs visible lag).
  - Requires visibility-lag monitoring to detect prolonged CAS contention.
