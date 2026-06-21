# ADR-041 Execution Goal

Use this instruction file as the execution brief for implementing ADR-041:
Tiered Object-Storage Orchestration Event Log.

## Objective

Research, design, and implement ADR-041 in the Arco repo with production-grade
engineering discipline, current primary-source research, strong tests, and clear
boundaries between implemented behavior and future gated optimizations.

## Context

- Work in `/Users/ethanurbanski/arco`.
- Start from `docs/adr/adr-041-tiered-object-storage-orchestration-event-log.md`.
- Treat ADR-041 as accepted architecture that refines ADR-018 and ADR-020 for
  orchestration hot-path storage.
- Preserve current callback, task-token, and active-attempt semantics as the
  default worker ingestion path.
- Preserve unrelated dirty worktree changes.
- Do not claim active DuckDB actors, Quack fan-in, or trusted direct upload are
  shipped unless the required lease, replay, fencing, authorization, fallback,
  and recovery tests exist.

## Research Requirements

- Re-read ADR-041, ADR-018, ADR-020, ADR-023, ADR-032 immutable manifest pointer
  ADR, ADR-034, ADR-035, and ADR-040.
- Inspect current orchestration storage, callback, compaction, dispatch, worker
  contract, manifest, CAS, and system-table code before proposing edits.
- Research current primary-source standards and documentation for:
  - object-store conditional writes and consistency for GCS and S3;
  - object notification failure modes and anti-entropy patterns;
  - Parquet schema and versioning practices for event logs and projections;
  - lease and fencing-token patterns for distributed writers;
  - DuckDB and Quack capabilities and security boundaries.
- Use primary sources only for external claims. Record links and summarize the
  engineering implications in a short research note before implementation.

## Execution Requirements

- First produce a concrete implementation plan with phases, exact files, tests,
  migration concerns, and rollback or compatibility strategy.
- Then execute the smallest production-grade vertical slice that advances
  ADR-041 without overclaiming future features.
- Prefer test-driven development:
  - write failing tests for authority boundaries, schema validation, dedupe, CAS,
    fencing, manifest visibility, and worker cleanup semantics;
  - implement the minimum production code needed to pass each test;
  - broaden coverage where behavior crosses module boundaries.
- Keep object storage as durable authority.
- Workers and ingest endpoints may write only L0 inbox bundles and allowed
  receipt evidence.
- Only the compactor may write L1 canonical segments, L1 shard indexes, L2
  projections, manifests, snapshots, and quarantine outputs.
- Use create-if-absent semantics for bundle, segment, and projection writes.
- Use compare-and-swap for manifest and shard-index updates.
- Maintain compatibility with existing one-object-per-event orchestration ledger
  reads during migration.

## Quality Bar

- Use project-native abstractions and naming.
- Keep the implementation scoped; do not refactor unrelated code.
- Avoid speculative runtime docs. Docs must distinguish accepted architecture,
  implemented behavior, and gated future optimizations.
- Add tests before production code where practical.
- Include negative tests for unauthorized writers, stale fencing tokens,
  duplicate bundles, stale attempts, manifest CAS loss, and missed notification
  recovery.
- Do not route business transaction rows through the orchestration ledger.
- Do not make reader-triggered lazy compaction responsible for dispatch, retries,
  cancellation, timers, or worker cleanup.

## Verification

- Run targeted tests for each changed module.
- Run relevant package or workspace tests.
- Run:
  - `cargo xtask adr-check`
  - `cargo xtask repo-hygiene-check`
  - any existing integrity, engine-boundary, or orchestration-specific gate
    touched by the change.
- If a command fails, debug from the failing evidence. Do not summarize success
  until the command exits cleanly.
- Report deterministic local proof separately from any live cloud or object-store
  proof.

## Deliverables

- Research note with primary-source links and concrete implications.
- Implementation plan.
- Code and tests for the selected ADR-041 implementation slice.
- Updated docs only where behavior is implemented.
- Verification transcript with commands, exit status, and key results.
- Clear list of remaining ADR-041 phases and blockers.
