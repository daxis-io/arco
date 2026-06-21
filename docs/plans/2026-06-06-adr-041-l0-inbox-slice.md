# ADR-041 L0 Inbox Slice Plan

Date: 2026-06-06

## Goal

Implement the smallest production-grade ADR-041 slice that moves orchestration
hot-path storage toward tiered object-storage event logs without overclaiming
unimplemented L1/L2 compaction or changing callback visibility semantics.

## Current-State Constraints

- Existing worker callbacks still append one JSON object per event and trigger
  the current orchestration micro-compactor. That path remains the default until
  L0 inbox promotion to canonical L1/L2 is implemented and tested.
- Existing `state/orchestration/l0` means micro-compaction projection deltas,
  not ADR-041 `_inbox/orchestration` ingestion bundles. The new code must avoid
  ambiguous reuse of that path.
- Existing manifest and reader behavior remains pointer-first and compatible
  with legacy one-object orchestration ledger reads during migration.

## Selected Vertical Slice

1. Add canonical ADR-041 path builders for:
   - `_inbox/orchestration/run={run_id}/producer={producer_id}/attempt={attempt_id}/bundle={bundle_id}.parquet`
   - `receipts/orchestration/run={run_id}/producer={producer_id}/receipt_segment={receipt_id}.json`
   - `ledger/orchestration/segments/shard={shard_id}/seq={start_seq}-{end_seq}.parquet`
   - `ledger/orchestration/index/shard={shard_id}/current.json`
   - `ledger/orchestration/index/shard={shard_id}/history/{commit_id}.json`
2. Add an `orchestration::event_log` module in `arco-flow` with:
   - `L0BundleEvent`
   - `L0BundleMetadata`
   - `L0InboxBundle`
   - `L0InboxWriter`
   - typed validation errors
3. Implement create-if-absent L0 bundle writes through `ScopedStorage` and
   `WritePrecondition::DoesNotExist`.
4. Add tests first for:
   - path contracts;
   - bundle metadata derivation;
   - rejected mixed tenant/workspace/run scope;
   - rejected mixed `task_key` scope for the same attempt;
   - rejected bundle metadata outside the writer storage scope;
   - rejected duplicate `event_id`;
   - rejected duplicate `producer_id + producer_seq`;
   - ADR-listed Parquet object output for stored bundles;
   - duplicate bundle path does not overwrite existing content;
   - no writes to legacy `ledger/orchestration/{date}/{event_id}.json` from the
     new L0 writer.
5. Update ADR-041 docs only to mark this slice as implemented if code lands and
   verification passes. Do not mark callback routing, L1 promotion, L2
   projection, active DuckDB actors, trusted direct upload, or Quack fan-in as
   shipped.

## Test Commands

- `cargo test -p arco-core --test flow_paths_contracts`
- `cargo test -p arco-flow --test orchestration_event_log_tests`
- Targeted existing callback/ledger tests touched by integration points, if
  any production integration is changed.
- Required goal gates after the slice compiles:
  - `cargo xtask adr-check`
  - `cargo xtask repo-hygiene-check`

## Migration and Rollback

- Migration is additive. The existing ledger writer, current compactor, and
  manifest-published readers remain unchanged.
- Rollback is removing the additive L0 inbox module and path helpers. No runtime
  callback path depends on the new writer in this slice.
- Compatibility is preserved because no existing one-object ledger reader or
  callback writer is removed.

## Remaining ADR-041 Phases

- Callback ingest routing to L0 bundles with receipt evidence.
- Stateless compactor promotion from accepted L0 bundles to L1 segments.
- L1 shard-index CAS with stale-fencing tests.
- L2 projection publication and `system.orchestration.*` manifest proof.
- Missed notification recovery sweeps for `_inbox/orchestration`.
- Active-run DuckDB actor with lease/replay/crash-recovery proof.
- Private Quack fan-in with authorization and callback fallback.
