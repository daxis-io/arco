# ADR-034 PI-2 Adoption Inventory

Historical note as of April 8, 2026: this inventory records the PI-2 rollout snapshot. The later
PI-3 cleanup changed the steady-state repair automation default to `full` and removed the
compatibility helpers called out below.

## Scope And Method

This inventory is repo-grounded. It was produced from targeted searches over the active writer and
maintenance surfaces in:

- `crates/arco-api/src/routes/`
- `crates/arco-api/src/orchestration_compaction.rs`
- `crates/arco-flow/src/compaction_client.rs`
- `crates/arco-flow/src/orchestration/`
- `crates/arco-flow/src/bin/`
- `crates/arco-compactor/src/`
- `crates/arco-catalog/src/`

Searches used:

```bash
rg -n "append_events_and_compact|append_event_and_compact|compact_events_fenced|sync_compact|rebuild_from_ledger_manifest_path|compact_events_with_epoch|repairScope|repair_scope" crates docs
rg -n "\.append_all\(|\.append\(" crates/arco-api/src crates/arco-flow/src crates/arco-compactor/src crates/arco-catalog/src
rg -n "with_sync_compactor\(|sync_compactor\(\)" crates/arco-api/src/routes crates/arco-catalog/src/writer.rs
```

## PI-1 Gate Verification

PI-1 was mostly present in-repo, but one blocker directly affected PI-2 safety:

- `crates/arco-flow/src/bin/arco_flow_compactor.rs`
  - `/internal/reconcile` did not expose an explicit orchestration repair scope and defaulted to broader repair behavior through the old `repair()` path
  - PI-2 fixes:
    - added `repairScope`
    - defaulted orchestration reconcile and automation to `current_head_only`
    - kept `full` as an explicit opt-in

## Migrated Active Writer Callsites

Orchestration API writers already on the fenced helper path:

- `crates/arco-api/src/routes/orchestration.rs`
- `crates/arco-api/src/routes/manifests.rs`
- `crates/arco-api/src/orchestration_compaction.rs`

Shared orchestration writer helper used by PI-2 flow services:

- `crates/arco-flow/src/orchestration/flow_service.rs`
  - now guarantees visible success even when `ARCO_FLOW_COMPACTOR_URL` is unset by using inline fenced compaction instead of append-only behavior

Flow-service writers migrated in PI-2:

- `crates/arco-flow/src/bin/arco_flow_automation_reconciler.rs`
  - pending automation events now use `append_events_and_compact(...)`
- `crates/arco-flow/src/bin/arco_flow_dispatcher.rs`
  - ready, dispatch, and timer emission paths now use `append_events_and_compact(...)`
- `crates/arco-flow/src/bin/arco_flow_sweeper.rs`
  - sweep-emitted orchestration events now use `append_events_and_compact(...)`
- `crates/arco-flow/src/bin/arco_flow_timer_ingest.rs`
  - timer-fired writes now use `append_events_and_compact(...)`

Catalog API writer paths already on the shared fenced contract:

- `crates/arco-api/src/routes/catalogs.rs`
- `crates/arco-api/src/routes/lineage.rs`
- `crates/arco-api/src/routes/namespaces.rs`
- `crates/arco-api/src/routes/tables.rs`
- `crates/arco-api/src/routes/uc/mod.rs`
- `crates/arco-catalog/src/writer.rs`
  - active writer methods acquire fencing and issue `SyncCompactRequest` commits

Catalog maintenance/service paths already using the fenced contract:

- `crates/arco-compactor/src/sync_compact.rs`
- `crates/arco-catalog/src/tier1_compactor.rs`

## Migrated Maintenance And Repair Flows

Catalog:

- `crates/arco-compactor/src/main.rs`
  - `/internal/reconcile` keeps explicit `repairScope`
  - automated repair executor added with `disabled|dry_run|enforce`, explicit domains, and current-head-only default

Orchestration:

- `crates/arco-flow/src/bin/arco_flow_compactor.rs`
  - `/internal/reconcile` now supports explicit `repairScope`
  - automated repair executor added with `disabled|dry_run|enforce` and current-head-only default
- `crates/arco-flow/src/orchestration/compactor/reconciler.rs`
  - repair logic now supports `CurrentHeadOnly` vs `Full`

## Compatibility-Only Callsites Left For PI-3

This section is historical PI-2 inventory. Those compatibility surfaces were removed by the later
PI-3 cleanup:

- `crates/arco-flow/src/bin/arco_flow_compactor.rs`
  - the old unfenced `compact_events_with_epoch(...)` and
    `rebuild_from_ledger_manifest_path(...)` helper framing is no longer present
  - active runtime entrypoints are the canonical fenced helpers
- `crates/arco-flow/src/compaction_client.rs`
  - the unfenced `compact_orchestration_events(...)` shim was removed
- stored/public `epoch` field alias
  - the compatibility alias was removed from active request handling

## Intentionally Deferred Items

Deferred to PI-3 or later, not PI-2:

- compatibility removal for orchestration compactor request shapes
- compatibility removal for stored/public `epoch`
- root transactions
- any default-on global fenced cutover

No remaining active PI-2 writer callsites were found that still bypass the shared fenced contract.
