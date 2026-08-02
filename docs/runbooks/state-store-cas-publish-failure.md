# Runbook: Control-Store CAS Publication Failure

Failure state (Tier-1 control-store strategy, 2026-06-25, Failure States table):
"manifest write succeeds, pointer CAS fails — old state visible; orphan manifest
cleanup; caller retries on new head."

## Symptoms

- Control-store commits fail with `CatalogError::CasFailed` carrying the message
  `control MVP pointer CAS lost to a newer manifest`.
- Callers of `ControlMvpTxn::commit()` see errors while reads keep returning the
  previous (still consistent) state.
- Orphan objects accumulate under
  `state-store/control-mvp/{domain}/txlog/` and
  `state-store/control-mvp/{domain}/manifests/` that no pointer references.

## Detection

- Alert: `ArcoControlStoreCasPublishFailureRateHigh`
  (`infra/monitoring/alerts.yaml`, group `arco.state_store`).
- Secondary: `ArcoCasRetryRateHigh` (legacy `cas_retry_total`) if the same
  contention affects the current Tier-1 path.

## Diagnosis

Grounding: `crates/arco-catalog/src/state_store/control_mvp.rs`,
`ControlMvpTxn::commit_inner`. The publish protocol is:

1. write the immutable transaction object (`txlog/{tx_id}.json`, precondition
   `DoesNotExist`);
2. write the immutable manifest object (`manifests/{manifest_id}.json`,
   precondition `DoesNotExist`);
3. CAS-overwrite `current.pointer.json` with precondition
   `MatchesVersion(base pointer version)` (or `DoesNotExist` for the first
   commit). A precondition failure is the CAS loss.

Steps:

1. Inspect the current pointer and compare its `logical_sequence` with the
   failing writer's base:

   ```bash
   gcloud storage cat \
     "gs://${BUCKET}/tenant=${TENANT}/workspace=${WORKSPACE}/state-store/control-mvp/${DOMAIN}/current.pointer.json" | jq
   ```

2. Occasional CAS losses under concurrency are expected behavior: the losing
   writer must retry from `begin_control_txn`, which reloads the pointer and
   replays the new head (`load_current_base_state`).
3. A sustained failure rate means a competing writer is publishing against the
   same domain. Verify the sole-writer assumption: only the API service account
   holds write authority under `state-store/`
   (`infra/terraform/iam_conditions.tf`, binding `api_write_state_store`), so a
   second writer is either a second API deployment/revision targeting the same
   tenant/workspace/domain or an operator using API credentials. See
   `docs/runbooks/state-store-writer-fencing-loss.md`.
4. List orphan candidates: any `txlog/` or `manifests/` object whose id is not
   reachable from the current pointer's manifest `tx_refs` chain. Orphans are
   never visible (the read path only follows the pointer) and must not be
   projected or revalidated into state.

## Remediation

- Transient contention: retry the operation with the same request/idempotency
  key; `begin_control_txn` rebases on the new head automatically.
- Competing writer: stop the extra writer (scale the API service for the
  affected scope back to intended topology; revoke any ad-hoc credentials).
- Orphan artifacts: leave them in place. They are physically unreferenced and
  harmless to correctness. There is no automated orphan cleanup for
  `state-store/` yet (catalog GC deletes only under `snapshots/`, ledger, and
  old snapshot versions — `crates/arco-catalog/src/gc/collector.rs`); do not
  delete manually unless storage cost forces it and the object is provably
  unreferenced by every manifest and checkpoint.

## Current Wiring Status

Honest status as of 2026-07-30 (program audit): the control store has no
production callers — commits happen only in tests. The
`arco_state_store_cas_publish_*` metrics are reserved in
`crates/arco-catalog/src/metrics.rs` but nothing emits them yet, so the
detection alert cannot fire until the emitter is wired. The retry loop
described above is caller responsibility; no automated retry machinery exists.
