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
  `control/v1/domains/{domain}/transactions/`,
  `control/v1/domains/{domain}/segments/l0/`,
  `control/v1/domains/{domain}/indexes/`, and
  `control/v1/domains/{domain}/manifests/` that no head references (all
  control-store paths in this runbook are relative to
  `tenant={tenant}/workspace={workspace}/`).

## Detection

- Alert: `ArcoControlStoreCasPublishFailureRateHigh`
  (`infra/monitoring/alerts.yaml`, group `arco.state_store`).
- Secondary: `ArcoCasRetryRateHigh` (legacy `cas_retry_total`) if the same
  contention affects the current Tier-1 path.

## Diagnosis

Code reference (internal, not operator-callable): `ControlMvpTxn::commit_inner`
in `crates/arco-catalog/src/state_store/control_mvp.rs`. The publish protocol
is:

1. write the immutable transaction object (`transactions/{tx_id}.json`,
   precondition `DoesNotExist`);
2. write the immutable L0 segment and its index (`segments/l0/{id}.arrow`,
   `indexes/{id}.idx`, precondition `DoesNotExist`), plus any L1 anchor
   segments the candidate renders (`segments/l1/{id}.arrow`,
   `indexes/{id}.idx`, create-if-absent, byte-identical collisions tolerated);
3. write the immutable manifest object (`manifests/{manifest_id}.json`,
   precondition `DoesNotExist`);
4. CAS-overwrite `head/current.json` — the only mutable authority object —
   with
   precondition `MatchesVersion(base head version)` (or `DoesNotExist` for the
   first commit). A precondition failure is the CAS loss.

Steps:

1. Inspect the current head and compare its `logical_sequence` with the
   failing writer's base:

   ```bash
   gcloud storage cat \
     "gs://${BUCKET}/tenant=${TENANT}/workspace=${WORKSPACE}/control/v1/domains/${DOMAIN}/head/current.json" | jq
   ```

2. Occasional CAS losses under concurrency are expected behavior: the losing
   writer must retry from `begin_control_txn`, which reloads the head and
   replays the new base (internally `load_current_base_state`).
3. A sustained failure rate means a competing writer is publishing against the
   same domain. Verify the sole-writer assumption: only the API service account
   holds write authority under `control/`
   (`infra/terraform/iam_conditions.tf`, binding `api_write_state_store`,
   conditioned on the `control/` prefix as of 2026-09-23), so a second writer
   is either a second API deployment/revision targeting the same
   tenant/workspace/domain or an operator using API credentials. The scheduled
   `arco-control-store-worker` job runs under the same service account and
   publishes maintenance and acknowledgement heads through exact CAS; losing
   an occasional race to an API commit is expected for it and does not count
   as a competing writer. See
   `docs/runbooks/state-store-writer-fencing-loss.md`.
4. List orphan candidates: any `transactions/`, `segments/`, `indexes/`, or
   `manifests/` object whose id is not reachable from the current head's
   manifest (its `base_states` L1 anchors plus the `tx_refs` suffix) or from a
   retained checkpoint or token pin. Orphans are never visible (the read path
   only follows the head) and must not be projected or revalidated into state.

## Remediation

- Transient contention: retry the operation with the same request/idempotency
  key; `begin_control_txn` rebases on the new head automatically.
- Competing writer: stop the extra writer (scale the API service for the
  affected scope back to intended topology; revoke any ad-hoc credentials).
- Orphan artifacts: leave them in place. They are physically unreferenced and
  harmless to correctness. Conservative control-store GC
  (`ControlMvpMaintenanceWorker::plan_gc_page_at` / `collect_gc_page_at`,
  `crates/arco-catalog/src/state_store/control_mvp.rs`) deletes unreachable
  candidates only once they are seven days old, retains token and checkpoint
  pins for 30 days, and revalidates the head and retention epoch before each
  deletion; as of 2026-09-23 the scheduled `arco-control-store-worker` job
  runs it for the `catalog` and `projection-outbox-acks` domains
  (`docs/runbooks/control-store-worker.md`). The generic catalog GC
  (`crates/arco-catalog/src/gc/collector.rs`) never deletes under
  `control/v1/`. Do not delete manually.

## Current Wiring Status

Status as of 2026-09-23: `arco-api` selects the `control/v1` catalog
authority for exactly one root configured through
`ARCO_CATALOG_CONTROL_V1_TENANT_ID`, `ARCO_CATALOG_CONTROL_V1_WORKSPACE_ID`, and
`ARCO_CATALOG_CONTROL_V1_CURSOR_KEY` (`crates/arco-api/src/config.rs`); native,
UC, and Iceberg catalog routes go through it
(`crates/arco-api/src/routes/catalog_authority.rs`). Every other root defaults
to the legacy path, no deployed environment sets those variables, no provider
adapter is live-qualified, and the control store is not authoritative on any
deployed root. The `arco_state_store_cas_publish_total` and
`arco_state_store_cas_publish_failures_total` emitters exist as of 2026-09-23
(`crates/arco-catalog/src/metrics.rs`), so the detection alert can fire once a
root is bound. The rebase-and-retry loop described above runs inside the API's
1.5-second catalog conflict budget; exhaustion returns a retryable authority
conflict with `Retry-After` rather than a partial success.
