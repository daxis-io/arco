# Runbook: Control-Store Replay Latency / Bytes Over Budget

Prototype budgets (Tier-1 control-store strategy, 2026-06-25): cold writer
startup to first write-ready state <= 2 s; maximum manifest-reachable replay on
cold start <= 64 MiB; "control compaction backlog — alert before replay budget
is exceeded."

## Symptoms

- Cold starts of a control-store domain get slower: every
  `begin_control_txn`/read loads the manifest's L1 `base_states` and replays
  the L0 transaction suffix reachable from the current manifest.
- Replay p95 approaches or exceeds 2 s; manifest-reachable bytes approach the
  64 MiB promotion budget.
- Cost/latency grows with the un-consolidated L0 suffix: each commit adds a
  `tx_refs` entry until layout maintenance publishes an equivalent L1 anchor.

## Detection

Alerts (`infra/monitoring/alerts.yaml`, group `arco.state_store`):

- `ArcoControlStoreReplayLatencyHigh` — replay p95 above the 2 s cold-start
  budget;
- `ArcoControlStoreReplayBytesNearBudget` — replay bytes above 48 MiB (75% of
  the 64 MiB budget), firing before the budget is breached as the strategy
  requires.

The corresponding promotion-gate measurements are
`ManifestReachableReplayBytes` and
`CompactionBacklogBeforeReplayBudgetBreach`
(`crates/arco-catalog/src/state_store/promotion_gate.rs`).

## Diagnosis

Code reference (internal, not operator-callable): `replay_manifest` in
`crates/arco-catalog/src/state_store/control_mvp.rs`. It loads the
manifest's `base_states` (consolidated L1
segments, verified through their checksummed indexes), then loads and applies
**every** `tx_refs` entry in the L0 suffix, and verifies the result against
`manifest.state_checksum_sha256`. Replay is therefore bounded by layout
maintenance, not by checkpoints: commits durably request maintenance at 16
reachable L0 segments and fail closed with `MaintenanceBackpressure` at 32
(`L0_MAINTENANCE_INTENT_THRESHOLD` / `L0_MAINTENANCE_BACKPRESSURE_THRESHOLD`).
Checkpoint objects (`ArcoStateAdmin::checkpoint` writes them) pin retained
reads but do not shorten current-head replay.

1. Measure the actual chain: read `head/current.json`, fetch the manifest,
   count `base_states` and `tx_refs`, and size the L1 and L0 segment prefixes
   (which also include unreachable segments not yet collected):

   ```bash
   BASE="gs://${BUCKET}/tenant=${TENANT}/workspace=${WORKSPACE}/control/v1/domains/${DOMAIN}"
   gcloud storage cat "${BASE}/head/current.json" | jq -r .manifest_id
   gcloud storage cat "${BASE}/manifests/${MANIFEST_ID}.json" | jq '{l1: (.base_states | length), l0: (.tx_refs | length)}'
   gcloud storage du "${BASE}/segments/l1/" "${BASE}/segments/l0/"
   ```

2. Identify the growth driver: a chatty caller committing many small
   transactions, or a domain that simply accumulated history.
3. Confirm whether layout maintenance is keeping up: `arco_state_store_l0_segments`
   should drop after each worker run, and a rising count together with
   `arco_state_store_maintenance_backpressure_total` incrementing means the
   worker is not running or its jobs are not publishing.

## Remediation

- Run layout maintenance: as of 2026-09-23 the scheduled
  `arco-control-store-worker` job (`docs/runbooks/control-store-worker.md`)
  drives `DurableMaintenanceWorker::{prepare_at,start_at,resume_at,advance_at,publish_at}`
  (`crates/arco-catalog/src/state_store/control_mvp/maintenance.rs`), which
  publishes an equivalent L1 anchor through exact head CAS without advancing
  the logical sequence. Execute the job manually if the schedule is behind.
- Short term while maintenance catches up: reduce commit volume on the
  affected domain (batch writes into fewer transactions).
- Taking checkpoints helps `read_checkpoint` consumers pin known-good states
  but does **not** reduce current-head replay cost — do not treat checkpoint
  cadence as a mitigation for this alert.
- A domain that breaches the 64 MiB budget is out of the promotion contract
  and must not take on new write traffic until maintenance has consolidated
  it.
- Record breaches in the domain's promotion evidence: the Phase 3C gate
  (`promotion_gate.rs`) treats these measurements as required inputs.

## Retention horizon (second maintenance kind)

As of 2026-09-26 `DurableMaintenanceWorker` has a second job kind beside
consolidation: `prepare_horizon_at` admits a `RetentionHorizon` job that
renders the current head into fresh L1 shards without expired rows (expiry
hint older than one hour before the job's clock) and tombstones at or below
the certified horizon, then runs the same `start_at`/`advance_at`/`publish_at`
cycle. It needs no maintenance intent, returns no plan when nothing is
eligible, and bounds retained rows rather than the L0 suffix; it is not a
mitigation for this alert. As of 2026-09-27 the scheduled worker job runs it
once per domain per run, after that domain's consolidation slot
(`docs/runbooks/control-store-worker.md`). Contract:
`../plans/state-store-retention-format-v1.md`.

The two kinds share the head's layout generation and claim the workspace
retention epoch at activation, so at most one of two concurrent jobs
publishes; the other fails its compatibility check with `PreconditionFailed`
and is abandoned. For a horizon job:

- `Superseded` (`MaintenanceStatus::Superseded` in `maintenance.rs`) means
  what it means for consolidation: the head moved past the job's source and
  the 24 h descriptor lifetime expired before a regenerated publication
  landed. Before expiry a consumed attempt returns to `ReadyToPublish` and
  the next `publish_at` regenerates over the new head. `publish_at` also refuses
  a horizon job with `PreconditionFailed` (the persisted status is not changed)
  when a commit after preparation rewrote a key the admitted plan purges; abandon it and call
  `prepare_horizon_at` again. The worker reports this refusal as `deferred`
  and replays the same job on every run until its persisted record is
  older than the 24 h descriptor lifetime; see the worker runbook's known
  limitations.
- A stuck retention epoch (`stuck_epoch` in the worker's epoch phase, see
  `docs/runbooks/control-store-worker.md`) blocks horizon activation exactly
  as it blocks consolidation: `start_at` claims the epoch under the retention
  lock and fails closed while a foreign epoch is in flight.

## Outbox trim and retained rows

As of 2026-09-27 the worker's `trim` phase removes acknowledged catalog
projection outbox records after each drain
(`CatalogProjectionMaterializer::trim_once`; the fixed-consumer path that
installs no binding metadata in the catalog root). Outbox rows therefore no
longer accumulate in the replayed catalog state beyond one run's backlog:
trimmed records are folded out at replay, and the trim commit itself is one
L0 segment the next consolidation folds. A published retention-horizon job
additionally drops expired rows and tombstones no retained reader can
observe. Both bound the retained rows a replay materializes, not the
per-commit cost: format 9 still replays the whole retained state
(`base_states` plus the L0 suffix) on every commit, so the 2 s and 64 MiB
budgets remain governed by consolidation cadence and by how much live state
the domain holds.

## Current Wiring Status

Status as of 2026-09-27: the `arco_state_store_replay_duration_seconds` and
`arco_state_store_replay_bytes` emitters exist, along with
`arco_state_store_l0_segments` and
`arco_state_store_maintenance_backpressure_total`, so the alerts can fire once
a root is bound. The control store is route-wired for one exact root behind
`ARCO_CATALOG_CONTROL_V1_*`, legacy by default, not provider-qualified, and not
authoritative on any deployed root; the promotion gate has not run against
real provider measurements. Maintenance (consolidation and retention horizon)
and the catalog outbox trim are scheduled through the cron-driven worker job,
not through queue-driven wake.
