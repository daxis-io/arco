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

Grounding: `crates/arco-catalog/src/state_store/control_mvp.rs`.
`replay_manifest` loads the manifest's `base_states` (consolidated L1
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

## Current Wiring Status

Status as of 2026-09-23: the `arco_state_store_replay_duration_seconds` and
`arco_state_store_replay_bytes` emitters exist, along with
`arco_state_store_l0_segments` and
`arco_state_store_maintenance_backpressure_total`, so the alerts can fire once
a root is bound. The control store is route-wired for one exact root behind
`ARCO_CATALOG_CONTROL_V1_*`, legacy by default, not provider-qualified, and not
authoritative on any deployed root; the promotion gate has not run against
real provider measurements. Maintenance is scheduled through the cron-driven
worker job, not through queue-driven wake.
