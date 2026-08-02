# Runbook: Control-Store Replay Latency / Bytes Over Budget

Prototype budgets (Tier-1 control-store strategy, 2026-06-25): cold writer
startup to first write-ready state <= 2 s; maximum manifest-reachable replay on
cold start <= 64 MiB; "control compaction backlog — alert before replay budget
is exceeded."

## Symptoms

- Cold starts of a control-store domain get slower: every
  `begin_control_txn`/read replays the full transaction chain reachable from
  the current manifest.
- Replay p95 approaches or exceeds 2 s; manifest-reachable bytes approach the
  64 MiB promotion budget.
- Cost/latency grows linearly with commit count for the domain, because the
  MVP manifest carries the complete `tx_refs` history.

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
`replay_manifest` loads and applies **every** `tx_refs` entry, then verifies
the result against `manifest.state_checksum_sha256`. Checkpoint objects exist
(`ArcoStateAdmin::checkpoint` writes them), but replay is not
checkpoint-anchored: nothing bounds the transaction suffix that must be
replayed. This is the known Phase 3B gap (checkpoint-anchored bounded replay,
issue #334).

1. Measure the actual chain: read `current.pointer.json`, fetch the manifest,
   count `tx_refs`, and sum the sizes of the referenced `txlog/` objects:

   ```bash
   BASE="gs://${BUCKET}/tenant=${TENANT}/workspace=${WORKSPACE}/state-store/control-mvp/${DOMAIN}"
   gcloud storage cat "${BASE}/current.pointer.json" | jq -r .manifest_id
   gcloud storage cat "${BASE}/manifests/${MANIFEST_ID}.json" | jq '.tx_refs | length'
   gcloud storage du "${BASE}/txlog/"
   ```

2. Identify the growth driver: a chatty caller committing many small
   transactions, or a domain that simply accumulated history.
3. Confirm whether the budget pressure is projected to breach before the
   bounded-replay fix can land for the domain.

## Remediation

- Short term (all that exists today): reduce commit volume on the affected
  domain (batch writes into fewer transactions); no operational lever can
  shorten an existing chain, because compaction of the manifest chain is
  unimplemented.
- Taking checkpoints helps `read_checkpoint` consumers pin known-good states
  but does **not** reduce current-head replay cost — do not treat checkpoint
  cadence as a mitigation for this alert.
- Real fix: checkpoint-anchored bounded replay (materialized state plus a
  bounded transaction suffix), the open Phase 3B completion item (#334). A
  domain that breaches the 64 MiB budget before that lands is out of the
  promotion contract and must not take on new write traffic.
- Record breaches in the domain's promotion evidence: the Phase 3C gate
  (`promotion_gate.rs`) treats these measurements as required inputs.

## Current Wiring Status

Honest status as of 2026-07-30 (program audit): the
`arco_state_store_replay_*` metrics are reserved in
`crates/arco-catalog/src/metrics.rs` with no emitter, the promotion gate has
never run against real measurements, and the control store has no production
callers — replay cost is currently only observable in tests and benchmarks.
