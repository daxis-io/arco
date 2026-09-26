# Runbook: StateToken / CheckpointToken Expiry

Failure states (Tier-1 control-store strategy, 2026-06-25, Failure States
table): "state token expires — no read-after-write retention guarantee; use
later covering manifest or return TokenExpired" and "checkpoint expires — no
long-read guarantee; reader renews or fails with CheckpointExpired."

## Symptoms

- `read_at(StateToken)` fails with `NotFound` for the token's manifest, or with
  `StateToken logical sequence does not match manifest`
  (`invariant_violation`).
- `read_checkpoint(CheckpointToken)` fails with `NotFound` for the checkpoint
  object or its referenced manifest.
- Projection consumers see
  `ProjectionOutboxAckReadStatus::TokenUnavailable { manifest_id, logical_sequence }`
  (`crates/arco-catalog/src/state_store/projection_outbox_acks.rs`), meaning a
  retained read below the current head is no longer serviceable.
- Long-running exports or comparisons pinned to an old token start failing
  while current-head reads stay healthy.

## Detection

- No dedicated alert exists (see wiring status). Client-visible failures roll
  up into `ArcoApiErrorRateHigh` once the surfaces have API callers.
- `TokenUnavailable` statuses in projection-ack consumers are the precise
  programmatic signal.

## Diagnosis

Grounding: `crates/arco-catalog/src/state_store/control_mvp.rs`.

- A `StateToken` is `{ scope, logical_sequence, authority_manifest_id }`,
  issued by `current_state_token()` and returned from every commit as
  `CommitOutcome::state_token`. `ArcoStateReader::read_at` resolves it by
  loading the named manifest (internally `load_state_at_token`), verifying
  the sequence matches, then replaying the manifest's L1 anchors and
  transaction suffix with full checksum validation.
- A `CheckpointToken` is `{ scope, checkpoint_id }`. `read_checkpoint` loads
  the immutable checkpoint object, then the manifest it names, verified
  against `checkpoint.manifest_checksum_sha256`.
- Neither token carries a TTL today. "Expiry" therefore materializes as the
  referenced artifacts becoming unreadable (deleted, retention-cleaned, or
  never durable), surfacing as `NotFound` — not as a typed `TokenExpired` /
  `CheckpointExpired` error. The typed errors and the retention budget
  (StateToken read-after-write retention >= 1 hour for the prototype) are
  strategy-plan commitments, not shipped code.

Steps:

1. Confirm whether the token's artifact is actually gone:

   ```bash
   gcloud storage ls \
     "gs://${BUCKET}/tenant=${TENANT}/workspace=${WORKSPACE}/control/v1/domains/${DOMAIN}/manifests/${MANIFEST_ID}.json"
   ```

2. If the artifact exists but the read fails on a checksum or sequence
   mismatch, this is not expiry — switch to
   `docs/runbooks/state-store-corrupt-artifact.md`.
3. If the artifact is gone, determine what removed it. Control-store GC
   (`ControlMvpMaintenanceWorker::collect_gc_page_at`, run by the scheduled
   `arco-control-store-worker` job as of 2026-09-23) retains token and
   checkpoint pins for 30 days and deletes unreachable candidates only after
   seven days; the generic catalog GC never deletes under `control/v1/`.
   Removal of a younger artifact therefore implies manual cleanup or bucket
   lifecycle rules — audit both, plus the worker's GC summary logs.

## Remediation

- Reader-side: re-anchor on the current head — call `current_state_token()`
  (which follows `head/current.json`) and re-run the read; the strategy
  contract is "use a later covering manifest."
- Long reads: take a fresh checkpoint (`ArcoStateAdmin::checkpoint` writes an
  immutable checkpoint object naming the current manifest) and resume from it;
  renewal is the contract for long-read consumers.
- Do not attempt to reconstruct a deleted manifest in place; committed history
  is roll-forward only.
- If bucket lifecycle rules deleted retained artifacts, fix the lifecycle
  policy before resuming: retention for `control/v1/` artifacts must outlive
  the 30-day token/checkpoint pin window.

## Current Wiring Status

Status as of 2026-09-23: tokens are issued and redeemed on the route-wired
control authority (one exact root behind `ARCO_CATALOG_CONTROL_V1_*`, legacy
for every other root, not provider-qualified, not authoritative on any
deployed root); the winning `StateToken` stays internal to the API, and
control list cursors carry it only inside authenticated encryption.
`TokenExpired`/`CheckpointExpired` typed errors and TTLs remain unimplemented;
retention is enforced by the conservative GC pin window (30 days) instead, and
there is no expiry alert or metric. The `ProjectionOutboxAckReadStatus`
surface has tests but no production consumer.
