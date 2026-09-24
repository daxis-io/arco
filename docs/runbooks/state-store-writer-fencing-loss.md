# Runbook: Control-Store Writer Lease Loss / Writer-Epoch Fencing Loss

Failure states (Tier-1 control-store strategy, 2026-06-25, Failure States
table): "writer loses lease before CAS — no acknowledgement; new writer fences
old epoch and recovers" and "writer acknowledges then crashes — committed token
is visible; new writer reloads manifest and resumes."

## Symptoms

- A writer that previously published successfully starts losing every head
  CAS (`control MVP pointer CAS lost to a newer manifest`).
- Control store: `begin_control_txn` or publish fails with
  `CatalogError::StaleWriterEpoch` ("control MVP writer epoch N is superseded
  by published epoch M") after another writer claimed authority.
- On the current (legacy) Tier-1 path: publish attempts rejected with
  `stale epoch: writer epoch N is behind pointer epoch M`
  (`crates/arco-catalog/src/tier1_writer.rs`).
- Two service revisions both believe they are the active writer for a scope
  (deploy overlap, stuck rollout, split-brain after a lock expiry).

## Detection

- Control store: `ArcoControlStoreCasPublishFailureRateHigh`
  (`infra/monitoring/alerts.yaml`, group `arco.state_store`) — sustained CAS
  loss is the observable signature of a fenced/competing writer.
- Orchestration ledger path: `ArcoFlowStaleFenceRejects`
  (`arco_flow_orch_compactor_stale_fencing_rejects_total`) fires on stale
  fencing-token rejects.
- There is no lease-expiry alert: the control store fences by writer epoch,
  not by lease (see wiring status below).

## Diagnosis

Two different fencing regimes exist today:

1. **Current Tier-1 path (implemented):** writers hold a distributed lock and
   carry its fencing token as a writer epoch.
   `crates/arco-catalog/src/tier1_writer.rs` compares the writer epoch against
   the pointer epoch and refuses to publish when
   `writer_epoch < pointer.epoch`. A rejected writer has lost the lease: some
   newer writer already advanced the epoch.
2. **Object-store control store (`control/v1`, epoch-fenced):** the publish
   protocol in `crates/arco-catalog/src/state_store/control_mvp.rs` relies on
   three mechanisms — the IAM sole-writer binding
   (`infra/terraform/iam_conditions.tf`, `api_write_state_store`: only the API
   service account can write `control/`), the exact-version CAS precondition
   on `head/current.json`, and the `writer_epoch` carried in the head and manifest.
   Publication requires the held epoch to equal the published one
   (`validate_publication_epoch`): a lower epoch fails closed with
   `CatalogError::StaleWriterEpoch`, and a higher, never-claimed epoch fails
   with `PreconditionFailed`. Only
   `ControlMvpStateStore::claim_writer_authority` advances the epoch, through
   the same head CAS; as of 2026-09-23 writer-authority claims are fenced by
   claim id, so an ambiguous claim adopts only its exact claimed bytes.
   Cooperative store-maintenance writers adopt the published epoch instead of
   fencing: the projection ack writer through `at_current_writer_epoch` (or
   an explicit `with_writer_epoch`), and the durable maintenance worker by
   copying the published epoch into each candidate head and refusing to
   publish if the epoch moved. There is no lease object.

Steps:

1. Identify every writer candidate for the scope: Cloud Run revisions of the
   API service currently serving traffic (`gcloud run revisions list`), plus
   any job or operator session using API credentials.
2. For the legacy path, read the pointer epoch and the lock:
   the lock objects live under the workspace `locks/` prefix; the fencing
   token sequence is the epoch. Compare against the rejected writer's log line.
3. For the control store, inspect `head/current.json` version/generation
   history (object generation metadata) and its `writer_epoch` to see the
   interleaving of publishes and epoch claims.
4. Distinguish benign from dangerous:
   - benign: a superseded writer that keeps losing CAS and only retries — no
     corruption is possible, old writers cannot overwrite committed state;
   - dangerous: two writers alternating successful publishes (both fresh) —
     this violates the single-writer operating assumption and inflates
     contention, though every individual commit remains CAS-serialized.

## Remediation

- Converge to one writer: complete or roll back the overlapping deployment;
  ensure exactly one API deployment serves the tenant/workspace/domain scope.
- Superseded writer (legacy path): let it terminate; the new writer re-reads
  the pointer and resumes from the committed head — "writer acknowledges then
  crashes" needs no repair because the committed token stays visible.
- Never delete or rewind `head/current.json` to un-fence an old writer:
  that discards committed state. Recovery is always roll-forward from the
  current head. A head that already publishes the terminal epoch `u64::MAX`
  is refused in place rather than repaired; recover by restoring retained
  authority into a fresh domain scope.
- If a stale writer must be blocked immediately and cannot be drained, remove
  its service account binding (break-glass IAM change) rather than mutating
  control-store objects.

## Current Wiring Status

Status as of 2026-09-23: the control store fences by `writer_epoch` (claimed
through head CAS, published-epoch equality on every commit, claim-id fencing
of writer-authority claims) but still has no lease, lease TTL, or
lease-expiry alert. Sole-writer protection for `control/` rests on the IAM
prefix binding (`api_write_state_store`, conditioned on `control/` as of
2026-09-23), head CAS, and epoch fencing. The scheduled
`arco-control-store-worker` job runs under the same API service account and
publishes only as a cooperative writer. The control authority is route-wired
for one exact root behind `ARCO_CATALOG_CONTROL_V1_*`, legacy by default, not
provider-qualified, and not authoritative on any deployed root, so the
control-store sections above are exercised by tests and any bound root only.
