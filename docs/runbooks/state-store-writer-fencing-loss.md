# Runbook: Control-Store Writer Lease Loss / Writer-Epoch Fencing Loss

Failure states (Tier-1 control-store strategy, 2026-06-25, Failure States
table): "writer loses lease before CAS — no acknowledgement; new writer fences
old epoch and recovers" and "writer acknowledges then crashes — committed token
is visible; new writer reloads manifest and resumes."

## Symptoms

- A writer that previously published successfully starts losing every pointer
  CAS (`control MVP pointer CAS lost to a newer manifest`).
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
- There is no lease-expiry alert because the control-store MVP has no lease
  (see wiring status below).

## Diagnosis

Two different fencing regimes exist today:

1. **Current Tier-1 path (implemented):** writers hold a distributed lock and
   carry its fencing token as a writer epoch.
   `crates/arco-catalog/src/tier1_writer.rs` compares the writer epoch against
   the pointer epoch and refuses to publish when
   `writer_epoch < pointer.epoch`. A rejected writer has lost the lease: some
   newer writer already advanced the epoch.
2. **Object-store control store (MVP, not yet fenced):** the publish protocol
   in `crates/arco-catalog/src/state_store/control_mvp.rs` relies on two
   mechanisms only — the IAM sole-writer binding
   (`infra/terraform/iam_conditions.tf`, `api_write_state_store`: only the API
   service account can write `state-store/`) and the pointer CAS generation
   precondition. There is no `writer_epoch` field in the pointer or manifest
   and no lease object. The strategy plan's two-condition publish rule
   (`current_manifest.writer_epoch <= writer_epoch`) is a committed design, not
   shipped code.

Steps:

1. Identify every writer candidate for the scope: Cloud Run revisions of the
   API service currently serving traffic (`gcloud run revisions list`), plus
   any job or operator session using API credentials.
2. For the legacy path, read the pointer epoch and the lock:
   the lock objects live under the workspace `locks/` prefix; the fencing
   token sequence is the epoch. Compare against the rejected writer's log line.
3. For the control store, inspect `current.pointer.json` version/generation
   history (object generation metadata) to see the interleaving of publishes.
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
- Never delete or rewind `current.pointer.json` to un-fence an old writer:
  that discards committed state. Recovery is always roll-forward from the
  current pointer.
- If a stale writer must be blocked immediately and cannot be drained, remove
  its service account binding (break-glass IAM change) rather than mutating
  control-store objects.

## Current Wiring Status

Honest status as of 2026-07-30 (program audit): the control-store MVP has no
writer lease, no writer epoch, and no epoch-loss failure-state test; adding
writer-epoch fencing to the MVP publish protocol is an open gate-closure item
(audit section 9.2 item 5). Until it lands, sole-writer protection for
`state-store/` rests entirely on the IAM prefix binding and CAS generations.
The control store also has no production callers yet, so this runbook's
control-store sections describe behavior exercised only by tests.
