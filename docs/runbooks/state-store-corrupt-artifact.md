# Runbook: Corrupt Control-Store Artifact (Checksum-Chain Failure)

Failure state (Tier-1 control-store strategy, 2026-06-25, Failure States
table): "segment corruption detected — fail closed for control reads; repair
from txlog/checkpoint/archive."

## Symptoms

Control-store reads fail closed with one of the checksum-chain errors from
`crates/arco-catalog/src/state_store/control_mvp.rs`:

- `control MVP manifest reference checksum` mismatch — the manifest bytes do
  not hash to the checksum recorded in the pointer (or checkpoint) that named
  them;
- `control MVP transaction reference checksum` mismatch — a transaction
  object's bytes do not hash to the checksum in the manifest's `tx_refs` entry;
- `control MVP manifest state checksum does not match replay` — replaying the
  full transaction chain produced a state whose checksum disagrees with
  `manifest.state_checksum_sha256`;
- envelope/JSON decode failures for pointer, manifest, transaction, or
  checkpoint objects.

Reads for the affected scope return errors; no stale or partial state is ever
served. Writes also fail, because `begin_control_txn` loads and replays the
current base first.

## Detection

- Alert: `ArcoControlStoreReadIntegrityFailures`
  (`infra/monitoring/alerts.yaml`, group `arco.state_store`; metric reserved,
  no emitter yet).
- Client-visible fallout rolls up into `ArcoApiErrorRateHigh` once the control
  store has API callers.

## Diagnosis

The integrity chain, validated on every read
(`load_pointer` -> `load_manifest_for_pointer` -> `replay_manifest` ->
`load_tx`):

```
current.pointer.json
  └─ manifest_id + manifest_checksum_sha256
       └─ manifests/{manifest_id}.json
            ├─ tx_refs[]: tx_id + checksum_sha256   (each txlog object)
            └─ state_checksum_sha256                (post-replay state)
```

Steps:

1. Fetch the pointer and verify the manifest hash yourself:

   ```bash
   BASE="gs://${BUCKET}/tenant=${TENANT}/workspace=${WORKSPACE}/state-store/control-mvp/${DOMAIN}"
   gcloud storage cat "${BASE}/current.pointer.json" | jq
   gcloud storage cat "${BASE}/manifests/${MANIFEST_ID}.json" | shasum -a 256
   ```

2. Walk `tx_refs` and hash each `txlog/{tx_id}.json`, comparing against the
   recorded `checksum_sha256`, to locate the first broken link.
3. Classify the break:
   - one transaction object corrupt: manifest and pointer are fine; state is
     unreconstructable only from that manifest chain;
   - manifest corrupt but pointer checksum matches nothing: pointer references
     a bad manifest;
   - pointer object itself corrupt/undecodable: no state is selectable at all;
   - a *checkpoint* object corrupt: only `read_checkpoint` consumers are
     affected; current reads keep working.
4. Check object generation history and audit logs for the corrupt object to
   find what wrote it. Given immutable-create preconditions
   (`DoesNotExist`) on txlog/manifest/checkpoint objects, corruption implies
   out-of-band mutation, storage-layer fault, or a torn client — all
   reportable incidents.
5. For the general storage-side procedure (listing, hashing, quarantine), the
   sibling `docs/runbooks/storage-integrity-verification.md` applies.

## Remediation

Recovery is roll-forward from verified artifacts; never patch bytes in place.

- Corrupt checkpoint only: delete nothing; issue a fresh checkpoint from the
  healthy current manifest and re-anchor long readers on it.
- Corrupt manifest or transaction in the *current* chain: identify the newest
  fully verifiable manifest (walk pointer history / checkpoints whose chains
  hash clean end-to-end) and restore forward from it through the workspace
  restore path (`docs/runbooks/state-store-restore-repair-required.md`
  describes the journaled flow). The restore participant re-renders candidate
  transaction/manifest/pointer bytes and re-validates every hash before any
  publish (`ControlMvpRestoreParticipant`).
- Orphan candidates that fail validation must be treated as physical artifact
  gaps: do not project or revalidate them into state (strategy rule for orphan
  transactions).
- After recovery, re-run the verification walk (step 1-2) against the new
  head before declaring the incident closed.

## Current Wiring Status

Honest status as of 2026-07-30 (program audit): the fail-closed validation
described here is implemented and adversarially tested (field-by-field JSON
corruption rejection in `crates/arco-catalog/tests/state_store_control_mvp.rs`),
but the store has no production callers, the integrity-failure metric has no
emitter, and there is no automated repair — the restore path exists in code
(Phase 7) yet is likewise hermetic.
