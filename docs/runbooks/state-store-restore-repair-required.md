# Runbook: Restore / Transaction REPAIR_REQUIRED

A durable workspace restore has entered the `REPAIR_REQUIRED` lifecycle state:
at least one participant needs deterministic repair before the restore can
proceed to `FINALIZING` / `VISIBLE`.

## Symptoms

- The restore journal records `"status": "REPAIR_REQUIRED"`
  (`WorkspaceRestoreStatus::RepairRequired`,
  `crates/arco-catalog/src/workspace_restore.rs`; statuses serialize
  SCREAMING_SNAKE_CASE).
- Restore commands report a failure category of `CAS_LOST`,
  `PARTICIPANT_FAILED`, or `STORAGE_UNCERTAIN` (`RestoreFailureCategory`).
- A control-store participant reports plan/visible mismatches such as
  `visible restore manifest checksum mismatch`,
  `visible restore candidate pointer digest mismatch`, or
  `visible restore transaction does not match planned restore metadata`
  (`ControlMvpRestoreParticipant` in
  `crates/arco-catalog/src/state_store/control_mvp.rs`).

## Detection

- No alert covers restore state today; `REPAIR_REQUIRED` is discovered from
  restore command output or by inspecting the durable restore journal in the
  workspace prefix. If the restore was driven through an API surface, failures
  roll up into `ArcoApiErrorRateHigh`.

## Diagnosis

Restore lifecycle (`WorkspaceRestoreStatus`):
`PREPARED -> APPLYING -> (REPAIR_REQUIRED) -> FINALIZING -> VISIBLE`.

The restore is journaled and roll-forward: every participant carries a plan
with pinned checksums, and recovery decides deterministically whether the
visible bytes match the plan.

1. Read the restore journal for the workspace and note the failing
   participant(s), attempt numbers, and failure category.
2. Interpret the failure category:
   - `CAS_LOST`: a publish inside the restore lost a CAS race — some other
     writer advanced the target; the journal intentionally parks in
     `REPAIR_REQUIRED` instead of guessing;
   - `PARTICIPANT_FAILED`: a participant returned a hard error while applying
     its plan;
   - `STORAGE_UNCERTAIN`: a write ended in an unknown state (timeout/5xx after
     send) — the artifacts may or may not be durable.
3. For a control-store participant, compare plan vs visible artifacts. The
   plan pins `transaction_sha256`, `candidate_manifest_sha256`, and
   `candidate_pointer_sha256` (`ControlMvpRestorePlan`); inspection
   (internal `inspect_visible_restore`) hashes what is actually visible:
   - visible bytes match the plan: the participant's work is durably applied
     and repair can mark it complete;
   - visible bytes differ: the artifacts belong to some other lineage — the
     restore must re-render from its pinned base or be abandoned;
   - artifacts absent: the participant never became durable and can be
     re-applied from the plan.
4. The restore participant validates every plan it loads (restore-plan
   version 8 with its pinned `observed_writer_epoch`, checkpoint interval,
   `committed_at_ms` stamp, `restore_key_policy_sha256`, scope, checksums,
   and shape) and fails closed with `invalid Control MVP restore plan`
   before touching storage — a plan that no
   longer validates must not be re-applied. Restore-plan versions 1 through 7
   are supersession-only: a version-8 plan may supersede them, but they are
   never re-applied (version 6 is the last shape written on authority format
   7 and cannot reproduce format-9 candidate bytes; version 7 predates
   restore key policies and binds none). A version-7 plan inspects
   `Superseded` even when its restore already landed. A version-8 plan
   rendered under a different restore key policy than the participant's
   inspects `Superseded`, so recovery replans it; it is not a checksum
   mismatch. A plan whose restore already landed inspects `Visible`
   whatever the participant's policy. A plan whose authority
   reference does not have the canonical `control/v1` manifest and checkpoint
   path shape returns `UnsupportedAuthorityFormat` with hard-cut recovery
   direction; separately, the head and manifest validators fail closed on any
   `format_version` other than the current on-disk authority format 9, and
   retained-token reads accept formats 9 and 8 only (8 being the synthetic
   authority-8 bounded roots that exist under test-utils). There is no
   conversion from format 7: a format-7 root is unreadable and is re-seeded.

## Remediation

- Re-run the restore recovery helper for the workspace. Recovery is designed
  to be re-entrant: it re-reads the journal, re-inspects each participant
  against its pinned checksums, applies only what is provably missing, and
  either advances the journal past `REPAIR_REQUIRED` or parks again with the
  same evidence.
- `CAS_LOST`: confirm no concurrent restore or writer is still active for the
  scope (see `docs/runbooks/state-store-writer-fencing-loss.md`), then re-run
  recovery so it rebases/parks deterministically.
- `STORAGE_UNCERTAIN`: re-run recovery — inspection resolves the uncertainty
  by hashing what is visible; never assume the write failed.
- Never edit the journal or participant artifacts by hand, and never delete
  candidate artifacts while a journal references them: the journal is the
  authority for restore state, and repair decisions are deterministic replays
  of it.
- If repeated recovery keeps parking in `REPAIR_REQUIRED` with checksum
  mismatches, treat it as the corrupt-artifact case
  (`docs/runbooks/state-store-corrupt-artifact.md`) and escalate: the visible
  lineage disagrees with every pinned plan.

## Catalog restore effects and the restore L0 limit

A catalog restore built with `catalog_restore_participant` carries the
restore key policy `catalog_restore_key_policy()`, which excludes
idempotency receipts (key tag 3). It changes more than the restored keys:

- Receipts are never restored. Every receipt live before the restore is
  deleted at the restore sequence, whether or not the restore source holds
  it. A keyed request replayed after the restore is not answered with its
  original response: it re-executes, as after its receipt expired. The
  outcome depends on the command; for example a keyed `create_catalog` of a
  catalog the restore brought back fails with a name conflict.
- Key tag 4 (residual audit rows written before retention step 3) follows
  the plain restore rules.
- The catalog projection publishes the restored state and one restore audit
  row on its next drain (`docs/runbooks/control-store-worker.md`).

Every restore write, including the receipt deletes, goes into one L0
segment together with the restore notice. A restore that does not fit fails
`Validation` when its participant plan is rendered:

```text
Control MVP restore <restore_id> attempt <attempt> of domain <domain> does not fit one L0 segment (puts: <n>, deletes: <n>, excluded-key deletes: <n>, plus one restore notice): <segment limit detail>
```

The error surfaces from planning, or from replanning a superseded
participant during recovery. The participant has written nothing and HEAD
is unchanged. The binding limit is the segment's 512 KiB index, reached at
roughly 390k live receipts. Live receipts are the last ~25 hours of catalog
mutations (keyed and unkeyed) plus any the retention horizon has not purged
yet: about 180k at the pilot rate. When `excluded-key deletes` dominates the counts, confirm that the
control-store worker's retention horizon is publishing
(`kind="retention_horizon"`, `outcome="published"`) and retry after expired
receipts are purged. A restore cannot be split across L0 segments.

Receipts no longer count against the restore source scan's budget
(1,000,000 rows, 64 MiB decoded). Before retention step 4 they did: about
119k receipts filled the 64 MiB, so a restore at the pilot rate failed at
planning.

The deletes are tombstones at the restore sequence. They stay in the
replayed state until the retention horizon passes that sequence, at least
30 days (`docs/runbooks/state-store-replay-budget.md`).

The bounded authority-8 restore cannot honour a key policy: planning and
advance refuse a participant whose policy excludes anything with
`UnsupportedOperation` (`bounded authority-8 restore cannot honour a restore
key policy`) before any read or write.

## Current Wiring Status

Status as of 2026-09-23: the restore machinery (Phase 7C/7D) is implemented
and heavily tested but still has no production restore command, operator
route, scheduled recovery job, restore metric, or alert; the scheduled
`arco-control-store-worker` job runs projection drain, layout maintenance,
and GC only, never restore. The control store it restores into is
route-wired for one exact root behind `ARCO_CATALOG_CONTROL_V1_*`, legacy by
default, not provider-qualified, and not authoritative on any deployed root.
This runbook describes code behavior exercised by
`crates/arco-catalog/tests/workspace_snapshot_restore.rs` and is the intended
procedure once the operator surface lands.

As of retention step 4 (2026-10-01) the worker's drain materializes catalog
restore notices, but no production composition registers restore
participants, with or without the catalog key policy. The S3 qualification
binary restores the catalog through `catalog_restore_participant`.
