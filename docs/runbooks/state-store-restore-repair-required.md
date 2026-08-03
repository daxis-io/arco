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
  `visible restore transaction checksum mismatch` or
  `visible restore manifest checksum mismatch`
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
   (`inspect_visible_restore`) hashes what is actually visible:
   - visible bytes match the plan: the participant's work is durably applied
     and repair can mark it complete;
   - visible bytes differ: the artifacts belong to some other lineage — the
     restore must re-render from its pinned base or be abandoned;
   - artifacts absent: the participant never became durable and can be
     re-applied from the plan.
4. `ControlMvpRestorePlan::validate` fails closed on any scope/checksum/shape
   inconsistency (`invalid Control MVP restore plan`) — a plan that no longer
   validates must not be re-applied.

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

## Current Wiring Status

Honest status as of 2026-07-30 (program audit): the restore machinery
(Phase 7C/7D) is implemented and heavily tested but hermetic — there is no
production restore command, operator surface, or scheduled recovery job, and
no restore metric or alert. This runbook describes code behavior exercised by
`crates/arco-catalog/tests/workspace_snapshot_restore.rs` and is the intended
procedure once the operator surface lands.
