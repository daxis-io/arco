# Step 3 terminal restore completion implementation plan

> Executor: use `executing-plans`, `test-driven-development`, and `verification-before-completion` task by task. Do not commit or publish without separate authorization.

**Goal:** Complete the frozen, private synthetic authority-8 durable bounded restore protocol through workspace visibility and local qualification.

**Architecture:** Keep Plan7's selected-unit and final-stream verifier as the only source of computation evidence. Carry its result as a nonserializable, invocation-bound value through the held workspace apply lock and retention epoch into a dedicated restore candidate. Persisted records support validation and read-only reconciliation; none can recreate permission to send HEAD.

**Tech stack:** Rust 1.88, `arco-catalog`, existing scoped storage, retention coordination, Plan7 codecs, and worktree-local Cargo target.

---

## Fixed inputs and working rules

- Initial source: verified `origin/main` `d162e59e8e181406b41d7f8517497d02df57508a`; retained worktree: `/Users/ethanurbanski/arco/.worktrees/step3-durable-restore-main-20260927`. After PR #444 advanced live `origin/main` to `17d3e1e5c15ccf6c6a590929d6711c111aeb2f0c`, the current integration worktree is `/Users/ethanurbanski/arco/.worktrees/step3-durable-restore-main-20260928`. Preserve both worktrees, the divergent root `main`, and all evidence.
- `Cargo.lock` SHA-256: `14c1c37ebe1c8bd24231070c483e522b18b2a0c017d65ea1ee006f86686d670d`. The three owning plans are `2026-09-13-state-store-durable-restore-contract.md`, `2026-09-19-state-store-restore-publication-intent.md`, and `2026-09-27-state-store-restore-format9-integration.md`; their hashes are in `/private/tmp/arco-step3-durable-restore-evidence-20260927/source-inputs.sha256`. These plans govern when this implementation plan is terse.
- Current red: `workspace_restore::plan7_tests::native_restore_driver_publishes_verified_terminal_candidate` in `crates/arco-catalog/src/workspace_restore.rs`. Its compiled failure and exact patch are retained in `/private/tmp/arco-step3-durable-restore-evidence-20260927/`. The 187 existing bounded-restore tests, focused expiry-hint rejection, format, and package Clippy passed before this plan; these are baseline component results only.
- Set `CARGO_TARGET_DIR=$PWD/target`, `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`, and `CARGO_PROFILE_TEST_DEBUG=0` in this worktree. Use `--locked`. Run one Cargo job at a time. For every behavior below: add a behavioral test, run it to an expected compiled failure, retain the raw red output and test diff, implement the smallest owning-layer change, rerun to green, then run nearby regressions. A compiler error is a diagnostic, not the red receipt.
- Preserve authority 7/restore-plan 6 wire bytes and behavior. Authority 8 stays explicitly synthetic and private. Reject all format-9 expiry hints before output; enabling format-9 restore requires a separate contract and qualification. Keep original request deadlines, source retention, writer/reclamation fences, and absent-target external witness.
- Preserve the frozen 24-hour execution, 8-day preparation retention, 7-day orphan eligibility, and 30-day protected source-token windows. No persisted intent, prepared descriptor, or seal may authorize another HEAD send after a possible send. No GC, export relocation, public restore toggle, provider, or production activation in this plan.

## 1. Carry same-invocation verification evidence

**Files:** `crates/arco-catalog/src/state_store/control_mvp/bounded/restore/units/driver.rs`, `units/coverage.rs`, `bounded/restore/certificate.rs`, `crates/arco-catalog/src/workspace_restore.rs`.

1. Add a focused red test around a valid terminal selected progress: corrupt a middle receipt or output after selection and assert the terminal path produces no candidate artifact, no armed intent, and no HEAD PUT. Use the existing `native_restore_driver_publishes_verified_terminal_candidate` as the positive red. Retain both failures.
2. Make `verify_terminal` retain the independently checked root/history assembly, exact selected terminal tuple, paired notice edits, and certificate in one private owned result. Its constructor runs only after `StandardStream::finish`, count/history checks, notice assembly, and a final workspace refence in the *same* call. The value must not implement serialization or have a constructor from persisted bytes.
3. Give that result a borrowed publication permit tied to `RestoreAdvanceContext`'s held apply lock, selected plan/owner generation, deadline, and active retention epoch. Refactor the bounded context's epoch loan only as needed to reach the existing `BoundedMutation::arm_restore_publication`; do not add a standalone store publication API. Verify cancellation at each await releases temporary owners without yielding a reusable permit.
4. Green the negative capability tests without sending HEAD yet. Keep a deliberate fail-closed terminal stop until tasks 2–4 complete. Run focused driver, final coverage, notice, deadline, selection, and expiry tests.

## 2. Define and validate the restore candidate wire boundary

**Files:** `crates/arco-catalog/src/state_store/control_mvp/bounded.rs`, `bounded/restore/certificate.rs`, `crates/arco-catalog/src/state_store/control_mvp/retained.rs`; add one private restore publication module under `bounded/restore/` only if this keeps the reader and writer smaller.

1. Red tests: a restore-kind manifest with a missing/forged certificate, wrong source or target scope/cut, wrong root/history/sequence, altered paired notice proof, or changed owner/plan must fail in current, retained-token, and candidate read paths. Existing ordinary manifest and prepared-candidate byte vectors must remain exact.
2. Add a dedicated restore manifest kind and optional restore-only artifact reference(s), omitted from ordinary serialization. Do not encode restore as a bounded transaction or as `SyntheticGenesis`. Present mode binds the original target parent; absent mode inherits the authenticated source lineage while keeping the original HEAD precondition `DoesNotExist`. Validate the source/parent distinction explicitly; the ordinary prepared-candidate validator's absent-parent rule does not apply to restore.
3. Bind the manifest's KV root, inherited/edited outbox roots, logical result sequence/history, projection source, certificate, and exact Plan7 identity. Reuse `RestoreCertificateV1::read_bounded`, directory proof verification, bounded retained reads, and existing root decoders. Reject noncanonical/missing/oversized bytes and unattached authority references. Ensure `read_bounded_token_from_manifest`, current-pointer reads, and bounded recovery all dispatch through the same restore validation, without loosening ordinary candidate validation.
4. Green the reader tests, then rerun ordinary authority-8 transaction/genesis and V7 token/restore-plan 6 regression tests. A readable private candidate still confers no publication or workspace visibility.

## 3. Prepare immutable artifacts and the final seal

**Files:** `bounded/restore/units/driver.rs`, `bounded/restore/certificate.rs`, `bounded.rs`, `physical/restore_io.rs`, and the private publication module if created.

1. Red tests: verified present and absent terminal results produce exactly the expected immutable candidate, certificate, and bounded seal; no artifact is published from a forged/stale verifier result. A valid-looking persisted seal alone never causes a HEAD send, including after restart.
2. From the task-1 in-memory result, write immutable certificate/provenance, restore manifest, and bounded final-seal summary; construct and validate the exact prepared restore descriptor bytes for `restore/v7/{candidate_id}/prepared.json` without writing them yet. Use existing immutable exact-byte replay and charged I/O patterns. Seal the original HEAD bytes/version or absence, candidate HEAD bytes/manifest digest, selected plan/owner/terminal receipt chain, result roots/history/notice, and certificate digest. Apply the 4 MiB seal and 1 MiB prepared caps before allocations/reads; keep existing 32-manifest/64 MiB recovery limits.
3. Re-fence target HEAD, writer/reclamation floors, source pin, request deadline, journal selection, and epoch before arming intent in task 4. Once that task may have written the prepared descriptor, restart is reconciliation-only even if later reads fail. Do not replay final computation from a seal.
4. Green immutable artifact/seal and prepared-byte construction tests for both target modes. Verify no prepared descriptor or candidate HEAD PUT in this task.

## 4. Arm intent and send HEAD once

**Files:** `crates/arco-catalog/src/workspace_restore.rs`, `crates/arco-catalog/src/retention_coordination/bounded.rs`, `bounded/restore/units/driver.rs`, `bounded.rs`.

1. Red tests with a counting storage backend: exact successful publication sends one HEAD PUT with the plan's *original* `MatchesVersion` or `DoesNotExist`; changed HEAD/fences yield a terminal conflict without rebasing; cancellation before/after each boundary never causes a second PUT. Retain distinct direct `PreconditionFailed`, lost-response, and transport-error receipts.
2. Under the still-held apply lock and verified permit, arm the existing sticky `ArmedRestorePublicationIntent` after artifacts/seal and before prepared publication. Persist the exact prepared bytes with immutable replay, then refence. Follow frozen ordering: artifacts → seal → refence and arm → prepared descriptor → refence → one original-condition HEAD PUT. No retry loop and no new condition based on a later HEAD.
3. Direct `PreconditionFailed` is definitive supersession. A successful response proceeds to authenticated visibility checks. Any uncertain send, cancellation, or unchargeable response leaves the epoch in flight and enters read-only candidate reconciliation. Keep intent V2 only while armed and original V1 bytes when absent.
4. Green count/order/fault tests. Re-run retention epoch tests, ordinary bounded commit/recovery tests, and the inherited request-deadline/fence tests.

## 5. Reconcile exactly, then expose workspace visibility

**Files:** `bounded/restore.rs`, `bounded.rs`, `crates/arco-catalog/src/workspace_restore.rs`, `crates/arco-catalog/src/retention_coordination/bounded.rs`.

1. Red tests for restart with exact candidate HEAD, authenticated descendant, conflicting child, unchanged original HEAD, missing/corrupt seal, oversized/corrupt prepared bytes, and absent-HEAD competing initial ancestry. Assert zero HEAD PUTs on *every* recovery path.
2. Read-only reconciliation returns `Committed`, `Superseded`, or `Unresolved` using bounded authenticated HEAD → manifest → seal/plan/certificate evidence. Exact candidate or authenticated descendant proves committed. Conflicting successor from the pinned original proves superseded. Absent-target supersession needs complete authenticated initial ancestry; missing or exhausted proof is unresolved. Do not infer from missing HEAD, a matching notice, or persisted intent alone.
3. Map a proven committed candidate to `RestoreParticipantInspection::Visible`; map a proven conflict to `Superseded`; keep uncertainty as an error/`RepairRequired`, never `Ready` or new `InProgress` after prepared publication. Clear an armed epoch only after proof-based terminal reconciliation and durable journal receipt. Do not change the existing global settlement rule for other intents.
4. Let `WorkspaceRestoreService` finish its existing participant journal and publish a read manifest only after *all* required participants are visible. The bounded participant remains private; mixed V6/Plan7 attempts must preserve one-unit-per-call behavior, original deadlines, and no premature read manifest. Green the existing terminal red test and add exact restart/idempotence assertions.

## 6. Independent correctness and failure qualification

**Files:** tests beside the owning code; an independent oracle script/fixture under `crates/arco-catalog/tests/` or `tools/` only where it can inspect published bytes without calling the implementation's merge/history helpers.

1. Compile red then green end-to-end present and absent tests with an independent map/byte oracle. Compare exact visible values, generations, tombstones, result sequence/history/commit ID, inherited outbox and incarnation order, single notice and paired indexes, certificate provenance, journal receipt, and read manifest. Include equal-value/different-generation, delete/recreate, source-only tombstones, binary keys/values, empty/gap/misaligned/maximum keys, and equivalent logical states on different physical layouts.
2. Fault matrix: receipt gaps/overlaps/duplicates/omitted sibling, forged root/descriptor/index/receipt, changed untouched provenance, altered generation/tombstone, stale writer/reclamation/source cut, competing worker and target writer, expired original deadline, cancellation at every receipt/progress/final/intent/prepared/HEAD boundary, lost response, and restart. Verify partial jobs never read as authority and persisted seal/intent never resends CAS.
3. Run the actual resumable workflow on the frozen large fixture axes: 2,419,200 retained receipt/audit rows, independently large active outbox, disjoint source/target difference above one 64 MiB segment and one million rows, and maximum source/current singleton pair. Keep target HEAD/fences quiet for the positive run. Record fixture-construction cost separately from executed restore work; report per-unit/final selected I/O, peak *live owned* memory, backend shared span, bytes, retries, failures, and artifact classes. Treat backpressure or an incomplete run as failed, not as a passing capacity claim.

## 7. Full local gates and evidence handoff

1. Freeze final source diff, `Cargo.lock`, toolchain, oracle source, fixtures, and raw result hashes. Retain prior red failures. Check source/binary/fixture correspondence and available physical disk before costly runs; no shared Cargo target or cleanup of historical evidence.
2. Run focused restore/workspace/retention/format-9 tests, `cargo fmt --all --check`, then `cargo clippy --workspace --all-targets --all-features -- -D warnings`. Run the repository's required local CI commands from `.github/workflows/ci.yml`: xtask doctor/ADR/integrity/boundary/parity/hygiene gates; workspace and API feature checks; core, API, flow, parity, doc, Python, protocol, and relevant cost suites. Use the workflow's exact flags and record any environment-only gate as blocked with reason. Run `git diff --check` and a fresh independent diff audit of authority, recovery, and tests.
3. Report **passed / failed / blocked / untested** per gate. A passing component, local workflow, or candidate read is not public restore, remote CI, provider qualification, or production readiness. A future push/PR/merge and remote required checks need their own authorization and source identity; provider work and production activation remain separate.

## Completion criterion

The positive red test and both independent end-to-end oracles pass; all fault and large streaming gates pass with retained receipts; format, strict workspace Clippy, and required full local CI pass; an independent audit finds no unvalidated restore read path or possible CAS resend. Only then is the private local Step 3 contract complete. Keep commit, push, PR mutation, merge, cleanup, provider, and production gates closed until separately authorized.
