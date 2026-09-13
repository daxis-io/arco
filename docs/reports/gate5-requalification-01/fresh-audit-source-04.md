# Fresh independent Gate 5 source audit

Reviewer: fresh-context read-only `gate5_final_audit`. Base: `745ed92e25ff7b4ec85aa0be57b02450642fda59`. Final reviewed source manifest: `2a3c4e9c32ab02f616810c0bb07d448ac1d34dccd45570a37be601f6fd9d537e`.

The reviewer assessed the complete source diff, untracked support/tests, frozen and remediation contracts, source manifest, checkpoint and evidence. The manifest covers 683 files; that count is not a claim that 683 files changed. This report preserves the review verdict, findings and required follow-up. Source review is complete; qualification evidence and the final archive are still pending.

## Verdict

**CONDITIONAL PASS for source `2a3c4e9...`; HOLD Gate 5 qualification/closeout until the exact-source post-audit lanes, regenerated reports, evidence index, and resealed reconstruction archive are terminal and independently verified.** One demonstrated P1 was found in the initially audited `bc4b27...` source. The current three-file repair resolves it without narrowing the supported public `*_at` clock contract. No open P0/P1/P2 source defect remains in `2a3c4e9...`.

## Completion

The four remediation areas are implemented: activation recovery/lifetime admission, disjoint cost phases, sequential generic-GC maintenance-root expansion without aggregate closures, and test/benchmark-only whole-job fixture orchestration. Authority/segment/directory/continuation versions and rewrite-equivalence v2 with v1 readability remain unchanged.

The earlier 54 mandatory green lanes/72 total runs, 86 scaling cases, 15 resume schedules, three lifecycle schedules, 32x128 model, 85 fault schedules, and pinned cargo-deny evidence belong exactly to `bc4b27...`. They are evidence for unchanged code, not exact-source proof for `2a3c4e9...`. The post-audit queue is still running.

Reviewer precision correction: `effective-clock-green-01` ran on behavior-identical pre-lint source `f5a208...`. The final `2a3c4e9...` differs only by `Option::or` to `or_else` at retention_coordination.rs:287 and has exact-source `clock-lint-02` green. Do not label that focused green itself exact-source; `catalog-03` is the planned exact-source behavioral rerun.

## Correctness: resolved P1

**Mixed wall/caller clock made a valid interrupted activation unrecoverable.**

- Before repair, retention_coordination.rs:103-120 created `started_at` from wall time, while `maintenance_root_submitted_before` required `created_at <= started_at < expires_at`. Public `prepare_at/start_at` legitimately use a supplied clock.
- The preventing test at state_store_reclamation_schedules.rs:1897 prepares at logical wall+48h, starts at that supplied creation time, pauses after the durable epoch PUT and before pin publication, cancels the invocation, expires the lease, then recovers at logical creation+25h. Under `bc4b27...`, the epoch had wall `started_at < created_at`; recovery failed and left the global retention epoch in flight. `effective-clock-red-01` exits 101 on that actual recovery path.
- The exact job could not repair its previously submitted root after expiry, and the surviving global retention epoch could block unrelated retention mutations until operator recovery.
- The repair at maintenance.rs:705 requires supplied `now >= created_at` and expires conservatively on `max(caller, wall)`. Activation authenticates prior submission and passes `(caller_now, expires_at)` only for a new claim. Retention coordination records `max(caller_now, claim wall)` for a fresh maintenance claim, checks the deadline immediately before PUT, and preserves the original timestamp for an exact in-flight replay. Prior-submission proof retains both interval bounds.
- The regression now passes. It also rejects a supplied time before creation, proves the submission interval, preserves epoch time, descriptor bytes, pin bytes and the eight-day deadline across repeated repair, and rejects new resume/advance after expiry.

No other demonstrated reachable correctness risk remains from the reviewed paths.

## Design

Prepare remains read-only. The workflow persists addressable immutable descriptor/pages, claims retention coordination before root publication, authenticates the entire plan again after waiting for the lock, and uses immutable revision plus selector-CAS progress. Publication reconstructs and proves the current source, emits at most one HEAD CAS per invocation, and preserves v1 decode while writing equivalence v2.

The effective-clock repair adds no schema, object class, deadline or public API. A maintenance epoch may have a logical `started_at` ahead of wall time, so its audit-only elapsed duration can temporarily be negative. Maintenance epochs are never timeout-adopted; this does not weaken fencing or recovery. The extra wall-clock sample after encoding is conservative: crossing expiry rejects before PUT.

A fixed-slot or deterministic-nonce protocol should not be added solely for losing concurrent progress races: it would introduce another claim/recovery protocol without satisfying a stated bound.

## Reliability

Expired recovery is limited to an exact matching in-flight job epoch or exact already-published pin bytes. Fresh unsubmitted work still receives live/compatibility checks and a final deadline check before the epoch PUT. Foreign epochs and unstable evidence fail closed.

Compatibility walks at most 32 manifests/64 MiB, and publication observation at most 64 manifests/64 MiB. When source/candidate ancestry cannot be proven within available authenticated evidence, the operation returns precondition/ambiguity and does not reuse outputs or submit HEAD. The frozen contract expressly rejects missing ancestry or unavailable protection. This is a fail-closed availability limit, not unsafe reuse or a required-success violation.

## Security

No scope/binding bypass, unvalidated path construction, authority-version downgrade or unauthenticated publication recovery was found. Descriptor identity binds scope, authority binding, source digests/generations, timestamps, nonce and render seed. Immutable readback and selector/HEAD CAS reconciliation compare exact bytes. Unknown/corrupt versions fail closed. No demonstrated security finding remains.

## Performance

The `bc4b27...` evidence reports maxima below every frozen limit: L1 1.006992x, cumulative allocations 4.341849x, conservative hashing 5.367573x, construction/source 1.012202x, and data ledger 0.802427. Phase verification checked 2,395 parent records and 79,035 integer leaf partitions exactly. The clock repair does not touch rendering/cost code, but final qualification must use the new exact-source scaling/resume/lifecycle reports now queued.

Allocation evidence measures cumulative invocation allocation rather than per-leaf allocation or RSS. Generic-GC one-closure peak is structural plus the 24-job no-retained-closure/final-corrupt-job-zero-deletion regression, without a runtime peak counter. The preserved eager outbox failure has no successful denominator.

## Tests

Earlier coverage includes catalog 907, workspace 2072, the 32x128 oracle, all 85 original write-fault schedules, 86 scaling cases, 15 resume schedules, three lifecycle schedules, reclamation, compatibility, corruption, expiry, fencing/ABA, production API exclusion, and the default benchmark's 256 setup commits/200 samples. The new future-clock epoch schedule is additional; original model operations and fault schedules remain unchanged.

Exact-source post-audit acceptance requires the full catalog suite including 21 reclamation tests, catalog/workspace Clippy, benchmark, new scaling/resume/lifecycle reports, fmt/diff, core/API checks, default/all-feature catalog libraries and cost smoke, workspace default/all-feature checks and docs/doctests. The failed wall-clock-guard attempt and failed `catalog-02` model run must remain preserved as diagnostics, not counted green.

## Style and actions

No style-only finding. Clock logic stays in the existing admission/epoch seam; `clock-lint-02` passes with warnings denied. No larger clock abstraction or new claim protocol is needed.

- P0: none.
- P1: resolved — mixed-clock activation epoch recovery, with preventing red/green reproduction and exact-source lint.
- P2: none.
- Closeout conditions: all post-audit commands must terminate successfully on `2a3c4e9...`; regenerate and verify reports/evidence; seal a fresh archive and independently verify external SHA-256, file manifest and exact-base patch reconstruction.

## Residual limits

Valid concurrent contenders can create distinct unselected immutable revisions: prepare_submission uses a fresh ULID, and select_revision writes the revision before selector CAS. The selected digest-linked history stays capped at 320; each invocation stays bounded; resume follows only the selected chain; GC streams bounded pages. The contract has no aggregate namespace-object/storage cap over offered concurrency. Unselected orphan metadata remains protected until the original deadline. This is a documented uncapped aggregate dimension, not a demonstrated P0/P1/P2.

Qualification remains local Darwin/MemoryBackend and credential-free. It does not establish provider behavior, remote CI, deployment, production readiness or Gates 6/7.
