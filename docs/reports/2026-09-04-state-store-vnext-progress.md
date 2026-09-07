# State Store vNext implementation progress

Baseline: `0235eb6c1552c5c87fe3a7638e10022889462b35` (PR #418).

Updated 2026-09-06. This worktree implements reclamation fencing, protection
from retained workspace roots, the deterministic operation-cost harness, and
the Gate 0 publication/reclamation schedules and independent logical model.
**Gate 0 passes locally** after resolving both retained-closure audit findings. Gates 2 through 7 remain
outstanding. Provider qualification, deployment, writer revocation, cutover,
and production readiness are unproven.

## Implemented behavior

- Authority format 5 requires `reclamation_generation` in HEAD and staged
  manifest, transaction, and checkpoint envelopes. Format 4 writers fail closed.
- Before deleting a nonempty GC candidate page, the retention-coordinated
  collector advances the generation with a checked exact-HEAD CAS. The selected
  manifest, logical sequence, writer epoch, and layout remain unchanged.
- A lost fence response must reconcile exact HEAD bytes and a stable version
  before deletion. A lost fence CAS invalidates the page. Candidate versions and
  the fenced HEAD version remain checked during deletion.
- Candidate paths carry the staging generation. Transaction identities also
  bind the exact observed HEAD version, allowing a frozen command to rerun after
  a HEAD-only transition without colliding with its abandoned artifacts.
- Maintenance regenerates after losing the fence CAS. Restore plans bind the
  generation in version 4; versions 1 through 3 are supersession-only evidence.
- Checkpoint materialization uses a fresh checkpoint-derived identity when it
  needs new segments. It may still reuse the selected, protected segment set.
- A prepared state reference requires acknowledged lineage and unexpired token
  protection, the currently selected manifest, or exact active external-root
  evidence. Staging an unpublished manifest does not create a durable pin.
- A prepared checkpoint reference does not extend the checkpoint's intrinsic
  retention. Beyond intrinsic retention, an exact active snapshot/export pin
  must protect the checkpoint and manifest. Released or expired pins cannot
  renew protection merely because the bytes remain.
- Export publication and retry revalidate source protection after acquiring
  retention coordination. Snapshot/export retained cuts resolve their authority
  again, and target-pin publication checks the current wall clock.
- Control GC automatically streams active workspace snapshot/export roots
  through bounded selector pages. It validates the selected revision chain,
  target binding, authority scope, and exact checkpoint/manifest digests before
  excluding their complete physical closures from the candidate page. Callers
  need not supply these roots as extra paths. Invalid root evidence aborts GC.
- A standalone checkpoint PUT with a lost response reconciles only against the
  exact immutable record bytes. Unresolved transport errors or cancellation
  preserve the durable publication epoch. A returned storage error is not proof
  that a delayed remote PUT has completed. Preparation and intermediate segment
  failures can leave only orphans and do not hold an uncertain publication epoch.
- Existing Arrow layout, full-state checksums, command retry behavior, hard
  limits, and seven-day orphan/thirty-day token floors remain in place.

## Deterministic coverage added

| Schedule | Checked outcome |
| --- | --- |
| Prepared commit crosses GC fence | Old CAS fails; token and data remain intact; fresh retry publishes |
| Commit pauses before/after transaction PUT | Stale publication fails; new identities carry the new generation |
| HEAD publication races before/after fence PUT | Deletion page is invalidated |
| Fence response is lost | Exact readback permits continuation; unavailable readback permits no deletion |
| DELETE is delayed across a normal commit | Authorized orphan is removed; remaining page aborts; restart replans |
| Reclamation generation is exhausted | No HEAD mutation or deletion |
| Maintenance pauses before/after manifest PUT | Outputs regenerate under the new generation; logical cut is preserved |
| Restore pauses before/after transaction PUT | Old plan is superseded; replanning succeeds |
| Frozen operation retries after writer-epoch claim | Fresh artifact identity avoids abandoned-object collision |
| Unpublished manifest is converted to a reference | Preparation fails |
| Prepared checkpoint reference outlives all protection | Resolution fails despite object existence |
| Active external root exceeds intrinsic checkpoint/token lifetime | GC automatically protects its closure; historical reads succeed |
| External pin is released while checkpoint bytes remain | Checkpoint and state-token validation reject revival; GC collects the checkpoint |
| Export outlives its source snapshot pin | Export independently protects authority across multiple selector pages |
| External pin selector is malformed | GC aborts before any DELETE |
| Checkpoint pauses before/after final PUT | GC waits until the retained checkpoint becomes visible |
| Checkpoint response is lost | Exact immutable readback reconciles and permits later publication |
| Checkpoint transport fails before a delayed remote PUT completes | Epoch remains in flight; GC cannot cross it |
| Checkpoint is cancelled before/after final PUT | Durable exclusion survives cancellation and lease expiry |
| Stale GC epoch is adopted while its DELETE remains pending | New generation fences old candidates; late DELETE cannot affect a fresh commit |
| Export waits for coordination while its source pin is released | First publication and retry fail without publishing a target pin |

Existing suites additionally cover checkpoint publication exclusion, retention
epoch recovery, restore ambiguity/recovery, corruption, root isolation,
historical reads, outbox incarnation semantics, and bounded GC pagination.

## Operation-cost harness

The old `control_mvp_gate` workload failed at L0 backpressure before reaching its
intended 256 setup commits. It also reported a point read as full replay and
counted retained tokens without validating their historical contents. The new
shared workload runs maintenance every 16 commits, and tests backpressure in a
separate root with maintenance intentionally absent.

The benchmark and ordinary PR test use the same workload. Measurements separate
begin, staging, point reads, bounded scans, commit, contention, full-command
retry, ambiguous publication reconciliation, maintenance, checkpoint, GC,
retained-token validation, and recovery. Backend accounting includes failed
attempts and counts GET, range GET, HEAD, PUT, LIST, LIST-page, DELETE, and HEAD
CAS. CAS is a subset of PUT and is not counted twice in request totals. Logical
storage calls are separate from backend attempts. The backend is `MemoryBackend`;
these are API attempts, not network requests or provider retry measurements.

Each operation reports transferred read bytes, attempted write bytes, request
counts, latency percentiles, and Rust allocation count/bytes during future polls.
Allocation measurement excludes harness-owned future storage, executor work
between polls, and spawned work; it does not measure process peak memory. The
workload uses no cache, so no cache savings are claimed. A deliberately duplicated
GET is verified to increase both backend requests and transferred bytes without
changing the logical-call count. A separate test checks allocations across a
future suspension.

Ratios preserve exact numerator/denominator pairs. Point-read amplification uses
returned value bytes; scans and retained-token validation use visible key/value
bytes; writes use staged key/value bytes. Maintenance amplification uses the
visible input state's key/value size (this workload has no outbox effects).
Operations without a logical byte denominator report no amplification ratio.
Losing logical candidates report discarded-candidate counts and abandoned
immutable-write attempt bytes. Candidate reuse is zero in this eager engine;
Gate 5 must instrument actual reuse when durable maintenance jobs exist.

The full profile contains 256 setup commits and 200 sampled commits; smoke uses
48 and 8. Every one of the 456 full-profile historical tokens is checked against
its exact expected contents after maintenance and again after GC. Contention
requires the stale candidate to lose exact HEAD CAS, then reruns the full read and
write command. Lost-response reconciliation verifies the resulting state. The
GC workload traverses all pages and verifies exactly one seeded orphan deletion.
A reopened store validates recovered contents.

The executable now emits an operation-cost JSON report, not a promotion-gate
report. This removes unsupported promotion and latency claims from the old
executable. Historical dry-run reports remain historical evidence.
The existing PR test job runs the smoke integration target with no latency
thresholds or cloud credentials. Provider prices are not inferred.

```sh
cargo test -p arco-catalog --test control_cost_smoke --locked
cargo bench -p arco-catalog --bench control_mvp_gate -- --smoke
cargo bench -p arco-catalog --bench control_mvp_gate
```

## Prior local verification (before this Gate 0 continuation)

- `cargo test -p arco-catalog --features test-utils --lib --tests --locked`:
  799 passed, zero failed, one existing ignored test across 27 suites.
- `cargo clippy -p arco-catalog --features test-utils --lib --tests --benches --locked -- -D warnings`:
  passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.

The full profile was also executed using `cargo test --bench control_mvp_gate`
in the unoptimized test profile; its latency values are diagnostic only.
The [raw local report](2026-09-05-state-store-operation-cost.json) records its
profile, backend limitations, request counts, byte ratios, allocations, and
latency diagnostics. Selected totals from that run:

| Operation | Samples | Backend attempts | Read bytes | Attempted write bytes |
| --- | ---: | ---: | ---: | ---: |
| `begin` | 456 | 12,410 | 19,445,221 | 0 |
| `point_read` | 3,200 | 68,800 | 76,912,000 | 0 |
| `scan` | 200 | 22,000 | 36,232,600 | 0 |
| `commit` | 200 | 1,000 | 0 | 1,728,312 |
| `contention_loser` | 1 | 6 | 479 | 8,778 |
| `maintenance` | 29 | 1,651 | 2,474,755 | 176,231 |
| `gc` | 1 | 29,321 | 38,059,068 | 10,373 |
| `retained_validation` | 912 | 115,324 | 175,128,470 | 0 |
| `recovery` | 1 | 19 | 30,909 | 0 |

Builds use `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`, and
`CARGO_PROFILE_TEST_DEBUG=0` to limit generated disk usage. These are local
checks on an uncommitted worktree; remote CI, provider execution, and deployment
were not run.

## Remaining work and qualification boundaries

The Gate 0 continuation adds exact immutable snapshot/export reconciliation,
validation of supplied legacy repair namespaces, and the explicit operator
recovery prerequisite that all remote publication mutations be resolved.
The requirement-to-test matrix below distinguishes new schedules from retained
contract coverage. Broader randomized restore/outbox faults remain Gate 7 work.

There is no new retention-renewal publisher or clone/fork API. Existing selected
revision chains and service retry validation remain the enforcement path for
retention extensions. Future publishers must validate live source protection
inside retention coordination before making a durable reference visible.

The Gate 1 deterministic harness is implemented and runs its intended workload.
Provider cost/latency evidence, future cache-hit metrics, and durable-maintenance
reuse measurements are not supplied by the current in-memory engine.

The [design clarification](../plans/2026-06-26-arco-tier1-single-authority-combined-vision.md#publication-compaction-and-internal-segment-maintenance)
distinguishes the legacy synchronous event-to-Parquet authority path from
internal storage-engine segment maintenance. The state store replaces the
former for migrated Tier-1 domains; Parquet becomes an asynchronous, watermarked
projection. Internal maintenance reorganizes already-committed state without
advancing its logical sequence and retains the existing backpressure safeguards.
Gate 2 only adapts existing maintenance readers/writers to the segment format;
Gate 5 adds durable incremental internal maintenance. Neither gate restores the
legacy compactor as the Tier-1 success gate or changes Tier-2 event contracts.

After Gate 0 verification, the remaining implementation sequence is:

2. Introduce the narrow range-read capability, independently decodable Arrow
   blocks inside immutable segments, authenticated directories/footer metadata,
   and cardinality-sized Bloom filters.
3. Specify and implement separate logical-history and physical-layout roots,
   maintenance semantic equivalence, and checkpoint validation records. Keep
   eager full-state checks until differential/corruption validation passes.
4. Implement lazy transactions with a pinned manifest and request-local overlay;
   logical conflicts must rerun the whole frozen catalog command.
5. Implement durable incremental internal segment maintenance: the manifest
   replay cut, retention-protected job progress at completed output-object
   boundaries, and safe reuse across ordinary concurrent logical commits.
   Preserve logical contents and sequence; this is physical-layout work, not
   legacy event-to-Parquet authority publication.
6. Add byte-bounded, scope/backend-isolated metadata and decoded-block caches,
   validation before admission, and concurrent miss coalescing.
7. Complete randomized models and fault schedules, required functional CI, and
   separately authorized exact-SHA real-S3 qualification and seven-day pilot soak.

No resident writer, WAL, cross-root transaction protocol, v4 migration, or
dual-reader path is introduced by this slice.

## Gate 0 continuation: implementation and exit matrix

The dedicated integration target is
[`state_store_reclamation_schedules`](../../crates/arco-catalog/tests/state_store_reclamation_schedules.rs).
It uses real `WorkspaceSnapshotService`, `ControlMvpStateStore`, and
`ControlMvpMaintenanceWorker` instances. Each faulted remote request runs in a
separate task that owns its request bytes and precondition. Issuance, application,
and response delivery have separate barriers; dropping the caller cannot cancel
the remote task. Timeouts guard deadlocks and never establish schedule ordering.
PUT and DELETE traces record paths, conditions, observed/result versions, and
operation order. The service clock override exists only under `test-utils`; its
backend uses the same clock for object timestamps. Normal builds use `Utc::now()`.

`RetentionMutationEpoch::put_immutable_reconciled` handles the snapshot/export
record, initial pin revision, and create-only initial selector. A successful
create or exact readback after a failed precondition succeeds. A transport error
succeeds only on exact readback; missing, different, or unreadable bytes preserve
uncertainty. Earlier uncertainty is never cleared by a later exact write.
Cancellation during PUT or reconciliation retains exclusion. Advanced selectors
still use the validated revision chain and are never overwritten by this helper.
Source lifetime is validated within retention coordination independently of byte
reconciliation.

`Reconciler::repair_with_scope` validates the report domain and the complete
candidate set before any deletion. It permits only canonical `snapshots/<domain>/`
legacy namespaces, or `state/executions/` for executions. It rejects control
objects, coordination objects, pins, cross-domain candidates, and unsafe paths.
Control reclamation still uses the maintenance worker's generation fence and
exact HEAD/candidate-version revalidation.

Publication epochs are never adopted automatically. The recovery API contract
and [runbook](../runbooks/control-plane-repair-and-dark-launch.md#recovery) require
all remote publication requests and legacy DELETEs to complete or be definitively
cancelled at the backend before operator settlement. Process death, lease expiry, and absent
readback do not establish that fact. Every new recovery schedule joins all its
pending remote operations before recovery. Existing recovery fixtures issue no
publication mutations. Automatic adoption is restricted to the new `control_gc`
operation kind, used only by generation-fenced control authority GC. Legacy
`catalog_gc` and `catalog_repair` records remain excluded regardless of age.
This adds a discriminator to the version-1 retention epoch envelope; older
records are read conservatively, and older binaries reject the unfamiliar kind.
Rollback to an older binary after even an idle `control_gc` record requires
operational coordination; this change does not supply a migration or rollback tool.
Authority format 5, restore format 4, and pin schemas do not change.

### Publication matrix

The fourteen integration tests expand into **167 deterministic fault schedules**,
**4 control-provider publication rejection cases**, and **3 retained-closure schedules**
and **32 fixed seeds of 64 model operations**. Counts below refer to cases within
tests, not separate Rust test functions.

| Requirement / publication boundary | Exact tests and schedule coverage | Invariant established |
| --- | --- | --- |
| Snapshot finalization: checkpoint L1 segment, index, checkpoint record | `checkpoint_artifacts_and_record_obey_publication_exclusion`: 3 boundaries x 5 faults = 15 schedules | Before/after-application cancellation, lost response, delayed remote application after transport error, and unreadable reconciliation retain exclusion when unresolved. Existing exact checkpoint-record reconciliation succeeds; segment/index errors remain conservative. GC cannot adopt the aged publication epoch. |
| Snapshot finalization: snapshot record, initial pin revision, initial selector | `exact_immutable_readback_reconciles_all_publication_classes`, `unresolved_publications_exclude_gc_even_after_cancellation_and_lease_expiry`, `immutable_preconditions_and_transport_readback_require_exact_bytes`: 3 boundaries x 8 cases | Exact lost-response or identical-winner readback succeeds and permits another coordinated publication. Different precondition winners conflict. Missing/different/unreadable transport readback and cancellation cannot authorize deletion. |
| Snapshot retry: missing initial revision and selector | Same three tests, `SnapshotRetry`: 2 boundaries x 8 cases | Existing immutable bytes and pin identity are preserved after a later commit. Retry completes the original cut; historical contents survive GC. |
| Export finalization: export record, initial pin revision, initial selector | Same three tests, `Export`: 3 boundaries x 8 cases | Export publication has the same exact-byte and uncertainty contract, and the acknowledged exported historical cut survives GC. |
| Export retry: missing initial revision and selector | Same three tests, `ExportRetry`: 2 boundaries x 8 cases | Existing export bytes and bound pin identity remain unchanged across retry and later logical commits. |
| Retention epoch claim and settlement | `lost_epoch_claim_and_settlement_responses_are_conservative`: 4 classes x 2 epoch writes x 5 faults = 40 schedules | Lost/unreadable responses, pending remote completion, and cancellation before/after application cannot silently clear an unresolved publication. An unapplied claim cannot have issued publication; an applied settlement follows completed mutations. All remote epoch requests finish before operator recovery. |
| Expiration during coordination wait | `expiration_while_waiting_for_coordination_rejects_every_publication_class`: 4 classes | Advancing the injected clock beyond retention while the lock PUT is paused causes rejection without target selector publication. |
| Source released during coordination wait | `released_source_while_waiting_cannot_publish_or_retry_an_export`: 2 classes | Both first export and retry revalidate source protection after acquiring coordination; still-present bytes cannot revive released protection. |
| Delayed DELETE across cancellation, collector restart, and new publication | `delayed_delete_survives_collector_cancellation_restart_and_new_retained_publication`: 1 schedule | The remote DELETE survives caller cancellation. A restarted collector adopts only the aged generation-fenced `control_gc` epoch; subsequent snapshot/export publication remains readable after late DELETE application. |
| Legacy DELETE error, cancellation before/after application, expiry, and restart | `legacy_delete_uncertainty_blocks_publication_even_after_stale_recovery_attempts`: 2 collectors x 4 publication classes x 3 faults = 24 schedules | Real repair and legacy GC retain exclusion while remote DELETEs survive cancellation or error. Aged legacy epochs cannot be adopted by either collector or control GC. Snapshot finalization/retry cannot revive the deleted compatibility object; independent valid export sources publish only after explicit terminal recovery. Retry records remain byte-identical. |
| Legacy required-object oracle | `legacy_collectors_preserve_acknowledged_snapshot_and_export_closures`: 2 collectors | Exact compatibility bytes and restore preflight remain valid for acknowledged snapshots and exports; source release preserves independent export protection; only releasing both permits deletion. These schedules cover legacy paths separately from the control model. |
| Legacy helper and recovery boundary | `legacy_epochs_are_never_adopted_and_cannot_use_fenced_deletion`, `cancelling_a_borrowed_legacy_delete_prevents_settlement_after_later_success` | Old serialized legacy operation kinds cannot acquire the safe control-GC exception. Cancelling only a borrowed DELETE future still prevents settlement, even after a later successful DELETE. |
| Provider objects in existing control roots | `control_provider_objects_in_existing_roots_survive_gc_until_release`: 1 retained-closure schedule | Control GC honors exact required and compatibility paths in existing active records in addition to authority closures. Snapshot/export preflight and object bytes survive collection and source release; releasing the final export allows reclamation. |
| Provider control-object publication and retry | `control_provider_objects_cannot_be_newly_published_or_retried`: 4 classes | All publication entry points reject provider-supplied control paths outside a canonical control implementation's declared manifest/checkpoint references, with the specific namespace-validation error. Existing immutable source/export fixture digests remain internally consistent. |
| Provider revival after control DELETE authorization | `control_provider_cannot_revive_a_pending_delete_after_epoch_settlement`: 1 fault schedule | A remote DELETE returns an error while control GC safely settles its epoch. Publication still rejects the visible candidate before that DELETE applies; no pin is acknowledged. |
| Earlier uncertainty and readback cancellation | Unit tests `exact_immutable_success_never_clears_an_earlier_uncertain_mutation`, `cancelled_immutable_precondition_readback_cannot_settle_the_epoch` | Success and identical-precondition readback cannot clear an earlier uncertain operation. Cancelling precondition reconciliation cannot leave a settleable invocation. |
| Supplied report namespace | Unit test `supplied_repair_report_cannot_escape_legacy_domain_namespace`: 7 forged reports | Invalid domain, control authority, retention coordination, pins, cross-domain paths, and traversal are rejected before even an earlier valid candidate can be deleted. Existing valid legacy repair tests remain in the full suite. |

### Retained contract matrix

The following existing tests remain part of the exit rather than being counted
as substitutes for the publication schedules above. Unless a suite is named,
these are unit tests in `state_store/control_mvp.rs`.

| Requirement | Retained tests | Invariant established |
| --- | --- | --- |
| Commit generation fencing | `reclamation_fence_invalidates_prepared_commit_without_changing_authority`, `reclamation_fence_rejects_commit_paused_before_and_after_artifact_put`, `frozen_operation_retry_after_head_only_change_gets_fresh_artifact_identity` | A staged or paused old-generation command cannot publish after reclamation; the complete command reruns with fresh identities and the logical cut is preserved. |
| Fence ambiguity and competing HEAD | `reclamation_fence_lost_response_requires_exact_readback_before_delete`, `reclamation_fence_racing_authority_invalidates_the_deletion_plan` | Stable exact HEAD bytes/version are required after ambiguity; a competing HEAD prevents deletion from the old plan. |
| Generation exhaustion | `reclamation_generation_exhaustion_never_authorizes_delete` | Checked generation overflow causes no HEAD mutation or deletion. |
| Maintenance and restore | `maintenance_crossing_reclamation_fence_regenerates_outputs`, `restore_crossing_reclamation_fence_is_superseded`; `workspace_snapshot_restore` and `state_store_control_mvp` integration suites | Maintenance regenerates under the new generation. Old restore plans are superseded; the existing restore recovery, corruption, historical-read, and outbox incarnation contracts remain covered. |
| Partial deletion and stale collector | `delayed_delete_with_concurrent_commit_aborts_remaining_page_and_restarts`, `stale_gc_epoch_recovery_cannot_reenable_pre_fence_candidates`; reconciler `repair_continues_after_failed_delete_but_retains_uncertain_epoch` | A late delete cannot validate the rest of a stale page. Restart replans under a new generation; failed legacy deletes allow remaining candidates to be processed but keep the epoch in flight. |
| Source protection beyond object existence | `persisted_reference_preparation_rejects_an_unpublished_manifest`, `expired_checkpoint_cannot_be_revived_by_preparing_a_longer_reference`, `released_external_pin_cannot_revive_still_present_checkpoint_bytes` | Staged, expired, or released source bytes cannot establish new retained authority. |
| External root closure and independent export lifetime | `active_external_pin_protects_checkpoint_closure_after_intrinsic_expiry`, `retained_export_independently_protects_authority_across_selector_pages`, `malformed_external_pin_aborts_control_gc_before_any_delete` | Active external pins protect the full closure after intrinsic retention ends; export protection survives source release and multiple selector pages; malformed evidence aborts GC. |
| Floors and bounded pagination | `control_gc_deletes_only_aged_unreachable_artifacts`, `control_gc_honors_checkpoint_retention_above_thirty_day_floor`, `control_gc_pages_and_converges_beyond_ten_thousand_objects` | The seven-day orphan floor, thirty-day token/checkpoint floor, larger checkpoint retention, and bounded GC pages remain enforced. |
| Advanced selectors and identity | `workspace_snapshot_services::{snapshot_exact_retry_accepts_a_valid_active_advanced_target_pin,export_exact_retry_accepts_a_valid_active_advanced_target_pin,exact_retries_reject_target_pin_renewal_beyond_the_immutable_cut_deadline}` plus its target/source pin-conflict tests | Valid advanced revision chains remain selected. Retries cannot alter immutable pin bindings or exceed the cut deadline. |
| Recovery classes | `retention_coordination::{adoption_never_covers_recent_reclamation_or_publication_epochs,an_aged_stale_reclamation_epoch_is_adopted_by_the_next_lease_holder,terminal_reconciliation_requires_the_exact_operation_identity}` | Only aged generation-fenced `ControlGc` epochs are automatically adoptable; terminal restore reconciliation still requires exact identity. |

### Independent model and audit

`independent_reclamation_model_32_seeds_of_64_operations` uses seeds 1 through 32.
Each 64-operation history exercises commits, repeated-key updates, deletes,
checkpoints, snapshots, exports, partial-pin retries, pin release, expiration,
maintenance, GC pages, and reopening. Maintenance runs before the existing L0
backpressure limit. The oracle records acknowledged logical maps, exact returned
tokens, original immutable record bytes, and pin lifecycles without consulting
`plan_gc` or the collector's protection-set calculation. Current reads and every
still-protected historical read are compared after each operation. A seeded
orphan must actually be collected; active retained roots must never refer to a
path for which the backend observed an authorized control DELETE. Failures print
the fixed seed and operation/acknowledgment trace.

The local audit checked completion, correctness, cancellation and ambiguity,
namespace boundaries, source lifetime, generation/version fencing, performance,
and test hygiene. It found and closed the readback-cancellation gap in the new
helper: uncertainty had initially been restored before awaiting failed-create
readback. A failing regression now requires it to remain set through that await.
The earlier red/green regressions reproduced lost immutable response handling,
forged repair-report acceptance, and the absence of an injected expiry clock.
Final verification also exposed a fixture error that addressed the lock's camelCase
expiry field by its Rust spelling. The helper now uses typed `LockInfo` and checks
that the persisted lease is actually expired before testing the durable epoch.
The fresh independent audit then identified two retained-closure defects. Repair
could settle after a pending legacy DELETE, and automatic legacy epoch adoption
could admit later publication. A real regression failed with "unresolved DELETE
must prevent settlement"; legacy uncertainty and recovery are now conservative.
The follow-up audit found that control GC dropped provider-required objects and
allowed later providers to revive deletion-authorized control paths. Separate
regressions reproduced ordinary deletion of an active required object and
unsupported control-path publication. GC now streams complete active required
paths; publication only accepts control manifest/checkpoint objects tied to a
validated canonical control implementation reference. Older retained records
remain readable and protected until release/expiry, but cannot be newly published
or retried with unsupported control paths. These deterministic closure oracles
cover provider paths separately from the 32-seed state-store model.
The fresh subagent's bounded source re-review approved both fixes and found no
remaining blocker in these paths. It independently checked publication entry
points, canonical namespace validation, old-record protection, and causal test
fixtures, and ran `git diff --check`. Cargo verification below was performed by
the primary agent, sequentially; the subagent did not claim independent runtime
verification. Direct raw writes to retention records are outside the service
publication trust boundary.
The only new persisted discriminator is `control_gc`; engine formats, retention
floors/caps, writer epoch, renewal publishing, and restore schemas remain unchanged.

The new target runs through ordinary PR CI's existing `cargo test --workspace
--all-features --exclude arco-flow --exclude arco-api` step. The explicit
cost-smoke target remains unchanged; no duplicate model invocation is added.
The audit fix changes the control GC epoch discriminator and adds a deletion
kind check, and expands streamed retained roots to include their exact required
paths. The Gate 1 stored full profile remains the original regression
baseline; a follow-up full profile is run below to verify the measured paths. Broader randomized
restore/outbox fault qualification remains Gate 7 scope.

### Final Gate 0 verification

**Verdict: Gate 0 passes locally after the audit fixes.** Both P0 findings are
resolved, the fresh subagent approved the bounded source changes, all required
publication/closure schedules and model comparisons pass, and the required local
checks below completed successfully. This supersedes the withdrawn earlier exit.
This is an uncommitted local candidate at baseline
`0235eb6c1552c5c87fe3a7638e10022889462b35`; it is not remote CI or provider evidence.

| Command | Actual final result |
| --- | --- |
| `cargo test -p arco-catalog --features test-utils --test state_store_reclamation_schedules --locked` | 14 passed, 0 failed; 167 fault schedules, 4 control-provider rejection cases, 3 retained-closure schedules, and 32 seeds x 64 operations; dedicated run completed in 89.39 seconds |
| `cargo test -p arco-catalog --features test-utils --lib --tests --locked` | 818 passed, 0 failed, 1 intentionally ignored across 28 suites; includes all 48 snapshot/export service tests, 48 restore-service tests, and all 3 cost-smoke tests |
| `cargo clippy -p arco-catalog --features test-utils --lib --tests --benches --locked -- -D warnings` | Passed with no warnings |
| `cargo fmt --all -- --check` | Passed |
| `git diff --check` | Passed |
| `cargo test -p arco-catalog --bench control_mvp_gate --locked` | Full profile passed: 256 setup commits, 200 samples; backend counts and byte totals match the original baseline across all 17 phases |

The [follow-up full cost report](2026-09-06-state-store-operation-cost.json)
preserves the same workload as the [original baseline](2026-09-05-state-store-operation-cost.json).
All backend request/byte accounting is identical, including GC's 29,321 attempts,
38,059,068 read bytes, and 10,373 attempted write bytes. GC allocation accounting
increased by 7 calls and 56 bytes; other phases' allocation totals are identical.
This in-memory workload
does not exercise provider-object retention; the separate deterministic schedules
above establish that invariant. Latencies from the unoptimized test profile remain
diagnostic and are not provider qualification.

The ignored test is `generate_golden_schemas`, an existing manual golden-file
generator. Rust commands ran sequentially with `CARGO_INCREMENTAL=0`,
`CARGO_PROFILE_DEV_DEBUG=0`, and `CARGO_PROFILE_TEST_DEBUG=0`. The full suite passed
on the final behavior; subsequent lint-only changes fixed redundant visibility
and test-helper typing/access, and the dedicated schedule target passed again.
All 48 snapshot/export and 48 restore-service tests and the 3 cost-smoke tests
passed. The original audit regressions and new provider-closure checks are part
of the ordinary CI target. Baseline and previous uncommitted
implementation/cost evidence are preserved. No commits, pushes, provider tests,
deployment, cutover, or production promotion were performed.

Authority format 5, restore-plan format 4, `StateToken`, pin schemas, retention
floors, hard caps, backpressure, and the separate writer epoch are unchanged by
this continuation. Gate 1's stored full cost profile remains the regression
baseline; Gates 2 through 7 remain the next implementation sequence.

Follow-up local logs are retained at `/private/tmp/state-store-vnext-audit-fix-tests-final.log`,
`/private/tmp/state-store-vnext-audit-fix-schedules-final.log`, and
`/private/tmp/state-store-vnext-audit-fix-clippy.log`. The failing legacy and
provider regressions are preserved in `/private/tmp/state-store-vnext-legacy-red.log`
and `/private/tmp/state-store-vnext-control-provider-red.log`; the latter's
initial delayed-error fixture also needed the expected collector error corrected
before its successful post-fix run. These logs are local evidence, not CI artifacts.
