# Gate 5 durable maintenance qualification

Gate 5 adds durable, incremental segment maintenance with authenticated recovery and physical-output reuse across compatible logical descendants. Local qualification and the independent source, evidence and recovery audits pass. Gates 6 (caches) and 7 (provider qualification) remain outstanding.

The implementation was qualified against base `745ed92e25ff7b4ec85aa0be57b02450642fda59`. The 683-file source manifest is `2a3c4e9c32ab02f616810c0bb07d448ac1d34dccd45570a37be601f6fd9d537e`; formatting before commit made no source changes. This report and the combined vision links were prepared for the local commit after the recovery archive was sealed. The archive remains the unchanged pre-commit checkpoint.

## Behavior and bounds

A job requires an independently supplied durable authority binding and externally supplied content-addressed descriptor identity. Immutable plan pages, digest-linked receipts and CAS-selected progress authenticate completed outputs. Each advance completes one validated segment/directory pair. The render cut owns all source L1 inputs and the exact L0 prefix; compatible descendants retain those inputs and append an unchanged ordered suffix. Publication reconstructs current authority, authenticates completed outputs, checks full semantic/history/physical equivalence and attempts one fenced HEAD CAS. Consumed attempts require fresh validation before physical-output reuse. Logical-command outputs cannot be reused.

Execution expires 24 hours after creation, without renewal; retention ends after eight days. Expired activation recovery requires proof of an earlier submission or exact already-published pin bytes. Unsubmitted recovery receives fresh admission under retention coordination. Shared admission rejects supplied time before creation and checks expiry against the later of supplied time and wall time. New maintenance epochs record the admitted effective clock, allowing exact repair of interrupted future-clock submissions without extending deadlines. Metadata-only resume and no-work paths cannot bypass expiry. Missing, conflicting, corrupt or unavailable evidence fails closed.

Generic catalog GC authenticates and discards one expanded maintenance closure at a time, validates the complete inventory, then deletes only in legacy namespaces. Dedicated control GC streams authenticated maintenance protection over bounded candidate pages. Full repair rejects control-store or retention candidates before any deletion. Whole-job orchestration lives in shared test/benchmark support; the public transaction trait is unchanged.

| Boundary | Retained limit |
|---|---:|
| Incremental advance | 1 shard, 2 outputs, 1 receipt, 1 selector CAS |
| Selected construction | 64 blocks, 64 owners, 64 MiB returned/raw Arrow |
| Job output | 256 shards, 512 references |
| Plan | 256 pages, 64 KiB/page, 8 MiB aggregate |
| Descriptor | 64 KiB |
| Selector/revision | 8 KiB each, 4 direct references/revision |
| Selected progress history | 320 revisions |
| Resume before construction | 1,280 metadata requests, 192 MiB, zero source reconstruction |
| Publication | 1 HEAD CAS/invocation, 16 submissions/job |
| Completed units | Zero rerendering or output PUTs |
| Rejected capacity admission | Zero PUTs |
| GC candidate/selector pages | 256 entries |
| Generic GC expansion | 1 job closure at a time |

Authority 7, restore-plan 6, segment/directory 1 and continuation 3 remain unchanged. Rewrite-equivalence v2 retains v1 readability and rejection by unsupported older readers.

## Verification and acceptance

The initial 54 mandatory and credential-free CI-equivalent lanes passed on source `bc4b27b29eab0ec33e0bc13adcb3d0403a8acc5ad5c470037a5f30814dd6875b`. A fresh audit found an additional mixed-clock activation defect. The repaired final source passed all 18 affected lanes, including 908 catalog tests, 21 reclamation schedules, the 32-seed x 128-operation maintenance model, the prior 32 x 64 reclamation model, all 85 original remote-write fault schedules and the new interrupted-activation recovery regression. Core passed 302 tests; default/all-feature catalog libraries passed 435/468; workspace doctests passed 37. Counts overlap.

Final-source Clippy, default/all-feature workspace checks, API check, documentation, default cost smoke and the benchmark passed. The benchmark retains 256 setup commits, 200 samples and 456 validated historical cuts. Scaling, resume and lifecycle acceptance passed 86 cases, 15 schedules and three schedules respectively.

Initial broader runtime evidence remains explicitly pinned to its source: workspace 2,072 tests, API 495, Flow integration 750, default/legacy Flow libraries 557/585, deterministic UAT, protocol, repository and Python checks. Pinned cargo-deny 0.18.9 passed policy on identical base/candidate dependency inputs and passed advisory checks. No policy waiver or dependency change was made. The independent reviewer confirmed the affected rerun scope: generic retention claims retain their behavior and downstream source, traits and dependencies are unchanged.

| Cost ratio | Observed maximum | Limit |
|---|---:|---:|
| New L1 bytes / eager | 1.006991525424 | 2 |
| Cumulative allocations / eager | 4.341848824789 | 8 |
| Disjoint hashing / conservative eager | 5.367230308219 | 8 |
| Construction reads / authenticated source | 1.012201725711 | 2 |
| Uninterrupted data reads / (4S + C) | 0.802426994740 | 1 |

`S` is eager source-data reads; `C` is complete candidate-data readback. Full preflight and reconstruction remain charged. Selection, source metadata, selected-data reads, rendering/validation, completed-output reuse and final equivalence have disjoint leaf counters, including zero-work phases. The reviewer independently verified all 2,395 cost parents and 79,035 integer partitions and reproduced the maxima from raw reports. Nested hashing classifications are not counted twice. Repeated resumes, descendant validation and failed publication attempts remain separately charged.

The exact-base eager reproduction retains 32 ordinary rewrites and 54 forced test-only rewrites, with ordinary no-intent observations separate. Its independently recomputed ratios produce the same maxima. The known eager outbox-ordering failure retains no successful denominator. The required eager fixture is checked in because benchmark support includes it at compile time.

The evidence index preserves 97 runs, including 13 failed runs and their dispositions. The rejected first clock fix failed the mandatory model and was removed. Two report-script mistakes and their corrected calculations are also retained. No failure is counted as a pass. The final independent audits have no open P0/P1/P2 finding.

Compact checked-in records:

- [Source manifest](gate5-requalification-01/source-stabilized-04.json).
- [Per-case acceptance calculations](gate5-requalification-01/cost-comparison-02.json).
- [Verification stages and terminal counts](gate5-requalification-01/verification-summary-post-audit.json).
- [Phase-partition verification](gate5-requalification-01/phase-partition-report-verification-02.json).
- [Independent source audit](gate5-requalification-01/fresh-audit-source-04.md).
- [Independent terminal-evidence audit](gate5-requalification-01/fresh-audit-terminal-05.md).

Names of raw reports, logs, frozen plans and intermediate artifacts in these records refer to archive members. They are preserved evidence, not normative documentation dependencies.

## Recovery and limitations

The local recovery archive is `/private/tmp/arco-gate5-final-requalification-20260908-01.tar.gz`, SHA-256 `74176264de961b39e64d786cf27a5e5edf8e9dbc7a016bf9a9592a5d122f823b`. It contains the full 1,916-file pre-commit candidate, binary patch, original failures, per-file manifest and recovery instructions. The independent reviewer compared every file with the worktree and reconstructed the patch from the exact base with zero missing/extra files or byte differences. The external closeout and independent-verification record sit beside the archive. The original 1,528-file recovery worktree and archive remain preserved.

Large raw measurements, logs and frozen planning documents remain local and archived. They are intentionally excluded from this commit; several raw reports exceed 100 MiB. The original frozen documents retain their exact bytes. This committed report provides the qualification summary without importing transient branch names or raw terminal output into repository documentation.

Evidence is local Darwin and credential-free MemoryBackend qualification. Allocations are cumulative invocation totals, not RSS or per-phase allocation distributions. The one-closure GC peak is structural, supplemented by a 24-job regression. Compatibility and publication observation walks are bounded and reject unavailable ancestry. The 320-revision limit bounds selected history; concurrent valid calls may leave additional unselected immutable metadata protected until the original deadline. No aggregate offered-concurrency storage bound is claimed. Remote CI, provider qualification, deployment, migration and cutover are outside this local result.
