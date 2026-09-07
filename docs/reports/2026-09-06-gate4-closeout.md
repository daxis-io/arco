# Gate 4: lazy control/v1 transactions

**Gate 4 is complete locally.** All required verification lanes pass, the fresh
independent audit approves the candidate with no P0/P1/P2 findings, and the final
recoverable checkpoint contains the source, documentation, evidence, and logs.
Gates 5–7 remain outstanding.

The candidate is the uncommitted diff from
`ca5023caa3ec1eb5b39edeef6291082f544a5153` in
`/Users/ethanurbanski/arco/.worktrees/state-store-vnext`, on
`codex/state-store-vnext`. The [approved plan](../plans/2026-09-06-state-store-vnext-gate-4.md)
defines the acceptance contract. No commit, push, merge, deployment, cutover, or
credentialed provider operation was performed.

## Resulting behavior

Transaction begin pins authenticated authority metadata without reconstructing
data. Point reads authenticate selected blocks and retain the difference between
never-present keys, live generations, and tombstone generations. Reads see the
request's staged writes; explicit assertions continue to inspect the pinned
base. Completed point, range, predicate, and scan observations are checked again
against freshly reconstructed pinned state at commit.

Ordered scans merge staged changes before pagination. Every transaction cursor
has a fresh local origin, including genesis cursors with no durable state token.
Later pages reflect staged changes above the exclusive boundary. Public readers,
other transactions, and opaque encoding reject these cursors. Public continuation
wire version 3 is unchanged.

Outbox staging and trimming helpers are asynchronous. Selective ID lookup checks
all owning L1 directories, including keyless shards, and reads only matching
outbox blocks. Trims are checked against exact incarnations; a failed or
cancelled multi-target validation stages no partial trim batch. Catalog, path
governance, projection acknowledgements, tests, benchmarks, and API callers use
the new helper signatures. `ArcoStateTxn` signatures are unchanged.

Commit reloads the pinned manifest and uses complete `replay_for_successor`
reconstruction before publication. Redundant-anchor equivalence, promoted-base
revalidation, semantic checksums, history/layout roots, rendered transaction and
anchor validation, capacity gates, writer fencing, immutable publication, exact
HEAD CAS, and uncertain-outcome handling remain executable. A lost CAS consumes
the attempt; `CatalogAuthority` reruns the frozen command and regenerates its
decisions, response, receipt, intents, and candidate objects.

The [integrity contract](../plans/state-store-integrity-format-v1.md) and
[token contract](../spec/state-token-and-checkpoint-contract.md) record the access,
cursor, compatibility, and lifetime rules. Authority 7, restore plan 6, and
segment/directory version 1 are unchanged.

## Executable cost evidence

The [block-scaling report](2026-09-06-gate4-block-scaling.json) and
[lazy-transaction report](2026-09-06-gate4-lazy-transactions.json) each contain the
same 17 fixture shapes as Gate 3: four block targets, four block-count cases,
four L1-count cases, and five L0 suffixes. Oversized rows, disabled filters,
projection resolution, genesis, and keyless outbox lookup are retained or added
as exceptional cases. The [comparison](2026-09-06-gate4-cost-comparison.json)
preserves exact numerators, denominators, matched lifecycles, test counts, and
hashes of the 28 prior reports that remain byte-for-byte unchanged. The progress
report is the explicitly requested historical-report update.

For the 4,096-row, 64 KiB target, one-L1, no-L0 fixture:

| Operation | Returned bytes | Cumulative allocation bytes |
| --- | ---: | ---: |
| Real eager reference begin | 4,717,175 | 51,134,027 |
| Lazy begin | 2,762 | 19,247 |
| First point read | 99,929 | 254,356 |
| Repeated memoized point | 0 | 0 |
| Complete begin + point + rollback | 102,691 | 273,683 |
| Complete range witness capture | 4,714,413 | 10,574,047 |
| Repeated identical witness | 0 | 1,570 |

Begin + point + rollback uses **2.177% of the returned bytes and 0.535% of the
cumulative allocations** of the measured eager reference, below both 10%
limits. Gate 3's preserved eager allocation was 51,131,615 bytes; the new real
reference includes the test-only pin/counter bookkeeping and is separately
reported. No lazy begin is labelled as an eager measurement.

| Required bound | Terminal executable result |
| --- | --- |
| Stable begin: one HEAD metadata call, pointer read, manifest read | Passed in all 17 lazy fixtures; no transaction, directory, or data reads |
| Genesis begin: one HEAD metadata call, no object reads | Passed in exceptional genesis fixture |
| Begin: zero replay rows, full checksums, rendered validations | Passed in all lazy fixtures and genesis |
| Cold point: at most `2N+1` metadata objects and `N+1` data blocks | Passed; no full-segment fallback |
| Negative point: outside bounds skips directory/data; Bloom-negative skips blocks | Passed in the corresponding no-L0 block fixtures |
| Memoized point or identical completed range witness: no additional reads | Passed in all 17 lazy fixtures |
| Overlay scans: at most `B+(N+1)(P-1)` block reads | Passed in all 17 lazy fixtures; no full-segment fallback |
| Outbox ID: at most `S+2N` metadata/candidate blocks; zero KV blocks | Passed in KV-only fixtures; keyless mixed fixture selects one outbox block |
| Begin and cold point allocations: at most `12 × returned bytes + 128 KiB` | Passed in all 17 lazy fixtures |
| 4,096-row no-L0 begin/point/rollback: at most 10% of eager bytes/allocations | Passed across all corresponding block-target and L1 fixtures |
| Matched no-read lifecycle: lazy data-read bytes no greater than eager | Passed in all 17 fixtures, with reconstruction/checksum counters asserted |

The matched no-read lifecycle for the example fixture reads **9,354,540 data
bytes in each implementation**. Eager begin + commit returns 9,431,588 total
bytes; lazy begin + commit returns 9,433,926, including an extra 2,338-byte
manifest validation read. Both consume 8,192 replay/apply rows and four complete
semantic checksums. At L0 suffix 31 both attempts intentionally fail the existing
capacity gate before the first PUT. Reduced total commit work is not claimed.

The [default operation report](2026-09-06-gate4-operation-cost-default.json) and
[instrumented operation report](2026-09-06-gate4-operation-cost.json) each preserve
256 setup commits and 200 samples. The latter separates commit replay, candidate
rendering, immutable publication, and HEAD CAS within commit and contention
operations. It also records full-command retry, uncertain-outcome reconciliation,
maintenance, checkpoints, retained validation, GC, and recovery. All 456
historical workload tokens are checked after maintenance and again after GC.

Nested phase counters are already included in operation totals. SHA helper work
includes raw/envelope digest validation, canonical roots, and full semantic
checksums; these must not be added together. Witness input bytes include length
words, generations, and discriminators. Rendered validation includes its replay
and checksum work. Allocation totals count allocator requests during future
polls, exclude spawned work, and do not measure RSS.

## Verification and provenance

[Verification JSON](2026-09-06-gate4-verification.json) records exact commands,
environment overrides, Rust/Cargo versions, terminal exit codes, elapsed times,
log paths/hashes, and 492 source-input hashes. The tested-source aggregate is
`fe58a12ce53f9fca6922268a02aa44b49af2b811c44ca2056331edd5fd865c8e`.
Source bytes were unchanged throughout the final lanes. Cargo ownership was
sequential, with `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`, and
`CARGO_PROFILE_TEST_DEBUG=0`.

| Lane | Result | Terminal log under `/private/tmp/arco-gate4-verification-20260906/final/` |
| --- | --- | --- |
| Gate 0 reclamation schedules | 16 passed, including the 32-seed × 64-operation model | `01.log` |
| Catalog library and integration tests | 868 passed across 29 targets; three ignored | `02.log` |
| Catalog strict Clippy, including benches | Passed with `-D warnings` | `03.log` |
| Original default benchmark workload | Passed, 256 setup / 200 samples | `04.log` |
| Workspace formatting | Passed | `05.log` |
| Diff whitespace check | Passed | `06.log` |
| Authenticated block scaling | 17 fixtures passed | `07-retry.log` |
| Lazy transaction scaling | 17 fixtures passed | `08-retry.log` |
| Core tests and doctests | 295 passed, six ignored documentation examples | `09-retry.log` |
| API all-targets compatibility check | Passed | `10-retry.log` |
| Instrumented original benchmark workload | Passed, 256 setup / 200 samples | `11-retry.log` |

The two scaling tests ignored by the ordinary catalog suite pass in their
explicit lanes. Its remaining ignored test is the existing golden-file
generator. Gate 0's separate 16-test lane is also included in the catalog suite;
these counts are not additive. The first block-scaling invocation completed its
assertions but failed to write a relative report destination from Cargo's crate
working directory. `07.log` and its nonzero exit status remain in the evidence;
the absolute workspace destination retry passed without source changes.

Focused regression logs, including the initial failing tests and subsequent
passing runs, are retained in `/private/tmp/arco-gate4-verification-20260906/`.
The independent logical oracle models pinned snapshots, overlays, observations,
assertions, generations, ranges, and HEAD outcomes without calling production
replay or witness helpers. The lazy/eager differential test runs 16 seeds × 64
operations. Other regressions cover corruption after memoization, missing pinned
authority, cursor isolation, tombstones and empty progress pages, request budgets,
maintenance/reclamation/retention transitions, exact outbox incarnations, failed
and cancelled trim staging, and regenerated catalog outputs after a CAS loss.

## Independent review and recovery

The [fresh independent audit](2026-09-06-gate4-audit.md) **approves local Gate 4**
with no demonstrated P0/P1/P2 findings. The reviewer started with no inherited
implementation conversation and received the approved contract, exact base,
complete binary diff including new files, source hashes, terminal logs, cost
reports, and a verified pre-audit checkpoint. The audited patch SHA-256 is
`dfbb6834041df4aa9924febd71096c97f756357ee3ad490504ca215c988be4a5`.
The reviewer independently checked source/log hashes, all mandatory contracts,
all 17 scaling shapes, nested counter partitions, no-read lifecycle equality,
and zero-PUT capacity failures. No source correction or affected-check rerun was
needed. Final documentation records that verdict; source hashes remain unchanged.

Final checkpoint:
`/private/tmp/arco-gate4-checkpoint-20260907-030154-final.tar.gz`.
Its SHA-256 is recorded in the adjacent `.tar.gz.sha256` file and in
`/private/tmp/arco-gate4-checkpoint-result.json`. The archive contains complete
copies of every changed/new worktree file, a binary patch including untracked
files, payload hashes in `MANIFEST.json`, terminal logs, audit evidence, helper
scripts, and `RECOVER.md`. Git metadata, generated build trees, and credential
files are excluded. Every archive member was verified against its payload hash;
every worktree member was compared with the final worktree. The binary patch was
also applied to exact-base files in a generated isolated directory, and all
recovered bytes matched.

To recover, first verify the adjacent archive hash. Extract into a separate
directory, create a fresh checkout at the exact base above, and apply
`changes.patch` with `git apply --binary`. Verify the recovered files against
`MANIFEST.json` and the tested-source hashes in the verification report. Preserve
the current worktree and prior checkpoints. This is recovery guidance, not
authorization to publish or advance Gates 5–7.

Pre-audit checkpoint:
`/private/tmp/arco-gate4-checkpoint-20260907-025021.tar.gz`, SHA-256
`7973d4b9db293a3d279ba2082ba1a8299e6ea29ec4ae3a5db28c0fe134ec675b`.
Its binary patch was applied to exact-base files in a generated isolated
directory, and all recovered files matched the candidate. Every archived member
was checked against its manifest, and every archived worktree file against the
candidate. This checkpoint predates the final review/closeout documents.

## Limits and remaining work

The evidence uses local `MemoryBackend` fixtures, with no provider qualification,
deployment, cutover, or production-readiness claim. The 64 MiB / one-million-entry
request accounting cap is not an RSS bound. Complete range evidence streams in
bounded chunks without a new total-range limit; complete commit reconstruction
and other eager boundaries remain separately measured work.

Transaction pins and cursors are nondurable and do not renew retention.
Memoized successful reads may remain available after collection, but a later
required object read or commit fails closed when its pinned closure is missing
or corrupt. Async outbox helpers are an approved source compatibility change.
Gates 5–7 are not implemented or qualified by this work.
