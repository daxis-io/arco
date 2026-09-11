# Gate 6: authenticated, byte-bounded read caches

Frozen before production changes or candidate measurements, 2026-09-09.
Base: `d246e9522a031a475a0e20cd86c1c8dd5f18dbbb`, parent
`745ed92e25ff7b4ec85aa0be57b02450642fda59`, tree
`9149b4e7874de0c352301b3b3261ae06cb41a3e2`. The supplied 683-file
Gate 5 manifest is `2a3c4e9c32ab02f616810c0bb07d448ac1d34dccd45570a37be601f6fd9d537e`.

## Execution and authority

Use `.worktrees/state-store-vnext-gate6-20260909-01`, the branch recorded in
the external execution checkpoint, a single source writer and sequential
Cargo queue. Fresh `git fetch --no-prune origin` succeeded; origin/main remains
`745ed92e25ff7b4ec85aa0be57b02450642fda59`. The base stays pinned regardless of
remote movement. Root untracked files and Gate 5 evidence remain untouched.
Use a newly owned target with CARGO_INCREMENTAL=0, CARGO_PROFILE_DEV_DEBUG=0 and
CARGO_PROFILE_TEST_DEBUG=0. External execution evidence is under
`/private/tmp/arco-gate6-20260909-01`. Gate 5 archives are reference evidence;
never apply their recovery patches over the Gate 5 commit.

Reproduce unchanged Gate 5 benchmark/acceptance workloads. Extend the existing
measurement harness first, hash the test-only overlay separately, then run
identical common workloads on exact base production and candidate source.
Preserve original reports and failures under their original names.

## Contents and public interface

One private module caches authenticated segment directories (owned encoded bytes
and decoded metadata), authenticated unhydrated transaction metadata, validated
owned ControlMvpSegmentRow blocks, and small complete-segment validation
certificates. Manifests, checkpoints, HEAD, maintenance descriptors/pages/
revisions/selectors, retention, restore evidence and publication reconciliation
remain uncached. Overlays, staged writes, hydrated transactions and rendered
publication candidates remain request-owned.

Expose an opaque cloneable ControlMvpReadCache, metadata/decoded capacity
configuration and aggregate statistics. Store constructors enable defaults;
clones share; separately reopened stores start empty. Support configuration,
obtaining/attaching compatible handles, and explicit disabling. Do not change
the public transaction trait.

Retain StateStoreBindingIdentity (and therefore its originating backend Arc),
tenant, typed physical root, workspace context and domain. Reject attaching to
another backend or scope; equal provider configuration does not imply identity.
Keys bind complete owning reference, canonical object identity, class, level,
logical sequence, sizes, digests, transaction history, block descriptor,
supported format and observed version. Maintenance additionally uses the
independently supplied DurableAuthorityBinding as an explicit namespace.

## Frozen ownership limits

| Bound | Value |
| --- | ---: |
| Metadata pool including reservations/live evictions | 32 MiB |
| Decoded pool including reservations/live evictions | 128 MiB |
| Final metadata entry | 8 MiB |
| Final decoded block | 8 MiB |
| One metadata reservation | 16 MiB |
| Aggregate reservations across pools | 64 MiB |
| Concurrent cache-owned loads | 8 |
| Live metadata records, including leased evictions | 1,024 |
| Live decoded records, including leased evictions | 4,096 |
| Participants per load, including initiator | 32 |
| Aggregate participants | 256 |

Decoded capacity is twice the existing 64 MiB operation limit. Gate 5 source
and output maxima are 8,151,306 and 8,471,341 bytes. Metadata capacity is half
the operation limit and exceeds ordinary 1–2 KiB metadata. These capacity
choices do not guarantee admission of every maximum-size object set.

FIFO, no TTL. Count uncached fallback on capacity/version/size/load/participant
limits. Zero capacity takes the direct disabled path. Existing operation
bounds apply to fallback; no uncached aggregate concurrency or RSS bound.

Charge retained allocation capacities, owned encoded bytes, decoded row storage,
keys, tables, queues, shared futures and participants. Allow 64 bytes per
nonempty allocation, minimum 4 KiB per record, 512 bytes per participant and
8 KiB administration per active handle, inside configured pools. Reserve before
cache-owned work. Metadata reservation: 16 times encoded bound plus bookkeeping.
Decoded reservation: encoded ownership, twice encoded size for payloads,
authenticated row storage, allocation allowances and bookkeeping. Check the
authenticated row count before Arrow materialization. Reservation limits differ
from residency limits. Copy backend slices before retention; Arrow is decoder
scratch. Charge shared ownership once until final lease drops, even after
eviction or declined residency. Measure request copies, backend buffers,
scratch and errors separately. Underestimation fails qualification.

## Authentication and concurrency

Every substitution requires an independent uncached HEAD on the exact object,
matching nonempty version and size. Never cache/coalesce HEAD. Admission is
HEAD-before, fetch/range, full existing authentication/validation, HEAD-after;
observations must agree. Missing/error/unusable/changed version falls back to
Gate 5. Successful load with ineligible final HEAD returns uncached. Reclaimed
objects cannot be revived; expired objects that physically exist retain Gate 5
raw-reader behavior. Protection/expiry/release/authorization remain authoritative.

Authenticate the owner before lookup. Validate lengths, checksums, formats,
scope/identity, Arrow structure, block metadata and KV/outbox/trim rules before
admission. One private atomic operation acquires a complete certificate and all
certified blocks. Selective hits cannot mint complete proof. Certificates require
full raw hashing, all block validation and rebuilt directory/Bloom equality and
bind segment and directory versions. Missing certificate/block uses the complete
Gate 5 loader. Always rerun hydration, ordering, uniqueness, replay, history/state
checksums, anchor and maintenance equivalence. Preserve fresh commit manifests,
fences, deadlines/earlier-submission evidence, exact reconciliation and HEAD CAS.

Preserve authority 7, restore 6, segment/directory 1, continuation 3, maintenance
schema/policy 1, rewrite-equivalence v2 with v1 readability, all Gate 5 numeric
limits, 24-hour execution and eight-day retention.

Pinned futures Shared with weak in-flight table; strong refs are participant-owned;
no detached tasks. Create reservation, participant ownership, unique generation
and cleanup guard before shared future construction. Prune dead weak entries.
Initiator cancellation preserves work for others; last participant cancellation
drops operation and ownership. Cleanup removes only its own generation. Short
completion lock, never across I/O/decode/caller copying. Failures remove keys,
fan out exact typed variants/fields privately, allow retry. No public Clone on
CatalogError.

## Frozen verification and acceptance

Regressions precede behavior. Extend oracle, backend faults and cost harness.
Cover scope/backend/durable identity; corruption/truncation/unsupported/substitution;
warm reclaimed readers and lazy transactions; expired/released protection;
restore/epochs/GC/layout competition; overlays; ambiguous publication; reopen;
logical/history/outbox/order parity. Ownership schedules cover coalescing, distinct
pressure, initiator/waiter/all cancellation, never-polled drop, cleanup races,
retry, live eviction, oversized/shared buffers, empty entries, disabled and
sustained pressure.

Measure disabled, cold, fully admitted warm, eviction-heavy and concurrent modes.
Existing fixtures: block targets 32/64/128/256 KiB, owners/blocks 1/4/16/64,
suffixes 0/1/8/16/31; maintenance/history/publication/recovery. Pressure pools
1 MiB metadata/4 MiB decoded. Bursts 1/8/32 and >8 distinct eligible loads.

| Scenario | Required result |
| --- | --- |
| All | Exact parity, authority checks and ownership/concurrency bounds |
| Disabled | Baseline physical/decode/hash work; allocations <=1.05x base |
| Cold | Payload attempts/bytes <=base; allocations <=2x; p50 <=1.25x, p99 <=1.5x |
| Fully admitted warm | Zero eligible fetch/decode; point/scan p50 <=0.75x disabled |
| Same-key 32 | One eligible fetch/decode; 32 logical demands |
| Pressure/cancellation | No budget overrun, poisoned key, reservation/flight leak |

The common initial read fixture uses 64 keys of 1 KiB and 40 warm samples per
operation; the original default benchmark retains 200 samples.
Use five predetermined timing repetitions (1,2,3,4,5); retain each, compare median
per-run percentiles. Include added HEAD attempts/latency. Keep zero-work phases
zero without ratios. Separate demand and physical GET/range/HEAD attempts/bytes,
decode/validation/hash/allocations, cache events, participants/coalescing,
resident/reserved/live-evicted charges and high-water marks. Attribute shared
physical work once to captured originating phase regardless of polling caller.
Preserve parent/leaf partitions and explicit zero phases.

Rerun 32x128 maintenance model, 32x64 reclamation, all 85 remote-write schedules,
logical-clock regression, 86 scaling cases, 15 resume and three lifecycle
schedules; default benchmark 256 setup/200 samples. Sequential catalog schedules,
libraries/integrations, Clippy tests/benches denied warnings, default coverage,
Gate 5/6 acceptance, core/API, formatting/diff checks. Reproduce credential-free
current workflow lanes: workspace, API/Flow matrices/runtime, deterministic UAT,
docs/doctests/mdBook, protocol, Python, hygiene, dependency policy/advisories.
Use cargo-deny 0.18.9, Buf 1.70.0 and CI Python/uv settings; hygiene alternate
index contains intended tracked source/docs. Preserve failures and identical
base/candidate policy inputs. Unresolved mandatory failure prevents completion.

## Audit and recovery

After qualification commission fresh-context read-only final audit with contract,
invariants, base/diff/new files, manifests, logs, comparisons, acceptance,
ownership proofs, regressions and failures. Fix P0/P1/P2, rerun affected checks.
Update combined design and dated qualification/progress report separating local,
remote CI, provider and deployment status.

Seal complete source/docs/evidence, binary patch, failures, manifests, contract
and recovery instructions; exclude credentials/Git/build trees. External checksum.
Independently inspect compressed archive paths/sets/bytes/sizes against worktree
and reconstruct patch on exact Gate 5 base. Stop uncommitted and unstaged with
external closeout recording exact base/branch/path/source/bounds/counts/results/
audit/limits/checksum/reconstruction. No commits, pushes, PRs, merges, deployment,
credentials, provider traffic, migration or cutover. Gate 7 real-S3 and seven-day
pilot remain separately authorized.

## Additional measurement protocol frozen before the layout cache matrix

Use the existing 17 layout fixtures: four block targets, four block-count cases,
four owner-count cases, and five suffix lengths. Preserve their original direct
workload assertions. On each fixture, measure five independent repetitions with
8 warm points and 8 warm complete scans per repetition, with independently cold
point and scan handles. Run disabled, default enabled, and 1/4 MiB pressure
capacities. Compare the same fixture and repetition protocol on the Gate 5
production source with separately hashed test-only instrumentation. The ordinary
64-key fixture retains its already frozen 40 warm samples and five repetitions.

Nonzero configurations must fund the fixed administration and retained identity;
otherwise configuration returns a validation error. This avoids allocating an
unfunded handle or silently disabling a requested cache. Once a handle exists,
entry capacity failures remain counted fallbacks, including a one-byte decoded
pool. Either zero pool capacity explicitly disables the handle.

The maintenance timing comparison uses five independent repetitions of the
existing KV, empty, outbox and keyless smoke fixtures (64 KiB target, one owner,
one suffix). The retained-history/publication/recovery comparison uses five
independent repetitions of the existing smoke profile: 48 setup commits and
8 samples. The original default benchmark remains 256 setup commits/200 samples.
Explicit environment selection configures store and durable worker caches for
these test-only comparisons; ordinary legacy cost lanes select disabled caches.
All production constructors retain the default behavior specified above.

Allocation classes record nested synchronous allocator measurements for Arrow
scratch, decoded row ownership, JSON decoding, request copies, cache copies and
private error fan-out. The backend harness separately measures GET/range allocator
requests. These disjoint classes are subsets of the cumulative poll allocation
count, not RSS or the retained cache charge. Untagged orchestration allocations
remain in the cumulative total. Cache ledger peaks include the fixed 8 KiB
administration allowance plus retained identity allocations. Nonresident ownership
includes both evicted leases and completed values awaiting or declined admission.

HEAD reports include attempts, missing/error outcomes and returned native metadata
bytes: `size_of::<ObjectMeta>()` plus owned path/version/ETag string capacities.
These are backend-API metadata bytes, not HTTP header or wire-byte estimates.
HEAD allocator requests and latency remain included in each operation comparison.

### Identical fixture inputs

Exact disabled-work comparisons use an opt-in, thread-bound `test-utils` clock
and sequential identifiers on both the Gate 5 harness overlay and candidate.
Without a fixture guard, wall time and identifier entropy retain their original
behavior. This also covers the frozen eager fixture's output identifier. The
algorithm is unchanged. The guard cannot move between executor threads.

This correction follows preserved comparisons 02 and 03: random characters in
the publication pointer's decimal-byte JSON changed serialized lengths and thus
hash/read/write byte counts. The acceptance thresholds remain unchanged. Real
clock, expiry, recovery, and concurrent tests run without this guard. Every
prior report and failed comparison remains in the evidence directory.

### Cold suffix allocation correction

Layout comparison 02 preserves 15 failed allocation checks: five repetitions
at each suffix count 8, 16, and 31 exceeded the unchanged 2x cold allocation
limit. A focused default-suite regression reproduces the 31-transaction case
against the same fixture with caches disabled before the correction.

One `Arc<String>` now owns the encoded key across the entry, lookup table,
FIFO and flight. Its buffer capacity and Arc allocation are charged once;
record bookkeeping covers table nodes and pointer copies. Preallocating an
ordinary 1 KiB key buffer avoids repeated serializer growth; larger keys may
grow and remain subject to the same admission accounting. Synchronous admission
returns a small future owning the selected lease or participant. Cancellation
before the returned future is polled therefore exercises real admitted ownership.
The cleanup guard still precedes Shared construction. No lock spans I/O or decode.

The diagnostic probe reduced transaction/directory/block future sizes from
1688/1624/1912 to 352/544/592 bytes and ordinary key serialization allocations
from 1920 to 1064 bytes. Those measurements are explanatory; the frozen workload
acceptance calculations, ownership tests, and final audit remain the gates.

Default construction uses the same nonzero-capacity validation as explicit cache
configuration. It returns a validation error if the retained scope identity
cannot fit default administration; it cannot silently disable caches. The
oversized-scope regression failed before this constructor correction. Normal
read and cache-loading code is unchanged by this correction.

### Object-version capture ownership correction

A backend may return a short version string with excess allocation capacity.
Before cache-owned work can retain it, conversion through an owned boxed string
removes that excess capacity. Reservations also include the normalized version
retained by the loader in addition to the serialized key. Complete-segment
validation reserves both its segment and directory version captures.

Preventing regressions preserve a one-byte version with a 16 MiB backing
allocation and a 400 KiB version whose key and loader capture together cannot
fit a 1 MiB metadata pool. The first failed before normalization; the second
failed before capture reservation. Both use existing direct fallback semantics.
The frozen limits and acceptance thresholds are unchanged. Candidate cache
comparisons are rerun after this correction; the baseline harness is unchanged.

### Cold validation and fixture qualification corrections

Layout comparisons 04 and 05 preserve cold-point latency failures for suffixes
8, 16, and 31. A complete baseline timing refresh did not resolve those failures.
A preserved CPU profile identified reference validation and authentication work.
The candidate keeps every validation and hash operation, uses an ASCII fast path
with the original Unicode fallback for immutable IDs, checks digest bytes without
iterator adapters, and decodes fixed-size canonical digests into stack storage.
Canonical framing starts with an ordinary 256-byte buffer and retains its exact
byte encoding. Equivalence tests cover ASCII, C1 controls, Unicode whitespace,
and every tested character at each digest position. Thresholds are unchanged.

The original lazy-workload comparison also exposed nondeterministic copy inputs:
MemoryBackend enumeration order changed assigned object versions. A preventing
regression fails before sorting those fixture objects. Measured exceptional,
resume, and lifecycle drivers now use the same scoped clock/nonce facility, and
their stores honor the requested measurement mode. The paired baseline overlay
includes only those fixture corrections, not the candidate validation changes.


A later ownership regression uses an authenticated directory with a 256 KiB
segment identifier and pauses the actual block loader. Its serialized key plus
separately retained owner/path allocations exceed a 1,400 KiB decoded pool, but
the original reservation admitted it. Reservations now include a second key
charge as a conservative bound for owner, descriptor and path captures. Final
entry charges still own the shared key once; capacities and acceptance thresholds
are unchanged. The failing command and subsequent qualification are preserved.


The original 86-case scaling comparison subsequently exposed 12 disabled-path
cumulative-allocation failures in planning phases. Eagerly reserving 256 bytes
for every canonical encoder reduced allocation counts but raised bytes for small
reference hashes. That preallocation was removed, restoring Gate 5 buffer growth.
The equivalent ASCII/hex checks and stack digest decoding remain. All original
failures and affected-source reruns are preserved; the 1.05x ceiling is unchanged.

### Paused I/O path ownership correction (2026-09-10 UTC)

A real paused block load with a valid 256 KiB owner ID retained 2,626,589 allocation bytes against 2,313,951 reserved bytes plus 512 participant bytes. The preventing regression measures net live allocations before backend payloads or decoder scratch exist. The common reservation now includes five key charges: the owned serialized key plus four conservative key equivalents for the owner/descriptor and outer, direct-loader and scoped-I/O paths, including formatting capacity growth. Fixed capacities, final-entry limits and quantitative acceptance thresholds are unchanged. The preserved red run is `candidate-live-path-red-02`; the first attempted run failed only the repository qualification lint.

### GC retention interpretation correction (2026-09-10 UTC)

The original lifecycle comparison exposed 32 additional HEADs while interpreting a live maintenance pin for GC. This shared interpreter creates its own store and has no independently configured worker binding. It now explicitly disables that store's caches, covering all three GC/reachability callers. The preventing lifecycle regression failed with 32 HEADs instead of zero before this correction. The four-mode cache matrices preserve 435 explicit zero slots for this phase and perform no work in it; their cache results are unaffected. Lifecycle acceptance and the complete catalog/GC schedules are rerun on the corrected source. No threshold, authority check or retention rule changes.

### Explicit overlapping I/O measurements (2026-09-10 UTC)

The common in-memory timing bursts can finish inline and therefore observe one active participant. A separate measurement now reuses the paused backend to force actual overlapping directory reads, with five repetitions each of 1, 8 and 32 same-key callers, nine distinct objects, and 257 callers spanning eight 32-participant loads plus a ninth-object fallback. It records pre-release and final ownership, real payload/HEAD work, native metadata bytes, cumulative poll allocations and their backend subset, full validation/decode/hash phase counters, and exact raw/decoded parity. This supplements the unchanged timing harness and production implementation. Synthetic cancellation and record-limit tests remain separate proofs.

## Final audit corrections, 2026-09-10

A completed insertion that collides with an existing resident block, or cannot
allocate another FIFO generation, now increments both documented decline and
fallback counters. Its lease remains charged until its final owner drops. Two
preserved red runs cover those branches. The large-scope constructor regression
now requires the exact administration-capacity validation error. Cache capacities,
authentication, allocation policy and acceptance thresholds are unchanged.


## Fresh-audit correction protocol — 2026-09-10

The fresh audit invalidated the earlier cold p99 qualification: singleton cold
phases make p99 identical to p50. Existing reports, failures and archive 02 remain
preserved. Before renewed measurements, freeze 200 independent cold point and
200 independent cold scan observations within each of repetitions 1–5, for both
the common fixture and every existing layout fixture. This sample count follows
the original benchmark convention; it is an addition to the measurement protocol,
not a retroactive claim that the original contract specified a minimum.

Each observation constructs a fresh reader store/cache. Assert zero prior cache
loads/demands before opening the reader and record the number of verified fresh
starts per phase. Warm phases retain 40 common / 8 layout observations against
the final cold scan's cache. Preserve per-run percentiles and compare their medians
using the unchanged cold p50 <=1.25x and p99 <=1.5x base thresholds. All four modes
use the identical test-only workload overlay on exact Gate 5 production source
and the candidate. Include HEAD work and existing allocation/work bounds.

The comparator must reject missing, mismatched or fewer than 200 cold samples,
missing fresh-start evidence and anything other than five repetitions. Preventing
tests must reject the historical singleton reports and catch a synthetic case
where cold p50 passes but cold p99 fails. Expand the existing shared-error test to
all current typed variants with distinct fields, 32 waiters, retry and cleanup.
No production cache behavior change is planned. Broader paused allocator probes
remain optional because the audit found no missing accounting term.

Rerun affected common/layout comparisons, focused cache tests, catalog default
and test-utils coverage, Clippy for tests/benches, formatting and documentation
checks. Record explicit applicability of unaffected historical production lanes.
After independent review, bind the final source/evidence and seal a new recovery
archive; keep the candidate uncommitted and unstaged.
