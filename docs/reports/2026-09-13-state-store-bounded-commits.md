# Authority 8 bounded catalog commits — Step 2

The complete Step 2 local scaling matrix passed on commit
`24d8d80a236417c9f917b7dca23ce096eaa1952c`. The measurements and original verification
results below belong to that exact source. This branch now also integrates main at
`fa87b28f5a53da59e8c4d8e8eb4957dd4ba0ab86`; its separate integration checks do not
transfer those quantitative performance results to the combined source. Durable
large restore and provider qualification remain outside Step 2.

## Source and authority boundary

The implementation starts exactly at `e29a5f6a3f99c4050a78a794a85c7e125a421a12`
on branch `feat/state-store-bounded-commits-20260912-01`. That is PR #429's pinned
head. At the measured Step 2 commit, subsequent integration changes and PR #430's
runtime-cache commit `0da54a1ca5101e5d523b18f7aa116eb4ace059b8` were not incorporated.
The subsequent merge of main incorporates both PRs while preserving the measured
commit and its evidence archive. Cargo dependencies remain unchanged and locked.

[The frozen contract](../plans/2026-09-12-state-bounded-commits.md), amendment 22,
has SHA-256 `84117e3d9fb3099bdf7f3f2bfcd5981fc578b34bd5cbdbb3f5c9509e8baf3cce`.
Contract versions, commands, source manifests, reconstructable deltas, tool digests,
raw measurements and failures are retained in
`/private/tmp/arco-state-bounded-commits-20260912-01`. The final matrix source
fingerprint is `2eff68ed8da2a62d73aecca31780fe174b35fcaf54259c9722c95ae1123c78f5`;
source remained unchanged throughout execution. The archive gate checks every
final Rust/configuration/dependency source against that measured source, allowing
only subsequent documentation changes.

Production constructors select authority 7. Explicit synthetic/test constructors
select authority 8 at the same scoped HEAD path. HEAD pinning authenticates stable
metadata, bounded pointer bytes and the named manifest. Role-bound roots authenticate
physical descriptors, index coverage, immutable object versions and selected blocks.
Persistent path-copy updates and an independent transition verifier replace ordinary
full materialization. V2 receipts, audits and projection intents carry logical
identity; transition certificates authenticate physical provenance. Three roots bind
KV, active outbox IDs and delivery order in one conditional publication.

Format 7 and restore-plan 6 remain supported. Authority 8 checkpoint, restore,
conversion, maintenance, reclamation and old projection-worker operations fail
closed. Synthetic catalog construction requires a V2 notifier. No restore-plan 7
or production cutover is introduced.

## Executed work and bounded cost

Six independent fixture axes cover retained receipt/audit inventories of 4,096,
65,536, 1,048,576 and 2,419,200 rows with 128 active outbox records, plus 4,096 retained
rows with 4,096 and 65,536 active outbox records. Fixtures use explicit streaming
synthetic genesis, production-shaped V2 records and verified paired outbox indexes.
This constructs retained inventory; it does not execute its historical mutations.

Each axis runs ten scenarios in disabled/default/pressure cache modes, with five
repetitions of 200 requests: **180,000 requests in 900 complete groups**. Independent
validation reports zero failures and exact operation/phase identity coverage.
Measured categories are:

- 108,000 successful actual catalog commands across create catalog, create schema,
  register table, patch catalog, rename table and drop table.
- 18,000 exact catalog replays.
- 18,000 stale-generation scenarios, each containing a winner commit and a failed
  loser transaction.
- 18,000 ordered-outbox page reads and 18,000 exact-incarnation trims.

Setup separately executes 18,000 replay seed commits and 1,170 bootstrap catalog
commits, in addition to six synthetic genesis fixtures. Each measured request resets
a private HEAD to the same authenticated fixture/bootstrap predecessor. These are
repeated narrow operations over controlled retained inventories, not one continuous
180,000-commit history or 1,209,600 executed pilot mutations.

The following maxima include predicates, descriptors, external fence probes, proof
validation, failed operations and retries. Columns aggregate different requests;
they do not describe one request with all maxima simultaneously.

| Retained rows | Active outbox | Selected blocks | Rewritten blocks | Block decodes | Decoded rows | Directory references | Range GETs | Metadata HEADs | PUT attempts | CAS attempts | Request allocation upper bound, bytes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4,096 | 128 | 7 | 7 | 37 | 4,201 | 1,680 | 855 | 257 | 33 | 2 | 15,908,990 |
| 65,536 | 128 | 7 | 7 | 37 | 4,201 | 7,623 | 5,480 | 257 | 37 | 2 | 30,921,772 |
| 1,048,576 | 128 | 7 | 7 | 37 | 4,209 | 10,323 | 6,560 | 257 | 37 | 2 | 37,301,322 |
| 2,419,200 | 128 | 7 | 7 | 37 | 4,209 | 12,637 | 7,927 | 257 | 40 | 2 | 41,896,604 |
| 4,096 | 4,096 | 6 | 6 | 35 | 4,580 | 2,218 | 916 | 243 | 33 | 2 | 16,735,847 |
| 4,096 | 65,536 | 6 | 6 | 35 | 4,580 | 3,749 | 1,373 | 243 | 34 | 2 | 19,772,099 |

| Retained rows | Active outbox | Read bytes | Written bytes | SHA helper bytes |
| ---: | ---: | ---: | ---: | ---: |
| 4,096 | 128 | 2,491,008 | 321,918 | 5,268,385 |
| 65,536 | 128 | 4,045,065 | 409,988 | 7,558,949 |
| 1,048,576 | 128 | 4,760,250 | 425,683 | 8,680,280 |
| 2,419,200 | 128 | 5,431,204 | 436,569 | 9,674,270 |
| 4,096 | 4,096 | 2,633,964 | 354,952 | 5,698,056 |
| 4,096 | 65,536 | 2,945,369 | 407,967 | 6,445,746 |

All requests satisfy the declared T=16 block bound, at most 4T block decodes and
2T rewritten blocks. Directory-page reads and writes each satisfy `4D(T+1)` using
actual depth D. Every ordinary request reports zero full replay, full-state checksum
and streaming-builder inputs. Maximum conservative allocation is **41,896,604 bytes**
against **67,108,864 bytes (64 MiB)**. Cumulative allocator bytes conservatively bound
request peak ownership; the net poll watermark is diagnostic only. Cache pools are
accounted separately: default metadata/decoded pools are 32/128 MiB, pressure pools
1/4 MiB, and disabled mode retains neither. Store and catalog ownership statistics
are distinct; accounting-underestimate checks pass.

The matrix ran in 4,420.88 seconds on an Apple M4 Max, 16 CPUs, 64 GiB RAM,
macOS 26.6 build 25G72. Rust 1.88 used locked offline dependencies, a separately
owned sequential Cargo queue/target, incremental compilation disabled, dev/test
debug information disabled, test optimization level 3, and assertions/overflow
checks enabled. Representative disabled-cache rename latency follows; medians are
the median of five repetition medians and p95 is the largest repetition p95.
All 900 repetition summaries, other scenarios and cache modes remain in raw evidence.

| Retained rows | Active outbox | Median, ms | Worst repetition p95, ms |
| ---: | ---: | ---: | ---: |
| 4,096 | 128 | 15.430 | 15.782 |
| 65,536 | 128 | 34.5955 | 36.228 |
| 1,048,576 | 128 | 40.756 | 41.809 |
| 2,419,200 | 128 | 47.7215 | 50.186 |
| 4,096 | 4,096 | 17.850 | 19.505 |
| 4,096 | 65,536 | 23.2715 | 25.710 |

These measurements establish bounded local work under the declared shapes, not
constant latency or a provider SLO. The testing-only memory backend retains physical
objects within each axis. Its inventory reaches 598,333–763,898 objects and
6,925,900,064–10,238,422,032 object bytes. Each axis has 30,196 prepared candidates:
2 attached to the restored current ancestry and 30,194 unattached. Envelope/manifest
class counts and bytes are not transitive unreachable-byte totals or GC eligibility.
Exact per-axis inventories and current roots appear in `matrix-v3-report-data.json`
and the authenticated-inventory rows in the raw stream.

## Behavioral verification and retained failures

The independent small-state catalog model checks objects, names/IDs, generations,
tombstones, receipts, audit, sequence and ordered outbox semantics. A separate Python
codec validates V2 logical IDs/history from exact published KV bytes and exported
provenance. Partitioned equivalent predecessors have an independent history-equality
regression. Authentication, conflict, retained-read, paired-index corruption, cache,
publication faults and bounded recovery regressions are compiled behavioral tests.

Final verification on identical Rust source passed:

| Lane | Result |
| --- | --- |
| Core and feature-enabled catalog packages | 1,377 passed, 0 failed, 31 ignored across 51 suites |
| Default/format-7 catalog package | 970 passed, 0 failed, 10 ignored across 35 suites |
| Full matrix and independent analyzer | 180,000 requests, 900 groups, zero failures |
| Published catalog parity and independent logical codec | All six command families passed |
| Oversized singleton lane and independent analyzer | All nine payload/cache cases passed |
| Strict Clippy, formatting, documentation, repository hygiene | Passed |

Ignored tests are recorded explicitly; these totals do not claim every ignored
lane ran. The selected matrix and singleton tests were executed separately. Command
receipts bind exact commands, exits, profiles, source/tool/log digests and test names
in `verification-inventory-final.json`. Prior failures and interrupted runs remain:

- The original singleton bound `16 MiB + 12P` failed at 8 MiB with 176,616,192
  allocated bytes against 117,440,512 allowed. Amendment 16 froze `16 MiB + 24P`
  after inspecting Arrow and independent proof copies. Final maxima for 300 KiB,
  8 MiB and 60 MiB are 5,645,128; 176,627,392; and 1,132,912,623 bytes. All nine
  cases meet the amended scratch ceiling and the existing 64 MiB encoded segment
  ceiling. The earlier bound remains failed evidence.
- A largest-axis preflight exceeded 64 MiB through repeated external fence-key
  reads. A compiled regression preceded bounded per-read-budget authenticated
  fence reuse: at most 1,024 entries/128 KiB payload, with fresh bounded reads on
  overflow. Independent verification owns its own budget. No shared runtime cache
  from PR #430 was imported.
- Full matrix v1 failed at request 22,576 when the testing-only memory backend's
  global HashMap resized: 98,513,779 allocated bytes. A reduced compiled regression
  reproduced one tiny PUT allocating 1,327,243 bytes. The backend now uses the
  standard-library BTreeMap; API, versions and conditional-write locking remain.
  All backend allocations remain counted, and core/catalog regressions were rerun.
- Full matrix v2 was intentionally interrupted after 37,319 complete requests to
  preserve disk headroom. It has no observed runtime assertion failure but remains
  incomplete. Amendment 22 changed only evidence compression to bounded-window
  zstd; the same-byte round-trip probe and transport failure canaries pass.
- Earlier compilation failures and deliberately interrupted unoptimized model
  runs are retained separately; they are not behavioral-red or passing-matrix proof.

Final raw evidence contains 542,712 JSONL records and 51,314,103,927 bytes, SHA-256
`5e00c634b00f03fb0a451d203c0b086243bc6d3cad8abd17aff036321c15f8a1`.
`bounded-full-matrix-v3.jsonl.zst` contains 333,468,415 bytes, SHA-256
`c85991eeccb23b9b9e67e21965d1280c5b84ba6b4f04c3694c93a7cf1f3c8808`.
The transport verifies exact round-trip bytes with Python 3.14.5/libzstd 1.5.7,
level 3 and window log 25. This evidence encoding is not a storage wire-format change.

## Step 3 handoff: durable restore preparation

A durable restore must prepare bounded immutable units carrying an immutable unit
ID, retained source witness, exact input/output roots, key interval including gaps,
artifact digests, coverage proof, owner/attempt generation, deadline and fence tuple.
Partial units cannot mint readable authority. The ordinary mutation verifier's
16 selected/32 rendered block limits are not a large-restore plan.

Final merge must prove complete coverage without overlaps or omissions, preserve
tombstone generations and unchanged provenance, and establish the three-root outbox
bijection before one candidate HEAD CAS. Use logical codec V2 for identity and
transition certificates for physical provenance. Projection-source descriptors
bind candidate KV root bytes/digest before outbox entries and the manifest are
rendered, avoiding a manifest self-hash cycle.

Prepared records bind scope, candidate ID/kind, exact original HEAD bytes/digest,
version, manifest witness, writer epoch and reclamation generation (or the explicitly
absent form), exact candidate HEAD/manifest bytes/digests, and transaction plus
three-role transitions or an exclusive genesis witness. Validate all cross-bindings,
roles, parents, scopes, digests and exclusions before publication or recovery.
Descriptor existence does not establish commitment.

A directly observed conditional HEAD precondition failure returns
`CatalogError::CasFailed`; this is the trusted conflict allowing frozen-command
reexecution. Candidate-ID restart reconciliation never resubmits CAS and returns
only `Committed`, `Superseded` or `Unresolved`. It cannot reconstruct a lost provider
response. An originally absent HEAD is not proof of failed publication: if current
HEAD is not the exact candidate, only complete authenticated ancestry to the initial
root proves supersession; otherwise the outcome remains unresolved.

Current recovery admits at most 32 ancestry manifests and one 64 MiB encoded-evidence
budget beginning before the prepared-descriptor read, reserving each maximum bounded
GET probe first. Missing evidence, exhausted budgets and cancellation remain
unresolved. Step 3 must add durable deadlines and ownership semantics. Timeout,
cancellation or expiry cannot revoke a possibly sent CAS, authorize retry or permit
deletion. Perform final read-only reconciliation and recheck current HEAD, writer
and reclamation fences; unresolved candidates remain retained.

## Step 4 handoff: conversion, maintenance, retention and GC

Trace the following graph with exact scope, digest and immutable-version checks:

| Object | Required reachability edges |
| --- | --- |
| Scoped HEAD and retained private token | Exact manifest digest, scope, sequence and fences |
| Authority-8 manifest | Exact parent ID/digest, kind/fences, all three roots, transaction/transitions, projection source or genesis witness |
| Directory-v1 root/page | Every child page, leaf descriptor and non-inline external `/keys/` fence object, including digest/length/bytes and reused subtrees |
| Physical descriptor | Role, segment/index IDs, versions, lengths, digests and checked block locator |
| Segment index | Complete authenticated block coverage and segment identity |
| Transition certificate | Old/new roots, every authorized replacement and unchanged references; old evidence needed for recovery |
| Projection-source descriptor | Scope, logical sequence/ID and exact candidate KV root bytes/digest |
| Active-ID and delivery-order entries | Exact incarnation, paired entry and physical source descriptor |
| Prepared candidate | Original HEAD/manifest witness and exact candidate HEAD, manifest, transaction/proofs or genesis witness |
| Synthetic genesis witness | Authenticated roots, row/block counts and synthetic history construction identity |

Preparations within their deadlines and unresolved preparations are GC roots.
Authenticate full reachability from current and retained authority and candidate
identifiers, including parents, all three directories, external fences, descriptors,
indexes/data, provenance and paired outbox. Pin HEAD version and writer/reclamation
fences, then revalidate before deletion. Expiry alone is never deletion eligibility:
Step 4 needs a durable eligibility proof, no remaining references and fenced
nonpublication. Inventory classification in this report does not supply that proof.

Exact-incarnation trims allow later restaging with a new incarnation; retained
continuations stay pinned. Conversion must verify the initial outbox bijection.
Occupancy rebalancing and repacking remain deferred. Durable large restore, full
lifecycle integration, complete pilot execution, provider qualification, deployment
and production cutover remain later work.

## Current-main integration

The integration combines Step 2 commit `24d8d80a236417c9f917b7dca23ce096eaa1952c`
with main `fa87b28f5a53da59e8c4d8e8eb4957dd4ba0ab86`. Runtime-cache construction and
synthetic authority-8 catalog construction coexist. A new regression exercises the
real legacy projection worker against authority 8 in disabled, default and pressure
cache modes: it rejects the format before invoking the handler or acknowledging work.

Local integration evidence includes 1,404 passing core/feature catalog tests, the
additional projection-worker regression, 976 passing default catalog tests, five
API protocol tests, independent logical validation of six published catalog tuples,
and all nine oversized-singleton cases. The feature suite precedes the final
test-only regression; that regression recompiles and passes separately. Final
static checks, all-axes preflight, source manifests, audit and archive receipts are
recorded under `/private/tmp/arco-state-integration-restore-20260913-01`.

The independent integration audit requires a 180-request preflight across every
fixture/cache/scenario combination. It does not require repeating the historical
180,000-request matrix for integration correctness because the bounded primitives
and matrix scenarios are unchanged. Any quantitative performance acceptance claim
for this combined source requires a new complete matrix. Compiler diagnostics, a
missing offline dependency-cache entry and the initial all-target benchmark lint
failure remain retained. The benchmark fix is confined to allowing intentionally
unused shared test support at the harness-free benchmark inclusion.

Production remains authority 7, restore-plan 6 remains unchanged, and authority-8
lifecycle operations remain unsupported here. Step 3 is developed separately from
this integration; neither this report nor the integration establishes durable large
restore, continuous pilot execution, provider qualification or production cutover.
