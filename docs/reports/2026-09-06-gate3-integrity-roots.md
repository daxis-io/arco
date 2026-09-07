# Gate 3 integrity roots and rewrite validation

**Gate 3 is complete locally.** All nine final verification lanes and scaling
bounds pass. Fresh read-only reviewer `/root/gate3_final_review` (Carver) returned
**APPROVED LOCALLY**, with no unresolved correctness or safety blocker in tested
source aggregate `798b4953f5049c43829b17db0ef72fb1f0fbf2e216b758c138c68b12b6aeb0ec`.

The candidate is uncommitted on the state-store vNext work branch, based on
`0235eb6c1552c5c87fe3a7638e10022889462b35`. Gate 2 completed before these edits;
its approved source/evidence checkpoint is recorded in
[Gate 2 closeout](2026-09-06-gate2-closeout.md). The initial dirty baseline and
both original operation-cost reports remain preserved.

## Format and behavior

Authority format 7 and restore-plan format 6 replace Gate 2's outer formats.
Segment/directory format 1, 64 KiB block targeting and continuation version 3
remain unchanged. Older authority formats fail closed; restore plans 1–5 are
supersession-only. There is no migration or dual authority reader.

The [canonical format contract](../plans/state-store-integrity-format-v1.md)
defines distinct scope/version/domain-bound SHA-256 logical-history and
physical-layout roots. Empty commits advance history. Maintenance preserves
history and the full-state checksum while changing physical ownership. HEAD-only
writer/reclamation fencing preserves both roots. Each suffix reference carries
an exact metadata length and a preceding/mutation/result history link; eager
replay recomputes mutation digests from decoded records.

All rendered L1 states and L0 transactions decode through the normal codec
before publication and must match independent expected state/mutations. Reused
inline anchors are verified before promotion and again before a transaction
publishes a materialized base, covering corruption after begin. Maintenance
binds source identity/digest and equivalence evidence. Checkpoints separately
bind their source roots and their actual materialized physical root. Common
validation governs reads, persistence, restore and retained GC closures.

Nonempty restore continues destination history, including a destination behind
the retained source. Empty restore seeds the exact retained source history and
then appends the restore mutation. Generation, tombstone and outbox incarnation
semantics are retained. Public staging continues to reject trimming and adding
the same record ID in one transaction; separate committed incarnations remain
supported.

Authority JSON reads and immutable-publication readbacks now use bounded ranges.
Pointer, manifest, owning-state, transaction and checkpoint IDs are validated as
safe scoped path components before their references are used. Digest validation
remains independent of public token identity equality.

## Acceptance evidence

| Requirement | Evidence |
| --- | --- |
| Canonical scope/history/physical encoding | Independent Python binary inputs and SHA-256 vectors, including bounded nonempty state references and transaction references, checked by Rust tests |
| Equal visible state from different histories | `convergent_state_preserves_distinct_history_and_equivalent_layouts_preserve_history` checks distinct history despite convergent semantic state |
| Equivalent layouts preserve semantics | Different 32/256 KiB block targets and partitions retain history/full-state checksum; maintenance and checkpoints have distinct physical roots |
| Lost, duplicated, altered and reordered records | Exact rendered-state regression tests cover KV rows, outbox order and incarnations; ordinary codec/global ordinal validation remains active |
| Transaction and ancestry tampering | Duplicate/noncanonical IDs, suffix sequence/link validation, coherently replaced data/semantic checksum, historical root replacement and intermediate-parent substitution fail closed |
| Forged equivalence/checkpoint evidence | Source-history/physical-root and checkpoint-layout substitutions are rejected at the applicable local/source/ancestry boundary |
| Projection provenance and resolver limits | Exact observed record payload/scope/ID/origin validation; both count and byte-budget exhaustion and missing links return ambiguity; substitution returns integrity failure |
| Empty/nonempty restore | Independent oracle encodes request identity, notice bytes, writes, generations, tombstones, ordered source/destination incarnations and history for both base kinds |
| Anchor promotion and stale publication | Corruption before and after begin fails without changing HEAD; existing exact-CAS/reclamation schedules remain required |
| Retained snapshots/exports and GC | Independent 32-seed × 64-operation model tracks generations, tombstones, outbox incarnations and history alongside all Gate 0 fault schedules |
| Reader cost preservation | Exact comparison of Gate 2/3 selected data bytes and requested ranges across the same 17 standard scaling fixtures, plus exceptional cases; all comparisons passed |

The logical oracle directly encodes the format and semantic JSON checksum. It
does not call production replay, mutation hashing, partitioning, maintenance or
GC-planning helpers. The restore fixture independently specifies expected
mutations rather than deriving them from the candidate transaction.

## Original workload costs

The required benchmark preserves 256 setup commits, 200 samples and all 17
phases. The new [operation-cost artifact](2026-09-06-gate3-operation-cost.json)
records the complete backend/byte/allocation/latency profile. The original
baseline reports and approved Gate 2 cost report remain unchanged.

| Phase | Samples | Gate 2 read bytes | Gate 3 read bytes | Gate 2 allocated bytes | Gate 3 allocated bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Current point | 3,200 | 87,900,800 | 102,864,000 | 330,068,800 | 483,608,000 |
| Scan | 200 | 26,875,400 | 30,616,200 | 128,610,400 | 167,074,400 |
| Eager begin | 456 | 20,909,340 | 22,914,848 | 116,886,756 | 143,046,968 |
| Commit | 200 | 0 | 61,100 | 26,950,770 | 40,534,086 |
| Maintenance | 29 | 2,658,760 | 2,921,492 | 19,666,457 | 24,992,818 |
| GC | 1 | 40,363,363 | 55,638,498 | 265,100,309 | 422,361,546 |
| Retained validation | 912 | 122,019,244 | 132,509,576 | 582,811,740 | 712,630,272 |

This small-segment workload exposes added metadata and validation costs: point
read bytes increase 17.02%, eager-begin bytes 9.59%, and their cumulative
allocated bytes increase 46.52% and 22.38%, respectively. Commit read bytes
include revalidation of promoted materialized bases before publication. These
are measured costs of the stronger contract, not a read-cost improvement claim.
The scaling lane separately tests selected-block behavior at larger segment sizes.

Canonical-root and rendered-validation counters are enabled by `test-utils` in
the scaling lane. The required original benchmark command omits that feature;
its zero-valued optional hash counters mean uninstrumented, not zero hash work.
Allocation counts are cumulative allocator calls/bytes during future polls,
not retained heap or process RSS. Latencies remain diagnostic.

## Scaling and integrity work

The [scaling artifact](2026-09-06-gate3-block-scaling.json) and
[Gate 2/3 comparison](2026-09-06-gate3-cost-comparison.json) establish exact equality
of selected data bytes and requested data ranges for every operation across all
17 standard samples. Encoded data/index sizes, block counts and page counts also
match. Oversized-row, disabled-filter and projection cases remain separate and
preserve their selected-data behavior. All original executable cost bounds pass.

For the identical 4,096-row data at 64 KiB versus 256 KiB targets:

| Metric | 64 KiB | 256 KiB | Ratio or bound |
| --- | ---: | ---: | ---: |
| Encoded data bytes | 4,677,270 | 4,537,892 | 1.030715, <= 1.15 |
| Complete scan data bytes | 6,937,566 | 13,613,676 | 0.509603, <= 1.15 |
| Point data bytes | 62,786 | 255,874 | 0.245379, <= 0.40 |
| Scan block requests | 111 | 54 | <= 4.5 × 54 + 1 |
| Directory bytes | 37,143 | 17,342 | 2.141795, <= 3 and absolute caps |

The 64 KiB fixture has 75 blocks and 37 pages, giving exactly `B + P − 1 = 111`
scan block reads. A pinned L1 point reads one directory and one block, with zero
full data GETs. Its 251,455 cumulative allocated bytes and authentication work
are unchanged from Gate 2. A Bloom-negative miss reads only the directory; an
outside-bounds miss reads neither directory nor data.

The same fixture isolates added root work:

| Phase | Additional returned metadata bytes | Gate 3 canonical hash calls / bytes | Gate 2 / Gate 3 allocated bytes |
| --- | ---: | ---: | ---: |
| Reader open | 867 | 2 / 882 | 9,418 / 18,002 |
| Pinned point | 0 | 0 / 0 | 251,455 / 251,455 |
| Current point | 867 | 1 / 441 | 259,779 / 266,454 |
| Eager begin | 867 | 1 / 441 | 51,124,476 / 51,131,615 |

Maintenance separately records 68 canonical hashes over 4,443,667 bytes and one
exact rendered-state validation over 4,714,413 bytes. Its complete operation
allocates 324,867,583 cumulative bytes in 135,387 calls; this includes eager
source reconstruction, rendering, canonical encoding and verification, not just
the added check. Total SHA-helper input is 113,431,154 bytes; canonical bytes
are a subset and must not be added again. Every fixture's maintenance costs and
per-phase metadata/hash/allocation deltas are retained in the comparison JSON.

## Final command matrix

All commands ran sequentially with `CARGO_INCREMENTAL=0`,
`CARGO_PROFILE_DEV_DEBUG=0`, and `CARGO_PROFILE_TEST_DEBUG=0`. Exact commands,
terminal statuses, durations and tested-source hashes are in
[Gate 3 verification](2026-09-06-gate3-verification.json). Logs are retained at
`/private/tmp/arco-gate3-verification-20260906-122723`.

| Command | Terminal result |
| --- | --- |
| `cargo test -p arco-catalog --features test-utils --test state_store_reclamation_schedules --locked` | Exit 0; 16 passed, including 32 seeds × 64 operations |
| `cargo test -p arco-catalog --features test-utils --lib --tests --locked` | Exit 0; 848 passed, 2 ignored across 28 suites |
| `cargo clippy -p arco-catalog --features test-utils --lib --tests --benches --locked -- -D warnings` | Exit 0 |
| `cargo test -p arco-catalog --bench control_mvp_gate --locked` | Exit 0; original 256-commit / 200-sample profile |
| `cargo fmt --all -- --check` | Exit 0 |
| `git diff --check` | Exit 0 |
| `cargo test -p arco-catalog --features test-utils --test control_cost_smoke authenticated_block_scaling_acceptance --locked -- --ignored` | Exit 0; scaling test completed in 248.38 seconds |
| `cargo test -p arco-core --locked` | Exit 0; 295 passed, 6 ignored |
| `cargo check -p arco-api --all-targets --locked` | Exit 0 |

The ignored scaling test ran explicitly in its separate lane; the other catalog
ignore is the existing golden-schema generator. These results do not claim that
other intentionally ignored tests ran. The 24 changed source/compile-time input
hashes were rechecked against the final worktree after the matrix; aggregate
`798b4953f5049c43829b17db0ef72fb1f0fbf2e216b758c138c68b12b6aeb0ec`.

## Audit corrections

The fresh Gate 3 review demonstrated and prompted fixes for:

- A nonempty destination behind its source incorrectly rejected restore.
- Duplicate/noncanonical transaction IDs and malformed owning metadata passed
  local validation; percent-containing IDs were incompatible with scoped paths.
- A corrupt redundant inline anchor could be promoted after a readable parent,
  and maintenance could replace a layout without validating that redundant
  owning source. Both paths now use complete source-anchor validation.
- The oracle applied outbox additions before exact trims, and lacked independent
  empty/nonempty restore coverage and nonempty physical-layout vectors.

Each behavior has a focused regression. Bounded read conversion also required
fault adapters to enforce injected read failures for ranges; failing logs are
preserved and affected schedules rerun. Malformed-Arrow fixtures independently
reseal physical roots and exact transaction lengths, so failures reach their
intended decoder boundary; legacy restore fixtures omit the new transaction
reference when reproducing the old field set. No injected failure was weakened
to make the tests pass.

## Qualification limits

Reader opening authenticates its root and local metadata; selective operations
verify only selected directories/blocks. Full-state operations retain whole-state
verification. Ordinary witnessed history reads do not recursively load parents.
The 4,096-manifest / 64 MiB resolver limit can make sufficiently long or pruned
ancestry unavailable for projection fallback/reconciliation; parent links do not
extend retention. Missing proof never becomes a successful acknowledgment or a
false supersession.

MemoryBackend measurements establish storage API calls, returned bytes and local
allocation/hash work, not provider traffic, billing or latency qualification.
Transactions remain eager. Gates 4–7 (lazy transactions, durable incremental
maintenance, caches, provider qualification) remain outstanding. No commit, push,
deployment, cutover or credentialed provider operation is part of this work.

## Final audit and recoverable handoff

The reviewer independently checked the authentication chain, both integrity
roots, canonical vectors, all whole-state boundaries, ancestry limits/error
classes, projection provenance, retention/GC, empty/nonempty restore, exact
command results and cost artifacts. All demonstrated findings are resolved.
The maintenance red regression is retained at
`/private/tmp/arco-gate3-verification-20260906-122408/01.stdout`; its green
regression and final complete matrix are retained separately.

The final dirty source, format documents, reports, tested-input hashes, binary
tracked patch, status and terminal logs are archived at
`/private/tmp/arco-gate3-checkpoint-20260906`. `RECOVER.md`, `file-hashes.json`,
`aggregate-content-sha256.txt` and `archive-sha256.json` describe recovery and
integrity checks. Credentials, Git metadata and generated build trees are
excluded. Archive members are read back and checked against the worktree.
The prior Gate 2 checkpoint remains intact at
`/private/tmp/arco-gate2-checkpoint-20260906`.

This completes Gate 3 independently of the approved Gate 2 checkpoint. Stop here
with Gates 4–7 outstanding. The worktree remains uncommitted at the original base
HEAD; no push, deployment, cutover or credentialed provider operation occurred.
