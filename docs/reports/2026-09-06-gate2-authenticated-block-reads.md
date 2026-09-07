# Gate 2: authenticated block reads

Status: implementation and command/cost verification complete; final audit verdict
and recoverable checkpoint pending. Gate 3 has not started.

Base commit: `0235eb6c1552c5c87fe3a7638e10022889462b35` on the state-store vNext work
branch. All changes remain uncommitted. The 24-file starting
candidate and both original operation-cost reports are preserved in
`/private/tmp/arco-gates-2-3-baseline-wonihzqn/source-evidence.tar.gz`, together
with binary tracked patches, status, and file hashes. The starting aggregate
content hash is `990b423f5052f280cc0ea824d1b135ce7ca5a4b9df6a298cde55f5da16c74050`.

## Contract

The implementation writes authority 6, restore plans 5, segment directories 1,
and scan continuations 3. See
[`state-store-block-format-v1.md`](../plans/state-store-block-format-v1.md)
for encoding, limits, authentication, Bloom hashing, and validation boundaries.

Opaque roots carry private raw digests. Historical direct reads authenticate the
exact selected root. Projection and transaction-membership fallback use bounded
authenticated parents; incomplete reconciliation remains ambiguous. Parent links
do not extend retention. Current HEAD sequence and fence counters must be
consistent with its authenticated manifest.

Segments contain independent complete IPC files. Directories and selected blocks
are authenticated before pruning/decoding. Point reads and scans use bounded
ranges; eager transactions still reconstruct and check the entire state. Every
full segment read uses a declared-length-plus-one probe. Existing publication,
reclamation, writer fencing, backpressure, retention and uncertainty handling
remain exercised by the command matrix.

## Independent audit and regressions

Fresh read-only review: `/root/gate2_final_review` (Plato). The reviewer confirmed
and rechecked fixes for checkpoint closure validation, keyless outbox shards,
HEAD sequence and counter binding, IPC compression rejection, bounded eager data
reads, and L1 KV incarnation/ordinal validation. Final verdict is pending costs
and the complete verification matrix.

Regression evidence is retained under the baseline task directory. The first
compression test attempt lacked writer compression support; a dev-only Arrow
compression feature enabled a genuine red run before restoring the preflight
guard. The first fence regression had invalid unclaimed-epoch setup; the corrected
fixture demonstrably accepts the rollback without the guard and rejects it with
the guard. Setup failures are not counted as demonstrated regressions.

## Acceptance and evidence

The scaling lane uses deterministic 32-byte keys and 1 KiB values. It separates
opening, pinned point/scan, current-root point, eager begin, and projection source
resolution. Test-only L1 partition sizing preserves production reader caps and
L0 limits. Object-class read counts, requested ranges, returned bytes, failures,
cumulative allocator calls/bytes, and SHA-256 helper work are recorded. Bloom
probe hashing and spawned allocator work are explicitly excluded from those
instrumentation counters.

The executable assertions cover one selected directory/block, zero full data
GETs, Bloom negatives, manifest-bound misses, disjoint segment scaling, L0 suffix
bounds, scan block rereads, four writer targets, allocation bounds, and separately
reported oversized rows/disabled filters. The fixed 100,000-absent-key Bloom probe
test is part of the library suite.
An independent Python implementation of the documented hash/bit encoding gives
zero false negatives and 848 false positives among 100,000 absent probes
(0.848%); its filter digest and inputs are recorded in
`2026-09-06-gate2-bloom-vectors.json`.

The unchanged original workload is recorded separately in
`2026-09-06-gate2-operation-cost.json`; both earlier reports remain byte-for-byte
preserved. Its small-segment point workload pays the additional authenticated
directory metadata, so it is not a universal read-cost improvement:

| Original workload phase | Samples | Returned bytes vs baseline | Allocated bytes vs baseline |
| --- | ---: | ---: | ---: |
| Current point | 3,200 | 1.142875x | 1.005704x |
| Scan | 200 | 0.741746x | 0.579822x |
| Eager begin | 456 | 1.075295x | 0.987190x |

The separate scaling lane measures the intended large-segment selective-read
benefit, without changing the original workload to conceal metadata overhead.

The 4,096-row writer-target comparison passed the following executable bounds:

| Metric, 64 KiB / 256 KiB | Observed | Required |
| --- | ---: | ---: |
| Encoded data bytes | 1.030715x | <= 1.15x |
| Complete paginated scan data bytes | 0.509603x | <= 1.15x |
| Pinned point data bytes | 0.245379x | <= 0.40x |
| Complete scan block requests | 111 / 54 | <= 4.5x + 1 |
| Index bytes | 37,143 / 17,342 (2.141795x) | <= 3x; <= 512 KiB |

All targets scanned 4,096 rows in 37 pages. The 64 KiB fixture contains 75
blocks, so its 111 range reads meet `B + P - 1` exactly. Its point hit reads one
37,143-byte directory and one 62,786-byte block, with no full data GET. The
256 KiB point hit reads one 255,874-byte block. Oversized and disabled-filter
cases are separate: the 300 KiB row is isolated in a two-block segment; the
104,859-key tombstone transaction uses 164 blocks and a 58,507-byte directory
with its filter explicitly disabled. Projection resolution reads three manifests
and no data blocks; its outbox opening cost is measured outside resolution.

The final block-count sweep uses 55/220/880/3,520 rows and produces exactly
1/4/16/64 blocks. All 17 standard samples and three exceptional cases passed.

## Final command matrix

All commands ran sequentially with `CARGO_INCREMENTAL=0`,
`CARGO_PROFILE_DEV_DEBUG=0`, and `CARGO_PROFILE_TEST_DEBUG=0`. The ordered
commands, exit codes and timings are in `2026-09-06-gate2-verification.json`;
stdout/stderr are retained in
`/private/tmp/arco-gate2-verification-20260906-112425`.

| Command | Terminal result |
| --- | --- |
| `cargo test -p arco-catalog --features test-utils --test state_store_reclamation_schedules --locked` | Exit 0; 14 passed, including 32 seeds of 64 operations |
| `cargo test -p arco-catalog --features test-utils --lib --tests --locked` | Exit 0; 832 passed, 2 ignored across 28 suites |
| `cargo clippy -p arco-catalog --features test-utils --lib --tests --benches --locked -- -D warnings` | Exit 0 |
| `cargo test -p arco-catalog --bench control_mvp_gate --locked` | Exit 0; original benchmark assertions passed |
| `cargo fmt --all -- --check` | Exit 0 |
| `git diff --check` | Exit 0 |
| `cargo test -p arco-catalog --features test-utils --test control_cost_smoke authenticated_block_scaling_acceptance --locked -- --ignored` | Exit 0; explicit scaling acceptance passed in 210.37 seconds |
| `cargo test -p arco-core --locked` | Exit 0; 295 passed, 6 ignored |
| `cargo check -p arco-api --all-targets --locked` | Exit 0 |

The explicitly ignored scaling test was executed in its separate lane. These
results do not claim that other deliberately ignored tests ran.

MemoryBackend evidence establishes storage API behavior only. It does not prove
provider traffic, billing, deployment, cutover, or production qualification.
