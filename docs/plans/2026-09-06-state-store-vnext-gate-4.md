# Gate 4: lazy control/v1 transactions

## Outcome and authority

Implement metadata-only transaction begin, authenticated selective reads, and a
request-local overlay. Commit retains full reconstruction and every Gate 3
checksum, history, layout, and materialized-rewrite validation guarantee. Gate 4
improves beginning, inspecting, and abandoning transactions; reduced total
commit work is not an acceptance claim.

The execution base is `ca5023caa3ec1eb5b39edeef6291082f544a5153` on the
state-store vNext work branch in `/Users/ethanurbanski/arco/.worktrees/state-store-vnext`.
Recheck provenance, status, processes, disk, and Gate 3's 24 tested inputs before
editing. Use one source-editing owner and sequential Cargo ownership. Preserve
all worktrees, reports, and checkpoints. Authority 7, restore plan 6,
segment/directory 1, and continuation wire version 3 remain unchanged.

This plan is approved for local implementation and verification without another
routine approval checkpoint. Stop after locally completing Gate 4. Gates 5–7
remain outstanding. No commit, push, merge, deployment, cutover, or credentialed
provider operation is authorized.

## Implementation

### 1. Separate the transaction pin from eager reconstruction

Introduce a private transaction base with explicit genesis and authenticated
manifest variants. It holds the selected manifest, raw digest, logical sequence,
history/layout roots, captured HEAD version, writer epoch, reclamation
generation, and layout generation.

`begin_control_txn` validates options, scope, authority metadata, fencing, and
sequence arithmetic, then derives candidate identities using the existing
operation/HEAD/reclamation binding. It fetches no transaction objects,
directories, or data blocks. Keep `ControlMvpBase` as the fully materialized
representation used by restore and reconciliation. Extract metadata pinning and
materialization into explicit helpers; never substitute a partially populated
`ReplayState` for a complete one.

### 2. Authenticated point reads and witnesses

Factor an internal versioned point reader from the authenticated reader,
preserving never-present, present with generation, and tombstone with generation
observations. Strip tombstones only for the public value result.

`ControlMvpTxn::get` checks staged writes first. Staged values retain generation
`None`; staged deletes return absence. Base-dependent reads register positive or
negative observations and may memoize authenticated results. Overlay-only reads
record their local origin without inventing base absence. Explicit assertions
inspect the pinned base even after staged writes/deletes. Preserve range and
predicate witness encoding and `PredicateInputSet` behavior. At commit, validate
observations and assertions against freshly reconstructed pinned state before
applying mutations.

### 3. Merge scans and stream range evidence

Extend the authenticated ordered merge to include the overlay before pagination.
Preserve binary ordering, empty keys/values, generations, tombstones, and the
last fully resolved key. Use an internal resolved-row stream for range/predicate
evidence that includes tombstones. A retained tombstone makes
`assert_range_empty` fail; empty/reversed half-open ranges remain empty.

Stream wide ranges in bounded chunks without accumulating all rows or imposing
a total-range read limit. Successful evidence covers the complete declared
range. Memoize completed fingerprints where useful; partial traversal is never
proof. Automatic scan observations cover the fully resolved interval, including
gaps/tombstones; a terminal page records exhaustion of the remaining prefix.

Every transaction scan has a private local continuation origin bound to a fresh
in-memory nonce, optional authenticated base token, scope, prefix, and exclusive
boundary. This supports genesis without fabricating a `StateToken`. Cursors
work only in their originating transaction, are rejected by public readers and
opaque encoding, and preserve the v3 wire. Dynamic keysets do not revisit keys
at/below the boundary; current staged inserts/updates/deletes above it affect
later pages. Empty pages may advance over tombstones or staged deletes. Update
`ScanPage::observed_token` documentation to permit staged genesis pages.

### 4. Asynchronous outbox helpers

Make `stage_projection_outbox`, `stage_projection_intent`, and
`trim_projection_outbox` asynchronous; make crate-local `range_witness`
asynchronous and fallible. Update catalog execution, path governance, projection
acknowledgements, tests, benchmarks, and dependent API callers. Keep
`ArcoStateTxn` signatures unchanged.

Authenticated outbox-ID lookup inspects all owning L1 directories because their
bounds/Bloom filters describe KV keys only. Fetch matching outbox blocks,
including keyless shards. Process relevant L0 trims/additions in logical order,
exact-incarnation trim first, then addition. Apply normal selected-row validation.
Preserve duplicate-ID rejection, no trim-and-add of the same ID in one
transaction, exact trim incarnations, durable ordering, and provenance.

Validate a multi-target trim into temporary state before appending any staged
trims. Failure/cancellation leaves no partially staged batch. Commit independently
rechecks duplicate/incarnation rules against complete replay.

### 5. Complete publication boundary

At commit, freshly reconstruct the pinned manifest with `replay_for_successor`
before publishing any artifact, bypassing selective memoization. Preserve
redundant-anchor equivalence and promoted-base revalidation, then:

1. Validate observations, explicit preconditions, and writer fencing.
2. Apply canonical mutations and exact outbox trims.
3. Compute complete semantic checksum and history root.
4. Render/validate transaction bytes and any materialized anchor.
5. Compute/validate physical references, manifest, and HEAD bytes.
6. Publish immutable artifacts, then attempt the captured exact HEAD CAS.

Retain capacity failures before first PUT, L0 maintenance/backpressure,
reclamation-bound candidate identity, separate fencing, and uncertainty
classification. Failed CAS consumes the attempt. `CatalogAuthority` re-executes
the frozen command within its existing budget, rebuilding reads, decisions,
receipts, responses, intents, and candidate objects. No logical rebasing or
staged-output reuse. Keep reconstruction explicit in reconciliation, complete
outbox inspection, checkpoints, restore, maintenance, and all eager boundaries.

### 6. Request bounds and failures

Retain 64 MiB segments/raw Arrow batches, 512 KiB directories, 4 MiB returned
scan pages, one million page rows, and 64 segment/block fetch caps. Bound retained
overlay, observations, and memo payload accounting to 64 MiB and one million
entries. Skip optional memo admission before rejecting essential growth with
`MaintenanceBackpressure`. These are accounting limits, not RSS guarantees;
full commit reconstruction is measured separately.

Preserve validation, precondition, storage, integrity, backpressure, stale-writer,
CAS, and ambiguous-outcome error types. Never translate unavailable/corrupt
evidence to absence. Pins are nondurable with no deadline/retention renewal;
reclaimed objects cause reads/commit to fail closed. Successful memoized reads
may remain available, but commit freshly validates its full closure.

## Executable acceptance bounds

Repeat the 17 scaling fixtures: block targets 32/64/128/256 KiB, block counts
1/4/16/64, disjoint L1 counts 1/4/16/64, and L0 suffixes 0/1/8/16/31. Retain
oversized-row, disabled-filter, and projection cases. Let N be L0 length, S the
owning L1 count, B distinct required blocks including lookahead, and P pages.

| Operation | Required bound |
| --- | --- |
| Stable HEAD begin | One HEAD metadata, pointer read, manifest read; zero transaction/directory/data reads |
| Genesis begin | One HEAD metadata; zero object reads |
| Begin computation | Zero replay rows, full checksums, rendered validations |
| Cold point | At most one selected L1 directory/block; with L0 at most 2N+1 metadata objects and N+1 blocks |
| No-L0 negative | Outside bounds zero directory/data; Bloom-negative one directory, zero blocks |
| Repeated point / identical witness | Zero additional backend reads |
| Overlay scans | No full-segment fallback; at most B+(N+1)(P−1) blocks; no-L0 B+P−1 |
| Cold outbox ID | At most S+2N metadata and S+2N candidate blocks; zero KV blocks |
| Begin/cold point allocation | At most 12 times returned metadata/data bytes + 128 KiB |
| 4096-row no-L0 begin/point/rollback | Bytes and cumulative allocation each at most 10% of real eager begin |
| Equivalent no-read lifecycle | Total data reads no greater than eager begin+commit; executable separately counted reconstruction/validation |

Gate 3's 4096-row/64 KiB eager begin reads 4,717,175 bytes and allocates
51,131,615 cumulative bytes. Use a real eager reference; never label a lazy
operation `eager_begin`. Record begin, first/repeated/different points, scans,
witness capture/reuse, staging, commit replay, candidate rendering/publication,
CAS loss, full-command retry, reconciliation, eager escapes, and matched whole
transaction totals separately. Measure metadata, selected/full data requests,
ranges, bytes, allocations, authentication/witness/canonical hashing, and rendered
validation; identify nested counters. Preserve the 256-setup/200-sample workload
and all earlier reports.

## Tests and verification

Extend the independent oracle with pinned snapshots, overlays, observations,
assertions, and HEAD transitions. Add a test-only eager reference preserving
pre-Gate-4 reads/preconditions. Compare both with independently calculated
outcomes; the oracle cannot call production replay/witness helpers. Cover:

- Read-your-writes, repeated writes/deletes, never-present/present-empty,
  tombstone generations, binary/empty keys.
- Point/range/predicate assertions, tombstone-only/overlapping ranges,
  concurrent insert/delete, delete/reinsert, same-value ABA.
- Pagination across blocks/shards/L0, staged keys around boundaries, empty
  progress pages, genesis, oversized rows, foreign/public cursor rejection.
- Outbox order/duplicates/incarnations/keyless shards/provenance and failed or
  cancelled multi-target staging.
- Pins across logical commits, maintenance, writer claims, reclamation and
  post-retention collection.
- CAS loss with changed decisions and regenerated outputs; cancellation,
  lost responses, incomplete reconciliation.
- Missing/corrupt accessed objects, unrelated corruption deferred to commit,
  corruption after memoized read, unchanged HEAD on failed validation.
- Existing checkpoints, restore, retained GC, and Gate 0 fault schedules.

Run focused tests, then these lanes sequentially:

```sh
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0
cargo test -p arco-catalog --features test-utils --test state_store_reclamation_schedules --locked
cargo test -p arco-catalog --features test-utils --lib --tests --locked
cargo clippy -p arco-catalog --features test-utils --lib --tests --benches --locked -- -D warnings
cargo test -p arco-catalog --bench control_mvp_gate --locked
cargo fmt --all -- --check
git diff --check
ARCO_SCALING_REPORT=docs/reports/2026-09-06-gate4-block-scaling.json cargo test -p arco-catalog --features test-utils --test control_cost_smoke authenticated_block_scaling_acceptance --locked -- --ignored
ARCO_LAZY_TXN_REPORT=docs/reports/2026-09-06-gate4-lazy-transactions.json cargo test -p arco-catalog --features test-utils --test control_cost_smoke lazy_transaction_scaling_acceptance --locked -- --ignored
cargo test -p arco-core --locked
cargo check -p arco-api --all-targets --locked
```

Execution note: Cargo starts integration tests in the crate directory. Resolve
`ARCO_SCALING_REPORT` and `ARCO_LAZY_TXN_REPORT` to absolute paths under this
worktree's `docs/reports` directory. The first relative-path scaling invocation
completed its assertions but failed to write the report; its terminal log is
preserved, and the lane is rerun with the corrected destination.

## Evidence and exit

Create Gate 4 operation-cost, scaling, lazy-transaction, comparison, verification,
and closeout artifacts. Preserve prior reports byte-for-byte (the existing
progress report is explicitly updated). Update design contracts for lazy access,
async compatibility, local cursors, nondurable lifetime, and eager boundaries.
Capture commands, environment, statuses, logs, source hashes, acceptance results,
comparisons, and limitations.

Obtain a fresh read-only subagent audit with no inherited implementation
conversation, supplying this contract, base revision, actual diff, and evidence.
Resolve demonstrated findings and rerun affected checks. The planning audit does
not satisfy this review; unproven mandatory criteria keep Gate 4 incomplete.

Preserve a recoverable checkpoint with binary patch, new source/docs/evidence,
file hashes, archive hash, logs, and recovery instructions. Verify archive
contents against the final worktree. Exclude credentials, Git metadata, and build
trees.
