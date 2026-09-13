# Gates 2 and 3: authenticated block reads and integrity roots

Execution contract: implement and locally qualify Gate 2, obtain a fresh read-only
audit, preserve a hashed recoverable source/evidence checkpoint, then implement
and independently qualify Gate 3. Stop after Gate 3. Do not commit, push, deploy,
cut over, or run credentialed provider operations. One source owner and sequential
Cargo ownership. Preserve eager transactions, full-state checksums, frozen-command
re-execution, exact HEAD CAS, reclamation generation, separate writer fencing,
backpressure, retention floors, and uncertain-publication handling. Tier-1 success
remains fenced control-pointer publication; Parquet projections are asynchronous.

Starting worktree: `/Users/ethanurbanski/arco/.worktrees/state-store-vnext`, on the
state-store vNext work branch at `0235eb6c1552c5c87fe3a7638e10022889462b35`.
The 24 preexisting changed/untracked files and both baseline cost reports are
preserved in `/private/tmp/arco-gates-2-3-baseline-wonihzqn`, including a binary
tracked patch, source/evidence archive, status, per-file hashes, and archive hash.
Aggregate source hash: `990b423f5052f280cc0ea824d1b135ce7ca5a4b9df6a298cde55f5da16c74050`.
Preflight found approximately 30 GiB free and an unrelated Axon Rust build using
a separate target directory. Exclude credentials and generated build trees from
archives; preserve existing evidence.

## Gate 2

- Authenticate historical roots with private expected raw SHA-256 witnesses in
  opaque StateToken/CheckpointToken values. Preserve identity-field equality,
  independently check witnesses, and seal manifest witnesses in continuation v3.
  Reject older continuations explicitly. Mint witnesses from finalized bytes,
  HEAD, authenticated checkpoints, or persisted authority references.
- Every non-genesis manifest has an exact parent ID/digest pair. ProjectionIntentV1
  wire serialization stays unchanged to avoid hash cycles. Loaded outbox records
  carry private authenticated observed-root witnesses; source resolution verifies
  payload, scope, record ID, and origin sequence against the record.
- Projection fallback and old transaction membership use authenticated parent
  links, one manifest at a time, at most 4,096 manifests and 64 MiB metadata.
  Digest/cycle/transition mismatches are integrity failures; incomplete proofs
  are AmbiguousAuthorityOutcome, never successful acknowledgment or supersession.
  Exact witnessed reads open directly. Parent links do not extend retention.
- Authority format 6, restore-plan format 5, segment/directory format 1. Decode
  bounded version headers before current fields/references; unsupported authority
  versions return UnsupportedAuthorityFormat; older restore plans are
  supersession-only. No migration or dual readers.
- Segments concatenate complete, independently decodable Arrow IPC files, each
  with supported schema, exactly one batch, footer, no dictionaries or unsupported
  compression. Target 64 KiB; ordinary multirow blocks at most 256 KiB; isolate
  larger rows within applicable segment limits. Keep 64 MiB segment, 512 KiB
  index, one-million-row caps and L1 half-cap writing. At most 4,096 blocks.
  Rows sort by kind/binary key, retaining logical ordinals/outbox incarnations;
  empty segments have an explicit empty block and empty key metadata.
- Owning references bind segment/index lengths and whole-object digests;
  authenticated directories bind block offsets, lengths, kinds, key bounds,
  counts, ordinal metadata, and raw digests. Validate contiguous full coverage,
  nonoverlap, checked arithmetic, counts and ordering before pruning/decoding.
- Add only ScopedAuthorityStore::get_range(path, Range<u64>) through the existing
  scoped backend. Read directories with declared-length-plus-one bounded probes;
  selected blocks use exact ranges, length/digest checks and Arrow preflight.
  Full-state operations validate all blocks, global rows/ordinals, whole segment
  digests and semantic replay checksum; selective reads validate accessed data.
- Bloom filters cover distinct KV keys including tombstones. Enabled: at least
  ten bits/key, byte rounding, seven probes, persisted SHA-256 double-hash v1,
  documented integer encoding/bit numbering. Empty: zero bits/probes and absence.
  Disabled: may match. Raw cap 128 KiB. L1 splits to fit metadata budget; L0 may
  disable the filter, but directory overflow fails capacity before publication.
- Points use manifest bounds, one selected L1 directory and at most one candidate
  KV block per overlapping segment, then L0 in sequence. Scans merge ordered
  iterators by key/sequence, fetch only to establish safe next keys, retain
  tombstone progress and stop at row/byte/segment/64-block page limits. Cursors
  name the last fully resolved key; unsafe overlays return backpressure.
- Use the codec for commit, anchors, maintenance, checkpoint and restore.

Gate 2 evidence: extend the shared cost harness while preserving its workload and
both baseline reports. Account separately for manifests, transactions, directories,
blocks, requested ranges, returned bytes, full reads, failures, allocations, and
authentication. Deterministic 32-byte keys/1 KiB values; independently vary blocks
and disjoint L1 segments through 1/4/16/64 and L0 suffix 0/1/8/16/31. Document
test-only writer sizing. Separate reader opening, pinned/current operations,
projection resolution, and eager begin. Required bounds:

| Scenario | Acceptance |
| --- | --- |
| Pinned L1 point hit | One directory, one block range, no full data GET |
| Uniform segment >=16 blocks | Point data <=2x average block bytes |
| Bloom-negative miss | One directory, no block reads |
| Outside manifest bounds | No directory/data reads |
| Increasing disjoint L1 count | Still one selected directory/block |
| L0 suffix N | <=2N+1 metadata objects, <=N+1 blocks |
| No-L0 scan B blocks/P pages | <=B+P-1 block reads |
| Enabled Bloom | No false negatives; <2% FP over 100,000 fixed absent probes |

Compare 32/64/128/256 KiB targets on identical 4,096 rows. 64 versus 256 KiB:
encoded and complete-scan data <=1.15x; point data <=0.40x; scan requests <=4.5x+1;
index bytes <=3x and absolute caps. Separate oversized-row/disabled-filter cases.
Allocation growth follows directories and selected blocks; eager reconstruction
is separate. Latency is diagnostic; MemoryBackend proves API calls, not provider
traffic/billing. Cover empty/binary keys, values/misses, updates/deletes, boundaries,
pagination, overlays, partial/appended/truncated bytes, overflow/malformed Arrow,
substitution, corrupted filters/scope and coherent historical-root replacement.

## Gate 3 (only after Gate 2 exit and archive)

Refine against exact Gate 2 source. Authority format 7, restore-plan format 6;
segment/directory v1 stays. Fresh fixtures only. SHA-256 roots use separate fixed
domain tags, encoding versions, implementation/authority binding, exact scope,
length-prefixed fields, fixed-width integers and collection counts.

- Logical-history root: scope-bound genesis; chain every committed mutation,
  including empty transactions, with sequence and canonical mutation digest.
  Digest covers request identity, sorted KV mutations/generations/discriminants,
  ordered outbox additions and exact trim incarnations. Exclude physical IDs,
  block/layout/fencing details and transient reads/precondition evaluations.
  Persist anchor sequence/root and each suffix's preceding/result roots/mutation
  digest. Validate continuity locally; full replay recomputes mutations.
- Physical-layout root: ordered role-tagged owning references for base, anchors
  and suffix, with canonical IDs, lengths, formats, bounds and raw digests.
  Transaction/directory digests transitively bind L0/block metadata. No hash cycle.
- Keep full-state checksum over sequence, all generations/tombstones/values and
  ordered outbox. Mutations advance history; equivalent maintenance preserves
  history/state but changes layout. HEAD-only fences preserve both roots.
- Decode exact rendered outputs through the normal codec and compare independent
  complete expected state before maintenance/anchors/checkpoint/restore publication.
  Maintenance evidence binds source ID/digest and equality of sequence/history,
  every KV/version/tombstone and ordered outbox/incarnations. Exact captured HEAD
  CAS/fences remain required; contention regenerates.
- Restore to nonempty destination continues destination history; empty destination
  seeds exact retained source lineage then appends restore mutation.
- Embedded checkpoint evidence binds source manifest digest/history/layout/state
  checksum and distinct physical root over actual checkpoint references. Share
  validation/owning-reference enumeration across publication, persistence,
  resolution, restore and GC. Keep evidence inside immutable objects.
- Reader opening authenticates root/local metadata; point/scan validate accessed
  data; eager begin/checkpoints/persistence/maintenance/restore verify whole state.
  Ordinary current/witnessed reads do not recursively fetch ancestors. Distinguish
  integrity, logical CAS conflicts, and incomplete reconciliation.

Extend the independent oracle to generations, tombstones, ordered incarnations
and history. Add canonical vectors and differential convergent histories/layouts;
lost/duplicate/reordered/altered rows/links; forged equivalence/scope/checkpoint
layout; coherent root/intermediate replacement; provenance/restaged IDs/concurrent
HEAD; missing/budget-exhausted ancestry; empty/nonempty restore/stale maintenance;
retained closures across GC and all Gate 0 schedules. Repeat Gate 2 scaling bounds;
separate added metadata/hash/allocation/full-rewrite validation costs.

## Exit verification for each gate

Run sequentially with CARGO_INCREMENTAL=0, CARGO_PROFILE_DEV_DEBUG=0 and
CARGO_PROFILE_TEST_DEBUG=0; retain commands, logs and terminal statuses:

```sh
cargo test -p arco-catalog --features test-utils --test state_store_reclamation_schedules --locked
cargo test -p arco-catalog --features test-utils --lib --tests --locked
cargo clippy -p arco-catalog --features test-utils --lib --tests --benches --locked -- -D warnings
cargo test -p arco-catalog --bench control_mvp_gate --locked
cargo fmt --all -- --check
git diff --check
```

Also run the new scaling lane, arco-core range tests and dependent arco-api checks.
Each gate requires all checks/cost bounds and a fresh independent read-only audit
with no unresolved correctness/safety blocker. Resolve findings and repeat affected
checks. Update progress/design/cost artifacts with exact format deltas, matrices,
terminal results, audit verdicts and limitations; report each gate separately.
Gate 2 additionally requires a hashed source/evidence archive before Gate 3.
Stop with Gates 4–7 (lazy transactions, durable incremental maintenance, caches,
provider qualification) outstanding.
