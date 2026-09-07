# Authority 7 integrity commitments, encoding 1

Gate 3 writes authority format 7 and restore-plan format 6. Segment/directory
format 1 and continuation version 3 are unchanged. Fresh fixtures are required;
older authority formats are rejected and older restore plans are supersession-only.
The approved authority-6 candidate remains in the Gate 2 checkpoint archive.

## Canonical primitives

All roots use SHA-256 and lowercase hexadecimal output. Encoded digests are
exactly 32 raw bytes, after validating the 64-character lowercase hex input.
Integers are unsigned, fixed-width big-endian. `bytes(X)` is a u64 byte length
followed by the exact bytes. `optional(X)` is one byte 0 for absent, or one byte 1
followed by `bytes(X)` for present; absent and present-empty are distinct.

Every hash starts with:

```
bytes(fixed domain tag)
u32(encoding version = 1)
bytes("arco-state-control-mvp")
u32(authority format = 7)
bytes(exact tenant UTF-8)
bytes(exact workspace UTF-8)
bytes(exact domain UTF-8)
```

There is no normalization of scope strings or binary keys. Collection counts are
explicit u64 values. The independently generated binary inputs and expected
digests are in `../reports/2026-09-06-gate3-canonical-vectors.json` and are checked
against the Rust implementation.

## Logical history

Genesis uses tag `arco/control-v1/history-genesis` and no additional fields.
The manifest persists `history_anchor { sequence, root }` and `history_root`.
The anchor sequence equals the selected base-state sequence, or zero with the
scope-bound genesis root when there are no base states.

A mutation digest uses tag `arco/control-v1/mutation`, followed by:

1. Optional request-ID UTF-8 bytes.
2. KV count; entries sorted by unsigned binary key. Each entry is key bytes,
   u64 generation, and optional value bytes. The absent-value discriminant is
   the tombstone; a present empty value remains a value.
3. Outbox-addition count; entries in durable logical order. Each entry is record
   ID UTF-8 bytes, exact opaque payload bytes, and u64 origin sequence.
4. Trim count; exact incarnations sorted by record ID UTF-8 bytes then origin.
   Each entry is record-ID bytes and u64 origin sequence.

The history step uses tag `arco/control-v1/history-step`, then preceding root,
u64 committed sequence, and mutation digest. Empty committed transactions still
advance history. Physical owning IDs, block boundaries, layout generation,
reclamation generation, writer epoch, reads and successful precondition checks
are absent. Application outbox payloads are durable logical data; embedded
application provenance is preserved byte-for-byte during physical rewrites.

Every suffix transaction reference and transaction metadata object carries the
same `history { preceding_root, mutation_sha256, resulting_root }`. Root opening
validates suffix continuity from the anchor. Eager replay decodes transaction
data, recomputes mutation digests, and requires exact link agreement. Full-state
checksums remain the existing comparison over sequence, generations, tombstones,
values and ordered outbox contents; they are not replaced by the history root.

## Physical layout

Manifest layout uses tag `arco/control-v1/manifest-layout`. It encodes three
ordered collections, each preceded by its u8 role and u64 count: role 1 base
states, role 2 inline anchors, and role 3 transaction suffixes. Empty collections
are explicit. The root field itself is excluded.

Each owning state reference encodes state ID bytes, u64 logical sequence, u64
segment length, u64 directory length, u32 segment format 1, u32 directory format
1, optional minimum binary key, optional maximum binary key, segment raw digest,
and directory raw digest. The role fixes L1 ownership. Bounds are decoded from
validated hexadecimal metadata before encoding. An empty/keyless reference has
both bounds absent. State sets validate lengths, digests, common sequence,
unique identities, ordered disjoint key bounds, and trailing keyless shards.

Each transaction reference encodes transaction ID bytes, u64 sequence, u64 raw
metadata length, u32 authority format 7, and raw transaction digest. That digest
transitively binds its L0 segment/directory lengths, digests and history link;
the directory digest transitively binds block metadata and raw block digests.

Checkpoint layout uses the distinct tag `arco/control-v1/checkpoint-layout` and
one role-4 state-reference collection. A checkpoint computes this root from its
actual state objects, even when they differ from the manifest's objects. Copying
the source manifest's physical root into this field is invalid.

## Rewrite and checkpoint evidence

Before any materialized L1 output is published, its exact rendered block/index
bytes pass the normal codec. The combined result must equal the independently
expected sequence, history root, complete KV generations/tombstones/values and
ordered outbox incarnations. Rendered L0 mutation bytes also decode through the
normal codec and must reproduce the expected mutation digest. This applies to
inline anchors, existing consolidation, checkpoints and restore outputs.

Maintenance persists equivalence evidence with encoding version 1, exact source
manifest ID/digest, source history/physical roots, logical sequence and semantic
checksum. Local metadata checks bind that evidence to its exact parent ID/digest reference
and the candidate's unchanged logical state. When ancestry is traversed, the
source physical/history roots must also match the authenticated parent. The
exact captured HEAD CAS and fence conditions still govern publication; contention
regenerates the candidate. The evidence does not retain the predecessor.

Checkpoint validation binds encoding version 1, source manifest raw digest,
source history root, source physical root, semantic checksum and the independent
checkpoint physical root. Common source/state checks govern checkpoint reads,
persistence and resolution; GC uses the same local/source metadata validation
and owning state references. Creation retains full source replay verification.

Nonempty restore continues the destination's history. Empty restore selects the
exact retained source as its candidate parent, then appends the restore mutation.
Restore plan 6 embeds the exact transaction reference, including its length and
history link. Old plans cannot become publishable by relying on missing fields.

## Access and retention boundaries

Reader opening authenticates the selected root and local metadata. Point reads
and scans authenticate selected directories/blocks. Gate 4 transaction begin
pins authenticated metadata only. Commit freshly authenticates the pinned
manifest, reconstructs with `replay_for_successor`, checks redundant anchors, and
revalidates promoted bases before publishing anything. Checkpoint
creation/read/persistence, reconciliation, complete outbox inspection, maintenance
and restore retain whole-state checks.
Ordinary witnessed historical reads do not recursively fetch predecessors.
Parent links remain resolution metadata, not retention pins.

Resolution processes at most 4,096 manifests and 64 MiB, one manifest at a time.
Range probes respect the remaining byte budget. Incomplete ancestry or exhausted
budgets are ambiguous outcomes; digest, history, cycle and transition mismatches
are integrity errors. Neither becomes a successful acknowledgment or false
supersession. HEAD-only writer/reclamation advances preserve both integrity roots.

All evidence is local. Gate 4 adds the transaction access contract below; provider
qualification, durable incremental maintenance and caches remain outstanding.


## Gate 4 transaction access contract

Transaction begin reads HEAD metadata, the pointer, and its authenticated manifest.
Genesis reads HEAD metadata only. A private genesis/manifest pin records exact
HEAD version, writer and reclamation fences, sequence, manifest digest, and the
manifest history/layout commitments. It contains no partial replay state.

Point observations distinguish never-present, live generation, and tombstone
generation. Staged values have no committed generation; staged deletes read as
absence. Overlay-only point reads record their local origin. Explicit assertions
always inspect the pinned base, even after a write/delete. Completed range
fingerprints preserve the existing encoding, include tombstones and gaps, and
are reused within the request. A retained tombstone makes range-empty fail.
Empty and reversed half-open ranges remain empty. Wide ranges hash resolved
rows in bounded chunks without collecting a full result or imposing a total
range traversal limit.

Public and transaction scans share an authenticated ordered row stream. The
transaction merges staged writes before page limits, and records base evidence
through the last fully resolved key. A terminal page records remaining-prefix
exhaustion. Every transaction cursor carries a private, fresh in-memory origin,
optional base authority, scope, prefix, and exclusive boundary. It works only in
that transaction, cannot be encoded as an opaque public continuation, and is
rejected by public readers. No continuation v3 fields change. Genesis may return
staged values and a local cursor without a durable `StateToken`. Inserts, updates,
and deletes above the boundary affect subsequent pages; keys at/below it are
never revisited. Empty pages can make progress over tombstones or staged deletes.

`stage_projection_outbox`, `stage_projection_intent`, and
`trim_projection_outbox` are async source API changes; crate-local
`range_witness` is async and fallible. `ArcoStateTxn` signatures are unchanged.
Outbox lookup visits all L1 owners including keyless shards, since manifest bounds
and Bloom filters cover only KV keys. It fetches matching outbox blocks and
applies exact L0 trims before additions. A multi-target trim stages nothing until
all validation completes; errors and cancellation preserve batch atomicity.
Commit independently rejects duplicate IDs, invalid incarnations, and a trim and
addition of the same ID in one transaction.

The retained overlay, observations and optional memo payloads are accounted
against 64 MiB and one million entries. Essential growth can evict optional memo
values; an unadmittable operation fails with `MaintenanceBackpressure` before
staging. This accounting is not an RSS bound. Existing object, raw Arrow, page,
segment and block caps continue to apply. Commit reconstruction is measured
separately and retains the complete semantic checksum, history, physical root,
rendered transaction/anchor validation, capacity-before-PUT, fencing and exact
HEAD CAS boundary. CAS failure consumes the attempt; catalog retries rerun the
frozen command and regenerate all outputs and candidate identities.

Pins are nondurable and provide no deadline or retention renewal. A required
object that is missing or corrupt fails closed when accessed. A memoized read
may remain available, but commit bypasses the memo and freshly authenticates the
manifest and all reconstruction/validation inputs. Unrelated corruption may be
deferred until commit; it never becomes negative evidence or a publishable base.
