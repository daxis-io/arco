# Control authority block format 1

Gate 2 authority envelopes and HEAD use version 6; restore plans use version 5.
Segment directories independently use version 1. Older/unknown authority headers
are rejected before typed payload decoding or following owning references. Older
restore plans remain readable solely for supersession. Continuations use AES-GCM
envelope version 3, prefix `v3.`, AAD `arco/control-v1/scan-continuation/v3`, and a
required lowercase 64-hex raw manifest SHA-256 witness. Versions 1 and 2 are
explicitly unsupported. There is no migration or dual reader.

## Authentication and ownership

Opaque state/checkpoint tokens privately carry the expected digest of their exact
raw immutable root bytes. Their public equality continues comparing identity;
every root read independently checks the digest. HEAD, finalized publication
bytes, authenticated checkpoints, and persisted references provide witnesses.
Identity-only provisional tokens used to serialize intents cannot open readers.
Current HEAD must match its manifest's logical sequence, and its separate writer
epoch and reclamation generation must be at least the authenticated manifest's
counters. Greater counters permit existing HEAD-only fencing advances.

Each non-genesis manifest binds its exact parent ID and digest. Ancestry walkers
authenticate one manifest at a time and validate logical extension or equivalent
maintenance transitions. Their limits are 4,096 manifests and 64 MiB metadata.
Missing ancestry or exhausted limits produce ambiguous authority outcomes;
digest/cycle/transition mismatches produce integrity errors. Parent links do not
extend retention and are not recursively enumerated by GC. Age-selected GC roots
remain conservative retention candidates, not trusted historical read tokens.

ProjectionIntentV1 serialization is unchanged. Authenticated replay privately
stamps outbox records with their observed root token. Source resolution checks
the exact serialized intent payload, scope, record ID and origin sequence against
that record, then finds the source through authenticated parents. This avoids
putting a manifest's own digest in its outbox and creating a hash cycle.

Owning segment references bind raw segment/index SHA-256 digests and exact byte
lengths. L1 owning references also bind binary KV bounds. Directories bind scope,
implementation, level, sequence, total rows, segment length/digest, filter metadata
and every block. Directory reads request `[0, declared_length + 1)` and reject
short or appended content. Selected block reads request exactly their bound span
and require exact returned length and raw digest before Arrow preflight.
Eager whole-segment reads also use a declared-length-plus-one bounded range and
reject appended or truncated objects before decoding.

## Segment container

A segment is the concatenation of complete Arrow IPC files. Every block has its
own schema, one batch, and footer. No schema or dictionary state is shared.
The existing eight-column schema remains: record kind, binary key, nullable
binary value, generation, tombstone, logical sequence, logical ordinal, nullable
origin sequence. Dictionaries, unsupported schemas/compression and malformed
Arrow metadata remain rejected by normal preflight/decoding.

Writer target is 64 KiB. Ordinary multirow blocks are capped at 256 KiB. A larger
indivisible row is isolated and bounded by the applicable segment byte cap.
Readers cap segments at 64 MiB, indexes at 512 KiB, rows at one million, and
directories at 4,096 blocks. L1 writers retain half-cap limits and recursively
partition on capacity failure. Test-only sizing adjusts the target and L1 row
partition without relaxing production reader caps or L0 backpressure.

Blocks contain one record kind and strictly increasing binary keys. Blocks sort
by kind/key, with nonoverlapping bounds within a kind. Logical ordinals preserve
semantic ordering independently of this physical order, including outbox
incarnations. Empty segments contain one explicit empty IPC block with null
kind/key/ordinal metadata. `recordBatchOffsets` now records container block starts;
individual IPC footer offsets remain local to each complete file.

Each directory block binds offset, length, kind, minimum/maximum binary key hex,
row count, minimum/maximum logical ordinal, and raw digest. Spans must be checked,
contiguous, nonoverlapping, and cover exactly the declared segment length.
Aggregate rows/KV counts and directory bounds must agree with block metadata.
Full-state decoding validates all blocks, reconstructed metadata, global key and
ordinal consistency, complete segment digest, and the replay-state checksum.
KV ordinals are contiguous from zero across L1 shards and KV rows carry no outbox
origin sequence. L0 KV generations equal the transaction sequence. IPC record
batch compression is rejected during bounded message preflight, before decoding.
Selective reads validate authenticated directories and selected blocks only.

## Bloom encoding

The directory persists `bloomHashVersion = 1`, mode, probe count, distinct KV-key
count, and hexadecimal filter bytes. All distinct KV keys, including tombstones,
participate. Enabled storage is `ceil(10 * key_count / 8)` bytes with seven probes.
The raw cap is 128 KiB. L1 partitions before filter/index capacity is exceeded.
An L0 transaction may explicitly disable its filter to fit the index budget;
directory overflow still fails before publication.

For key bytes K, compute SHA-256(K). Read bytes 0..8 and 8..16 as unsigned big-endian
64-bit integers h1 and h2, and set h2's low bit. Probe i in 0..7 uses
`((h1 + i * h2) mod 2^64) mod bit_count`. Bit b is mask `1 << (b mod 8)` in byte
`floor(b / 8)`; byte storage is hex-encoded in increasing byte order. Empty mode
has zero keys, bits, and probes, and guarantees absence. Disabled mode has zero
bits/probes and always means possible membership. Neither mode uses modulo zero.

## Selective reads and pages

Point reads choose L1 using manifest bounds, load only the chosen directory and
at most one KV block per overlapping segment, then apply L0 in logical sequence.
Scans merge one ordered L1 stream and ordered L0 streams. Each stream loads its
next block only when needed to establish the next safe key. The latest logical
sequence wins; tombstones still advance the fully resolved boundary. Page limits
cover rows, decoded bytes, segments, raw Arrow bytes and 64 block fetches.
If required overlays prevent any safe progress, the reader returns backpressure.
Continuations name the last fully resolved key and exact observed root witness.

No cache, lazy transaction engine, pending-projection retention map, ancestry
index, provider qualification, or Tier-1 Parquet publication barrier is added.
