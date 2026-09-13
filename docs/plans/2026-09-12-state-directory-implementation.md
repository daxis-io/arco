# Bounded state directory: implementation slice 1

This implements the first slice of the reviewed capacity design. The input is the
exact 1,099-file capacity candidate, manifest
11af6ae7f7649040bf4796da52729f01fb744000db3145cfdbe673ce2951f98b.
One source writer and sequential Cargo queue; isolated target and evidence under
/private/tmp/arco-state-directory-20260912-01. Preserve every previous candidate.
No cloud traffic, commits, staging, authority activation or conversion is included.

## Frozen component contract

Implement a crate-internal immutable ordered directory over logical block references.
The reference contains first/last key, row count, encoded block length and block hash.
It identifies a candidate block: its caller must authenticate and decode that block,
including checking actual endpoints/counts and semantic values. This layer does not
pretend that an opaque block hash proves its declared logical contents. Integration
with existing Arrow decoding and transition/equivalence certificates is slice 2.

The component has a separate directory version 1. It does not claim authority 8 or
restore-plan 7 support. Existing authority 7 parsing and publication remain unchanged.
Root references must originate from a trusted writer or an authenticated enclosing
authority; decoding a root is structural validation, not authority authentication.
Unknown versions fail closed. Fixed golden bytes/digests bind the codec contract.

- Immutable page objects: at most 64 KiB, 128 children and eight directory levels.
  All children in a page have the same level, ordered disjoint inclusive intervals,
  positive row counts, and checked aggregate counts. Empty state has one empty page.
- Complete fence keys are separately hash-bound, at most 256 KiB. Pages store fixed-size key digest/length references. This avoids silently
  limiting existing long keys merely to fit directory metadata. Every fence used for
  ordering or exclusion is authenticated before trusting that comparison.
- Canonical fixed-width little-endian references; magic strings identify page/root
  version 1. Domain-separated SHA-256 binds tenant/workspace/domain, object kind,
  length and raw bytes. Page hashes are physical identities, not logical history.
- New objects stay under scoped `control/directory/v1/domains/<domain>/`. Paths derive
  from validated scope and binary digests. No caller-supplied provider path is accepted.
- Stream ordered leaf references into a builder with at most 128 references per
  level. Flush pages incrementally; never materialize a whole directory or root.
  Publication is outside this component. Failed preparation leaves only unreachable
  objects; this component neither cleans them up nor asserts retention protection.
- Each read has caller-lowered hard ceilings of 4,096 object reads and 64 MiB probe
  bytes. Charge exact bounded probes before I/O. Return at most 128 block references.
  Read pages/keys afresh with a one-byte overrun probe and exact digest/length checks.
  No new cache or uncharged retained ownership pool is introduced.
- Point lookup returns the block whose interval contains the key, or authenticated
  absence between intervals. It does not assert membership inside a candidate block.
  Range lookup validates all ordering metadata on visited pages, descends only into
  intersecting intervals, and paginates at an exclusive previous-block boundary. Its
  continuation is tied to the exact root; no range omission on page boundaries.

## Tests and acceptance before completion

Write tests before implementation and retain the initial failure. Verify streaming
multi-level construction, exact block-order parity, points, gaps, bounded scans,
restart from encoded root, deterministic repeated construction, cross-scope rejection,
unknown versions, malformed counts/levels/lengths, overflow, reordered/duplicate
intervals, wrong child summaries, missing/replaced/deleted objects, and budget checks.
Use existing allocation-counter for bounded decoder and small-query peak allocations.
Include keys larger than a directory page and real existing Arrow block digest vectors.
Keep the original catalog suite passing, plus targeted Clippy, formatting and hygiene.
Commission the previously requested fresh read-only audit after stabilization.

Full-state semantic conversion, bounded catalog commits, durable restore, maintenance,
GC/retention and pilot qualification remain later slices. No directory can be made
visible through an authority HEAD by this component alone.

## Pre-integration corrections from independent review

Page packing and decoding also enforce a 4 MiB worst-case probe budget for the
page itself plus every nonempty first/last fence. Flush dynamically below 128
children when needed. Point lookup follows one candidate path without pagination
lookahead: at most eight such pages plus two endpoint keys, under 64 MiB and
4,096 objects even at maximum key length/depth. Empty keys are canonical scoped
hash constants and do not require zero-byte object/range operations.

Read budgets are fail-only safety guards: exhaustion returns an error and no
partial result, with already charged probes retained in the budget. Row-limit
pagination is the progress mechanism; callers may lower the row limit or raise a
caller-lowered budget within the hard ceiling. A partial unauthenticated page
never produces a successful cursor. Ranges are [lower, upper); an empty range
returns no blocks, reversed bounds fail. Cursors bind exact root and both bounds.

The builder defers a full-page flush until the next child or finalization, so the
exact eight-level capacity can finish; an additional level fails closed. Retries
use the existing immutable matching-write helper: same bytes are accepted after
conditional-create conflict and exact bounded readback; mismatches fail. A storage
error poisons that builder, and rebuilding from ordered input reconciles prior puts.

The canonical scope SHA-256 preimage is `arco.directory.scope.v1\0` followed by
tenant, workspace and domain in order, each framed as u64 little-endian UTF-8 byte
length then UTF-8 bytes. Object hashes use `arco.directory.object.v1\0`, framed
kind length and ASCII kind (`keys` or `pages`), scope digest, u64 little-endian
raw length, then raw bytes. Golden vectors are independently encoded in Python.

Version-1 pages are `ARCODIR1`, scope digest (32 bytes), depth (u8), child count
(u16 LE), then fixed 117-byte references. Each reference is depth (u8), byte length
(u32 LE), row count (u64 LE), first and last fence (each digest32 + length u32 LE),
and child/block digest32. Root bytes are `ARCOROT1`, scope digest, and one reference
(157 bytes total). Reject trailing bytes and unknown magic, counts or levels.

A block digest/length does not locate a range within an existing packed segment.
Slice 2 must bind the segment locator/offset through authenticated physical metadata
or explicitly store independently addressed blocks. This locator stays outside
logical history. This slice verifies compatibility of existing Arrow block bytes
and directory descriptors; it performs no semantic conversion or root activation.

## Legacy singleton compatibility correction

The existing 256 KiB block limit applies to multi-row blocks. Existing single-row
Arrow blocks can exceed it, up to the 64 MiB segment ceiling. Directory descriptors
preserve this exact conditional rule; they do not read block payloads in this slice.
A real 300 KiB-value L1 block vector must build and look up successfully. The previous
blanket 256 KiB descriptor admission was reproduced as a failing regression first.

The 256 KiB directory fence limit is a representation bound, not an asserted legacy
transaction key limit. Current L1 KV indexes encode each endpoint in hex multiple
times under a 512 KiB total index ceiling; even two hex copies of a key over 256 KiB
cannot fit. A real oversized-key L1 encode test confirms index backpressure. Thus
the bound covers existing valid L1 KV endpoints, while arbitrary transaction keys
and future outbox-directory key encoding/conversion still require explicit slice-2
compatibility validation. No unsupported key is silently truncated or reinterpreted.

## Short-fence locality amendment before final wire seal

The first unreleased-v1 codec incurred 521 object reads for a three-level point
lookup with eight-byte keys. Retain that codec, independent vector, and measurements
as superseded diagnostics. Inline keys of at most 64 bytes inside their fence
reference; longer keys retain external authenticated storage. No cache is added.

Each fence now encodes digest32, length u32 LE, and 64 inline bytes. Length is the
only mode discriminator: lengths 0..64 use exact key bytes then zero padding, with
the scoped key digest checked before comparison. Lengths over64 require all-zero
inline bytes and exact bounded external readback. Empty remains the scoped hash of
empty bytes plus zero length/padding. Reject noncanonical padding and bad digests.
References become245bytes, roots285bytes, and a full page31,403bytes. The4MiB
page-probe bound charges only external fences plus the page probe. Three-level
point reads over short fences must perform exactly three page reads.

This supersedes the earlier117-byte reference /157-byte root specification before
release or authority activation. It is not a supported-to-supported format change;
no earlier directory source has been committed, deployed, or made authoritative.
The independent fixed vector and full tests must be regenerated/rerun on these bytes.
