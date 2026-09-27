# Restore standard output metadata and immutable publication

2026-09-14. Additive Step 3 contract frozen before implementation and measurements.
This freezes a candidate implementation and its qualification obligations. No
native restore capability is enabled by a component pass.

Reuse the existing production Arrow, block metadata, segment index and physical
descriptor codecs. Standard output remains one nonempty KV block whose checked
encoded envelope is at most 256 KiB. Keep generated segment identity frozen by
the selected plan/ordinal/part/role. No segment splitting/reencoding, directory
builder, candidate manifest, or HEAD publication occurs in this output primitive.

## Allocation and ownership

Use the existing shared pre-execution allocation guard and sticky physical route.
All arithmetic is checked; all existing live ownership and final cumulative carry
remain charged. Require the qualified 64-bit target and existing block/index/
descriptor layout guards. Unsupported size/layout/overflow stops before allocation.

- Arrow: existing frozen standard A_arrow plus 64 KiB for transfer/bookkeeping.
  Convert the encoded Vec to Bytes inside that same guarded closure, retaining
  its full allocation bound. Bytes transfers backing without payload copying;
  any Shared header allocation is covered and measured. Guarded output must not
  outlive its backing reservation or release it separately.
- Define N as row count, E as first plus last key lengths, B=ceil(10N/8),
  S as the sum of tenant/workspace/domain byte lengths, I as segment ID length.
  Existing block_metadata plus build_segment_index reservation:
  64 KiB + 64N + 3B + 5E + S + I + 128.
  This includes block/segment digest strings, both block/index endpoint copies,
  optional UTF-8 endpoints, cumulative filtered key-vector growth, raw/hex Bloom,
  scope/id strings, fixed model vectors, and source-bounded counter bookkeeping.
  Keep inputs and their ownership guards alive until construction is complete.
- Descriptor construction clones existing guarded model fields under a new
  reservation: 64 KiB + S + I + U + W + 2E + 192, where U/W are the byte
  lengths of the admitted segment/index object versions. The index guard remains
  live through cloning. Returned version capacities are separately invoiced on
  arrival; using their lengths for fresh clones does not replace that invoice.
- Index JSON reserves 512 KiB + 64 KiB; descriptor JSON reserves 1 MiB + 64 KiB.
  Allocate one fixed-capacity byte slice and serialize the concrete production
  struct through std::io::Cursor<&mut [u8]>. A full slice returns WriteZero and
  cannot grow. Truncate visible length and convert to Bytes inside the guard;
  retain the full backing capacity. Discard serializer errors into one fixed
  typed backpressure message. No arbitrary custom Serialize implementation is
  admitted: the private entry accepts only the existing index or descriptor.

The 64 KiB fixed terms cover enumerated fixed vectors/digests, serializer error
boxes/static error strings, Bytes shared headers, and fixed counter nodes, with
source proof and compiled qualification. They are not measured maxima. Pin the
Rust 1.88 std/io capture (commit 6b00bc3880198600130e1cf62b8f8a93494488cc),
locked serde_json/bytes/hex/sha sources and callers. Source drift invalidates
qualification. The previous proposed 32N, Block<=120 and private ErrorImpl=32
premises are withdrawn; the final model uses 64N and Block<=192, and no exact
private ErrorImpl size is assumed by this fixed allowance.

## Immutable writes

Publish segment, index, then descriptor with DoesNotExist. Reserve each operation
before I/O, retain guarded expected Bytes while awaiting, and invoice every arrived
WriteResult/error before further I/O or allocation. Capture exact nonempty object
versions only from accepted results. Account output packed bytes separately from
control metadata, and count all writes/HEADs/ranges/bytes/counters and failures.

On PreconditionFailed, require HEAD-before, classified exact range probe, and
HEAD-after. Both metadata sizes must equal expected length and both versions must
match the conditional failure's current_version. Require byte equality and known,
admitted response ownership. No collision result establishes commitment. A clean
three-object path has three PUTs; all three collisions require twelve operations.
Changed/missing/corrupt/unknown/oversized responses, exhausted budgets, cancellation,
or indeterminate storage failures stop the invocation. Descriptor publication
uses the exact segment/index versions proven by this sequence. No output can
publish a receipt/progress selector or readable authority by itself.

The ordinary 32 MiB packed-output target, combined 64 MiB request ownership,
workspace control invoice, and exact physical response accounting stay independent
and cumulative as already frozen. Singleton output requires its explicit phase
and 64 MiB+24P accounting; this standard proof cannot be used for it.

## Required verification

Compiled behavioral red for missing reservation/cap enforcement before changes.
Exact production byte parity, both metadata kinds, escaped/Unicode/opaque strings,
cap overflow without Vec growth, backing-capacity ownership and release, low live
allowance and final carry, error/panic/underestimate stop, and max standard rows.
Publication tests cover successful writes, exact/different collisions, mutated
before/after metadata, inflated response capacity, cancellation at each boundary,
and zero further I/O after failed admission. Require exact-source audit and final
checks before accepting output publication. Native merge, complete coverage,
singleton phases, final publication and whole-invocation accounting remain required.

## Amendment 1: physical storage errors and shared I/O admission

Before the next affected run, account core storage errors on PUT and collision
HEAD/range paths before conversion or another await. Known core errors contain
one owned String; move it into the private physical Storage error without
formatting or copying. Retain its response ownership handle in the stopped
physical invocation through cleanup; the outer service still invoices the
returned CatalogError. The arrival census conservatively includes both inline
error layouts and String capacity. An opaque boxed source or a future unknown
variant has unprovable ownership: record unknown response evidence, do not format
its source, and stop with a fixed error. Such a run cannot pass qualification.
No public format-7 error conversion changes.

Final microchunk range-reservation field names become I/O-reservation names,
because the same cumulative byte/operation admission now includes writes.
Physical counters retain separate range/HEAD/write counts and submitted bytes.
