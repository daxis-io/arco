# Standard restore KV merge

Additive Step 3 contract, 2026-09-14, frozen before implementation or measurement.
Reuse Plan7 Mode and the existing production row validator. This primitive merges
two authenticated, sorted KV slices for one already-proved key interval. It does
not establish directory coverage, select an interval, or authorize publication.
The executor must bind input sequences and slices to the pinned Plan7 roots.

Present mode: source visible wins; equal visible bytes preserve current generation;
new, changed, or revived values receive the result sequence. A current visible
key without a source visible value becomes a result-sequence tombstone. Current
tombstones retain their generations. Source-only tombstones disappear.
Absent mode requires an empty current slice: source tombstones retain generation,
source visible values receive the result sequence. Values and keys are opaque.
All output rows have result sequence, no origin, KV kind, and contiguous checked
ordinals from the supplied starting ordinal. Result sequence is base + 1 with
checked arithmetic. Present source sequence may exceed the result sequence.
Absent mode additionally requires base sequence equal source sequence. Require
explicit source/base sequences even for empty slices. Each nonempty side also
supplies its exact authenticated descriptor block sequence, which must be nonzero
and no greater than its Plan7 manifest sequence. Validate per-side L1
sequence against that descriptor sequence, row semantics,
KV kind, strict key order, and contiguous input ordinals before producing output.

Use two sorted iterators and one Vec with capacity N_source + N_current. No map,
sort, full replay, checksum, Arrow encoding, or streaming directory builder is
part of merging. Reject unqualified layouts (usize != 64 bits or Row > 96 bytes).
Before scanning rows, bound each side to floor(256 KiB / 41) rows. Bound combined
key and value lengths to 512 KiB. The caller must separately authenticate each
input block's encoded length <= 256 KiB; larger blocks require singleton phases.

Reserve 64 KiB + (N_source + N_current) * sizeof(Row) + sum(input key/value lengths)
with checked arithmetic before cloning or validation that can allocate errors.
Fresh key/value clones allocate their lengths; the one preallocated row vector
never grows. The fixed allowance covers existing validator errors and allocation
counter bookkeeping. Input guards remain live through the existing shared
allocation closure and output retains its WorkingValue reservation. Admission
failure, malformed input, or overflow stops the route. Ordinary ownership and
final cumulative carry limits remain in force; this is not whole-invocation or
backend scratch qualification.

Retain a compiled behavioral red, independent small-state map oracle for both
modes, binary/empty values, all missing/visible/tombstone combinations, generation
and ordinal overflow, wrong kinds/sequences/order, bounded-allocation observations,
low-budget failure and sticky stop. Native advancement stays disabled until unit
durability, independent coverage, singleton phases and final publication pass.

Amendment 1: the original both-input-sequences ordering premise was incorrect.
The frozen logical codec uses current/base + 1 in present mode, without taking a
maximum with source sequence. Retain the original contract, first green run and
compiled failing newer-source regression as diagnostics. Re-run affected evidence
after binding explicit source/base sequences and deriving result sequence.

Amendment 2: path-copy roots reuse older physical blocks. Plan7 manifest sequences
are upper bounds, not exact row/descriptor sequences. The native caller must
authenticate the exact selected descriptor and supply its block sequence along
with the Plan7 source/base bounds. Retain the compiled older-block regression.
This primitive accepts at most one decoded block or contiguous fragment per side;
it must not concatenate blocks with different sequences. Empty-side directory
coverage remains an executor/final-verifier obligation.

Admission preflight returns allocation-free success/failure. On failure, reserve
the same 64 KiB fixed term before constructing the typed capacity error; check
input ownership inside this guard too. Keep admitted failure allocation charged
through stopped cleanup. A guard admission failure itself follows the existing
shared guard/service error invoice; it cannot execute the merge closure.
