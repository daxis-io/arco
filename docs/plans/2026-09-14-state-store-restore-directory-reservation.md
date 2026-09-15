# Restore directory allocation admission

2026-09-14. Additive Step 3 contract, frozen before implementation.
The authority formats, directory-v1 bytes, and existing request ceilings stay fixed.

For native restore traversal, replace observational all-remaining decoder
reservations with these checked cumulative requested-allocation bounds:

- Directory construction and physical directory path declaration: 16 KiB +
  32 times the sum of the byte lengths of the three StateScope strings and
  the storage tenant, physical workspace or metastore ID, and workspace context.
- Page decoding: 16 KiB + 128 * size_of::<Node>().
- Fence/order validation and empty results: 16 KiB.
- Returned position: 16 KiB + first fence length + last fence length +
  actual path depth * size_of::<([u8; 32], u32)>().

Require usize::BITS == 64, size_of::<Node>() <= 256, path depth <= 8,
and the existing page/fence bounds. All arithmetic is checked. Failure is
sticky typed backpressure before executing the admitted closure or further I/O.
Reservations replace, rather than add to, the shared decoder reservation and
use its exact live/cumulative allowance and initial carry. Observed allocation
is the overrun witness; panic or any underestimate remains nonpassing.

The pinned source uses Vec::with_capacity(count) for decode_page after checking
count <= 128 and exact encoded length. Each Node is Copy, its two KeyRef values
are inline, and no decoded key allocates. Digest and all page/fence checks use
stack hash state, borrowed slices, and fixed error strings. The result copies
each endpoint once with to_vec and copies exactly the selected fixed path.
There is no sorting, recursive allocation, or duplicate key cache in this reader.
The 16 KiB fixed term covers fixed errors and decoder bookkeeping; it is not
a measured maximum. Scope/path construction adds storage and StateScope String
clones, fixed prefixes, one 64-byte hex digest, and Rust String formatting.
The variable coefficient covers cumulative geometric String growth and the
invalid-path error conversion, including the rendered path. No arbitrary
backend error is admitted by this bound: responses retain separate accounting.

The ordinary metadata invoice, final stream operation/read-byte invoice,
classified response ownership, and all carried values remain separately charged.
This component does not admit descriptor/index JSON or output encoding, enable
native restore advance, add singleton phases, or prove a whole restore invocation.
The exact-source red/green tests, maximum page/endpoint cases, and independent
review are required before accepting this component.
