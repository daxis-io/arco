# Native restore buffer-span admission

Frozen before the new regression and implementation. This supplements the Step 3
restore contract and final-stream admission without changing their work or memory
ceilings, authority-7 codecs, directory-v1 bytes, or production Arrow encoder.

The native authority-8 restore reader already selects eight primitive/Binary
fields, one V5 record batch, eight field nodes, and eighteen buffer descriptors.
Before FileReader can allocate its body, alignment copies, arrays, or rows, the
restore-specific borrowed preflight must additionally establish:

- Every positive-length buffer span is in the declared body and disjoint from
  every other positive-length buffer span. Empty buffers may share offsets.
- Buffer roles follow the existing eight-field schema: validity/value for each
  primitive field and validity/offsets/data for each Binary field. Physical
  offsets need not be sorted; semantic role order comes from the schema.
- The UInt8 value buffer holds at least N bytes; every UInt64 value buffer holds
  at least 8N bytes; Boolean values hold at least ceil(N/8) bytes; each Binary
  offset buffer holds at least 4(N+1) bytes. N is the positive bounded batch row
  count, subsequently required to equal the authenticated descriptor count.
- A present validity buffer, or one required by a positive null count, holds at
  least ceil(N/8) bytes. Existing null-count, node-length, compression, body-span,
  schema, dictionary, and metadata restrictions remain mandatory.

All arithmetic is checked. Invalid spans or minima return an invariant error
before owned Arrow decoding. The checks inspect at most eighteen descriptors;
no sort, new collection, codec, or transport is introduced. Production encoding
already emits disjoint buffers with these sizes. This new restriction applies
only to native synthetic restore admission.

The source review is retained as
`native-one-payload-cumulative-reservation-01.md`, SHA-256
`e69bd2463cee7abda35481f27fd29515296873f9ba377c0a71f896f216e788b3`.
Its summed overlapping-buffer estimate identifies a missing proof premise; it is
not a measured amplification result. Disjoint spans let the sum of alignment
inputs and copied Binary data be bounded by the body; the UInt64 minimum also
bounds row-vector cardinality in terms of authenticated encoded length.

This amendment does not freeze or claim the proposed 8 MiB fixed decoder term,
33P standard reservation, or 23P singleton reservation. Those still require
complete target/source validation, pre-allocation admission, response/carry
invoices, failed-operation evidence, and independent review. Native advance
remains disabled until the complete restore workflow is verified.
