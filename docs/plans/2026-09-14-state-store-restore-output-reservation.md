# One standard restore output block: allocation admission

2026-09-14. Additive Step3 contract, frozen before implementation and measurements.
Authority formats, Arrow/index bytes and existing request ceilings remain unchanged.
This private synthetic component calls production encode_arrow_block exactly once.
It does not call encode_segment or split/re-encode after observing output size.

For borrowed input rows compute checked N, total key length K and total present
value length V. Let Q=ceil(N/8), R=round64(Q). Require N>0, usize::BITS==64,
and the fixed production eight-field, V5, no-compression, one-batch grammar.
Before the first Arrow allocation, calculate:

```
D = K + V + 41N + 8 + 9Q + 18*63
P_bound = D + 49,376
A = 3 MiB + 8K + 8V + 328N + 48Q + 8R
```

Require P_bound<=256KiB and reserve A against the existing minimum of live
ownership and, on an explicit final step, cumulative allocation plus initial
carry. All arithmetic is checked. Oversized/empty/overflow/unqualified inputs
return sticky typed backpressure before the encoder callback or I/O. Actual
output must not exceed P_bound. The returned Vec and its backing capacity stay
under the allocation guard; input rows and storage responses are separately owned.
Observed cumulative allocations are an overrun witness, never a replacement
for pre-admission. Panic, any underestimate or poisoned ledger is nonpassing.
Encoder work must be counted separately from decoder work while both contribute
to final-step cumulative allocation. No work invoice may be reset by dropping
an earlier output in the same step.

The source derivation includes default Arrow builders and their geometric
growth, 18 aligned IPC body buffers, eight possible all-valid bitmap buffers,
private arrow_data, final output Vec, three FlatBuffer builders, schema and
batch message copies, footer and fixed errors. The body includes Boolean data
and all eight V5 validity maps, hence 9Q. Metadata has no row/key/value bytes.
Each fixed schema, batch-message and footer document is bounded by16KiB.
The framing upper bound is 3*16KiB+224=49,376 bytes. The schema/footer grammar
has18 table instances. The two copied documents are charged at32KiB combined.
The2MiB fixed allocation term plus1MiB final-output growth term in A retains
at least1.375MiB fixed slack after conservative metadata/side-vector/default-
builder charges. It is source-derived, not a sampled heap maximum.

Qualification binds Rust1.88.0, locked Arrow54.3.1, FlatBuffers24.12.23 and
all source trees through the existing source-drift manifest, plus the exact
encoder caller. A compiled framing check must verify exact schema field names,
types/nullability, V5/no compression, one batch, eight nodes, eighteen buffers,
and each metadata document<=16KiB. Source cardinality supplies the universal
bound; boundary/null-pattern allocation tests supply drift and overrun evidence.
Retain compiled low-admission and over-envelope failures before implementation,
exact production-byte parity and decode parity for admitted rows, boundary
N/K/V cases, both null patterns, and final carry exhaustion.

Reviewed pre-change inputs:
- native-output-filewriter-cumulative-bound-01.md:
  aa554b2b05e5b609d963e6f3f4652c102a83fe2c84d9ff54097f8a4cdbec7910
- native-output-filewriter-qualification-02.md:
  501f799106bc5e2eb6f6bcd4cea3f6cd03a37c63483916cbeedf9c0b1addde71
- native-output-filewriter-cumulative-bound-review-01.md:
  674540a908a74d793af25c6c431119df1f1963632b87c42f10716a6e8e05bd65
- directory component source:
  d3932f57d5cc8fc2ab73a60f59b0e681b9a28cf55090d171b35a750eb911ed2c

Index/Bloom/descriptor serialization, writes/collisions/returned metadata,
directory output, singleton phases, durable merge/progress, final coverage,
publication/recovery and full scale evidence remain required. This component
cannot publish authority or enable supports_bounded_restore_advance. The future
window driver must select a smaller row prefix or its explicit singleton phase
before calling this standard-only helper; no fallback widens its admission.

Amendment01, before behavioral red or encoder implementation: the independent
source correction itemizes640KiB of fixed allocations, leaving1.375MiB in the
2MiB fixed reserve. This replaces the earlier1.5MiB slack statement only;
D, P_bound, A, phase limits, and acceptance behavior are unchanged. Original
contract SHA256:3b52b16cdb948bf2eb99022cbd52ef8590fcf126c41df222dba05662622a6cef.
Correction source:native-output-filewriter-cumulative-bound-correction-03.md,
SHA256:2075d61446827fa7b46613efdcb17082b224b2a0447329bcfdbed4f2f81279eb.
