# Restore descriptor and index allocation admission

2026-09-14. Additive Step 3 contract, frozen before implementation and measurements.
This freezes candidate bounds and their required qualification; it does not declare
qualification complete. Authority formats, physical codecs, and request limits stay fixed.

Use the existing shared allocation guard before descriptor/index checksum, JSON
parsing, or binding validation. For the exact authenticated encoded length, reserve
cumulative requested allocation, including allocations freed before return:

- Descriptor: 512 KiB + 60P + 2F + 4 KiB, where 0 < P <= 1 MiB is the
  descriptor byte length and F is the sum of the owning leaf endpoint lengths.
- Index: 8 MiB + 80Q + 128 KiB, where 0 < Q <= 512 KiB is the index byte length.

Require 64-bit usize, Descriptor <= 520 bytes, ControlMvpSegmentIndex <= 512 bytes,
and ControlMvpBlock <= 192 bytes. Check every arithmetic operation and size before
entering the allocation closure. Exhausted live or final cumulative allowance,
unsupported layout, overflow, cancellation, panic, or observed underestimate stops
the route. Oversized metadata remains corruption/backpressure under existing rules.
The encoded response, ObjectMeta, existing live objects, and initial external carry
remain independently charged against the combined remaining allowance. No nominal
64 MiB calculation permits ignoring those costs. A failed decoder retains its
actual error allocation through the existing guard. No trailing HEAD or payload
read follows rejected admission or decoding.

The source proof uses Rust 1.88 commit 6b00bc3880198600130e1cf62b8f8a93494488cc,
locked serde/serde_core 1.0.229, serde_json 1.0.151, and the production descriptors,
index validators and error conversion. RawVec cumulative growth is less than four
times final appended length plus its fixed initial minimum. Each retained String
uses the exact byte-slice copy. Rust Debug can expand raw JSON U+007F to six bytes;
two growing error Strings plus Box<str> may therefore cost 54 input bytes per
wrong-typed string byte. The descriptor coefficient includes parser scratch,
retained strings, endpoint hex decodes, and that error expansion. F is separate
because the caller's authenticated endpoints are copied by binding validation.
The 512 KiB term must cover all static error/formatter/parser bookkeeping, with
an exact-source enumeration and hostile small-input allocation qualification.

The index version-header and full-index parses each have a scratch vector. Ignored
JSON nesting uses a vector, not the ordinary recursive-depth ceiling. Successfully
appended blocks consume at least 50 encoded bytes even when Option fields are
omitted (the compact witness is 56 bytes). A hostile offsets vector may contain
Q/2 zero elements independently of block count. Parsed blocks and offsets use
cumulative growth bounds; validation temporaries are charged only after the
4096-block guard. Full-parse failure and successful validation are exclusive:
normal parse/validation is below 44Q plus fixed costs; parse-error accounting
partitions consumed bytes from the offending string and is below 58Q plus fixed
costs. The 80Q coefficient must not be justified by the withdrawn 36-byte error
coefficient. Keep exact runtime feature/cfg and source hashes in qualification.

Required regressions: low descriptor and index admission before their decoder and
trailing HEAD, ordinary/final routes and nonzero carry; exact production success;
raw DEL wrong-type strings around growth thresholds and near each metadata cap;
omitted-option minimal blocks, a near-cap offsets vector with one block, deeply
nested ignored JSON, maximum legal descriptor length/endpoints, and static error
and target layout qualification. Observed allocation must never exceed its bound.
Bind source/feature manifests and demonstrate source-drift rejection independently
for serde_core and Rust escape/printable sources before accepting the component.

This component leaves physical path declaration, output metadata/PUT accounting,
whole-invocation carry, durable merge, singleton phases, and restore publication
as integration work. Native restore advance remains disabled until the full
Step 3 contract and its final verification pass.

## Amendment 01: wrong-typed numbers

Before qualification, include wrong-typed long integers and fractions near the
metadata ceiling. Schema integer/string types do not exclude serde_json's
float parser on hostile input. Bind and enumerate the enabled float_roundtrip
lexical path and its scratch/Bigint allocations. Ignored numeric fields must
also be traced. The frozen formulas remain candidates pending this source and
behavioral evidence; an uncovered term requires an amendment before acceptance.

## Amendment 02: scratch for the offending escaped string

The parser can copy the offending string into its scratch vector after any
escape, in addition to formatting that string into the error. Add up to 4B
cumulative scratch for its B encoded bytes. This corrects the descriptor
parse-error source bound to 58P and the index full-parse error bound to 62Q.
The frozen 60P/80Q reservations are unchanged. Add repeated escaped sequences
near each cap alongside the existing leading-escape raw-DEL tests. Preserve
the earlier 54P/58Q derivations as superseded proof diagnostics.
