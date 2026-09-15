# Bounded private unit-record decoding

2026-09-14. Additive Step 3 contract, frozen before implementation and measurement.
Preserve the existing exact JCS selector/progress/receipt wire shapes and their
4 MiB maximum. This component bounds the typed graph before materialization;
allocation admission, canonical encoding, durable native units and publication
remain separate required work. Native restore advance stays disabled.

Replace Serde's internally tagged Content-buffering path for GlobalCut,
SideCursor, SingletonState and UnitReservationV1 with decode-only ordinary
derived union structs and checked conversion. Keep the domain Serialize derives.
Serde's try_from dispatch must precede tagged enum dispatch in the locked source.
Accept kind before or after other fields. Reject unknown and duplicate fields,
unknown kinds, missing required fields and all fields outside the chosen kind.
Distinguish missing from present null. Pending singleton source/current require
explicit null or a witness; other singleton kinds reject those fields even when
null. Old direct Serde parsing defaulted missing optional fields, but the existing
exact-JCS comparison already rejected the missing canonical fields.

DirectoryPosition.path admits at most eight elements. Receipt source_inputs and
current_inputs each admit at most 16, outputs at most 32; later semantic checks
still enforce the combined input limit. Allocate fixed list capacity before
elements, then inspect the terminal slot with a Deserialize marker that rejects
without parsing an extra element. Exactly-full arrays remain valid. Do not
deserialize an extra typed witness, build a generic JSON Value, or require a
kind-first spelling. New helpers remain private to unit records.

Retain compiled behavioral red evidence for early list rejection and unknown
pre-kind member allocation. Verify all variants, tag order, duplicate/unknown
fields, null/missing distinctions, exact-limit lists, over-limit lists with an
invalid final typed value, and unchanged production-shaped JCS fixtures. Bind
the relevant locked Serde derive/visitor paths and final source in review.

This does not claim a complete allocation bound. A following codec admission
must charge raw ownership separately and reserve parsing, failure formatting,
JCS maps/vectors, escapes and typed owners before work. Raw backslash count E
has only E <= P, not E <= P/2. Valid quote-containing scopes remain supported.
All carried records must share the existing 64 MiB owner ledger and the separate
32 MiB/4096 control-I/O invoice. An expensive legal wire record may receive typed
backpressure; there is no new smaller wire cap or assumed unlimited scratch.
