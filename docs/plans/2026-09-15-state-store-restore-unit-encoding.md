# Step 3: admitted outgoing unit records

This component follows the admitted decoder checkpoint with source fingerprint
2daf786f0c83ce644a94d166f7413f0531da812273cda6bc544566dba4298f69.
The pinned base, Rust 1.88 toolchain, locked dependencies, sequential Cargo queue,
ownership limits, and Plan7 wire versions remain unchanged. Native restore stays
disabled. Record construction, semantic body/chain hashes, complete invocation
accounting, and selected-plan/publication authority are separate work.

## Closed encoding and admission

The private codec accepts only guarded selector, progress, or receipt models.
Their `WorkingValue` owner must be the same physical-I/O ledger used for encoding.
Before serialization, check the finite shape: each path has at most eight entries,
receipt inputs at most sixteen per side, and receipt outputs at most thirty-two.
Enum variants already bound the remaining graph. These are structural admission
checks, not semantic receipt or selected-plan validation.

Run a compact-JSON counting writer under an explicit 64 KiB reservation. It owns
only checked counters for encoded length P and raw backslash count E. It retains
no output bytes. The writer stops at the existing 4 MiB record ceiling, and maps
size/arithmetic or serialization failure to fixed errors. A foreign owner or
invalid shape is rejected before the serializer runs. No generic JSON value,
unbounded JCS helper, custom serializer, or unowned model enters this path.

For the frozen graph of static ASCII field names, enums, strings, booleans, u32,
and u64 values, compact JSON and the pinned JCS writer have equal length and
raw backslash count. Member ordering does not change either count. The source
qualification must establish this premise and the fixed counting/error cost.

Before canonical serialization, reserve with checked arithmetic:

    21*P + 16*E + 2 MiB + 24

This is the qualified decoder's successful JCS bound with the two 4P parser
terms removed. It retains P fixed output, 20P cumulative nested JCS vectors,
16E boxed formatter scopes, and the conservative 2 MiB fixed graph/error term.
Already-owned model storage remains charged separately. Checked overflow or a
missing bound takes an explicit fixed rejection reservation; there is no
observational remaining-budget fallback. Insufficient budget rejects before JCS.

Serialize into a fixed slice-backed cursor of exactly P bytes and require its
final length and raw backslash count to equal the preflight counts. Move that
vector into `Bytes` and
perform its first clone inside the same reservation, so any shared-header
allocation is measured and retained. Subsequent transport clones may not create
unowned sharing metadata. Return `WorkingValue<Bytes>` without releasing its
owner; this is the existing conditional control-record writer's input type.
All failures stop the route. Final-stream work retains cumulative allocations
and carry accounting; preflight and canonical encoding each record their work.

## Required evidence

Retain compiled behavioral red evidence before the change. Verify exact output
parity for all three production-shaped families on ordinary and final routes,
including real quote-containing scopes and the maximum receipt graph. Exercise
foreign owners, invalid paths/lists, insufficient working memory, final carry,
wire-cap rejection, integer endpoints, escape forms, and fixed output exhaustion.
Verify that the returned bytes retain their owner across awaits and cancellation,
and that a later `Bytes` clone allocates zero bytes. Compose the result with the
classified immutable record writer and selector CAS, then read and decode the
published bytes through the admitted codec. A stored record alone is no authority.

Bind source and compiled feature/layout evidence for both serializers, the
counter's fixed error path, the shared allocation wrapper, and the first-clone
behavior. Observations cannot replace the source bound. Run affected tests,
catalog suites with and without test-utils, strict Clippy, docs, formatting,
source guards, hygiene including new files, and independent component review.
Preserve diagnostics and substantive amendments before affected reruns.

Amendment 1, before implementation or green measurement: assign the first
`Bytes` sharing header a separate 24-byte term, rather than consuming unused
fixed-term slack. Check both P and E after JCS. The initial three compiled red
regressions exercised the unbounded baseline and remain applicable.
