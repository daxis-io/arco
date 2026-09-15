# Step 3: admitted unit-record decoding and canonical comparison

This component extends the frozen Plan7 typed-decoding contract. It does not
enable native restore, authenticate a selected record, change wire versions,
or qualify outgoing record construction, semantic hash passes, or publication.
The source base remains 42dd18119ec6708c7dfafa15ced0842093652f94 plus the retained
Step 3 delta. Rust 1.88, locked dependencies, and the separately owned sequential
Cargo queue and evidence directory remain required.

## Closed codec and ownership

Only selector, progress, and receipt concrete types enter the new private codec.
Raw bytes must retain their classified response owner from the same physical-I/O
ledger throughout decoding. The result retains its working-memory owner across
awaits and drops. No generic JSON tree or unbounded `jcs` helper participates.
The existing typed wire visitors and bounded lists remain unchanged.

For raw size P in 1..=4 MiB, let E count every raw backslash byte (E <= P).
Count P/E without allocating. Before parsing, reserve, with checked arithmetic:

    max(60*P + 512 KiB, 29*P + 16*E + 2 MiB)

Raw response ownership is charged separately. A missing/overflowed bound uses
an explicit fixed 64 KiB rejection reservation; it never selects observational
remaining-budget admission. Insufficient ownership budget rejects before the
parser runs. Every failure stops the route; subsequent native work cannot run.

The failure branch retains the pinned metadata JSON allocation proof, including
dynamic Serde errors and float_roundtrip numeric scratch. The success branch
includes 4P typed-string cumulative growth, 4P parser scratch, 20P cumulative
nested JCS key/value growth through at most five object levels, P fixed output,
and 16E boxed formatter scopes. The fixed 2 MiB must cover the entire finite
typed graph, JCS B-tree nodes/scopes, vector minimum growth and formatter stack,
canonical omission slack, and static error state. Qualification must bind the
source derivation and compiled target layouts, not just successful measurements.
The codec fails closed outside a 64-bit target or when the fixed typed backing
bound (32 inputs, 32 outputs, 32 path entries, four singleton boxes) exceeds
64 KiB. The parser artifact must retain the qualified serde_json feature set.

Canonical serialization uses the existing serde_jcs writer with a fixed
`Cursor<&mut [u8]>` of P+32 bytes. This permits the only omitted nullable field,
progress.last_receipt (at most 20 additional bytes), to serialize before exact
comparison rejects its omission. The writer cannot grow. Serializer and parser
errors map to static catalog messages without formatting third-party errors.
Exact canonical length and byte equality are required. A parsed record confers
no digest, selected-plan, coverage, chain, or publication authority.

## Behavioral and source evidence

Retain compiled behavioral red evidence before implementation, then verify all
three production-shaped record families on ordinary and final routes. Cover
same-owner retention and foreign-owner rejection, insufficient budget before
parsing, exhausted final carry, exact JCS equality, duplicate/unknown fields,
omitted last_receipt, fixed-writer exhaustion, maximum lists/paths/boxes,
numeric/escaped failures, and legal strings containing quotes and backslashes.
Heavily escaped records remain legal wire values; reservation backpressure is
not a smaller wire cap. No quote restriction is introduced.

Count actual cumulative allocations and retain failed operations. Qualify fixed
typed layouts and pinned JCS B-tree/Box/formatter cardinality. Zero observed
underestimates is necessary but cannot replace the source proof. Retain all
earlier negative evidence and any amended proof before affected reruns. Run
affected tests, default and test-utils catalog lanes, strict Clippy, formatting,
docs, source guards, and a fresh component review before sealing this component.

Native enablement, outgoing admitted encoding/hashing, the complete invocation
owner, durable receipt/progress/selector driver, terminal independent coverage,
fences, prepared candidate, and one HEAD publication remain separately gated.
