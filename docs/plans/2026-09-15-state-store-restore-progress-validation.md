# Step 3: admitted structural progress and selector validation

Continue from receipt-validation checkpoint
0181cc8a3e80b9216a481a199864d681856d51dd1254cadd03774692526676af,
base42dd18119ec6708c7dfafa15ced0842093652f94 and Rust1.88. Preserve all wire
formats, ceilings and the disabled native advance boundary.

Add two closed private codec entries accepting current physical IO/route,
a same-ledger WorkingValue<ExpectedPlan> and a same-ledger progress or selector
model. Return WorkingValue<()> retaining temporary structural allocations;
every failure stops the same route. No caller-provided size/hash witnesses.

For progress, reserve64KiB for owner,64-bit/fixed-layout and depth8 cursor
preflight, then count compact serialized P from that exact immutable typed model
using the existing capped4MiB counting codec. Calculate with checked arithmetic:
R = 4P + 4*(expected.prefix.len()+128) + 64KiB.
This preserves the qualified at-most-two visits per stored binary field and
base64 cost below2L+8, with18 calls and the one non-genesis receipt-path format
covered separately. The same typed compact-count argument is recorded in
unit-native-structural-allocation-compact-count-addendum-03.md,
SHAbfb36cb94ce093a83ad801ab85492dfffaa9468864dd7319a28858b71e070dbd.

Only for next_ordinal0, compute genesis-none and genesis-chain with the sealed
admitted hash functions on the progress guard. Hold both hash guards until the
structural pass completes. Envelope equality with ExpectedPlan is required
before accepting the computed chain; thus using progress projection fields
cannot authenticate a different plan/identity/owner. Non-genesis performs no
hashes. Extract a shared structural core receiving an optional precomputed
chain, require exactly Some for genesis and None otherwise, and preserve all
existing envelope, digest spelling, cursor, singleton, last-receipt path,
terminal and frozen-genesis semantics. The legacy adapter retains its own
expected-plan genesis computation. The core performs no JCS or hashing and
copies no model/tail state. Its only dynamic path is the existing receipt path.

For selector, reserve64KiB before owner,64-bit/fixed-layout checks and the
existing structural validator. Its closed graph has only field comparisons,
lexical digest checks and bounded static errors; no binary decode, dynamic path,
hash, clone or JCS operation. The progress path remains uninterpreted until the
separate selector/current binding check; selector shape alone establishes no
selected-progress authority.

Retain compiled behavioral red before native admission: foreign expected/model
owners for both entries, missing progress hash reservation and insufficient
selector fixed reservation. Then verify legacy parity for genesis/non-genesis,
terminal, envelope/hash/reference/path/cursor/singleton/count mutations; exact
genesis witness cardinality; depth8 large-key progress, depth9/count-cap rejects;
both routes and final carry, hash retention on late failure, zero underestimates
and guard release. Independently measure structural bytes against the formula.
Run full feature/default suites, strictClippy, docs,formatting,layout, complete
source continuation and a fresh component audit before acceptance.

This admits typed structure only. Exact raw identity, storage/version/path
binding across records, direct predecessor validation, bounded tail ownership,
physical coverage, durable execution, final verification, publication capability
and whole-invocation ownership remain separate Step3 obligations. No readable
authority is minted and native advance stays disabled.
