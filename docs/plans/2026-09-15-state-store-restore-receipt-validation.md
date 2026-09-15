# Step 3: admitted structural receipt validation

Continue from output-ID checkpoint 988d52adc8eb9f3cd649e2d257a457d87b2b102f6a89e84be365d91f7a87fae2, base 42dd18119ec6708c7dfafa15ced0842093652f94 and Rust 1.88.
Keep native advance disabled and all wire formats/ceilings unchanged.

Add a closed private codec validation entry accepting only current physical IO,
route, same-ledger WorkingValue<ExpectedPlan> and WorkingValue<Receipt>. Compute
exact compact encoded size P from that very typed receipt under the existing
64KiB admitted preflight; do not accept an independent caller size or raw/model
pair. Preflight checks same owner, 64-bit/fixed layout, 16/16/32 list lengths and
8-level cursor paths before visiting strings. P is capped at4MiB by compact count.

Derive body and chain hashes with existing admitted functions. Derive each of
at most32 output IDs with the admitted output-ID function; retain all results
in a fixed stack array of optional WorkingValue<String>. No uncharged Vec or
second string copy. Preserve each guard's own charge through the structural
pass. All hash work, including later failures, stays in physical counters.

Extract one reusable typed receipt core and output-list core so both legacy and
native wrappers use identical envelope, digest spelling/equality, cursor,
singleton, leaf/block endpoint, strict ordering, count and reservation rules.
Legacy wrapper still computes legacy hashes. Native core accepts precomputed
hashes and exact output-ID list, and must never call JCS, generic tagged hash,
raw hashing, path builders, or clone owned receipt/tail state. Reject missing,
extra or swapped IDs. Native caller controls all computed hashes; no public
caller-supplied hash witnesses can bypass computation.

Run typed structural core inside checked R=6P+64KiB decode reservation. Source
census: at most302 base64 calls; each stored field at most3 visits; each binary
call <2L+8. Record payloads and prior guards remain charged independently.
Return WorkingValue<()> for temporary cumulative validation allocation, with
same-route sticky failure. Success means structural consistency only: physical
source authority, output existence, complete coverage, direct predecessor,
selector version and publication remain subsequent validation obligations.

Before implementation retain compiled red tests for missing admission and
foreign receipt/expected owners. Verify legacy parity on existing valid/invalid
models, envelope/body/chain/output-ID tampering, all32 outputs and16combined
inputs, deep cursor/large key/error cases, both routes, allocation bounds and
zero underestimates, retained hash work on semantic failure, and guard release.
Run full suites/strictchecks, source continuation proofs, and fresh audit before
component acceptance. Source qualification02 is superseded for size binding by compact-count
addendum03, SHA bfb36cb94ce093a83ad801ab85492dfffaa9468864dd7319a28858b71e070dbd.
It confirms direct typed compact counting is sufficient; no independent raw
length or raw/typed bundle is introduced for structural allocation sizing.
