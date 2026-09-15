# Step 3: bound selected progress reads

Continue from progress-validation checkpoint
00e4216e237f3b257727e82ea52999ddd4a3b2c4d57327f51ef1835a6f8fc547,
base42dd18119ec6708c7dfafa15ced0842093652f94, Rust1.88. Keep native advance off.

One private async reader accepts only current IO/route and same-ledger guarded
ExpectedPlan. Preflight owner/64-bit/fixed returned-layout under64KiB before any
I/O. Read exactly the expected candidate's Selector using the existing bounded
HEAD/range/HEAD helper. Missing selector is an error, never genesis; initialization
remains separate. Decode exact JCS and perform admitted selector validation.

Within64KiB admission, parse the selector path using borrowed strips against
ExpectedPlan.prefix followed by /progress/, exactly20 ASCII digits and .json;
parse checked u64. Reject alternate paths before reading them. The closed record
enum constructs the actual read path. Decode that Progress object and require
its next_ordinal to equal the requested ordinal. Compute admitted raw SHA256 and
validate admitted progress shape; compare the exact raw hash to selector digest.

Only non-genesis reads Receipt(next_ordinal-1). Decode exact JCS, run admitted
receipt validation, and hash exact raw bytes using the sealed raw hash helper.
A hash-free direct-edge core under64KiB requires exact receipt cardinality,
checked ordinal+1, raw last-reference digest, chain, cursor, singleton, and
checked cumulative counts equality. Progress shape already checked canonical
last receipt path; the actual receipt locator comes only from the closed enum.
No caller-supplied hashes, paths, models, ordinals or raw records reach this API.
No full receipt history or source/output coverage is implied by one local edge.

Return a private non-Clone stack bundle with the original accounted selector raw
bytes and metadata, plus the guarded validated progress. Retain selector version
for future exact CAS; returning this bundle does not publish or permit CAS by
itself. Drop raw progress, its metadata, decoded selector, receipt and temporary
hash/validation guards as soon as their checks are complete. No response/model
clone, heap list, chain collection or path-building outside existing admission.
The exact stored selector version remains the selection witness if it changes
later; current progress/receipt bytes must be treated as immutable selected
objects, and a later CAS must use this original selector version.

Read ceilings: genesis2 records, non-genesis3; each record uses2metadataHEADs and
1boundedrange at most4MiB+1; no ancestor scan. Existing whole-invocation control,
request and final-chunk admission apply and may reject individually valid large
records. New glue uses only64KiB preflight/parse/compare reservations; nested
codecs and hashes retain their separate qualified payload-dependent bounds.
Every error stops the route, retains failed work, and never implies retry-safe
publication. Cancellation preserves existing I/O ownership/stop behavior.

Compiled behavioral red for wrong selector digest and a disconnected direct
receipt precedes enforcement. Verify valid genesis/non-genesis and maximal
ordinal, malformed/noncanonical path, different expected owner, missing selected
record, changed raw bytes and semantic edge, both routes, read/hash counters,
stopped retry, cancellation ownership and retained selector bytes/version.
Full suites, strict checks, source continuation and fresh scoped audit before
component acceptance. Wire formats and old validators stay unchanged.

This binds exact stored records to an already selected expected plan and the
observed selector version only. It establishes no unseen prefix, physical merge
coverage, output existence, durable advancement, final seal, publication,
whole-service qualification or production support.
