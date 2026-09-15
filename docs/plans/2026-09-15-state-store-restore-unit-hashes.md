# Step 3: admitted unit digest passes

Continue from outgoing encoder checkpoint
31cf4424af54b4b1793d70b5339209d677838d3c5cdd41ddff037b78d7b7102a.
The base, Rust 1.88, dependencies, wire versions, and existing admission ceilings
remain frozen. Native restore remains disabled.

Add closed private hashing entry points for the receipt body and chain, genesis
none and genesis chain, and exact incoming or encoded unit bytes. Borrow receipt
or progress fields from an existing same-ledger WorkingValue. The genesis chain
also borrows its previously computed, same-ledger genesis-none digest. These
functions compute identities; they do not authenticate a selected Plan, validate
the digest supplied to a chain body, or grant readable/publication authority.
Receipt body excludes its body and chain digest fields. Chain body includes the
supplied body digest. Genesis none commits to plan digest, identity, and owner
generation; genesis chain additionally commits to the supplied genesis digest.
Preserve all legacy hash helpers as compatibility oracles.

Before counting, reject foreign ownership, unqualified layout, or model shapes
beyond the existing path/list bounds under an explicit 64 KiB reservation. Count
each borrowed projection through the already qualified compact writer. Let P be
its exact encoded length and E its raw backslash count, with 0 < P <= 4 MiB and
E <= P. Reserve checked 21P + 16E + 2 MiB + 71 before fixed JCS serialization.
The projection graphs are subsets of the qualified receipt/progress graph;
genesis chain adds one scalar digest while remaining smaller than progress.
Require exact P and E parity after serialization. The 71-byte term is the sole
result String allocation: `sha256:` plus 64 lower-case hexadecimal characters.
Use stack SHA-256 state and a fixed hexadecimal alphabet, without format! or a
second encoded heap string. Do not materialize tag concatenations.

Tagged hashes consume exactly tag || one NUL byte || exact JCS bytes. Raw hashes
consume only the supplied bytes, with the same owner and 1..4 MiB wire cap and
an explicit 64 KiB allocation reservation. Raw identity remains distinct from a
semantic body or chain identity. Arbitrary raw bytes are not parsed or normalized.

Record hash operations and exact input bytes in the physical request work
ledger. Check counter arithmetic inside admission before the hashing closure.
Commit counters only when the closure completes its hash, including when final
allocation accounting later rejects the result. A pre-hash serialization or
admission rejection performs no hash and records none. All errors stop the route;
retry cannot perform another pass. These counters cover these new passes only;
complete invocation and existing descriptor/payload hash accounting remain open.
Count and hash allocation operations reuse the existing encoding allocation
wrapper, including final-stream cumulative allocation and carry accounting.
The digest remains a WorkingValue through awaits and cancellation.

Retain compiled behavioral red evidence for missing admission, owner rejection,
and shape rejection. Verify all four tagged projections against legacy and
independent tag/JCS vectors, exact raw parity on both physical routes, field
tampering, hash domain separation, maximum shape and escape cases, P/E parity
failure before hashing, wire limits, insufficient memory/carry, counter overflow,
sticky failure, and digest lifetime. Bind SHA/digest/hex implementation sources
and qualification to the compiled source. Preserve all failures and amendments.
Run affected and full catalog suites, strict Clippy, docs, formatting, source
guards, hygiene including new files, and independent component review before
claiming this component complete. Durable units, coverage, and publication are
subsequent work under the existing Step 3 contract.

Amendment 1, before the final affected measurements: hash work is recorded only
in the physical request ledger. Remove the redundant legacy test-utils global
counter call; insertion into its persistent phase B-tree lacks a source bound
within this component's fixed allocation term. Native invocation reporting must
consume the physical hash counters and must not use legacy global counters alone
as evidence for these passes. The prior tests remain diagnostics, not final
allocation qualification. No wire identity or admission ceiling changes.
