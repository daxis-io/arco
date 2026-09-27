# Step 3: admitted output identity

Continue from expected-plan checkpoint
3a8a5286bd12d3e634c4875e90a4be6deaf7c88da73bd185cfb5871bb325c502.
Rust 1.88, dependencies, wire identities and admission ceilings remain fixed.
Native advance remains disabled.

Add one private fifth tagged hash entry in units/codec/hashes.rs. It accepts
only the current physical I/O/route, a borrowed same-ledger
WorkingValue<ExpectedPlan>, ordinal u64, part u32 and physical Role. It hashes
exact OutputIdBody (candidate seed, ordinal, part, role), using the existing
restore-output-v1 domain followed by one NUL and exact JCS. Return the existing
wire identity: 64 lowercase hexadecimal characters without the sha256 prefix.
All three roles and complete integer ranges retain legacy hash behavior;
legal output placement remains a separate structural/coverage validation.

Reuse the existing admitted compact count, fixed JCS and SHA implementation.
Under the existing 64 KiB preflight reject foreign ownership and a malformed
candidate seed (sha256: plus 64 lowercase hexadecimal bytes). Compute that
shape predicate without allocation. This projection has one fixed digest and
three scalar fields and is a strict subset of the qualified typed graph.
Reserve the existing checked 21P + 16E + 2 MiB + 71 before JCS and hashing.
Keep the sole 71-byte result allocation; remove its seven-byte prefix in place
before returning the WorkingValue. Do not allocate a second hexadecimal string
or concatenate the tag. Specialize only the private tagged helper for the two
existing wire presentations; retain all four prefixed tagged paths and raw
hashes unchanged in behavior.

The physical ledger records one hash and exact tag+NUL+JCS input bytes.
Native captured counters must not double-count it. Admission, ownership and
shape failures perform no hash and stop the route. Retain the result charge
through use and release only on drop; final carry remains cumulative.

Before implementation retain compiled behavioral red evidence for missing
reservation and foreign-owner rejection. Verify independent exact output
vectors and legacy parity across roles, ordinal/part extrema, seed changes,
malformed seed, both routes, exact counters, result width/capacity/release,
zero underestimates, and existing hash family regressions. Rebind source and
compiled dependencies, final checks and fresh component review. This identity
computation does not validate output existence, coverage, receipts, candidate
publication or whole-invocation ownership.
