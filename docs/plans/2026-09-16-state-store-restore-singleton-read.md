# Step 3 one-payload singleton physical reader

Base 0bd8c6d5. Reuse descriptor/index authentication and version-fenced Arrow
payload reads. The private reader requires a same-owner leaf, authority 8, a
fresh ordinary payload invocation and the original 64 MiB ownership policy.
Directory membership remains the caller's separate proof.

Authenticate descriptor and index under the standard metadata budget, require
one KV row, then admit checked scratch of 64 MiB + 24P for the authenticated
encoded block length P (at most 64 MiB). The enclosing mutable borrow immediately
consumes that allowance for the selected descriptor. Global physical-owner
counters prevent a fresh route from forgetting earlier payload reads or writes.
A consumed singleton allowance cannot read another payload or publish ordinary
payload output. Standard final microchunks retain their existing limit.

The returned descriptor and decoded row keep the existing ownership guards.
Metadata requests still use the original workspace control budget; shared/cache
pool limits do not grow. Cancellation or failure stops the physical owner.
This is read admission, not rooted absence, receipt phase execution, singleton
output encoding, independent final verification or publication authority.
Native advancement remains disabled.

Fresh compiled behavioral red precedes admission and owner enforcement. Tests
read two 40 MiB values in separate invocations, find the production codec's
maximum writer-valid value for a fixed key and reject the next byte, traverse
all nine physical cancellation boundaries, and reject foreign owners, final
routes, prior reads/writes, changed indexes/objects, non-KV/multirow descriptors,
and a second same-length payload under a fresh route. Peak ownership and native
work counters are checked separately from fixture construction.

The prior /private/tmp/arco-state-integration-restore-20260913-01 evidence directory
was unavailable at this session's refresh; its location is unknown. Earlier
results remain historical. Fresh commands, failures, source manifests and
measurements are retained in /private/tmp/arco-step3-singleton-20260916-01.
