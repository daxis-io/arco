# Step 3 logical-history accounting foundation

Continue from standard-coverage commit 014249e2. The existing canonical encoder
now records one hash stream when created and input bytes at each update, so a
failed or cancelled stream cannot erase already performed hash work. Successful
logical wire bytes, golden vectors and aggregate hash counts remain unchanged.

Restore history allocates one previous-key buffer at construction using the
existing 256 KiB directory key ceiling. Accepted tuples reuse that capacity.
The accepted mutation count distinguishes the first empty key from a repeated
empty key. Oversized keys, unordered tuples and generation/count mismatches
poison the stream before accepting further mutations.

Compiled behavioral regressions demonstrate the earlier missing partial hash
accounting and per-row key allocation. Boundary tests cover empty keys,
duplicate empty keys, exact maximum keys, oversized keys, retained capacity
reuse, and unchanged logical vectors. The full feature suite passes 955 tests
with two ignored before the documentation source seal; final checks are bound
separately in the evidence directory.

This foundation does not yet place the history object under the final invocation
owner or integrate verified output rows and directory leaves. Charged assembly,
singleton phases, durable publication/reconciliation, fences, whole-invocation
ownership and the large actual workflow remain Step3 work. Native automatic
advancement remains disabled.
