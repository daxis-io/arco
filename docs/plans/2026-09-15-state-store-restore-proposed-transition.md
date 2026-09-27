# Step 3: admitted proposed transition binding

Continue from selected-progress checkpoint SHA256
939a9789ac9c539538b4155573bc13a5eaf46232a51575f567d82512a4b95058.
Base42dd18119ec6708c7dfafa15ced0842093652f94; Rust1.88 locked offline,
incremental0 and debug0. Native advance remains disabled.

Add one private admitted validator taking IO/route, guarded ExpectedPlan,
SelectedProgress, guarded proposed receipt and guarded proposed progress.
Before hashing or encoding, reserve64KiB and check all five retained owners
(expected, selected raw/meta/progress, and both proposed models), 64-bit layout,
selected plan envelope, nonterminal selected progress and checked next ordinal.
A selected bundle is only usable with its matching expected plan, even on the
same ledger. Validate both proposed shapes with existing admitted codecs.
Compute genesis predecessor only for selected genesis using the existing
admitted hash. Encode the exact proposed receipt with the existing admitted
codec and hash its bytes internally; accept no caller-supplied raw digest.
Under64KiB compare receipt ordinal, before cursor, singleton-before, prefix
counts, predecessor raw digest and predecessor chain with selected progress.
Reuse the selected direct-edge core for proposed progress, receipt, and rawhash;
shape validation already authenticates the canonical last-receipt path.
Return only a guarded unit result, not publication authority. Re-encoding is
intentional until a durable writer owns and retains the encoded records.

No I/O, new path construction, cloning, chain scan, output verification or CAS.
Existing qualified codecs own payload-dependent allocations. Fixed new glue
reservations are64KiB. Every failure stops the route and preserves accounting;
retry after failure performs no additional hash or I/O. Preserve old validators
and all wire bytes. A compiled semantic regression for disconnected but valid
records precedes the binding implementation. Verify both routes, valid and
altered predecessor/raw/chain/cursor/count edges, owner rejection, terminal and
maxordinal rejection, allocation admission and no I/O; full relevant checks
and independent audit precede component acceptance. Complete merge coverage,
durable writes, selector CAS/reconciliation and whole-invocation qualification
remain separate obligations.
