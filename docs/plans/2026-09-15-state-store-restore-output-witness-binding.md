# Step 3: bind receipt output witnesses to persisted output artifacts

Base0259b2b4dfcb08f59234ad4846b5c15a0fd762fd checkpoints all earlier work;
prior component root31a1a8bb48aa4f6160b219bf423cf0054218cf17342857bcdf2873e970c98b0a.
Rust1.88 locked/offline, incremental0/debug0; native advance remains disabled.

Add a private admitted validator for one guarded receipt OutputWitness and one
StandardRestoreOutput returned by the existing immutable three-object writer.
Require expected-plan, witness, descriptor and encoded-descriptor owners to match
IO before hash or path allocation. This closed product currently has exactly one
production constructor, the successful writer; no untrusted deserialized product
is accepted. Final independent verification must still reread actual objects.
Derive the deterministic output ID internally from expected plan, ordinal and
witness part, using the admitted hash helper. Hash the exact retained descriptor
bytes with the admitted raw encoder hash. Under admission, require descriptor
scope/encoding/KV role/L1 identity/versions and exact ID. Match descriptor raw
hash and address, size, index address/size/hash, block offset/length/hash/rows,
and exact directory and block endpoints/rows/bytes. Reject broader leaf fences,
wrong objects and cross-role/scope/ID substitutions. No new storage I/O.

Preflight uses64KiB, checks64-bit layout, all witness string lengths and both
physical endpoint hex strings with checked sums capped at4MiB. Comparison reserves
64KiB plus existing directory scope reservation plus4times that string sum. This
covers endpoint base64 decoding, canonical reencoding and physical hex decoding,
borrowed comparisons, two scoped path constructions and fixed digest formatting.
Descriptor JSON remains capped by the qualified writer. Each owned model and raw
bytes remains guarded through comparison. Error is sticky and failed allocations
remain charged; success releases temporary owners. No wire-format changes.

Compile a behavioral regression showing valid-shaped substituted index/descriptor
witnesses are accepted before physical binding, then enforce it. Test real output
writes and exact independent witness projection, single-field substitutions,
foreign owners, both routes, missing64KiB admission, zero additional I/O and
stopped retry, zero allocation underestimates. Run full affected suites, strict
checks and fresh independent audit before accepting the component. This does not
prove full merge coverage, selected receipt completeness, durable selector CAS,
publication authority, full Step3 or provider qualification.
