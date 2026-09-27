# Step 3 singleton comparison witness amendment

Base bc4b2d6a97c32fa986225d6ad5a104cb70c72ad4. Native advancement remains disabled.

The existing private receipt-v1 Pending state requires at least one full value
witness. Requiring both witnesses to remain identical across CompareSource to
CompareCurrent prevents constructing two independently read oversized values
when their combined encoded lengths exceed the 64 MiB input allowance.

Preserve the wire fields, phase names, four receipt ladder and all ceilings.
None to CompareSource reads the source when present, retaining source with no
current witness; when source is absent it may read the sole current value.
CompareSource to CompareCurrent may fill a previously missing current witness,
while retaining the exact key, source and any already present current witness.
CompareCurrent to Emit freezes both witnesses. Emit to Complete advances the
key only after canonical output. Never remove or change an observed witness.

A null current at CompareSource is unresolved. At CompareCurrent and Emit it
must mean independently proved rooted absence. This structural transition does
not prove absence, membership or payload truth. Native phase execution and the
same-invocation final verifier must authenticate all those facts, actual payload
length/digest, generation/tombstone, exact work counts and output semantics.
Each comparison phase may reread one selected present payload; those reads must
be charged. No metadata-only shortcut or zero payload allowance is introduced.

Acceptance: compiled behavioral red for filling current after one source read;
negative coverage for changed source/key, changed or removed observed current,
late insertion at Emit, skipped phases and empty Pending. Existing wire vectors
and receipt/cursor validation remain compatible. Full physical singleton
execution, maximum payload pair and scratch proof remain separate requirements.
