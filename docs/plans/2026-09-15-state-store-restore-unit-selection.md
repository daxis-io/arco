# Step 3: private selected-plan loan

Continue from unit-hash checkpoint
bf66d8e9720edfec6d60dad5f55c16d1a29614f91e6a89458181778017fbb756.
Base, toolchain, wire formats, and invocation ceilings remain unchanged. Native
bounded advance remains disabled.

Add a private service-owned loan from RestoreAdvanceContext for one native unit
phase. It must call the existing bounded refence before returning anything,
then require the supplied participant plan to equal the exact selected
RestoreParticipantPlanRecord.plan. Return references to that record's plan and
plan_sha256 together with the same mutable WorkspaceIoBudget. The digest is the
existing canonical retained participant-plan wire digest, including plan_kind;
it is not the canonical hash of a bare Plan7 value.

Only workspace_restore may construct the loan. Its fields remain private; a
crate-private borrowing accessor lends the inseparable selected pair and budget.
The loan borrows the mutable context, so callers release it before re-fencing
again. Native code can retain only copies made under its ownership admission.
Do not accept a caller-supplied digest or a second budget. Legacy contexts,
mismatched supplied plans, expired source/execution windows, and changed selected
attempts/journals must return an error without issuing a loan. Preserve the
existing refence read/fence/deadline ordering and error classifications.

This loan establishes which workspace record supplied the typed plan and its
digest after that refence. It does not validate Plan7 roots against a particular
store, authenticate physical source objects, authorize final HEAD publication,
or prove service-wide parsed metadata allocation bounds. The future admitted
ExpectedPlan constructor must separately validate exact store scope, durable
authority binding, and all six directory roots; validate_shape alone is not
sufficient. No new public caller capability, wire record, or alternate journal
is introduced.

Retain compiled behavioral red evidence before adding the refence and exact-plan
checks. Use real live-fence fixtures to reject a supplied different plan and a
stale selected journal. Verify the successful loan names the exact selected
record and borrows the original budget, rejects legacy context, and cannot
outlive or independently construct its private source capability. Run affected
workspace restore tests, catalog suites, strict Clippy, docs, formatting, source
continuation guards, hygiene including new files, and independent component
review. Existing service metadata admission remains unchanged and explicitly
outside this narrow join's allocation qualification.
