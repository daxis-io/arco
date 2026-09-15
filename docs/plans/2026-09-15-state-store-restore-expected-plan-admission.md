# Step 3: admitted selected-plan projection

Continue from native-counter checkpoint
2115d7b42e3ba25545393fb770e58086747b9b290a785829d17e6c08ab9366d3,
base 42dd18119ec6708c7dfafa15ced0842093652f94, Rust 1.88 and locked dependencies.
Keep all wire formats, request/work/cache ceilings and disabled native advance.

Add one private constructor in units.rs accepting only a borrowed
WorkingValue<OwnedSelectedPlan>, the current RestorePhysicalIo and its existing
route. Its result is WorkingValue<ExpectedPlan<'a>>, borrowing that inseparable
selected guard. It accepts no independent store, plan, digest, or budget.

The retained selection already revalidates the participant record, canonical
enum-wire digest and Plan7 deserialization/validate_shape before lending the
private plan. own_selected_plan copies that exact private pair. Preserve this
provenance chain; do not repeat broad validate_shape or reserialize the plan
inside the new native projection. Memory ownership alone is insufficient.

A 64 KiB admitted preflight checks same physical ledger, 64-bit target, fixed
ExpectedPlan layout, digest spelling and all six encoded root widths. Each root
must have exactly 380 bytes before binary decoding: directory-v1 roots contain
285 bytes. Width only bounds allocation; validate_roots(io.store()) must still
check exact configured scope, durable authority binding, canonical base64 and
all six directory root structures. Only then may ExpectedPlan::from_selected
project borrowed fields and construct its prefix. No storage I/O occurs here.

Freeze checked cumulative constructor reservation, with D taken from the trusted
io.store().scope.domain().len():

    R = directory_scope_reservation(io.store())
      + 65,536 + 3,990 + 9*D + 512

The existing scope component covers directory construction, scope/path clones
and its error paths. base64 0.22.1 requests 285 decode bytes and 380 canonical
re-encode bytes per root, at most 3,990 across six roots. The additional domain
clone and two Rust 1.88 String formats fit D + 4*(19+D) + 4*(95+D) = 9D+456;
512 retains fixed slack. The 64 KiB fixed envelope covers fixed validation/error
work and requires runtime layout and complete-closure allocation qualification.
The already-admitted selected graph stays charged separately, without a second
copy. Drop the preflight result before full admission.

Every failure stops the same route. Excessive reservation rejects before root
work and projected-prefix allocation. A returned expected plan must not outlive
its selected guard; its working guard retains cumulative fresh allocations
through use and releases only its own charge. A root or store mismatch cannot
produce an expected plan. Exact root width and same-ledger checks must apply
also to test-constructed private negative models.

Before implementing, retain compiled failing regressions for missing store/root
validation and missing complete reservation. Verify the actual selection/copy
path; present/absent models; wrong ledger/scope/binding; all six width checks;
malformed base64 and canonical malformed directory roots; long domains and
existing directory invalid-path errors; exact/below admission; guard lifetime,
release and final carry; six-root counters; and zero accounting underestimates
on admitted supported/error fixtures. Bind source census, dependency digests,
actual allocation/layout observations, fresh component review and both catalog
configurations, strict Clippy, docs, formatting and continuation guards.

This constructs a configured-store-validated unit view. It does not authenticate
source object bytes or merge coverage, enable durable unit execution, establish
publication authority, or qualify service/backend/whole-invocation ownership.
