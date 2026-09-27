# Step 3: fixed native work capture

Continue from selected-copy checkpoint
3f6a07c858d7facd8c90cecaf06e0339651cb14f928bb16e6916b48455d07d89.
Keep base42dd18119ec6708c7dfafa15ced0842093652f94, Rust1.88, locked dependencies,
all wire formats, request/cache/work ceilings, and disabled native advance.

The existing synchronous native allocation closures can invoke legacy cost
hooks that grow WORK/BOUNDED_WORK BTreeMaps. Those TLS maps survive release of
the returned working guard. Finite insertion bounds alone cannot establish
complete live ownership. Replace those map effects inside native allocation
closures with one fixed, allocation-free capture. Do not add a separately
excluded instrumentation pool or duplicate quiet codec implementations.

NativeWork contains exactly [u64;36], the six existing BoundedWork u64 fields,
and an overflow marker. A const-initialized TLS Option holds the current
capture. A non-Send RAII guard replaces/saves the prior fixed value, and normal
finish extracts the child and restores the prior value unchanged. There is no
heap stack, string label, map, vector, Box or callback in captured state.
Nested wrappers each invoice their own captured value directly; do not merge
child counts into the parent capture as well. Drop restores prior TLS even on
unwind. The qualified decoder panic is caught before finish, preserving work
performed before rejection.

Route all existing cost::record and cost::bounded_work calls into active fixed
capture, using checked arithmetic and flagging invalid slots/conversion or
addition overflow. PhaseGuard retains its phase replacement/restoration but
skips global map insertion during capture. Without capture, historical test-utils
maps and fixed test cells keep existing behavior. No new instrumentation work
is enabled for ordinary non-test/non-feature production. Counter hooks and
capture must exist for exactly every configuration where bounded native
allocation can succeed: cfg(any(test, feature="test-utils")). The existing
non-test/non-feature allocation rejection remains intact.

Each successful allocation wrapper starts capture around its synchronous
measured/catch_unwind closure and finishes before fallible postprocessing. Merge
all36slots and all6bounded fields into a fixed aggregate owned by the same
RestorePhysicalIo. Preserve captured work on Ok, Err, panic, and later ownership
retention failure. Checked aggregate overflow marks the ownership report
nonpassing, retains the existing error/working accounting, stops the physical
route, and prevents subsequent I/O. Keep direct admitted-unit hash counters
separate from captured legacy slots, with explicit final aggregation required;
no direct hash pass is counted twice. Dropping a working value or ending a final
microchunk cannot reset native captured work.

Freeze zero heap allocations for empty/update/nested capture. Qualify NativeWork,
TLS option and RAII guard layouts each <=1KiB on the pinned64-bit target. Their
fixed stack/TLS storage does not create persistent request heap backing. Capture
is synchronous and must never cross await or thread migration. Preserve existing
per-request allocation reservations, since no captured counter heap is added.

Retain compiled behavioral reds for global-map growth during a real admitted
directory constructor/root decode and for missing native counts. Verify exact
SHA/directory/semantic counter parity in default-test and test-utils builds,
all-slot/all-field capture, nonnative compatibility, phase restoration, nesting,
thread isolation, ordinary Err/panic, capture/aggregate overflow, post-retention
failure, final cumulative work and zero allocation/counter underestimates. Pin
hook cfgs and call sites with a source guard. Run affected/catalog suites,
strict Clippy, docs, formatting, new-file hygiene, continuation guards and an
independent component audit before sealing.

This capture owns effects inside the allocation closure. It does not account
service phases entered before capture, the service-owned selected-plan graph,
backend async transport scratch, or complete native invocation work. Those
remain separate required boundaries. No enabled native executor, authenticated
coverage or HEAD publication is claimed by this component.
