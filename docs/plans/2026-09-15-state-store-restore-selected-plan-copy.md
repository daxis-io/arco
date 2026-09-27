# Step 3: owned selected Plan7 admission

Continue from private selection checkpoint
cc54f7c0bd4f35417750e2b1a0b1f6818a2e1c5a5b336fc5b896264670272b26.
Base42dd18119ec6708c7dfafa15ced0842093652f94, Rust1.88, locked dependencies,
wire versions and invocation ceilings remain unchanged. Native advance stays
disabled. This component copies selection provenance; it does not validate
physical authority or permit publication.

The only producer of the private owned selected model accepts the whole
RestoreUnitSelection loan, the current RestorePhysicalIo, and the caller's
existing UnitPayloadAdmission. It borrows the plan, retained enum-wire digest,
and original WorkspaceIoBudget together, and uses that budget for its ordinary
admission route. It never creates a new workspace or payload budget. One
WorkingValue owns a direct ControlMvpRestorePlanV7 and String digest together;
there is no separately supplied plan/digest pair or general public getter.

Require the V7 enum, canonical 71-byte prefixed digest, qualified 64-bit layout,
and checked length arithmetic under an initial 64KiB allocation reservation.
Compute L as the sum of every String leaf copied by the actual Plan7 Clone
implementation: all PlanFields strings, both scopes, identity, source reference
including optional checkpoint strings, source manifest, and either target.
The selected digest is separate. For valid StateToken plans the graph contains
38 present-target or34 absent-target strings, plus the digest. Defensive optional
checkpoint lengths are included even though selected valid Plan7 excludes them.
There are no variable owned collections in this graph.

The copy reservation is checked L+71+64KiB. The fixed term covers qualified
model layouts and error/bookkeeping scratch. Use the existing derived direct
Plan7 Clone and String::to_owned inside the admitted closure. The pinned Rust
String->Vec->[u8] Copy->with_capacity->RawVec chain requests L+71 backing bytes;
no serialization roundtrip, custom per-field copier, or clone outside admission
is needed. All errors stop the physical route. The returned guard retains both
copies across awaits and loan release; their storage is independent of the
borrowed workspace record.

The future native driver must construct this value once before final streaming
starts. In the same physical ledger, FinalMicrochunk::begin automatically
includes it in starting carry; external carry must not count it again. No
OrdinaryUnit reconstruction may run during or between final microchunks. This
component has no native driver and makes no final-driver ordering claim yet.
The later private ExpectedPlan constructor must validate exact store scope,
durable binding and all six fixed-width roots before any plan roots or paths
are used. A guarded selected copy alone establishes neither of those checks.

Retain a compiled failing pre-admission regression before adding the bound.
Reuse a real service-refenced selection fixture. Verify exact independent
copies and enum-wire digest, actual clone allocation <=L+71 for both target
shapes and variable string widths, sticky budget failures before cloning,
retained ownership across yield/drop, and same-ledger initial final carry.
Run affected tests, catalog feature/default suites, strict Clippy, docs, fmt,
source guards and new-file hygiene, and independent component review.

The directory instrumentation review found a separate remaining boundary:
WORK/BOUNDED_WORK are finite TLS maps, but their backing survives Directory
guard release. Their per-insertion bounds do not establish complete invocation
ownership. Preserve that issue for native counter integration; do not silently
exclude a new instrumentation pool or claim this copy fixes it.
