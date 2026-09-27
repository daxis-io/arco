# Authority-8 absent-target fence input

Status: implementation contract for the next boundary; not qualification evidence.
Base: b0af2065fce458fc27d18693005373314519b7f4.

Trusted composition supplies a fence witness independently of restore input. The
witness pins StateScope, DurableAuthorityBinding, relative object path, positive
encoded byte size (at most 4 KiB), prefixed SHA-256, and nonempty object version.
The configured path must be under the domain's control prefix. Restore never
creates, repairs, advances, or substitutes this object. A missing witness cannot
be replaced with source fields or an absent HEAD observation.

The external object is canonical JCS with no unknown fields or defaults:

```
record_type = "control_mvp_restore_fence"
version = 1
scope: StateScope
durable_authority_binding: DurableAuthorityBinding
writer_epoch: u64
reclamation_generation: u64
```

Read it with a bounded HEAD/read/HEAD stable observation through the control
ledger. Authenticate the pinned size, version and raw digest before decoding;
require exact JCS bytes, scope and durable binding. Reject u64::MAX writer epoch.
The configured writer epoch must equal the authenticated writer epoch. Both
observed floors must cover the authenticated source manifest's floors.

Planning stores those observed values in existing Plan7 absent fields and stores
the authenticated source manifest floors in its source-parent fields. No Plan7
wire field changes. Base roots, history and sequence inherit the source. Each
subsequent planning/inspection/advance/finalization/publication check uses the
trusted pin again and compares the observed floors with Plan7. A present HEAD or
changed authenticated fences supersedes the attempt. Missing, corrupt or
unavailable proof fails closed. The final publication condition remains
WritePrecondition::DoesNotExist; no helper may translate it into an overwrite.

The external fence pin remains trusted configuration across restart. Persisted
Plan7/seal/prepared records cannot supply or replace it. This document defines
only absent-fence admission. Native terminal finalization, candidate codecs,
publication intent and recovery remain separate gates.
