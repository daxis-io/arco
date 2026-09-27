# Step 3: durable unit records, exact selector CAS and reconciliation

Freeze before implementation or measurements. Base077d349589337f494bab75e38e1fd38c005b10f2,
prior output-binding checkpoint ec691bbb6a0b39e146ea06a47034636725179e87d297bf501aea64070be70a73.
Rust1.88 locked/offline; incremental0/debug0; native advance remains disabled.

A private assembly function accepts the same-ledger ExpectedPlan, authenticated
SelectedProgress, guarded receipt/progress models and at most32 existing writer
output products. Under64KiB preflight enforce matching output-list cardinality.
Reuse admitted local-transition validation before effect, and bind every output
witness to its corresponding product. Copy one bounded output witness at a time
under the existing output string census bound; release each copy after binding.
Preserve u32 part identities. Encode the exact receipt and progress with existing
admitted codecs, hash exact progress bytes, construct and encode the selector
under64KiB+4times the borrowed identity/prefix/digest lengths (checked), and retain
all three encoded bodies plus borrowed original selection until publication.
No I/O participates in assembly. Immutable models cannot change under these loans.

Return an opaque prepared unit, consumed by publication; its constructor is the
only production way to reach the publisher. Before any write, recheck ownership.
Write Receipt(N), then Progress(N+1) with the existing exact immutable replay
writer; then issue one Selector MatchesVersion using the ORIGINAL selected meta
version. Never initialize or use unconditional writes. A success reports the CAS
succeeded, not that selection can never subsequently advance. A known CAS loss
explicitly reads and authenticates current selector/progress/direct receipt and
accepts only the exact proposed progress digest/ordinal. Different progress is
not adopted. Immutable mismatch, missing/corrupt evidence, I/O error, resource
exhaustion and cancellation stop the route; no resend or recovery read follows an
indeterminate error in that invocation.

Before the first await, expose a private fixed-size retained recovery target with
plan/candidate/prior/proposed digests and ordinal. It grants no retry or publish
capability. A fresh invocation may reconcile it read-only against a same-ledger
ExpectedPlan and the authenticated selected reader. Return ExactSelected,
PriorSelected or DifferentSelected observations. A later ordinal alone does not
prove the target committed; no ancestry inference. Errors remain unresolved.
PriorSelected does NOT establish retry safety: selected Plan7 inspection, active
source/deadline/fence gates and authenticated prepared.json absence still belong
to the disabled native driver. No HEAD publication or workspace coordination is
settled by this component. Retained target is fixed stack data; no old invocation
heap owner is carried uncharged into new recovery. No public wire/API change.

Reuse existing control response/cancellation accounting and all codec bounds.
Selected/read/write ceilings remain unchanged; clean publication uses exactly3
PUTs, immutable collisions use bounded exact readback, known loss adds at most
3 selected-record reads. A paused/cancelled publication retains encoded bodies,
version and response guards until drop and stops before another effect.

Compiled behavioral red precedes orchestration. Cover exact receipt/progress/
selector bytes, malformed and disconnected proposals rejected before writes,
missing/extra/mismatched output products, both routes, success, exact replay,
barrier-controlled identical writers, changed selector version, different winner,
immutable mismatch, before/after/lost-response and cancellation at all3 writes,
fresh recovery and selector advancement after the proposed progress, ownership
and budget failures and zero allocation underestimates. Retain failures; run
full affected suites/strictchecks/format/docs/layout and fresh scoped audit.
This is durable local unit-record publication only. Complete merge/input coverage,
whole-invocation ownership, public native advancement, prepared/HEAD publication,
full lifecycle and provider qualification remain unqualified.

## Clarification: non-exact known CAS loss

Publisher outcomes distinguish Written, ExactSelected and Conflict(observation).
Known CAS loss with PriorSelected or DifferentSelected stops the invocation and
returns the explicit non-success Conflict outcome. It cannot be interpreted as
successful advancement. Only fresh read-only reconciliation exposes those
observations without a publication attempt. Retain the compiled conflict-stop
red before this tightening; do not reinterpret historical evidence.
