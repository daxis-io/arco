# Bounded restore advance admission clarification

Frozen before implementing or measuring the bounded service dispatch path. This
supplements the invocation clarification and preserves existing V7-only behavior.

`StateRestoreParticipant` adds the source-compatible optional method
`supports_bounded_restore_advance() -> bool`, defaulting to false. The method is pure and stable for the participant binding's lifetime. The workspace
service checks it before acquiring a bounded apply lock or claiming an epoch.
An explicit bounded participant overrides it only together with its bounded
`advance_restore` implementation. Existing legacy implementers need no changes;
their default advance still forwards one legacy apply. The advance default
continues rejecting a bounded context before legacy I/O as a second guard.
This capability does not advertise production restore support or change the
synthetic store's ROLL_FORWARD_RESTORE flag.

Service-owned source and selection checks, including context refence, complete
before the scoped uncertain-mutation guard is armed. The guard starts immediately
before implementation-owned advance and spans the adapter result and its durable
receipt/journal outcome. A cancellation, arbitrary adapter error, or handled
ambiguity remains in flight. A definite service pre-dispatch rejection can settle
its unchanged epoch. Unsupported adapters fail before the epoch claim; an
Unsupported error from an opted-in adapter is not assumed to be pre-send.

Do not infer dispatch identity from thin receiver addresses or a context boolean:
a wrapper can delegate after performing I/O, and an inline first field can share
its wrapper's address. No error classification alone clears uncertainty.

Refence authenticates the immutable attempt and active source pin, checks the held
lease and exact epoch, then rereads the exact selected journal bytes/version as
its final awaited control operation. It checks the original execution/source
deadlines after that read. No further awaited control I/O can precede the caller's
publication using that refence result. The participant must refence again before
prepared-candidate and HEAD publication; the service's initial dispatch check
cannot authorize later publication after arbitrary work.

Required checks: default capability denial creates no apply epoch or adapter I/O;
explicit bounded progress settles and stops after one participant; expired or
changed selection before dispatch is safe to settle; callback cancellation and
an opted-in wrapper's delegated error remain in flight; replacement during pin
validation is rejected by the final exact journal check. This clarification alone
is no runtime, resource, coverage, or final-publication proof.
