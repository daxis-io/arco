# Native restore control record publication

2026-09-14. Additive Step 3 contract, frozen before implementation and measurement.
This joins the sealed raw control reader to conditional physical writes; it does
not enable native advance, authenticate a typed record, or publish catalog HEAD.

Reuse the closed selector/progress/receipt addresses and the existing scoped
authority storage, response owners, route invoices, cancellation guard and
immutable collision verifier. Accept only nonempty guarded raw bytes through the
4 MiB record ceiling. The caller must supply exact canonical bytes from the
future admitted typed codec and authenticate the selected plan and record chain.

Progress and receipt objects use DoesNotExist. Their precondition failure must
pass the existing metadata-before, classified exact size+1 probe, metadata-after
collision proof before returning. A differing existing immutable body is terminal.
The selector uses DoesNotExist for initialization or MatchesVersion for advance.
A returned selector precondition failure is an explicit conflict outcome, with
its arrived version still owned and charged. It performs no hidden winner read.
Later reconciliation must explicitly read and authenticate the winner.

Reserve path/scope construction and any expected-version clone before allocating
them. The reservation is the existing directory scope bound plus the expected
version length; checked overflow or exhausted admission stops before storage.
The private raw transport accepts a borrowed expected version only for selector
writes and rejects an empty token. Its caller retains and authenticates the
original version source. Keep the cloned precondition's working guard alive
across its move into the storage future and through completion or cancellation.
Retain the existing working owner of the bytes across the write. The supplied
Bytes handle must have its first sharing allocation accounted by its encoder.

Extract the existing once-only conditional PUT body without duplicating response
classification, storage error ownership or invoices. Keep existing output
DoesNotExist writes and collision proof behavior unchanged. Every attempted PUT,
submitted byte, returned version capacity and known/unknown error remains charged
before another await. An error or cancellation makes the route terminal; no
recovery read or resend follows it in the invocation. No unconditional write is
available. Ordinary 32 MiB/4096 control I/O and 64 MiB request ownership, plus
the existing final microchunk cumulative/carry ceilings, remain unchanged.

Retain a compiled behavioral red before implementation. Cover all three closed
addresses and both routes, selector initialization and exact/stale CAS, immutable
replay and mismatch, foreign byte owners, malformed identity/version/length,
pre-I/O budget exhaustion, returned version/error ownership and cancellation.
Require zero subsequent I/O after terminal failure and preserve existing output,
reader, merge, metadata, payload, cache and format-7 regressions. Bind the final
shared PUT extraction to the previously sealed source in an independent review.
