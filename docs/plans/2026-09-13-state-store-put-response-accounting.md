# Step 3 PUT response ownership correction

The bounded epoch component review identified uncharged successful and failed PUT version String capacity. The existing 32 MiB shared metadata allowance applies to actual returned HEAD and PUT metadata ownership, including failed calls and retries. Charge owned String capacities immediately after return, before copying, retaining, rendering diagnostics, or further I/O. No refund. Charge rejection after an epoch CAS is AmbiguousAuthorityOutcome; lock response rejection stops without cleanup I/O and leaves TTL expiration available. Diagnostics must not render raw version tokens or complete epoch records.

This corrects missing accounting; it does not change fixed probe caps, operation ceilings, authority codecs, or V7 routes. As with HEAD, returned metadata cannot be pre-admitted by the current backend interface. Evidence proves caller accounting and fail-stop after return, not a hard preallocation bound inside an arbitrary backend. The existing outer peak allocation measurement must include backend-returned ownership.

Accepted read-only review: step3-bounded-epoch-component-review-01.md SHA-256 2f8c12e0910273928ac8ea374126a54dda213604a9762fad6f84c40dba632308. Preserve step3-put-metadata-red-01 as a compiler diagnostic; it is not behavioral red evidence.
