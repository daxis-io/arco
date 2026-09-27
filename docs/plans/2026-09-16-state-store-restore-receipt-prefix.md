# Native final receipt-prefix verification

This component follows the charged native directory builder at 33646edc. It
implements structural receipt traversal, not physical merge/coverage verification
or publication authority. Native automatic advancement remains disabled.

The constructor accepts only the same-owner private terminal selected-progress
bundle obtained through the ordinary control reader. It validates the selected
selector and terminal progress, and starts at the plan's exact genesis digests.
Each final microchunk reads one exact ascending immutable ordinal receipt with
metadata-before, a bounded range, and metadata-after. Missing bytes, changed
versions, unknown ownership, malformed canonical bytes and invalid local hashes
fail closed. Generic control records remain excluded from the final ledger.

The carried tail checks ordinal, predecessor raw and chain digests, cursor,
singleton state and cumulative counts. Completion matches the selected terminal
last receipt, raw digest, chain, cursor, singleton state and counts exactly.
Unattached objects beyond the selected count are not scanned. Cancellation stops
the prefix before another read; carried owners drop independently of the retained
failure diagnostic. No persisted receipt or successful prefix mints authority.

Canonical-path construction reserves checked `4 * (prefix length + 128)` plus
64 KiB in the constructor. A compiled 32 KiB-domain regression reproduced the
former underestimate. Real-plan 32/63 KiB-domain tests cover the correction. The
proposed 128 KiB-domain tail regression instead failed the existing HEAD limit;
the tail finding was withdrawn and its original reservation remains unchanged.

The retained oracle now covers 26 present/absent fixtures, including a supported
48 KiB key. Larger 64/63 KiB-key probes fail production segment-index construction
before any receipt exists; they do not demonstrate receipt-reader failures.
These samples do not qualify the maximum writer-valid receipt shape. The
remaining prefix evidence includes that maximum-shape liveness proof.

Step 3 still requires independent physical coverage and exact logical history,
singleton phases, absent-target fence floors, paired outbox/notice assembly,
private publication authority, prepared candidate and exact HEAD publication,
read-only restart reconciliation, complete public-invocation accounting, large
restore qualification and final fresh-context audit. The terminal native driver
still fails closed pending that integration.
