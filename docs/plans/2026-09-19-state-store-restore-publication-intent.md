# Durable restore publication intent

This extends the existing retention epoch envelope with optional
`armed_restore_publication`. Omission preserves existing V1 epoch bytes. An armed
restore record uses envelope version 2 so old binaries reject it before their
permissive V1 decoder could discard the intent. New readers reject a V1 record
with restore intent and a V2 record without it. Explicit operator recovery returns
the cleared record to V1. The intent is
mutually exclusive with `armed_retained_pointer`, and is valid only in an
in-flight `workspace_restore_apply` epoch.

The intent is a strict object with `intent_type="arco.restore.publication-intent"`,
`version=1`, `domain`, `plan_sha256`, `owner_generation`, `candidate_id`,
`prepared_path`, `prepared_sha256`, and `precondition`. Digests are lowercase
64-digit raw SHA-256 hex; the candidate ID uses the same encoding. Owner generation
is the positive selected participant attempt. The prepared path is exactly
`control/v1/domains/{domain}/restore/v7/{candidate_id}/prepared.json`.
The original HEAD condition uses the existing tagged precondition codec:
`{"kind":"does_not_exist"}` or `{"kind":"matches_version","version":...}`.
The epoch retains its existing 256 KiB bound.

Arming borrows the existing uncertain-mutation loan. It rejects preexisting
uncertainty and either armed intent, authenticates the held epoch and lease, then
marks uncertainty sticky before publishing the armed epoch. Successful arming
does not erase uncertainty. Dropping, cancellation, failed admission, or a lost
response cannot authorize another send or generic settlement. This record is a
reconciliation boundary, never a publication capability.

The caller must already hold the private same-invocation verified candidate
capability. Its later ordering remains artifacts, seal, refence and intent,
prepared descriptor, refence, one original-condition HEAD send. Persisted intent
cannot reconstruct the capability, even when prepared metadata is absent.
Generic and automatic epoch settlement must preserve an armed restore intent;
only proof-based restore reconciliation may clear it. The preexisting explicit
operator recovery override retains its separate requirement to resolve every
remote mutation before discarding an intent.

This document specifies the coordination record. It does not qualify final
verification, candidate construction, HEAD publication, or restore recovery.
