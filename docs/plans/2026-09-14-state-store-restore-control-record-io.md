# Native restore control record transport

2026-09-14. Additive Step 3 contract, frozen before implementation/measurement.
Use the existing physical restore ownership ledger, classified storage responses,
workspace control invoice, final microchunk invoice and sticky attempt guards.
Do not route native records through the unclassified workspace-service reader.
The service retains its existing transport. No native advance capability is
enabled by this component.

Use a closed typed address for selector, progress ordinal or receipt ordinal,
under the selected candidate's existing scoped control/v1 restore/v7 prefix.
Candidate IDs remain exactly 64 lowercase hexadecimal characters. Centralize the
three existing unit path spellings; no caller-supplied arbitrary control path is
accepted by the physical reader. This does not change any persisted wire bytes.

For a raw record read, reserve scope/path construction before allocating it.
Use the existing directory scope reservation (16 KiB + 32 times the sum of scope
component lengths), which includes the fixed candidate/ordinal/path suffixes.
Retain the declared path and its guard throughout the operation. HEAD-before
must have a nonempty version and a size in 1..=4 MiB. Before the range read,
reserve exactly size+1 bytes and one operation. Use classified range ownership,
retain every admitted response, then require HEAD-after to have the same size
and nonempty version. Require exactly the declared body length. A changed,
missing-after, oversized, unavailable or unclassifiable response stops the route.
There is no hidden retry. An initial successful HEAD returning no object may
return None; it never establishes genesis, record validity or authority.

Return the pinned raw bytes and metadata under their existing response owners.
The caller must authenticate the raw digest, exact JCS model, selected Plan7
identity and record chain before trusting any field. Parsing/encoding reservations
and durable progress publication are separate required joins, not implied by a
successful raw read.

All metadata HEADs, ranges, returned bytes, arrived versions/errors, failed
operations and cancellation remain charged to the existing route. Keep the same
ordinary 32 MiB/4096 operation invoice, 64 MiB request ownership and final
microchunk carry/cumulative limits. Existing physical required-object readers
must still stop on absence. Add a compiled behavioral red for native ordinary
record reads; verify all three addresses, initial absence, size/version changes,
exact/cap probes, unknown/oversized backing, insufficient budget, cancellation
at each awaited step and zero later I/O after failure. Preserve all existing
payload, metadata, output, merge and format-7 lanes.

Follow-on conditional writes must reuse charged physical PUT machinery, retain
guarded encoded bytes/versions, and use only DoesNotExist or exact version CAS.
Storage error or cancellation stops the native route; no recovery read or resend
occurs in that invocation. A returned selector precondition failure can be
reconciled by an explicitly bounded winner read and unit validation. No record
or selector alone establishes readable catalog authority.
