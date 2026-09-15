# Restore error ownership accounting clarification

This supplements the frozen invocation and bytes-ownership contracts. It does
not enable native restore advancement or establish the decoder scratch bound.

A failed native decode retains its admitted working-memory reservation in the
stopped physical invocation through private cleanup. Panic conversion belongs
inside the decoder allocation observation. Success values cannot move out of
their reservation independently.

On participant return, the workspace service synchronously invoices every
returned error before any inspection, recovery, or cleanup await. This invoice
uses the same outer WorkspaceIoBudget through the public invocation's return.
The physical allocation-time peak remains separate evidence; the synchronous
post-return invoice is not a second physical allocation.

The error invoice is size_of::<CatalogError>() plus every owned String capacity,
using an exhaustive enum match. AlreadyExists and NotFound invoice both entity
and name capacities; all other current variants invoice message capacity. The
owned-object term follows existing ObjectMeta and WriteResult accounting and
is conservative for an inline error. Multiple returned errors accumulate.

An arrived error charge cannot be rolled back on admission failure. Overflow
records usize::MAX; an exceeded budget becomes sticky-stopped. Later operation
or byte admission rejects before storage I/O. Rejection errors are themselves
invoiced, and the original participant error is preserved. The mutation remains
uncertain; a later separately admitted recovery invocation must reconcile it.

Required evidence includes compiled red/green checks for the private error held
across an await, measured panic conversion, cumulative capacity census, and a
real workspace advance error that exhausts admission without subsequent
inspection, journal mutation, settlement, or lock-release I/O.
