# Gate 3 refinement against the approved Gate 2 checkpoint

Gate 2 checkpoint aggregate:
`3e0e1431e74cfec468d02cb33cb885f5427b5d0e2783fd1c78c1a5575c9752f2`.
Source and evidence are recoverable from
`/private/tmp/arco-gate2-checkpoint-20260906`. Gate 2 approval and all local exit
criteria are recorded in `../reports/2026-09-06-gate2-closeout.md`.

## Implementation mapping

1. Add a private integrity module beneath `control_mvp` with explicit canonical
   binary encoders, scope/version/domain-bound SHA-256 roots, history links,
   rewrite equivalence evidence and checkpoint validation evidence. Authority
   becomes 7; restore plans become 6; block/directory format stays 1.
2. Persist a history anchor and final history root in each manifest. Each suffix
   transaction reference carries its byte length and preceding/mutation/result
   history link; transaction metadata repeats that link. Manifest opening checks
   local suffix continuity. Eager replay recomputes canonical mutation digests.
3. Carry history through the eager base and replay state. Fresh genesis is
   scope-bound. Inline anchors and maintenance move the replay anchor without
   advancing history. Restore inherits the selected candidate parent's history:
   current destination when nonempty, exact retained source when empty.
4. Hash role-tagged owning references into physical roots, including identities,
   lengths, formats, bounds and digests. A checkpoint computes its own root over
   its actual state references and separately binds its source manifest roots.
   Physical maintenance may change this root while preserving history/state.
5. Centralize exact rendered-state decoding and shard combination so every
   materialized rewrite compares all versions, tombstones, values and ordered
   outbox incarnations against its independent expected state before publication.
   Validate rendered transaction bytes similarly, including restore outputs.
6. Use common checkpoint local/source/state validation and owning-reference
   enumeration in publication, persistence, resolution, restore and GC. Bound
   authority JSON fetches before decoding. Ordinary root opens remain local;
   parent links never become an implicit retention requirement.
7. Extend the independent logical oracle and canonical vectors. Exercise
   convergent histories, equivalent layouts, invalid links/rows/evidence,
   provenance, restores and retained closures. Repeat the approved scaling lane,
   recording root hashing and full-rewrite validation work separately.

Opaque outbox payload bytes are durable logical content, including any source
provenance encoded by their application protocol. Physical owning-reference IDs,
block boundaries and fence/layout counters are excluded from mutation encoding.
Physical rewrites preserve these outbox bytes exactly.

## Exit

One source editor and sequential Cargo ownership remain in force. Run all six
required commands in order, then scaling, core and dependent API checks. Obtain
a fresh read-only Gate 3 audit and resolve demonstrated findings. Record the
format contract, canonical vectors, cost deltas, command results and limitations.
Stop after Gate 3 without commits, pushes or provider actions. Gates 4–7 remain
outstanding.
