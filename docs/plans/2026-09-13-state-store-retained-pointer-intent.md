# Step 3 amendment 3: armed retained-pointer intent

Date frozen: 2026-09-13

Parent contract: `docs/plans/2026-09-13-state-store-retained-source-amendment.md` SHA-256 `798d895e1d6c0e236d9e8e4d2f1c62db2ac0a739b56a953a30df846f9659b0c2`

This amendment freezes the minimal crash-recovery state required by the selected retained-source pointer. It adds no journal, caller credential, snapshot field, public reference field, or ordinary catalog pointer. One optional armed intent lives in the existing workspace retention mutation epoch record. When absent, canonical V1 epoch bytes and all V7 behavior remain unchanged.

## Why the intent is required

The existing epoch records only operation kind/ID and epoch. After a retained-pointer CAS may have been sent, a restart cannot reconstruct the complete expected pointer bytes from an unselected descriptor or from the immutable workspace snapshot. An absent/different pointer also cannot prove that the remote CAS is no longer pending. Persisting the exact expected bytes and exact precondition before send is the smallest durable state that permits exact lost-response reconciliation without repeating a possibly sent CAS.

## Record shape and compatibility

Keep the outer record type `arco.retention_mutation_epoch`, outer version `1`, and exact path `retention/coordination/mutation-epoch.json`. Add:

```text
armed_retained_pointer: Option<ArmedRetainedPointerIntent>
```

The field uses `#[serde(default, skip_serializing_if = "Option::is_none")]`. A record with no intent therefore preserves the exact prior canonical JCS bytes. Existing readers already accept unknown fields; an old claimant still fails closed on an `IN_FLIGHT` record. Old operator recovery remains an explicit override that is permitted only after all remote mutations are independently terminal.

`ArmedRetainedPointerIntent` is deterministic fixed-field serde JSON with `deny_unknown_fields`:

```text
intent_type: "arco.retained-source.pointer-intent"
version: 1
domain: String
pointer_path: String
reference_key_hex: String
expected_pointer_json: String
expected_pointer_raw_sha256: String
precondition: RetainedPointerIntentPrecondition
```

`RetainedPointerIntentPrecondition` is a deny-unknown-fields tagged enum with exactly one of these canonical shapes:

```json
{"kind":"does_not_exist"}
{"kind":"matches_version","version":"<opaque nonempty provider version>"}
```

The expected pointer is canonical UTF-8 JSON, so storing it as a JSON string preserves its exact bytes without a byte-array expansion or a new binary codec. Its raw digest is lowercase 64-character SHA-256 of `expected_pointer_json.as_bytes()`.

## Exact validation

The outer record accepts an intent only when all conditions hold:

1. Outer state is `IN_FLIGHT`, `completed_at` is absent, outer epoch is positive, and outer kind is `WorkspaceSnapshotFinalize` or `WorkspaceSnapshotRetry`.
2. `intent_type` and intent version are exact. `domain` is one nonblank path-safe state-scope component. `reference_key_hex` is exactly 64 lowercase hexadecimal characters.
3. `pointer_path` is exactly `control/v1/domains/<domain>/retained/v1/current.json`. It is workspace-relative and cannot name another domain, scope, role, arbitrary selector, lock, epoch record, or ordinary catalog HEAD.
4. `expected_pointer_json.as_bytes()` has length `1..=64 KiB`; its recorded raw SHA-256 is exact. The configured adapter's private validator must decode/re-encode it canonically and require retained pointer type/version, exact scope and `DurableAuthorityBinding`, exact `last_operation_id == outer.operation_id`, exact `capture_epoch == outer.epoch`, and a root containing exact singleton membership for `reference_key_hex` and the prepared reference/descriptor.
5. `does_not_exist` is valid only for pointer genesis: previous generation zero, no previous root, generation one. `matches_version` requires an opaque version length `1..=4096` bytes, no control characters, a bounded stable predecessor observation from the same invocation, checked `generation == previous_generation + 1`, and exact previous root digest equality. No default/empty version is allowed.
6. The outer canonical epoch record, including the escaped expected pointer string, is `1..=256 KiB`. The 256 KiB ceiling safely contains a maximally sized 64 KiB canonical pointer plus JSON string escaping and fixed epoch/intent fields. Encoding above the ceiling fails before a write.
7. At most one intent is present. Arming a second intent before the first is definitively cleared is a precondition failure. A non-workspace-snapshot epoch and an `IDLE` epoch must have `armed_retained_pointer == None`.

The retention-coordination module validates the envelope, sizes, path, digest, kind, and precondition. The private configured authority adapter validates pointer schema, scope, binding, operation/epoch, predecessor, selected root transition, descriptor, and exact reference membership. Neither layer treats the descriptor alone as authority.

## Bounded epoch and lock I/O

For a V8/mixed public invocation, every epoch read uses at most four stable attempts:

```text
HEAD-before
reserve 256 KiB + 1 and one range-read object
range GET 0..(256 KiB + 1)
HEAD-after
```

Charge each HEAD and range call against the shared 4096 storage-operation budget before I/O, including errors and retries. Do not refund the fixed range reservation. Require equal nonempty versions, equal HEAD sizes, returned length equal to that size, size in `1..=256 KiB`, and canonical decode/re-encode equality. Exhaustion or instability is ambiguous and fail-closed.

The existing global retention lock remains mandatory. V8/mixed acquire, extend, and release use a 64 KiB + 1 bounded lock-record probe. The lock's five-attempt acquisition ceiling is unchanged. Every lock HEAD/range/PUT is charged to the same operation budget. Fixed conservative pre-admission is allowed only when the underlying lock implementation actually uses the bounded range.

## State transitions

### Arm

1. Snapshot bytes are immutable and exact; its initial pin is selected and active.
2. The caller holds the global retention lock and an owned in-flight snapshot epoch with no intent.
3. Renew the lock through the bounded route. Validate the exact current epoch bytes/version and private prepared pointer.
4. Set `armed_retained_pointer = Some(intent)` and CAS the epoch record against `claimed_version`. Charge one operation; encoded bytes must fit 256 KiB.
5. On success, update both the in-memory record and `claimed_version` to the returned version.
6. On transport error or precondition failure, perform bounded stable epoch readback. Continue to pointer send only if the complete selected epoch bytes equal the complete intended armed record; adopt that readback's exact version as `claimed_version`. Otherwise return unresolved. Cancellation during arming or readback sends no pointer CAS and leaves the epoch in flight.

### Send and reconcile

Set in-memory uncertainty before awaiting the exact pointer CAS. Send exactly the precondition and bytes stored in the armed intent, once.

- Direct success: independently validate stable exact selected pointer bytes and singleton membership, then clear the intent.
- Direct `PreconditionFailed`: bounded stable readback equal to the complete expected bytes plus exact membership is idempotent success. A stable foreign pointer is a definitive conflict; clear the intent, return the conflict, and do not merge/recompute in the invocation.
- Transport error, cancellation, absent pointer, different/unstable bytes, invalid membership, or exhausted budget: retain the intent and in-flight epoch. Do not send again.

Pointer stable readback retains the frozen fixed `64 KiB + 1`, at-most-four-attempt protocol. Every HEAD/range operation and failed attempt is charged.

### Clear

After a definitive pointer result, renew/re-fence boundedly, set the optional intent to `None`, and CAS the epoch record against `claimed_version`. Update the record/version on success. A transport error or precondition failure requires bounded stable equality to the complete expected cleared record before continuing. Otherwise the epoch remains uncertain/in flight. Only after clear may the next domain arm its intent.

### Settle

Ordinary `settle` and generic `settle_terminal_matching` must reject an epoch with `Some(intent)`. The private snapshot reconciliation path may transition an armed in-flight record directly to the exact canonical `IDLE`/`None` record only after stable complete pointer-byte equality and independently validated singleton/reference membership. Its CAS uncertainty is reconciled by stable complete epoch-record equality. Explicit operator recovery can settle an armed intent only after the operator has verified the remote pointer request terminal; the existing audit warning remains mandatory.

## Restart behavior

Acquire the global retention lock and boundedly stable-read the epoch before claiming a retry.

- Matching snapshot epoch with `Some(intent)`: load the exact immutable snapshot and active selected pin under the shared bounds. Stable-read the intent path. Settle only when raw pointer bytes equal `expected_pointer_json.as_bytes()` and the configured adapter validates every pointer/root/descriptor/reference field against the snapshot. Send zero pointer CAS operations. Absent/different/foreign/unstable/exhausted evidence remains unresolved with the intent in flight and sends zero pointer CAS operations.
- Matching snapshot epoch with `None`: no pointer CAS can be pending under this protocol. If the exact snapshot is absent, fail closed because an earlier immutable snapshot write may still be pending and the original private source proof is gone. If exact snapshot and active pin exist, settle the old epoch while holding the lock. Return if all memberships already exist. Otherwise claim a new `WorkspaceSnapshotRetry` epoch and ask the configured adapter to reauthenticate the snapshot's exact reference from current HEAD or bounded authenticated current ancestry. Only that fresh proof may prepare the same reference/deadline under the new epoch. Snapshot/pin alone is not source authority and does not bind the original durable location; missing/exhausted ancestry remains unresolved.
- Foreign operation kind/ID: fail closed and perform no capture or retained-pointer write.

A crash after arming but before actually sending the pointer is deliberately indistinguishable from a crash after send with no visible result. An absent pointer therefore remains unresolved and requires explicit backend/operator reconciliation. This is the availability cost of the no-repeat safety rule.

## Multiple domains

Process V8 domains in canonical registry order with one global intent slot. Exact membership authenticated before the invocation requires zero selector writes. For a missing membership, arm/send/reconcile/clear before inspecting the next missing domain. A crash leaves at most one possibly sent pointer CAS; completed/preexisting domains remain independently selected, and later domains remain unsent. No batch of unjournaled pointer CAS operations is permitted.

## Compiled preventing evidence required before green

1. Canonical V1 epoch fixtures with `None` encode byte-for-byte identically before and after this amendment; V7 snapshot, export, GC, repair, checkpoint, maintenance-root, and restore-apply paths never emit the optional field.
2. Decode rejects unknown/repeated intent fields, wrong type/version, intent on `IDLE` or foreign kind, unsafe/foreign-domain path, malformed reference key/digest, empty/oversized/noncanonical pointer JSON, invalid/oversized version, precondition inconsistent with genesis/successor, pointer operation/epoch mismatch, and outer records over 256 KiB.
3. Arm success, CAS lost response with exact armed readback, CAS lost response without exact readback, direct precondition loss, and cancellation during arm/readback assert exact `claimed_version`, operation counts, zero pointer sends before exact arm proof, and durable state.
4. Pointer send without write, committed lost response, response before clear, cancellation during pointer readback, exact preexisting entry, foreign pointer, and exhausted read budget assert zero/one send count and exact durable intent/epoch state.
5. Generic settlement with `Some(intent)` is red; private exact-pointer/member reconciliation is green; absent/different pointer remains red/in-flight; explicit operator override retains its audit evidence.
6. Two and three V8 domain schedules crash before/after each arm/send/clear boundary. Assert at most one armed intent and one possibly sent CAS, zero resend after restart, canonical order, and new retry publication only after prior terminal settlement.
7. Oversized epoch/lock records and all four unstable stable-read attempts fail within the 32 MiB/4096-operation invocation ledger before unbounded allocation.

This amendment changes only the new V8/mixed capture/retry coordination route. It does not transfer performance evidence, authorize a new source lifetime, permit public attachment, or require the full ordinary 180k matrix unless implementation changes shared V7 workspace or ordinary directory behavior.
