# Step 3 same-invocation standard data assembly

Continue from history accounting commit bae11ae1. The private assembler consumes
the exact selected ascending receipt prefix, independently checks each physical
interval, streams result-generation rows through admitted logical history, and
feeds already-owned verified leaves into the charged directory builder. It
returns the root and history digest while retaining the notice under its original
history owner. This result is not a publication capability or readable authority.

History creation, tuple batches and digest finish require the same physical
owner and final phase. The initial key/scope/notice allocation is admitted before
creation; accepted tuples reuse the key buffer. Failed admission, bad metadata,
rejected tuples or finalization stop the owner. The shared open-state check also
rejects empty batches after finish. Digest finalization retains the original
notice allocation rather than moving it out of its owner.

Initialization shares the first actual receipt chunk. Verified output allocations
remain charged across builder insertion, including unprocessed array slots.
Each renewal corresponds to a receipt, physical verification primitive, builder
leaf insertion or builder finish; terminal closure does not get a separate
CPU-only allowance. The final builder chunk also finishes the logical digest.

Compiled behavioral failures precede implementation and the closed-stream fix.
Tests cover exact/below reservations, owner/phase mismatches, invalid continuation,
partitioned hash work, zero underestimates and owner release. Real authority-8
transactions exercise unchanged/changed targets and multiple output blocks and
receipts with two 180 KB source values. The oracle reads the result root and
checks values, generations, tombstones and logical history; it does not claim
independent directory-byte parity. A protected retained root for the changed
source is explicit fixture construction, not an executed workspace capture.

The small combined stream traverses 58 physical cancellation boundaries.
Coherent terminal-count, predecessor-chain and omitted-output forgeries pass
local selected-edge admission and fail independent assembly without changing
HEAD. Existing standard interval and native builder tests remain separate
component evidence. Maximum builder-cascade cancellation and whole-public
ownership are not established by these small fixtures.

Remaining Step3 requirements include oversized singleton phases, absent-target
fence floors, original deadlines and live fencing through long final streams,
inherited paired outbox plus the physical restore notice, restore certificate
validation, held workspace publication authority, exact prepared/HEAD publication
and read-only reconciliation, the large actual resumable workflow, final audit
and delivery. Native automatic advancement remains disabled.
