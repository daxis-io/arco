# Step 3 native unit driver checkpoint

This extends the durable restore contract from base
606ee8d9a80565dd218f12f2fc639b331e00c4c2. It is a standard-unit component, not
completed Step 3. Production authority7 and restore-plan6 stay compatible.
Native automatic advancement remains disabled; the actual adapter is exercised
through an explicit test wrapper under workspace restore coordination.

## Durable preparation

Typed gate reads pin HEAD, manifest and prepared-candidate records with bounded
metadata-before/range/metadata-after checks on the one control ledger. Compare
exact frozen HEAD bytes and version before reading a successor manifest: an
already-proved lost condition is Superseded even if successor bytes are missing.
Unchanged targets authenticate both original and source manifests, roots, history,
sequences and writer/reclamation fences. A prepared candidate switches to an
ambiguous/read-only recovery boundary; its existence does not prove commitment.
Absent targets remain unsupported pending authenticated target fence floors.

Bootstrap persists immutable Progress(0), then uses selector DoesNotExist. Existing
selection is authenticated and reused. A standard advance prepares one bounded
window, persists its immutable receipt and progress, and uses the original selector
version exactly once. Every selector dispatch follows a fresh workspace selection,
source-pin, epoch and journal-last refence with live frozen deadline checks, after
all awaited physical observations and immutable staging. Private staged bundles
retain the original admitted owners across budget loans. Losing selection invokes
read-only reconciliation. Cancellation or an uncertain response does not create
retry permission.

A successful unit returns InProgress with no participant visibility evidence and
leaves catalog HEAD unchanged. Subsequent calls authenticate selected progress and
its direct edge before continuation. Generic Ready cannot settle an uncertain
in-flight workspace epoch. Terminal progress still returns unsupported finalization.

## Accounting and phase boundaries

Standard units retain the existing64MiB request ceiling and unit block/byte limits.
Every gate and generic control-record operation uses WorkspaceIoBudget. Merge
selection and segment/index/descriptor output cannot use FinalMicrochunk. A future
terminal receipt reader and directory builder need dedicated final-stream seams.

Native semantic errors are constructed under admitted allocation guards. Newly
created static diagnostics from a refused/stopped allocation gate are invoiced by
exact String capacity and retained in one accumulated failed-allocation owner.
They do not count as decoder execution or a decoder underestimate. Admission
failure/stopped/overflow remains nonpassing; no budget is increased. Final peak
updates do not call a fallible response helper that could allocate a second error.

## Evidence and remaining completion gates

Retain compiled behavioral reds, failures, commands, source/tool manifests, raw
logs and subsequent greens in the separately owned Step3 evidence directory.
Coverage includes real two-call workspace continuation, expiry after Progress(0)
and Progress(1) staging with no selector afterward, lost/cancelled bootstrap writes,
corruption and supersession, empty binary keys, independent small-state merge
semantics, rejected final routes, exact diagnostic allocation and ownership.

Remaining full-Step3 obligations include an independently streamed receipt and
physical coverage/history verifier; charged deterministic directory building;
oversized singleton phases; observed absent-target fencing; inherited paired
outbox plus one restore notice; restore-specific manifest/certificate/provenance;
prepared-candidate publication and read-only reconciliation across restart and
uncertainty; long-stream live deadlines/fences; large actual resumable workflow
fixtures; final fresh-context audit and complete reconstructable delivery package.
These component results do not establish large restore, provider qualification,
full pilot execution, deployment, production cutover or lifecycle/GC support.
