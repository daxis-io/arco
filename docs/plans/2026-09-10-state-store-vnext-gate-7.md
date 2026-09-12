# Gate 7 state-store qualification — frozen execution contract

Frozen before implementation and measurement on 2026-09-10. This document implements
the user-approved Gate 7 plan. Gate 7 is incomplete until local qualification,
separately approved real S3 qualification, and the full pilot have passed.

## Source, ownership and authority

Base commit: `92fd19f11a547ece5004ac94cad83a3527471812`.
Base tree: `9a1a5abd6f22a89077d6e44c0d8490d20ad34131`.
Parent: `d246e9522a031a475a0e20cd86c1c8dd5f18dbbb`.
Qualified manifest: `75f4bffbac1e52b550375280d5af435de82691dbdcfa4a54f5748b6ce1c6e876`.
Prior recovery archive: `205b37d9e14f29612ef352f5fdb9bbb0a1f42b1beee53fb96e6803d8a20e1947`.

Retain actual fetch result and before/after refs, status, tool versions, active
processes, ownership, and free disk. Choose an unused dated branch/worktree from
the pinned base. Preserve root dirt, Gate 6, targets, archives, and failures.
Never apply the Gate 6 pre-commit patch on its commit. One source writer and one
sequential Cargo queue own new target and evidence directories. Disable incremental
compilation and dev/test debug info. Refresh disk before builds. Use Rust 1.88,
cargo-deny 0.18.9, Buf 1.70.0, mdBook 0.4.52, Python 3.11, uv 0.11.15.

Preserve authority 7, restore-plan 6, segment/directory 1, continuation 3,
maintenance schema/policy 1, rewrite-equivalence v2 with v1 readability, all Gate 5
limits, 24-hour maintenance execution, eight-day maintenance retention, seven-day
orphan eligibility, and thirty-day token/checkpoint protection. Preserve the entire
Gate 6 cache configuration/ownership contract. Every substitution freshly validates
object version. Mutable authority, overlays, and publication reconciliation remain
uncached. No migration, routing change, dual authority, resident writer, WAL,
cross-root transaction protocol, limit increase, or retention redesign.

## Local inventory and oracle

Extend existing reclamation fixture, independently encoded LogicalOracle, remote
operation barriers, and measurement support; no generic qualification framework.
Keep original tests/models intact. Add 32 seeds (33–64), 128 operations, three
cache modes (disabled/default/pressure), two block targets (32/256 KiB): 24,576
operations. Actual successful family coverage is mandatory, not attempted branches.
Families: older retained-cut restore; distinct ordered outbox add/trim/restage and
incarnation histories; retained reads; lazy conflicts; cache reopen/warming;
maintenance resume/expiry; GC; competing publication; combined failure/restart.

Add 96 barrier schedules: restore transaction/manifest/HEAD, maintenance
selection/HEAD, reclamation delete, acknowledgement retirement, source outbox trim;
each crossed with pause-before, pause-after, lost response, delayed error and all
three cache modes. Sleeps do not establish ordering. After accepted transitions
compare sequence, history root, visible contents, ordered outbox identity/payload/
origin, retained readers and physical ownership to independent expectations.
Preserve seeds, generated operations, actual schedules, publication outcomes,
source identity and complete failure traces. Before any production behavior change,
retain a failing regression. If qualification tests pass, leave production unchanged.
Use ControlCatalogAuthority and CatalogProjectionMaterializer for catalog coverage;
use direct store probes for cache/fault schedules.

Capacity prerequisite: 1,209,600 successful catalog mutations retain at least
2,419,200 receipt/audit rows. Reproduce growth with actual encodings and exercise
restore/replay/maintenance bounds. A confirmed conflict blocks this selected pilot;
do not raise limits, discard records, rotate roots, or redesign retention. Complete
independent local work and document the architectural follow-up.

## Executable and provider package

One executable in arco-storage-s3, existing dependencies/shared support; one Python
stdlib supervisor for a dedicated Linux host with systemd. Commands cover inventory,
configuration validation, credential-free runs, bounded provider runs, pilot
supervision, status, stop, recovery. Missing configuration, credentials, executable
identity or discovered scenarios fails before qualification traffic.

Use ScopedStorage and unique synthetic tenant/workspace. Even raw conformance paths
must remain under the manifest's disposable namespace. Trace catalog, projection,
acknowledgement, retention, maintenance, recovery and cleanup paths. Loopback tests
exercise the actual S3 adapter: conditional create/replace, stale/malformed CAS,
concurrent winners, addressed GET/HEAD, delete/recreate, opaque version tokens,
range edges, >1,000-object bounded listing, read retries, single-attempt conditional
writes, timeout/error mapping and exact reconciliation. Integrated scenarios cover
commit/restore, retained readers, authenticated warm/cold reads, changed/deleted warm
objects, maintenance/reclamation/restart, projection delivery/ack/trim and watermark
equality. Distinguish conflict from uncertain publication for S3 412/409/404.

Freeze five provider repetitions. Per repetition/operation/cache mode collect 200
independently cold point/scan observations, fresh-cache proofs and raw durations.
Label injected client failures even if S3 received the operation.

Prepare separate non-executable blocked packets when prerequisites are unavailable,
or concrete approval manifests for (1) bounded real S3 and (2) cohort plus pilot.
Bind base SHA, complete candidate source manifest/tree, binary patch, executable or
image digest, build inputs, scenario inventory and contract digest without committing.
Each records verified account/principal, host, region, bucket/prefix, credential
acquisition/renewal, permissions, endpoints, operations/concurrency/deadlines,
evidence destination, diagnostic allowlist, abort and cleanup scope. Historical
resources are discovery leads only. No resource creation, IAM/SCP mutation, image
publication or workflow dispatch. Prefix-limited listing and least-privilege object
access; exact KMS rights only if required by verified bucket configuration.

Provider ceilings: 6 hours, 2 million S3 requests, 20 GiB stored, $25 total.
Cohort/pilot ceilings: 25 hours preparation + 168 hours pilot + 2 hours closeout,
100 million requests, 512 GiB stored, $500 total. Calculate current-price costs for
selected host/storage, admit retries conservatively, distinguish adapter calls,
transport observations and upper bounds, stop at first applicable ceiling.

## Pilot protocol

Dedicated Linux host/systemd; do not extend private executor's 45-minute boundary.
One synthetic workspace-as-metastore root: one catalog, eight schemas, 128 tables
per schema, four columns/table. Four competing catalog clients, eight readers,
maintenance/projection workers and bounded GC worker.

Every 24 hours: one fixed hour at 25 successful semantic mutations/second (at least
90,000/hour), other 23 hours at 1/second. Mix: 80% metadata update, 10% rename,
5% create, 5% drop; paired churn preserves population. Aggregate reads 32/second,
60% point, 20% bounded scan, 20% retained. Separate default/disabled/pressure/freshly
opened reader cohorts. Separate semantic successes from retries/idempotent replay,
projection acknowledgements and physical maintenance.

Create maintenance/reclamation cohort at least 25 hours before pilot; preserve
original timestamps. Verify eight-day protection before expiry near pilot hour 167,
then eligible collection. Observe new tokens; seven days does not prove thirty-day
expiry. Outside qualification hour restart one writer at daily hour 6, maintenance
at hour 12, projection at hour 18; others continue. Predetermined bounded client
fault schedules retain interruptions and reconciliation results.

Acceptance: warm narrow-write p99 <=250 ms, warm point p99 <=50 ms, bounded scan
p99 <=250 ms for frozen page shape; cold writer startup <=2 s; manifest-reachable
replay <=64 MiB; projection lag p99 <=10 s and no normal interval >60 s. Existing
maintenance intent/backpressure and cache ownership limits hold. Zero correctness,
fencing, authentication, retained-data or evidence-integrity failures, and zero
unresolved publication ambiguity. Warn at 24 reachable L0 segments; backpressure
outside designated drill prevents qualification.

Record raw durations/outcomes, physical I/O, CPU/RSS, growth, per-handle cache
ledgers, maintenance backlog, retention and restarts. Compare growth to forecast
and ceilings. Persist run ID, source/config digests, bounded flushed evidence,
heartbeat, progress and stop state outside checkout; atomic checkpoints. Supply
unit/cgroup handles, status/stop/restart instructions.

Require 604,800 seconds actual elapsed operation with UTC and monotonic evidence.
Planned worker restarts count only while remaining workload/supervision meet the
contract. Unplanned gap >60 seconds, host reboot, source/config change or unverifiable
clock discontinuity resets window and preserves prior attempt. Session termination
does not stop the job. Stop proof requires inactive unit/all children, durable stop
and outstanding-operation disposition. Reconcile uncertainty before cleanup;
cleanup only manifest-authorized disposable objects, retaining evidence/protected
cohorts according to disposition.

## Qualification, audit and recovery

Sequentially run original 32x128 maintenance and 32x64 reclamation models; 85 remote
write schedules and logical-clock recovery; 86 scaling cases; 15 resume and three
lifecycle schedules; all new models/schedules, S3 loopback, runner restart/stop,
containment, elapsed/capacity validators. Runner tests reject 24 hours, six days,
seven calendar dates, missing telemetry, source drift, lost checkpoints, surviving
children and exceeded ceilings.

Run applicable credential-free workflow lanes: catalog/core/storage, workspace,
API/Flow matrices, deterministic UAT, protocol, Python, dependency policy,
formatting, Clippy, docs and hygiene. Reused evidence needs byte verification and
source-dependency applicability/differences. Rerun affected production/dependency/
measurement inputs; missing evidence or unresolved mandatory failures is nonpassing.

Retain corrected Gate 6 measurements: five predetermined runs; 200 independent cold
point/scan samples per run; raw durations; nearest-rank percentiles; median per-run
comparisons; original warm counts/thresholds. Include HEAD latency, decode/hash/
validation, allocations, ownership peaks, explicit zeros. Do not aggregate discarded
handle ledgers into RSS or total cache activity.

After stabilization obtain fresh-context read-only audit of contract, entire diff
including new files, exact manifests, inventory, raw evidence/calculations/failures
and both packets. Resolve demonstrated P0/P1/P2 in scope and rerun affected checks;
retain architectural blockers. Update combined design, dated report and soak
runbook identifying 168-hour protocol.

Seal full candidate, exact Gate 6 base snapshot, binary patch, docs and evidence
closure in a new recovery archive. Preserve prior archives/failures; exclude
credentials, Git metadata, build trees and redundant reconstruction scratch.
Independently inspect actual compressed members and reconstruct from both Git base
and archive base, verifying paths/hashes/sizes/modes. Final checksum/closeout external.
Leave candidate uncommitted and unstaged. Separately report implementation/local
qualification/remote CI/real S3/pilot preparation-running-complete/deployment-cutover.
Complete independent local preparation before asking separate approval for each
concrete digest-bound external manifest. Blocked prerequisites stay blocked.

Amendments must be written and hashed before affected measurements, preserving
superseded contracts. No amendment may silently weaken user acceptance.

## Execution amendment 1 — correctness test build profile

Before the replacement correctness run, use `CARGO_PROFILE_TEST_OPT_LEVEL=1` in a
new owned target. The initial unoptimized new-model attempt is retained as an
incomplete execution, with its completed cases and interruption explicitly recorded.
Run all 32 seeds x 128 operations x three cache modes x two layouts again; retain
all assertions and actual-family requirements. This changes correctness-test build
inputs only. It does not change or qualify latency/allocation measurements, the
Gate 6 benchmark profile, thresholds, repetitions, or sampling contract.

## Execution clarification 2: schedule cache ownership

The three-mode axis of the 96 controlled schedules is the retained catalog-reader
cohort. Before each boundary, prove that disabled has no cache and enabled cohorts
have exactly their frozen capacities, warm the retained reader, and retain its
post-boundary ledger and value assertion. Maintenance, acknowledgement and GC
capabilities keep their existing cache ownership; GC remains uncached. The pressure
cohort means the frozen small-capacity configuration and does not claim eviction
unless an observed ledger proves it. This clarification precedes the affected
schedule rerun and introduces no production configuration or format change.

## Execution amendment 3: bounded provider continuation (2026-09-11)

Continue the exact sealed local candidate in its existing worktree, preserving its
archive, manifest, failures and targets. The user subsequently authorized resource
prerequisite creation; provider traffic, pilot execution and further organization
security-policy changes retain their separate concrete approval boundaries. The
approved diagnostic SCP exception was applied and restored with independent proof.

Reuse the integrated scenarios for five complete provider repetitions, each in an
explicitly listed disposable ScopedStorage workspace. Each repetition collects 200
independent cold observations per operation/cache mode (6,000 total). Credential-free
scenarios retain their original inventory and sampling. Provider transport clients
retain ordinary retry defaults and zero retries for conditional writes, with a bounded
request timeout. This configuration is isolated behind the qualification feature;
production constructor defaults and authority/cache contracts remain unchanged.

Use the pinned object_store HTTP connector to count and admit every actual physical
request attempt before dispatch, including retries, range HEADs, reconciliation HEADs
and listing continuation requests. Reserve submitted bytes conservatively per attempt. Retain adapter calls, raw operation duration/outcome, actual transport attempts
and conservative stored-byte/cost upper bounds separately. Account for every attempted PUT conservatively even after delete
or conflict. Reject path/endpoint/credential/configuration drift before traffic and
stop admission at the first request, storage, elapsed, evidence or priced-cost bound.

Bind executable, source manifest, patch, contract, build inputs, exact inventory,
account/role/host and namespace in a separately hashed manifest. Provider results
remain execution evidence until the frozen correctness and measurement inventory is
validated. No provider elapsed-retention or seven-day pilot claim follows from an
injected clock. Failed or interrupted runs preserve objects and publication evidence;
no automatic broad cleanup or blind resumption. Pilot capacity remains blocked.

### Amendment 3 correction before measurement

The first build exposed an incorrect cache-version assumption: this workspace pins
object_store 0.11.2, whose HTTP connector is private. The attempted 0.14-only hook is
removed and the failed build retained. No dependency version is upgraded. The
qualification-only constructor uses one ordinary retry, zero conditional retries
and a 30-second request timeout; the production constructor defaults are unchanged.
Reserve two physical attempts for GET/HEAD/DELETE, four for range (HEAD + GET),
three for PUT (including conflict reconciliation), and two attempts per potential
listing page (one page per requested key plus exhaustion). Run unbounded listing
through bounded pages and require ordered cursor progress. Reserve all submitted
PUT bytes twice; never refund on conflict/delete. These are conservative adapter
request bounds, not observed provider HTTP counts. Rehearsal compares them against
actual HTTP observations. AWS listing progress is part of the provider conformance
assumption; the adapter does not expose internal empty truncated pages.


## Execution amendment 4: hard listing admission (2026-09-11)

The fresh audit confirms that the pinned SDK can hide empty truncated list pages.
The amendment 3 per-key request reservation cannot establish a hard ceiling.
Before provider traffic, route only the qualification listing client through a
local, endpoint-restricted CONNECT proxy. Use HTTP/1, `Connection: close` and an
idle pool of zero so each listing request needs a separate admitted connection.
Preserve ordinary clients, conditional-write single attempts, production defaults,
and every cache/format/ownership contract. This listing transport configuration
must be disclosed in provider measurements and tested on the exact pinned stack.

Reserve 1,000,000 listing connections and their full worst-case request cost before
admission. Count the remaining adapter reservations against the same total request
and cost ceilings; list calls consume that reserved connection pool, never per-key
estimates. The proxy durably admits each connection before connecting upstream,
refuses connection N+1, allows only the manifest's exact S3 authority, and never
resets or refunds counters. TLS remains end-to-end. Proxy failure stops the run.
No socket count is labelled an observed HTTP count: the one-request-per-connection
invariant plus raw loopback transport evidence establishes an upper bound.

Partition the evidence ceiling before traffic: reserve 4 MiB for manifests,
checkpoints and verdicts, then allocate three quarters of the remainder to the
Rust operation journal and one quarter to supervisor output, telemetry and proxy
admissions. Every writer checks its allocation before writing. Missing or exceeded
allocations fail the run, including at closeout. Keep all prior failed attempts.

The remaining admission and stop fixes require exact systemd property checks,
provider-vs-rehearsal identity validation, complete source membership, and a fresh
stop nonce acknowledged after the request's checkpoint sequence. A crash without
that acknowledgement cannot establish durable stop or permit cleanup. Pilot
capacity remains blocked; these changes do not start or qualify any provider run.


### Amendment 4 allocation refinement before acceptance measurements

Reserve 100,000 listing connections, replacing the initial 1,000,000 allocation.
The frozen normal rehearsal uses 100 listing connections; the smaller hard slice
still leaves 1,000 times that inventory and avoids needlessly precharging half the
entire request ceiling. Keep the 2,000,000 total and $25 first-ceiling rules. This
is a stricter listing ceiling, not a refund or a change to scenario/sample counts.


## Execution amendment 5: qualification rejects redirects (2026-09-11)

The exact pinned object_store 0.11.2 / reqwest 0.12.28 stack follows HTTP redirects
inside a request, even with zero configured retries. The retained redirect regression
shows that a retry count alone cannot prove a conditional single physical attempt.
Vendor the unchanged 0.11.2 source with one opt-in ClientOptions method disabling
redirects, defaulting off. Enable it only for qualification clients. Preserve the
upstream crate checksum, complete upstream file hashes and the small patch. No
version upgrade or production-constructor default changes are authorized by this
amendment. Requalify the affected adapter, dependency and build lanes. Both 307 and
308 conditional probes must fail after exactly one HTTP request; preserve uncertain
publication and reconcile before cleanup. The listing proxy still bounds hidden
pagination; ordinary request reservations retain their earlier counts because
qualification clients now reject redirects.

## Amendment 6 — complete transport and Linux admission controls

Before affected measurements, verify the existing HTTP/1 default for every qualification
S3 client. This excludes HTTP/2 protocol NACK retries below object_store from ordinary
request reservations and conditional single-attempt behavior. Exact pinned source
inspection and TLS ALPN evidence establish the default; no additional override is needed. Retain the no-redirect
patch and test against TLS peers offering both h2 and HTTP/1.1. Production constructor
defaults remain unchanged.

Validate the exact reviewed systemd unit artifact and effective properties before
starting it. Reject start/stop hooks, drop-ins, failure-triggered units and unsafe kill
settings. Recheck the effective contract inside supervision. Record whole-cgroup CPU,
memory, physical I/O and process counts alongside main-process samples; account for
supervision/proxy work in resource evidence. Listing connection and relay deadlines
use both UTC expiry and remaining monotonic budget before and after blocking I/O.

Bind the Python 3.11 interpreter path, executable hash and exact version in build
inputs; verify the actual parent executable before traffic. Pre-start admission hashes
the worker before invoking its self-validation. Enable systemd CPU/IO/memory/task
accounting. Detect all cgroup survivors, including detached process groups, before
recording completion. Bind terminal journals and the verification verdict by hash/size
in the checkpoint and recheck that closure before reporting reconciled stop or recovery.
