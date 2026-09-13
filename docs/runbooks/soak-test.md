# Runbook: Soak Test

## Status
Deferred (requires staging environment)

## Overview
Validate stability under sustained load (memory growth, error rates, compaction health).

## Preconditions
- Staging environment with production-like data volume
- Traffic generator capable of a 24h run
- Monitoring dashboards and alerts enabled

## Procedure
1. Start steady-state traffic for catalog reads, writes, and query endpoints.
2. Maintain load for 24 hours.
3. Monitor error rate, latency, CPU, memory, and storage growth.
4. Capture any alerts and investigate regressions.

## Evidence to capture
- Start/end metrics export
- Error logs and alert timeline
- Storage growth summary

## Rollback
If errors exceed thresholds, stop traffic and restore the prior release.

## Gate 7 state-store protocol

The 24-hour procedure above does **not** qualify Gate 7. Gate 7 requires the full
[168-hour contract](../plans/2026-09-10-state-store-vnext-gate-7.md), including
604,800 seconds supported by UTC and monotonic evidence. Seven calendar dates or
six elapsed days do not qualify. The selected pilot is currently blocked by the
[retained-row capacity prerequisite](../reports/2026-09-10-gate7-state-store-qualification.md).
Do not prepare cohorts or launch workload until the architectural prerequisite and
concrete execution packet are approved under their separate authority boundaries.

Use a dedicated Linux host supervised by systemd; never extend the private executor's
45-minute boundary. Create the protected maintenance cohort at least 25 hours before
the pilot, retain its timestamps, prove protection before eight days and eligible
collection after expiry. Observe new thirty-day tokens without claiming expiry proof.

The frozen daily workload uses four writers, eight readers, maintenance/projection/GC,
one 25-mutation/s hour and 23 one-mutation/s hours, and 32 reads/s. Daily writer,
maintenance and projection restarts occur at hours 6, 12 and 18 outside the fixed
high-rate hour. Only continued verified workload during planned individual-worker
restarts contributes elapsed time. A gap over 60 seconds, reboot, source/config drift
or unverifiable clock discontinuity resets the qualifying window and preserves the
old attempt.

Keep run ID, source/configuration digests, raw chunks, heartbeat, progress and atomic
checksummed checkpoints outside the checkout. `scripts/gate7_qualification.py` provides
capacity/packet checks, durable-window transitions, read-only Linux recovery assessment
and dedicated-unit status/stop verification. Current components
are not a qualified full pilot supervisor. No launch unit is emitted while execution
prerequisites are missing.

Stop proof requires an inactive systemd unit, no surviving children anywhere in its
cgroup, a new stop-request nonce acknowledged by an advanced worker checkpoint, durable
stopped state and disposition of all outstanding operations. Stop requests are
published exclusively in a separate file; the controller never overwrites the worker
checkpoint. A stale terminal checkpoint cannot acknowledge a new request. Preserve
objects while publication is uncertain. Only the approved manifest's disposable paths
may be cleaned up, with protected cohorts and evidence retained as specified. Session
termination alone neither stops a supervised job nor proves its stop.

The read-only recovery command is:

```sh
python3 scripts/gate7_qualification.py recover /approved/evidence/checkpoint.json /approved/evidence/last-chunk.raw --source-sha256 <reviewed-source-digest> --config-sha256 <reviewed-config-digest>
```

Replace the paths and digests with the approved run's actual bindings. Recovery reads
the Linux boot ID and both clocks, binds the checkpoint to the last chunk header, attempt start and chain head, and emits
`resume`, `reset` or `blocked`; it never launches a worker or credits downtime.
`resume` still requires the next heartbeat's workload evidence to prove continuity.
A reset preserves the old attempt and requires a fresh qualifying window. The pure
window transition cannot substitute for the missing workload verifier or systemd
supervisor. Its `window_complete` flag is not a pilot or Gate 7 verdict.

A rejected heartbeat is retained but is not relabeled as a new attempt's baseline.
The next attempt starts from a newly labeled heartbeat. A terminal stopped run retains
its elapsed accounting and cannot resume; a new run requires its own run ID and evidence
directory. An unresolved publication cannot be recorded as a successful terminal stop.

## Bounded provider qualification

The separate six-hour provider runner is `arco-state-store-qualification provider`.
Its manifest must pass `provider-validate` before traffic. The Linux launcher is `python3 -B scripts/gate7_provider.py start`, with the approved
manifest/digest, exact executable and reviewed unit file/digest. It checks the unit
before systemd can execute start hooks. The unit invokes the same script with
`supervise`; never bypass pre-start admission with a direct start command. Run it only in the digest-bound systemd unit:
`Type=exec`, `KillMode=mixed`, `Restart=no`, `TimeoutStopSec=75`,
`NoNewPrivileges=yes`, `MemoryMax` at most 24 GiB and `TasksMax` at most 64.
Enable `CPUAccounting`, `IOAccounting`, `MemoryAccounting` and `TasksAccounting`.
Start/stop hooks, drop-ins and OnFailure units must be empty; use SIGTERM,
`SendSIGKILL=yes` and `TimeoutStopFailureMode=terminate`. The supervisor verifies
these properties before launch, on active heartbeats and immediately before a stop
request reaches systemd. It binds the unit file digest and owns the listing admission proxy. Build inputs bind the exact
Python 3.11 interpreter as well as the Rust executable.

Use the same script's `status`, `stop` and `recover` commands with the same manifest
and digest. These commands report inactive processes separately from publication
reconciliation. Recovery never silently resumes an interrupted run. Preserve the
operation journal, supervisor checkpoint, telemetry, proxy admissions and systemd
journal; resolve uncertain publication before any approved cleanup. The evidence
directory and its `-supervision` sibling live outside the source export. A session
ending does not stop the unit. The bounded provider runner does not implement the
168-hour pilot workload and cannot qualify that phase.

The workload supervisor does not stop EC2. Before provider admission, bind and test
an independent host shutdown mechanism and its stopped-state proof. Until then,
compute/IPv4 costs are a forecast with manual stop control; no hard total-cost
qualification claim is permitted, and the provider packet remains blocked.
