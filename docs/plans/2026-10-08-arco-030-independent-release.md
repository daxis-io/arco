# Arco 0.3.0 independent integration release

The implementation starts from live main
`49568afa2aaed73490524005d105e745f7dbda1f`. The divergent root and retained
worktrees remain untouched. The retained engine/runtime candidate is
`5314d42e87398f498752c47acfa31dce07d216b1` plus its uncommitted changes;
only the generic transport, API fallback, and operator read handoff are inputs.

The release profile is an authenticated catalog API, immutable Flow acceptance,
an operator-owned worker ingress, and owner-verified output publication. Consumers
own SQL validation, execution engines, hosting, and delivery policy. HTTP timers
and priority, row/column policy, test-only tenant identity, and private authority-8
deletion are excluded. Control-store operator routes remain default-off.

| Increment | Implementation owner | Acceptance gate |
| --- | --- | --- |
| Baseline | Workspace metadata, locks, toolchain, release inputs | Live source and retained candidate hashes; no overwritten evidence |
| Generic execution seam | Flow dispatch, dispatcher and sweeper | Canonical envelopes; durable duplicate recovery; capacity and timer refusal; packaged processes without GCP |
| Frozen execution | Run-key reservation and accepted plan | Manifest A survives deployment B, restart, concurrent replay, and partial event publication; missing proof blocks recovery |
| Publication | Worker output, callback and inspection | Immutable identity, integrity and schema; worker claims alone cannot verify visibility; publication retries preserve computation |
| Read handoff | Catalog projection and internal operator route | Verified scope; one authority/projection cut; exact immutable files; no URLs on inconsistent or unavailable projections |
| Independent qualification | API/Flow reference worker and storage conformance | Exact binaries and fixtures; R2-specific identity, limits and executed provider scenarios |
| Release packet | Cargo, Python, CLI, OpenAPI, changelog, migration and release notes | Repository release checks and required final-commit CI before separately authorized signed publication |

Transient receipts, logs, and executable identities are retained outside tracked
documentation, following the evidence policy. Every gate records `passed`,
`failed`, `blocked`, or `untested`. Main CI run `37823805248` passed; skipped
credentialed IAM tests and tag discipline do not qualify providers or a release.
The latest published release observed at implementation start is `v0.2.1`;
`v0.3.0` does not yet exist.

Routine checks use `cargo shared` with the repository's Rust 1.88 toolchain.
Release builds and retained executable proofs use dedicated outputs. Disk is
checked before substantial builds; no cleanup is authorized by this plan.

Publication, provider activation, prototype promotion, and Turntable work remain
separate actions. Issued signed URLs retain their existing expiry semantics;
immediate bearer URL revocation is not promised.
