# Tenant identity-root lifecycle integration

## Source and boundary

This child is based on `origin/main@0240ecbe` (authority format 9). It carries
the preserved synthetic and physical-lifecycle changes from the earlier
isolated worktree without changing that worktree.
The current `RootStorage` retention interface is reused directly.
The local source manifest is
`/private/tmp/arco-tenant-identity-integrated-20260926/source.sha256`
(SHA-256 `9b6349aee855136b3938ce595d78dce73487cefb15b7297b21be1fe909f65097`).

The `IdentityStore` probe is available only with `test-utils` and accepts only
`SyntheticIdentityMutation`. `ControlMvpStateStore::new` rejects identity roots
before storage I/O while preserving metastore-root admission. Identity layout
maintenance remains capped at 15 commits. This memory-backend child does not implement principal mutations,
revocation, purge, provider qualification, authorization, or production routing.

## Verification

| Gate | Result |
|---|---|
| Focused identity unit tests | Passed: 9/9. |
| Focused identity integration tests | Passed: 5/5. |
| Control MVP integration tests | Passed: 84/84, including preserved metastore admission and identity rejection. |
| Core/catalog suite and rustdoc except long Gate 7 case | Passed: 1,517 tests, 0 failed, 35 ignored, 1 filtered. Local log SHA-256: `f0128770aac65683925f1e2ccc3cbe4f0baa0ece7e90e0f2251764c1bea06306`. |
| Core/catalog all-target/all-feature Clippy | Passed with `-D warnings`. Log SHA-256: `836f233b590ed4c05b2a8531d8e26f2009e291edbb38941999d5f05c71e192cc`. |
| Formatting and diff whitespace | Passed: `cargo fmt --all -- --check` and `git diff --check`. |
| Final-source Gate 7 model | Running against rebuilt test binary SHA-256 `59e6e4f588afa0114c285f2ba8f6dca48d0415b48427949109674d222863e4d8`. |
| Workspace all-target/all-feature Clippy | Pending; the preserved earlier attempt ran out of disk while compiling DuckDB. |
| Provider and principal authority | Untested and not admitted. |

The first broad integration run failed three existing tests that expected the
production constructor to admit identity and metastore roots. The metastore
admission remains, while identity admission is limited to the test-only probe;
that probe retains the physical namespace and checkpoint coverage. The failed log is preserved at
`/private/tmp/arco-tenant-identity-integrated-20260926/core-catalog.log`.
