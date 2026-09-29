# Tenant revocation named-cut probe: local receipts

Base: refreshed `origin/main` at `e8fa1d116106f14696f914df3510e946e0c91fed`,
the merge of PR #445. Its postmerge `Core Tests` check completed successfully
(`gh api`, checked 2026-09-28 local time). Worktree:
`.worktrees/tenant-identity-revocation-20260928`. Toolchain: `rustc 1.88.0`,
`cargo 1.88.0`. The commands below used this worktree's dedicated
`CARGO_TARGET_DIR=$PWD/target`, `CARGO_PROFILE_TEST_DEBUG=0`, and
`CARGO_INCREMENTAL=0`; Clippy and the default-feature check also used
`CARGO_PROFILE_DEV_DEBUG=0`. The ignored `target/receipts/` logs were retained.

## Tested source identity

| Source | SHA-256 |
| --- | --- |
| `crates/arco-catalog/src/authz/compiler.rs` | `44509bc1f4d14372420f8ec1093018b28cfb986d082cb16ce157cd3105596f51` |
| `crates/arco-catalog/src/authz/mod.rs` | `5f718a78ac3d2b1dd7a6d021fe9f61fe6c79c4c8f884cbe4a9aa14855d14055f` |
| `crates/arco-catalog/src/authz/tenant_probe.rs` | `26c4226f1c94336e5c66b3d97657d0be0008e2f09afbfa3b442df541b49b45dc` |
| `crates/arco-catalog/src/state_store/identity_probe.rs` | `60e6d92a8ea1d14174f3f57f4b6a71abf2f4a7ca436910ae375dd6a72216e26b` |
| `crates/arco-catalog/tests/tenant_revocation_probe.rs` | `2b8bbb5eddaa08a553a19f689a6f016ab4ed0b57bc2358b3c1f12a422496d524` |
| [Follow-up gates](../plans/2026-09-27-tenant-identity-principal-follow-up-gates.md) | `438dc00f015c80a1f0b434ae6900a805460da1af823ac35d3cb71ec7e26afab0` |

## Checks

| Check | Result | Retained log SHA-256 |
| --- | --- | --- |
| `cargo fmt --all --check` | passed | `format.log`: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| Focused `arco-catalog` tests: `tenant_revocation_probe`, `identity_principals`, `permission_compilation`, `authz_decisions` | passed: 4 + 3 + 4 + 6 | `focused_final.log`: `e73387c224c28852eed89b23fe230684e33ec2784f12b1562e7d34e34f261e6f` |
| Two-metastore disable case with `--nocapture` | passed; exact tokens and UTC observations below | `revocation_evidence.log`: `814ff813efdede9e3e8c38bcaa23ea6567d4c9e02355315f0b78a3170e1f7a15` |
| `cargo test -p arco-core -p arco-catalog --features arco-catalog/test-utils --locked --quiet` | passed, including 1,115 catalog unit tests and integration, core, and doctests | `core_catalog_final.log`: `8d1c405666be7d09fe2e43adac096ad6fe88ca25e83dfe3c435de33269be4ab4` |
| `cargo test -p arco-catalog --features test-utils --doc --locked` | passed: 3 ordinary and 11 compile-fail; 5 ignored | `rustdoc_final.log`: `e2df46338ebaf2dbd5db5feaad0d3bf195714b261cfb8d13ddf8a0ec7e8a776b` |
| `cargo clippy -p arco-core -p arco-catalog --all-targets --features arco-catalog/test-utils --locked -- -D warnings` | passed | `clippy_final.log`: `b2f9fd7ed19a5f267e5ba6cdd1ee103c0679ae7c0719c7704184314b3aef55b8` |
| `cargo check -p arco-catalog --locked` without `test-utils` | passed; probe module absent | `default_check.log`: `e6aa5ecb7b1c7ace8fd950326b14eca0b324be170b3051f6fa71f5af353ea0fa` |
| `cargo xtask repo-hygiene-check` on staged files | passed | `hygiene.log`: `fa708c1e59e80d38665a1b66d73793bc06a675b9a9b96f511bcba773f52a446d` |
| `git diff --cached --check` | passed | no output |

The captured disable receipt used identity sequence 3 for both compiled cuts
and identity sequence 4 for the disable. The exact manifest IDs were:

| Authority | Manifest ID |
| --- | --- |
| identity cut | `manifest-00000000000000000003-01m3njpk9zcergydx2z55n8vvd-head-4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5-rg-00000000000000000000` |
| disable | `manifest-00000000000000000004-01m3njpkajfs2r9qpry5agjf0h-head-e629fa6598d732768f7c726b4b621285f9c3b85303900aa912017db7617d8bdb-rg-00000000000000000000` |
| first metastore cut | `manifest-00000000000000000001-01m3njpka4ym4na6rjyya812wc-head-e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-rg-00000000000000000000` |
| second metastore cut | `manifest-00000000000000000001-01m3njpka6j5zxhyydrxt1xwpw-head-e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-rg-00000000000000000000` |

The disable commit returned at `2026-09-29T03:17:58.743039+00:00`. The first
metastore denied its cached cut at `03:17:58.746802+00:00`, the second at
`03:17:58.752759+00:00`; both returned `stale_authority_cut`. These are local
`MemoryBackend` observations, not a production freshness bound.

## Retained failures and limits

- The test-first run failed with `E0432` before `tenant_probe` existed. A later
  test-first run failed with `E0061` before the securable hierarchy was moved
  into the token-bound metastore fixture. The historical-read assertion failed
  with `None` before its catalog marker was published. The first Clippy run
  found test-only lint errors; they were corrected. These failures were not
  silently treated as passing receipts.
- The earlier principal-slice [receipt](2026-09-27-tenant-identity-principal-receipts.md)
  records an unchanged restore-counter test failure. This branch's two full
  core/catalog runs passed, but do not establish that intermittent failure is
  fixed. The first full-run log is `core_catalog_full.log`, SHA-256
  `747761c6fb37a0883e3d9dec80f2cea72679837e2e19d1c89739df57bbf9defa`.
- The metastore authorization state and historical catalog marker are test
  fixtures stored under real metastore authority tokens. This does not connect
  the production metastore ledger, catalog reader, grant-write admission, or
  credential vending to tenant identity. Production routing remains closed.
  Five seconds is a test target pending a separate policy decision; no
  production freshness budget is implemented or qualified. Principal purge
  also remains unimplemented, with its retention period undecided.
