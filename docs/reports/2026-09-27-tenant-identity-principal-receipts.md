# Tenant identity principal slice: local receipts

Base: refreshed `origin/main` at `17d3e1e5c15ccf6c6a590929d6711c111aeb2f0c`.
PR #441 was `MERGED` at `2026-09-27T14:03:36Z`, merge commit
`7c06862a4d20afb8f0d055a026aab0105de8b1e2` (`gh pr view 441`).
Worktree: `.worktrees/tenant-identity-principals-20260927`.
Toolchain: `rustc 1.88.0`, `cargo 1.88.0`. Build and test commands below used
`CARGO_TARGET_DIR=$PWD/target` in this worktree. Recorded logs remain under
`target/receipts/`; that target is ignored and was not cleaned.

## Source identity

SHA-256 of the final implementation and gate documents tested below:

| File | SHA-256 |
| --- | --- |
| `crates/arco-catalog/src/state_store/control_mvp.rs` | `dfbd5417f68cf65f407a6432207fb01d6342e18510fefd7497072c4ccbe3e7be` |
| `crates/arco-catalog/src/state_store/identity_probe.rs` | `33067eff087e3f6fa3d59a3b4e73ae5ca7a5d08589edb4982cf4b070074f8b68` |
| `crates/arco-catalog/tests/identity_principals.rs` | `6658ec1513b5b7d4e0bd6984af165ec2724b1b937352469f9a95f83e40a3a925` |
| [Roadmap](../plans/2026-06-27-arco-unified-execution-roadmap.md) | `e9f44bc6c76bd9860b564179fe7e9f0c79782b0f7604fb349dfe9150948c9251` |
| [Follow-up gates](../plans/2026-09-27-tenant-identity-principal-follow-up-gates.md) | `522d238d208ab9e8e4123d435ecdc55cb14db69f6e699ca06e598d51bc359372` |

## Gates

| Command (after the target setting above) | Result | Retained log SHA-256 |
| --- | --- | --- |
| `cargo fmt --all --check` | passed | `format_latest.log`: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `cargo test -p arco-catalog --features test-utils --test identity_principals --test identity_store_synthetic` | passed: 3 principal, 5 physical probe tests | `focused_latest.log`: `8afab799bad84aa97dbfb18b92acf303573672ccf608ba9ec57e497a631753f2` |
| `cargo test -p arco-catalog --features test-utils --doc` | passed: 3 ordinary and 11 compile-fail examples; 5 ignored | `rustdoc_latest.log`: `48fe0db92dba79f1eb4308989e4b7d8c5d625f0f9da7c5dbcde2b963f3b7fa3a` |
| `cargo clippy -p arco-core -p arco-catalog --all-targets --features arco-catalog/test-utils -- -D warnings` | passed | `clippy_latest.log`: `767c57c84cdf1d9b155bb4b7e9e93df4355ac6e14aa0ba6c6e02c47055565ca2` |
| `cargo test -p arco-core -p arco-catalog --features arco-catalog/test-utils -- --skip native_counter_capture_keeps_failed_work_and_stops_route` | passed; exactly one catalog unit test filtered out | `core_catalog_default_skip_final.log`: `a3990789b72702206aaba6fb7ff484ec3deac8a7ade032372cd334d063e1f931` |
| `cargo xtask repo-hygiene-check` | passed on the staged report with relative links | `hygiene_staged.log`: `07e436a9e8bc6991a5ae8646b0c9f7fbb84f979492489326a3868ab18382b481` |
| `git diff --check` | passed | `diff_check_latest.log`: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |

## Retained failures

- The test-first run failed with `E0432` because `IdentityMutation`,
  `IdentityOrigin`, and `PrincipalIdentityStore` did not yet exist. The first
  implementation build then failed `missing_docs`; the first Clippy runs found
  `assigning_clones`, `too_many_lines`, and `default_trait_access`. These were
  corrected before the final-source receipts above.
- An unfiltered `cargo test -p arco-core -p arco-catalog --features
  arco-catalog/test-utils --no-default-features` failed after 1114 passing
  catalog unit tests in unchanged
  `native_counter_capture_keeps_failed_work_and_stops_route`. The assertion at
  `restore_io.rs:11470` observed `working_underestimates == 2`, expected `0`.
  Log `core_catalog.log` SHA-256:
  `bbfb2789f3870949685fdd3ec649df948cfc5c1698f2e174e366cb7fe6c92e29`.
- The same no-default run with that test skipped completed catalog tests but
  failed eight `arco-core` JWT tests because `jsonwebtoken` had no process
  `CryptoProvider` without the default crypto feature. Log
  `core_catalog_skip_known.log` SHA-256:
  `a3e4fe6ef942401dca23ac4fb81057960e25aceaea9e3607fd3231a7c6b8bbad`.
- On final source and default features, the unchanged restore counter test
  still fails alone at `restore_io.rs:11470` with observed `2`, expected `0`.
  `cargo test -p arco-catalog --features test-utils --lib
  native_counter_capture_keeps_failed_work_and_stops_route` returned 101.
  Log `restore_counter_latest.log` SHA-256:
  `db9b81c219868896b3faefdcaff40e717ffaf78e168a9a14bdd77657f698ad37`.
- The first hygiene run after staging this report rejected a forbidden literal
  plan-directory path in its source table. The table now uses relative links.
  Log `hygiene_latest.log` SHA-256:
  `f7d3a220641cce1dca9edbe5efc908e6996c5b3396e2afdb3faaf74953ed8ae4`.

The one excluded catalog test is a remaining local gate failure, not a passed
suite. No tenant-wide revocation, principal purge, production routing, provider
qualification, or merge is claimed by these receipts.
