## Summary

<!-- Brief description of changes -->

## Changes

-

## Testing

- [ ] Tests added/updated
- [ ] `cargo test --workspace` passes
- [ ] `cargo clippy --workspace` passes

---

## Invariant Checklist

Before merging, verify:

### Architecture Invariants

- [ ] Two-tier consistency model preserved (Tier-1 strong via lock+CAS, Tier-2 eventual via append+compact)
- [ ] Tenant/workspace scoping enforced at API boundary (no cross-tenant access)
- [ ] Manifest atomic publish semantics maintained (snapshot not visible until CAS succeeds)
- [ ] Readers never need ledger (all reads from Parquet snapshots)
- [ ] No bucket listing required for correctness (anti-entropy uses listing for repair only)

### Code Quality

- [ ] Schema evolution rules followed (additive-only, version gates for breaking changes)
- [ ] No new hardcoded paths (use `CatalogPaths` module)
- [ ] Idempotency keys supported where applicable
- [ ] No secrets in logs (signed URLs, tokens redacted)

### Verification

- [ ] Invariant tests pass (`cargo test -p arco-test-utils --test tier2`)
- [ ] Schema compatibility tests pass
- [ ] ADR conformance check passes (`cargo xtask adr-check`)

---

## Related Issues

<!-- Link related issues: Fixes #123, Closes #456 -->
