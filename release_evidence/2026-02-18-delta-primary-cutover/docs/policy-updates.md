# Delta-Primary Policy Updates Evidence

- Verification date (UTC): 2026-02-19
- Scope: runbook, ADR, and security-scope alignment for the Delta-primary cutover
- Result: PASS

## Runbook policy evidence

- `docs/runbooks/lake-format-policy.md:4` marks the policy active effective `2026-02-18`.
- `docs/runbooks/lake-format-policy.md:7` states Delta is the default and primary table format.
- `docs/runbooks/lake-format-policy.md:8` preserves Iceberg support as secondary compatibility.
- `docs/runbooks/lake-format-policy.md:13` and `docs/runbooks/lake-format-policy.md:14` define API defaulting to `delta` and unknown-format rejection.
- `docs/runbooks/lake-format-policy.md:19` records Python SDK default `IoConfig.format = delta`.

## ADR evidence

- `docs/adr/adr-030-delta-uc-metastore.md:5` is Accepted in the `2026-02-18` Delta-primary release train.
- `docs/adr/adr-030-delta-uc-metastore.md:75` onward defines Delta subsystem behavior and commit coordination constraints.
- `docs/adr/adr-031-unity-catalog-api-facade.md:5` is Accepted in the `2026-02-18` Delta-primary release train.
- `docs/adr/adr-031-unity-catalog-api-facade.md:69` and `docs/adr/adr-031-unity-catalog-api-facade.md:70` require pinned UC OpenAPI parity testing.

## Security scope evidence

- `docs/runbooks/pen-test-scope.md:15` includes UC facade routes in pen-test scope.
- `docs/runbooks/pen-test-scope.md:16` through `docs/runbooks/pen-test-scope.md:18` include Delta commit control-plane endpoints in pen-test scope.

## Verification commands used

```text
nl -ba docs/runbooks/lake-format-policy.md | sed -n '1,120p'
nl -ba docs/adr/adr-030-delta-uc-metastore.md | sed -n '1,80p'
nl -ba docs/adr/adr-031-unity-catalog-api-facade.md | sed -n '1,80p'
nl -ba docs/runbooks/pen-test-scope.md | sed -n '1,120p'
```
