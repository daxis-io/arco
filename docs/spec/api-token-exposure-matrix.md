> Status: Draft Phase 0 contract scaffold.
> Implementation status: Describes current baseline and proposed target semantics.
> Architecture status: The control-store path is prototype-approved only, not accepted production architecture.
> Compatibility status: Public/API exposure decisions are unresolved unless explicitly stated in this document.

# API Token Exposure Matrix

This matrix is a draft compatibility decision surface. It does not approve
public `StateToken` response-body exposure by default, and it does not imply that
current APIs already return future control-store tokens.

## Controlled Values

Matrix cells use only these values until a follow-up ADR or compatibility test
settles a surface:

- `yes`
- `no`
- `candidate`
- `internal-only`
- `header-preferred`
- `body-extension-needs-compat-test`
- `not applicable`
- `unknown`

## Matrix

| Surface | StateToken body | StateToken header | Internal-only binding | ProjectionWatermark body | ProjectionWatermark header | Compatibility risk | Required tests | Current decision | Open questions |
|---|---|---|---|---|---|---|---|---|---|
| Arco-native API | `candidate` | `header-preferred` | `no` | `candidate` | `header-preferred` | `unknown` | `yes` | `candidate` | `yes` |
| Internal service call | `no` | `no` | `internal-only` | `no` | `no` | `not applicable` | `yes` | `internal-only` | `yes` |
| Iceberg REST compatibility | `body-extension-needs-compat-test` | `header-preferred` | `candidate` | `body-extension-needs-compat-test` | `header-preferred` | `unknown` | `yes` | `unknown` | `yes` |
| UC-like compatibility | `body-extension-needs-compat-test` | `header-preferred` | `candidate` | `body-extension-needs-compat-test` | `header-preferred` | `unknown` | `yes` | `unknown` | `yes` |
| SQL/system-table surface | `no` | `no` | `no` | `yes` | `no` | `candidate` | `yes` | `candidate` | `yes` |
| Worker/runtime callback surface | `no` | `no` | `internal-only` | `no` | `no` | `unknown` | `yes` | `internal-only` | `yes` |

## Current Baseline Limitations

Current compatibility routes must not claim future control-store token semantics.
Where the current path cannot provide a retained `StateToken`, it should expose
no token or an explicit unsupported condition in a future typed seam.

## Required Test Families

Future compatibility work should add tests for:

- header round-trips and cache/proxy behavior;
- response-body extension tolerance for compatibility clients;
- absence of fake tokens on current-baseline routes;
- token scope mismatch handling;
- stale projection watermark reporting;
- internal-only token propagation across service calls.

These tests are not added in Phase 0 Slice 1.
