# Gate 3 Summary - Product Surface

Date: 2025-12-31
Status: GO (3.4 out of scope for this audit)

## Summary Table

| Item | Status | Key Evidence |
|------|--------|--------------|
| 3.1 CatalogReader/Writer | IMPLEMENTED | Full facades with idempotency + allowlist |
| 3.2 REST API surface | IMPLEMENTED | OpenAPI validated, auth context, error model |
| 3.3 Browser read path | IMPLEMENTED | Signed URL + DuckDB E2E passing |
| 3.4 SDKs/clients | OUT OF SCOPE | Python SDK planned post-MVP |

## Gate 3 Overall: **GO** (3.4 explicitly out of scope)

## 3.1 CatalogReader/CatalogWriter Facades

**Status: IMPLEMENTED**

| Component | Path | Evidence |
|-----------|------|----------|
| CatalogReader | `crates/arco-catalog/src/reader.rs` | Browser allowlist, signed URL minting |
| CatalogWriter | `crates/arco-catalog/src/writer.rs` | Domain-split architecture, idempotency |
| WriteOptions | `crates/arco-catalog/src/write_options.rs` | Strongly typed idempotency context |
| Concurrency tests | `crates/arco-catalog/tests/concurrent_writers.rs` | Tier-1 idempotency behavior |
| Failure tests | `crates/arco-catalog/tests/failure_injection.rs` | Crash safety + recovery |

## 3.2 REST API Surface

**Status: IMPLEMENTED**

| Component | Path | Evidence |
|-----------|------|----------|
| OpenAPI spec | `crates/arco-api/openapi.json` | Checked-in, versioned |
| OpenAPI generator | `crates/arco-api/src/bin/gen_openapi.rs` | CLI tool |
| Contract test | `crates/arco-api/tests/openapi_contract.rs` | Validates spec matches impl |
| Auth context | `crates/arco-api/src/context.rs` | JWT + debug headers |
| Error model | `crates/arco-api/src/error.rs` | Stable ApiErrorBody |
| Test log | `ci-logs/openapi-contract-test.txt` | PASSED |

## 3.3 Browser Read Path

**Status: IMPLEMENTED**

| Component | Path | Evidence |
|-----------|------|----------|
| Browser route | `crates/arco-api/src/routes/browser.rs` | Signed URL minting |
| CORS config | `crates/arco-api/src/config.rs` | Env-configurable CORS |
| E2E test | `crates/arco-api/tests/browser_e2e.rs` | DuckDB read_parquet over signed URL |
| Test log | `ci-logs/browser-e2e-tests.txt` | 15 tests PASSED |

## 3.4 SDKs/Clients

**Status: OUT OF SCOPE**

The Python SDK exists but is not in scope for this audit. No TS/Rust client work is required for the catalog MVP audit.

### Notes
- OpenAPI spec exists and is valid
- Client generation is planned post-MVP

### Evidence
- `crates/arco-api/openapi.json` - API contract
- `python/arco/pyproject.toml` - Python SDK package (out of scope)

## Recommendation

Gate 3.1–3.3 are **GO** for MVP. 

Gate 3.4 is **OUT OF SCOPE** for this audit; track as post-MVP deliverable.
