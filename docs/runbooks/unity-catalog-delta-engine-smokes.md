# UC + Delta Engine Smoke Runbook

## Purpose

Validate Q2 interoperability core flows for engine-style clients against both:

- UC facade endpoints (`/api/2.1/unity-catalog/*`)
- Arco-native Delta endpoints (`/api/v1/delta/*`)

## Smoke suites

Run from repository root:

```bash
cargo test -p arco-integration-tests --test delta_engine_smoke spark_engine_smoke_uc_and_native_delta
cargo test -p arco-integration-tests --test delta_engine_smoke delta_rs_engine_smoke_uc_and_native_delta
cargo test -p arco-integration-tests --test delta_engine_smoke pyspark_engine_smoke_uc_and_native_delta
```

Or run all engine suites in one shot:

```bash
cargo test -p arco-integration-tests --test delta_engine_smoke
```

## What each suite validates

- UC discover flow: catalog/schema/table create + table lookup.
- UC delta commit flow: `POST` + `GET /delta/preview/commits` with idempotent coordinator semantics.
- Native delta flow: staged payload + committed write through `/api/v1/delta/tables/{table_id}/commits*`.

## CI hook

- `.github/workflows/ci.yml` includes:

```bash
cargo test -p arco-integration-tests --test delta_engine_smoke
```
