# API Reference

## Canonical Contract

The canonical REST contract snapshot is:

- `crates/arco-api/openapi.json`

Any API-facing change should update this file as part of the same pull request.

## Service Implementation

Primary API composition and routing live in:

- `crates/arco-api/src/server.rs`
- `crates/arco-api/src/routes/`

## Auth and Request Context

Authentication and request-context extraction are implemented in:

- `crates/arco-api/src/config.rs`
- `crates/arco-api/src/context.rs`

## API Change Policy

When changing public API behavior:

1. Update code and tests.
2. Update `crates/arco-api/openapi.json`.
3. Update relevant mdBook docs under `docs/guide/src/`.
4. Include migration notes in PR/release notes for breaking or behavior-sensitive changes.
