# Issue 198 UC Support Follow-Through Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close issue #198 by hardening the merged UC support registry follow-through so known unsupported and planned UC operations keep structured `501 NOT_SUPPORTED` behavior under custom mounts, and docs consistently describe the current partial governance surfaces.

**Architecture:** Keep `arco-uc::support` mount-agnostic and match only facade-relative UC paths. The API server should preserve the actual request path in error messages but pass the nested router's current URI path to support matching, so default and custom mount prefixes share the same registry behavior. Documentation should distinguish partial UC adapter behavior from still-planned native governance writer/system-table parity.

**Tech Stack:** Rust, Axum nested routers, `tower::ServiceExt` integration tests, mdBook documentation.

---

### Task 1: Add Failing Custom-Mount Fallback Coverage

**Files:**
- Modify: `crates/arco-integration-tests/tests/unity_catalog_smoke.rs`

**Step 1: Write the failing test**

Add a test proving a known planned UC operation under a custom mount returns structured `501`, not `404`:

```rust
#[tokio::test]
async fn custom_mount_known_uc_gaps_return_structured_501() {
    let mut config = uc_enabled_config();
    config.unity_catalog.mount_prefix = "/uc".to_string();
    let server = ServerBuilder::new().config(config).build();
    let router = server.test_router();

    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/uc/volumes/main.default.raw")
                .header("Authorization", "Bearer dev-token")
                .header("X-Tenant-Id", "acme")
                .header("X-Workspace-Id", "analytics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(payload["error"]["error_code"], "NOT_SUPPORTED");
    let message = payload["error"]["message"].as_str().expect("message");
    assert!(message.contains("GET /uc/volumes/main.default.raw"));
    assert!(message.contains("support-level=planned"));
    assert!(message.contains("route-group=Volumes"));
}
```

Also extend the existing default-mount smoke test to assert `GET /api/2.1/unity-catalog/volumes/main.default.raw` returns `501 NOT_SUPPORTED`, so both mount modes are locked down at the API-server boundary.

**Step 2: Run test to verify it fails**

Run:

```bash
CARGO_TARGET_DIR=/Users/ethanurbanski/arco/target cargo test -p arco-integration-tests --test unity_catalog_smoke custom_mount_known_uc_gaps_return_structured_501 -- --nocapture
```

Expected before implementation: FAIL with `404` or a missing `NOT_SUPPORTED` payload for the custom mount path.

### Task 2: Fix Registry Fallback Matching Without Making Support Mount-Aware

**Files:**
- Modify: `crates/arco-uc/src/router.rs`
- Modify: `crates/arco-uc/src/support.rs` only if a helper signature needs to carry display path separately
- Test: `crates/arco-integration-tests/tests/unity_catalog_smoke.rs`

**Step 1: Implement the minimal fix**

Change the fallback so matching uses the nested router's current URI path, while the error message still reports the original client path. The intended shape is:

```rust
use axum::extract::{OriginalUri, Request};

async fn not_found(method: Method, uri: axum::http::Uri, original_uri: OriginalUri) -> UnityCatalogError {
    let match_path = uri.path();
    let display_path = original_uri.0.path();
    if let Some(message) = crate::support::unsupported_message_for_display(
        &method,
        match_path,
        display_path,
    ) {
        return UnityCatalogError::NotImplemented { message };
    }
    UnityCatalogError::NotFound {
        message: format!("route not found: {display_path}"),
    }
}
```

If Axum extractor support is cleaner with `Request`, use that instead, but keep these invariants:

- Registry matching remains facade-relative.
- Default mount behavior remains unchanged.
- Custom mount known UC gaps return `501 NOT_SUPPORTED`.
- Unknown non-UC paths under custom mounts remain `404`.
- Error messages include the client-visible original path.

**Step 2: Run focused tests**

Run:

```bash
CARGO_TARGET_DIR=/Users/ethanurbanski/arco/target cargo test -p arco-integration-tests --test unity_catalog_smoke -- --nocapture
CARGO_TARGET_DIR=/Users/ethanurbanski/arco/target cargo test -p arco-uc --test support_registry -- --nocapture
```

Expected after implementation: both commands pass.

### Task 3: Align Follow-Through Docs

**Files:**
- Modify: `docs/guide/src/reference/control-plane-scope.md`
- Modify: `docs/guide/src/reference/unity-catalog-openapi-inventory.md` only if wording needs a clarifying cross-reference
- Modify: `docs/guide/src/reference/catalog-api-contract.md` only if the fallback wording needs to mention custom mounts

**Step 1: Update stale scorecard rows**

Revise `control-plane-scope.md` so it no longer says the following are pure placeholder behavior:

- Permissions/authz state
- Storage credentials
- External locations
- Temporary credential vending

Use `Partial` where code now has a real adapter path, and keep the native-governance limitations explicit:

- Writer-backed grant mutation remains planned.
- Provider secret integration remains planned.
- Update/delete lifecycle and broader binding lifecycle remain planned.
- Full route-wide compiled-grant enforcement remains separate work.
- System tables remain planned.

**Step 2: Keep compatibility wording precise**

If needed, add one sentence to the UC inventory or catalog API contract:

```markdown
The `501` contract applies at the configured UC mount prefix; registry matching itself remains facade-relative.
```

Do not claim full UC parity or promote planned object families.

**Step 3: Run docs verification**

Run:

```bash
cd docs/guide && mdbook build
```

Expected: build succeeds.

### Task 4: Final Verification and Review

**Files:**
- All changed files

**Step 1: Run focused verification**

Run:

```bash
CARGO_TARGET_DIR=/Users/ethanurbanski/arco/target cargo test -p arco-integration-tests --test unity_catalog_smoke -- --nocapture
CARGO_TARGET_DIR=/Users/ethanurbanski/arco/target cargo test -p arco-uc --test support_registry -- --nocapture
CARGO_TARGET_DIR=/Users/ethanurbanski/arco/target cargo test -p arco-uc --test openapi_compliance -- --nocapture
cargo fmt --check
git diff --check
cd docs/guide && mdbook build
```

**Step 2: Review checklist**

Confirm:

- Custom UC mount known gaps return structured `501 NOT_SUPPORTED`.
- Unknown custom-mount non-UC paths remain `404`.
- Registry matching stays centralized in `arco_uc::support`.
- Docs distinguish partial UC adapters from planned native governance parity.
- No new UC endpoint family is implemented or documented as callable.

**Step 3: Commit**

```bash
git add crates/arco-uc/src/router.rs crates/arco-uc/src/support.rs crates/arco-integration-tests/tests/unity_catalog_smoke.rs docs/guide/src/reference/control-plane-scope.md docs/guide/src/reference/unity-catalog-openapi-inventory.md docs/guide/src/reference/catalog-api-contract.md docs/plans/2026-05-29-issue-198-uc-support-follow-through.md
git commit -m "fix: harden UC support registry follow-through"
```
