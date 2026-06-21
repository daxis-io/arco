//! Worker dispatch authentication contract tests.

use arco_flow::dispatch::worker_auth::{WORKER_DISPATCH_SECRET_HEADER, worker_dispatch_headers};

#[test]
fn worker_dispatch_headers_use_custom_secret_header() {
    let headers = worker_dispatch_headers("dispatch-secret").expect("headers");

    assert_eq!(
        headers
            .get(WORKER_DISPATCH_SECRET_HEADER)
            .map(String::as_str),
        Some("dispatch-secret")
    );
    assert!(
        !headers.contains_key("Authorization"),
        "Cloud Tasks owns Authorization when OIDC is enabled"
    );
}

#[test]
fn worker_dispatch_headers_reject_empty_secret() {
    let err = worker_dispatch_headers("  ").expect_err("empty secret must fail");

    assert!(err.to_string().contains("ARCO_FLOW_WORKER_DISPATCH_SECRET"));
}
