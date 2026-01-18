//! Selection semantics (hermetic, deterministic).

use arco_flow::orchestration::{
    AssetGraph, SelectionOptions, build_task_defs_for_selection, canonicalize_asset_key,
    compute_selection_fingerprint,
};

#[test]
fn selection_does_not_include_downstream_by_default() {
    let mut graph = AssetGraph::new();
    graph.insert_asset("analytics.a".to_string(), vec![]);
    graph.insert_asset("analytics.b".to_string(), vec!["analytics.a".to_string()]);
    graph.insert_asset("analytics.c".to_string(), vec!["analytics.b".to_string()]);

    let tasks = build_task_defs_for_selection(
        &graph,
        &["analytics.a".to_string()],
        SelectionOptions::none(),
        None,
    )
    .expect("plan");

    let keys: Vec<_> = tasks.iter().map(|t| t.key.as_str()).collect();
    assert_eq!(keys, vec!["analytics.a"]);
    assert!(tasks[0].depends_on.is_empty());
}

#[test]
fn selection_includes_downstream_only_when_explicit() {
    let mut graph = AssetGraph::new();
    graph.insert_asset("analytics.a".to_string(), vec![]);
    graph.insert_asset("analytics.b".to_string(), vec!["analytics.a".to_string()]);
    graph.insert_asset("analytics.c".to_string(), vec!["analytics.b".to_string()]);

    let tasks = build_task_defs_for_selection(
        &graph,
        &["analytics.a".to_string()],
        SelectionOptions {
            include_upstream: false,
            include_downstream: true,
        },
        None,
    )
    .expect("plan");

    let keys: Vec<_> = tasks.iter().map(|t| t.key.as_str()).collect();
    assert_eq!(keys, vec!["analytics.a", "analytics.b", "analytics.c"]);

    let deps_by_key: std::collections::HashMap<_, _> = tasks
        .iter()
        .map(|t| (t.key.as_str(), t.depends_on.clone()))
        .collect();
    assert_eq!(
        deps_by_key.get("analytics.a").unwrap(),
        &Vec::<String>::new()
    );
    assert_eq!(
        deps_by_key.get("analytics.b").unwrap(),
        &vec!["analytics.a".to_string()]
    );
    assert_eq!(
        deps_by_key.get("analytics.c").unwrap(),
        &vec!["analytics.b".to_string()]
    );
}

#[test]
fn selection_fingerprint_is_order_insensitive() {
    let fp1 = compute_selection_fingerprint(
        &[
            "analytics/users".to_string(),
            "analytics.orders".to_string(),
        ],
        SelectionOptions::none(),
    )
    .expect("fingerprint");

    let fp2 = compute_selection_fingerprint(
        &[
            "analytics.orders".to_string(),
            "analytics.users".to_string(),
        ],
        SelectionOptions::none(),
    )
    .expect("fingerprint");

    assert_eq!(fp1, fp2);
}

#[test]
fn selection_includes_upstream_when_requested() {
    let mut graph = AssetGraph::new();
    graph.insert_asset("analytics.a".to_string(), vec![]);
    graph.insert_asset("analytics.b".to_string(), vec!["analytics.a".to_string()]);
    graph.insert_asset("analytics.c".to_string(), vec!["analytics.b".to_string()]);

    let tasks = build_task_defs_for_selection(
        &graph,
        &["analytics.c".to_string()],
        SelectionOptions {
            include_upstream: true,
            include_downstream: false,
        },
        None,
    )
    .expect("plan");

    let keys: Vec<_> = tasks.iter().map(|t| t.key.as_str()).collect();
    assert_eq!(keys, vec!["analytics.a", "analytics.b", "analytics.c"]);
}

#[test]
fn canonicalize_asset_key_rejects_invalid_inputs() {
    let invalid = [
        "analytics/",
        "/users",
        "analytics//users",
        "analytics.users.",
        "analytics..users",
        "analytics.users..daily",
        "analytics/users/extra",
        " analytics.users",
        "analytics.users ",
        "Analytics.users",
        "analytics.Users",
        "analytics.user-name",
        "analytics.user$name",
    ];

    for value in invalid {
        assert!(
            canonicalize_asset_key(value).is_err(),
            "expected invalid asset key: {value}"
        );
    }
}

#[test]
fn canonicalize_asset_key_accepts_dot_or_slash_formats() {
    assert_eq!(
        canonicalize_asset_key("analytics/users").expect("canonicalize"),
        "analytics.users"
    );
    assert_eq!(
        canonicalize_asset_key("analytics/users.daily").expect("canonicalize"),
        "analytics.users.daily"
    );
    assert_eq!(
        canonicalize_asset_key("analytics.users.daily").expect("canonicalize"),
        "analytics.users.daily"
    );
    assert_eq!(
        canonicalize_asset_key("analytics.users").expect("canonicalize"),
        "analytics.users"
    );
}
