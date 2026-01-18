//! Selection semantics (hermetic, deterministic).

use arco_flow::orchestration::{
    AssetGraph, SelectionOptions, build_task_defs_for_selection, compute_selection_fingerprint,
};

#[test]
fn selection_does_not_include_downstream_by_default() {
    let mut graph = AssetGraph::new();
    // a -> b -> c
    graph.insert_asset("a".to_string(), vec![]);
    graph.insert_asset("b".to_string(), vec!["a".to_string()]);
    graph.insert_asset("c".to_string(), vec!["b".to_string()]);

    let tasks =
        build_task_defs_for_selection(&graph, &["a".to_string()], SelectionOptions::none(), None)
            .expect("plan");

    let keys: Vec<_> = tasks.iter().map(|t| t.key.as_str()).collect();
    assert_eq!(keys, vec!["a"]);
    assert!(tasks[0].depends_on.is_empty());
}

#[test]
fn selection_includes_downstream_only_when_explicit() {
    let mut graph = AssetGraph::new();
    graph.insert_asset("a".to_string(), vec![]);
    graph.insert_asset("b".to_string(), vec!["a".to_string()]);
    graph.insert_asset("c".to_string(), vec!["b".to_string()]);

    let tasks = build_task_defs_for_selection(
        &graph,
        &["a".to_string()],
        SelectionOptions {
            include_upstream: false,
            include_downstream: true,
        },
        None,
    )
    .expect("plan");

    let keys: Vec<_> = tasks.iter().map(|t| t.key.as_str()).collect();
    assert_eq!(keys, vec!["a", "b", "c"]);

    let deps_by_key: std::collections::HashMap<_, _> = tasks
        .iter()
        .map(|t| (t.key.as_str(), t.depends_on.clone()))
        .collect();
    assert_eq!(deps_by_key.get("a").unwrap(), &Vec::<String>::new());
    assert_eq!(deps_by_key.get("b").unwrap(), &vec!["a".to_string()]);
    assert_eq!(deps_by_key.get("c").unwrap(), &vec!["b".to_string()]);
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
    graph.insert_asset("a".to_string(), vec![]);
    graph.insert_asset("b".to_string(), vec!["a".to_string()]);
    graph.insert_asset("c".to_string(), vec!["b".to_string()]);

    let tasks = build_task_defs_for_selection(
        &graph,
        &["c".to_string()],
        SelectionOptions {
            include_upstream: true,
            include_downstream: false,
        },
        None,
    )
    .expect("plan");

    let keys: Vec<_> = tasks.iter().map(|t| t.key.as_str()).collect();
    assert_eq!(keys, vec!["a", "b", "c"]);
}
