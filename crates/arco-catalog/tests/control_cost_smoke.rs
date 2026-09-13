//! The same operation-cost workload as the benchmark, without timing gates.
#![allow(clippy::expect_used, clippy::panic)]

#[path = "../benches/support/control_cost.rs"]
mod control_cost;

#[tokio::test]
async fn operation_cost_smoke_executes_and_validates_historical_contents() {
    assert!(
        control_cost::Profile::benchmark().setup_commits
            > control_cost::Profile::smoke().setup_commits
    );
    let report = control_cost::run(control_cost::Profile::smoke()).await;
    assert_eq!(
        report.allocation_accounting,
        "Rust allocator calls during future polls; excludes spawned work"
    );
    assert!(report.validated_historical_tokens >= 48);
    assert!(report.expected_backpressure_observed);
    for phase in [
        "begin",
        "point_read",
        "scan",
        "commit",
        "contention_loser",
        "maintenance",
        "gc",
        "recovery",
        "ambiguous_commit_reconciliation",
        "retained_validation",
    ] {
        assert!(
            report
                .operations
                .iter()
                .any(|operation| operation.phase == phase && operation.samples > 0),
            "missing operation: {phase}"
        );
    }
    let loser = report
        .operations
        .iter()
        .find(|operation| operation.phase == "contention_loser")
        .expect("loser");
    assert_eq!(loser.backend.head_cas_attempts, 1);
    assert_eq!(loser.backend.precondition_failures, 1);
    assert!(loser.abandoned_immutable_bytes > 0);
    assert_eq!(loser.discarded_candidates, 1);
    assert_eq!(loser.reused_candidates, 0);
    let gc = report
        .operations
        .iter()
        .find(|operation| operation.phase == "gc")
        .expect("gc");
    assert_eq!(gc.backend.delete_attempts, 1);
    assert!(gc.backend.list_page_attempts > 0);
    let commit = report
        .operations
        .iter()
        .find(|operation| operation.phase == "commit")
        .expect("commit");
    assert!(commit.allocations.count > 0 && commit.allocations.bytes > 0);
    assert!(
        commit
            .write_amplification
            .as_ref()
            .is_some_and(|ratio| ratio.numerator > ratio.denominator)
    );
}

#[tokio::test]
async fn operation_cost_accounting_detects_deliberate_read_amplification() {
    let normal = control_cost::probe_backend_accounting(1).await;
    let amplified = control_cost::probe_backend_accounting(2).await;
    assert_eq!(
        normal.get_attempts, 2,
        "successful and failed GETs are both attempted requests"
    );
    assert_eq!(normal.range_get_attempts, 1);
    assert_eq!(
        normal.read_bytes, 11,
        "8 full-object bytes plus a 3-byte range"
    );
    assert_eq!(normal.put_attempts, 2);
    assert_eq!(normal.write_attempt_bytes, 16);
    assert_eq!(normal.precondition_failures, 1);
    assert_eq!(normal.head_attempts, 1);
    assert_eq!(normal.list_page_attempts, 1);
    assert_eq!(normal.list_attempts, 1);
    assert_eq!(normal.delete_attempts, 1);
    assert_eq!(amplified.get_attempts, normal.get_attempts * 2);
    assert_eq!(amplified.read_bytes, normal.read_bytes + 8);
    assert_eq!(
        normal.logical_storage_calls,
        amplified.logical_storage_calls
    );
}

#[tokio::test]
async fn operation_allocation_counts_follow_future_polls_across_suspension() {
    let ((), allocations) = control_cost::measure_allocations(async {
        let bytes = std::hint::black_box(vec![0_u8; 4096]);
        tokio::task::yield_now().await;
        std::hint::black_box(bytes);
    })
    .await;
    assert!(allocations.count >= 1);
    assert!(allocations.bytes >= 4096);
}

#[cfg(feature = "test-utils")]
#[tokio::test]
#[ignore = "explicit scaling lane: cargo test --test control_cost_smoke --features test-utils authenticated_block_scaling_acceptance -- --ignored"]
async fn authenticated_block_scaling_acceptance() {
    let samples = Box::pin(control_cost::run_scaling()).await;
    if let Ok(path) = std::env::var("ARCO_SCALING_REPORT") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&samples).expect("encode scaling report"),
        )
        .expect("write scaling report");
    }
}

#[cfg(feature = "test-utils")]
#[tokio::test]
#[ignore = "explicit Gate 4 lazy transaction scaling lane"]
async fn lazy_transaction_scaling_acceptance() {
    let report = Box::pin(control_cost::run_lazy_scaling()).await;
    if let Ok(path) = std::env::var("ARCO_LAZY_TXN_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).expect("report"))
            .expect("write report");
    }
}
