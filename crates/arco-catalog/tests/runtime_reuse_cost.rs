//! Runtime request and projection catch-up qualification drivers.
#![cfg(feature = "test-utils")]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
#[path = "../benches/support/control_cost.rs"]
#[allow(dead_code)]
mod control_cost;
use control_cost::runtime_reuse_cost;
fn statistics(bindings: &arco_catalog::CatalogAuthorityBindings) -> serde_json::Value {
    serde_json::to_value(bindings.test_read_cache_statistics()).unwrap()
}
fn write(report: &serde_json::Value) {
    std::fs::write(
        std::env::var("ARCO_RUNTIME_REPORT").unwrap(),
        serde_json::to_vec_pretty(report).unwrap(),
    )
    .unwrap();
}
#[tokio::test]
async fn separate_requests_reuse_payloads_with_fresh_heads() {
    let report = runtime_reuse_cost::runtime(statistics, 1, 2).await;
    for fixture in report.as_array().unwrap() {
        for op in fixture["operations"].as_array().unwrap() {
            if op["phase"].as_str().unwrap().starts_with("warm") {
                assert!(op["backend"]["head_attempts"].as_u64().unwrap() > 0);
                assert_eq!(op["backend"]["block_decode_calls"], 0);
                assert_eq!(op["backend"]["metadata_decode_calls"], 0);
                for kind in ["transaction", "directory", "data"] {
                    assert_eq!(
                        op["backend"]["object_reads"][kind]["returned_bytes"]
                            .as_u64()
                            .unwrap_or(0),
                        0
                    );
                }
            }
        }
        for cache in fixture["cache"].as_array().unwrap() {
            assert_eq!(cache["underestimates"], 0);
            assert_eq!(cache["active_loads"], 0);
        }
    }
}
#[tokio::test]
async fn request_refresh_preserves_old_pagination_cut() {
    runtime_reuse_cost::fresh_authority_and_retained_pagination().await;
}
#[tokio::test]
async fn projection_retry_after_publication_preserves_every_watermark() {
    runtime_reuse_cost::projection(8, 1, true).await;
}
#[tokio::test]
#[ignore = "explicit five repetitions, 200 independent cold and 40 warm observations"]
async fn runtime_measurement() {
    let mut reports = Vec::new();
    let selected = std::env::var("ARCO_RUNTIME_REPETITION")
        .ok()
        .map(|s| s.parse::<usize>().unwrap());
    assert!(selected.is_none_or(|r| (1..=5).contains(&r)));
    for repetition in (1..=5).filter(|r| selected.is_none_or(|selected| selected == *r)) {
        reports.push(serde_json::json!({"repetition":repetition,"fixtures":runtime_reuse_cost::runtime(statistics, 200, 40).await}));
        write(&serde_json::json!(reports));
        eprintln!("runtime repetition {repetition}/5 complete");
    }
    write(&serde_json::json!(reports));
}
#[tokio::test]
#[ignore = "explicit frozen projection matrix and interruption recovery"]
async fn projection_measurement() {
    let mut reports = Vec::new();
    for repetition in 1..=5 {
        for tables in [8, 64] {
            for backlog in [1, 8, 16] {
                for interrupted in [false, true] {
                    reports.push(serde_json::json!({"repetition":repetition,"sample":runtime_reuse_cost::projection(tables, backlog, interrupted).await}));
                    write(&serde_json::json!(reports));
                    eprintln!(
                        "projection repetition={repetition} tables={tables} backlog={backlog} interrupted={interrupted}"
                    );
                }
            }
        }
    }
    write(&serde_json::json!(reports));
}

#[tokio::test]
async fn warm_cache_preserves_object_lifetime_failures() {
    runtime_reuse_cost::warm_cache_cannot_revive_missing_or_replaced_objects().await;
}

#[tokio::test]
async fn cancelled_requests_release_shared_fixed_share_reservations() {
    runtime_reuse_cost::registry_cancellation_keeps_shared_reservations_bounded().await;
}

#[tokio::test]
async fn projection_retry_quarantines_conflicting_manifest_contents() {
    runtime_reuse_cost::conflicting_publication_is_still_quarantined().await;
}

#[tokio::test]
async fn sixteen_pending_intents_drain_through_ack_backpressure() {
    let report = runtime_reuse_cost::projection(8, 16, false).await;
    assert!(report["maintenance_retries"].as_u64().unwrap() > 0);
}
