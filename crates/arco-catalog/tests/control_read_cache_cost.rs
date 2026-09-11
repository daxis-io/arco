//! Common Gate 5/Gate 6 read-cache measurements.
#![allow(clippy::expect_used)]
#[path = "../benches/support/control_cost.rs"]
#[allow(dead_code)]
mod control_cost;

#[tokio::test]
#[ignore = "explicit Gate 6 baseline/candidate timing repetitions"]
async fn read_cache_cost_comparison() {
    let report = control_cost::read_cache_cost::run(configure, statistics).await;
    let path = std::env::var("ARCO_READ_CACHE_REPORT").expect("report path");
    std::fs::write(path, serde_json::to_vec_pretty(&report).expect("report")).expect("write");
}

#[tokio::test]
async fn fully_admitted_warm_reads_do_not_fetch_payloads() {
    let report = control_cost::read_cache_cost::run(configure, statistics).await;
    for op in report["operations"].as_array().expect("operations") {
        if op["phase"].as_str().expect("phase").starts_with("cold_") {
            assert_eq!(op["samples"], 200, "cold tails require 200 observations");
            let phase = op["phase"].as_str().expect("phase");
            assert_eq!(report["cold_fresh_starts"][phase], 200);
        }
        if op["phase"].as_str().expect("phase").starts_with("warm_") {
            assert_eq!(op["backend"]["block_decode_calls"], 0);
            assert_eq!(op["backend"]["metadata_decode_calls"], 0);
            for kind in ["data", "directory", "transaction"] {
                let bytes = op["backend"]["object_reads"][kind]["returned_bytes"]
                    .as_u64()
                    .unwrap_or(0);
                assert_eq!(bytes, 0, "warm eligible {kind} must be cached");
            }
        }
    }
}

fn configure(store: arco_catalog::ControlMvpStateStore) -> arco_catalog::ControlMvpStateStore {
    match std::env::var("ARCO_READ_CACHE_MODE").as_deref() {
        Ok("disabled") => store.without_read_cache(),
        Ok("pressure") => store
            .with_read_cache_config(arco_catalog::ControlMvpReadCacheConfig {
                metadata_bytes: 1024 * 1024,
                decoded_bytes: 4 * 1024 * 1024,
            })
            .expect("valid pressure capacity"),
        _ => store
            .with_read_cache_config(arco_catalog::ControlMvpReadCacheConfig::default())
            .expect("valid default capacity"),
    }
}
fn statistics(store: &arco_catalog::ControlMvpStateStore) -> serde_json::Value {
    store.read_cache().map_or(serde_json::Value::Null, |cache| {
        serde_json::to_value(cache.statistics()).expect("statistics")
    })
}

#[tokio::test]
#[ignore = "explicit working set exceeds the 4 MiB decoded pressure pool"]
async fn eviction_pressure_remains_bounded() {
    let configure = |store: arco_catalog::ControlMvpStateStore| {
        store
            .with_read_cache_config(arco_catalog::ControlMvpReadCacheConfig {
                metadata_bytes: 1024 * 1024,
                decoded_bytes: 4 * 1024 * 1024,
            })
            .expect("valid pressure capacity")
    };
    let report = control_cost::read_cache_cost::pressure(configure, statistics).await;
    let cache = &report["cache"];
    assert!(cache["evictions"].as_u64().expect("evictions") > 0);
    for pool in ["metadata", "decoded"] {
        assert!(
            cache[pool]["high_water_bytes"]
                .as_u64()
                .expect("high water")
                <= cache[pool]["capacity_bytes"].as_u64().expect("capacity")
        );
        assert_eq!(cache[pool]["reserved_bytes"], 0);
    }
    assert_eq!(cache["active_loads"], 0);
    assert_eq!(cache["underestimates"], 0);
    if let Ok(path) = std::env::var("ARCO_READ_CACHE_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).expect("report")).expect("write");
    }
}

#[tokio::test]
#[cfg(feature = "test-utils")]
#[ignore = "explicit five-repetition 17-layout Gate 6 matrix"]
async fn read_cache_layout_matrix() {
    let report = control_cost::read_cache_cost::layout_matrix(configure, statistics).await;
    let path = std::env::var("ARCO_READ_CACHE_REPORT").expect("report path");
    std::fs::write(path, serde_json::to_vec_pretty(&report).expect("report")).expect("write");
}

#[tokio::test]
#[cfg(feature = "test-utils")]
#[ignore = "explicit five-repetition maintenance cache comparison"]
async fn read_cache_maintenance_comparison() {
    let mut reports = Vec::new();
    for repetition in 1..=5 {
        let samples = Box::pin(control_cost::durable_fixture_smoke()).await;
        reports.push(serde_json::json!({"repetition":repetition,"samples":samples}));
    }
    let path = std::env::var("ARCO_READ_CACHE_REPORT").expect("report path");
    std::fs::write(path, serde_json::to_vec_pretty(&reports).expect("report")).expect("write");
}

#[tokio::test]
#[ignore = "explicit five-repetition retained history, publication and recovery comparison"]
async fn read_cache_recovery_comparison() {
    let mut reports = Vec::new();
    for repetition in 1..=5 {
        let report = Box::pin(control_cost::run(control_cost::Profile::smoke())).await;
        reports.push(serde_json::json!({"repetition":repetition,"report":report}));
    }
    let path = std::env::var("ARCO_READ_CACHE_REPORT").expect("report path");
    std::fs::write(path, serde_json::to_vec_pretty(&reports).expect("report")).expect("write");
}

#[tokio::test]
#[cfg(feature = "test-utils")]
async fn gc_retention_interpretation_keeps_read_caches_disabled() {
    let report = Box::pin(control_cost::run_durable_lifecycle()).await;
    let sample = report["samples"]
        .as_array()
        .expect("lifecycle samples")
        .iter()
        .find(|sample| sample["schedule"] == "reclamation_invalidation")
        .expect("maintenance retention interpretation");
    assert_eq!(
        sample["measured"]["total"]["phases"]["maintenance-GC-root"]["head_attempts"], 0,
        "GC pin interpretation has no independent worker binding and must stay uncached"
    );
}
