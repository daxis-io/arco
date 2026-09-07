//! Operation-cost evidence for the control store. This executable shares its
//! workload with the ordinary CI smoke test; it never claims provider promotion.
//! Run: `cargo bench -p arco-catalog --bench control_mvp_gate [-- --smoke]`
#![allow(missing_docs, clippy::expect_used, clippy::print_stdout)]

#[path = "support/control_cost.rs"]
mod control_cost;

fn main() {
    let profile = if std::env::args().any(|argument| argument == "--smoke") {
        control_cost::Profile::smoke()
    } else {
        control_cost::Profile::benchmark()
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    #[cfg(feature = "test-utils")]
    if std::env::args().any(|argument| argument == "--scaling") {
        let report = runtime.block_on(Box::pin(control_cost::run_scaling()));
        println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("scaling report")
        );
        return;
    }
    let report = runtime.block_on(control_cost::run(profile));
    // Verify accounting even when the benchmark is invoked outside cargo test.
    let probe = runtime.block_on(control_cost::probe_backend_accounting(1));
    assert_eq!(probe.read_bytes, 11);
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize operation costs")
    );
}
