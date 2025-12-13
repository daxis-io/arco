//! Benchmarks for plan generation operations.
//!
//! These benchmarks measure the performance of deterministic plan generation
//! to ensure they meet the documented performance budgets.

use criterion::{Criterion, criterion_group, criterion_main};

fn plan_generation_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("plan_generation");

    group.bench_function("empty_plan", |b| {
        b.iter(|| {
            // TODO: Implement actual plan generation benchmark
            // Target: < 200ms P95 for 100 tasks
            arco_flow::plan::Plan::new()
        });
    });

    group.finish();
}

criterion_group!(benches, plan_generation_benchmark);
criterion_main!(benches);
