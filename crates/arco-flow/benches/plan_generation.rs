#![allow(missing_docs)]

//! Benchmarks for plan generation operations.
//!
//! These benchmarks measure the performance of deterministic plan generation
//! to ensure they meet the documented performance budgets.

use arco_core::{AssetId, TaskId};
use arco_flow::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
use arco_flow::task_key::TaskOperation;
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn plan_generation_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("plan_generation");

    for task_count in [1_usize, 10, 100] {
        group.bench_function(format!("linear_plan_{task_count}"), |b| {
            b.iter(|| {
                let mut builder = PlanBuilder::new("tenant", "workspace");

                let mut previous: Option<TaskId> = None;
                for i in 0..task_count {
                    let task_id = TaskId::generate();
                    let upstream_task_ids = previous.map_or_else(Vec::new, |id| vec![id]);

                    builder = builder.add_task(TaskSpec {
                        task_id,
                        asset_id: AssetId::generate(),
                        asset_key: AssetKey::new("benchmark", format!("task_{i}")),
                        operation: TaskOperation::Materialize,
                        partition_key: None,
                        upstream_task_ids,
                        stage: 0,
                        priority: 0,
                        resources: ResourceRequirements::default(),
                    });

                    previous = Some(task_id);
                }

                let fingerprint = match builder.build() {
                    Ok(plan) => plan.fingerprint,
                    Err(_) => String::new(),
                };
                black_box(fingerprint);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, plan_generation_benchmark);
criterion_main!(benches);
