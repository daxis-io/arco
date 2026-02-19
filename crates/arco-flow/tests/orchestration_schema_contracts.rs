//! Orchestration Parquet schema contract tests.
//!
//! Mirrors catalog schema contract rigor with checked-in golden files.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct GoldenField {
    name: String,
    data_type: String,
    nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct GoldenSchema {
    name: String,
    version: u32,
    fields: Vec<GoldenField>,
}

fn schema_to_golden(name: &str, schema: &Schema) -> GoldenSchema {
    GoldenSchema {
        name: name.to_string(),
        version: 1,
        fields: schema
            .fields()
            .iter()
            .map(|f| GoldenField {
                name: f.name().clone(),
                data_type: format!("{:?}", f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect(),
    }
}

fn load_golden_schema(name: &str) -> GoldenSchema {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = Path::new(manifest_dir)
        .join("tests")
        .join("golden_schemas")
        .join("orchestration")
        .join(format!("{name}.schema.json"));

    let json = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Golden schema not found: {}. Error: {}", path.display(), e));
    serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("Invalid golden schema JSON at {}: {}", path.display(), e))
}

fn is_backward_compatible(golden: &GoldenSchema, current: &GoldenSchema) -> Result<(), String> {
    let current_fields: HashMap<_, _> = current
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();

    for golden_field in &golden.fields {
        match current_fields.get(golden_field.name.as_str()) {
            Some(current_field) => {
                if golden_field.data_type != current_field.data_type {
                    return Err(format!(
                        "Field '{}' type changed: {} -> {}",
                        golden_field.name, golden_field.data_type, current_field.data_type
                    ));
                }
                if golden_field.nullable && !current_field.nullable {
                    return Err(format!(
                        "Field '{}' changed nullable -> non-nullable",
                        golden_field.name
                    ));
                }
            }
            None => {
                return Err(format!("Field '{}' removed from schema", golden_field.name));
            }
        }
    }

    let golden_names: HashSet<_> = golden.fields.iter().map(|f| f.name.as_str()).collect();
    for current_field in &current.fields {
        if !golden_names.contains(current_field.name.as_str()) && !current_field.nullable {
            return Err(format!(
                "New field '{}' must be nullable",
                current_field.name
            ));
        }
    }

    Ok(())
}

fn current_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        ("runs", arco_flow::orchestration::compactor::run_schema()),
        ("tasks", arco_flow::orchestration::compactor::task_schema()),
        (
            "dep_satisfaction",
            arco_flow::orchestration::compactor::dep_satisfaction_parquet_schema(),
        ),
        (
            "timers",
            arco_flow::orchestration::compactor::timer_schema(),
        ),
        (
            "dispatch_outbox",
            arco_flow::orchestration::compactor::dispatch_outbox_parquet_schema(),
        ),
        (
            "sensor_state",
            arco_flow::orchestration::compactor::sensor_state_parquet_schema(),
        ),
        (
            "sensor_evals",
            arco_flow::orchestration::compactor::sensor_evals_parquet_schema(),
        ),
        (
            "run_key_index",
            arco_flow::orchestration::compactor::run_key_index_parquet_schema(),
        ),
        (
            "run_key_conflicts",
            arco_flow::orchestration::compactor::run_key_conflicts_parquet_schema(),
        ),
        (
            "partition_status",
            arco_flow::orchestration::compactor::partition_status_parquet_schema(),
        ),
        (
            "idempotency_keys",
            arco_flow::orchestration::compactor::idempotency_keys_parquet_schema(),
        ),
    ]
}

#[test]
#[ignore]
fn generate_golden_schemas() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let golden_dir = Path::new(manifest_dir)
        .join("tests")
        .join("golden_schemas")
        .join("orchestration");
    fs::create_dir_all(&golden_dir).expect("create golden schema directory");

    for (name, schema) in current_schemas() {
        let golden = schema_to_golden(name, &schema);
        let path = golden_dir.join(format!("{name}.schema.json"));
        let json = serde_json::to_string_pretty(&golden).expect("serialize golden schema");
        fs::write(path, json).expect("write golden schema");
    }
}

#[test]
fn contract_orchestration_parquet_schemas_backward_compatible() {
    for (name, schema) in current_schemas() {
        let golden = load_golden_schema(name);
        let current = schema_to_golden(name, &schema);
        if let Err(msg) = is_backward_compatible(&golden, &current) {
            panic!("Schema '{name}' is not backward compatible: {msg}");
        }
    }
}
