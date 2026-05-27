//! Golden fixture parity between JSON worker contract types and generated proto types.

#![allow(clippy::expect_used)]

use std::fs;
use std::path::PathBuf;

use arco_proto::arco::orchestration::v1::WorkerDispatchEnvelope as ProtoWorkerDispatchEnvelope;
use arco_worker_contract::WorkerDispatchEnvelope as JsonWorkerDispatchEnvelope;
use chrono::DateTime;
use prost_types::{Struct, Timestamp, Value, value};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("worker_protocol")
        .join(name)
}

fn fixture_text(name: &str) -> String {
    fs::read_to_string(fixture_path(name)).expect("read fixture")
}

fn timestamp(value: DateTime<chrono::Utc>) -> Timestamp {
    Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

fn struct_from_json(value: serde_json::Value) -> Struct {
    let mut fields = std::collections::BTreeMap::new();
    if let serde_json::Value::Object(map) = value {
        for (key, value) in map {
            let kind = match value {
                serde_json::Value::String(value) => value::Kind::StringValue(value),
                serde_json::Value::Bool(value) => value::Kind::BoolValue(value),
                serde_json::Value::Number(value) => {
                    value::Kind::NumberValue(value.as_f64().expect("number as f64"))
                }
                serde_json::Value::Null => value::Kind::NullValue(0),
                nested => value::Kind::StringValue(nested.to_string()),
            };
            fields.insert(key, Value { kind: Some(kind) });
        }
    }
    Struct { fields }
}

#[test]
fn canonical_dispatch_fixture_maps_to_generated_proto_shape() {
    let fixture = fixture_text("worker_dispatch_envelope.json");
    let json_contract: JsonWorkerDispatchEnvelope =
        serde_json::from_str(&fixture).expect("parse contract fixture");

    let proto = ProtoWorkerDispatchEnvelope {
        tenant_id: json_contract.tenant_id.clone(),
        workspace_id: json_contract.workspace_id.clone(),
        task_id: json_contract.task_id.clone(),
        task_key: json_contract.task_key.clone(),
        run_id: json_contract.run_id.clone(),
        attempt: json_contract.attempt,
        attempt_id: json_contract.attempt_id.clone(),
        dispatch_id: json_contract.dispatch_id.clone(),
        execution_location_id: json_contract.execution_location_id.clone(),
        worker_queue: json_contract.worker_queue.clone(),
        callback_base_url: json_contract.callback_base_url.clone(),
        task_token: json_contract.task_token.clone(),
        token_expires_at: Some(timestamp(json_contract.token_expires_at)),
        traceparent: json_contract.traceparent.clone(),
        payload: Some(struct_from_json(json_contract.payload)),
    };

    assert_eq!(proto.task_id, json_contract.task_id);
    assert_eq!(proto.task_key, "analytics.daily_sales");
    assert_eq!(proto.execution_location_id.as_deref(), Some("local-dev"));
    assert!(
        proto
            .payload
            .expect("payload")
            .fields
            .contains_key("partition")
    );
}

#[test]
fn legacy_dispatch_fixture_remains_json_read_compatible() {
    let fixture = fixture_text("worker_dispatch_envelope_legacy_snake_case.json");
    let json_contract: JsonWorkerDispatchEnvelope =
        serde_json::from_str(&fixture).expect("parse legacy contract fixture");

    assert_eq!(json_contract.task_id, "analytics.daily_sales");
    assert_eq!(json_contract.task_key, "analytics.daily_sales");
    assert_eq!(
        json_contract.execution_location_id.as_deref(),
        Some("local-dev")
    );
}
