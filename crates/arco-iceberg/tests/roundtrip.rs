//! Roundtrip serde contract tests for Iceberg REST types.

use arco_iceberg::error::IcebergErrorResponse;
use arco_iceberg::types::{
    ConfigResponse, GetNamespaceResponse, ListNamespacesResponse, ListTablesResponse,
    LoadTableResponse, TableCredentialsResponse,
};
use serde_json::Value;

#[test]
fn test_config_response_roundtrip() {
    let json = r#"{"defaults":{},"overrides":{"prefix":"arco","namespace-separator":"%1F"},"idempotency-key-lifetime":"PT1H","endpoints":["GET /v1/config"]}"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: ConfigResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_list_namespaces_roundtrip() {
    let json = r#"{"namespaces":[["db1"],["db2","schema1"]],"next-page-token":"token123"}"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: ListNamespacesResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_list_namespaces_empty_roundtrip() {
    let json = r#"{"namespaces":[]}"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: ListNamespacesResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_list_namespaces_null_token_is_omitted() {
    let json = r#"{"namespaces":[],"next-page-token":null}"#;

    let parsed: ListNamespacesResponse =
        serde_json::from_str(json).expect("deserialization failed");
    assert!(parsed.next_page_token.is_none());

    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");
    assert!(roundtrip.get("next-page-token").is_none());
}

#[test]
fn test_get_namespace_roundtrip() {
    let json = r#"{"namespace":["prod","analytics"],"properties":{"owner":"data-team","location":"gs://bucket/prod/analytics"}}"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: GetNamespaceResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_list_tables_roundtrip() {
    let json = r#"{"identifiers":[{"namespace":["db"],"name":"table1"},{"namespace":["db","schema"],"name":"table2"}],"next-page-token":"token-1"}"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: ListTablesResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_load_table_response_roundtrip_minimal() {
    let json = r#"{
        "metadata-location": "gs://bucket/metadata/v1.metadata.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
            "location": "gs://bucket/table",
            "last-sequence-number": 0,
            "last-updated-ms": 1234567890000,
            "last-column-id": 3,
            "current-schema-id": 0,
            "schemas": [],
            "current-snapshot-id": null,
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "properties": {},
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "refs": {},
            "default-sort-order-id": 0,
            "sort-orders": []
        }
    }"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: LoadTableResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_load_table_response_roundtrip() {
    let json = r#"{
        "metadata-location": "gs://bucket/metadata/v1.metadata.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
            "location": "gs://bucket/table",
            "last-sequence-number": 0,
            "last-updated-ms": 1234567890000,
            "last-column-id": 3,
            "current-schema-id": 0,
            "schemas": [],
            "current-snapshot-id": null,
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "properties": {},
            "default-spec-id": 0,
            "partition-specs": [],
            "last-partition-id": 0,
            "refs": {},
            "default-sort-order-id": 0,
            "sort-orders": []
        },
        "config": {"read.default.name-mapping": "true"},
        "storage-credentials": [
            {
                "prefix": "gs://bucket/",
                "config": {
                    "gcs.oauth2.token": "token",
                    "gcs.oauth2.token-expires-at": "2025-01-15T15:00:00Z"
                }
            }
        ]
    }"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: LoadTableResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_table_credentials_roundtrip() {
    let json = r#"{"storage-credentials":[{"prefix":"s3://bucket/warehouse/","config":{"s3.access-key-id":"AKIAIOSFODNN7EXAMPLE","s3.secret-access-key":"secret","s3.session-token":"session","client.region":"us-east-1"}}]}"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: TableCredentialsResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_table_credentials_multiple_prefixes_roundtrip() {
    let json = r#"{"storage-credentials":[{"prefix":"s3://bucket/warehouse/","config":{"s3.access-key-id":"AKIAIOSFODNN7EXAMPLE","s3.secret-access-key":"secret","s3.session-token":"session","client.region":"us-east-1"}},{"prefix":"gs://bucket/","config":{"gcs.oauth2.token":"token","gcs.oauth2.token-expires-at":"2025-01-15T15:00:00Z"}}]}"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: TableCredentialsResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_error_response_roundtrip() {
    let json = r#"{"error":{"message":"Table does not exist: ns.tbl","type":"NoSuchTableException","code":404}}"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: IcebergErrorResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_error_response_roundtrip_unauthorized() {
    let json = r#"{"error":{"message":"Unauthorized","type":"UnauthorizedException","code":401}}"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: IcebergErrorResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_error_response_roundtrip_forbidden() {
    let json = r#"{"error":{"message":"Forbidden","type":"ForbiddenException","code":403}}"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: IcebergErrorResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_error_response_roundtrip_internal() {
    let json =
        r#"{"error":{"message":"Internal error","type":"InternalServerException","code":500}}"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: IcebergErrorResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}

#[test]
fn test_error_response_roundtrip_service_unavailable() {
    let json = r#"{"error":{"message":"Service unavailable","type":"ServiceUnavailableException","code":503}}"#;

    let value: Value = serde_json::from_str(json).expect("parse failed");
    let parsed: IcebergErrorResponse =
        serde_json::from_value(value.clone()).expect("deserialization failed");
    let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

    assert_eq!(roundtrip, value);
}
