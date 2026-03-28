//! Generated protobuf types for Arco.
//!
//! This crate provides Rust types generated from the proto/ definitions.
//! All cross-language contracts are defined via Protobuf.

#![forbid(unsafe_code)]
#![allow(missing_docs)] // Generated code doesn't have docs
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

#[allow(clippy::all, clippy::cargo, clippy::nursery, clippy::pedantic)]
mod generated {
    #![allow(unused_qualifications)]

    // Include generated code; all types are re-exported at crate root.
    include!(concat!(env!("OUT_DIR"), "/arco.v1.rs"));
    include!(concat!(env!("OUT_DIR"), "/arco.v1.serde.rs"));
}

pub use generated::*;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;

    #[test]
    fn test_tenant_id_roundtrip() {
        let tenant = TenantId {
            value: "acme-corp".to_string(),
        };

        assert_eq!(tenant.value, "acme-corp");
    }

    #[test]
    fn test_partition_key_serialization() -> Result<(), prost::DecodeError> {
        use prost::Message;

        let fixture = include_str!("../fixtures/partition_key_v2.json");
        let pk: PartitionKey = serde_json::from_str(fixture).expect("v2 fixture should parse");
        let encoded = pk.encode_to_vec();
        let decoded = PartitionKey::decode(encoded.as_slice())?;
        let original: JsonValue = serde_json::from_str(fixture).expect("fixture should be valid JSON");
        let decoded_json = serde_json::to_value(&decoded).expect("decoded partition key should serialize");

        assert_eq!(decoded_json, original);
        Ok(())
    }

    #[test]
    fn test_request_header_has_all_fields() {
        let header = RequestHeader {
            tenant_id: Some(TenantId {
                value: "acme".into(),
            }),
            workspace_id: Some(WorkspaceId {
                value: "production".into(),
            }),
            trace_parent: "00-abc123-def456-01".into(),
            idempotency_key: "idem_001".into(),
            request_time: None,
            request_id: "req_12345".into(),
        };

        assert!(header.tenant_id.is_some());
        assert!(header.workspace_id.is_some());
        assert!(!header.idempotency_key.is_empty());
        assert!(!header.request_id.is_empty());
    }
}
