//! Generated protobuf types for Arco.
//!
//! This crate provides Rust types generated from the proto/ definitions.
//! All cross-language contracts are defined via Protobuf.

#![forbid(unsafe_code)]
#![allow(missing_docs)] // Generated code doesn't have docs
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

#[allow(clippy::all, clippy::cargo, clippy::nursery, clippy::pedantic)]
mod generated {
    // Include generated code; all types are re-exported at crate root.
    include!(concat!(env!("OUT_DIR"), "/arco.v1.rs"));
}

pub use generated::*;

#[cfg(test)]
mod tests {
    use super::*;

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

        let mut dimensions = std::collections::BTreeMap::new();
        dimensions.insert(
            "date".to_string(),
            ScalarValue {
                value: Some(scalar_value::Value::DateValue("2025-01-15".to_string())),
            },
        );

        let pk = PartitionKey { dimensions };
        let encoded = pk.encode_to_vec();
        let decoded = PartitionKey::decode(encoded.as_slice())?;

        assert_eq!(decoded.dimensions.len(), 1);
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
