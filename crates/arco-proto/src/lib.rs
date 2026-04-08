//! Generated protobuf types for Arco.
//!
//! This crate provides Rust types generated from the proto/ definitions.
//! All cross-language contracts are defined via Protobuf.

#![forbid(unsafe_code)]
#![allow(missing_docs)] // Generated code doesn't have docs
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

mod codec;
#[allow(
    unused_qualifications,
    deprecated,
    clippy::all,
    clippy::cargo,
    clippy::nursery,
    clippy::pedantic
)]
mod generated {
    // Include generated code; all types are re-exported at crate root.
    include!(concat!(env!("OUT_DIR"), "/arco.v1.rs"));
}

pub use codec::{ProstCodec, ProstDecoder, ProstEncoder};
pub use generated::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutputContractError {
    MissingMaterializationId,
    UnknownVisibilityState(i32),
    UnspecifiedVisibilityState,
    PendingHasPublishedFields,
    VisibleMissingFiles,
    VisibleMissingPublishedAt,
    VisibleHasPublishError,
    FailedMissingPublishError,
    FailedHasPublishedFields,
}

impl std::fmt::Display for TaskOutputContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingMaterializationId => {
                f.write_str("task output must include materialization_id")
            }
            Self::UnknownVisibilityState(value) => {
                write!(f, "task output has unknown visibility_state value: {value}")
            }
            Self::UnspecifiedVisibilityState => {
                f.write_str("task output visibility_state must not be UNSPECIFIED")
            }
            Self::PendingHasPublishedFields => f.write_str(
                "pending task output must not include published files, non-zero stats, timestamp, or error",
            ),
            Self::VisibleMissingFiles => {
                f.write_str("visible task output must include at least one published file")
            }
            Self::VisibleMissingPublishedAt => {
                f.write_str("visible task output must include published_at")
            }
            Self::VisibleHasPublishError => {
                f.write_str("visible task output must not include publish_error")
            }
            Self::FailedMissingPublishError => {
                f.write_str("failed task output must include publish_error")
            }
            Self::FailedHasPublishedFields => f.write_str(
                "failed task output must not include published files, non-zero stats, or timestamp",
            ),
        }
    }
}

impl std::error::Error for TaskOutputContractError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPlaneTransactionContractError {
    MissingHeader,
    MissingCatalogDdl,
    MissingCatalogDdlOp,
    EmptyOrchestrationEvents,
    EmptyRootMutations,
    EmptyRootOrchestrationEvents(usize),
    MissingRootMutationKind(usize),
}

impl std::fmt::Display for ControlPlaneTransactionContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingHeader => f.write_str("request header is required"),
            Self::MissingCatalogDdl => f.write_str("catalog DDL payload is required"),
            Self::MissingCatalogDdlOp => f.write_str("catalog DDL operation is required"),
            Self::EmptyOrchestrationEvents => {
                f.write_str("orchestration batch must include at least one event")
            }
            Self::EmptyRootMutations => {
                f.write_str("root transaction must include at least one mutation")
            }
            Self::EmptyRootOrchestrationEvents(index) => {
                write!(
                    f,
                    "root orchestration mutation at index {index} must include at least one event"
                )
            }
            Self::MissingRootMutationKind(index) => {
                write!(f, "root mutation at index {index} must set kind")
            }
        }
    }
}

impl std::error::Error for ControlPlaneTransactionContractError {}

impl TaskOutput {
    pub fn validate_contract(&self) -> Result<(), TaskOutputContractError> {
        if self.materialization_id.is_none() {
            return Err(TaskOutputContractError::MissingMaterializationId);
        }

        let visibility_state = OutputVisibilityState::try_from(self.visibility_state)
            .map_err(|_| TaskOutputContractError::UnknownVisibilityState(self.visibility_state))?;

        match visibility_state {
            OutputVisibilityState::Unspecified => {
                Err(TaskOutputContractError::UnspecifiedVisibilityState)
            }
            OutputVisibilityState::Pending => {
                if !self.files.is_empty()
                    || self.row_count != 0
                    || self.byte_size != 0
                    || self.published_at.is_some()
                    || self.publish_error.is_some()
                {
                    return Err(TaskOutputContractError::PendingHasPublishedFields);
                }
                Ok(())
            }
            OutputVisibilityState::Visible => {
                if self.files.is_empty() {
                    return Err(TaskOutputContractError::VisibleMissingFiles);
                }
                if self.published_at.is_none() {
                    return Err(TaskOutputContractError::VisibleMissingPublishedAt);
                }
                if self.publish_error.is_some() {
                    return Err(TaskOutputContractError::VisibleHasPublishError);
                }
                Ok(())
            }
            OutputVisibilityState::Failed => {
                if self.publish_error.is_none() {
                    return Err(TaskOutputContractError::FailedMissingPublishError);
                }
                if !self.files.is_empty()
                    || self.row_count != 0
                    || self.byte_size != 0
                    || self.published_at.is_some()
                {
                    return Err(TaskOutputContractError::FailedHasPublishedFields);
                }
                Ok(())
            }
        }
    }
}

impl ApplyCatalogDdlRequest {
    pub fn validate_contract(&self) -> Result<(), ControlPlaneTransactionContractError> {
        if self.header.is_none() {
            return Err(ControlPlaneTransactionContractError::MissingHeader);
        }

        let ddl = self
            .ddl
            .as_ref()
            .ok_or(ControlPlaneTransactionContractError::MissingCatalogDdl)?;

        if ddl.op.is_none() {
            return Err(ControlPlaneTransactionContractError::MissingCatalogDdlOp);
        }

        Ok(())
    }
}

impl CommitOrchestrationBatchRequest {
    pub fn validate_contract(&self) -> Result<(), ControlPlaneTransactionContractError> {
        if self.header.is_none() {
            return Err(ControlPlaneTransactionContractError::MissingHeader);
        }

        if self.events.is_empty() {
            return Err(ControlPlaneTransactionContractError::EmptyOrchestrationEvents);
        }

        Ok(())
    }
}

impl CommitRootTransactionRequest {
    pub fn validate_contract(&self) -> Result<(), ControlPlaneTransactionContractError> {
        if self.header.is_none() {
            return Err(ControlPlaneTransactionContractError::MissingHeader);
        }

        if self.mutations.is_empty() {
            return Err(ControlPlaneTransactionContractError::EmptyRootMutations);
        }

        if let Some(index) = self
            .mutations
            .iter()
            .position(|mutation| mutation.kind.is_none())
        {
            return Err(ControlPlaneTransactionContractError::MissingRootMutationKind(index));
        }

        if let Some(index) = self
            .mutations
            .iter()
            .enumerate()
            .find_map(|(index, mutation)| match mutation.kind.as_ref() {
                Some(domain_mutation::Kind::Orchestration(spec)) if spec.events.is_empty() => {
                    Some(index)
                }
                _ => None,
            })
        {
            return Err(ControlPlaneTransactionContractError::EmptyRootOrchestrationEvents(index));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_output() -> TaskOutput {
        TaskOutput {
            materialization_id: Some(MaterializationId {
                value: "mat_01HQXYZ".into(),
            }),
            files: Vec::new(),
            row_count: 0,
            byte_size: 0,
            visibility_state: OutputVisibilityState::Pending as i32,
            published_at: None,
            publish_error: None,
        }
    }

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

    #[test]
    fn task_output_visibility_contract() {
        let output = pending_output();

        assert_eq!(output.row_count, 0);
        assert_eq!(output.byte_size, 0);
        assert_eq!(
            output.visibility_state,
            OutputVisibilityState::Pending as i32
        );
        assert!(output.published_at.is_none());
        assert!(output.publish_error.is_none());
    }

    #[test]
    fn output_visibility_state_enum_values() {
        assert_eq!(OutputVisibilityState::Unspecified as i32, 0);
        assert_eq!(OutputVisibilityState::Pending as i32, 1);
        assert_eq!(OutputVisibilityState::Visible as i32, 2);
        assert_eq!(OutputVisibilityState::Failed as i32, 3);
    }

    #[test]
    fn task_output_contract_accepts_pending_output() {
        let output = pending_output();

        assert_eq!(output.validate_contract(), Ok(()));
    }

    #[test]
    fn task_output_contract_rejects_unspecified_visibility() {
        let mut output = pending_output();
        output.visibility_state = OutputVisibilityState::Unspecified as i32;

        assert_eq!(
            output.validate_contract(),
            Err(TaskOutputContractError::UnspecifiedVisibilityState)
        );
    }

    #[test]
    fn task_output_contract_rejects_visible_without_published_at() {
        let mut output = pending_output();
        output.visibility_state = OutputVisibilityState::Visible as i32;
        output.files.push(FileEntry {
            path: "s3://bucket/output.parquet".into(),
            size_bytes: 128,
            row_count: 0,
            content_hash: "abc123".into(),
            format: "parquet".into(),
        });
        output.row_count = 0;
        output.byte_size = 128;

        assert_eq!(
            output.validate_contract(),
            Err(TaskOutputContractError::VisibleMissingPublishedAt)
        );
    }

    #[test]
    fn task_output_contract_rejects_failed_without_publish_error() {
        let mut output = pending_output();
        output.visibility_state = OutputVisibilityState::Failed as i32;

        assert_eq!(
            output.validate_contract(),
            Err(TaskOutputContractError::FailedMissingPublishError)
        );
    }

    #[test]
    fn task_output_contract_roundtrips_zero_stats() -> Result<(), prost::DecodeError> {
        use prost::Message;

        let pending_stats = pending_output();
        let pending_encoded = pending_stats.encode_to_vec();
        let pending_decoded = TaskOutput::decode(pending_encoded.as_slice())?;
        assert_eq!(pending_decoded.row_count, 0);
        assert_eq!(pending_decoded.byte_size, 0);

        let mut zero_stats = pending_output();
        zero_stats.visibility_state = OutputVisibilityState::Visible as i32;
        zero_stats.files.push(FileEntry {
            path: "s3://bucket/output.parquet".into(),
            size_bytes: 0,
            row_count: 0,
            content_hash: "def456".into(),
            format: "parquet".into(),
        });
        zero_stats.row_count = 0;
        zero_stats.byte_size = 0;
        zero_stats.published_at = Some(prost_types::Timestamp {
            seconds: 1_742_770_800,
            nanos: 0,
        });
        let zero_encoded = zero_stats.encode_to_vec();
        let zero_decoded = TaskOutput::decode(zero_encoded.as_slice())?;
        assert_eq!(zero_decoded.row_count, 0);
        assert_eq!(zero_decoded.byte_size, 0);

        Ok(())
    }
}
