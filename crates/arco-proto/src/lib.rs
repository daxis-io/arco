//! Generated protobuf types for Arco.

#![forbid(unsafe_code)]
#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

mod codec;

pub use codec::{ProstCodec, ProstDecoder, ProstEncoder};

pub mod arco {
    pub mod common {
        #[allow(
            unused_qualifications,
            deprecated,
            clippy::all,
            clippy::cargo,
            clippy::nursery,
            clippy::pedantic
        )]
        pub mod v1 {
            tonic::include_proto!("arco.common.v1");
        }
    }

    pub mod catalog {
        #[allow(
            unused_qualifications,
            deprecated,
            clippy::all,
            clippy::cargo,
            clippy::nursery,
            clippy::pedantic
        )]
        pub mod v1 {
            tonic::include_proto!("arco.catalog.v1");
        }
    }

    pub mod orchestration {
        #[allow(
            unused_qualifications,
            deprecated,
            clippy::all,
            clippy::cargo,
            clippy::nursery,
            clippy::pedantic
        )]
        pub mod v1 {
            tonic::include_proto!("arco.orchestration.v1");
        }
    }

    pub mod controlplane {
        #[allow(
            unused_qualifications,
            deprecated,
            clippy::all,
            clippy::cargo,
            clippy::nursery,
            clippy::pedantic
        )]
        pub mod v1 {
            tonic::include_proto!("arco.controlplane.v1");
        }
    }
}

pub use arco::catalog::v1::*;
pub use arco::common::v1::*;
pub use arco::controlplane::v1::*;
pub use arco::orchestration::v1::*;

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
                "pending task output must not include published files, stats, timestamp, or error",
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
                "failed task output must not include published files, stats, or timestamp",
            ),
        }
    }
}

impl std::error::Error for TaskOutputContractError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPlaneTransactionContractError {
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
                    || self.row_count.is_some()
                    || self.byte_size.is_some()
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
                    || self.row_count.is_some()
                    || self.byte_size.is_some()
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
        if self.events.is_empty() {
            return Err(ControlPlaneTransactionContractError::EmptyOrchestrationEvents);
        }

        Ok(())
    }
}

impl CommitRootTransactionRequest {
    pub fn validate_contract(&self) -> Result<(), ControlPlaneTransactionContractError> {
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
            materialization_id: Some("mat_01HQXYZ".into()),
            files: Vec::new(),
            row_count: None,
            byte_size: None,
            visibility_state: OutputVisibilityState::Pending as i32,
            published_at: None,
            publish_error: None,
        }
    }

    #[test]
    fn task_output_pending_contract_accepts_empty_published_fields() {
        assert_eq!(pending_output().validate_contract(), Ok(()));
    }

    #[test]
    fn apply_catalog_ddl_requires_operation() {
        let request = ApplyCatalogDdlRequest {
            ddl: Some(CatalogDdlOperation { op: None }),
        };

        assert_eq!(
            request.validate_contract(),
            Err(ControlPlaneTransactionContractError::MissingCatalogDdlOp)
        );
    }
}
