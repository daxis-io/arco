//! Generated protobuf types for Arco.

#![forbid(unsafe_code)]
#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

mod codec;
mod serde_helpers;

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

// Temporary compatibility re-exports while downstream crates finish migrating to
// the nested `arco::<domain>::v1` public surface.
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
    InvalidOrchestrationEvent(usize, OrchestrationEventContractError),
    EmptyRootMutations,
    MissingRootCatalogDdlOp(usize),
    MissingRootMetastoreMutationOp(usize),
    EmptyRootOrchestrationEvents(usize),
    InvalidRootOrchestrationEvent(usize, usize, OrchestrationEventContractError),
    MissingRootMutationKind(usize),
    MissingRootMetastoreScope(usize),
    InvalidRootMetastoreScope(usize, CatalogControlPlaneScopeContractError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogControlPlaneScopeContractError {
    EmptyTenantId,
    EmptyWorkspaceId,
    EmptyEffectiveMetastoreId,
    EmptyRequestId,
}

impl std::fmt::Display for ControlPlaneTransactionContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingCatalogDdl => f.write_str("catalog DDL payload is required"),
            Self::MissingCatalogDdlOp => f.write_str("catalog DDL operation is required"),
            Self::EmptyOrchestrationEvents => {
                f.write_str("orchestration batch must include at least one event")
            }
            Self::InvalidOrchestrationEvent(index, error) => {
                write!(
                    f,
                    "orchestration event at index {index} is invalid: {error}"
                )
            }
            Self::EmptyRootMutations => {
                f.write_str("root transaction must include at least one mutation")
            }
            Self::MissingRootCatalogDdlOp(index) => {
                write!(
                    f,
                    "root catalog mutation at index {index} must set catalog DDL operation"
                )
            }
            Self::MissingRootMetastoreMutationOp(index) => {
                write!(
                    f,
                    "root metastore mutation at index {index} must set metastore mutation operation"
                )
            }
            Self::EmptyRootOrchestrationEvents(index) => {
                write!(
                    f,
                    "root orchestration mutation at index {index} must include at least one event"
                )
            }
            Self::InvalidRootOrchestrationEvent(mutation_index, event_index, error) => {
                write!(
                    f,
                    "root orchestration mutation at index {mutation_index} has invalid event at index {event_index}: {error}"
                )
            }
            Self::MissingRootMutationKind(index) => {
                write!(f, "root mutation at index {index} must set kind")
            }
            Self::MissingRootMetastoreScope(index) => {
                write!(
                    f,
                    "root scoped metastore mutation at index {index} must set scope"
                )
            }
            Self::InvalidRootMetastoreScope(index, error) => {
                write!(
                    f,
                    "root scoped metastore mutation at index {index} has invalid scope: {error}"
                )
            }
        }
    }
}

impl std::error::Error for ControlPlaneTransactionContractError {}

impl std::fmt::Display for CatalogControlPlaneScopeContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyTenantId => f.write_str("tenant_id is required"),
            Self::EmptyWorkspaceId => f.write_str("workspace_id is required"),
            Self::EmptyEffectiveMetastoreId => f.write_str("effective metastore_id is required"),
            Self::EmptyRequestId => f.write_str("request_id is required"),
        }
    }
}

impl std::error::Error for CatalogControlPlaneScopeContractError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrchestrationEventContractError {
    EmptyEventId,
    MissingTimestamp,
    EmptySource,
    EmptyIdempotencyKey,
    MissingEventKind,
    MissingTriggerInfo(&'static str),
    MissingTriggerKind(&'static str),
    MissingTriggerField(&'static str, &'static str),
    UnsupportedTriggerKind(&'static str, &'static str),
}

impl std::fmt::Display for OrchestrationEventContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyEventId => f.write_str("event_id is required"),
            Self::MissingTimestamp => f.write_str("timestamp is required"),
            Self::EmptySource => f.write_str("source is required"),
            Self::EmptyIdempotencyKey => f.write_str("idempotency_key is required"),
            Self::MissingEventKind => f.write_str("event payload is required"),
            Self::MissingTriggerInfo(event_name) => {
                write!(f, "{event_name} trigger is required")
            }
            Self::MissingTriggerKind(event_name) => {
                write!(f, "{event_name} trigger.kind is required")
            }
            Self::MissingTriggerField(event_name, field_name) => {
                write!(f, "{event_name} trigger field {field_name} is required")
            }
            Self::UnsupportedTriggerKind(event_name, trigger_kind) => {
                write!(
                    f,
                    "{event_name} trigger kind {trigger_kind} is not supported"
                )
            }
        }
    }
}

impl std::error::Error for OrchestrationEventContractError {}

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

        if let Some((index, error)) =
            self.events.iter().enumerate().find_map(|(index, event)| {
                event.validate_contract().err().map(|error| (index, error))
            })
        {
            return Err(
                ControlPlaneTransactionContractError::InvalidOrchestrationEvent(index, error),
            );
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

        for (index, mutation) in self.mutations.iter().enumerate() {
            match mutation.kind.as_ref() {
                Some(domain_mutation::Kind::Catalog(operation)) => {
                    if operation.op.is_none() {
                        return Err(
                            ControlPlaneTransactionContractError::MissingRootCatalogDdlOp(index),
                        );
                    }
                }
                Some(domain_mutation::Kind::Orchestration(spec)) => {
                    if spec.events.is_empty() {
                        return Err(
                            ControlPlaneTransactionContractError::EmptyRootOrchestrationEvents(
                                index,
                            ),
                        );
                    }

                    if let Some((event_index, error)) =
                        spec.events
                            .iter()
                            .enumerate()
                            .find_map(|(event_index, event)| {
                                event
                                    .validate_contract()
                                    .err()
                                    .map(|error| (event_index, error))
                            })
                    {
                        return Err(
                            ControlPlaneTransactionContractError::InvalidRootOrchestrationEvent(
                                index,
                                event_index,
                                error,
                            ),
                        );
                    }
                }
                Some(domain_mutation::Kind::Metastore(mutation)) => {
                    if !mutation.has_contract_operation() {
                        return Err(
                            ControlPlaneTransactionContractError::MissingRootMetastoreMutationOp(
                                index,
                            ),
                        );
                    }
                }
                Some(domain_mutation::Kind::ScopedMetastore(mutation)) => {
                    if !mutation.has_contract_operation() {
                        return Err(
                            ControlPlaneTransactionContractError::MissingRootMetastoreMutationOp(
                                index,
                            ),
                        );
                    }
                    let scope = mutation.scope.as_ref().ok_or(
                        ControlPlaneTransactionContractError::MissingRootMetastoreScope(index),
                    )?;
                    scope.validate_contract().map_err(|error| {
                        ControlPlaneTransactionContractError::InvalidRootMetastoreScope(
                            index, error,
                        )
                    })?;
                }
                None => unreachable!("checked above"),
            }
        }

        Ok(())
    }
}

impl CatalogControlPlaneScope {
    pub fn effective_metastore_id(&self) -> &str {
        if self.metastore_id.is_empty() {
            self.workspace_id.as_str()
        } else {
            self.metastore_id.as_str()
        }
    }

    pub fn validate_contract(&self) -> Result<(), CatalogControlPlaneScopeContractError> {
        if self.tenant_id.is_empty() {
            return Err(CatalogControlPlaneScopeContractError::EmptyTenantId);
        }
        if self.effective_metastore_id().is_empty() {
            return Err(CatalogControlPlaneScopeContractError::EmptyEffectiveMetastoreId);
        }
        if self.workspace_id.is_empty() {
            return Err(CatalogControlPlaneScopeContractError::EmptyWorkspaceId);
        }
        if self.request_id.is_empty() {
            return Err(CatalogControlPlaneScopeContractError::EmptyRequestId);
        }

        Ok(())
    }
}

impl MetastoreMutation {
    pub fn has_contract_operation(&self) -> bool {
        match self.op.as_ref() {
            Some(metastore_mutation::Op::Grant(grant)) => grant.op.is_some(),
            Some(_) => true,
            None => false,
        }
    }
}

impl ScopedMetastoreMutation {
    pub fn has_contract_operation(&self) -> bool {
        self.mutation
            .as_ref()
            .is_some_and(MetastoreMutation::has_contract_operation)
    }
}

impl OrchestrationEventEnvelope {
    pub fn validate_contract(&self) -> Result<(), OrchestrationEventContractError> {
        if self.event_id.is_empty() {
            return Err(OrchestrationEventContractError::EmptyEventId);
        }
        if self.timestamp.is_none() {
            return Err(OrchestrationEventContractError::MissingTimestamp);
        }
        if self.source.is_empty() {
            return Err(OrchestrationEventContractError::EmptySource);
        }
        if self.idempotency_key.is_empty() {
            return Err(OrchestrationEventContractError::EmptyIdempotencyKey);
        }
        let Some(event) = self.event.as_ref() else {
            return Err(OrchestrationEventContractError::MissingEventKind);
        };

        match event {
            orchestration_event_envelope::Event::RunRequested(event) => {
                validate_run_requested_trigger(event)?;
            }
            orchestration_event_envelope::Event::RunTriggered(event) => {
                validate_run_triggered_trigger(event)?;
            }
            _ => {}
        }

        Ok(())
    }
}

fn required_trigger<'a>(
    event_name: &'static str,
    trigger: Option<&'a TriggerInfo>,
) -> Result<&'a trigger_info::Trigger, OrchestrationEventContractError> {
    let trigger_info = trigger.ok_or(OrchestrationEventContractError::MissingTriggerInfo(
        event_name,
    ))?;
    trigger_info
        .trigger
        .as_ref()
        .ok_or(OrchestrationEventContractError::MissingTriggerKind(
            event_name,
        ))
}

fn validate_run_requested_trigger(
    event: &RunRequested,
) -> Result<(), OrchestrationEventContractError> {
    match required_trigger("run_requested", event.trigger.as_ref())? {
        trigger_info::Trigger::Manual(manual) => {
            if manual.request_id.is_none() {
                return Err(OrchestrationEventContractError::MissingTriggerField(
                    "run_requested",
                    "manual.request_id",
                ));
            }
            Ok(())
        }
        trigger_info::Trigger::Schedule(schedule) => {
            if schedule.tick_id.is_none() {
                return Err(OrchestrationEventContractError::MissingTriggerField(
                    "run_requested",
                    "schedule.tick_id",
                ));
            }
            Ok(())
        }
        trigger_info::Trigger::Sensor(sensor) => {
            if sensor.eval_id.is_none() {
                return Err(OrchestrationEventContractError::MissingTriggerField(
                    "run_requested",
                    "sensor.eval_id",
                ));
            }
            Ok(())
        }
        trigger_info::Trigger::Backfill(backfill) => {
            if backfill.chunk_id.is_none() {
                return Err(OrchestrationEventContractError::MissingTriggerField(
                    "run_requested",
                    "backfill.chunk_id",
                ));
            }
            Ok(())
        }
        trigger_info::Trigger::Materialization(_) => {
            Err(OrchestrationEventContractError::UnsupportedTriggerKind(
                "run_requested",
                "materialization",
            ))
        }
        trigger_info::Trigger::Webhook(_) => Err(
            OrchestrationEventContractError::UnsupportedTriggerKind("run_requested", "webhook"),
        ),
    }
}

fn validate_run_triggered_trigger(
    event: &RunTriggered,
) -> Result<(), OrchestrationEventContractError> {
    match required_trigger("run_triggered", event.trigger.as_ref())? {
        trigger_info::Trigger::Backfill(_) => Err(
            OrchestrationEventContractError::UnsupportedTriggerKind("run_triggered", "backfill"),
        ),
        trigger_info::Trigger::Manual(_)
        | trigger_info::Trigger::Schedule(_)
        | trigger_info::Trigger::Materialization(_)
        | trigger_info::Trigger::Webhook(_)
        | trigger_info::Trigger::Sensor(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GENERATED_CATALOG_RS: &str =
        include_str!(concat!(env!("OUT_DIR"), "/arco.catalog.v1.rs"));
    const GENERATED_ORCHESTRATION_RS: &str =
        include_str!(concat!(env!("OUT_DIR"), "/arco.orchestration.v1.rs"));

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

    #[test]
    fn catalog_proto_does_not_generate_dead_id_based_helper_messages() {
        assert!(
            !GENERATED_CATALOG_RS.contains("pub struct Column "),
            "catalog proto should not generate a dead public Column helper message"
        );
        assert!(
            !GENERATED_CATALOG_RS.contains("pub struct LineageEdge "),
            "catalog proto should not generate a dead public LineageEdge helper message"
        );
    }

    #[test]
    fn orchestration_proto_does_not_generate_partial_progress_json_escape_hatch() {
        assert!(
            !GENERATED_ORCHESTRATION_RS.contains("partial_progress_json"),
            "orchestration proto should not generate a public partial_progress_json field"
        );
    }
}
