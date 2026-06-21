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
    InvalidCatalogDdl(CatalogDdlContractError),
    EmptyOrchestrationEvents,
    InvalidOrchestrationEvent(usize, OrchestrationEventContractError),
    EmptyRootMutations,
    MissingRootCatalogDdlOp(usize),
    InvalidRootCatalogDdl(usize, CatalogDdlContractError),
    MissingRootMetastoreMutationOp(usize),
    InvalidRootMetastoreMutation(usize, MetastoreMutationContractError),
    EmptyRootOrchestrationEvents(usize),
    InvalidRootOrchestrationEvent(usize, usize, OrchestrationEventContractError),
    MissingRootMutationKind(usize),
    MissingRootMetastoreScope(usize),
    InvalidRootMetastoreScope(usize, CatalogControlPlaneScopeContractError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogDdlContractError {
    EmptyField {
        message: &'static str,
        field: &'static str,
    },
    EmptyColumnField {
        column_index: usize,
        field: &'static str,
    },
    UnknownTableFormat(i32),
    UnspecifiedTableFormat(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetastoreMutationContractError {
    MissingOperation,
    MissingGrantOperation,
    EmptyField {
        message: &'static str,
        field: &'static str,
    },
    UnknownLifecycleState(&'static str, i32),
    UnspecifiedLifecycleState(&'static str),
    UnknownPrincipalType(i32),
    UnspecifiedPrincipalType,
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
            Self::InvalidCatalogDdl(error) => {
                write!(f, "catalog DDL operation is invalid: {error}")
            }
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
            Self::InvalidRootCatalogDdl(index, error) => {
                write!(
                    f,
                    "root catalog mutation at index {index} is invalid: {error}"
                )
            }
            Self::MissingRootMetastoreMutationOp(index) => {
                write!(
                    f,
                    "root metastore mutation at index {index} must set metastore mutation operation"
                )
            }
            Self::InvalidRootMetastoreMutation(index, error) => {
                write!(
                    f,
                    "root metastore mutation at index {index} is invalid: {error}"
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

impl std::fmt::Display for CatalogDdlContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyField { message, field } => {
                write!(f, "{message}.{field} is required")
            }
            Self::EmptyColumnField {
                column_index,
                field,
            } => {
                write!(
                    f,
                    "register_table.columns[{column_index}].{field} is required"
                )
            }
            Self::UnknownTableFormat(value) => {
                write!(f, "unknown table format value: {value}")
            }
            Self::UnspecifiedTableFormat(message) => {
                write!(f, "{message}.format must not be TABLE_FORMAT_UNSPECIFIED")
            }
        }
    }
}

impl std::error::Error for CatalogDdlContractError {}

impl std::fmt::Display for MetastoreMutationContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingOperation => f.write_str("metastore mutation operation is required"),
            Self::MissingGrantOperation => f.write_str("grant mutation operation is required"),
            Self::EmptyField { message, field } => {
                write!(f, "{message}.{field} is required")
            }
            Self::UnknownLifecycleState(message, value) => {
                write!(f, "{message}.lifecycle_state has unknown value: {value}")
            }
            Self::UnspecifiedLifecycleState(message) => {
                write!(f, "{message}.lifecycle_state must not be UNSPECIFIED")
            }
            Self::UnknownPrincipalType(value) => {
                write!(f, "principal.principal_type has unknown value: {value}")
            }
            Self::UnspecifiedPrincipalType => {
                f.write_str("principal.principal_type must not be UNSPECIFIED")
            }
        }
    }
}

impl std::error::Error for MetastoreMutationContractError {}

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
    EmptyEventField(&'static str, &'static str),
    ZeroEventField(&'static str, &'static str),
    MissingEventTimestamp(&'static str, &'static str),
    MissingEventMessage(&'static str, &'static str),
    UnknownEventEnum(&'static str, &'static str, i32),
    UnspecifiedEventEnum(&'static str, &'static str),
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
            Self::EmptyEventField(event_name, field_name) => {
                write!(f, "{event_name}.{field_name} is required")
            }
            Self::ZeroEventField(event_name, field_name) => {
                write!(f, "{event_name}.{field_name} must be greater than zero")
            }
            Self::MissingEventTimestamp(event_name, field_name) => {
                write!(f, "{event_name}.{field_name} timestamp is required")
            }
            Self::MissingEventMessage(event_name, field_name) => {
                write!(f, "{event_name}.{field_name} message is required")
            }
            Self::UnknownEventEnum(event_name, field_name, value) => {
                write!(f, "{event_name}.{field_name} has unknown value: {value}")
            }
            Self::UnspecifiedEventEnum(event_name, field_name) => {
                write!(f, "{event_name}.{field_name} must not be UNSPECIFIED")
            }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerCallbackContractError {
    AttemptMustBePositive,
    ProgressPctOutOfRange(u32),
}

impl std::fmt::Display for WorkerCallbackContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AttemptMustBePositive => f.write_str("worker callback attempt must be >= 1"),
            Self::ProgressPctOutOfRange(value) => {
                write!(
                    f,
                    "worker heartbeat progress_pct must be between 0 and 100: {value}"
                )
            }
        }
    }
}

impl std::error::Error for WorkerCallbackContractError {}

fn validate_worker_attempt(attempt: u32) -> Result<(), WorkerCallbackContractError> {
    if attempt == 0 {
        Err(WorkerCallbackContractError::AttemptMustBePositive)
    } else {
        Ok(())
    }
}

impl WorkerDispatchEnvelope {
    pub fn validate_contract(&self) -> Result<(), WorkerCallbackContractError> {
        validate_worker_attempt(self.attempt)
    }
}

impl WorkerTaskStartedRequest {
    pub fn validate_contract(&self) -> Result<(), WorkerCallbackContractError> {
        validate_worker_attempt(self.attempt)
    }
}

impl WorkerHeartbeatRequest {
    pub fn validate_contract(&self) -> Result<(), WorkerCallbackContractError> {
        validate_worker_attempt(self.attempt)?;
        if let Some(progress_pct) = self.progress_pct {
            if progress_pct > 100 {
                return Err(WorkerCallbackContractError::ProgressPctOutOfRange(
                    progress_pct,
                ));
            }
        }
        Ok(())
    }
}

impl WorkerTaskCompletedRequest {
    pub fn validate_contract(&self) -> Result<(), WorkerCallbackContractError> {
        validate_worker_attempt(self.attempt)
    }
}

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
        ddl.validate_contract()
            .map_err(ControlPlaneTransactionContractError::InvalidCatalogDdl)?;

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
                    operation.validate_contract().map_err(|error| {
                        ControlPlaneTransactionContractError::InvalidRootCatalogDdl(index, error)
                    })?;
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
                    mutation.validate_contract().map_err(|error| {
                        ControlPlaneTransactionContractError::InvalidRootMetastoreMutation(
                            index, error,
                        )
                    })?;
                }
                Some(domain_mutation::Kind::ScopedMetastore(mutation)) => {
                    if !mutation.has_contract_operation() {
                        return Err(
                            ControlPlaneTransactionContractError::MissingRootMetastoreMutationOp(
                                index,
                            ),
                        );
                    }
                    mutation.validate_contract().map_err(|error| {
                        ControlPlaneTransactionContractError::InvalidRootMetastoreMutation(
                            index, error,
                        )
                    })?;
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

impl CatalogDdlOperation {
    pub fn validate_contract(&self) -> Result<(), CatalogDdlContractError> {
        let Some(op) = self.op.as_ref() else {
            return Ok(());
        };

        match op {
            catalog_ddl_operation::Op::CreateCatalog(op) => {
                validate_catalog_field("create_catalog", "catalog", &op.catalog)?;
            }
            catalog_ddl_operation::Op::CreateSchema(op) => {
                validate_catalog_field("create_schema", "catalog", &op.catalog)?;
                validate_catalog_field("create_schema", "schema", &op.schema)?;
            }
            catalog_ddl_operation::Op::RegisterTable(op) => {
                validate_catalog_field("register_table", "catalog", &op.catalog)?;
                validate_catalog_field("register_table", "schema", &op.schema)?;
                validate_catalog_field("register_table", "table", &op.table)?;
                if let Some(format) = op.format {
                    validate_table_format_value("register_table", format)?;
                }
                for (index, column) in op.columns.iter().enumerate() {
                    if column.name.is_empty() {
                        return Err(CatalogDdlContractError::EmptyColumnField {
                            column_index: index,
                            field: "name",
                        });
                    }
                    if column.data_type.is_empty() {
                        return Err(CatalogDdlContractError::EmptyColumnField {
                            column_index: index,
                            field: "data_type",
                        });
                    }
                }
            }
            catalog_ddl_operation::Op::UpdateTable(op) => {
                validate_catalog_field("update_table", "catalog", &op.catalog)?;
                validate_catalog_field("update_table", "schema", &op.schema)?;
                validate_catalog_field("update_table", "table", &op.table)?;
                if let Some(format) = op.format {
                    validate_known_table_format_value(format)?;
                }
            }
            catalog_ddl_operation::Op::DropTable(op) => {
                validate_catalog_field("drop_table", "catalog", &op.catalog)?;
                validate_catalog_field("drop_table", "schema", &op.schema)?;
                validate_catalog_field("drop_table", "table", &op.table)?;
            }
            catalog_ddl_operation::Op::RenameTable(op) => {
                validate_catalog_field("rename_table", "catalog", &op.catalog)?;
                validate_catalog_field("rename_table", "schema", &op.schema)?;
                validate_catalog_field("rename_table", "table", &op.table)?;
                validate_catalog_field("rename_table", "new_table", &op.new_table)?;
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

    pub fn validate_contract(&self) -> Result<(), MetastoreMutationContractError> {
        let Some(op) = self.op.as_ref() else {
            return Err(MetastoreMutationContractError::MissingOperation);
        };

        match op {
            metastore_mutation::Op::Grant(mutation) => validate_grant_mutation(mutation),
            metastore_mutation::Op::StorageCredential(value) => validate_storage_credential(value),
            metastore_mutation::Op::ExternalLocation(value) => validate_external_location(value),
            metastore_mutation::Op::WorkspaceBinding(value) => validate_workspace_binding(value),
            metastore_mutation::Op::GovernanceAttachment(value) => {
                validate_governance_attachment(value)
            }
            metastore_mutation::Op::Volume(value) => validate_volume(value),
            metastore_mutation::Op::Function(value) => validate_function(value),
            metastore_mutation::Op::RegisteredModel(value) => validate_registered_model(value),
            metastore_mutation::Op::ModelVersion(value) => validate_model_version(value),
            metastore_mutation::Op::Principal(value) => validate_principal(value),
            metastore_mutation::Op::GroupMembership(value) => validate_group_membership(value),
            metastore_mutation::Op::ServiceCredential(value) => validate_service_credential(value),
            metastore_mutation::Op::ExternalServiceConnection(value) => {
                validate_external_service_connection(value)
            }
            metastore_mutation::Op::ManagedStorageRoot(value) => {
                validate_managed_storage_root(value)
            }
            metastore_mutation::Op::View(value) => validate_view(value),
            metastore_mutation::Op::PolicyAttachment(value) => validate_policy_attachment(value),
            metastore_mutation::Op::Share(value) => validate_share(value),
            metastore_mutation::Op::Provider(value) => validate_provider(value),
            metastore_mutation::Op::Recipient(value) => validate_recipient(value),
        }
    }
}

impl ScopedMetastoreMutation {
    pub fn has_contract_operation(&self) -> bool {
        self.mutation
            .as_ref()
            .is_some_and(MetastoreMutation::has_contract_operation)
    }

    pub fn validate_contract(&self) -> Result<(), MetastoreMutationContractError> {
        self.mutation
            .as_ref()
            .ok_or(MetastoreMutationContractError::MissingOperation)?
            .validate_contract()
    }
}

fn validate_catalog_field(
    message: &'static str,
    field: &'static str,
    value: &str,
) -> Result<(), CatalogDdlContractError> {
    if value.is_empty() {
        Err(CatalogDdlContractError::EmptyField { message, field })
    } else {
        Ok(())
    }
}

fn validate_table_format_value(
    message: &'static str,
    value: i32,
) -> Result<(), CatalogDdlContractError> {
    let format = validate_known_table_format_value(value)?;
    if format == TableFormat::Unspecified {
        Err(CatalogDdlContractError::UnspecifiedTableFormat(message))
    } else {
        Ok(())
    }
}

fn validate_known_table_format_value(value: i32) -> Result<TableFormat, CatalogDdlContractError> {
    TableFormat::try_from(value).map_err(|_| CatalogDdlContractError::UnknownTableFormat(value))
}

fn validate_required_metastore_field(
    message: &'static str,
    field: &'static str,
    value: &str,
) -> Result<(), MetastoreMutationContractError> {
    if value.is_empty() {
        Err(MetastoreMutationContractError::EmptyField { message, field })
    } else {
        Ok(())
    }
}

fn validate_lifecycle_state(
    message: &'static str,
    value: i32,
) -> Result<(), MetastoreMutationContractError> {
    let state = CatalogObjectLifecycleState::try_from(value)
        .map_err(|_| MetastoreMutationContractError::UnknownLifecycleState(message, value))?;
    if state == CatalogObjectLifecycleState::Unspecified {
        Err(MetastoreMutationContractError::UnspecifiedLifecycleState(
            message,
        ))
    } else {
        Ok(())
    }
}

fn validate_principal_type(value: i32) -> Result<(), MetastoreMutationContractError> {
    let principal_type = PrincipalType::try_from(value)
        .map_err(|_| MetastoreMutationContractError::UnknownPrincipalType(value))?;
    if principal_type == PrincipalType::Unspecified {
        Err(MetastoreMutationContractError::UnspecifiedPrincipalType)
    } else {
        Ok(())
    }
}

fn validate_grant_mutation(mutation: &GrantMutation) -> Result<(), MetastoreMutationContractError> {
    let Some(op) = mutation.op.as_ref() else {
        return Err(MetastoreMutationContractError::MissingGrantOperation);
    };
    match op {
        grant_mutation::Op::Grant(value) | grant_mutation::Op::Revoke(value) => {
            validate_grant(value)
        }
    }
}

fn validate_grant(value: &Grant) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("grant", "grant_id", &value.grant_id)?;
    validate_required_metastore_field("grant", "object_id", &value.object_id)?;
    validate_required_metastore_field("grant", "object_type", &value.object_type)?;
    validate_required_metastore_field("grant", "principal", &value.principal)?;
    validate_required_metastore_field("grant", "privilege", &value.privilege)?;
    validate_required_metastore_field("grant", "granted_by", &value.granted_by)?;
    validate_required_metastore_field("grant", "owner", &value.owner)?;
    validate_lifecycle_state("grant", value.lifecycle_state)
}

fn validate_storage_credential(
    value: &StorageCredential,
) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("storage_credential", "credential_id", &value.credential_id)?;
    validate_required_metastore_field("storage_credential", "name", &value.name)?;
    validate_required_metastore_field("storage_credential", "cloud", &value.cloud)?;
    validate_required_metastore_field("storage_credential", "owner", &value.owner)?;
    validate_lifecycle_state("storage_credential", value.lifecycle_state)
}

fn validate_service_credential(
    value: &ServiceCredential,
) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field(
        "service_credential",
        "service_credential_id",
        &value.service_credential_id,
    )?;
    validate_required_metastore_field("service_credential", "name", &value.name)?;
    validate_required_metastore_field("service_credential", "service", &value.service)?;
    validate_required_metastore_field("service_credential", "owner", &value.owner)?;
    validate_lifecycle_state("service_credential", value.lifecycle_state)
}

fn validate_external_location(
    value: &ExternalLocation,
) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("external_location", "location_id", &value.location_id)?;
    validate_required_metastore_field("external_location", "name", &value.name)?;
    validate_required_metastore_field("external_location", "url", &value.url)?;
    validate_required_metastore_field("external_location", "credential_id", &value.credential_id)?;
    validate_required_metastore_field("external_location", "owner", &value.owner)?;
    validate_lifecycle_state("external_location", value.lifecycle_state)
}

fn validate_external_service_connection(
    value: &ExternalServiceConnection,
) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field(
        "external_service_connection",
        "connection_id",
        &value.connection_id,
    )?;
    validate_required_metastore_field("external_service_connection", "name", &value.name)?;
    validate_required_metastore_field(
        "external_service_connection",
        "service_credential_id",
        &value.service_credential_id,
    )?;
    validate_required_metastore_field("external_service_connection", "service", &value.service)?;
    validate_required_metastore_field("external_service_connection", "owner", &value.owner)?;
    validate_lifecycle_state("external_service_connection", value.lifecycle_state)
}

fn validate_managed_storage_root(
    value: &ManagedStorageRoot,
) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("managed_storage_root", "root_id", &value.root_id)?;
    validate_required_metastore_field("managed_storage_root", "name", &value.name)?;
    validate_required_metastore_field("managed_storage_root", "workspace_id", &value.workspace_id)?;
    validate_required_metastore_field("managed_storage_root", "url", &value.url)?;
    validate_required_metastore_field("managed_storage_root", "owner", &value.owner)?;
    validate_lifecycle_state("managed_storage_root", value.lifecycle_state)
}

fn validate_workspace_binding(
    value: &WorkspaceBinding,
) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("workspace_binding", "binding_id", &value.binding_id)?;
    validate_required_metastore_field("workspace_binding", "workspace_id", &value.workspace_id)?;
    validate_required_metastore_field("workspace_binding", "object_id", &value.object_id)?;
    validate_required_metastore_field("workspace_binding", "object_type", &value.object_type)?;
    validate_required_metastore_field("workspace_binding", "owner", &value.owner)?;
    validate_lifecycle_state("workspace_binding", value.lifecycle_state)
}

fn validate_governance_attachment(
    value: &GovernanceAttachment,
) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field(
        "governance_attachment",
        "attachment_id",
        &value.attachment_id,
    )?;
    validate_required_metastore_field("governance_attachment", "object_id", &value.object_id)?;
    validate_required_metastore_field("governance_attachment", "object_type", &value.object_type)?;
    validate_required_metastore_field(
        "governance_attachment",
        "attachment_type",
        &value.attachment_type,
    )?;
    validate_required_metastore_field("governance_attachment", "value", &value.value)?;
    validate_required_metastore_field("governance_attachment", "created_by", &value.created_by)?;
    validate_required_metastore_field("governance_attachment", "owner", &value.owner)?;
    validate_lifecycle_state("governance_attachment", value.lifecycle_state)
}

fn validate_policy_attachment(
    value: &PolicyAttachment,
) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field(
        "policy_attachment",
        "policy_attachment_id",
        &value.policy_attachment_id,
    )?;
    validate_required_metastore_field("policy_attachment", "object_id", &value.object_id)?;
    validate_required_metastore_field("policy_attachment", "object_type", &value.object_type)?;
    validate_required_metastore_field("policy_attachment", "policy_id", &value.policy_id)?;
    validate_required_metastore_field("policy_attachment", "policy_type", &value.policy_type)?;
    validate_required_metastore_field("policy_attachment", "owner", &value.owner)?;
    validate_lifecycle_state("policy_attachment", value.lifecycle_state)
}

fn validate_volume(value: &Volume) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("volume", "volume_id", &value.volume_id)?;
    validate_required_metastore_field("volume", "catalog", &value.catalog)?;
    validate_required_metastore_field("volume", "schema", &value.schema)?;
    validate_required_metastore_field("volume", "volume", &value.volume)?;
    validate_required_metastore_field("volume", "storage_location", &value.storage_location)?;
    validate_required_metastore_field("volume", "owner", &value.owner)?;
    validate_lifecycle_state("volume", value.lifecycle_state)
}

fn validate_function(value: &Function) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("function", "function_id", &value.function_id)?;
    validate_required_metastore_field("function", "catalog", &value.catalog)?;
    validate_required_metastore_field("function", "schema", &value.schema)?;
    validate_required_metastore_field("function", "function", &value.function)?;
    validate_required_metastore_field("function", "owner", &value.owner)?;
    validate_lifecycle_state("function", value.lifecycle_state)
}

fn validate_view(value: &View) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("view", "view_id", &value.view_id)?;
    validate_required_metastore_field("view", "catalog", &value.catalog)?;
    validate_required_metastore_field("view", "schema", &value.schema)?;
    validate_required_metastore_field("view", "view", &value.view)?;
    validate_required_metastore_field("view", "owner", &value.owner)?;
    validate_lifecycle_state("view", value.lifecycle_state)
}

fn validate_registered_model(
    value: &RegisteredModel,
) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("registered_model", "model_id", &value.model_id)?;
    validate_required_metastore_field("registered_model", "catalog", &value.catalog)?;
    validate_required_metastore_field("registered_model", "schema", &value.schema)?;
    validate_required_metastore_field("registered_model", "model", &value.model)?;
    validate_required_metastore_field("registered_model", "owner", &value.owner)?;
    validate_lifecycle_state("registered_model", value.lifecycle_state)
}

fn validate_model_version(value: &ModelVersion) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field(
        "model_version",
        "model_version_id",
        &value.model_version_id,
    )?;
    validate_required_metastore_field("model_version", "model_id", &value.model_id)?;
    validate_required_metastore_field("model_version", "version", &value.version)?;
    validate_required_metastore_field(
        "model_version",
        "storage_location",
        &value.storage_location,
    )?;
    validate_required_metastore_field("model_version", "owner", &value.owner)?;
    validate_lifecycle_state("model_version", value.lifecycle_state)
}

fn validate_share(value: &Share) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("share", "share_id", &value.share_id)?;
    validate_required_metastore_field("share", "name", &value.name)?;
    validate_required_metastore_field("share", "owner", &value.owner)?;
    validate_lifecycle_state("share", value.lifecycle_state)
}

fn validate_provider(value: &Provider) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("provider", "provider_id", &value.provider_id)?;
    validate_required_metastore_field("provider", "name", &value.name)?;
    validate_required_metastore_field("provider", "owner", &value.owner)?;
    validate_lifecycle_state("provider", value.lifecycle_state)
}

fn validate_recipient(value: &Recipient) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("recipient", "recipient_id", &value.recipient_id)?;
    validate_required_metastore_field("recipient", "name", &value.name)?;
    validate_required_metastore_field("recipient", "owner", &value.owner)?;
    validate_lifecycle_state("recipient", value.lifecycle_state)
}

fn validate_principal(value: &Principal) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("principal", "principal_id", &value.principal_id)?;
    validate_principal_type(value.principal_type)?;
    validate_required_metastore_field("principal", "display_name", &value.display_name)?;
    validate_required_metastore_field("principal", "owner", &value.owner)?;
    validate_lifecycle_state("principal", value.lifecycle_state)
}

fn validate_group_membership(
    value: &GroupMembership,
) -> Result<(), MetastoreMutationContractError> {
    validate_required_metastore_field("group_membership", "membership_id", &value.membership_id)?;
    validate_required_metastore_field(
        "group_membership",
        "group_principal_id",
        &value.group_principal_id,
    )?;
    validate_required_metastore_field(
        "group_membership",
        "member_principal_id",
        &value.member_principal_id,
    )?;
    validate_lifecycle_state("group_membership", value.lifecycle_state)
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
            orchestration_event_envelope::Event::RunTriggered(event) => {
                validate_run_triggered_event(event)?;
            }
            orchestration_event_envelope::Event::PlanCreated(event) => {
                validate_plan_created_event(event)?;
            }
            orchestration_event_envelope::Event::RunCancelRequested(event) => {
                validate_run_cancel_requested_event(event)?;
            }
            orchestration_event_envelope::Event::TaskStarted(event) => {
                validate_task_started_event(event)?;
            }
            orchestration_event_envelope::Event::TaskHeartbeat(event) => {
                validate_task_heartbeat_event(event)?;
            }
            orchestration_event_envelope::Event::TaskFinished(event) => {
                validate_task_finished_event(event)?;
            }
            orchestration_event_envelope::Event::TaskCompletionRecorded(event) => {
                validate_task_completion_recorded_event(event)?;
            }
            orchestration_event_envelope::Event::DispatchRequested(event) => {
                validate_dispatch_requested_event(event)?;
            }
            orchestration_event_envelope::Event::TimerRequested(event) => {
                validate_timer_requested_event(event)?;
            }
            orchestration_event_envelope::Event::DispatchEnqueued(event) => {
                validate_dispatch_enqueued_event(event)?;
            }
            orchestration_event_envelope::Event::TimerEnqueued(event) => {
                validate_timer_enqueued_event(event)?;
            }
            orchestration_event_envelope::Event::TimerFired(event) => {
                validate_timer_fired_event(event)?;
            }
            orchestration_event_envelope::Event::ScheduleDefinitionUpserted(event) => {
                validate_schedule_definition_upserted_event(event)?;
            }
            orchestration_event_envelope::Event::ScheduleTicked(event) => {
                validate_schedule_ticked_event(event)?;
            }
            orchestration_event_envelope::Event::SensorEvaluated(event) => {
                validate_sensor_evaluated_event(event)?;
            }
            orchestration_event_envelope::Event::RunRequested(event) => {
                validate_run_requested_event(event)?;
            }
            orchestration_event_envelope::Event::BackfillCreated(event) => {
                validate_backfill_created_event(event)?;
            }
            orchestration_event_envelope::Event::BackfillChunkPlanned(event) => {
                validate_backfill_chunk_planned_event(event)?;
            }
            orchestration_event_envelope::Event::BackfillStateChanged(event) => {
                validate_backfill_state_changed_event(event)?;
            }
            orchestration_event_envelope::Event::TaskOutputVisibilityChanged(event) => {
                validate_task_output_visibility_changed_event(event)?;
            }
        }

        Ok(())
    }
}

fn validate_event_string(
    event_name: &'static str,
    field_name: &'static str,
    value: &str,
) -> Result<(), OrchestrationEventContractError> {
    if value.is_empty() {
        Err(OrchestrationEventContractError::EmptyEventField(
            event_name, field_name,
        ))
    } else {
        Ok(())
    }
}

fn validate_event_optional_string(
    event_name: &'static str,
    field_name: &'static str,
    value: Option<&String>,
) -> Result<(), OrchestrationEventContractError> {
    if value.is_some_and(String::is_empty) {
        Err(OrchestrationEventContractError::EmptyEventField(
            event_name, field_name,
        ))
    } else {
        Ok(())
    }
}

fn validate_event_nonzero_u32(
    event_name: &'static str,
    field_name: &'static str,
    value: u32,
) -> Result<(), OrchestrationEventContractError> {
    if value == 0 {
        Err(OrchestrationEventContractError::ZeroEventField(
            event_name, field_name,
        ))
    } else {
        Ok(())
    }
}

fn validate_event_optional_nonzero_u32(
    event_name: &'static str,
    field_name: &'static str,
    value: Option<u32>,
) -> Result<(), OrchestrationEventContractError> {
    if value == Some(0) {
        Err(OrchestrationEventContractError::ZeroEventField(
            event_name, field_name,
        ))
    } else {
        Ok(())
    }
}

fn validate_event_positive_i64(
    event_name: &'static str,
    field_name: &'static str,
    value: i64,
) -> Result<(), OrchestrationEventContractError> {
    if value <= 0 {
        Err(OrchestrationEventContractError::ZeroEventField(
            event_name, field_name,
        ))
    } else {
        Ok(())
    }
}

fn validate_event_timestamp(
    event_name: &'static str,
    field_name: &'static str,
    value: Option<&prost_types::Timestamp>,
) -> Result<(), OrchestrationEventContractError> {
    if value.is_none() {
        Err(OrchestrationEventContractError::MissingEventTimestamp(
            event_name, field_name,
        ))
    } else {
        Ok(())
    }
}

fn validate_event_enum<TEnum>(
    event_name: &'static str,
    field_name: &'static str,
    value: i32,
    unspecified: &TEnum,
) -> Result<(), OrchestrationEventContractError>
where
    TEnum: TryFrom<i32> + PartialEq,
{
    let parsed = TEnum::try_from(value).map_err(|_| {
        OrchestrationEventContractError::UnknownEventEnum(event_name, field_name, value)
    })?;
    if &parsed == unspecified {
        Err(OrchestrationEventContractError::UnspecifiedEventEnum(
            event_name, field_name,
        ))
    } else {
        Ok(())
    }
}

fn validate_event_string_list(
    event_name: &'static str,
    field_name: &'static str,
    values: &[String],
) -> Result<(), OrchestrationEventContractError> {
    if values.is_empty() || values.iter().any(String::is_empty) {
        Err(OrchestrationEventContractError::EmptyEventField(
            event_name, field_name,
        ))
    } else {
        Ok(())
    }
}

fn validate_task_def(
    event_name: &'static str,
    task: &TaskDef,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string(event_name, "tasks.key", &task.key)?;
    validate_event_nonzero_u32(event_name, "tasks.max_attempts", task.max_attempts)?;
    validate_event_nonzero_u32(
        event_name,
        "tasks.heartbeat_timeout_sec",
        task.heartbeat_timeout_sec,
    )
}

fn validate_task_error(
    event_name: &'static str,
    field_name: &'static str,
    error: Option<&TaskError>,
) -> Result<(), OrchestrationEventContractError> {
    let Some(error) = error else {
        return Ok(());
    };
    validate_event_enum(
        event_name,
        field_name,
        error.category,
        &TaskErrorCategory::Unspecified,
    )?;
    validate_event_string(event_name, "error.message", &error.message)
}

fn validate_trigger_source(
    event_name: &'static str,
    source: Option<&TriggerSource>,
) -> Result<(), OrchestrationEventContractError> {
    let source = source.ok_or(OrchestrationEventContractError::MissingEventMessage(
        event_name,
        "trigger_source",
    ))?;
    match source.source.as_ref() {
        Some(trigger_source::Source::Push(push)) => validate_event_string(
            event_name,
            "trigger_source.push.message_id",
            &push.message_id,
        ),
        Some(trigger_source::Source::Poll(poll)) => validate_event_positive_i64(
            event_name,
            "trigger_source.poll.poll_epoch",
            poll.poll_epoch,
        ),
        None => Err(OrchestrationEventContractError::MissingEventMessage(
            event_name,
            "trigger_source.source",
        )),
    }
}

fn validate_partition_selector(
    event_name: &'static str,
    selector: Option<&PartitionSelector>,
) -> Result<(), OrchestrationEventContractError> {
    let selector = selector.ok_or(OrchestrationEventContractError::MissingEventMessage(
        event_name,
        "partition_selector",
    ))?;
    match selector.selector.as_ref() {
        Some(partition_selector::Selector::Range(range)) => {
            validate_event_string(event_name, "partition_selector.range.start", &range.start)?;
            validate_event_string(event_name, "partition_selector.range.end", &range.end)
        }
        Some(partition_selector::Selector::Explicit(explicit)) => validate_event_string_list(
            event_name,
            "partition_selector.explicit.partition_keys",
            &explicit.partition_keys,
        ),
        Some(partition_selector::Selector::Filter(filter)) => {
            if filter.filters.is_empty()
                || filter
                    .filters
                    .iter()
                    .any(|(key, value)| key.is_empty() || value.is_empty())
            {
                Err(OrchestrationEventContractError::EmptyEventField(
                    event_name,
                    "partition_selector.filter.filters",
                ))
            } else {
                Ok(())
            }
        }
        None => Err(OrchestrationEventContractError::MissingEventMessage(
            event_name,
            "partition_selector.selector",
        )),
    }
}

fn validate_run_triggered_event(
    event: &RunTriggered,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("run_triggered", "run_id", &event.run_id)?;
    validate_event_string("run_triggered", "plan_id", &event.plan_id)?;
    validate_event_optional_string("run_triggered", "run_key", event.run_key.as_ref())?;
    validate_event_optional_string("run_triggered", "code_version", event.code_version.as_ref())?;
    validate_run_triggered_trigger(event)
}

fn validate_plan_created_event(event: &PlanCreated) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("plan_created", "run_id", &event.run_id)?;
    validate_event_string("plan_created", "plan_id", &event.plan_id)?;
    if event.tasks.is_empty() {
        return Err(OrchestrationEventContractError::EmptyEventField(
            "plan_created",
            "tasks",
        ));
    }
    for task in &event.tasks {
        validate_task_def("plan_created", task)?;
    }
    Ok(())
}

fn validate_run_cancel_requested_event(
    event: &RunCancelRequested,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("run_cancel_requested", "run_id", &event.run_id)?;
    validate_event_string("run_cancel_requested", "requested_by", &event.requested_by)?;
    validate_event_optional_string("run_cancel_requested", "reason", event.reason.as_ref())
}

fn validate_task_started_event(event: &TaskStarted) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("task_started", "run_id", &event.run_id)?;
    validate_event_string("task_started", "task_key", &event.task_key)?;
    validate_event_nonzero_u32("task_started", "attempt", event.attempt)?;
    validate_event_string("task_started", "attempt_id", &event.attempt_id)?;
    validate_event_string("task_started", "worker_id", &event.worker_id)
}

fn validate_task_heartbeat_event(
    event: &TaskHeartbeat,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("task_heartbeat", "run_id", &event.run_id)?;
    validate_event_string("task_heartbeat", "task_key", &event.task_key)?;
    validate_event_nonzero_u32("task_heartbeat", "attempt", event.attempt)?;
    validate_event_string("task_heartbeat", "attempt_id", &event.attempt_id)?;
    validate_event_string("task_heartbeat", "worker_id", &event.worker_id)?;
    validate_event_timestamp(
        "task_heartbeat",
        "heartbeat_at",
        event.heartbeat_at.as_ref(),
    )?;
    validate_event_optional_string("task_heartbeat", "message", event.message.as_ref())
}

fn validate_task_finished_event(
    event: &TaskFinished,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("task_finished", "run_id", &event.run_id)?;
    validate_event_string("task_finished", "task_key", &event.task_key)?;
    validate_event_nonzero_u32("task_finished", "attempt", event.attempt)?;
    validate_event_string("task_finished", "attempt_id", &event.attempt_id)?;
    validate_event_string("task_finished", "worker_id", &event.worker_id)?;
    validate_event_enum(
        "task_finished",
        "outcome",
        event.outcome,
        &TaskOutcome::Unspecified,
    )?;
    validate_task_error("task_finished", "error", event.error.as_ref())?;
    validate_event_optional_string(
        "task_finished",
        "cancelled_during_phase",
        event.cancelled_during_phase.as_ref(),
    )?;
    validate_event_optional_string("task_finished", "asset_key", event.asset_key.as_ref())?;
    validate_event_optional_string("task_finished", "code_version", event.code_version.as_ref())
}

fn validate_task_completion_recorded_event(
    event: &TaskCompletionRecorded,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("task_completion_recorded", "run_id", &event.run_id)?;
    validate_event_string("task_completion_recorded", "task_key", &event.task_key)?;
    validate_event_nonzero_u32("task_completion_recorded", "attempt", event.attempt)?;
    validate_event_string("task_completion_recorded", "attempt_id", &event.attempt_id)?;
    validate_event_string("task_completion_recorded", "worker_id", &event.worker_id)?;
    validate_event_enum(
        "task_completion_recorded",
        "outcome",
        event.outcome,
        &TaskOutcome::Unspecified,
    )?;
    validate_task_error("task_completion_recorded", "error", event.error.as_ref())?;
    validate_event_optional_string(
        "task_completion_recorded",
        "cancelled_during_phase",
        event.cancelled_during_phase.as_ref(),
    )?;
    validate_event_optional_string(
        "task_completion_recorded",
        "asset_key",
        event.asset_key.as_ref(),
    )?;
    validate_event_optional_string(
        "task_completion_recorded",
        "code_version",
        event.code_version.as_ref(),
    )?;
    if let Some(update) = event.output_visibility.as_ref() {
        validate_task_output_visibility_update(update)?;
    }
    Ok(())
}

fn validate_task_output_visibility_update(
    update: &TaskOutputVisibilityUpdate,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_enum(
        "task_completion_recorded",
        "output_visibility.visibility_state",
        update.visibility_state,
        &OutputVisibilityState::Unspecified,
    )?;
    validate_event_timestamp(
        "task_completion_recorded",
        "output_visibility.published_at",
        update.published_at.as_ref(),
    )?;
    validate_event_optional_string(
        "task_completion_recorded",
        "output_visibility.publish_error",
        update.publish_error.as_ref(),
    )
}

fn validate_task_output_visibility_changed_event(
    event: &TaskOutputVisibilityChanged,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("task_output_visibility_changed", "run_id", &event.run_id)?;
    validate_event_string(
        "task_output_visibility_changed",
        "task_key",
        &event.task_key,
    )?;
    validate_event_nonzero_u32("task_output_visibility_changed", "attempt", event.attempt)?;
    validate_event_string(
        "task_output_visibility_changed",
        "attempt_id",
        &event.attempt_id,
    )?;
    validate_event_enum(
        "task_output_visibility_changed",
        "visibility_state",
        event.visibility_state,
        &OutputVisibilityState::Unspecified,
    )?;
    validate_event_timestamp(
        "task_output_visibility_changed",
        "published_at",
        event.published_at.as_ref(),
    )?;
    validate_event_optional_string(
        "task_output_visibility_changed",
        "publish_error",
        event.publish_error.as_ref(),
    )
}

fn validate_dispatch_requested_event(
    event: &DispatchRequested,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("dispatch_requested", "run_id", &event.run_id)?;
    validate_event_string("dispatch_requested", "task_key", &event.task_key)?;
    validate_event_nonzero_u32("dispatch_requested", "attempt", event.attempt)?;
    validate_event_string("dispatch_requested", "attempt_id", &event.attempt_id)?;
    validate_event_string("dispatch_requested", "worker_queue", &event.worker_queue)?;
    validate_event_string("dispatch_requested", "dispatch_id", &event.dispatch_id)
}

fn validate_timer_requested_event(
    event: &TimerRequested,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("timer_requested", "timer_id", &event.timer_id)?;
    validate_event_enum(
        "timer_requested",
        "timer_type",
        event.timer_type,
        &TimerType::Unspecified,
    )?;
    validate_event_optional_string("timer_requested", "run_id", event.run_id.as_ref())?;
    validate_event_optional_string("timer_requested", "task_key", event.task_key.as_ref())?;
    validate_event_optional_nonzero_u32("timer_requested", "attempt", event.attempt)?;
    validate_event_timestamp("timer_requested", "fire_at", event.fire_at.as_ref())
}

fn validate_dispatch_enqueued_event(
    event: &DispatchEnqueued,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("dispatch_enqueued", "dispatch_id", &event.dispatch_id)?;
    validate_event_optional_string("dispatch_enqueued", "run_id", event.run_id.as_ref())?;
    validate_event_optional_string("dispatch_enqueued", "task_key", event.task_key.as_ref())?;
    validate_event_optional_nonzero_u32("dispatch_enqueued", "attempt", event.attempt)?;
    validate_event_string("dispatch_enqueued", "cloud_task_id", &event.cloud_task_id)
}

fn validate_timer_enqueued_event(
    event: &TimerEnqueued,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("timer_enqueued", "timer_id", &event.timer_id)?;
    validate_event_optional_string("timer_enqueued", "run_id", event.run_id.as_ref())?;
    validate_event_optional_string("timer_enqueued", "task_key", event.task_key.as_ref())?;
    validate_event_optional_nonzero_u32("timer_enqueued", "attempt", event.attempt)?;
    validate_event_string("timer_enqueued", "cloud_task_id", &event.cloud_task_id)
}

fn validate_timer_fired_event(event: &TimerFired) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("timer_fired", "timer_id", &event.timer_id)?;
    validate_event_enum(
        "timer_fired",
        "timer_type",
        event.timer_type,
        &TimerType::Unspecified,
    )?;
    validate_event_optional_string("timer_fired", "run_id", event.run_id.as_ref())?;
    validate_event_optional_string("timer_fired", "task_key", event.task_key.as_ref())?;
    validate_event_optional_nonzero_u32("timer_fired", "attempt", event.attempt)
}

fn validate_schedule_definition_upserted_event(
    event: &ScheduleDefinitionUpserted,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string(
        "schedule_definition_upserted",
        "schedule_id",
        &event.schedule_id,
    )?;
    validate_event_string(
        "schedule_definition_upserted",
        "cron_expression",
        &event.cron_expression,
    )?;
    validate_event_string("schedule_definition_upserted", "timezone", &event.timezone)
}

fn validate_schedule_ticked_event(
    event: &ScheduleTicked,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("schedule_ticked", "schedule_id", &event.schedule_id)?;
    validate_event_timestamp(
        "schedule_ticked",
        "scheduled_for",
        event.scheduled_for.as_ref(),
    )?;
    validate_event_string("schedule_ticked", "tick_id", &event.tick_id)?;
    validate_event_string(
        "schedule_ticked",
        "definition_version",
        &event.definition_version,
    )?;
    validate_event_enum(
        "schedule_ticked",
        "status",
        event.status,
        &TickStatus::Unspecified,
    )?;
    validate_event_optional_string(
        "schedule_ticked",
        "skipped_reason",
        event.skipped_reason.as_ref(),
    )?;
    validate_event_optional_string(
        "schedule_ticked",
        "failure_message",
        event.failure_message.as_ref(),
    )?;
    validate_event_optional_string("schedule_ticked", "run_key", event.run_key.as_ref())?;
    validate_event_optional_string(
        "schedule_ticked",
        "request_fingerprint",
        event.request_fingerprint.as_ref(),
    )
}

fn validate_sensor_evaluated_event(
    event: &SensorEvaluated,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("sensor_evaluated", "sensor_id", &event.sensor_id)?;
    validate_event_string("sensor_evaluated", "eval_id", &event.eval_id)?;
    validate_event_optional_string(
        "sensor_evaluated",
        "cursor_before",
        event.cursor_before.as_ref(),
    )?;
    validate_event_optional_string(
        "sensor_evaluated",
        "cursor_after",
        event.cursor_after.as_ref(),
    )?;
    validate_event_optional_nonzero_u32(
        "sensor_evaluated",
        "expected_state_version",
        event.expected_state_version,
    )?;
    validate_trigger_source("sensor_evaluated", event.trigger_source.as_ref())?;
    validate_event_enum(
        "sensor_evaluated",
        "status",
        event.status,
        &SensorEvalStatus::Unspecified,
    )?;
    validate_event_optional_string(
        "sensor_evaluated",
        "error_message",
        event.error_message.as_ref(),
    )
}

fn validate_run_requested_event(
    event: &RunRequested,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("run_requested", "run_key", &event.run_key)?;
    validate_event_string(
        "run_requested",
        "request_fingerprint",
        &event.request_fingerprint,
    )?;
    validate_run_requested_trigger(event)
}

fn validate_backfill_created_event(
    event: &BackfillCreated,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("backfill_created", "backfill_id", &event.backfill_id)?;
    validate_event_string(
        "backfill_created",
        "client_request_id",
        &event.client_request_id,
    )?;
    validate_partition_selector("backfill_created", event.partition_selector.as_ref())?;
    validate_event_nonzero_u32(
        "backfill_created",
        "total_partitions",
        event.total_partitions,
    )?;
    validate_event_nonzero_u32("backfill_created", "chunk_size", event.chunk_size)?;
    validate_event_nonzero_u32(
        "backfill_created",
        "max_concurrent_runs",
        event.max_concurrent_runs,
    )?;
    validate_event_optional_string(
        "backfill_created",
        "parent_backfill_id",
        event.parent_backfill_id.as_ref(),
    )
}

fn validate_backfill_chunk_planned_event(
    event: &BackfillChunkPlanned,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("backfill_chunk_planned", "backfill_id", &event.backfill_id)?;
    validate_event_string("backfill_chunk_planned", "chunk_id", &event.chunk_id)?;
    validate_event_string_list(
        "backfill_chunk_planned",
        "partition_keys",
        &event.partition_keys,
    )?;
    validate_event_string("backfill_chunk_planned", "run_key", &event.run_key)?;
    validate_event_string(
        "backfill_chunk_planned",
        "request_fingerprint",
        &event.request_fingerprint,
    )
}

fn validate_backfill_state_changed_event(
    event: &BackfillStateChanged,
) -> Result<(), OrchestrationEventContractError> {
    validate_event_string("backfill_state_changed", "backfill_id", &event.backfill_id)?;
    validate_event_enum(
        "backfill_state_changed",
        "from_state",
        event.from_state,
        &BackfillState::Unspecified,
    )?;
    validate_event_enum(
        "backfill_state_changed",
        "to_state",
        event.to_state,
        &BackfillState::Unspecified,
    )?;
    validate_event_nonzero_u32(
        "backfill_state_changed",
        "state_version",
        event.state_version,
    )?;
    validate_event_optional_string(
        "backfill_state_changed",
        "changed_by",
        event.changed_by.as_ref(),
    )
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
            validate_event_string("run_requested", "manual.user_id", &manual.user_id)?;
            match manual.request_id.as_ref() {
                Some(request_id) if !request_id.is_empty() => Ok(()),
                Some(_) => Err(OrchestrationEventContractError::EmptyEventField(
                    "run_requested",
                    "manual.request_id",
                )),
                None => Err(OrchestrationEventContractError::MissingTriggerField(
                    "run_requested",
                    "manual.request_id",
                )),
            }
        }
        trigger_info::Trigger::Schedule(schedule) => {
            validate_event_string(
                "run_requested",
                "schedule.schedule_id",
                &schedule.schedule_id,
            )?;
            match schedule.tick_id.as_ref() {
                Some(tick_id) if !tick_id.is_empty() => Ok(()),
                Some(_) => Err(OrchestrationEventContractError::EmptyEventField(
                    "run_requested",
                    "schedule.tick_id",
                )),
                None => Err(OrchestrationEventContractError::MissingTriggerField(
                    "run_requested",
                    "schedule.tick_id",
                )),
            }
        }
        trigger_info::Trigger::Sensor(sensor) => {
            validate_event_string("run_requested", "sensor.sensor_id", &sensor.sensor_id)?;
            validate_event_optional_string(
                "run_requested",
                "sensor.cursor",
                sensor.cursor.as_ref(),
            )?;
            match sensor.eval_id.as_ref() {
                Some(eval_id) if !eval_id.is_empty() => Ok(()),
                Some(_) => Err(OrchestrationEventContractError::EmptyEventField(
                    "run_requested",
                    "sensor.eval_id",
                )),
                None => Err(OrchestrationEventContractError::MissingTriggerField(
                    "run_requested",
                    "sensor.eval_id",
                )),
            }
        }
        trigger_info::Trigger::Backfill(backfill) => {
            validate_event_string(
                "run_requested",
                "backfill.backfill_id",
                &backfill.backfill_id,
            )?;
            match backfill.chunk_id.as_ref() {
                Some(chunk_id) if !chunk_id.is_empty() => Ok(()),
                Some(_) => Err(OrchestrationEventContractError::EmptyEventField(
                    "run_requested",
                    "backfill.chunk_id",
                )),
                None => Err(OrchestrationEventContractError::MissingTriggerField(
                    "run_requested",
                    "backfill.chunk_id",
                )),
            }
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
        trigger_info::Trigger::Manual(manual) => {
            validate_event_string("run_triggered", "manual.user_id", &manual.user_id)?;
            validate_event_optional_string(
                "run_triggered",
                "manual.request_id",
                manual.request_id.as_ref(),
            )
        }
        trigger_info::Trigger::Schedule(schedule) => {
            validate_event_string(
                "run_triggered",
                "schedule.schedule_id",
                &schedule.schedule_id,
            )?;
            validate_event_optional_string(
                "run_triggered",
                "schedule.tick_id",
                schedule.tick_id.as_ref(),
            )
        }
        trigger_info::Trigger::Materialization(materialization) => validate_event_string(
            "run_triggered",
            "materialization.upstream_materialization_id",
            &materialization.upstream_materialization_id,
        ),
        trigger_info::Trigger::Webhook(webhook) => {
            validate_event_string("run_triggered", "webhook.webhook_id", &webhook.webhook_id)
        }
        trigger_info::Trigger::Sensor(sensor) => {
            validate_event_string("run_triggered", "sensor.sensor_id", &sensor.sensor_id)?;
            validate_event_optional_string(
                "run_triggered",
                "sensor.cursor",
                sensor.cursor.as_ref(),
            )?;
            validate_event_optional_string(
                "run_triggered",
                "sensor.eval_id",
                sensor.eval_id.as_ref(),
            )
        }
        trigger_info::Trigger::Backfill(_) => Err(
            OrchestrationEventContractError::UnsupportedTriggerKind("run_triggered", "backfill"),
        ),
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
