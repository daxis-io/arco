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
