//! Durable, direct-addressed handles over existing control-plane transactions.

// Phase 7D deliberately adds this internal composition seam without transport.
// Production builds cannot reach it until a later phase wires a caller, while
// test builds keep ordinary dead-code linting over the complete exercised seam.
#![cfg_attr(not(test), allow(dead_code))]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use ulid::Ulid;
use uuid::Uuid;

use arco_catalog::ColumnDefinition;
use arco_catalog::manifest::CommitRecord;
use arco_core::ScopedStorage;
use arco_core::canonical_json::to_canonical_bytes;
use arco_core::catalog_paths::{CatalogDomain, CatalogPaths};
use arco_core::control_plane_transactions::{
    CatalogTxReceipt, ControlPlaneHandleFailureCategory, ControlPlaneHandleMutationRef,
    ControlPlaneHandleParticipant, ControlPlaneHandleRecord, ControlPlaneHandleScope,
    ControlPlaneHandleStatus, ControlPlaneIdempotencyRecord, ControlPlaneTxDomain,
    ControlPlaneTxKind, ControlPlaneTxPaths, ControlPlaneTxRecord, ControlPlaneTxStatus,
    OrchestrationTxReceipt, RootTxManifest, RootTxManifestDomain, RootTxReceipt, RootTxRecord,
};
use arco_core::storage::{WritePrecondition, WriteResult};
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use arco_flow::orchestration::ledger::LedgerWriter;
use arco_flow::orchestration::proto::event_to_proto_envelope;
use arco_proto::arco::catalog::v1::{
    CatalogDdlOperation, ColumnDefinition as ProtoColumnDefinition, CreateCatalogOp,
    CreateSchemaOp, DropTableOp, RegisterTableOp, RenameTableOp, TableFormat as ProtoTableFormat,
    UpdateTableOp, catalog_ddl_operation,
};
use arco_proto::arco::controlplane::v1::{
    CommitRootTransactionRequest, DomainMutation, OrchestrationBatchSpec, domain_mutation,
};

use super::{
    CatalogMutation, ControlPlaneTransactionService, OrchestrationBatchMutation,
    ResolvedRequestMetadata, RootMutation, root_domain_commit_from_catalog,
    root_domain_commit_from_orchestration, root_request_hash,
};
use crate::context::RequestContext;
use crate::error::ApiError;
use crate::server::AppState;

const STAGED_MUTATION_RECORD_TYPE: &str = "control_plane_transaction_handle_mutation";
const STAGED_MUTATION_RECORD_VERSION: u32 = 1;
const HANDLE_IDENTITY_AUTHORITY_RECORD_TYPE: &str =
    "control_plane_transaction_handle_identity_authority";
const HANDLE_IDENTITY_AUTHORITY_RECORD_VERSION: u32 = 1;
const HANDLE_IDENTITY_AUTHORITY_CAS_ATTEMPTS: usize = 8;
const REVIEW_TOKEN_PREFIX: &str = "review_";

fn parse_canonical_json(bytes: &[u8], label: &str) -> Result<serde_json::Value, ApiError> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|_| ApiError::internal(format!("{label} is corrupt")))?;
    let canonical = to_canonical_bytes(&value)
        .map_err(|_| ApiError::internal(format!("{label} cannot be canonicalized")))?;
    if canonical.as_slice() != bytes {
        return Err(ApiError::internal(format!("{label} is not canonical JSON")));
    }
    Ok(value)
}

/// A high-entropy review secret returned only when its handle is first created.
pub(super) struct ReviewToken(String);

impl ReviewToken {
    fn generate() -> Self {
        Self(format!(
            "{REVIEW_TOKEN_PREFIX}{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        ))
    }

    /// Explicitly exposes the token to the one caller that receives it.
    pub(super) fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for ReviewToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted>")
    }
}

/// Result of the first successful handle creation.
pub(super) struct CreateHandleResult {
    pub(super) handle: ControlPlaneHandleRecord,
    pub(super) review_token: ReviewToken,
}

impl std::fmt::Debug for CreateHandleResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CreateHandleResult")
            .field("handle", &self.handle)
            .field("review_token", &self.review_token)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedHandleMutation {
    record_type: String,
    version: u32,
    handle_id: String,
    scope: ControlPlaneHandleScope,
    ordinal: u64,
    kind: ControlPlaneTxKind,
    mutation: StagedMutation,
}

impl PersistedHandleMutation {
    fn new(
        handle: &ControlPlaneHandleRecord,
        ordinal: u64,
        mutation: StagedMutation,
    ) -> Result<Self, ApiError> {
        let record = Self {
            record_type: STAGED_MUTATION_RECORD_TYPE.to_string(),
            version: STAGED_MUTATION_RECORD_VERSION,
            handle_id: handle.handle_id.clone(),
            scope: handle.scope.clone(),
            ordinal,
            kind: mutation.kind(),
            mutation,
        };
        record.validate(handle)?;
        Ok(record)
    }

    fn from_slice(bytes: &[u8], handle: &ControlPlaneHandleRecord) -> Result<Self, ApiError> {
        let value = parse_canonical_json(bytes, "staged handle mutation")?;
        if value.get("record_type").and_then(serde_json::Value::as_str)
            != Some(STAGED_MUTATION_RECORD_TYPE)
        {
            return Err(ApiError::internal(
                "staged handle mutation has an unsupported record type",
            ));
        }
        if value.get("version").and_then(serde_json::Value::as_u64)
            != Some(u64::from(STAGED_MUTATION_RECORD_VERSION))
        {
            return Err(ApiError::internal(
                "staged handle mutation has an unsupported version",
            ));
        }
        let record: Self = serde_json::from_value(value)
            .map_err(|_| ApiError::internal("staged handle mutation is corrupt"))?;
        record.validate(handle)?;
        Ok(record)
    }

    fn to_vec(&self, handle: &ControlPlaneHandleRecord) -> Result<Vec<u8>, ApiError> {
        self.validate(handle)?;
        to_canonical_bytes(self)
            .map_err(|_| ApiError::internal("failed to encode staged handle mutation"))
    }

    fn validate(&self, handle: &ControlPlaneHandleRecord) -> Result<(), ApiError> {
        if self.record_type != STAGED_MUTATION_RECORD_TYPE
            || self.version != STAGED_MUTATION_RECORD_VERSION
        {
            return Err(ApiError::internal(
                "staged handle mutation contract is unsupported",
            ));
        }
        if self.handle_id != handle.handle_id || self.scope != handle.scope {
            return Err(ApiError::internal(
                "staged handle mutation scope does not match its handle",
            ));
        }
        if self.ordinal == 0 || self.kind != self.mutation.kind() {
            return Err(ApiError::internal(
                "staged handle mutation identity is inconsistent",
            ));
        }
        self.mutation.validate(&handle.scope)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mutation_type", rename_all = "snake_case")]
enum StagedMutation {
    Catalog { operation: StagedCatalogMutation },
    Orchestration { events: Vec<OrchestrationEvent> },
    Root { mutations: Vec<StagedRootMutation> },
}

impl StagedMutation {
    const fn kind(&self) -> ControlPlaneTxKind {
        match self {
            Self::Catalog { .. } => ControlPlaneTxKind::CatalogDdl,
            Self::Orchestration { .. } => ControlPlaneTxKind::OrchestrationBatch,
            Self::Root { .. } => ControlPlaneTxKind::RootCommit,
        }
    }

    const fn domain(&self) -> ControlPlaneTxDomain {
        match self.kind() {
            ControlPlaneTxKind::CatalogDdl => ControlPlaneTxDomain::Catalog,
            ControlPlaneTxKind::OrchestrationBatch => ControlPlaneTxDomain::Orchestration,
            ControlPlaneTxKind::RootCommit => ControlPlaneTxDomain::Root,
        }
    }

    fn validate(&self, scope: &ControlPlaneHandleScope) -> Result<(), ApiError> {
        match self {
            Self::Catalog { operation } => operation.validate(),
            Self::Orchestration { events } => validate_events(events, scope),
            Self::Root { mutations } => {
                if mutations.is_empty() {
                    return Err(ApiError::bad_request(
                        "root handle mutation must contain at least one participant",
                    ));
                }
                let mut catalog = false;
                let mut orchestration = false;
                for mutation in mutations {
                    match mutation {
                        StagedRootMutation::Catalog { operation } => {
                            if std::mem::replace(&mut catalog, true) {
                                return Err(ApiError::bad_request(
                                    "duplicate catalog root mutation is unsupported",
                                ));
                            }
                            operation.validate()?;
                        }
                        StagedRootMutation::Orchestration { events } => {
                            if std::mem::replace(&mut orchestration, true) {
                                return Err(ApiError::bad_request(
                                    "duplicate orchestration root mutation is unsupported",
                                ));
                            }
                            validate_events(events, scope)?;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "domain", rename_all = "snake_case")]
enum StagedRootMutation {
    Catalog { operation: StagedCatalogMutation },
    Orchestration { events: Vec<OrchestrationEvent> },
}

impl StagedRootMutation {
    const fn domain(&self) -> ControlPlaneTxDomain {
        match self {
            Self::Catalog { .. } => ControlPlaneTxDomain::Catalog,
            Self::Orchestration { .. } => ControlPlaneTxDomain::Orchestration,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HandleClaimIdentity {
    domain: ControlPlaneTxDomain,
    kind: ControlPlaneTxKind,
    idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HandleIdentityIntent {
    mutation_ref: ControlPlaneHandleMutationRef,
    claim_identities: Vec<HandleClaimIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HandleIdentityAuthorityRecord {
    record_type: String,
    version: u32,
    handle_id: String,
    scope: ControlPlaneHandleScope,
    ordinal: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    handle_intent: Option<HandleIdentityIntent>,
    #[serde(default)]
    legacy_reservations: Vec<HandleClaimIdentity>,
}

impl HandleIdentityAuthorityRecord {
    fn new(
        handle_id: &str,
        scope: &ControlPlaneHandleScope,
        ordinal: u64,
    ) -> Result<Self, ApiError> {
        let record = Self {
            record_type: HANDLE_IDENTITY_AUTHORITY_RECORD_TYPE.to_string(),
            version: HANDLE_IDENTITY_AUTHORITY_RECORD_VERSION,
            handle_id: handle_id.to_string(),
            scope: scope.clone(),
            ordinal,
            handle_intent: None,
            legacy_reservations: Vec::new(),
        };
        record.validate(handle_id, scope, ordinal)?;
        Ok(record)
    }

    fn from_slice(
        bytes: &[u8],
        handle_id: &str,
        scope: &ControlPlaneHandleScope,
        ordinal: u64,
    ) -> Result<Self, ApiError> {
        let value = parse_canonical_json(bytes, "handle identity authority")?;
        if value.get("record_type").and_then(serde_json::Value::as_str)
            != Some(HANDLE_IDENTITY_AUTHORITY_RECORD_TYPE)
        {
            return Err(ApiError::internal(
                "handle identity authority has an unsupported record type",
            ));
        }
        if value.get("version").and_then(serde_json::Value::as_u64)
            != Some(u64::from(HANDLE_IDENTITY_AUTHORITY_RECORD_VERSION))
        {
            return Err(ApiError::internal(
                "handle identity authority has an unsupported version",
            ));
        }
        let record: Self = serde_json::from_value(value)
            .map_err(|_| ApiError::internal("handle identity authority is corrupt"))?;
        record.validate(handle_id, scope, ordinal)?;
        Ok(record)
    }

    fn to_vec(
        &self,
        handle_id: &str,
        scope: &ControlPlaneHandleScope,
        ordinal: u64,
    ) -> Result<Vec<u8>, ApiError> {
        self.validate(handle_id, scope, ordinal)?;
        to_canonical_bytes(self)
            .map_err(|_| ApiError::internal("failed to encode handle identity authority"))
    }

    fn validate(
        &self,
        handle_id: &str,
        scope: &ControlPlaneHandleScope,
        ordinal: u64,
    ) -> Result<(), ApiError> {
        if self.record_type != HANDLE_IDENTITY_AUTHORITY_RECORD_TYPE
            || self.version != HANDLE_IDENTITY_AUTHORITY_RECORD_VERSION
        {
            return Err(ApiError::internal(
                "handle identity authority contract is unsupported",
            ));
        }
        ControlPlaneTxPaths::handle_record(handle_id).map_err(ApiError::from)?;
        scope.validate().map_err(ApiError::from)?;
        if self.handle_id != handle_id
            || self.scope != *scope
            || self.ordinal != ordinal
            || ordinal == 0
        {
            return Err(ApiError::internal(
                "handle identity authority does not match its exact path",
            ));
        }
        handle_identity_authority_path(&self.handle_id, self.ordinal)?;
        validate_sorted_claim_identities(
            &self.legacy_reservations,
            &self.handle_id,
            self.ordinal,
            "legacy identity reservations",
        )?;
        if let Some(intent) = &self.handle_intent {
            intent
                .mutation_ref
                .validate(&self.handle_id)
                .map_err(ApiError::from)?;
            if intent.mutation_ref.ordinal != self.ordinal {
                return Err(ApiError::internal(
                    "handle identity intent ordinal does not match its authority",
                ));
            }
            validate_sorted_claim_identities(
                &intent.claim_identities,
                &self.handle_id,
                self.ordinal,
                "handle intent claim identities",
            )?;
            if intent.claim_identities.is_empty() {
                return Err(ApiError::internal(
                    "handle identity intent must contain a claim identity",
                ));
            }
            if claims_overlap(&self.legacy_reservations, &intent.claim_identities) {
                return Err(ApiError::internal(
                    "handle identity authority contains overlapping owners",
                ));
            }
        }
        Ok(())
    }
}

impl PersistedHandleMutation {
    fn direct_identity(&self) -> String {
        format!("handle:{}:mutation:{:020}", self.handle_id, self.ordinal)
    }

    fn claim_identities(&self) -> Vec<HandleClaimIdentity> {
        let direct = self.direct_identity();
        let mut identities = vec![HandleClaimIdentity {
            domain: self.mutation.domain(),
            kind: self.kind,
            idempotency_key: direct.clone(),
        }];
        if let StagedMutation::Root { mutations } = &self.mutation {
            identities.extend(mutations.iter().map(|mutation| {
                let domain = mutation.domain();
                let kind = match domain {
                    ControlPlaneTxDomain::Catalog => ControlPlaneTxKind::CatalogDdl,
                    ControlPlaneTxDomain::Orchestration => ControlPlaneTxKind::OrchestrationBatch,
                    ControlPlaneTxDomain::Root => unreachable!("root children are domain-scoped"),
                };
                HandleClaimIdentity {
                    domain,
                    kind,
                    idempotency_key: format!("root:{direct}:{}", domain.as_str()),
                }
            }));
        }
        sort_claim_identities(&mut identities);
        identities
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum StagedCatalogMutation {
    CreateCatalog {
        catalog: String,
        description: Option<String>,
    },
    CreateSchema {
        catalog: String,
        schema: String,
        description: Option<String>,
    },
    RegisterTable {
        catalog: String,
        schema: String,
        table: String,
        description: Option<String>,
        location: Option<String>,
        format: Option<String>,
        columns: Vec<StagedColumnDefinition>,
    },
    UpdateTable {
        catalog: String,
        schema: String,
        table: String,
        description: StagedTextPatch,
        location: StagedTextPatch,
        format: StagedTextPatch,
    },
    DropTable {
        catalog: String,
        schema: String,
        table: String,
    },
    RenameTable {
        catalog: String,
        schema: String,
        table: String,
        new_table: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", content = "value", rename_all = "snake_case")]
enum StagedTextPatch {
    Unchanged,
    Clear,
    Set(String),
}

impl StagedTextPatch {
    fn from_nested(value: Option<Option<String>>) -> Self {
        match value {
            None => Self::Unchanged,
            Some(None) => Self::Clear,
            Some(Some(value)) => Self::Set(value),
        }
    }

    fn to_nested(&self) -> Option<Option<String>> {
        match self {
            Self::Unchanged => None,
            Self::Clear => Some(None),
            Self::Set(value) => Some(Some(value.clone())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StagedColumnDefinition {
    name: String,
    data_type: String,
    is_nullable: bool,
    ordinal: i32,
    description: Option<String>,
}

impl From<&ColumnDefinition> for StagedColumnDefinition {
    fn from(column: &ColumnDefinition) -> Self {
        Self {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            is_nullable: column.is_nullable,
            ordinal: column.ordinal,
            description: column.description.clone(),
        }
    }
}

impl StagedCatalogMutation {
    fn from_runtime(mutation: &CatalogMutation) -> Self {
        match mutation {
            CatalogMutation::CreateCatalog {
                catalog,
                description,
            } => Self::CreateCatalog {
                catalog: catalog.clone(),
                description: description.clone(),
            },
            CatalogMutation::CreateSchema {
                catalog,
                schema,
                description,
            } => Self::CreateSchema {
                catalog: catalog.clone(),
                schema: schema.clone(),
                description: description.clone(),
            },
            CatalogMutation::RegisterTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
                columns,
            } => Self::RegisterTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                description: description.clone(),
                location: location.clone(),
                format: format.clone(),
                columns: columns.iter().map(StagedColumnDefinition::from).collect(),
            },
            CatalogMutation::UpdateTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
            } => Self::UpdateTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                description: StagedTextPatch::from_nested(description.clone()),
                location: StagedTextPatch::from_nested(location.clone()),
                format: StagedTextPatch::from_nested(format.clone()),
            },
            CatalogMutation::DropTable {
                catalog,
                schema,
                table,
            } => Self::DropTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
            },
            CatalogMutation::RenameTable {
                catalog,
                schema,
                table,
                new_table,
            } => Self::RenameTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                new_table: new_table.clone(),
            },
        }
    }

    fn to_runtime(&self) -> CatalogMutation {
        match self {
            Self::CreateCatalog {
                catalog,
                description,
            } => CatalogMutation::CreateCatalog {
                catalog: catalog.clone(),
                description: description.clone(),
            },
            Self::CreateSchema {
                catalog,
                schema,
                description,
            } => CatalogMutation::CreateSchema {
                catalog: catalog.clone(),
                schema: schema.clone(),
                description: description.clone(),
            },
            Self::RegisterTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
                columns,
            } => CatalogMutation::RegisterTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                description: description.clone(),
                location: location.clone(),
                format: format.clone(),
                columns: columns
                    .iter()
                    .map(|column| ColumnDefinition {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        is_nullable: column.is_nullable,
                        ordinal: column.ordinal,
                        description: column.description.clone(),
                    })
                    .collect(),
            },
            Self::UpdateTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
            } => CatalogMutation::UpdateTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                description: description.to_nested(),
                location: location.to_nested(),
                format: format.to_nested(),
            },
            Self::DropTable {
                catalog,
                schema,
                table,
            } => CatalogMutation::DropTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
            },
            Self::RenameTable {
                catalog,
                schema,
                table,
                new_table,
            } => CatalogMutation::RenameTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                new_table: new_table.clone(),
            },
        }
    }

    fn validate(&self) -> Result<(), ApiError> {
        let operation = self.to_proto()?;
        operation
            .validate_contract()
            .map_err(|_| ApiError::bad_request("catalog handle mutation is invalid"))?;
        match self {
            Self::UpdateTable {
                location: StagedTextPatch::Set(location),
                ..
            }
            | Self::RegisterTable {
                location: Some(location),
                ..
            } => validate_credential_free_location(location),
            _ => Ok(()),
        }
    }

    fn to_proto(&self) -> Result<CatalogDdlOperation, ApiError> {
        let op = match self {
            Self::CreateCatalog {
                catalog,
                description,
            } => catalog_ddl_operation::Op::CreateCatalog(CreateCatalogOp {
                catalog: catalog.clone(),
                description: description.clone(),
            }),
            Self::CreateSchema {
                catalog,
                schema,
                description,
            } => catalog_ddl_operation::Op::CreateSchema(CreateSchemaOp {
                catalog: catalog.clone(),
                schema: schema.clone(),
                description: description.clone(),
            }),
            Self::RegisterTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
                columns,
            } => catalog_ddl_operation::Op::RegisterTable(RegisterTableOp {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                description: description.clone(),
                location: location.clone(),
                format: format.as_deref().map(format_to_proto).transpose()?,
                columns: columns
                    .iter()
                    .map(|column| ProtoColumnDefinition {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        is_nullable: column.is_nullable,
                        ordinal: column.ordinal,
                        description: column.description.clone(),
                    })
                    .collect(),
            }),
            Self::UpdateTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
            } => catalog_ddl_operation::Op::UpdateTable(UpdateTableOp {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                description: patch_to_proto_text(description),
                location: patch_to_proto_text(location),
                format: patch_to_proto_format(format)?,
            }),
            Self::DropTable {
                catalog,
                schema,
                table,
            } => catalog_ddl_operation::Op::DropTable(DropTableOp {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
            }),
            Self::RenameTable {
                catalog,
                schema,
                table,
                new_table,
            } => catalog_ddl_operation::Op::RenameTable(RenameTableOp {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                new_table: new_table.clone(),
            }),
        };
        Ok(CatalogDdlOperation { op: Some(op) })
    }
}

fn patch_to_proto_text(patch: &StagedTextPatch) -> Option<String> {
    match patch {
        StagedTextPatch::Unchanged => None,
        StagedTextPatch::Clear => Some(String::new()),
        StagedTextPatch::Set(value) => Some(value.clone()),
    }
}

fn patch_to_proto_format(patch: &StagedTextPatch) -> Result<Option<i32>, ApiError> {
    match patch {
        StagedTextPatch::Unchanged => Ok(None),
        StagedTextPatch::Clear => Ok(Some(ProtoTableFormat::Unspecified as i32)),
        StagedTextPatch::Set(value) => Ok(Some(format_to_proto(value)?)),
    }
}

fn format_to_proto(value: &str) -> Result<i32, ApiError> {
    match value {
        "delta" => Ok(ProtoTableFormat::Delta as i32),
        "iceberg" => Ok(ProtoTableFormat::Iceberg as i32),
        "parquet" => Ok(ProtoTableFormat::Parquet as i32),
        _ => Err(ApiError::internal("persisted table format is unsupported")),
    }
}

fn validate_events(
    events: &[OrchestrationEvent],
    scope: &ControlPlaneHandleScope,
) -> Result<(), ApiError> {
    if events.is_empty() {
        return Err(ApiError::bad_request(
            "orchestration handle mutation must contain at least one event",
        ));
    }
    let mut event_paths = BTreeSet::new();
    for event in events {
        if event.tenant_id != scope.tenant_id || event.workspace_id != scope.workspace_id {
            return Err(ApiError::bad_request(
                "orchestration event scope does not match its handle",
            ));
        }
        if !event_paths.insert(LedgerWriter::event_path(event)) {
            return Err(ApiError::bad_request(
                "duplicate orchestration event path is unsupported",
            ));
        }
        validate_credential_free_location(&event.source)?;
        let output = match &event.data {
            OrchestrationEventData::TaskFinished { output, .. }
            | OrchestrationEventData::TaskCompletionRecorded { output, .. } => output.as_ref(),
            _ => None,
        };
        if let Some(output_path) = output.and_then(|output| output.output_path.as_deref()) {
            validate_credential_free_location(output_path)?;
        }
        event_to_proto_envelope(event)
            .map_err(|_| ApiError::bad_request("orchestration handle event is invalid"))?;
    }
    Ok(())
}

fn validate_credential_free_location(location: &str) -> Result<(), ApiError> {
    let bytes = location.as_bytes();
    let is_drive_absolute = matches!(
        bytes,
        [drive, b':', b'/', ..] if drive.is_ascii_alphabetic()
    );
    if location.is_empty() || location.starts_with('/') || is_drive_absolute {
        return Err(ApiError::bad_request(
            "blank, credential-bearing, or absolute filesystem locations are unsupported",
        ));
    }
    if location
        .chars()
        .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(ApiError::bad_request(
            "catalog location contains invalid control characters",
        ));
    }
    if location.contains('?') || location.contains('#') {
        return Err(ApiError::bad_request(
            "credential-bearing catalog locations are unsupported",
        ));
    }
    if location.contains('\\') {
        return Err(ApiError::bad_request(
            "credential-bearing or malformed catalog locations are unsupported",
        ));
    }
    let parsed = if location.starts_with("//") {
        let base = reqwest::Url::parse("https://catalog-location.invalid/")
            .map_err(|_| ApiError::internal("catalog location validation base is invalid"))?;
        Some(
            reqwest::Url::options()
                .base_url(Some(&base))
                .parse(location)
                .map_err(|_| {
                    ApiError::bad_request("catalog location URI is malformed or unsupported")
                })?,
        )
    } else {
        match reqwest::Url::parse(location) {
            Ok(parsed) => Some(parsed),
            Err(_) if !has_uri_scheme(location) => None,
            Err(_) => {
                return Err(ApiError::bad_request(
                    "catalog location URI is malformed or unsupported",
                ));
            }
        }
    };
    if let Some(parsed) = parsed {
        if parsed.scheme().eq_ignore_ascii_case("file") {
            return Err(ApiError::bad_request(
                "absolute filesystem locations are unsupported",
            ));
        }
        if parsed.cannot_be_a_base() {
            return Err(ApiError::bad_request(
                "credential-bearing or opaque catalog locations are unsupported",
            ));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(ApiError::bad_request(
                "credential-bearing catalog locations are unsupported",
            ));
        }
    }
    Ok(())
}

fn has_uri_scheme(value: &str) -> bool {
    let Some((scheme, _)) = value.split_once(':') else {
        return false;
    };
    let mut bytes = scheme.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_alphabetic())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
}

fn is_canonical_ulid(value: &str) -> bool {
    Ulid::from_string(value)
        .ok()
        .is_some_and(|parsed| parsed.to_string() == value)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedHandleOwnedIdentity {
    handle_id: String,
    ordinal: u64,
    child_domain: Option<ControlPlaneTxDomain>,
}

fn parse_handle_owned_identity(value: &str) -> Option<ParsedHandleOwnedIdentity> {
    fn direct(value: &str) -> Option<(String, u64)> {
        let value = value.strip_prefix("handle:")?;
        let (handle_id, encoded_ordinal) = value.split_once(":mutation:")?;
        if handle_id.contains(':') {
            return None;
        }
        ControlPlaneTxPaths::handle_record(handle_id).ok()?;
        let ordinal = encoded_ordinal.parse::<u64>().ok()?;
        if ordinal == 0 || format!("{ordinal:020}") != encoded_ordinal {
            return None;
        }
        Some((handle_id.to_string(), ordinal))
    }

    let (direct_identity, child_domain) = if let Some(child) = value.strip_prefix("root:") {
        let (direct_identity, domain) = child.rsplit_once(':')?;
        let domain = match domain {
            "catalog" => ControlPlaneTxDomain::Catalog,
            "orchestration" => ControlPlaneTxDomain::Orchestration,
            _ => return None,
        };
        (direct_identity, Some(domain))
    } else {
        (value, None)
    };
    let (handle_id, ordinal) = direct(direct_identity)?;
    Some(ParsedHandleOwnedIdentity {
        handle_id,
        ordinal,
        child_domain,
    })
}

fn handle_claim_kind_rank(kind: ControlPlaneTxKind) -> u8 {
    match kind {
        ControlPlaneTxKind::CatalogDdl => 0,
        ControlPlaneTxKind::OrchestrationBatch => 1,
        ControlPlaneTxKind::RootCommit => 2,
    }
}

fn expected_handle_claim_kind(domain: ControlPlaneTxDomain) -> ControlPlaneTxKind {
    match domain {
        ControlPlaneTxDomain::Catalog => ControlPlaneTxKind::CatalogDdl,
        ControlPlaneTxDomain::Orchestration => ControlPlaneTxKind::OrchestrationBatch,
        ControlPlaneTxDomain::Root => ControlPlaneTxKind::RootCommit,
    }
}

fn sort_claim_identities(identities: &mut [HandleClaimIdentity]) {
    identities.sort_by(|left, right| {
        left.domain
            .cmp(&right.domain)
            .then_with(|| {
                handle_claim_kind_rank(left.kind).cmp(&handle_claim_kind_rank(right.kind))
            })
            .then_with(|| left.idempotency_key.cmp(&right.idempotency_key))
    });
}

fn validate_sorted_claim_identities(
    identities: &[HandleClaimIdentity],
    handle_id: &str,
    ordinal: u64,
    field: &str,
) -> Result<(), ApiError> {
    let mut canonical = identities.to_vec();
    sort_claim_identities(&mut canonical);
    if canonical != identities || canonical.windows(2).any(|pair| pair.first() == pair.get(1)) {
        return Err(ApiError::internal(format!(
            "{field} must be sorted and unique"
        )));
    }
    for identity in identities {
        if identity.kind != expected_handle_claim_kind(identity.domain) {
            return Err(ApiError::internal(format!(
                "{field} contains a domain-kind mismatch"
            )));
        }
        let parsed = parse_handle_owned_identity(&identity.idempotency_key).ok_or_else(|| {
            ApiError::internal(format!("{field} contains a noncanonical identity"))
        })?;
        if parsed.handle_id != handle_id
            || parsed.ordinal != ordinal
            || parsed
                .child_domain
                .is_some_and(|domain| domain != identity.domain)
        {
            return Err(ApiError::internal(format!(
                "{field} does not match its exact authority path"
            )));
        }
    }
    Ok(())
}

fn handle_identity_authority_path(handle_id: &str, ordinal: u64) -> Result<String, ApiError> {
    ControlPlaneTxPaths::handle_identity_authority(handle_id, ordinal).map_err(ApiError::from)
}

async fn load_exact_versioned_handle(
    service: &ControlPlaneTransactionService<'_>,
    handle_id: &str,
) -> Result<Option<VersionedHandle>, ApiError> {
    let path = ControlPlaneTxPaths::handle_record(handle_id).map_err(ApiError::from)?;
    let metadata = match service.storage.head_raw(&path).await {
        Ok(Some(metadata)) => metadata,
        Ok(None)
        | Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
            return Ok(None);
        }
        Err(error) => return Err(ApiError::from(error)),
    };
    let bytes = service.storage.get_raw(&path).await?;
    let record =
        ControlPlaneHandleRecord::from_json_slice(bytes.as_ref()).map_err(ApiError::from)?;
    if record.handle_id != handle_id {
        return Err(ApiError::internal(
            "transaction handle record identity does not match its exact path",
        ));
    }
    if record.scope.tenant_id != service.ctx.tenant
        || record.scope.workspace_id != service.ctx.workspace
    {
        return Err(ApiError::forbidden(
            "transaction handle is outside the current workspace scope",
        ));
    }
    Ok(Some(VersionedHandle {
        record,
        version: metadata.version,
    }))
}

async fn load_exact_staged_mutation(
    service: &ControlPlaneTransactionService<'_>,
    handle: &ControlPlaneHandleRecord,
    reference: &ControlPlaneHandleMutationRef,
) -> Result<PersistedHandleMutation, ApiError> {
    reference
        .validate(&handle.handle_id)
        .map_err(ApiError::from)?;
    let bytes = get_optional(&service.storage, &reference.path)
        .await?
        .ok_or_else(|| ApiError::internal("required staged handle mutation is missing"))?;
    if sha256(bytes.as_ref()) != reference.sha256 {
        return Err(ApiError::internal(
            "required staged handle mutation checksum is corrupt",
        ));
    }
    let mutation = PersistedHandleMutation::from_slice(bytes.as_ref(), handle)?;
    if mutation.ordinal != reference.ordinal || mutation.kind != reference.kind {
        return Err(ApiError::internal(
            "staged handle mutation does not match its immutable reference",
        ));
    }
    Ok(mutation)
}

#[derive(Debug)]
struct VersionedHandleIdentityAuthority {
    record: HandleIdentityAuthorityRecord,
    version: String,
}

async fn load_exact_identity_authority(
    service: &ControlPlaneTransactionService<'_>,
    handle_id: &str,
    scope: &ControlPlaneHandleScope,
    ordinal: u64,
) -> Result<Option<VersionedHandleIdentityAuthority>, ApiError> {
    let path = handle_identity_authority_path(handle_id, ordinal)?;
    let metadata = match service.storage.head_raw(&path).await {
        Ok(Some(metadata)) => metadata,
        Ok(None)
        | Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
            return Ok(None);
        }
        Err(error) => return Err(ApiError::from(error)),
    };
    let bytes = match service.storage.get_raw(&path).await {
        Ok(bytes) => bytes,
        Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
            return Err(ApiError::internal(
                "handle identity authority disappeared during exact read",
            ));
        }
        Err(error) => return Err(ApiError::from(error)),
    };
    let record =
        HandleIdentityAuthorityRecord::from_slice(bytes.as_ref(), handle_id, scope, ordinal)?;
    Ok(Some(VersionedHandleIdentityAuthority {
        record,
        version: metadata.version,
    }))
}

async fn write_identity_authority_cas(
    service: &ControlPlaneTransactionService<'_>,
    handle_id: &str,
    scope: &ControlPlaneHandleScope,
    current: Option<&VersionedHandleIdentityAuthority>,
    next: &HandleIdentityAuthorityRecord,
) -> Result<bool, ApiError> {
    let path = handle_identity_authority_path(handle_id, next.ordinal)?;
    let bytes = next.to_vec(handle_id, scope, next.ordinal)?;
    let precondition = current.map_or(WritePrecondition::DoesNotExist, |stored| {
        WritePrecondition::MatchesVersion(stored.version.clone())
    });
    match service
        .storage
        .put_raw(&path, Bytes::from(bytes), precondition)
        .await?
    {
        WriteResult::Success { .. } => Ok(true),
        WriteResult::PreconditionFailed { .. } => Ok(false),
    }
}

async fn validate_exact_handle_intent(
    service: &ControlPlaneTransactionService<'_>,
    handle: &ControlPlaneHandleRecord,
    authority: &HandleIdentityAuthorityRecord,
) -> Result<Option<PersistedHandleMutation>, ApiError> {
    let Some(intent) = &authority.handle_intent else {
        return Ok(None);
    };
    let staged = load_exact_staged_mutation(service, handle, &intent.mutation_ref).await?;
    if staged.claim_identities() != intent.claim_identities {
        return Err(ApiError::internal(
            "handle identity intent does not match its exact staged claim set",
        ));
    }
    if let Some(reference) = handle
        .mutation_refs
        .iter()
        .find(|reference| reference.ordinal == authority.ordinal)
        && reference != &intent.mutation_ref
    {
        return Err(ApiError::internal(
            "handle identity intent diverges from its immutable handle reference",
        ));
    }
    Ok(Some(staged))
}

fn handle_identity_intent(
    staged: &PersistedHandleMutation,
    mutation_ref: ControlPlaneHandleMutationRef,
) -> HandleIdentityIntent {
    HandleIdentityIntent {
        mutation_ref,
        claim_identities: staged.claim_identities(),
    }
}

fn claims_overlap(reservations: &[HandleClaimIdentity], claims: &[HandleClaimIdentity]) -> bool {
    reservations
        .iter()
        .any(|reservation| claims.contains(reservation))
}

async fn install_handle_identity_intent(
    service: &ControlPlaneTransactionService<'_>,
    handle: &ControlPlaneHandleRecord,
    staged: &PersistedHandleMutation,
    mutation_ref: &ControlPlaneHandleMutationRef,
) -> Result<(), ApiError> {
    let desired = handle_identity_intent(staged, mutation_ref.clone());
    for _ in 0..HANDLE_IDENTITY_AUTHORITY_CAS_ATTEMPTS {
        let current = load_exact_identity_authority(
            service,
            &handle.handle_id,
            &handle.scope,
            staged.ordinal,
        )
        .await?;
        if current.is_none()
            && handle
                .mutation_refs
                .iter()
                .any(|reference| reference.ordinal == staged.ordinal)
        {
            return Err(ApiError::internal(
                "referenced handle mutation is missing its identity authority",
            ));
        }
        let mut next = current.as_ref().map_or_else(
            || HandleIdentityAuthorityRecord::new(&handle.handle_id, &handle.scope, staged.ordinal),
            |stored| Ok(stored.record.clone()),
        )?;
        if let Some(existing) = &next.handle_intent {
            validate_exact_handle_intent(service, handle, &next).await?;
            if existing == &desired {
                return Ok(());
            }
            return Err(ApiError::conflict(
                "staged mutation ordinal conflicts with durable handle intent",
            ));
        }
        if claims_overlap(&next.legacy_reservations, &desired.claim_identities) {
            return Err(ApiError::conflict(
                "staged handle identity is reserved by legacy execution",
            ));
        }
        for identity in &desired.claim_identities {
            let path = ControlPlaneTxPaths::idempotency(identity.domain, &identity.idempotency_key);
            if get_optional(&service.storage, &path).await?.is_some() {
                return Err(ApiError::conflict(
                    "staged handle identity is already claimed by low-level execution",
                ));
            }
        }
        next.handle_intent = Some(desired.clone());
        if write_identity_authority_cas(
            service,
            &handle.handle_id,
            &handle.scope,
            current.as_ref(),
            &next,
        )
        .await?
        {
            return Ok(());
        }
    }
    Err(ApiError::conflict(
        "handle identity authority changed during staged mutation CAS",
    ))
}

fn eligible_legacy_handle_claim(
    parsed: &ParsedHandleOwnedIdentity,
    domain: ControlPlaneTxDomain,
    kind: ControlPlaneTxKind,
    idempotency_key: &str,
) -> Option<HandleClaimIdentity> {
    if kind != expected_handle_claim_kind(domain)
        || parsed
            .child_domain
            .is_some_and(|child_domain| child_domain != domain)
    {
        return None;
    }
    Some(HandleClaimIdentity {
        domain,
        kind,
        idempotency_key: idempotency_key.to_string(),
    })
}

pub(super) fn guard_legacy_handle_identity<'a>(
    service: &'a ControlPlaneTransactionService<'_>,
    domain: ControlPlaneTxDomain,
    kind: ControlPlaneTxKind,
    idempotency_key: &'a str,
) -> std::pin::Pin<Box<dyn Future<Output = Result<Option<()>, ApiError>> + Send + 'a>> {
    Box::pin(reserve_legacy_handle_identity(
        service,
        domain,
        kind,
        idempotency_key,
    ))
}

async fn reserve_legacy_handle_identity(
    service: &ControlPlaneTransactionService<'_>,
    domain: ControlPlaneTxDomain,
    kind: ControlPlaneTxKind,
    idempotency_key: &str,
) -> Result<Option<()>, ApiError> {
    let Some(parsed) = parse_handle_owned_identity(idempotency_key) else {
        return Ok(None);
    };
    let Some(claim) = eligible_legacy_handle_claim(&parsed, domain, kind, idempotency_key) else {
        return Ok(None);
    };
    validate_sorted_claim_identities(
        std::slice::from_ref(&claim),
        &parsed.handle_id,
        parsed.ordinal,
        "legacy identity reservation",
    )?;
    let scope =
        ControlPlaneHandleScope::new(service.ctx.tenant.clone(), service.ctx.workspace.clone())
            .map_err(ApiError::from)?;

    for _ in 0..HANDLE_IDENTITY_AUTHORITY_CAS_ATTEMPTS {
        let handle = load_exact_versioned_handle(service, &parsed.handle_id).await?;
        let current =
            load_exact_identity_authority(service, &parsed.handle_id, &scope, parsed.ordinal)
                .await?;
        let handle_reference = handle.as_ref().and_then(|handle| {
            handle
                .record
                .mutation_refs
                .iter()
                .find(|reference| reference.ordinal == parsed.ordinal)
        });
        if handle_reference.is_some() && current.is_none() {
            return Err(ApiError::internal(
                "referenced handle mutation is missing its identity authority",
            ));
        }
        let mut next = current.as_ref().map_or_else(
            || HandleIdentityAuthorityRecord::new(&parsed.handle_id, &scope, parsed.ordinal),
            |stored| Ok(stored.record.clone()),
        )?;

        if handle_reference.is_some() && next.handle_intent.is_none() {
            return Err(ApiError::internal(
                "referenced handle mutation has no durable handle intent",
            ));
        }

        if next.handle_intent.is_some() {
            let handle = handle.as_ref().ok_or_else(|| {
                ApiError::internal("handle identity intent exists without its exact handle")
            })?;
            if let Some(staged) =
                validate_exact_handle_intent(service, &handle.record, &next).await?
                && staged.claim_identities().contains(&claim)
            {
                return Err(ApiError::conflict(
                    "idempotency identity is reserved by a durable transaction handle",
                ));
            }
        }
        if next.legacy_reservations.contains(&claim) {
            return Ok(Some(()));
        }
        next.legacy_reservations.push(claim.clone());
        sort_claim_identities(&mut next.legacy_reservations);
        if write_identity_authority_cas(service, &parsed.handle_id, &scope, current.as_ref(), &next)
            .await?
        {
            return Ok(Some(()));
        }
    }
    Err(ApiError::conflict(
        "handle identity authority changed during legacy reservation CAS",
    ))
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn token_matches(verifier: &str, candidate: &str) -> bool {
    let Some(entropy) = candidate.strip_prefix(REVIEW_TOKEN_PREFIX) else {
        return false;
    };
    if entropy.len() != 64
        || !entropy
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return false;
    }
    let candidate = sha256(candidate.as_bytes());
    if verifier.len() != candidate.len() {
        return false;
    }
    verifier
        .bytes()
        .zip(candidate.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[derive(Debug)]
struct VersionedHandle {
    record: ControlPlaneHandleRecord,
    version: String,
}

/// Internal Rust service for durable transaction handles.
///
/// This module deliberately has no transport registration. It composes the
/// existing transaction service and stores only exact-addressed handle data.
pub(super) struct ControlPlaneTransactionHandleService<'a> {
    transaction_service: ControlPlaneTransactionService<'a>,
}

impl<'a> ControlPlaneTransactionHandleService<'a> {
    /// Creates a handle service bound to one authenticated workspace scope.
    pub(super) fn new(state: &'a AppState, ctx: RequestContext) -> Result<Self, ApiError> {
        Ok(Self {
            transaction_service: ControlPlaneTransactionService::new(state, ctx)?,
        })
    }

    /// Creates a fresh handle and returns its review token exactly once.
    pub(super) async fn create_handle(
        &self,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<CreateHandleResult, ApiError> {
        if ttl.is_zero() {
            return Err(ApiError::bad_request("handle TTL must be positive"));
        }
        let ttl = chrono::Duration::from_std(ttl)
            .map_err(|_| ApiError::bad_request("handle TTL is too large"))?;
        let expires_at = now
            .checked_add_signed(ttl)
            .ok_or_else(|| ApiError::bad_request("handle expiry overflows"))?;
        let scope = ControlPlaneHandleScope::new(
            self.transaction_service.ctx.tenant.clone(),
            self.transaction_service.ctx.workspace.clone(),
        )
        .map_err(ApiError::from)?;

        for _ in 0..4 {
            let handle_id = format!("hdl_{}", Ulid::new());
            let review_token = ReviewToken::generate();
            let handle = ControlPlaneHandleRecord::new(
                handle_id,
                scope.clone(),
                now,
                expires_at,
                sha256(review_token.expose().as_bytes()),
            )
            .map_err(ApiError::from)?;
            let path =
                ControlPlaneTxPaths::handle_record(&handle.handle_id).map_err(ApiError::from)?;
            let bytes = Bytes::from(handle.to_json_vec().map_err(ApiError::from)?);
            match self
                .transaction_service
                .storage
                .put_raw(&path, bytes.clone(), WritePrecondition::DoesNotExist)
                .await
            {
                Ok(WriteResult::Success { .. }) => {
                    return Ok(CreateHandleResult {
                        handle,
                        review_token,
                    });
                }
                Ok(WriteResult::PreconditionFailed { .. }) => {}
                Err(write_error) => {
                    match get_optional(&self.transaction_service.storage, &path).await {
                        Ok(Some(stored)) if stored == bytes => {
                            return Ok(CreateHandleResult {
                                handle,
                                review_token,
                            });
                        }
                        Ok(Some(_)) => {
                            return Err(ApiError::conflict(
                                "ambiguous transaction handle creation found divergent stored state",
                            ));
                        }
                        Ok(None) => {}
                        Err(_) => return Err(ApiError::from(write_error)),
                    }
                }
            }
        }
        Err(ApiError::conflict(
            "failed to allocate a unique transaction handle",
        ))
    }

    /// Gets one handle by its exact validated identifier.
    pub(super) async fn get_handle(
        &self,
        handle_id: &str,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        self.load_handle(handle_id)
            .await?
            .map(|stored| stored.record)
            .ok_or_else(|| {
                ApiError::not_found(format!("transaction handle not found: {handle_id}"))
            })
    }

    /// Stages one supported catalog DDL operation at an explicit ordinal.
    pub(super) async fn stage_catalog(
        &self,
        handle_id: &str,
        review_token: &str,
        ordinal: u64,
        operation: CatalogDdlOperation,
        now: DateTime<Utc>,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        self.authorize_stage_request(handle_id, review_token)
            .await?;
        operation
            .validate_contract()
            .map_err(|_| ApiError::bad_request("catalog handle mutation is invalid"))?;
        let mutation = CatalogMutation::from_proto(&operation)
            .map_err(|_| ApiError::bad_request("catalog handle mutation is invalid"))?;
        self.stage_mutation(
            handle_id,
            review_token,
            ordinal,
            StagedMutation::Catalog {
                operation: StagedCatalogMutation::from_runtime(&mutation),
            },
            now,
        )
        .await
    }

    /// Stages one supported orchestration batch at an explicit ordinal.
    pub(super) async fn stage_orchestration(
        &self,
        handle_id: &str,
        review_token: &str,
        ordinal: u64,
        batch: OrchestrationBatchSpec,
        now: DateTime<Utc>,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        self.authorize_stage_request(handle_id, review_token)
            .await?;
        let batch = OrchestrationBatchMutation::from_spec(&batch)
            .map_err(|_| ApiError::bad_request("orchestration handle mutation is invalid"))?;
        let events = batch
            .events(&self.handle_metadata("stage", "stage"))
            .map_err(|_| ApiError::bad_request("orchestration handle mutation is invalid"))?;
        self.stage_mutation(
            handle_id,
            review_token,
            ordinal,
            StagedMutation::Orchestration { events },
            now,
        )
        .await
    }

    /// Stages one supported root transaction at an explicit ordinal.
    ///
    /// Metastore and scoped-metastore payloads are rejected before any staged
    /// object is written because those operations are not currently executable
    /// and may contain credentials or grants.
    pub(super) async fn stage_root(
        &self,
        handle_id: &str,
        review_token: &str,
        ordinal: u64,
        mutations: Vec<DomainMutation>,
        now: DateTime<Utc>,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        self.authorize_stage_request(handle_id, review_token)
            .await?;
        let metadata = self.handle_metadata("stage-root", "stage-root");
        let mut staged = Vec::with_capacity(mutations.len());
        for mutation in &mutations {
            match RootMutation::from_proto(mutation)
                .map_err(|_| ApiError::bad_request("root handle mutation is invalid"))?
            {
                RootMutation::Catalog(operation) => {
                    staged.push(StagedRootMutation::Catalog {
                        operation: StagedCatalogMutation::from_runtime(&operation),
                    });
                }
                RootMutation::Orchestration(batch) => {
                    staged.push(StagedRootMutation::Orchestration {
                        events: batch.events(&metadata).map_err(|_| {
                            ApiError::bad_request("root handle mutation is invalid")
                        })?,
                    });
                }
                RootMutation::Metastore(_) | RootMutation::ScopedMetastore(_) => {
                    return Err(ApiError::not_acceptable(
                        "unsupported credential, grant, or metastore mutation in durable handle",
                    ));
                }
            }
        }
        self.stage_mutation(
            handle_id,
            review_token,
            ordinal,
            StagedMutation::Root { mutations: staged },
            now,
        )
        .await
    }

    async fn stage_mutation(
        &self,
        handle_id: &str,
        review_token: &str,
        ordinal: u64,
        mutation: StagedMutation,
        now: DateTime<Utc>,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        let stored = self.load_handle(handle_id).await?.ok_or_else(|| {
            ApiError::not_found(format!("transaction handle not found: {handle_id}"))
        })?;
        self.verify_review_token(&stored.record, review_token)?;
        if stored.record.status != ControlPlaneHandleStatus::Open {
            return Err(ApiError::conflict(format!(
                "transaction handle is terminal or frozen in {:?}",
                stored.record.status
            )));
        }
        if now >= stored.record.expires_at {
            let _ = self.expire_loaded(stored, now).await?;
            return Err(ApiError::conflict(
                "transaction handle is expired and terminal",
            ));
        }
        if now < stored.record.updated_at {
            return Err(ApiError::bad_request(
                "handle mutation time precedes the current revision",
            ));
        }

        let staged = PersistedHandleMutation::new(&stored.record, ordinal, mutation)?;
        let staged_bytes = staged.to_vec(&stored.record)?;
        if staged_bytes
            .windows(review_token.len())
            .any(|window| window == review_token.as_bytes())
        {
            return Err(ApiError::not_acceptable(
                "review credentials cannot be persisted in staged mutations",
            ));
        }
        let digest = sha256(&staged_bytes);
        let mutation_path =
            ControlPlaneTxPaths::handle_mutation(handle_id, ordinal).map_err(ApiError::from)?;
        let reference = ControlPlaneHandleMutationRef::new(handle_id, ordinal, staged.kind, digest)
            .map_err(ApiError::from)?;

        if let Some(existing_ref) = stored
            .record
            .mutation_refs
            .iter()
            .find(|item| item.ordinal == ordinal)
        {
            if existing_ref != &reference {
                return Err(ApiError::conflict(
                    "staged mutation ordinal conflicts with immutable content",
                ));
            }
            let existing = self
                .transaction_service
                .storage
                .get_raw(&mutation_path)
                .await?;
            if existing.as_ref() != staged_bytes.as_slice() {
                return Err(ApiError::conflict(
                    "staged mutation path contains conflicting immutable content",
                ));
            }
            install_handle_identity_intent(
                &self.transaction_service,
                &stored.record,
                &staged,
                &reference,
            )
            .await?;
            return Ok(stored.record);
        }

        let next_ordinal = u64::try_from(stored.record.mutation_refs.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| ApiError::bad_request("too many staged handle mutations"))?;
        if ordinal != next_ordinal {
            return Err(ApiError::bad_request(format!(
                "staged mutation ordinal must be the next canonical ordinal {next_ordinal}"
            )));
        }

        if let Some(authority) = load_exact_identity_authority(
            &self.transaction_service,
            handle_id,
            &stored.record.scope,
            ordinal,
        )
        .await?
            && let Some(existing_intent) = &authority.record.handle_intent
        {
            validate_exact_handle_intent(
                &self.transaction_service,
                &stored.record,
                &authority.record,
            )
            .await?;
            if existing_intent != &handle_identity_intent(&staged, reference.clone()) {
                return Err(ApiError::conflict(
                    "staged mutation ordinal conflicts with durable handle intent",
                ));
            }
        }

        match self
            .transaction_service
            .storage
            .put_raw(
                &mutation_path,
                Bytes::from(staged_bytes.clone()),
                WritePrecondition::DoesNotExist,
            )
            .await?
        {
            WriteResult::Success { .. } => {}
            WriteResult::PreconditionFailed { .. } => {
                let existing = self
                    .transaction_service
                    .storage
                    .get_raw(&mutation_path)
                    .await?;
                if existing.as_ref() != staged_bytes.as_slice() {
                    return Err(ApiError::conflict(
                        "staged mutation path contains conflicting immutable content",
                    ));
                }
            }
        }

        install_handle_identity_intent(
            &self.transaction_service,
            &stored.record,
            &staged,
            &reference,
        )
        .await?;
        let mut next = stored.record.clone();
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| ApiError::conflict("handle revision overflows"))?;
        next.updated_at = now;
        next.mutation_refs.push(reference.clone());
        next.validate().map_err(ApiError::from)?;

        if self.write_handle_cas(&stored, &next).await? {
            return Ok(next);
        }
        let winner = self.get_handle(handle_id).await?;
        if winner.status == ControlPlaneHandleStatus::Open
            && same_handle_identity(&winner, &stored.record)
            && winner.mutation_refs == next.mutation_refs
            && winner.participants == next.participants
        {
            return Ok(winner);
        }
        Err(ApiError::conflict(
            "transaction handle changed during staged mutation CAS",
        ))
    }

    async fn authorize_stage_request(
        &self,
        handle_id: &str,
        review_token: &str,
    ) -> Result<(), ApiError> {
        let stored = self.load_handle_required(handle_id).await?;
        self.verify_review_token(&stored.record, review_token)
    }

    fn build_participants(
        &self,
        handle: &ControlPlaneHandleRecord,
        staged: &[PersistedHandleMutation],
    ) -> Result<Vec<ControlPlaneHandleParticipant>, ApiError> {
        staged
            .iter()
            .map(|mutation| {
                let identity = format!(
                    "handle:{}:mutation:{:020}",
                    handle.handle_id, mutation.ordinal
                );
                let metadata = self.handle_metadata(&identity, &identity);
                Ok(ControlPlaneHandleParticipant {
                    ordinal: mutation.ordinal,
                    kind: mutation.kind,
                    domain: mutation.mutation.domain(),
                    request_hash: self.staged_request_hash(mutation, &metadata)?,
                    request_id: identity.clone(),
                    idempotency_key: identity,
                    tx_id: None,
                    low_level_status: None,
                    receipt_path: None,
                })
            })
            .collect()
    }

    fn staged_request_hash(
        &self,
        staged: &PersistedHandleMutation,
        metadata: &ResolvedRequestMetadata,
    ) -> Result<String, ApiError> {
        match &staged.mutation {
            StagedMutation::Catalog { operation } => operation.to_runtime().request_hash(),
            StagedMutation::Orchestration { events } => {
                OrchestrationBatchMutation { events: Vec::new() }.request_hash_for_events(events)
            }
            StagedMutation::Root { mutations } => {
                let request = root_request_from_staged(mutations)?;
                let mutations = request
                    .mutations
                    .iter()
                    .map(RootMutation::from_proto)
                    .collect::<Result<Vec<_>, _>>()?;
                root_request_hash(&mutations, metadata)
            }
        }
    }

    fn validate_staged_participant_binding(
        &self,
        participant: &ControlPlaneHandleParticipant,
        staged: &PersistedHandleMutation,
    ) -> Result<(), ApiError> {
        let metadata = self.handle_metadata(&participant.request_id, &participant.idempotency_key);
        if participant.ordinal != staged.ordinal
            || participant.kind != staged.kind
            || participant.domain != staged.mutation.domain()
            || participant.request_hash != self.staged_request_hash(staged, &metadata)?
        {
            return Err(ApiError::internal(
                "prepared handle participant does not match its immutable staged mutation",
            ));
        }
        Ok(())
    }

    /// Freezes all exact-addressed staged objects and their deterministic
    /// low-level participant identities.
    pub(super) async fn prepare_handle(
        &self,
        handle_id: &str,
        review_token: &str,
        now: DateTime<Utc>,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        let mut stored = self.load_handle(handle_id).await?.ok_or_else(|| {
            ApiError::not_found(format!("transaction handle not found: {handle_id}"))
        })?;
        self.verify_review_token(&stored.record, review_token)?;
        if now < stored.record.updated_at {
            return Err(ApiError::bad_request(
                "handle prepare time precedes the current revision",
            ));
        }
        if now >= stored.record.expires_at {
            let _ = self.expire_loaded(stored, now).await?;
            return Err(ApiError::conflict(
                "transaction handle is expired and terminal",
            ));
        }
        if stored.record.status == ControlPlaneHandleStatus::Open {
            if stored.record.mutation_refs.is_empty() {
                return Err(ApiError::bad_request(
                    "transaction handle must stage at least one mutation before prepare",
                ));
            }
            let mut preparing = stored.record.clone();
            preparing.status = ControlPlaneHandleStatus::Preparing;
            preparing.revision = preparing
                .revision
                .checked_add(1)
                .ok_or_else(|| ApiError::conflict("handle revision overflows"))?;
            preparing.updated_at = now;
            preparing.validate().map_err(ApiError::from)?;
            if self.write_handle_cas(&stored, &preparing).await? {
                stored = self.load_handle_required(handle_id).await?;
            } else {
                stored = self.load_handle_required(handle_id).await?;
                if now < stored.record.updated_at {
                    return Err(ApiError::bad_request(
                        "handle prepare time precedes the current revision",
                    ));
                }
                if now >= stored.record.expires_at {
                    let _ = self.expire_loaded(stored, now).await?;
                    return Err(ApiError::conflict(
                        "transaction handle is expired and terminal",
                    ));
                }
            }
        }
        if stored.record.status == ControlPlaneHandleStatus::Prepared {
            let staged = self.load_all_staged(&stored.record).await?;
            let expected = self.build_participants(&stored.record, &staged)?;
            if stored.record.participants != expected {
                return Err(ApiError::conflict(
                    "prepared transaction handle participant identity is inconsistent",
                ));
            }
            return Ok(stored.record);
        }
        if stored.record.status != ControlPlaneHandleStatus::Preparing {
            return Err(ApiError::conflict(format!(
                "transaction handle cannot prepare from {:?}",
                stored.record.status
            )));
        }

        let staged = self.load_all_staged(&stored.record).await?;
        let participants = self.build_participants(&stored.record, &staged)?;
        let mut prepared = stored.record.clone();
        prepared.status = ControlPlaneHandleStatus::Prepared;
        prepared.revision = prepared
            .revision
            .checked_add(1)
            .ok_or_else(|| ApiError::conflict("handle revision overflows"))?;
        prepared.updated_at = now;
        prepared.prepared_at = Some(now);
        prepared.participants = participants;
        prepared.validate().map_err(ApiError::from)?;
        if self.write_handle_cas(&stored, &prepared).await? {
            return Ok(prepared);
        }
        let winner = self.get_handle(handle_id).await?;
        if winner.status == ControlPlaneHandleStatus::Prepared
            && same_handle_identity(&winner, &prepared)
            && winner.mutation_refs == prepared.mutation_refs
            && winner.participants == prepared.participants
        {
            return Ok(winner);
        }
        Err(ApiError::conflict(
            "transaction handle changed during prepare CAS",
        ))
    }

    /// Aborts an uncommitted handle. Once visibility is possible, recovery is
    /// mandatory and abort is forbidden.
    pub(super) async fn abort_handle(
        &self,
        handle_id: &str,
        review_token: &str,
        now: DateTime<Utc>,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        let stored = self.load_handle_required(handle_id).await?;
        self.verify_review_token(&stored.record, review_token)?;
        if now >= stored.record.expires_at
            && matches!(
                stored.record.status,
                ControlPlaneHandleStatus::Open
                    | ControlPlaneHandleStatus::Preparing
                    | ControlPlaneHandleStatus::Prepared
            )
        {
            return self.expire_loaded(stored, now).await;
        }
        if stored.record.status == ControlPlaneHandleStatus::Aborted {
            return Ok(stored.record);
        }
        if !matches!(
            stored.record.status,
            ControlPlaneHandleStatus::Open
                | ControlPlaneHandleStatus::Preparing
                | ControlPlaneHandleStatus::Prepared
        ) {
            return Err(ApiError::conflict(
                "transaction handle cannot abort after visibility is possible or terminal",
            ));
        }
        self.ensure_no_low_level_claims(&stored, now).await?;
        self.terminal_transition(stored, ControlPlaneHandleStatus::Aborted, now)
            .await
    }

    /// Expires a pre-visibility handle at or after its immutable TTL boundary.
    pub(super) async fn expire_handle(
        &self,
        handle_id: &str,
        review_token: &str,
        now: DateTime<Utc>,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        let stored = self.load_handle_required(handle_id).await?;
        self.verify_review_token(&stored.record, review_token)?;
        if stored.record.status == ControlPlaneHandleStatus::Expired {
            return Ok(stored.record);
        }
        if now < stored.record.expires_at {
            return Err(ApiError::precondition_failed(
                "transaction handle TTL has not elapsed",
            ));
        }
        self.expire_loaded(stored, now).await
    }

    async fn expire_loaded(
        &self,
        stored: VersionedHandle,
        now: DateTime<Utc>,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        if !matches!(
            stored.record.status,
            ControlPlaneHandleStatus::Open
                | ControlPlaneHandleStatus::Preparing
                | ControlPlaneHandleStatus::Prepared
        ) {
            return Err(ApiError::conflict(
                "transaction handle cannot expire after visibility is possible or terminal",
            ));
        }
        self.ensure_no_low_level_claims(&stored, now).await?;
        self.terminal_transition(stored, ControlPlaneHandleStatus::Expired, now)
            .await
    }

    async fn ensure_no_low_level_claims(
        &self,
        stored: &VersionedHandle,
        now: DateTime<Utc>,
    ) -> Result<(), ApiError> {
        let handle = &stored.record;
        if handle.mutation_refs.is_empty() {
            return Ok(());
        }
        let staged = self.load_all_staged(handle).await?;
        let expected = self.build_participants(handle, &staged)?;
        if !handle.participants.is_empty()
            && !handle
                .participants
                .iter()
                .zip(&expected)
                .all(|(actual, expected)| same_participant_definition(actual, expected))
        {
            return Err(ApiError::conflict(
                "transaction handle participants changed before terminalization",
            ));
        }
        let participants = if handle.participants.is_empty() {
            &expected
        } else {
            &handle.participants
        };
        for (participant, mutation) in participants.iter().zip(&staged) {
            let evidence = self.inspect_participant(participant, mutation).await;
            let claim_exists = evidence.is_err()
                || evidence.as_ref().is_ok_and(|evidence| {
                    evidence.tx_id.is_some()
                        || evidence.status.is_some()
                        || evidence.receipt_path.is_some()
                        || evidence.uncertain
                });
            if claim_exists {
                let mut repair = handle.clone();
                repair.status = ControlPlaneHandleStatus::RepairRequired;
                repair.revision = repair
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| ApiError::conflict("handle revision overflows"))?;
                repair.updated_at = now.max(repair.updated_at);
                let claim_started_at = repair
                    .prepared_at
                    .unwrap_or_else(|| handle.updated_at.min(repair.updated_at));
                repair.prepared_at = Some(claim_started_at);
                repair.committing_at = Some(
                    repair
                        .committing_at
                        .unwrap_or(claim_started_at)
                        .max(claim_started_at),
                );
                repair.participants.clone_from(participants);
                repair.failure_category = Some(
                    if evidence.as_ref().is_ok_and(|evidence| {
                        evidence.status == Some(ControlPlaneTxStatus::Aborted)
                    }) {
                        ControlPlaneHandleFailureCategory::ParticipantAborted
                    } else {
                        ControlPlaneHandleFailureCategory::ParticipantUncertain
                    },
                );
                if let Ok(evidence) = evidence
                    && evidence.status.is_some()
                {
                    let index = usize::try_from(participant.ordinal - 1)
                        .map_err(|_| ApiError::internal("participant ordinal exceeds usize"))?;
                    let retained = repair
                        .participants
                        .get_mut(index)
                        .ok_or_else(|| ApiError::internal("claim participant index is missing"))?;
                    retained.tx_id = evidence.tx_id;
                    retained.low_level_status = evidence.status;
                    retained.receipt_path = evidence.receipt_path;
                }
                repair.validate().map_err(ApiError::from)?;
                if !self.write_handle_cas(stored, &repair).await? {
                    let winner = self.load_handle_required(&repair.handle_id).await?;
                    if !matches!(
                        winner.record.status,
                        ControlPlaneHandleStatus::RepairRequired
                            | ControlPlaneHandleStatus::Committing
                            | ControlPlaneHandleStatus::Visible
                    ) {
                        return Err(ApiError::conflict(
                            "transaction handle changed while retaining a low-level claim",
                        ));
                    }
                }
                return Err(ApiError::conflict(
                    "low-level transaction claim exists; recovery is required",
                ));
            }
        }
        Ok(())
    }

    async fn terminal_transition(
        &self,
        stored: VersionedHandle,
        status: ControlPlaneHandleStatus,
        now: DateTime<Utc>,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        if now < stored.record.updated_at {
            return Err(ApiError::bad_request(
                "terminal transition time precedes current handle revision",
            ));
        }
        let mut next = stored.record.clone();
        next.status = status;
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| ApiError::conflict("handle revision overflows"))?;
        next.updated_at = now;
        next.terminal_at = Some(now);
        next.failure_category = None;
        next.validate().map_err(ApiError::from)?;
        if self.write_handle_cas(&stored, &next).await? {
            return Ok(next);
        }
        let winner = self.get_handle(&stored.record.handle_id).await?;
        if winner.status == status && same_handle_definition(&winner, &stored.record) {
            return Ok(winner);
        }
        Err(ApiError::conflict(
            "transaction handle changed during terminal CAS",
        ))
    }

    fn handle_metadata(&self, request_id: &str, idempotency_key: &str) -> ResolvedRequestMetadata {
        ResolvedRequestMetadata {
            tenant: self.transaction_service.ctx.tenant.clone(),
            workspace: self.transaction_service.ctx.workspace.clone(),
            request_id: request_id.to_string(),
            idempotency_key: idempotency_key.to_string(),
        }
    }

    fn verify_review_token(
        &self,
        handle: &ControlPlaneHandleRecord,
        review_token: &str,
    ) -> Result<(), ApiError> {
        if !token_matches(&handle.review_token_verifier, review_token) {
            return Err(ApiError::forbidden(
                "invalid transaction handle review token",
            ));
        }
        Ok(())
    }

    async fn load_handle_required(&self, handle_id: &str) -> Result<VersionedHandle, ApiError> {
        self.load_handle(handle_id).await?.ok_or_else(|| {
            ApiError::not_found(format!("transaction handle not found: {handle_id}"))
        })
    }

    async fn load_handle(&self, handle_id: &str) -> Result<Option<VersionedHandle>, ApiError> {
        load_exact_versioned_handle(&self.transaction_service, handle_id).await
    }

    async fn write_handle_cas(
        &self,
        stored: &VersionedHandle,
        next: &ControlPlaneHandleRecord,
    ) -> Result<bool, ApiError> {
        let path =
            ControlPlaneTxPaths::handle_record(&stored.record.handle_id).map_err(ApiError::from)?;
        let bytes = Bytes::from(next.to_json_vec().map_err(ApiError::from)?);
        match self
            .transaction_service
            .storage
            .put_raw(
                &path,
                bytes,
                WritePrecondition::MatchesVersion(stored.version.clone()),
            )
            .await?
        {
            WriteResult::Success { .. } => Ok(true),
            WriteResult::PreconditionFailed { .. } => Ok(false),
        }
    }

    async fn load_all_staged(
        &self,
        handle: &ControlPlaneHandleRecord,
    ) -> Result<Vec<PersistedHandleMutation>, ApiError> {
        let mut staged = Vec::with_capacity(handle.mutation_refs.len());
        for reference in &handle.mutation_refs {
            let authority = load_exact_identity_authority(
                &self.transaction_service,
                &handle.handle_id,
                &handle.scope,
                reference.ordinal,
            )
            .await?
            .ok_or_else(|| {
                ApiError::internal("referenced handle mutation is missing its identity authority")
            })?;
            let mutation =
                validate_exact_handle_intent(&self.transaction_service, handle, &authority.record)
                    .await?
                    .ok_or_else(|| {
                        ApiError::internal(
                            "referenced handle mutation has no durable handle intent",
                        )
                    })?;
            staged.push(mutation);
        }
        Ok(staged)
    }
}

#[derive(Debug)]
struct LowLevelEvidence {
    tx_id: Option<String>,
    status: Option<ControlPlaneTxStatus>,
    receipt_path: Option<String>,
    uncertain: bool,
    repair_pending: bool,
    durable_append_present: bool,
}

#[derive(Debug)]
struct LowLevelInspection {
    evidence: LowLevelEvidence,
    visible_record: Option<ControlPlaneTxRecord<serde_json::Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LowLevelReconciliationMode {
    Allowed,
    Forbidden,
    ObserveOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandleDriveMode {
    Commit,
    Recovery,
}

#[derive(Debug, Clone)]
struct LowLevelExpectation {
    domain: ControlPlaneTxDomain,
    kind: ControlPlaneTxKind,
    request_id: String,
    idempotency_key: String,
    request_hash: String,
    expected_tx_id: Option<String>,
}

impl From<&ControlPlaneHandleParticipant> for LowLevelExpectation {
    fn from(participant: &ControlPlaneHandleParticipant) -> Self {
        Self {
            domain: participant.domain,
            kind: participant.kind,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            expected_tx_id: participant.tx_id.clone(),
        }
    }
}

impl LowLevelEvidence {
    fn missing() -> Self {
        Self {
            tx_id: None,
            status: None,
            receipt_path: None,
            uncertain: false,
            repair_pending: false,
            durable_append_present: false,
        }
    }

    fn visible(tx_id: String, receipt_path: Option<String>) -> Self {
        Self {
            tx_id: Some(tx_id),
            status: Some(ControlPlaneTxStatus::Visible),
            receipt_path,
            uncertain: false,
            repair_pending: false,
            durable_append_present: false,
        }
    }
}

impl ControlPlaneTransactionHandleService<'_> {
    /// Commits a prepared handle or resumes an already-started commit using
    /// the same persisted low-level idempotency identities.
    pub(super) async fn commit_handle(
        &self,
        handle_id: &str,
        review_token: &str,
        now: DateTime<Utc>,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        let mut stored = self.load_handle_required(handle_id).await?;
        self.verify_review_token(&stored.record, review_token)?;
        if now < stored.record.updated_at {
            return Err(ApiError::bad_request(
                "handle commit time precedes the current revision",
            ));
        }
        match stored.record.status {
            ControlPlaneHandleStatus::Visible => {
                self.verify_exact_visible_handle(&stored.record).await?;
                return Ok(stored.record);
            }
            ControlPlaneHandleStatus::Prepared => {
                if now >= stored.record.expires_at {
                    let _ = self.expire_loaded(stored, now).await?;
                    return Err(ApiError::conflict(
                        "transaction handle is expired and terminal",
                    ));
                }
                ensure_revision_capacity(
                    &stored.record,
                    remaining_participant_count(&stored.record)
                        .checked_add(3)
                        .ok_or_else(|| ApiError::conflict("handle revision reserve overflows"))?,
                )?;
                let mut committing = stored.record.clone();
                committing.status = ControlPlaneHandleStatus::Committing;
                committing.revision = committing
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| ApiError::conflict("handle revision overflows"))?;
                committing.updated_at = now;
                committing.committing_at = Some(now);
                committing.validate().map_err(ApiError::from)?;
                if self.write_handle_cas(&stored, &committing).await? {
                    stored = self.load_handle_required(handle_id).await?;
                } else {
                    let winner = self.load_handle_required(handle_id).await?;
                    if !same_handle_identity(&winner.record, &stored.record)
                        || !same_handle_definition(&winner.record, &stored.record)
                        || !matches!(
                            winner.record.status,
                            ControlPlaneHandleStatus::Committing
                                | ControlPlaneHandleStatus::RepairRequired
                                | ControlPlaneHandleStatus::Visible
                        )
                    {
                        return Err(ApiError::conflict(
                            "transaction handle changed before commit could start",
                        ));
                    }
                    stored = winner;
                }
            }
            ControlPlaneHandleStatus::Committing => {}
            ControlPlaneHandleStatus::RepairRequired => {
                return Err(ApiError::conflict(
                    "repair-required transaction handle must use recovery",
                ));
            }
            ControlPlaneHandleStatus::Open | ControlPlaneHandleStatus::Preparing => {
                return Err(ApiError::conflict(
                    "transaction handle must be prepared before commit",
                ));
            }
            ControlPlaneHandleStatus::Aborted | ControlPlaneHandleStatus::Expired => {
                return Err(ApiError::conflict(
                    "terminal transaction handle cannot be committed",
                ));
            }
        }
        if stored.record.status == ControlPlaneHandleStatus::Visible {
            self.verify_exact_visible_handle(&stored.record).await?;
            return Ok(stored.record);
        }
        Box::pin(self.drive_commit(stored, now, HandleDriveMode::Commit)).await
    }

    /// Recovers an uncertain or partially visible handle without undoing or
    /// allocating a new handle participant identity.
    pub(super) async fn recover_handle(
        &self,
        handle_id: &str,
        review_token: &str,
        now: DateTime<Utc>,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        let stored = self.load_handle_required(handle_id).await?;
        self.verify_review_token(&stored.record, review_token)?;
        if now < stored.record.updated_at {
            return Err(ApiError::bad_request(
                "handle recovery time precedes the current revision",
            ));
        }
        match stored.record.status {
            ControlPlaneHandleStatus::Visible => {
                self.verify_exact_visible_handle(&stored.record).await?;
                Ok(stored.record)
            }
            ControlPlaneHandleStatus::Committing | ControlPlaneHandleStatus::RepairRequired => {
                Box::pin(self.drive_commit(stored, now, HandleDriveMode::Recovery)).await
            }
            _ => Err(ApiError::conflict(
                "transaction handle is not in a recoverable visibility state",
            )),
        }
    }

    async fn drive_commit(
        &self,
        mut stored: VersionedHandle,
        now: DateTime<Utc>,
        mode: HandleDriveMode,
    ) -> Result<ControlPlaneHandleRecord, ApiError> {
        if mode == HandleDriveMode::Commit
            && stored.record.status == ControlPlaneHandleStatus::RepairRequired
        {
            return Err(ApiError::conflict(
                "repair-required transaction handle must use recovery",
            ));
        }
        let staged = self.load_all_staged(&stored.record).await?;
        if staged.len() != stored.record.participants.len() {
            return Err(ApiError::internal(
                "prepared transaction handle participant set is inconsistent",
            ));
        }

        self.ensure_drive_revision_capacity(&stored.record)?;

        for (index, mutation) in staged.iter().enumerate() {
            self.ensure_drive_revision_capacity(&stored.record)?;
            let participant = stored
                .record
                .participants
                .get(index)
                .ok_or_else(|| ApiError::internal("prepared handle participant index is missing"))?
                .clone();
            self.validate_staged_participant_binding(&participant, mutation)?;
            let observed = match self.inspect_participant(&participant, mutation).await {
                Ok(evidence) => evidence,
                Err(_inspection_error) => {
                    let _ = self
                        .mark_repair_required(
                            stored,
                            index,
                            None,
                            now,
                            ControlPlaneHandleFailureCategory::ParticipantUncertain,
                        )
                        .await?;
                    return Err(ApiError::conflict(
                        "transaction handle requires exact-path recovery",
                    ));
                }
            };
            if participant.low_level_status == Some(ControlPlaneTxStatus::Visible) {
                let receipt_matches = participant.receipt_path.is_none()
                    || participant.receipt_path == observed.receipt_path;
                if observed.status == Some(ControlPlaneTxStatus::Visible)
                    && observed.tx_id == participant.tx_id
                    && receipt_matches
                {
                    continue;
                }
                let _ = self
                    .mark_repair_required(
                        stored,
                        index,
                        None,
                        now,
                        ControlPlaneHandleFailureCategory::ParticipantUncertain,
                    )
                    .await?;
                return Err(ApiError::conflict(
                    "cached participant visibility is not exact-readable",
                ));
            }
            if observed.status == Some(ControlPlaneTxStatus::Visible) {
                stored = self
                    .persist_participant_evidence(stored, index, observed, now)
                    .await?;
                if stored.record.status == ControlPlaneHandleStatus::Visible {
                    self.verify_exact_visible_participants(&stored.record, &staged)
                        .await?;
                    return Ok(stored.record);
                }
                if mode == HandleDriveMode::Commit
                    && stored.record.status == ControlPlaneHandleStatus::RepairRequired
                {
                    return Err(ApiError::conflict(
                        "commit lost participant evidence to repair recovery",
                    ));
                }
                continue;
            }

            let recoverable_root_aborted = participant.kind == ControlPlaneTxKind::RootCommit
                && observed.status == Some(ControlPlaneTxStatus::Aborted)
                && observed.tx_id.is_some()
                && !observed.uncertain
                && participant
                    .tx_id
                    .as_ref()
                    .is_none_or(|tx_id| Some(tx_id) == observed.tx_id.as_ref());
            if observed.status == Some(ControlPlaneTxStatus::Aborted) && !recoverable_root_aborted {
                let _ = self
                    .mark_repair_required(
                        stored,
                        index,
                        Some(observed),
                        now,
                        ControlPlaneHandleFailureCategory::ParticipantAborted,
                    )
                    .await?;
                return Err(ApiError::conflict(
                    "aborted low-level participant requires in-place operator recovery",
                ));
            }

            let recoverable_root_partial = participant.kind == ControlPlaneTxKind::RootCommit
                && observed.tx_id.is_some()
                && observed.status == Some(ControlPlaneTxStatus::Prepared)
                && (!observed.uncertain
                    || (mode == HandleDriveMode::Recovery
                        && observed.repair_pending
                        && !observed.durable_append_present))
                && participant
                    .tx_id
                    .as_ref()
                    .is_none_or(|tx_id| Some(tx_id) == observed.tx_id.as_ref());
            let recoverable_orchestration_append = participant.kind
                == ControlPlaneTxKind::OrchestrationBatch
                && observed.status == Some(ControlPlaneTxStatus::Prepared)
                && observed.repair_pending
                && observed.durable_append_present;
            let recoverable_clean_prepared = mode == HandleDriveMode::Recovery
                && observed.status == Some(ControlPlaneTxStatus::Prepared)
                && observed.tx_id.is_some()
                && !observed.repair_pending
                && !observed.durable_append_present
                && matches!(
                    participant.kind,
                    ControlPlaneTxKind::CatalogDdl
                        | ControlPlaneTxKind::OrchestrationBatch
                        | ControlPlaneTxKind::RootCommit
                );
            if (observed.uncertain || observed.status == Some(ControlPlaneTxStatus::Prepared))
                && !recoverable_root_partial
                && !recoverable_orchestration_append
                && !recoverable_clean_prepared
            {
                let _ = self
                    .mark_repair_required(
                        stored,
                        index,
                        Some(observed),
                        now,
                        ControlPlaneHandleFailureCategory::ParticipantUncertain,
                    )
                    .await?;
                return Err(ApiError::conflict(
                    "transaction handle requires exact-path recovery",
                ));
            }

            let mut effective_participant = participant.clone();
            if effective_participant.tx_id.is_none() {
                effective_participant.tx_id.clone_from(&observed.tx_id);
                effective_participant.low_level_status = observed.status;
            }
            match Box::pin(self.execute_participant(&effective_participant, mutation)).await {
                Ok(provisional) => {
                    let Ok(evidence) = self
                        .inspect_exact_visible_participant(
                            &effective_participant,
                            mutation,
                            Some(&provisional),
                        )
                        .await
                    else {
                        let _ = self
                            .mark_repair_required(
                                stored,
                                index,
                                None,
                                now,
                                ControlPlaneHandleFailureCategory::ParticipantUncertain,
                            )
                            .await?;
                        return Err(ApiError::conflict(
                            "executor response lacks exact durable participant proof",
                        ));
                    };
                    stored = self
                        .persist_participant_evidence(stored, index, evidence, now)
                        .await?;
                    if stored.record.status == ControlPlaneHandleStatus::Visible {
                        self.verify_exact_visible_participants(&stored.record, &staged)
                            .await?;
                        return Ok(stored.record);
                    }
                    if mode == HandleDriveMode::Commit
                        && stored.record.status == ControlPlaneHandleStatus::RepairRequired
                    {
                        return Err(ApiError::conflict(
                            "commit lost participant evidence to repair recovery",
                        ));
                    }
                }
                Err(_execution_error) => {
                    let evidence = self
                        .inspect_participant(&participant, mutation)
                        .await
                        .unwrap_or_else(|_| LowLevelEvidence {
                            tx_id: participant.tx_id.clone(),
                            status: participant.low_level_status,
                            receipt_path: participant.receipt_path.clone(),
                            uncertain: true,
                            repair_pending: false,
                            durable_append_present: false,
                        });
                    let category = if evidence.status == Some(ControlPlaneTxStatus::Aborted)
                        && !evidence.uncertain
                    {
                        ControlPlaneHandleFailureCategory::ParticipantAborted
                    } else {
                        ControlPlaneHandleFailureCategory::ParticipantUncertain
                    };
                    let _ = self
                        .mark_repair_required(stored, index, Some(evidence), now, category)
                        .await?;
                    return Err(ApiError::conflict(
                        "transaction handle requires exact-path recovery",
                    ));
                }
            }
        }

        if stored.record.visible_participant_count() != stored.record.participants.len() {
            return Err(ApiError::conflict(
                "transaction handle remains partially visible and requires recovery",
            ));
        }
        if self
            .verify_exact_visible_participants(&stored.record, &staged)
            .await
            .is_err()
        {
            let _ = self
                .mark_repair_required(
                    stored,
                    0,
                    None,
                    now,
                    ControlPlaneHandleFailureCategory::ParticipantUncertain,
                )
                .await?;
            return Err(ApiError::conflict(
                "transaction handle finalization lacks exact participant proof",
            ));
        }
        let mut visible = stored.record.clone();
        visible.status = ControlPlaneHandleStatus::Visible;
        visible.revision = visible
            .revision
            .checked_add(1)
            .ok_or_else(|| ApiError::conflict("handle revision overflows"))?;
        visible.updated_at = now.max(visible.updated_at);
        visible.visible_at = Some(visible.updated_at);
        visible.failure_category = None;
        visible.validate().map_err(ApiError::from)?;
        if self.write_handle_cas(&stored, &visible).await? {
            return Ok(visible);
        }
        let winner = self.get_handle(&visible.handle_id).await?;
        if winner.status == ControlPlaneHandleStatus::Visible
            && same_handle_identity(&winner, &visible)
            && winner.mutation_refs == visible.mutation_refs
            && winner.participants == visible.participants
        {
            self.verify_exact_visible_handle(&winner).await?;
            return Ok(winner);
        }
        Err(ApiError::conflict(
            "transaction handle final CAS winner is not equivalent",
        ))
    }

    fn ensure_drive_revision_capacity(
        &self,
        handle: &ControlPlaneHandleRecord,
    ) -> Result<(), ApiError> {
        let repair_reserve = usize::from(handle.status == ControlPlaneHandleStatus::Committing);
        let required = remaining_participant_count(handle)
            .checked_add(1)
            .and_then(|value| value.checked_add(repair_reserve))
            .ok_or_else(|| ApiError::conflict("handle revision reserve overflows"))?;
        ensure_revision_capacity(handle, required)
    }

    async fn mark_repair_required(
        &self,
        stored: VersionedHandle,
        index: usize,
        evidence: Option<LowLevelEvidence>,
        now: DateTime<Utc>,
        category: ControlPlaneHandleFailureCategory,
    ) -> Result<VersionedHandle, ApiError> {
        if stored.record.status == ControlPlaneHandleStatus::RepairRequired {
            return Ok(stored);
        }
        let mut repair = stored.record.clone();
        repair.status = ControlPlaneHandleStatus::RepairRequired;
        repair.revision = repair
            .revision
            .checked_add(1)
            .ok_or_else(|| ApiError::conflict("handle revision overflows"))?;
        repair.updated_at = now.max(repair.updated_at);
        repair.failure_category = Some(category);
        if let Some(evidence) = evidence {
            let repair_participant = repair
                .participants
                .get_mut(index)
                .ok_or_else(|| ApiError::internal("repair handle participant index is missing"))?;
            if evidence.status.is_some() {
                repair_participant.tx_id = evidence.tx_id;
                repair_participant.low_level_status = evidence.status;
                repair_participant.receipt_path = evidence.receipt_path;
            }
        }
        repair.validate().map_err(ApiError::from)?;
        if self.write_handle_cas(&stored, &repair).await? {
            return self.load_handle_required(&repair.handle_id).await;
        }
        let winner = self.load_handle_required(&repair.handle_id).await?;
        if same_handle_definition(&winner.record, &repair)
            && matches!(
                winner.record.status,
                ControlPlaneHandleStatus::Committing
                    | ControlPlaneHandleStatus::RepairRequired
                    | ControlPlaneHandleStatus::Visible
            )
        {
            return Ok(winner);
        }
        Err(ApiError::conflict(
            "transaction handle CAS winner is unsafe after participant uncertainty",
        ))
    }

    async fn persist_participant_evidence(
        &self,
        stored: VersionedHandle,
        index: usize,
        evidence: LowLevelEvidence,
        now: DateTime<Utc>,
    ) -> Result<VersionedHandle, ApiError> {
        if evidence.status != Some(ControlPlaneTxStatus::Visible) {
            return Err(ApiError::conflict(
                "low-level participant is not visibly committed",
            ));
        }
        let mut next = stored.record.clone();
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| ApiError::conflict("handle revision overflows"))?;
        next.updated_at = now.max(next.updated_at);
        let participant = next
            .participants
            .get_mut(index)
            .ok_or_else(|| ApiError::internal("visible handle participant index is missing"))?;
        participant.tx_id = evidence.tx_id;
        participant.low_level_status = evidence.status;
        participant.receipt_path = evidence.receipt_path;
        let expected_tx_id = participant.tx_id.clone();
        next.validate().map_err(ApiError::from)?;
        if !self.write_handle_cas(&stored, &next).await? {
            let winner = self.load_handle_required(&next.handle_id).await?;
            if same_handle_definition(&winner.record, &next)
                && winner
                    .record
                    .participants
                    .get(index)
                    .is_some_and(|participant| {
                        participant.low_level_status == Some(ControlPlaneTxStatus::Visible)
                            && participant.tx_id == expected_tx_id
                    })
            {
                return Ok(winner);
            }
            return Err(ApiError::conflict(
                "transaction handle changed while recording visible participant",
            ));
        }
        self.load_handle_required(&next.handle_id).await
    }

    async fn execute_participant(
        &self,
        participant: &ControlPlaneHandleParticipant,
        staged: &PersistedHandleMutation,
    ) -> Result<LowLevelEvidence, ApiError> {
        let metadata = self.handle_metadata(&participant.request_id, &participant.idempotency_key);
        match &staged.mutation {
            StagedMutation::Catalog { operation } => {
                let outcome = self
                    .transaction_service
                    .execute_catalog_mutation_for_handle(
                        &metadata,
                        operation.to_runtime(),
                        participant.tx_id.as_deref(),
                    )
                    .await?;
                let receipt_path = self
                    .existing_receipt_path(CatalogPaths::commit(
                        CatalogDomain::Catalog,
                        &outcome.receipt.commit_id,
                    ))
                    .await?;
                Ok(LowLevelEvidence::visible(
                    outcome.receipt.tx_id,
                    receipt_path,
                ))
            }
            StagedMutation::Orchestration { events } => {
                let envelopes = events
                    .iter()
                    .map(event_to_proto_envelope)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| {
                        ApiError::internal(
                            "persisted orchestration event cannot map to the current contract",
                        )
                    })?;
                let outcome = self
                    .transaction_service
                    .execute_orchestration_batch_for_handle(
                        &metadata,
                        OrchestrationBatchMutation { events: envelopes },
                        participant.tx_id.as_deref(),
                    )
                    .await?;
                let receipt_path = self
                    .existing_receipt_path(ControlPlaneTxPaths::orchestration_commit_receipt(
                        &outcome.receipt.commit_id,
                    ))
                    .await?;
                Ok(LowLevelEvidence::visible(
                    outcome.receipt.tx_id,
                    receipt_path,
                ))
            }
            StagedMutation::Root { mutations } => {
                let request = root_request_from_staged(mutations)?;
                let mut context = self.transaction_service.ctx.clone();
                context.request_id.clone_from(&participant.request_id);
                context.idempotency_key = Some(participant.idempotency_key.clone());
                let service =
                    ControlPlaneTransactionService::new(self.transaction_service.state, context)?;
                let tx_id = if let Some(tx_id) = &participant.tx_id {
                    let receipt = Box::pin(service.recover_root_transaction_in_place(
                        request,
                        tx_id,
                        &participant.request_hash,
                    ))
                    .await?
                    .receipt;
                    receipt.tx_id
                } else {
                    let response = service.commit_root_transaction_for_handle(request).await?;
                    let receipt = response.receipt.ok_or_else(|| {
                        ApiError::internal("visible root transaction is missing receipt")
                    })?;
                    receipt.tx_id
                };
                Ok(LowLevelEvidence::visible(tx_id, None))
            }
        }
    }

    async fn inspect_participant(
        &self,
        participant: &ControlPlaneHandleParticipant,
        staged: &PersistedHandleMutation,
    ) -> Result<LowLevelEvidence, ApiError> {
        self.validate_staged_participant_binding(participant, staged)?;
        let expectation = LowLevelExpectation::from(participant);
        let root_mutations = match &staged.mutation {
            StagedMutation::Root { mutations } => Some(mutations.as_slice()),
            StagedMutation::Catalog { .. } | StagedMutation::Orchestration { .. } => None,
        };
        let orchestration_events = match &staged.mutation {
            StagedMutation::Orchestration { events } => Some(events.as_slice()),
            StagedMutation::Catalog { .. } | StagedMutation::Root { .. } => None,
        };
        let mut root_evidence = self
            .inspect_low_level(&expectation, root_mutations, orchestration_events)
            .await?;
        let mutations = match &staged.mutation {
            StagedMutation::Catalog { .. } => {
                if root_evidence.status == Some(ControlPlaneTxStatus::Visible) {
                    self.load_valid_catalog_authority(&expectation).await?;
                }
                return Ok(root_evidence);
            }
            StagedMutation::Orchestration { events } => {
                if root_evidence.status == Some(ControlPlaneTxStatus::Visible) {
                    self.load_valid_orchestration_authority(&expectation, events)
                        .await?;
                }
                return Ok(root_evidence);
            }
            StagedMutation::Root { mutations } => mutations,
        };
        if root_evidence.status == Some(ControlPlaneTxStatus::Visible) {
            self.verify_exact_visible_root_authority(participant, mutations)
                .await?;
            return Ok(root_evidence);
        }

        let mut child_claim_exists = false;
        let mut child_unrecoverable = false;
        for mutation in mutations {
            let (domain, kind, request_hash, orchestration_events) = match mutation {
                StagedRootMutation::Catalog { operation } => (
                    ControlPlaneTxDomain::Catalog,
                    ControlPlaneTxKind::CatalogDdl,
                    operation.to_runtime().request_hash()?,
                    None,
                ),
                StagedRootMutation::Orchestration { events } => (
                    ControlPlaneTxDomain::Orchestration,
                    ControlPlaneTxKind::OrchestrationBatch,
                    OrchestrationBatchMutation { events: Vec::new() }
                        .request_hash_for_events(events)?,
                    Some(events.as_slice()),
                ),
            };
            let key = format!("root:{}:{}", participant.idempotency_key, domain.as_str());
            let evidence = self
                .inspect_low_level(
                    &LowLevelExpectation {
                        domain,
                        kind,
                        request_id: participant.request_id.clone(),
                        idempotency_key: key,
                        request_hash,
                        expected_tx_id: None,
                    },
                    None,
                    orchestration_events,
                )
                .await?;
            let claim_exists = evidence.tx_id.is_some() || evidence.status.is_some();
            child_claim_exists |= claim_exists;
            let recoverable_orchestration_append = domain == ControlPlaneTxDomain::Orchestration
                && evidence.status == Some(ControlPlaneTxStatus::Prepared)
                && evidence.repair_pending
                && evidence.durable_append_present;
            child_unrecoverable |= claim_exists
                && evidence.status != Some(ControlPlaneTxStatus::Visible)
                && !recoverable_orchestration_append;
        }
        if child_claim_exists {
            root_evidence.receipt_path = None;
            if root_evidence.tx_id.is_some() {
                root_evidence.status = Some(ControlPlaneTxStatus::Prepared);
                root_evidence.uncertain = child_unrecoverable;
            } else {
                root_evidence.status = None;
                root_evidence.uncertain = true;
            }
        }
        Ok(root_evidence)
    }

    async fn load_exact_visible_record<TResult>(
        &self,
        expected: &LowLevelExpectation,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: Clone + DeserializeOwned + PartialEq,
    {
        let marker_path =
            ControlPlaneTxPaths::idempotency(expected.domain, &expected.idempotency_key);
        let marker_bytes = get_optional(&self.transaction_service.storage, &marker_path)
            .await?
            .ok_or_else(|| ApiError::internal("visible transaction marker is missing"))?;
        let marker: ControlPlaneIdempotencyRecord =
            serde_json::from_slice(marker_bytes.as_ref())
                .map_err(|_| ApiError::internal("visible transaction marker is corrupt"))?;
        if !is_canonical_ulid(&marker.tx_id)
            || marker.kind != expected.kind
            || marker.request_id != expected.request_id
            || marker.idempotency_key != expected.idempotency_key
            || marker.request_hash != expected.request_hash
            || expected
                .expected_tx_id
                .as_ref()
                .is_some_and(|tx_id| tx_id != &marker.tx_id)
        {
            return Err(ApiError::internal(
                "visible transaction marker does not match exact authority",
            ));
        }
        let record_path = ControlPlaneTxPaths::record(expected.domain, &marker.tx_id);
        let record_bytes = get_optional(&self.transaction_service.storage, &record_path)
            .await?
            .ok_or_else(|| ApiError::internal("visible transaction record is missing"))?;
        let record: ControlPlaneTxRecord<TResult> =
            serde_json::from_slice(record_bytes.as_ref())
                .map_err(|_| ApiError::internal("visible transaction record is corrupt"))?;
        if record.tx_id != marker.tx_id
            || record.kind != expected.kind
            || record.request_id != expected.request_id
            || record.idempotency_key != expected.idempotency_key
            || record.request_hash != expected.request_hash
            || record.status != ControlPlaneTxStatus::Visible
            || record.visible_at.is_none()
            || record
                .visible_at
                .is_some_and(|visible_at| visible_at < record.prepared_at)
            || record.result.is_none()
            || record.durable_append.is_some()
            || marker.visible_at != record.visible_at
        {
            return Err(ApiError::internal(
                "visible transaction record does not match exact authority",
            ));
        }
        let cached: ControlPlaneTxRecord<TResult> = marker
            .tx_record
            .clone()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| ApiError::internal("cached visible transaction record is corrupt"))?
            .ok_or_else(|| ApiError::internal("cached visible transaction record is missing"))?;
        if cached != record {
            return Err(ApiError::internal(
                "cached and exact visible transaction records diverge",
            ));
        }
        Ok(record)
    }

    async fn load_valid_catalog_authority(
        &self,
        expected: &LowLevelExpectation,
    ) -> Result<(ControlPlaneTxRecord<CatalogTxReceipt>, CatalogTxReceipt), ApiError> {
        let record: ControlPlaneTxRecord<CatalogTxReceipt> =
            self.load_exact_visible_record(expected).await?;
        let receipt = self.validate_catalog_authority_record(&record).await?;
        Ok((record, receipt))
    }

    async fn validate_catalog_authority_record(
        &self,
        record: &ControlPlaneTxRecord<CatalogTxReceipt>,
    ) -> Result<CatalogTxReceipt, ApiError> {
        self.transaction_service
            .validate_catalog_visible_authority(record)
            .await
    }

    async fn load_valid_orchestration_authority(
        &self,
        expected: &LowLevelExpectation,
        expected_events: &[OrchestrationEvent],
    ) -> Result<
        (
            ControlPlaneTxRecord<OrchestrationTxReceipt>,
            OrchestrationTxReceipt,
        ),
        ApiError,
    > {
        let record: ControlPlaneTxRecord<OrchestrationTxReceipt> =
            self.load_exact_visible_record(expected).await?;
        let receipt = self
            .validate_orchestration_authority_record(&record, expected_events)
            .await?;
        Ok((record, receipt))
    }

    async fn validate_orchestration_authority_record(
        &self,
        record: &ControlPlaneTxRecord<OrchestrationTxReceipt>,
        expected_events: &[OrchestrationEvent],
    ) -> Result<OrchestrationTxReceipt, ApiError> {
        self.transaction_service
            .validate_orchestration_visible_authority(record, expected_events)
            .await
    }

    async fn verify_exact_visible_root_authority(
        &self,
        participant: &ControlPlaneHandleParticipant,
        mutations: &[StagedRootMutation],
    ) -> Result<(), ApiError> {
        let root_expectation = LowLevelExpectation::from(participant);
        let root_record: RootTxRecord = self.load_exact_visible_record(&root_expectation).await?;
        self.validate_visible_root_authority_record(
            &root_expectation,
            mutations,
            &root_record,
            true,
        )
        .await?;
        self.ensure_root_audit_receipt(&root_record).await
    }

    async fn validate_visible_root_authority_record(
        &self,
        root_expectation: &LowLevelExpectation,
        mutations: &[StagedRootMutation],
        root_record: &RootTxRecord,
        reconcile_children: bool,
    ) -> Result<(), ApiError> {
        if root_record.lock_path != ControlPlaneTxPaths::root_lock()
            || root_record.fencing_token == 0
            || root_record
                .visible_at
                .is_none_or(|visible_at| visible_at < root_record.prepared_at)
        {
            return Err(ApiError::internal(
                "visible root transaction has non-canonical lock authority",
            ));
        }
        let receipt = root_record
            .result
            .as_ref()
            .ok_or_else(|| ApiError::internal("visible root transaction result is missing"))?;
        if receipt.tx_id != root_record.tx_id
            || !is_canonical_ulid(&receipt.root_commit_id)
            || receipt.read_token != format!("root:{}", root_record.tx_id)
            || Some(receipt.visible_at) != root_record.visible_at
        {
            return Err(ApiError::internal(
                "visible root receipt does not match its transaction authority",
            ));
        }
        let canonical_manifest_path = ControlPlaneTxPaths::root_super_manifest(&root_record.tx_id);
        if receipt.super_manifest_path != canonical_manifest_path {
            return Err(ApiError::internal(
                "visible root receipt names a non-canonical super-manifest",
            ));
        }
        let manifest_bytes =
            get_optional(&self.transaction_service.storage, &canonical_manifest_path)
                .await?
                .ok_or_else(|| ApiError::internal("visible root super-manifest is missing"))?;
        let manifest: RootTxManifest = serde_json::from_slice(manifest_bytes.as_ref())
            .map_err(|_| ApiError::internal("visible root super-manifest is corrupt"))?;
        let manifest_precedes_preparation = manifest.published_at < root_record.prepared_at;
        if manifest.tx_id != root_record.tx_id
            || manifest.fencing_token != root_record.fencing_token
            || manifest_precedes_preparation
        {
            return Err(ApiError::internal(
                "visible root super-manifest does not match root authority",
            ));
        }
        if manifest.published_at != receipt.visible_at {
            return Err(ApiError::internal(
                "visible root super-manifest publication time diverges from its receipt",
            ));
        }

        let mut receipt_commits = BTreeMap::new();
        for commit in &receipt.domain_commits {
            if commit.domain == ControlPlaneTxDomain::Root
                || receipt_commits
                    .insert(commit.domain, commit.clone())
                    .is_some()
            {
                return Err(ApiError::internal(
                    "visible root receipt contains an invalid domain set",
                ));
            }
        }
        let mut staged_domains = BTreeSet::new();
        for mutation in mutations {
            if !staged_domains.insert(mutation.domain()) {
                return Err(ApiError::internal(
                    "staged root mutation contains a duplicate domain",
                ));
            }
        }
        if receipt_commits.keys().copied().collect::<BTreeSet<_>>() != staged_domains
            || manifest.domains.keys().copied().collect::<BTreeSet<_>>() != staged_domains
        {
            return Err(ApiError::internal(
                "visible root authority domain sets do not match staged mutations",
            ));
        }
        let expected_manifest_domains = receipt_commits
            .iter()
            .map(|(domain, commit)| {
                (
                    *domain,
                    RootTxManifestDomain {
                        manifest_id: commit.manifest_id.clone(),
                        manifest_path: commit.manifest_path.clone(),
                        commit_id: commit.commit_id.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        if manifest.domains != expected_manifest_domains {
            return Err(ApiError::internal(
                "visible root super-manifest diverges from its receipt",
            ));
        }

        let root_metadata = self.handle_metadata(
            &root_expectation.request_id,
            &root_expectation.idempotency_key,
        );
        let mut child_expectations = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            let domain = mutation.domain();
            let commit = receipt_commits.get(&domain).ok_or_else(|| {
                ApiError::internal("visible root receipt is missing a staged domain commit")
            })?;
            if !is_canonical_ulid(&commit.tx_id) || !is_canonical_ulid(&commit.commit_id) {
                return Err(ApiError::internal(
                    "visible root domain commit identifiers are non-canonical",
                ));
            }
            let child_metadata = self
                .transaction_service
                .root_participant_metadata(&root_metadata, domain);
            let request_hash = match mutation {
                StagedRootMutation::Catalog { operation } => {
                    operation.to_runtime().request_hash()?
                }
                StagedRootMutation::Orchestration { events } => {
                    OrchestrationBatchMutation { events: Vec::new() }
                        .request_hash_for_events(events)?
                }
            };
            let child_expectation = LowLevelExpectation {
                domain,
                kind: match domain {
                    ControlPlaneTxDomain::Catalog => ControlPlaneTxKind::CatalogDdl,
                    ControlPlaneTxDomain::Orchestration => ControlPlaneTxKind::OrchestrationBatch,
                    ControlPlaneTxDomain::Root => unreachable!("root cannot be its own child"),
                },
                request_id: child_metadata.request_id,
                idempotency_key: child_metadata.idempotency_key,
                request_hash,
                expected_tx_id: Some(commit.tx_id.clone()),
            };
            let inspection = Box::pin(self.inspect_low_level_pass(
                &child_expectation,
                None,
                match mutation {
                    StagedRootMutation::Orchestration { events } => Some(events.as_slice()),
                    StagedRootMutation::Catalog { .. } => None,
                },
                LowLevelReconciliationMode::ObserveOnly,
            ))
            .await?;
            if inspection.evidence.status != Some(ControlPlaneTxStatus::Visible)
                || inspection.evidence.tx_id.as_deref() != Some(commit.tx_id.as_str())
            {
                return Err(ApiError::internal(
                    "visible root child transaction is not exact-readable",
                ));
            }
            let child_record = inspection.visible_record.ok_or_else(|| {
                ApiError::internal("visible root child transaction record is missing")
            })?;
            match mutation {
                StagedRootMutation::Catalog { .. } => {
                    let child_record = decode_typed_transaction_record::<CatalogTxReceipt>(
                        &child_record,
                        "catalog",
                    )?;
                    let result = self
                        .validate_catalog_authority_record(&child_record)
                        .await?;
                    if root_domain_commit_from_catalog(&result) != *commit {
                        return Err(ApiError::internal(
                            "visible catalog child diverges from the root receipt",
                        ));
                    }
                }
                StagedRootMutation::Orchestration { events } => {
                    let child_record = decode_typed_transaction_record::<OrchestrationTxReceipt>(
                        &child_record,
                        "orchestration",
                    )?;
                    let result = self
                        .validate_orchestration_authority_record(&child_record, events)
                        .await?;
                    if root_domain_commit_from_orchestration(&result) != *commit {
                        return Err(ApiError::internal(
                            "visible orchestration child diverges from the root receipt",
                        ));
                    }
                }
            }
            child_expectations.push((
                child_expectation,
                match mutation {
                    StagedRootMutation::Orchestration { events } => Some(events.as_slice()),
                    StagedRootMutation::Catalog { .. } => None,
                },
            ));
        }

        let root_audit_path = ControlPlaneTxPaths::root_commit_receipt(&receipt.root_commit_id);
        if let Some(bytes) =
            get_optional(&self.transaction_service.storage, &root_audit_path).await?
        {
            let audit: RootTxReceipt = serde_json::from_slice(bytes.as_ref())
                .map_err(|_| ApiError::internal("root audit receipt is corrupt"))?;
            if audit != *receipt {
                return Err(ApiError::internal(
                    "root audit receipt diverges from root authority",
                ));
            }
        } else if !root_record.repair_pending {
            return Err(ApiError::internal(
                "root audit receipt is missing without repair authority",
            ));
        }
        if reconcile_children {
            for (child_expectation, orchestration_events) in child_expectations {
                let evidence = Box::pin(self.inspect_low_level(
                    &child_expectation,
                    None,
                    orchestration_events,
                ))
                .await?;
                if evidence.status != Some(ControlPlaneTxStatus::Visible)
                    || evidence.tx_id.as_deref() != child_expectation.expected_tx_id.as_deref()
                {
                    return Err(ApiError::internal(
                        "visible root child transaction did not reconcile exactly",
                    ));
                }
            }
            Box::pin(self.validate_visible_root_authority_record(
                root_expectation,
                mutations,
                root_record,
                false,
            ))
            .await?;
        }
        Ok(())
    }

    async fn ensure_root_audit_receipt(&self, root_record: &RootTxRecord) -> Result<(), ApiError> {
        let receipt = root_record
            .result
            .as_ref()
            .ok_or_else(|| ApiError::internal("visible root result is missing"))?;
        let path = ControlPlaneTxPaths::root_commit_receipt(&receipt.root_commit_id);
        match self
            .transaction_service
            .write_json(&path, receipt, WritePrecondition::DoesNotExist)
            .await?
        {
            super::WriteOutcome::Written | super::WriteOutcome::PreconditionFailed => {}
        }
        let stored = get_optional(&self.transaction_service.storage, &path)
            .await?
            .ok_or_else(|| ApiError::internal("root audit receipt is missing after repair"))?;
        let stored: RootTxReceipt = serde_json::from_slice(stored.as_ref())
            .map_err(|_| ApiError::internal("root audit receipt is corrupt after repair"))?;
        if stored != *receipt {
            return Err(ApiError::internal(
                "root audit receipt conflicts with exact visible authority",
            ));
        }
        Ok(())
    }

    async fn inspect_exact_visible_participant(
        &self,
        participant: &ControlPlaneHandleParticipant,
        staged: &PersistedHandleMutation,
        provisional: Option<&LowLevelEvidence>,
    ) -> Result<LowLevelEvidence, ApiError> {
        let durable = self.inspect_participant(participant, staged).await?;
        if durable.status != Some(ControlPlaneTxStatus::Visible) || durable.tx_id.is_none() {
            return Err(ApiError::conflict(
                "participant visibility is not exact-readable",
            ));
        }
        if participant
            .tx_id
            .as_ref()
            .is_some_and(|tx_id| Some(tx_id) != durable.tx_id.as_ref())
            || participant
                .receipt_path
                .as_ref()
                .is_some_and(|path| Some(path) != durable.receipt_path.as_ref())
        {
            return Err(ApiError::conflict(
                "durable participant evidence conflicts with its handle journal",
            ));
        }
        if let Some(provisional) = provisional
            && (provisional.status != Some(ControlPlaneTxStatus::Visible)
                || provisional.tx_id != durable.tx_id
                || provisional
                    .receipt_path
                    .as_ref()
                    .is_some_and(|path| Some(path) != durable.receipt_path.as_ref()))
        {
            return Err(ApiError::conflict(
                "executor response conflicts with exact participant evidence",
            ));
        }
        Ok(durable)
    }

    async fn verify_exact_visible_participants(
        &self,
        handle: &ControlPlaneHandleRecord,
        staged: &[PersistedHandleMutation],
    ) -> Result<(), ApiError> {
        if staged.len() != handle.participants.len() || staged.is_empty() {
            return Err(ApiError::internal(
                "visible handle participant set is inconsistent",
            ));
        }
        for (participant, mutation) in handle.participants.iter().zip(staged) {
            self.inspect_exact_visible_participant(participant, mutation, None)
                .await?;
        }
        Ok(())
    }

    async fn verify_exact_visible_handle(
        &self,
        handle: &ControlPlaneHandleRecord,
    ) -> Result<(), ApiError> {
        let staged = self.load_all_staged(handle).await?;
        self.verify_exact_visible_participants(handle, &staged)
            .await
    }

    async fn inspect_low_level(
        &self,
        expected: &LowLevelExpectation,
        root_mutations: Option<&[StagedRootMutation]>,
        orchestration_events: Option<&[OrchestrationEvent]>,
    ) -> Result<LowLevelEvidence, ApiError> {
        Ok(self
            .inspect_low_level_pass(
                expected,
                root_mutations,
                orchestration_events,
                LowLevelReconciliationMode::Allowed,
            )
            .await?
            .evidence)
    }

    async fn inspect_low_level_pass(
        &self,
        expected: &LowLevelExpectation,
        root_mutations: Option<&[StagedRootMutation]>,
        orchestration_events: Option<&[OrchestrationEvent]>,
        reconciliation_mode: LowLevelReconciliationMode,
    ) -> Result<LowLevelInspection, ApiError> {
        let idempotency_path =
            ControlPlaneTxPaths::idempotency(expected.domain, &expected.idempotency_key);
        let Some(bytes) =
            get_optional(&self.transaction_service.storage, &idempotency_path).await?
        else {
            if let Some(tx_id) = &expected.expected_tx_id {
                return Ok(LowLevelInspection {
                    evidence: LowLevelEvidence {
                        tx_id: Some(tx_id.clone()),
                        status: None,
                        receipt_path: None,
                        uncertain: true,
                        repair_pending: false,
                        durable_append_present: false,
                    },
                    visible_record: None,
                });
            }
            return Ok(LowLevelInspection {
                evidence: LowLevelEvidence::missing(),
                visible_record: None,
            });
        };
        let marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(bytes.as_ref())
            .map_err(|_| ApiError::internal("low-level idempotency evidence is corrupt"))?;
        let marker_tx_id = Ulid::from_string(&marker.tx_id)
            .ok()
            .filter(|parsed| parsed.to_string() == marker.tx_id);
        if marker_tx_id.is_none()
            || marker.kind != expected.kind
            || marker.request_id != expected.request_id
            || marker.idempotency_key != expected.idempotency_key
            || marker.request_hash != expected.request_hash
            || expected
                .expected_tx_id
                .as_ref()
                .is_some_and(|tx_id| tx_id != &marker.tx_id)
        {
            return Err(ApiError::internal(
                "low-level idempotency evidence does not match handle participant",
            ));
        }
        let cached = marker
            .tx_record
            .clone()
            .map(serde_json::from_value::<ControlPlaneTxRecord<serde_json::Value>>)
            .transpose()
            .map_err(|_| ApiError::internal("cached low-level transaction evidence is corrupt"))?;
        let record_path = ControlPlaneTxPaths::record(expected.domain, &marker.tx_id);
        let stored = if let Some(bytes) =
            get_optional(&self.transaction_service.storage, &record_path).await?
        {
            Some(
                serde_json::from_slice::<ControlPlaneTxRecord<serde_json::Value>>(bytes.as_ref())
                    .map_err(|_| ApiError::internal("low-level transaction evidence is corrupt"))?,
            )
        } else {
            None
        };
        for record in stored.iter().chain(cached.iter()) {
            if record.tx_id != marker.tx_id
                || record.kind != expected.kind
                || record.request_id != expected.request_id
                || record.idempotency_key != expected.idempotency_key
                || record.request_hash != expected.request_hash
            {
                return Err(ApiError::internal(
                    "low-level transaction record does not match handle participant ownership",
                ));
            }
        }
        if let Some(cached) = &cached {
            if cached.status != ControlPlaneTxStatus::Visible {
                return Err(ApiError::internal(
                    "cached low-level transaction evidence is not visible",
                ));
            }
            if marker.visible_at != cached.visible_at {
                return Err(ApiError::internal(
                    "cached visible marker timestamp conflicts with its transaction record",
                ));
            }
        }
        let needs_reconciliation = match (&cached, &stored) {
            (Some(_), None) => true,
            (Some(cached), Some(stored)) if cached != stored => {
                if !is_valid_cached_visible_successor(stored, cached)
                    && !is_valid_cached_visible_repair_divergence(stored, cached)
                {
                    return Err(ApiError::internal(
                        "cached visible transaction conflicts with its exact transaction record",
                    ));
                }
                true
            }
            (None, Some(stored)) if stored.status == ControlPlaneTxStatus::Visible => true,
            _ => false,
        };

        let joined_repair_candidate = match (&stored, &cached) {
            (Some(stored), Some(cached))
                if is_valid_cached_visible_repair_divergence(stored, cached) =>
            {
                let mut joined = stored.clone();
                joined.repair_pending = true;
                Some(joined)
            }
            _ => None,
        };
        if let Some(candidate) = &joined_repair_candidate {
            Box::pin(self.validate_typed_visible_candidate(
                expected,
                root_mutations,
                orchestration_events,
                candidate,
                false,
            ))
            .await?;
        } else {
            for candidate in cached
                .iter()
                .chain(stored.iter())
                .filter(|record| record.status == ControlPlaneTxStatus::Visible)
            {
                Box::pin(self.validate_typed_visible_candidate(
                    expected,
                    root_mutations,
                    orchestration_events,
                    candidate,
                    false,
                ))
                .await?;
            }
        }
        let visible_candidate = joined_repair_candidate.as_ref().or_else(|| {
            cached
                .as_ref()
                .filter(|record| record.status == ControlPlaneTxStatus::Visible)
                .or_else(|| {
                    stored
                        .as_ref()
                        .filter(|record| record.status == ControlPlaneTxStatus::Visible)
                })
        });
        if reconciliation_mode == LowLevelReconciliationMode::Allowed
            && expected.domain == ControlPlaneTxDomain::Root
            && let Some(candidate) = visible_candidate
        {
            Box::pin(self.validate_typed_visible_candidate(
                expected,
                root_mutations,
                orchestration_events,
                candidate,
                true,
            ))
            .await?;
        }
        if needs_reconciliation {
            match reconciliation_mode {
                LowLevelReconciliationMode::Allowed => {
                    self.reconcile_visible_low_level(
                        expected,
                        root_mutations,
                        orchestration_events,
                        &idempotency_path,
                        &marker,
                    )
                    .await?;
                    return Box::pin(self.inspect_low_level_pass(
                        expected,
                        root_mutations,
                        orchestration_events,
                        LowLevelReconciliationMode::Forbidden,
                    ))
                    .await;
                }
                LowLevelReconciliationMode::Forbidden => {
                    return Err(ApiError::internal(
                        "visible low-level evidence did not converge on one exact transaction record",
                    ));
                }
                LowLevelReconciliationMode::ObserveOnly => {}
            }
        }
        let record = if needs_reconciliation {
            visible_candidate.cloned()
        } else {
            stored
        };
        let Some(record) = record else {
            return Ok(LowLevelInspection {
                evidence: LowLevelEvidence {
                    tx_id: Some(marker.tx_id),
                    status: None,
                    receipt_path: None,
                    uncertain: true,
                    repair_pending: false,
                    durable_append_present: false,
                },
                visible_record: None,
            });
        };
        if record.status != ControlPlaneTxStatus::Visible
            && (record.visible_at.is_some() || record.result.is_some())
        {
            return Err(ApiError::internal(
                "non-visible low-level transaction contains visibility evidence",
            ));
        }
        let receipt_path = if record.status == ControlPlaneTxStatus::Visible {
            if record.visible_at.is_none() {
                return Err(ApiError::internal(
                    "visible low-level transaction is missing visible_at",
                ));
            }
            if marker.tx_record.is_some() && marker.visible_at != record.visible_at {
                return Err(ApiError::internal(
                    "visible low-level marker timestamp does not match its transaction",
                ));
            }
            receipt_path_from_result(
                expected.domain,
                &record.tx_id,
                record.visible_at,
                record.result.as_ref(),
            )?
        } else {
            None
        };
        let receipt_path = match receipt_path {
            Some(path) => {
                self.existing_receipt_path_for_record(path, expected.domain, record.result.as_ref())
                    .await?
            }
            None => None,
        };
        let visible_record =
            (record.status == ControlPlaneTxStatus::Visible).then(|| record.clone());
        Ok(LowLevelInspection {
            evidence: LowLevelEvidence {
                tx_id: Some(record.tx_id),
                status: Some(record.status),
                receipt_path,
                uncertain: record.repair_pending || record.status == ControlPlaneTxStatus::Prepared,
                repair_pending: record.repair_pending,
                durable_append_present: record.durable_append.is_some(),
            },
            visible_record,
        })
    }

    async fn validate_typed_visible_candidate(
        &self,
        expected: &LowLevelExpectation,
        root_mutations: Option<&[StagedRootMutation]>,
        orchestration_events: Option<&[OrchestrationEvent]>,
        record: &ControlPlaneTxRecord<serde_json::Value>,
        reconcile_root_children: bool,
    ) -> Result<(), ApiError> {
        match expected.domain {
            ControlPlaneTxDomain::Catalog => {
                let record =
                    decode_typed_transaction_record::<CatalogTxReceipt>(record, "catalog")?;
                self.validate_catalog_authority_record(&record).await?;
            }
            ControlPlaneTxDomain::Orchestration => {
                let record = decode_typed_transaction_record::<OrchestrationTxReceipt>(
                    record,
                    "orchestration",
                )?;
                let events = orchestration_events.ok_or_else(|| {
                    ApiError::internal(
                        "visible orchestration transaction is missing its reviewed staged events",
                    )
                })?;
                self.validate_orchestration_authority_record(&record, events)
                    .await?;
            }
            ControlPlaneTxDomain::Root => {
                let mutations = root_mutations.ok_or_else(|| {
                    ApiError::internal(
                        "visible root transaction is missing its frozen staged context",
                    )
                })?;
                let record = decode_typed_transaction_record::<RootTxReceipt>(record, "root")?;
                self.validate_visible_root_authority_record(
                    expected,
                    mutations,
                    &record,
                    reconcile_root_children,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn reconcile_visible_low_level(
        &self,
        expected: &LowLevelExpectation,
        root_mutations: Option<&[StagedRootMutation]>,
        orchestration_events: Option<&[OrchestrationEvent]>,
        idempotency_path: &str,
        marker: &ControlPlaneIdempotencyRecord,
    ) -> Result<(), ApiError> {
        match expected.domain {
            ControlPlaneTxDomain::Catalog => {
                let winner = self
                    .transaction_service
                    .resolve_existing_visible_exact_record::<CatalogTxReceipt>(
                        expected.domain,
                        idempotency_path,
                        marker,
                    )
                    .await?;
                self.validate_catalog_authority_record(&winner).await?;
                self.transaction_service
                    .persist_idempotency_from_exact_record(
                        expected.domain,
                        idempotency_path,
                        &winner,
                    )
                    .await?;
            }
            ControlPlaneTxDomain::Orchestration => {
                let winner = self
                    .transaction_service
                    .resolve_existing_visible_exact_record::<OrchestrationTxReceipt>(
                        expected.domain,
                        idempotency_path,
                        marker,
                    )
                    .await?;
                let events = orchestration_events.ok_or_else(|| {
                    ApiError::internal(
                        "visible orchestration transaction is missing its reviewed staged events",
                    )
                })?;
                self.validate_orchestration_authority_record(&winner, events)
                    .await?;
                self.transaction_service
                    .persist_idempotency_from_exact_record(
                        expected.domain,
                        idempotency_path,
                        &winner,
                    )
                    .await?;
            }
            ControlPlaneTxDomain::Root => {
                let winner = self
                    .transaction_service
                    .resolve_existing_visible_exact_record::<RootTxReceipt>(
                        expected.domain,
                        idempotency_path,
                        marker,
                    )
                    .await?;
                let mutations = root_mutations.ok_or_else(|| {
                    ApiError::internal(
                        "visible root transaction is missing its frozen staged context",
                    )
                })?;
                self.validate_visible_root_authority_record(expected, mutations, &winner, false)
                    .await?;
                self.validate_visible_root_authority_record(expected, mutations, &winner, true)
                    .await?;
                self.ensure_root_audit_receipt(&winner).await?;
                self.transaction_service
                    .persist_idempotency_from_exact_record(
                        expected.domain,
                        idempotency_path,
                        &winner,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn existing_receipt_path(&self, path: String) -> Result<Option<String>, ApiError> {
        if get_optional(&self.transaction_service.storage, &path)
            .await?
            .is_some()
        {
            Ok(Some(path))
        } else {
            Ok(None)
        }
    }

    async fn existing_receipt_path_for_record(
        &self,
        path: String,
        domain: ControlPlaneTxDomain,
        expected: Option<&serde_json::Value>,
    ) -> Result<Option<String>, ApiError> {
        let Some(bytes) = get_optional(&self.transaction_service.storage, &path).await? else {
            return Ok(None);
        };
        match domain {
            ControlPlaneTxDomain::Catalog => {
                let actual: CommitRecord = serde_json::from_slice(bytes.as_ref())
                    .map_err(|_| ApiError::internal("catalog audit record is corrupt"))?;
                let expected: CatalogTxReceipt = serde_json::from_value(
                    expected
                        .cloned()
                        .ok_or_else(|| ApiError::internal("catalog result is missing"))?,
                )
                .map_err(|_| ApiError::internal("catalog result receipt is corrupt"))?;
                if actual.commit_id != expected.commit_id {
                    return Err(ApiError::internal(
                        "catalog audit record does not match transaction result",
                    ));
                }
            }
            ControlPlaneTxDomain::Orchestration => {
                let actual: OrchestrationTxReceipt = serde_json::from_slice(bytes.as_ref())
                    .map_err(|_| ApiError::internal("orchestration audit receipt is corrupt"))?;
                let expected: OrchestrationTxReceipt = serde_json::from_value(
                    expected
                        .cloned()
                        .ok_or_else(|| ApiError::internal("orchestration result is missing"))?,
                )
                .map_err(|_| ApiError::internal("orchestration result receipt is corrupt"))?;
                if actual != expected {
                    return Err(ApiError::internal(
                        "orchestration audit receipt does not match transaction result",
                    ));
                }
            }
            ControlPlaneTxDomain::Root => {
                let actual: RootTxReceipt = serde_json::from_slice(bytes.as_ref())
                    .map_err(|_| ApiError::internal("root audit receipt is corrupt"))?;
                let expected: RootTxReceipt = serde_json::from_value(
                    expected
                        .cloned()
                        .ok_or_else(|| ApiError::internal("root result is missing"))?,
                )
                .map_err(|_| ApiError::internal("root result receipt is corrupt"))?;
                if actual != expected {
                    return Err(ApiError::internal(
                        "root audit receipt does not match transaction result",
                    ));
                }
            }
        }
        Ok(Some(path))
    }
}

async fn get_optional(storage: &ScopedStorage, path: &str) -> Result<Option<Bytes>, ApiError> {
    match storage.get_raw(path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => Ok(None),
        Err(error) => Err(ApiError::from(error)),
    }
}

fn decode_typed_transaction_record<TResult>(
    record: &ControlPlaneTxRecord<serde_json::Value>,
    domain: &str,
) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
where
    TResult: DeserializeOwned,
{
    let encoded = serde_json::to_value(record)
        .map_err(|_| ApiError::internal(format!("visible {domain} authority is corrupt")))?;
    serde_json::from_value(encoded)
        .map_err(|_| ApiError::internal(format!("visible {domain} authority is corrupt")))
}

fn receipt_path_from_result(
    domain: ControlPlaneTxDomain,
    expected_tx_id: &str,
    expected_visible_at: Option<DateTime<Utc>>,
    result: Option<&serde_json::Value>,
) -> Result<Option<String>, ApiError> {
    let result = result.ok_or_else(|| {
        ApiError::internal("visible low-level transaction is missing its receipt result")
    })?;
    match domain {
        ControlPlaneTxDomain::Catalog => {
            let receipt: CatalogTxReceipt = serde_json::from_value(result.clone())
                .map_err(|_| ApiError::internal("visible catalog receipt is corrupt"))?;
            if receipt.tx_id != expected_tx_id || Some(receipt.visible_at) != expected_visible_at {
                return Err(ApiError::internal(
                    "visible catalog receipt does not match its transaction",
                ));
            }
            Ok(Some(CatalogPaths::commit(
                CatalogDomain::Catalog,
                &receipt.commit_id,
            )))
        }
        ControlPlaneTxDomain::Orchestration => {
            let receipt: OrchestrationTxReceipt = serde_json::from_value(result.clone())
                .map_err(|_| ApiError::internal("visible orchestration receipt is corrupt"))?;
            if receipt.tx_id != expected_tx_id || Some(receipt.visible_at) != expected_visible_at {
                return Err(ApiError::internal(
                    "visible orchestration receipt does not match its transaction",
                ));
            }
            Ok(Some(ControlPlaneTxPaths::orchestration_commit_receipt(
                &receipt.commit_id,
            )))
        }
        ControlPlaneTxDomain::Root => {
            let receipt: RootTxReceipt = serde_json::from_value(result.clone())
                .map_err(|_| ApiError::internal("visible root receipt is corrupt"))?;
            if receipt.tx_id != expected_tx_id || Some(receipt.visible_at) != expected_visible_at {
                return Err(ApiError::internal(
                    "visible root receipt does not match its transaction",
                ));
            }
            Ok(Some(ControlPlaneTxPaths::root_commit_receipt(
                &receipt.root_commit_id,
            )))
        }
    }
}

fn is_valid_cached_visible_successor(
    stored: &ControlPlaneTxRecord<serde_json::Value>,
    cached: &ControlPlaneTxRecord<serde_json::Value>,
) -> bool {
    matches!(
        stored.status,
        ControlPlaneTxStatus::Prepared | ControlPlaneTxStatus::Aborted
    ) && stored.visible_at.is_none()
        && stored.result.is_none()
        && cached.status == ControlPlaneTxStatus::Visible
        && cached.visible_at.is_some()
        && cached.result.is_some()
        && stored.tx_id == cached.tx_id
        && stored.kind == cached.kind
        && stored.request_id == cached.request_id
        && stored.idempotency_key == cached.idempotency_key
        && stored.request_hash == cached.request_hash
        && stored.lock_path == cached.lock_path
        && stored.prepared_at == cached.prepared_at
}

fn is_valid_cached_visible_repair_divergence(
    stored: &ControlPlaneTxRecord<serde_json::Value>,
    cached: &ControlPlaneTxRecord<serde_json::Value>,
) -> bool {
    if stored.status != ControlPlaneTxStatus::Visible
        || cached.status != ControlPlaneTxStatus::Visible
        || stored.repair_pending == cached.repair_pending
    {
        return false;
    }
    let mut normalized_stored = stored.clone();
    normalized_stored.repair_pending = false;
    let mut normalized_cached = cached.clone();
    normalized_cached.repair_pending = false;
    normalized_stored == normalized_cached
}

fn root_request_from_staged(
    mutations: &[StagedRootMutation],
) -> Result<CommitRootTransactionRequest, ApiError> {
    let mutations = mutations
        .iter()
        .map(|mutation| {
            let kind = match mutation {
                StagedRootMutation::Catalog { operation } => {
                    domain_mutation::Kind::Catalog(operation.to_proto()?)
                }
                StagedRootMutation::Orchestration { events } => {
                    let events = events
                        .iter()
                        .map(event_to_proto_envelope)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|_| {
                            ApiError::internal(
                                "persisted orchestration event cannot map to root contract",
                            )
                        })?;
                    domain_mutation::Kind::Orchestration(OrchestrationBatchSpec { events })
                }
            };
            Ok(DomainMutation { kind: Some(kind) })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(CommitRootTransactionRequest { mutations })
}

fn same_handle_identity(left: &ControlPlaneHandleRecord, right: &ControlPlaneHandleRecord) -> bool {
    left.record_type == right.record_type
        && left.version == right.version
        && left.handle_id == right.handle_id
        && left.scope == right.scope
        && left.created_at == right.created_at
        && left.expires_at == right.expires_at
        && left.review_token_verifier == right.review_token_verifier
}

fn same_participant_definition(
    left: &ControlPlaneHandleParticipant,
    right: &ControlPlaneHandleParticipant,
) -> bool {
    left.ordinal == right.ordinal
        && left.kind == right.kind
        && left.domain == right.domain
        && left.request_id == right.request_id
        && left.idempotency_key == right.idempotency_key
        && left.request_hash == right.request_hash
}

fn same_handle_definition(
    left: &ControlPlaneHandleRecord,
    right: &ControlPlaneHandleRecord,
) -> bool {
    same_handle_identity(left, right)
        && left.mutation_refs == right.mutation_refs
        && left.participants.len() == right.participants.len()
        && left
            .participants
            .iter()
            .zip(&right.participants)
            .all(|(left, right)| same_participant_definition(left, right))
}

fn remaining_participant_count(handle: &ControlPlaneHandleRecord) -> usize {
    handle
        .participants
        .iter()
        .filter(|participant| participant.low_level_status != Some(ControlPlaneTxStatus::Visible))
        .count()
}

fn ensure_revision_capacity(
    handle: &ControlPlaneHandleRecord,
    required: usize,
) -> Result<(), ApiError> {
    let required = u64::try_from(required)
        .map_err(|_| ApiError::conflict("handle revision reserve overflows"))?;
    handle
        .revision
        .checked_add(required)
        .ok_or_else(|| ApiError::conflict("insufficient handle revisions for safe recovery"))?;
    Ok(())
}
