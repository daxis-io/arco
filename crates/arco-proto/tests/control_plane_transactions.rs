//! Contract tests for the authoritative control-plane protobuf surface.

#![allow(
    clippy::cognitive_complexity,
    clippy::default_trait_access,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::too_many_lines
)]

use prost::Message;

use arco_proto::arco::catalog::v1::{
    Catalog, CatalogControlPlaneScope, CatalogDdlOperation, ColumnDefinition, CreateCatalogOp,
    CreateSchemaOp, DropTableOp, ExternalLocation, Function, GovernanceAttachment, Grant,
    GrantMutation, MetastoreMutation, ModelVersion, RegisterTableOp, RegisteredModel,
    RenameTableOp, Schema, StorageCredential, Table, TableFormat, UpdateTableOp, Volume,
    WorkspaceBinding, catalog_ddl_operation, metastore_mutation,
};
use arco_proto::arco::controlplane::v1::{
    ApplyCatalogDdlRequest, ApplyCatalogDdlResponse, CatalogTxReceipt, CatalogTxStatus,
    CommitOrchestrationBatchRequest, CommitOrchestrationBatchResponse,
    CommitRootTransactionRequest, CommitRootTransactionResponse, DomainCommit, DomainMutation,
    GetCatalogTransactionRequest, GetCatalogTransactionResponse,
    GetOrchestrationTransactionRequest, GetOrchestrationTransactionResponse,
    GetRootTransactionRequest, GetRootTransactionResponse, OrchestrationBatchSpec,
    OrchestrationTxReceipt, OrchestrationTxStatus, RootTxReceipt, RootTxStatus,
    ScopedMetastoreMutation, TransactionDomain, TransactionStatus, domain_mutation,
};
use arco_proto::arco::orchestration::v1::{
    BackfillTrigger, FileEntry, ManualTrigger, MaterializationTrigger, OrchestrationEventEnvelope,
    OutputVisibilityState, RunRequested, RunTriggered, ScheduleTrigger, SensorTrigger,
    TaskCallbackOutput, TaskError, TaskErrorCategory, TaskFinished, TaskOutcome, TaskOutput,
    TriggerInfo, WebhookTrigger, orchestration_event_envelope, trigger_info,
};
use arco_proto::{
    CatalogControlPlaneScopeContractError, ControlPlaneTransactionContractError,
    OrchestrationEventContractError,
};

#[test]
fn control_plane_transaction_messages_compile_and_roundtrip_basic_fields() {
    let request = ApplyCatalogDdlRequest {
        ddl: Some(CatalogDdlOperation {
            op: Some(catalog_ddl_operation::Op::CreateSchema(CreateSchemaOp {
                catalog: "default".to_string(),
                schema: "raw".to_string(),
                description: Some("raw landing schema".to_string()),
            })),
        }),
    };

    let orchestration = CommitOrchestrationBatchRequest {
        events: vec![sample_run_requested_event()],
    };

    let root = CommitRootTransactionRequest {
        mutations: vec![
            DomainMutation {
                kind: Some(domain_mutation::Kind::Catalog(CatalogDdlOperation {
                    op: Some(catalog_ddl_operation::Op::DropTable(DropTableOp {
                        catalog: "default".to_string(),
                        schema: "raw".to_string(),
                        table: "events".to_string(),
                    })),
                })),
            },
            DomainMutation {
                kind: Some(domain_mutation::Kind::Orchestration(
                    OrchestrationBatchSpec {
                        events: vec![sample_run_triggered_event()],
                    },
                )),
            },
        ],
    };

    let catalog_lookup = GetCatalogTransactionRequest {
        tx_id: "01JQTX".to_string(),
    };
    let orchestration_lookup = GetOrchestrationTransactionRequest {
        tx_id: "01JQORCHTX".to_string(),
    };
    let root_lookup = GetRootTransactionRequest {
        tx_id: "01JQROOTTX".to_string(),
    };

    let receipt = CatalogTxReceipt {
        tx_id: "01JQTX".to_string(),
        event_id: "01JQEVENT".to_string(),
        commit_id: "01JQCOMMIT".to_string(),
        manifest_id: "00000000000000000117".to_string(),
        snapshot_version: 17,
        pointer_version: "\"etag-123\"".to_string(),
        read_token: "catalog:00000000000000000117".to_string(),
        visible_at: None,
    };
    let catalog_response = ApplyCatalogDdlResponse {
        receipt: Some(receipt.clone()),
        repair_pending: true,
    };
    let catalog_status = GetCatalogTransactionResponse {
        status: Some(CatalogTxStatus {
            tx_id: "01JQTX".to_string(),
            status: TransactionStatus::Visible as i32,
            request_hash: "sha256:req".to_string(),
            lock_path: "locks/catalog.lock.json".to_string(),
            fencing_token: 42,
            prepared_at: None,
            visible_at: None,
            result: Some(receipt),
            repair_pending: true,
        }),
    };
    let orchestration_response = CommitOrchestrationBatchResponse {
        receipt: Some(OrchestrationTxReceipt {
            tx_id: "01JQORCHTX".to_string(),
            commit_id: "01JQORCHCOMMIT".to_string(),
            manifest_id: "00000000000000000493".to_string(),
            revision_ulid: "01JQREV".to_string(),
            delta_id: "01JQDELTA".to_string(),
            pointer_version: "\"etag-456\"".to_string(),
            events_processed: 1,
            read_token: "orchestration:00000000000000000493".to_string(),
            visible_at: None,
        }),
        repair_pending: false,
    };
    let orchestration_status = GetOrchestrationTransactionResponse {
        status: Some(OrchestrationTxStatus {
            tx_id: "01JQORCHTX".to_string(),
            status: TransactionStatus::Visible as i32,
            request_hash: "sha256:orch".to_string(),
            lock_path: "locks/orchestration.compaction.lock.json".to_string(),
            fencing_token: 7,
            prepared_at: None,
            visible_at: None,
            result: None,
            repair_pending: false,
        }),
    };
    let root_response = CommitRootTransactionResponse {
        receipt: Some(RootTxReceipt {
            tx_id: "01JQROOTTX".to_string(),
            root_commit_id: "01JQROOTCOMMIT".to_string(),
            super_manifest_path: "transactions/root/01JQROOTTX.manifest.json".to_string(),
            domain_commits: vec![DomainCommit {
                domain: TransactionDomain::Catalog as i32,
                tx_id: "01JQTX".to_string(),
                commit_id: "01JQCOMMIT".to_string(),
                manifest_id: "00000000000000000117".to_string(),
                manifest_path: "manifests/catalog/00000000000000000117.json".to_string(),
                read_token: "catalog:00000000000000000117".to_string(),
            }],
            read_token: "root:01JQROOTTX".to_string(),
            visible_at: None,
        }),
        repair_pending: true,
    };
    let root_status = GetRootTransactionResponse {
        status: Some(RootTxStatus {
            tx_id: "01JQROOTTX".to_string(),
            status: TransactionStatus::Visible as i32,
            request_hash: "sha256:root".to_string(),
            lock_path: "locks/root.lock.json".to_string(),
            fencing_token: 11,
            prepared_at: None,
            visible_at: None,
            super_manifest_path: "transactions/root/01JQROOTTX.manifest.json".to_string(),
            domains: Vec::new(),
            result: None,
            repair_pending: true,
        }),
    };

    assert!(request.ddl.is_some());
    assert_eq!(orchestration.events.len(), 1);
    assert_eq!(root.mutations.len(), 2);
    assert_eq!(catalog_lookup.tx_id, "01JQTX");
    assert_eq!(orchestration_lookup.tx_id, "01JQORCHTX");
    assert_eq!(root_lookup.tx_id, "01JQROOTTX");
    assert!(catalog_response.receipt.is_some());
    assert!(catalog_status.status.is_some());
    assert!(orchestration_response.receipt.is_some());
    assert!(orchestration_status.status.is_some());
    assert!(root_response.receipt.is_some());
    assert!(root_status.status.is_some());
}

#[test]
fn catalog_surface_uses_catalog_schema_table_json_field_names() {
    let catalog = Catalog {
        catalog: "default".to_string(),
        display_name: Some("Default".to_string()),
        description: Some("default catalog".to_string()),
        created_at: None,
        updated_at: None,
    };
    let schema = Schema {
        catalog: "default".to_string(),
        schema: "raw".to_string(),
        description: Some("raw landing schema".to_string()),
        created_at: None,
        updated_at: None,
    };
    let table = Table {
        catalog: "default".to_string(),
        schema: "raw".to_string(),
        table: "events".to_string(),
        description: Some("raw events".to_string()),
        location: Some("s3://bucket/raw/events".to_string()),
        format: TableFormat::Delta as i32,
        created_at: None,
        updated_at: None,
    };
    let catalog_json = serde_json::to_value(&catalog).expect("catalog should serialize");
    let schema_json = serde_json::to_value(&schema).expect("schema should serialize");
    let table_json = serde_json::to_value(&table).expect("table should serialize");

    assert!(catalog_json.get("catalog").is_some());
    assert!(catalog_json.get("id").is_none());
    assert!(catalog_json.get("displayName").is_some());
    assert!(schema_json.get("catalog").is_some());
    assert!(schema_json.get("schema").is_some());
    assert!(schema_json.get("catalogId").is_none());
    assert!(schema_json.get("id").is_none());
    assert!(table_json.get("catalog").is_some());
    assert!(table_json.get("schema").is_some());
    assert!(table_json.get("table").is_some());
    assert!(table_json.get("schemaId").is_none());
    assert!(table_json.get("id").is_none());
}

#[test]
fn catalog_table_format_enum_lives_with_catalog_contracts() {
    assert_eq!(TableFormat::Delta.as_str_name(), "TABLE_FORMAT_DELTA");
    assert_eq!(TableFormat::Iceberg.as_str_name(), "TABLE_FORMAT_ICEBERG");
    assert_eq!(TableFormat::Parquet.as_str_name(), "TABLE_FORMAT_PARQUET");
}

#[test]
fn catalog_ddl_surface_covers_authoritative_operations() {
    let create_catalog = CatalogDdlOperation {
        op: Some(catalog_ddl_operation::Op::CreateCatalog(CreateCatalogOp {
            catalog: "default".to_string(),
            description: None,
        })),
    };
    let create_schema = CatalogDdlOperation {
        op: Some(catalog_ddl_operation::Op::CreateSchema(CreateSchemaOp {
            catalog: "default".to_string(),
            schema: "raw".to_string(),
            description: None,
        })),
    };
    let register_table = CatalogDdlOperation {
        op: Some(catalog_ddl_operation::Op::RegisterTable(RegisterTableOp {
            catalog: "default".to_string(),
            schema: "raw".to_string(),
            table: "events".to_string(),
            description: Some("raw events".to_string()),
            location: Some("s3://bucket/raw/events".to_string()),
            format: TableFormat::Delta as i32,
            columns: vec![ColumnDefinition {
                name: "event_id".to_string(),
                data_type: "string".to_string(),
                is_nullable: false,
                ordinal: 0,
                description: None,
            }],
        })),
    };
    let update_table = CatalogDdlOperation {
        op: Some(catalog_ddl_operation::Op::UpdateTable(UpdateTableOp {
            catalog: "default".to_string(),
            schema: "raw".to_string(),
            table: "events".to_string(),
            description: Some("curated raw events".to_string()),
            location: Some("s3://bucket/curated/events".to_string()),
            format: Some(TableFormat::Iceberg as i32),
        })),
    };
    let drop_table = CatalogDdlOperation {
        op: Some(catalog_ddl_operation::Op::DropTable(DropTableOp {
            catalog: "default".to_string(),
            schema: "raw".to_string(),
            table: "events".to_string(),
        })),
    };
    let rename_table = CatalogDdlOperation {
        op: Some(catalog_ddl_operation::Op::RenameTable(RenameTableOp {
            catalog: "default".to_string(),
            schema: "raw".to_string(),
            table: "events".to_string(),
            new_table: "events_v2".to_string(),
        })),
    };

    assert!(create_catalog.op.is_some());
    assert!(create_schema.op.is_some());
    assert!(register_table.op.is_some());
    assert!(update_table.op.is_some());
    assert!(drop_table.op.is_some());
    assert!(rename_table.op.is_some());
}

#[test]
fn metastore_contract_exposes_stable_id_objects() {
    let grant = Grant {
        grant_id: "grant_01".to_string(),
        object_id: "table_01".to_string(),
        object_type: "TABLE".to_string(),
        principal: "user:alice@example.com".to_string(),
        privilege: "SELECT".to_string(),
        granted_by: "user:admin@example.com".to_string(),
        created_at: None,
        ..Default::default()
    };

    let storage_credential = StorageCredential {
        credential_id: "cred_01".to_string(),
        name: "lakehouse-prod".to_string(),
        cloud: "aws".to_string(),
        owner: "group:data-platform".to_string(),
        created_at: None,
        updated_at: None,
        ..Default::default()
    };

    let external_location = ExternalLocation {
        location_id: "loc_01".to_string(),
        name: "raw-prod".to_string(),
        url: "s3://bucket/raw".to_string(),
        credential_id: storage_credential.credential_id.clone(),
        owner: "group:data-platform".to_string(),
        created_at: None,
        updated_at: None,
        ..Default::default()
    };
    let binding = WorkspaceBinding {
        binding_id: "binding_01".to_string(),
        workspace_id: "workspace_01".to_string(),
        object_id: external_location.location_id.clone(),
        object_type: "EXTERNAL_LOCATION".to_string(),
        created_at: None,
        ..Default::default()
    };
    let attachment = GovernanceAttachment {
        attachment_id: "attach_01".to_string(),
        object_id: "table_01".to_string(),
        object_type: "TABLE".to_string(),
        attachment_type: "CLASSIFICATION".to_string(),
        value: "restricted".to_string(),
        created_by: "user:admin@example.com".to_string(),
        created_at: None,
        ..Default::default()
    };
    let volume = Volume {
        volume_id: "volume_01".to_string(),
        catalog: "default".to_string(),
        schema: "raw".to_string(),
        volume: "landing".to_string(),
        storage_location: "s3://bucket/volumes/landing".to_string(),
        owner: "group:data-platform".to_string(),
        created_at: None,
        updated_at: None,
        ..Default::default()
    };
    let function = Function {
        function_id: "function_01".to_string(),
        catalog: "default".to_string(),
        schema: "raw".to_string(),
        function: "normalize_email".to_string(),
        owner: "group:data-platform".to_string(),
        created_at: None,
        updated_at: None,
        ..Default::default()
    };
    let model = RegisteredModel {
        model_id: "model_01".to_string(),
        catalog: "default".to_string(),
        schema: "ml".to_string(),
        model: "churn".to_string(),
        owner: "group:ml-platform".to_string(),
        created_at: None,
        updated_at: None,
        ..Default::default()
    };
    let model_version = ModelVersion {
        model_version_id: "model_version_01".to_string(),
        model_id: model.model_id.clone(),
        version: "1".to_string(),
        storage_location: "s3://bucket/models/churn/1".to_string(),
        created_at: None,
        updated_at: None,
        ..Default::default()
    };
    let empty_mutation = MetastoreMutation { op: None };

    assert_eq!(grant.grant_id, "grant_01");
    assert_eq!(grant.object_id, "table_01");
    assert_eq!(storage_credential.credential_id, "cred_01");
    assert_eq!(
        external_location.credential_id,
        storage_credential.credential_id
    );
    assert_eq!(binding.object_id, external_location.location_id);
    assert_eq!(attachment.attachment_type, "CLASSIFICATION");
    assert_eq!(volume.volume_id, "volume_01");
    assert_eq!(function.function, "normalize_email");
    assert_eq!(model_version.model_id, model.model_id);
    assert!(empty_mutation.op.is_none());
}

#[test]
fn catalog_mutation_surface_uses_canonical_public_json_nouns() {
    let create_catalog = CreateCatalogOp::decode(encode_string_field(1, "default").as_slice())
        .expect("create_catalog should decode");
    let create_schema = CreateSchemaOp::decode(
        encode_message(&[(1, "default"), (2, "raw"), (3, "raw landing schema")]).as_slice(),
    )
    .expect("create_schema should decode");
    let register_table = RegisterTableOp::decode(
        encode_message(&[(1, "default"), (2, "raw"), (3, "events"), (4, "raw events")]).as_slice(),
    )
    .expect("register_table should decode");
    let update_table = UpdateTableOp::decode(
        encode_message(&[
            (1, "default"),
            (2, "raw"),
            (3, "events"),
            (4, "curated events"),
        ])
        .as_slice(),
    )
    .expect("update_table should decode");
    let drop_table = DropTableOp::decode(
        encode_message(&[(1, "default"), (2, "raw"), (3, "events")]).as_slice(),
    )
    .expect("drop_table should decode");
    let rename_table = RenameTableOp::decode(
        encode_message(&[(1, "default"), (2, "raw"), (3, "events"), (4, "events_v2")]).as_slice(),
    )
    .expect("rename_table should decode");

    let create_catalog_json =
        serde_json::to_value(&create_catalog).expect("create_catalog should serialize");
    let create_schema_json =
        serde_json::to_value(&create_schema).expect("create_schema should serialize");
    let register_table_json =
        serde_json::to_value(&register_table).expect("register_table should serialize");
    let update_table_json =
        serde_json::to_value(&update_table).expect("update_table should serialize");
    let drop_table_json = serde_json::to_value(&drop_table).expect("drop_table should serialize");
    let rename_table_json =
        serde_json::to_value(&rename_table).expect("rename_table should serialize");

    assert!(create_catalog_json.get("catalog").is_some());
    assert!(create_catalog_json.get("name").is_none());

    assert!(create_schema_json.get("catalog").is_some());
    assert!(create_schema_json.get("schema").is_some());
    assert!(create_schema_json.get("catalogName").is_none());
    assert!(create_schema_json.get("schemaName").is_none());

    assert!(register_table_json.get("catalog").is_some());
    assert!(register_table_json.get("schema").is_some());
    assert!(register_table_json.get("table").is_some());
    assert!(register_table_json.get("catalogName").is_none());
    assert!(register_table_json.get("schemaName").is_none());
    assert!(register_table_json.get("tableName").is_none());

    assert!(update_table_json.get("catalog").is_some());
    assert!(update_table_json.get("schema").is_some());
    assert!(update_table_json.get("table").is_some());
    assert!(update_table_json.get("catalogName").is_none());
    assert!(update_table_json.get("schemaName").is_none());
    assert!(update_table_json.get("tableName").is_none());

    assert!(drop_table_json.get("catalog").is_some());
    assert!(drop_table_json.get("schema").is_some());
    assert!(drop_table_json.get("table").is_some());
    assert!(drop_table_json.get("catalogName").is_none());
    assert!(drop_table_json.get("schemaName").is_none());
    assert!(drop_table_json.get("tableName").is_none());

    assert!(rename_table_json.get("catalog").is_some());
    assert!(rename_table_json.get("schema").is_some());
    assert!(rename_table_json.get("table").is_some());
    assert!(rename_table_json.get("newTable").is_some());
    assert!(rename_table_json.get("catalogName").is_none());
    assert!(rename_table_json.get("schemaName").is_none());
    assert!(rename_table_json.get("oldTableName").is_none());
    assert!(rename_table_json.get("newTableName").is_none());
}

#[test]
fn apply_catalog_ddl_rejects_missing_operation() {
    let request = ApplyCatalogDdlRequest {
        ddl: Some(CatalogDdlOperation { op: None }),
    };

    assert_eq!(
        request.validate_contract(),
        Err(ControlPlaneTransactionContractError::MissingCatalogDdlOp)
    );
}

#[test]
fn commit_orchestration_batch_rejects_empty_events() {
    let request = CommitOrchestrationBatchRequest { events: Vec::new() };

    assert_eq!(
        request.validate_contract(),
        Err(ControlPlaneTransactionContractError::EmptyOrchestrationEvents)
    );
}

#[test]
fn commit_orchestration_batch_rejects_missing_event_kind() {
    let mut event = sample_run_requested_event();
    event.event = None;
    let request = CommitOrchestrationBatchRequest {
        events: vec![event],
    };

    assert_eq!(
        request.validate_contract(),
        Err(
            ControlPlaneTransactionContractError::InvalidOrchestrationEvent(
                0,
                OrchestrationEventContractError::MissingEventKind,
            )
        )
    );
}

#[test]
fn commit_orchestration_batch_rejects_missing_event_timestamp() {
    let mut event = sample_run_requested_event();
    event.timestamp = None;
    let request = CommitOrchestrationBatchRequest {
        events: vec![event],
    };

    assert_eq!(
        request.validate_contract(),
        Err(
            ControlPlaneTransactionContractError::InvalidOrchestrationEvent(
                0,
                OrchestrationEventContractError::MissingTimestamp,
            )
        )
    );
}

#[test]
fn commit_orchestration_batch_rejects_empty_event_identifiers() {
    let mut event = sample_run_requested_event();
    event.event_id.clear();
    let request = CommitOrchestrationBatchRequest {
        events: vec![event],
    };

    assert_eq!(
        request.validate_contract(),
        Err(
            ControlPlaneTransactionContractError::InvalidOrchestrationEvent(
                0,
                OrchestrationEventContractError::EmptyEventId,
            )
        )
    );
}

#[test]
fn commit_orchestration_batch_rejects_run_requested_manual_trigger_without_request_id() {
    let mut event = sample_run_requested_event();
    event.event = Some(orchestration_event_envelope::Event::RunRequested(
        RunRequested {
            trigger: Some(TriggerInfo {
                trigger: Some(trigger_info::Trigger::Manual(ManualTrigger {
                    user_id: "user_01".to_string(),
                    request_id: None,
                })),
            }),
            ..sample_run_requested()
        },
    ));

    let request = CommitOrchestrationBatchRequest {
        events: vec![event],
    };

    assert_eq!(
        request.validate_contract(),
        Err(
            ControlPlaneTransactionContractError::InvalidOrchestrationEvent(
                0,
                OrchestrationEventContractError::MissingTriggerField(
                    "run_requested",
                    "manual.request_id",
                ),
            ),
        )
    );
}

#[test]
fn commit_orchestration_batch_rejects_run_requested_schedule_trigger_without_tick_id() {
    let mut event = sample_run_requested_event();
    event.event = Some(orchestration_event_envelope::Event::RunRequested(
        RunRequested {
            trigger: Some(TriggerInfo {
                trigger: Some(trigger_info::Trigger::Schedule(ScheduleTrigger {
                    schedule_id: "sched_01".to_string(),
                    tick_id: None,
                })),
            }),
            ..sample_run_requested()
        },
    ));

    let request = CommitOrchestrationBatchRequest {
        events: vec![event],
    };

    assert_eq!(
        request.validate_contract(),
        Err(
            ControlPlaneTransactionContractError::InvalidOrchestrationEvent(
                0,
                OrchestrationEventContractError::MissingTriggerField(
                    "run_requested",
                    "schedule.tick_id",
                ),
            ),
        )
    );
}

#[test]
fn commit_orchestration_batch_rejects_run_requested_sensor_trigger_without_eval_id() {
    let mut event = sample_run_requested_event();
    event.event = Some(orchestration_event_envelope::Event::RunRequested(
        RunRequested {
            trigger: Some(TriggerInfo {
                trigger: Some(trigger_info::Trigger::Sensor(SensorTrigger {
                    sensor_id: "sensor_01".to_string(),
                    cursor: Some("cursor-01".to_string()),
                    eval_id: None,
                })),
            }),
            ..sample_run_requested()
        },
    ));

    let request = CommitOrchestrationBatchRequest {
        events: vec![event],
    };

    assert_eq!(
        request.validate_contract(),
        Err(
            ControlPlaneTransactionContractError::InvalidOrchestrationEvent(
                0,
                OrchestrationEventContractError::MissingTriggerField(
                    "run_requested",
                    "sensor.eval_id",
                ),
            ),
        )
    );
}

#[test]
fn commit_orchestration_batch_rejects_run_requested_backfill_trigger_without_chunk_id() {
    let mut event = sample_run_requested_event();
    event.event = Some(orchestration_event_envelope::Event::RunRequested(
        RunRequested {
            trigger: Some(TriggerInfo {
                trigger: Some(trigger_info::Trigger::Backfill(BackfillTrigger {
                    backfill_id: "backfill_01".to_string(),
                    chunk_id: None,
                })),
            }),
            ..sample_run_requested()
        },
    ));

    let request = CommitOrchestrationBatchRequest {
        events: vec![event],
    };

    assert_eq!(
        request.validate_contract(),
        Err(
            ControlPlaneTransactionContractError::InvalidOrchestrationEvent(
                0,
                OrchestrationEventContractError::MissingTriggerField(
                    "run_requested",
                    "backfill.chunk_id",
                ),
            ),
        )
    );
}

#[test]
fn commit_orchestration_batch_rejects_run_requested_materialization_trigger() {
    let mut event = sample_run_requested_event();
    event.event = Some(orchestration_event_envelope::Event::RunRequested(
        RunRequested {
            trigger: Some(TriggerInfo {
                trigger: Some(trigger_info::Trigger::Materialization(
                    MaterializationTrigger {
                        upstream_materialization_id: "mat_01".to_string(),
                    },
                )),
            }),
            ..sample_run_requested()
        },
    ));

    let request = CommitOrchestrationBatchRequest {
        events: vec![event],
    };

    assert_eq!(
        request.validate_contract(),
        Err(
            ControlPlaneTransactionContractError::InvalidOrchestrationEvent(
                0,
                OrchestrationEventContractError::UnsupportedTriggerKind(
                    "run_requested",
                    "materialization",
                ),
            ),
        )
    );
}

#[test]
fn commit_orchestration_batch_rejects_run_requested_webhook_trigger() {
    let mut event = sample_run_requested_event();
    event.event = Some(orchestration_event_envelope::Event::RunRequested(
        RunRequested {
            trigger: Some(TriggerInfo {
                trigger: Some(trigger_info::Trigger::Webhook(WebhookTrigger {
                    webhook_id: "webhook_01".to_string(),
                })),
            }),
            ..sample_run_requested()
        },
    ));

    let request = CommitOrchestrationBatchRequest {
        events: vec![event],
    };

    assert_eq!(
        request.validate_contract(),
        Err(
            ControlPlaneTransactionContractError::InvalidOrchestrationEvent(
                0,
                OrchestrationEventContractError::UnsupportedTriggerKind("run_requested", "webhook",),
            ),
        )
    );
}

#[test]
fn commit_orchestration_batch_rejects_run_triggered_backfill_trigger() {
    let mut event = sample_run_triggered_event();
    event.event = Some(orchestration_event_envelope::Event::RunTriggered(
        RunTriggered {
            trigger: Some(TriggerInfo {
                trigger: Some(trigger_info::Trigger::Backfill(BackfillTrigger {
                    backfill_id: "backfill_01".to_string(),
                    chunk_id: Some("chunk_01".to_string()),
                })),
            }),
            ..sample_run_triggered()
        },
    ));

    let request = CommitOrchestrationBatchRequest {
        events: vec![event],
    };

    assert_eq!(
        request.validate_contract(),
        Err(
            ControlPlaneTransactionContractError::InvalidOrchestrationEvent(
                0,
                OrchestrationEventContractError::UnsupportedTriggerKind(
                    "run_triggered",
                    "backfill",
                ),
            ),
        )
    );
}

#[test]
fn commit_root_transaction_rejects_empty_mutations() {
    let request = CommitRootTransactionRequest {
        mutations: Vec::new(),
    };

    assert_eq!(
        request.validate_contract(),
        Err(ControlPlaneTransactionContractError::EmptyRootMutations)
    );
}

#[test]
fn commit_root_transaction_rejects_missing_mutation_kind() {
    let request = CommitRootTransactionRequest {
        mutations: vec![DomainMutation { kind: None }],
    };

    assert_eq!(
        request.validate_contract(),
        Err(ControlPlaneTransactionContractError::MissingRootMutationKind(0))
    );
}

#[test]
fn commit_root_transaction_rejects_missing_catalog_operation() {
    let request = CommitRootTransactionRequest {
        mutations: vec![DomainMutation {
            kind: Some(domain_mutation::Kind::Catalog(CatalogDdlOperation {
                op: None,
            })),
        }],
    };

    assert_eq!(
        request.validate_contract(),
        Err(ControlPlaneTransactionContractError::MissingRootCatalogDdlOp(0,))
    );
}

#[test]
fn metastore_root_transaction_accepts_mutations() {
    let request = CommitRootTransactionRequest {
        mutations: vec![DomainMutation {
            kind: Some(domain_mutation::Kind::Metastore(MetastoreMutation {
                op: Some(metastore_mutation::Op::StorageCredential(
                    StorageCredential {
                        credential_id: "cred_01".to_string(),
                        name: "lakehouse-prod".to_string(),
                        cloud: "aws".to_string(),
                        owner: "group:data-platform".to_string(),
                        created_at: None,
                        updated_at: None,
                        ..Default::default()
                    },
                )),
            })),
        }],
    };

    assert_eq!(request.validate_contract(), Ok(()));
}

#[test]
fn scoped_metastore_root_transaction_carries_scope_and_stable_object_id() {
    let request = CommitRootTransactionRequest {
        mutations: vec![DomainMutation {
            kind: Some(domain_mutation::Kind::ScopedMetastore(
                ScopedMetastoreMutation {
                    scope: Some(CatalogControlPlaneScope {
                        tenant_id: "tenant_01".to_string(),
                        workspace_id: "workspace_01".to_string(),
                        metastore_id: "metastore_01".to_string(),
                        request_id: "request_01".to_string(),
                    }),
                    mutation: Some(MetastoreMutation {
                        op: Some(metastore_mutation::Op::StorageCredential(
                            StorageCredential {
                                credential_id: "cred_01".to_string(),
                                name: "lakehouse-prod".to_string(),
                                cloud: "aws".to_string(),
                                owner: "group:data-platform".to_string(),
                                created_at: None,
                                updated_at: None,
                                ..Default::default()
                            },
                        )),
                    }),
                },
            )),
        }],
    };

    request
        .validate_contract()
        .expect("scoped metastore mutation should be valid");

    let decoded = CommitRootTransactionRequest::decode(request.encode_to_vec().as_slice())
        .expect("scoped metastore request should roundtrip");
    let Some(domain_mutation::Kind::ScopedMetastore(scoped)) = decoded
        .mutations
        .first()
        .and_then(|mutation| mutation.kind.as_ref())
    else {
        panic!("expected scoped metastore mutation");
    };
    let scope = scoped.scope.as_ref().expect("scope should roundtrip");
    assert_eq!(scope.tenant_id, "tenant_01");
    assert_eq!(scope.workspace_id, "workspace_01");
    assert_eq!(scope.metastore_id, "metastore_01");
    assert_eq!(scope.request_id, "request_01");

    let Some(MetastoreMutation {
        op: Some(metastore_mutation::Op::StorageCredential(storage_credential)),
    }) = scoped.mutation.as_ref()
    else {
        panic!("expected storage credential mutation");
    };
    assert_eq!(storage_credential.credential_id, "cred_01");
}

#[test]
fn scoped_metastore_root_transaction_uses_workspace_alias_when_metastore_is_omitted() {
    let scope = CatalogControlPlaneScope {
        tenant_id: "tenant_01".to_string(),
        workspace_id: "workspace_01".to_string(),
        metastore_id: String::new(),
        request_id: "request_01".to_string(),
    };

    assert!(scope.metastore_id.is_empty());
    assert_eq!(scope.effective_metastore_id(), "workspace_01");
}

#[test]
fn scoped_metastore_root_transaction_rejects_missing_scope() {
    let request = CommitRootTransactionRequest {
        mutations: vec![DomainMutation {
            kind: Some(domain_mutation::Kind::ScopedMetastore(
                ScopedMetastoreMutation {
                    scope: None,
                    mutation: Some(MetastoreMutation {
                        op: Some(metastore_mutation::Op::StorageCredential(
                            StorageCredential {
                                credential_id: "cred_01".to_string(),
                                name: "lakehouse-prod".to_string(),
                                cloud: "aws".to_string(),
                                owner: "group:data-platform".to_string(),
                                created_at: None,
                                updated_at: None,
                                ..Default::default()
                            },
                        )),
                    }),
                },
            )),
        }],
    };

    assert_eq!(
        request.validate_contract(),
        Err(ControlPlaneTransactionContractError::MissingRootMetastoreScope(0))
    );
}

#[test]
fn scoped_metastore_root_transaction_rejects_empty_required_scope_fields() {
    for (scope, expected_error) in [
        (
            CatalogControlPlaneScope {
                tenant_id: String::new(),
                workspace_id: "workspace_01".to_string(),
                metastore_id: "metastore_01".to_string(),
                request_id: "request_01".to_string(),
            },
            CatalogControlPlaneScopeContractError::EmptyTenantId,
        ),
        (
            CatalogControlPlaneScope {
                tenant_id: "tenant_01".to_string(),
                workspace_id: String::new(),
                metastore_id: "metastore_01".to_string(),
                request_id: "request_01".to_string(),
            },
            CatalogControlPlaneScopeContractError::EmptyWorkspaceId,
        ),
        (
            CatalogControlPlaneScope {
                tenant_id: "tenant_01".to_string(),
                workspace_id: "workspace_01".to_string(),
                metastore_id: "metastore_01".to_string(),
                request_id: String::new(),
            },
            CatalogControlPlaneScopeContractError::EmptyRequestId,
        ),
        (
            CatalogControlPlaneScope {
                tenant_id: "tenant_01".to_string(),
                workspace_id: String::new(),
                metastore_id: String::new(),
                request_id: "request_01".to_string(),
            },
            CatalogControlPlaneScopeContractError::EmptyEffectiveMetastoreId,
        ),
    ] {
        let request = CommitRootTransactionRequest {
            mutations: vec![DomainMutation {
                kind: Some(domain_mutation::Kind::ScopedMetastore(
                    ScopedMetastoreMutation {
                        scope: Some(scope),
                        mutation: Some(MetastoreMutation {
                            op: Some(metastore_mutation::Op::StorageCredential(
                                StorageCredential {
                                    credential_id: "cred_01".to_string(),
                                    name: "lakehouse-prod".to_string(),
                                    cloud: "aws".to_string(),
                                    owner: "group:data-platform".to_string(),
                                    created_at: None,
                                    updated_at: None,
                                    ..Default::default()
                                },
                            )),
                        }),
                    },
                )),
            }],
        };

        assert_eq!(
            request.validate_contract(),
            Err(
                ControlPlaneTransactionContractError::InvalidRootMetastoreScope(0, expected_error,)
            )
        );
    }
}

#[test]
fn metastore_root_transaction_rejects_empty_mutation() {
    let request = CommitRootTransactionRequest {
        mutations: vec![DomainMutation {
            kind: Some(domain_mutation::Kind::Metastore(MetastoreMutation {
                op: None,
            })),
        }],
    };

    assert_eq!(
        request.validate_contract(),
        Err(ControlPlaneTransactionContractError::MissingRootMetastoreMutationOp(0))
    );
}

#[test]
fn metastore_root_transaction_rejects_empty_nested_grant_mutation() {
    let request = CommitRootTransactionRequest {
        mutations: vec![DomainMutation {
            kind: Some(domain_mutation::Kind::Metastore(MetastoreMutation {
                op: Some(metastore_mutation::Op::Grant(GrantMutation { op: None })),
            })),
        }],
    };

    assert_eq!(
        request.validate_contract(),
        Err(ControlPlaneTransactionContractError::MissingRootMetastoreMutationOp(0))
    );
}

#[test]
fn commit_root_transaction_rejects_empty_orchestration_events() {
    let request = CommitRootTransactionRequest {
        mutations: vec![DomainMutation {
            kind: Some(domain_mutation::Kind::Orchestration(
                OrchestrationBatchSpec { events: Vec::new() },
            )),
        }],
    };

    assert_eq!(
        request.validate_contract(),
        Err(ControlPlaneTransactionContractError::EmptyRootOrchestrationEvents(0))
    );
}

#[test]
fn commit_root_transaction_rejects_invalid_nested_orchestration_events() {
    let mut event = sample_run_triggered_event();
    event.idempotency_key.clear();
    let request = CommitRootTransactionRequest {
        mutations: vec![DomainMutation {
            kind: Some(domain_mutation::Kind::Orchestration(
                OrchestrationBatchSpec {
                    events: vec![event],
                },
            )),
        }],
    };

    assert_eq!(
        request.validate_contract(),
        Err(
            ControlPlaneTransactionContractError::InvalidRootOrchestrationEvent(
                0,
                0,
                OrchestrationEventContractError::EmptyIdempotencyKey,
            ),
        )
    );
}

#[test]
fn typed_orchestration_envelope_roundtrips_with_variant_payloads() {
    let envelope = sample_task_finished_event();
    let encoded = envelope.encode_to_vec();
    let decoded =
        OrchestrationEventEnvelope::decode(encoded.as_slice()).expect("decode typed envelope");

    assert!(matches!(
        decoded.event,
        Some(orchestration_event_envelope::Event::TaskFinished(_))
    ));
}

#[test]
fn task_output_files_are_owned_by_orchestration_contract() {
    let output = TaskOutput {
        materialization_id: Some("mat_01".to_string()),
        files: vec![FileEntry {
            path: "s3://bucket/output/part-000.parquet".to_string(),
            size_bytes: 128,
            row_count: Some(5),
            content_hash: Some("sha256:abc123".to_string()),
            format: Some("parquet".to_string()),
        }],
        row_count: Some(5),
        byte_size: Some(128),
        visibility_state: OutputVisibilityState::Visible as i32,
        published_at: Some(prost_types::Timestamp {
            seconds: 1_776_000_003,
            nanos: 0,
        }),
        publish_error: None,
    };

    output
        .validate_contract()
        .expect("visible published output should validate");

    let encoded = output.encode_to_vec();
    let decoded = TaskOutput::decode(encoded.as_slice()).expect("decode published output");

    assert_eq!(decoded.files.len(), 1);
    assert_eq!(decoded.files[0].path, "s3://bucket/output/part-000.parquet");
    assert_eq!(decoded.files[0].size_bytes, 128);
    assert_eq!(decoded.files[0].row_count, Some(5));
    assert_eq!(
        decoded.files[0].content_hash.as_deref(),
        Some("sha256:abc123")
    );
    assert_eq!(decoded.files[0].format.as_deref(), Some("parquet"));
}

#[test]
fn root_super_manifest_path_preserves_legacy_wire_tags() {
    let path = "transactions/root/01JQROOTTX.manifest.json";

    let receipt_from_old_field = RootTxReceipt::decode(encode_string_field(4, path).as_slice())
        .expect("decode old receipt field");
    assert_eq!(receipt_from_old_field.super_manifest_path, path);

    let status_from_old_field = RootTxStatus::decode(encode_string_field(11, path).as_slice())
        .expect("decode old status field");
    assert_eq!(status_from_old_field.super_manifest_path, path);

    let receipt = RootTxReceipt {
        super_manifest_path: path.to_string(),
        ..RootTxReceipt::default()
    };
    assert_eq!(receipt.encode_to_vec(), encode_string_field(4, path));

    let status = RootTxStatus {
        super_manifest_path: path.to_string(),
        ..RootTxStatus::default()
    };
    assert_eq!(status.encode_to_vec(), encode_string_field(11, path));
}

fn sample_run_requested_event() -> OrchestrationEventEnvelope {
    OrchestrationEventEnvelope {
        event_id: "01JEVT".to_string(),
        event_version: 1,
        timestamp: Some(prost_types::Timestamp {
            seconds: 1_776_000_000,
            nanos: 0,
        }),
        source: "arco-flow/acme/prod".to_string(),
        idempotency_key: "run:req-01".to_string(),
        correlation_id: Some("run-01".to_string()),
        causation_id: None,
        event: Some(orchestration_event_envelope::Event::RunRequested(
            sample_run_requested(),
        )),
    }
}

fn sample_run_triggered_event() -> OrchestrationEventEnvelope {
    OrchestrationEventEnvelope {
        event_id: "01JRUN".to_string(),
        event_version: 1,
        timestamp: Some(prost_types::Timestamp {
            seconds: 1_776_000_001,
            nanos: 0,
        }),
        source: "arco-flow/acme/prod".to_string(),
        idempotency_key: "run:manual:req-01".to_string(),
        correlation_id: Some("run-01".to_string()),
        causation_id: None,
        event: Some(orchestration_event_envelope::Event::RunTriggered(
            sample_run_triggered(),
        )),
    }
}

fn sample_run_requested() -> RunRequested {
    RunRequested {
        run_key: "sched:daily:2026-04-09".to_string(),
        request_fingerprint: "sha256:req".to_string(),
        asset_selection: vec!["default.raw.events".to_string()],
        partition_selection: vec!["date=d:2026-04-09".to_string()],
        trigger: Some(TriggerInfo {
            trigger: Some(trigger_info::Trigger::Schedule(ScheduleTrigger {
                schedule_id: "sched_01".to_string(),
                tick_id: Some("tick_01".to_string()),
            })),
        }),
        labels: Default::default(),
    }
}

fn sample_run_triggered() -> RunTriggered {
    RunTriggered {
        run_id: "run-01".to_string(),
        plan_id: "plan-01".to_string(),
        trigger: Some(TriggerInfo {
            trigger: Some(trigger_info::Trigger::Manual(ManualTrigger {
                user_id: "user_01".to_string(),
                request_id: None,
            })),
        }),
        root_assets: vec!["default.raw.events".to_string()],
        run_key: Some("manual:req-01".to_string()),
        labels: Default::default(),
        code_version: Some("git:abc123".to_string()),
    }
}

fn sample_task_finished_event() -> OrchestrationEventEnvelope {
    OrchestrationEventEnvelope {
        event_id: "01JTFIN".to_string(),
        event_version: 1,
        timestamp: Some(prost_types::Timestamp {
            seconds: 1_776_000_002,
            nanos: 0,
        }),
        source: "arco-flow/acme/prod".to_string(),
        idempotency_key: "finished:run-01:extract:1".to_string(),
        correlation_id: Some("run-01".to_string()),
        causation_id: None,
        event: Some(orchestration_event_envelope::Event::TaskFinished(
            TaskFinished {
                run_id: "run-01".to_string(),
                task_key: "extract".to_string(),
                attempt: 1,
                attempt_id: "attempt-01".to_string(),
                worker_id: "worker-01".to_string(),
                outcome: TaskOutcome::Succeeded as i32,
                callback_output: Some(TaskCallbackOutput {
                    materialization_id: Some("mat_01".to_string()),
                    row_count: Some(5),
                    byte_size: Some(128),
                    output_path: Some("s3://bucket/output/part-000.parquet".to_string()),
                    delta_table: Some("default.raw.events".to_string()),
                    delta_version: Some(7),
                    delta_partition: Some("date=d:2026-04-09".to_string()),
                }),
                error: Some(TaskError {
                    category: TaskErrorCategory::UserCode as i32,
                    message: "boom".to_string(),
                    detail: None,
                    retryable: Some(false),
                }),
                metrics: None,
                cancelled_during_phase: None,
                asset_key: Some("default.raw.events".to_string()),
                partition_key: Some(arco_proto::arco::common::v1::PartitionKey {
                    dimensions: vec![arco_proto::arco::common::v1::PartitionDimension {
                        name: "date".to_string(),
                        value: Some(arco_proto::arco::common::v1::ScalarValue {
                            value: Some(
                                arco_proto::arco::common::v1::scalar_value::Value::DateValue(
                                    "2026-04-09".to_string(),
                                ),
                            ),
                        }),
                    }],
                }),
                code_version: Some("git:abc123".to_string()),
            },
        )),
    }
}

fn encode_string_field(field_number: u32, value: &str) -> Vec<u8> {
    let mut bytes = encode_varint(u64::from((field_number << 3) | 2));
    bytes.extend(encode_varint(
        u64::try_from(value.len()).expect("string length fits in u64"),
    ));
    bytes.extend_from_slice(value.as_bytes());
    bytes
}

fn encode_message(fields: &[(u32, &str)]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for (field_number, value) in fields {
        bytes.extend(encode_string_field(*field_number, value));
    }
    bytes
}

fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let mut byte = u8::try_from(value & 0x7f).expect("varint byte");
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        bytes.push(byte);
        if value == 0 {
            return bytes;
        }
    }
}
