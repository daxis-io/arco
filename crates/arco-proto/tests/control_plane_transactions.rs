//! Contract tests for generated control-plane transaction protobuf messages.

#![allow(clippy::expect_used)]

use prost::Message;

use arco_proto::{
    ApplyCatalogDdlRequest, CatalogDdlOperation as CatalogDdlEnvelope, OrchestrationBatchSpec,
};
use arco_proto::{
    ApplyCatalogDdlResponse, CatalogDdlOperation, CatalogTxReceipt, CatalogTxStatus,
    CommitOrchestrationBatchRequest, CommitOrchestrationBatchResponse,
    CommitRootTransactionRequest, CommitRootTransactionResponse,
    ControlPlaneTransactionContractError, DomainCommit, DomainMutation,
    GetCatalogTransactionRequest, GetCatalogTransactionResponse,
    GetOrchestrationTransactionRequest, GetOrchestrationTransactionResponse,
    GetRootTransactionRequest, GetRootTransactionResponse, OrchestrationEventEnvelope,
    OrchestrationTxReceipt, OrchestrationTxStatus, RequestHeader, RootTxReceipt, RootTxStatus,
    TenantId, TransactionDomain, TransactionStatus, WorkspaceId, domain_mutation,
};

#[test]
fn control_plane_transaction_messages_compile_and_roundtrip_basic_fields() {
    let header = RequestHeader {
        tenant_id: Some(TenantId {
            value: "acme".to_string(),
        }),
        workspace_id: Some(WorkspaceId {
            value: "prod".to_string(),
        }),
        trace_parent: "00-abc-def-01".to_string(),
        idempotency_key: "idem-01".to_string(),
        request_time: None,
        request_id: "req-01".to_string(),
    };

    let request = ApplyCatalogDdlRequest {
        header: Some(header.clone()),
        ddl: Some(CatalogDdlOperation {
            op: Some(arco_proto::catalog_ddl_operation::Op::DropTable(
                arco_proto::DropTableOp {
                    namespace: "raw".to_string(),
                    name: "events".to_string(),
                },
            )),
        }),
        require_visible: Some(true),
    };

    let orchestration = CommitOrchestrationBatchRequest {
        header: Some(header.clone()),
        events: vec![OrchestrationEventEnvelope {
            event_id: "01JEVT".to_string(),
            event_type: "RunRequested".to_string(),
            event_version: 1,
            timestamp: None,
            source: "arco-flow/acme/prod".to_string(),
            idempotency_key: "run:req-01".to_string(),
            correlation_id: Some("run-01".to_string()),
            causation_id: None,
            payload_json: br#"{"runId":"run-01"}"#.to_vec(),
        }],
        require_visible: Some(true),
        allow_inline_merge: Some(false),
    };

    let root = CommitRootTransactionRequest {
        header: Some(header.clone()),
        mutations: vec![DomainMutation {
            kind: Some(domain_mutation::Kind::Catalog(CatalogDdlOperation {
                op: Some(arco_proto::catalog_ddl_operation::Op::DropTable(
                    arco_proto::DropTableOp {
                        namespace: "raw".to_string(),
                        name: "events".to_string(),
                    },
                )),
            })),
        }],
    };

    let catalog_lookup = GetCatalogTransactionRequest {
        header: Some(header.clone()),
        tx_id: "01JQTX".to_string(),
    };
    let orchestration_lookup = GetOrchestrationTransactionRequest {
        header: Some(header.clone()),
        tx_id: "01JQORCHTX".to_string(),
    };
    let root_lookup = GetRootTransactionRequest {
        header: Some(header.clone()),
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
            repair_pending: true,
            request_id: "req-01".to_string(),
            idempotency_key: "idem-01".to_string(),
            request_hash: "sha256:req".to_string(),
            lock_path: "locks/catalog.lock.json".to_string(),
            fencing_token: 42,
            prepared_at: None,
            visible_at: None,
            result: Some(receipt),
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
            repair_pending: false,
            request_id: "req-01".to_string(),
            idempotency_key: "idem-01".to_string(),
            request_hash: "sha256:orch".to_string(),
            lock_path: "locks/orchestration.compaction.lock.json".to_string(),
            fencing_token: 7,
            prepared_at: None,
            visible_at: None,
            result: None,
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
            repair_pending: true,
            request_id: "req-01".to_string(),
            idempotency_key: "idem-01".to_string(),
            request_hash: "sha256:root".to_string(),
            lock_path: "locks/root.lock.json".to_string(),
            fencing_token: 11,
            prepared_at: None,
            visible_at: None,
            super_manifest_path: "transactions/root/01JQROOTTX.manifest.json".to_string(),
            domains: Vec::new(),
            result: None,
        }),
    };

    assert!(request.header.is_some());
    assert!(request.ddl.is_some());
    assert_eq!(orchestration.events.len(), 1);
    assert_eq!(root.mutations.len(), 1);
    assert_eq!(catalog_lookup.tx_id, "01JQTX");
    assert_eq!(orchestration_lookup.tx_id, "01JQORCHTX");
    assert_eq!(root_lookup.tx_id, "01JQROOTTX");
    assert!(catalog_response.receipt.is_some());
    assert_eq!(catalog_response.repair_pending, true);
    assert!(catalog_status.status.is_some());
    assert!(orchestration_response.receipt.is_some());
    assert!(orchestration_status.status.is_some());
    assert!(root_response.receipt.is_some());
    assert_eq!(root_response.repair_pending, true);
    assert!(root_status.status.is_some());
}

#[test]
fn apply_catalog_ddl_rejects_missing_operation() {
    let request = ApplyCatalogDdlRequest {
        header: Some(sample_header()),
        ddl: Some(CatalogDdlEnvelope { op: None }),
        require_visible: Some(true),
    };

    assert_eq!(
        request.validate_contract(),
        Err(ControlPlaneTransactionContractError::MissingCatalogDdlOp)
    );
}

#[test]
fn commit_orchestration_batch_rejects_empty_events() {
    let request = CommitOrchestrationBatchRequest {
        header: Some(sample_header()),
        events: Vec::new(),
        require_visible: Some(true),
        allow_inline_merge: Some(false),
    };

    assert_eq!(
        request.validate_contract(),
        Err(ControlPlaneTransactionContractError::EmptyOrchestrationEvents)
    );
}

#[test]
fn commit_root_transaction_rejects_empty_mutations() {
    let request = CommitRootTransactionRequest {
        header: Some(sample_header()),
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
        header: Some(sample_header()),
        mutations: vec![DomainMutation { kind: None }],
    };

    assert_eq!(
        request.validate_contract(),
        Err(ControlPlaneTransactionContractError::MissingRootMutationKind(0))
    );
}

#[test]
fn commit_root_transaction_accepts_enum_backed_domain_usage() {
    let request = CommitRootTransactionRequest {
        header: Some(sample_header()),
        mutations: vec![DomainMutation {
            kind: Some(domain_mutation::Kind::Orchestration(
                OrchestrationBatchSpec {
                    events: vec![OrchestrationEventEnvelope {
                        event_id: "01JEVT".to_string(),
                        event_type: "RunRequested".to_string(),
                        event_version: 1,
                        timestamp: None,
                        source: "arco-flow/acme/prod".to_string(),
                        idempotency_key: "run:req-01".to_string(),
                        correlation_id: None,
                        causation_id: None,
                        payload_json: br#"{"runId":"run-01"}"#.to_vec(),
                    }],
                    allow_inline_merge: Some(false),
                },
            )),
        }],
    };

    assert_eq!(request.validate_contract(), Ok(()));
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

fn sample_header() -> RequestHeader {
    RequestHeader {
        tenant_id: Some(TenantId {
            value: "acme".to_string(),
        }),
        workspace_id: Some(WorkspaceId {
            value: "prod".to_string(),
        }),
        trace_parent: "00-abc-def-01".to_string(),
        idempotency_key: "idem-01".to_string(),
        request_time: None,
        request_id: "req-01".to_string(),
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
