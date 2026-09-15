//! Small independent catalog model checked against authenticated published rows.
#![cfg(feature = "test-utils")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arco_catalog::catalog_authority::{
    BoundedCatalogTestCommand as Command, CatalogProjectionNotifierV2,
};
use arco_catalog::state_store::ProjectionIntentV2;
use arco_catalog::{
    ArcoStateAdmin, ArcoStateReader, CatalogPatch, ColumnDefinition, ControlCatalogAuthority,
    ControlMvpStateStore, RegisterTableInSchemaRequest, ScanRequest, StateScope, WriteOptions,
};
use arco_core::{MemoryBackend, ScopedStorage};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

struct Notifier;
impl CatalogProjectionNotifierV2 for Notifier {
    fn notify(&self, _: &ProjectionIntentV2) -> arco_catalog::Result<()> {
        Ok(())
    }
}

// Encode the documented zero-escaped component format independently of the
// catalog's private key helpers. IDs and names here contain no zero bytes.
fn key(prefix: &[u8], parts: &[&str]) -> Vec<u8> {
    let mut key = prefix.to_vec();
    for part in parts {
        assert!(!part.as_bytes().contains(&0));
        key.extend_from_slice(part.as_bytes());
        key.extend_from_slice(&[0, 0]);
    }
    key
}

#[derive(Clone, Debug, PartialEq)]
enum ExpectedValue {
    Json(Value),
    Id(String),
}
impl ExpectedValue {
    fn assert_bytes(&self, bytes: &[u8]) {
        match self {
            Self::Json(expected) => {
                assert_eq!(&serde_json::from_slice::<Value>(bytes).unwrap(), expected);
            }
            Self::Id(expected) => assert_eq!(bytes, expected.as_bytes()),
        }
    }
}

#[tokio::test]
// One independent model follows the complete catalog/schema/table command sequence.
#[allow(clippy::too_many_lines)]
async fn actual_catalog_commands_match_independent_objects_indexes_envelopes_and_history_inputs() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "parity", "workspace").unwrap();
    let scope = StateScope::new("parity", "workspace", "catalog");
    let authority = ControlCatalogAuthority::new_synthetic_bounded(
        storage.clone(),
        scope.clone(),
        Arc::new(Notifier),
    )
    .unwrap();
    let store = ControlMvpStateStore::new_synthetic_bounded(storage, scope).unwrap();
    let catalog_id = "01900000-0001-7000-8000-000000000001";
    let schema_id = "01900000-0002-7000-8000-000000000002";
    let table_id = "01900000-0003-7000-8000-000000000003";
    let column_id = "01900000-0004-7000-8000-000000000004";
    let catalog_key = key(&[1, 1], &[catalog_id]);
    let schema_key = key(&[1, 2], &[schema_id]);
    let table_key = key(&[1, 3], &[table_id]);
    let mut column_key = key(&[1, 4], &[table_id]);
    column_key.extend_from_slice(&0_u32.to_be_bytes());
    column_key.extend_from_slice(&key(&[], &[column_id]));
    let commands = [
        Command::CreateCatalog {
            id: catalog_id.into(),
            name: "catalog".into(),
        },
        Command::CreateSchema {
            id: schema_id.into(),
            catalog: "catalog".into(),
            name: "schema".into(),
        },
        Command::RegisterTable {
            id: table_id.into(),
            column_id: column_id.into(),
            catalog: "catalog".into(),
            schema: "schema".into(),
            request: RegisterTableInSchemaRequest {
                name: "table".into(),
                description: None,
                location: Some("s3://parity/table".into()),
                format: Some("delta".into()),
                table_type: Some("EXTERNAL".into()),
                properties: None,
                columns: vec![ColumnDefinition {
                    name: "id".into(),
                    data_type: "BIGINT".into(),
                    is_nullable: false,
                    ordinal: 0,
                    description: None,
                }],
            },
        },
        Command::PatchCatalog {
            name: "catalog".into(),
            patch: CatalogPatch {
                description: Some(Some("patched".into())),
                ..CatalogPatch::default()
            },
        },
        Command::RenameTable {
            catalog: "catalog".into(),
            schema: "schema".into(),
            name: "table".into(),
            new_name: "renamed".into(),
        },
        Command::DropTable {
            catalog: "catalog".into(),
            schema: "schema".into(),
            name: "renamed".into(),
        },
    ];
    let families = [
        "create_catalog",
        "create_schema",
        "register_table",
        "patch_catalog",
        "rename_table",
        "drop_table",
    ];
    let mut model: BTreeMap<Vec<u8>, (u64, Option<ExpectedValue>)> = BTreeMap::new();
    let mut catalog = Value::Null;
    let mut table = Value::Null;
    let mut expected_intents = Vec::new();
    for (index, command) in commands.into_iter().enumerate() {
        let sequence = index as u64 + 1;
        let idempotency = format!("parity-{index}");
        let prepared = authority
            .prepare_synthetic_bounded_command(
                command,
                WriteOptions::with_idempotency(idempotency.clone()),
            )
            .unwrap();
        let at = prepared.occurred_at_ms();
        let operation = prepared.operation_id().to_owned();
        let request_digest = prepared.request_digest().to_owned();
        let mut changes: BTreeMap<Vec<u8>, Option<ExpectedValue>> = BTreeMap::new();
        let response = match index {
            0 => {
                catalog = json!({"version":1,"id":catalog_id,"name":"catalog","description":null,
                    "properties":null,"storageRoot":null,"createdAt":at,"updatedAt":at});
                changes.insert(
                    catalog_key.clone(),
                    Some(ExpectedValue::Json(catalog.clone())),
                );
                changes.insert(
                    key(&[2, 1], &["", "catalog"]),
                    Some(ExpectedValue::Id(catalog_id.into())),
                );
                json!({"kind":"catalog","value":catalog})
            }
            1 => {
                let schema = json!({"version":1,"id":schema_id,"catalogId":catalog_id,"name":"schema",
                    "description":null,"properties":null,"storageRoot":null,"createdAt":at,"updatedAt":at});
                changes.insert(
                    schema_key.clone(),
                    Some(ExpectedValue::Json(schema.clone())),
                );
                changes.insert(
                    key(&[2, 2], &[catalog_id, "schema"]),
                    Some(ExpectedValue::Id(schema_id.into())),
                );
                json!({"kind":"schema","value":schema})
            }
            2 => {
                table = json!({"version":1,"id":table_id,"schemaId":schema_id,"name":"table",
                    "description":null,"location":"s3://parity/table","format":"delta","tableType":"EXTERNAL",
                    "properties":null,"createdAt":at,"updatedAt":at});
                changes.insert(table_key.clone(), Some(ExpectedValue::Json(table.clone())));
                changes.insert(
                    key(&[2, 3], &[schema_id, "table"]),
                    Some(ExpectedValue::Id(table_id.into())),
                );
                changes.insert(
                    column_key.clone(),
                    Some(ExpectedValue::Json(json!({"version":1,
                    "id":column_id,"tableId":table_id,"name":"id","dataType":"BIGINT",
                    "isNullable":false,"ordinal":0,"description":null}))),
                );
                json!({"kind":"table","value":table})
            }
            3 => {
                catalog["description"] = json!("patched");
                catalog["updatedAt"] = json!(at);
                changes.insert(
                    catalog_key.clone(),
                    Some(ExpectedValue::Json(catalog.clone())),
                );
                json!({"kind":"catalog","value":catalog})
            }
            4 => {
                table["name"] = json!("renamed");
                table["updatedAt"] = json!(at);
                changes.insert(table_key.clone(), Some(ExpectedValue::Json(table.clone())));
                changes.insert(key(&[2, 3], &[schema_id, "table"]), None);
                changes.insert(
                    key(&[2, 3], &[schema_id, "renamed"]),
                    Some(ExpectedValue::Id(table_id.into())),
                );
                json!({"kind":"table","value":table})
            }
            5 => {
                changes.insert(table_key.clone(), None);
                changes.insert(column_key.clone(), None);
                changes.insert(key(&[2, 3], &[schema_id, "renamed"]), None);
                json!({"kind":"deleted"})
            }
            _ => unreachable!(),
        };
        authority
            .execute_prepared_synthetic_bounded_command(prepared.clone())
            .await
            .unwrap();
        let token = store.current_state_token().await.unwrap();
        assert_eq!(token.logical_sequence(), sequence);
        let published = store.export_published_logical_v2(&token).await.unwrap();
        let logical_id = published["logicalCommitId"].as_str().unwrap();
        let audit = json!({"version":2,"operationId":operation,"operationFamily":families[index],
            "requestDigest":request_digest,"actor":"api","occurredAtMs":at,
            "logicalCommitId":logical_id,"logicalSequence":sequence});
        changes.insert(
            key(&[4], &[&operation]),
            Some(ExpectedValue::Json(audit.clone())),
        );
        changes.insert(
            key(
                &[3],
                &[
                    families[index],
                    &hex::encode(Sha256::digest(idempotency.as_bytes())),
                ],
            ),
            Some(ExpectedValue::Json(
                json!({"version":2,"operationFamily":families[index],
                "requestDigest":request_digest,"response":response,"logicalCommitId":logical_id,
                "logicalSequence":sequence}),
            )),
        );
        let writes = published["writes"].as_array().unwrap();
        assert_eq!(writes.len(), changes.len());
        for write in writes {
            assert_eq!(write["generation"], sequence);
            let key = hex::decode(write["rawKeyHex"].as_str().unwrap()).unwrap();
            let expected = changes.get(&key).expect("no undeclared writes");
            match expected {
                None => assert!(write["rawValueHex"].is_null()),
                Some(value) => value
                    .assert_bytes(&hex::decode(write["rawValueHex"].as_str().unwrap()).unwrap()),
            }
        }
        for (key, value) in changes {
            model.insert(key, (sequence, value));
        }
        let actual = store
            .scan(ScanRequest::new(b"").with_limits(100, 1024 * 1024, 16))
            .await
            .unwrap();
        assert!(actual.continuation().is_none());
        assert_eq!(
            actual.entries().len(),
            model.values().filter(|(_, value)| value.is_some()).count()
        );
        for entry in actual.entries() {
            let (generation, expected) = model.get(entry.key()).unwrap();
            assert_eq!(entry.value().generation(), Some(*generation));
            expected
                .as_ref()
                .unwrap()
                .assert_bytes(entry.value().bytes());
        }
        let additions = published["additionsV2"].as_array().unwrap();
        assert_eq!(additions.len(), 1);
        let intent: ProjectionIntentV2 = serde_json::from_value(additions[0].clone()).unwrap();
        assert_eq!(intent.intent_id(), operation);
        assert_eq!(
            serde_json::from_slice::<Value>(intent.payload()).unwrap(),
            audit
        );
        expected_intents.push(intent);
        let page = store
            .projection_outbox_page_v2(Some(token.clone()), None, 100)
            .await
            .unwrap();
        assert_eq!(page.records(), expected_intents);
        assert!(page.continuation().is_none());
        authority
            .execute_prepared_synthetic_bounded_command(prepared)
            .await
            .unwrap();
        assert_eq!(
            store.current_state_token().await.unwrap(),
            token,
            "exact replay cannot advance history"
        );
        if let Ok(path) = std::env::var("ARCO_BOUNDED_PARITY_JSONL") {
            use std::io::Write;
            let mut output = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap();
            writeln!(output, "{}", serde_json::to_string(&published).unwrap()).unwrap();
        }
    }
}
