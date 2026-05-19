//! Metastore control-plane scope contract tests.
//!
//! These tests pin the migration alias where `metastore_id` defaults to
//! `workspace_id` at the adapter boundary instead of becoming an empty durable
//! authority scope.

use arco_proto::arco::catalog::v1::{
    CatalogControlPlaneScope, MetastoreMutation, StorageCredential, metastore_mutation,
};
use arco_proto::arco::controlplane::v1::{ScopedMetastoreMutation, domain_mutation};

#[test]
fn metastore_scope_contract_preserves_authority_and_execution_context() {
    let scoped = ScopedMetastoreMutation {
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
    };

    let scope = scoped.scope.as_ref().expect("scope should be present");
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
fn metastore_scope_contract_resolves_compatibility_alias_before_durable_use() {
    let scope = CatalogControlPlaneScope {
        tenant_id: "tenant_01".to_string(),
        workspace_id: "workspace_01".to_string(),
        metastore_id: String::new(),
        request_id: "request_01".to_string(),
    };

    assert!(scope.metastore_id.is_empty());

    let durable_metastore_id = scope.effective_metastore_id().to_string();
    assert_eq!(durable_metastore_id, "workspace_01");
    assert!(!durable_metastore_id.is_empty());
}

#[test]
fn metastore_scope_contract_can_be_carried_by_domain_mutations() {
    let scoped = ScopedMetastoreMutation {
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
    };

    let mutation = domain_mutation::Kind::ScopedMetastore(scoped);

    let domain_mutation::Kind::ScopedMetastore(scoped) = mutation else {
        panic!("expected scoped metastore mutation");
    };
    assert_eq!(
        scoped
            .scope
            .as_ref()
            .expect("scope should be present")
            .effective_metastore_id(),
        "metastore_01"
    );
}
