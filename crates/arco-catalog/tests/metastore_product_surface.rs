//! Native catalog product surface contract tests.
//!
//! These tests pin the public metastore object-family contracts before runtime
//! state, routes, or compatibility adapters depend on them.

#![allow(clippy::expect_used)]

use arco_proto::arco::catalog::v1::{
    CatalogObjectLifecycleState, ExternalServiceConnection, GroupMembership, ManagedStorageRoot,
    PolicyAttachment, Principal, PrincipalType, Provider, Recipient, ServiceCredential, Share,
    View,
};

#[test]
fn metastore_product_surface_exposes_first_tranche_object_contracts() {
    let principal = Principal {
        principal_id: "principal_01".to_string(),
        principal_type: PrincipalType::User as i32,
        display_name: "Alice".to_string(),
        owner: "metastore-admin".to_string(),
        lifecycle_state: CatalogObjectLifecycleState::Active as i32,
        created_at: None,
        updated_at: None,
        comment: Some("human user".to_string()),
        properties: [("source".to_string(), "idp".to_string())].into(),
    };
    let group_membership = GroupMembership {
        membership_id: "membership_01".to_string(),
        group_principal_id: "principal_group_01".to_string(),
        member_principal_id: principal.principal_id.clone(),
        group_revision: 7,
        lifecycle_state: CatalogObjectLifecycleState::Active as i32,
        created_at: None,
        updated_at: None,
        comment: None,
        properties: Default::default(),
    };
    let service_credential = ServiceCredential {
        service_credential_id: "service_credential_01".to_string(),
        name: "ml-inference".to_string(),
        service: "model-serving".to_string(),
        owner: "group:ml-platform".to_string(),
        lifecycle_state: CatalogObjectLifecycleState::Active as i32,
        created_at: None,
        updated_at: None,
        comment: None,
        properties: Default::default(),
    };
    let external_service_connection = ExternalServiceConnection {
        connection_id: "connection_01".to_string(),
        name: "feature-store".to_string(),
        service_credential_id: service_credential.service_credential_id.clone(),
        service: "feature-store".to_string(),
        owner: "group:ml-platform".to_string(),
        lifecycle_state: CatalogObjectLifecycleState::Active as i32,
        created_at: None,
        updated_at: None,
        comment: None,
        properties: Default::default(),
    };
    let managed_root = ManagedStorageRoot {
        root_id: "managed_root_01".to_string(),
        name: "workspace-default".to_string(),
        workspace_id: "workspace_01".to_string(),
        url: "s3://bucket/workspaces/workspace_01".to_string(),
        owner: "group:data-platform".to_string(),
        lifecycle_state: CatalogObjectLifecycleState::Active as i32,
        created_at: None,
        updated_at: None,
        comment: None,
        properties: Default::default(),
    };
    let view = View {
        view_id: "view_01".to_string(),
        catalog: "default".to_string(),
        schema: "analytics".to_string(),
        view: "active_users".to_string(),
        owner: "group:analytics".to_string(),
        lifecycle_state: CatalogObjectLifecycleState::Active as i32,
        created_at: None,
        updated_at: None,
        comment: Some("logical user view".to_string()),
        properties: Default::default(),
    };
    let policy_attachment = PolicyAttachment {
        policy_attachment_id: "policy_attachment_01".to_string(),
        object_id: view.view_id.clone(),
        object_type: "VIEW".to_string(),
        policy_id: "policy_01".to_string(),
        policy_type: "MASKING_PLACEHOLDER".to_string(),
        owner: "group:governance".to_string(),
        lifecycle_state: CatalogObjectLifecycleState::Active as i32,
        created_at: None,
        updated_at: None,
        comment: None,
        properties: Default::default(),
    };
    let share = Share {
        share_id: "share_01".to_string(),
        name: "partner-share".to_string(),
        owner: "group:data-products".to_string(),
        lifecycle_state: CatalogObjectLifecycleState::Active as i32,
        created_at: None,
        updated_at: None,
        comment: None,
        properties: Default::default(),
    };
    let provider = Provider {
        provider_id: "provider_01".to_string(),
        name: "partner-provider".to_string(),
        owner: "group:data-products".to_string(),
        lifecycle_state: CatalogObjectLifecycleState::Active as i32,
        created_at: None,
        updated_at: None,
        comment: None,
        properties: Default::default(),
    };
    let recipient = Recipient {
        recipient_id: "recipient_01".to_string(),
        name: "partner-recipient".to_string(),
        owner: "group:data-products".to_string(),
        lifecycle_state: CatalogObjectLifecycleState::Active as i32,
        created_at: None,
        updated_at: None,
        comment: None,
        properties: Default::default(),
    };

    assert_eq!(principal.principal_type, PrincipalType::User as i32);
    assert_eq!(group_membership.group_revision, 7);
    assert_eq!(
        external_service_connection.service_credential_id,
        service_credential.service_credential_id
    );
    assert_eq!(managed_root.workspace_id, "workspace_01");
    assert_eq!(policy_attachment.object_id, view.view_id);
    assert_eq!(
        share.lifecycle_state,
        CatalogObjectLifecycleState::Active as i32
    );
    assert_eq!(
        provider.lifecycle_state,
        CatalogObjectLifecycleState::Active as i32
    );
    assert_eq!(
        recipient.lifecycle_state,
        CatalogObjectLifecycleState::Active as i32
    );
}

#[test]
fn metastore_product_surface_serde_uses_public_camel_case_names() {
    let principal = Principal {
        principal_id: "principal_01".to_string(),
        principal_type: PrincipalType::ServicePrincipal as i32,
        display_name: "pipeline-runner".to_string(),
        owner: "metastore-admin".to_string(),
        lifecycle_state: CatalogObjectLifecycleState::Active as i32,
        created_at: None,
        updated_at: None,
        comment: None,
        properties: Default::default(),
    };

    let json = serde_json::to_value(principal).expect("principal should serialize");

    assert!(json.get("principalId").is_some());
    assert!(json.get("principalType").is_some());
    assert!(json.get("lifecycleState").is_some());
    assert!(json.get("principal_id").is_none());
    assert!(json.get("lifecycle_state").is_none());
}
