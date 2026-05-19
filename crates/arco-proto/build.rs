//! Build script for compiling protobuf definitions.

const PROTO_FILES: &[&str] = &[
    "../../proto/arco/common/v1/common.proto",
    "../../proto/arco/catalog/v1/catalog.proto",
    "../../proto/arco/catalog/v1/metastore.proto",
    "../../proto/arco/orchestration/v1/orchestration.proto",
    "../../proto/arco/controlplane/v1/transactions.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = tonic_prost_build::configure()
        .codec_path("crate::ProstCodec")
        .btree_map(".");

    config = configure_common_serde_derives(config);
    config = configure_default_serde(config);
    config = configure_metastore_serde(config);
    config = configure_catalog_serialize_derives(config);
    config = configure_camel_case_serde(config);
    config = configure_catalog_timestamp_serde_fields(config);
    config = configure_metastore_serde_fields(config);

    config.compile_protos(PROTO_FILES, &["../../proto"])?;

    for file in PROTO_FILES {
        println!("cargo:rerun-if-changed={file}");
    }
    println!("cargo:rerun-if-changed=../../proto");

    Ok(())
}

fn configure_common_serde_derives(
    config: tonic_prost_build::Builder,
) -> tonic_prost_build::Builder {
    apply_type_attribute(
        config,
        &[
            ".arco.common.v1.PartitionDimension",
            ".arco.common.v1.PartitionKey",
            ".arco.common.v1.ScalarValue",
            ".arco.common.v1.NullValue",
            ".arco.catalog.v1.Principal",
            ".arco.catalog.v1.GroupMembership",
            ".arco.catalog.v1.Grant",
            ".arco.catalog.v1.StorageCredential",
            ".arco.catalog.v1.ServiceCredential",
            ".arco.catalog.v1.ExternalLocation",
            ".arco.catalog.v1.ExternalServiceConnection",
            ".arco.catalog.v1.ManagedStorageRoot",
            ".arco.catalog.v1.WorkspaceBinding",
            ".arco.catalog.v1.GovernanceAttachment",
            ".arco.catalog.v1.PolicyAttachment",
            ".arco.catalog.v1.Volume",
            ".arco.catalog.v1.Function",
            ".arco.catalog.v1.View",
            ".arco.catalog.v1.RegisteredModel",
            ".arco.catalog.v1.ModelVersion",
            ".arco.catalog.v1.Share",
            ".arco.catalog.v1.Provider",
            ".arco.catalog.v1.Recipient",
        ],
        "#[derive(serde::Serialize, serde::Deserialize)]",
    )
}

fn configure_default_serde(config: tonic_prost_build::Builder) -> tonic_prost_build::Builder {
    apply_type_attribute(
        config,
        &[
            ".arco.catalog.v1.Principal",
            ".arco.catalog.v1.GroupMembership",
            ".arco.catalog.v1.Grant",
            ".arco.catalog.v1.StorageCredential",
            ".arco.catalog.v1.ServiceCredential",
            ".arco.catalog.v1.ExternalLocation",
            ".arco.catalog.v1.ExternalServiceConnection",
            ".arco.catalog.v1.ManagedStorageRoot",
            ".arco.catalog.v1.WorkspaceBinding",
            ".arco.catalog.v1.GovernanceAttachment",
            ".arco.catalog.v1.PolicyAttachment",
            ".arco.catalog.v1.Volume",
            ".arco.catalog.v1.Function",
            ".arco.catalog.v1.View",
            ".arco.catalog.v1.RegisteredModel",
            ".arco.catalog.v1.ModelVersion",
            ".arco.catalog.v1.Share",
            ".arco.catalog.v1.Provider",
            ".arco.catalog.v1.Recipient",
        ],
        "#[serde(default)]",
    )
}

fn configure_catalog_serialize_derives(
    config: tonic_prost_build::Builder,
) -> tonic_prost_build::Builder {
    apply_type_attribute(
        config,
        &[
            ".arco.catalog.v1.Catalog",
            ".arco.catalog.v1.Schema",
            ".arco.catalog.v1.Table",
            ".arco.catalog.v1.TableFormat",
            ".arco.catalog.v1.ColumnDefinition",
            ".arco.catalog.v1.CreateCatalogOp",
            ".arco.catalog.v1.CreateSchemaOp",
            ".arco.catalog.v1.RegisterTableOp",
            ".arco.catalog.v1.UpdateTableOp",
            ".arco.catalog.v1.DropTableOp",
            ".arco.catalog.v1.RenameTableOp",
            ".arco.catalog.v1.CatalogDdlOperation",
        ],
        "#[derive(serde::Serialize)]",
    )
}

fn configure_camel_case_serde(config: tonic_prost_build::Builder) -> tonic_prost_build::Builder {
    apply_type_attribute(
        config,
        &[
            ".arco.common.v1.PartitionDimension",
            ".arco.common.v1.PartitionKey",
            ".arco.common.v1.ScalarValue",
            ".arco.catalog.v1.Principal",
            ".arco.catalog.v1.GroupMembership",
            ".arco.catalog.v1.Grant",
            ".arco.catalog.v1.StorageCredential",
            ".arco.catalog.v1.ServiceCredential",
            ".arco.catalog.v1.ExternalLocation",
            ".arco.catalog.v1.ExternalServiceConnection",
            ".arco.catalog.v1.ManagedStorageRoot",
            ".arco.catalog.v1.WorkspaceBinding",
            ".arco.catalog.v1.GovernanceAttachment",
            ".arco.catalog.v1.PolicyAttachment",
            ".arco.catalog.v1.Volume",
            ".arco.catalog.v1.Function",
            ".arco.catalog.v1.View",
            ".arco.catalog.v1.RegisteredModel",
            ".arco.catalog.v1.ModelVersion",
            ".arco.catalog.v1.Share",
            ".arco.catalog.v1.Provider",
            ".arco.catalog.v1.Recipient",
            ".arco.catalog.v1.Catalog",
            ".arco.catalog.v1.Schema",
            ".arco.catalog.v1.Table",
            ".arco.catalog.v1.ColumnDefinition",
            ".arco.catalog.v1.CreateCatalogOp",
            ".arco.catalog.v1.CreateSchemaOp",
            ".arco.catalog.v1.RegisterTableOp",
            ".arco.catalog.v1.UpdateTableOp",
            ".arco.catalog.v1.DropTableOp",
            ".arco.catalog.v1.RenameTableOp",
            ".arco.catalog.v1.CatalogDdlOperation",
        ],
        "#[serde(rename_all = \"camelCase\")]",
    )
}

fn configure_catalog_timestamp_serde_fields(
    mut config: tonic_prost_build::Builder,
) -> tonic_prost_build::Builder {
    for field in [
        ".arco.catalog.v1.Catalog.created_at",
        ".arco.catalog.v1.Catalog.updated_at",
        ".arco.catalog.v1.Schema.created_at",
        ".arco.catalog.v1.Schema.updated_at",
        ".arco.catalog.v1.Table.created_at",
        ".arco.catalog.v1.Table.updated_at",
    ] {
        config = config.field_attribute(
            field,
            "#[serde(skip_serializing_if = \"Option::is_none\", serialize_with = \"crate::serde_helpers::serialize_optional_timestamp\")]",
        );
    }

    config
}

fn apply_type_attribute(
    mut config: tonic_prost_build::Builder,
    types: &[&str],
    attribute: &str,
) -> tonic_prost_build::Builder {
    for &ty in types {
        config = config.type_attribute(ty, attribute);
    }

    config
}

fn configure_metastore_serde(config: tonic_prost_build::Builder) -> tonic_prost_build::Builder {
    config
        .enum_attribute(
            ".arco.catalog.v1.CatalogObjectLifecycleState",
            "#[derive(serde::Serialize, serde::Deserialize)] #[serde(rename_all = \"SCREAMING_SNAKE_CASE\")]",
        )
        .enum_attribute(
            ".arco.catalog.v1.PrincipalType",
            "#[derive(serde::Serialize, serde::Deserialize)] #[serde(rename_all = \"SCREAMING_SNAKE_CASE\")]",
        )
        .message_attribute(
            ".arco.catalog.v1.GrantMutation",
            "#[derive(serde::Serialize, serde::Deserialize)] #[serde(transparent)]",
        )
        .enum_attribute(
            ".arco.catalog.v1.GrantMutation.op",
            "#[derive(serde::Serialize, serde::Deserialize)] #[serde(rename_all = \"camelCase\")]",
        )
        .message_attribute(
            ".arco.catalog.v1.MetastoreMutation",
            "#[derive(serde::Serialize, serde::Deserialize)] #[serde(transparent)]",
        )
        .enum_attribute(
            ".arco.catalog.v1.MetastoreMutation.op",
            "#[derive(serde::Serialize, serde::Deserialize)] #[serde(rename_all = \"camelCase\")]",
        )
}

fn configure_metastore_serde_fields(
    mut config: tonic_prost_build::Builder,
) -> tonic_prost_build::Builder {
    for field in [
        ".arco.catalog.v1.Principal.created_at",
        ".arco.catalog.v1.Principal.updated_at",
        ".arco.catalog.v1.GroupMembership.created_at",
        ".arco.catalog.v1.GroupMembership.updated_at",
        ".arco.catalog.v1.Grant.created_at",
        ".arco.catalog.v1.Grant.updated_at",
        ".arco.catalog.v1.StorageCredential.created_at",
        ".arco.catalog.v1.StorageCredential.updated_at",
        ".arco.catalog.v1.ServiceCredential.created_at",
        ".arco.catalog.v1.ServiceCredential.updated_at",
        ".arco.catalog.v1.ExternalLocation.created_at",
        ".arco.catalog.v1.ExternalLocation.updated_at",
        ".arco.catalog.v1.ExternalServiceConnection.created_at",
        ".arco.catalog.v1.ExternalServiceConnection.updated_at",
        ".arco.catalog.v1.ManagedStorageRoot.created_at",
        ".arco.catalog.v1.ManagedStorageRoot.updated_at",
        ".arco.catalog.v1.WorkspaceBinding.created_at",
        ".arco.catalog.v1.WorkspaceBinding.updated_at",
        ".arco.catalog.v1.GovernanceAttachment.created_at",
        ".arco.catalog.v1.GovernanceAttachment.updated_at",
        ".arco.catalog.v1.PolicyAttachment.created_at",
        ".arco.catalog.v1.PolicyAttachment.updated_at",
        ".arco.catalog.v1.Volume.created_at",
        ".arco.catalog.v1.Volume.updated_at",
        ".arco.catalog.v1.Function.created_at",
        ".arco.catalog.v1.Function.updated_at",
        ".arco.catalog.v1.View.created_at",
        ".arco.catalog.v1.View.updated_at",
        ".arco.catalog.v1.RegisteredModel.created_at",
        ".arco.catalog.v1.RegisteredModel.updated_at",
        ".arco.catalog.v1.ModelVersion.created_at",
        ".arco.catalog.v1.ModelVersion.updated_at",
        ".arco.catalog.v1.Share.created_at",
        ".arco.catalog.v1.Share.updated_at",
        ".arco.catalog.v1.Provider.created_at",
        ".arco.catalog.v1.Provider.updated_at",
        ".arco.catalog.v1.Recipient.created_at",
        ".arco.catalog.v1.Recipient.updated_at",
    ] {
        config = config.field_attribute(
            field,
            "#[serde(default, skip_deserializing, skip_serializing_if = \"Option::is_none\", serialize_with = \"crate::serde_helpers::serialize_optional_timestamp\")]",
        );
    }

    config
}
